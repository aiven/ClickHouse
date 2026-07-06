# Patch 026 — backup-disk-type

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `3139011de01c39698a9b8dd4a8bb35cdc24ec674` | Tilman Moeller (co-author Kevin Michel) | original carry (applied from `0059-Backup_disk.patch`) |
| 26.3-aiven | (staged) | T3 worker (Dispatch 1 of 2) | drift-superseded (re-implemented) — code port only; integration test is Dispatch 2 |
| 26.3-aiven | (staged) | T3 worker (Dispatch 2 of 2) | added integration test `test_aiven_backup_disk` + honest pre/post evidence pair; surfaced a soft-delete correctness finding (see §4/§6) |
| 26.3-aiven | (staged) | T3 worker (extended-coverage dispatch) | extended `test_aiven_backup_disk` to the merge / mutation / TTL removal paths (3 new test functions) + honest pre/post evidence pair (see §4 "Extended coverage") |

The current uplift's row stays "(staged)" until the human commits (after Dispatch 2 adds the test).

## 1. Purpose

A new `backup` disk type that wraps an object-storage disk. Instead of physically
deleting objects, it writes a local **deletion-marker** file per object at
`<base>/<escapeForFileName(remote_path)>`. An external Aiven backup/GC system then
decides which objects are safe to physically delete (only those not referenced by
any backup). `exists`/`listObjects` filter out soft-deleted objects so the table
view stays consistent. This decouples ClickHouse's object lifecycle from physical
deletion, which is the durable motivation: backups must be able to reference object
versions that ClickHouse considers deleted.

Source SHA on `v25.8.18.1-lts-aiven`: `3139011de01c39698a9b8dd4a8bb35cdc24ec674`
(inventory row 026).
Original author: `tilman.moeller@aiven.io` (co-author `kevin.michel@aiven.io`), 2025-12-16.
Original purpose (quoted from `git log`):

> Add Backup disk type. This adds a new disk type that wraps an object storage disk
> and enables coordination with our backup system. When this disk type is used as a
> wrapper on top of an object storage disk, the files in object storage are not
> really deleted. Instead, a local file is created for each file in object storage
> that should be deleted. Our backup system will then be in charge to do garbage
> collection and only delete files that are not referenced by any backup.

There is no upstream 26.3 equivalent (verified: no `backup` disk type, no
`BackupObjectStorage`, no `wrapWithBackup` in `v26.3.10.62-lts`).

**Scope (single-location only).** 26.3 generalized `DiskObjectStorage` to back a
disk by *multiple* object storages keyed by "location" (the
`Disks/DiskObjectStorage/Replication/` subsystem: `ClusterConfiguration`,
`ObjectStorageRouter`, `BlobCopierThread`/`BlobKillerThread`), introduced upstream
as the squashed commit `66a57a5d5ee` ("sync disks", Mikhail Artemenko, 2026-02-03)
— geo/multi-cloud object-storage replication, almost certainly a ClickHouse Cloud
feature. Our deployment uses **single-location object-storage disks only** (tiered
storage). The backup layer soft-deletes objects only at the *local* location's
object storage, so on a multi-location disk the background replication/GC threads
would not observe those soft-deletes and the external-GC contract would be unsound.
Therefore `wrapWithBackup` **rejects a multi-location base disk** with
`BAD_ARGUMENTS` (see §3) rather than silently producing incorrect behavior. An
ordinary disk is the degenerate single-`"main"`-location case, so this guard never
fires for our configs.

## 2. Upstream-drift findings

> This patch is `still-needed-but-rewrite` (heavy). A verbatim cherry-pick is
> impossible: 26.3 (a) relocated the subtree, (b) redesigned the disk-layering
> model, and (c) grew/changed the `IObjectStorage` interface.

### Commands run

Verification was done by reading the live 26.3 source (not a stale spec):
`DiskObjectStorageCache.cpp`, `RegisterDiskCache.cpp`, `DiskObjectStorage.{h,cpp}`,
`ObjectStorages/Cached/CachedObjectStorage.{h,cpp}`, `ObjectStorages/IObjectStorage.{h,cpp}`,
`ObjectStorages/ObjectStorageIterator.h`, `IDisk.h`, `DiskEncrypted.h`,
`ReadOnlyDiskWrapper.h`, `Storages/MergeTree/MergeTreeData.cpp`.

### Findings

- **2a. Subtree relocation.** `src/Disks/ObjectStorages/...` moved under
  `src/Disks/DiskObjectStorage/ObjectStorages/...`, and `DiskObjectStorage.{cpp,h}`
  moved into `src/Disks/DiskObjectStorage/`. The decorator, register file, and
  `wrapWithBackup`/`getStructure` re-target accordingly. `IDisk.h`,
  `DiskEncrypted.h`, `ReadOnlyDiskWrapper.h`, `registerDisks.cpp`,
  `MergeTreeData.cpp`, `CMakeLists.txt` keep their paths.
- **2b. Disk-layering model redesigned (load-bearing).** In 25.8 a layer lived at
  the object-storage level: `wrapWithCache` mutated `object_storage` in place
  (`void`) and `getCacheLayersNames` walked the object-storage chain via
  `getWrappedObjectStorage`/`getCacheConfigName`. In 26.3 a layer is a **named
  `DiskObjectStorage`**: `wrapWithCache` is `const` and **returns a new disk**;
  the per-location object storage lives in an `ObjectStorageRouter` registry keyed
  by `cluster->getLocalLocation()`, and each layer links to the inner disk via
  `wrapped_disk`. `DiskObjectStorage::getCacheLayersNames` now walks the **disk
  chain** (`wrapped_disk`) collecting `layer->getName()`. Consequence: the patch's
  central mechanic (mutate `object_storage`; rename `getCacheLayersNames` →
  `getLayersNames`; add `IObjectStorage::getLayerName`/`getWrappedObjectStorage`)
  is **obsolete** — the 26.3 disk-level walk already enumerates *any* layer (cache
  or backup) by disk name, because a layer *is* a `DiskObjectStorage`. So the
  rename and the object-storage-traversal plumbing are NOT ported.
- **2c. `IObjectStorage` interface grew/changed (decorator must reconcile).**
  Against the 26.3 pure-virtual set (`IObjectStorage.h`):
  - Signature changes: `getObjectMetadata(path)` → `getObjectMetadata(path, bool with_tags)`;
    `readObject(object, read_settings, read_hint, file_size)` → 3-arg
    `readObject(object, read_settings, read_hint)` (26.3 dropped `file_size`).
  - New pure-virtuals to forward: `tryGetObjectMetadata(path, with_tags)`,
    `createKeyGenerator()`.
  - Methods removed from the interface (overriding them fails to compile):
    `areObjectKeysRandom`, `isWriteOnce`, `generateObjectKeyForPath`, `isPlain`
    (key generation now flows through `createKeyGenerator`).
  - Patch-only interface additions NOT carried: `getLayerName`,
    `getWrappedObjectStorage`.
  - New `iterate(path, max_keys, with_tags, start_after)` with a non-filtering
    base default (addressed — see §3 / Q1).
  - S3/Azure accessors (`getS3StorageClient`/`tryGetS3StorageClient`,
    `getAzureBlobStorageClient`/`getAzureBlobStorageAuthMethod`, `tagObjects`) have
    throwing defaults (addressed — see §3 / Q2).

- **Conclusion: `still-needed-but-rewrite`.** Semantics unchanged; re-implemented
  against 26.3's architecture per the 10-file plan (3 new + 7 edited; the 4 dropped
  files are the obsolete object-storage-traversal plumbing). The decorator is
  modeled directly on 26.3's `CachedObjectStorage` (the sibling layer) for fidelity.

## 3. C++ review

Applying `docs/aiven/skills/cpp-review-checklist.md`:

- **1 Lifetime + ownership:** ✓ — `BackupObjectStorage` holds an `ObjectStoragePtr`
  (`shared_ptr`, matching `CachedObjectStorage`); `wrapWithBackup` returns a
  `DiskObjectStoragePtr` and the new layer disk keeps the inner disk alive via
  `wrapped_disk` (`DiskObjectStorageConstPtr`), exactly mirroring `wrapWithCache`.
  No raw owning pointers (`log` is a `LoggerPtr`, replacing the source's
  `Poco::Logger *`).
- **2 Exception safety:** ✓ — delete becomes a single `FS::createFile`
  (marker write); on throw no object is physically removed and no partial object
  state exists. `wrapWithBackup` builds the new registry/router/disk before
  returning, so a throw leaves the original disk untouched.
- **3 Thread-safety + concurrency:** ✓ — the decorator adds no shared mutable
  state; it forwards to `object_storage` and reads/writes per-object marker files
  on the local filesystem. No locks, no threads, no sleeps.
- **4 Performance + memory:** ✓ (acceptable) — soft-delete adds one `stat`
  (`FS::exists` on the marker) per `exists`/listed object and one `createFile` per
  delete. This is a control-/maintenance-path cost, not a per-row hot path.
  `iterate` reuses the filtered `listObjects` (eager) instead of the wrapped
  storage's lazy iterator — a deliberate correctness-over-laziness trade documented
  below.
- **5 Settings as public API:** n/a — no new server/query settings; behavior is
  selected by `disk(type=backup, disk=...)` configuration only.
- **6 Error handling:** ✓ — `registerDiskBackup` uses the real `BAD_ARGUMENTS`
  code and distinctive messages ("Disk Backup requires `disk` field", "backup disk
  is allowed only on top of object storage"), mirroring `registerDiskCache`.
- **7 Upstream / vendored code:** ✓ — no `contrib/**`, `.claude/**`,
  `.github/workflows/**`, or root `AGENTS.md` edits. `IObjectStorage.h`,
  `CachedObjectStorage.h`, and `DiskObjectStorageCache.cpp` are deliberately
  untouched (26.3's disk-level walk already covers layer traversal).
- **8 Behavior under settings:** n/a — the feature is only active for disks
  explicitly configured as `type=backup`; ordinary disks are unaffected
  (`IDisk::supportsLayers()` defaults to `false`; only `DiskObjectStorage` returns
  `true`).

### Ratified design decisions (spec §6, all = yes)

- **Q1 — `iterate` filtering:** overridden. `BackupObjectStorage::iterate` calls the
  decorator's own filtering `listObjects` and wraps the result in
  `ObjectStorageIteratorFromList` (the same shape as the base default). This keeps
  `iterate` consistent with the soft-delete view. Invariant verified:
  `IObjectStorage::existsOrHasAnyChild` and the base `iterate` both call the
  virtual `listObjects`, which dispatches to the filtering override — so even
  callers that bypass our `iterate` still observe filtered results. The override
  loses the wrapped storage's lazy iteration (it materializes via `listObjects`);
  this matches the base default's behavior and is the correctness-preserving
  choice. A future optimization could wrap `object_storage->iterate` in a filtering
  iterator if listing performance becomes a concern.
- **Q2 — S3/Azure client forwarding:** forwarded. `getS3StorageClient`/
  `tryGetS3StorageClient` (under `USE_AWS_S3`), `getAzureBlobStorageClient`/
  `getAzureBlobStorageAuthMethod` (under `USE_AZURE_BLOB_STORAGE`), and `tagObjects`
  (under `USE_AZURE_BLOB_STORAGE || USE_AWS_S3`) forward to `object_storage`, so
  backup-over-S3/Azure does not hit the throwing base defaults on server-side
  copy / presigned-URL / tagging paths. Signatures match the interface exactly
  (`getS3StorageClient` is non-`const`; the Azure accessors are `const`).
- **Q3 — `MergeTreeData` delegate-layers branch:** intended (see §6 — this edit is
  BLOCKED in Dispatch 1; see below).
- **Q4 — metadata pass-through:** `wrapWithBackup` passes `metadata_storage`
  through unchanged (backup intercepts object deletes, not metadata layout), unlike
  `wrapWithCache` which wraps it in `MetadataStorageFromCacheObjectStorage`.
- **Q5 — single-location guard (added per maintainer context, 2026-06-04):**
  `wrapWithBackup` throws `BAD_ARGUMENTS` if `cluster->getConfiguration().size() > 1`.
  The backup layer only soft-deletes at the local location; on a multi-location
  (replicated) disk the `BlobCopierThread`/`BlobKillerThread` would not see the
  deletion markers, making the external-GC contract unsound. Our deployment is
  single-location (tiered storage), so this fail-fast encodes the supported
  invariant; revisit if multi-location tiered storage is ever adopted. (This needed
  adding `extern const int BAD_ARGUMENTS;` to `DiskObjectStorage.cpp`'s `ErrorCodes`
  block.)
- **Q6 — deferred-delete shared-queue defect + Fix A (ratified, 2026-06-04):**
  The original Dispatch-1 port had a confirmed correctness defect (recorded as the
  §4 "soft-delete is defeated" finding). On 26.3 blob deletion is **deferred**: a
  `DROP`/merge enqueues blobs into a single in-memory removal queue owned by
  `metadata_storage`, and a per-disk background `BlobKillerThread` later drains it
  via `object_storages->takePointingTo(location)->removeObjectsIfExist(...)`.
  `wrapWithBackup` swaps only the local-location object storage for
  `BackupObjectStorage` (markers ✓) while passing `metadata_storage` through
  unchanged (Q4), so the **backup disk and the wrapped/inner disk share ONE removal
  queue**. The `DiskObjectStorage` ctor additionally **chains** the inner killer
  (`blob_killer(make_shared<BlobKillerThread>(..., wrapped_disk ? wrapped_disk->blob_killer : nullptr))`),
  and `BlobKillerThread::triggerAndWait` fires the wrapped killer. Result: BOTH
  killers drain the same queue — the backup killer writes a marker (correct), but
  the inner killer routes to the RAW `LocalObjectStorage` and physically `unlink`s
  the blob, defeating the marker and the external-GC contract. (For a *cache* layer
  the inner physical delete is desired; for *backup* it must never happen.)

  **Fix A (ratified).** On the single shared removal queue, ONLY the backup disk's
  killer may run. Two small additions to `BlobKillerThread` — `detachWrapped`
  (sets `wrapped_blob_killer = nullptr` so `triggerAndWait` won't fire the inner
  killer) and `disable` (sets `enabled = false` and `task->deactivate()`;
  idempotent and safe after `startup` because `task` is created in the ctor) — and
  two calls at the end of `wrapWithBackup`: `backup_disk->blob_killer->detachWrapped()`
  and `blob_killer->disable()` (here `this` *is* the wrapped/inner disk, since
  `backup_disk->wrapped_disk == shared_from_this()`). The backup disk's own killer
  stays active (started by `registerDiskBackup`'s `backup_disk->startup(...)`) and
  becomes the **sole drainer** → markers + `recordAsRemoved`. We did NOT pursue
  Fix B (distinct location — the queue is not location-partitioned) or Fix C
  (metadata wrapper — unnecessary for correctness and would still require disabling
  the inner killer).

  **Member-name deviation (reported).** `BlobKillerThread::wrapped_blob_killer` was
  declared `const std::shared_ptr<BlobKillerThread>`; `detachWrapped` cannot reset a
  `const` member, so the `const` qualifier was dropped (the member is private and
  reset only once, at construction time in `wrapWithBackup`, before the backup
  killer is ever started — so no data race is introduced). All other assumed names
  (`blob_killer`, `enabled`, `task`, `applyNewSettings`) matched the live code.

  **Residual risk.** Fix A relies on the **base (wrapped) disk being private to the
  backup wrapper** — i.e. nothing else holds and drives the inner disk's killer over
  that shared queue. This holds for our single-location tiered-storage deployment
  (the inner `object_storage` disk exists only to be wrapped). If a future
  deployment exposed the same base disk both wrapped and unwrapped, disabling the
  inner killer would also stop the unwrapped disk's GC — out of scope here and
  guarded against indirectly by the Q5 single-location restriction.

## 4. Test design

**Test:** `tests/integration/test_aiven_backup_disk/test.py`
(`test_backup_disk_soft_delete`). Single ClickHouse node, no ZooKeeper.

**Shape (inline SQL custom disk, no MinIO/S3).** The test wraps a single-location
**local** object storage with a backup layer entirely from SQL:

```sql
CREATE TABLE backup_tbl (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    type = backup,
    path = '/var/lib/clickhouse/aiven_backup_markers/',
    disk = disk(type = object_storage, object_storage_type = local,
                path = '/var/lib/clickhouse/aiven_backup_obj/'));
```

Inline nested `disk(...)` flattens **post-order** (`InDepthNodeVisitor<…, false>`
in `DiskFromAST.cpp`), so the inner `object_storage` disk is created and registered
in the `DisksMap` first, then `registerDiskBackup` finds it by name and wraps it.
`DiskObjectStorage::isRemote` is `true`, so the inline local-object-storage path is
*not* subject to the `custom_local_disks_base_directory` prefix check — no server
config needed. `object_storage_type=local` stores each object at its absolute
`remote_path` on the node filesystem, and the markers land under the backup base
path, both inspectable via `exec_in_container`.

**Why inline and not a server-config disk (fallback not used):** the inline form
lets the server start on *both* binaries, so the pre/post divergence is a clean
`CREATE TABLE` success/failure rather than a messy pre-patch container-start
failure.

**Post-patch observable (asserted, PASS):** after `INSERT` + `DROP TABLE … SYNC`,
`BackupObjectStorage::removeObjectImpl` writes a deletion-marker file
`<base>/<escapeForFileName(remote_path)>` for each removed object; the test asserts
markers exist and that at least one decodes to a real object key under the wrapped
object-storage path. Evidence: `tmp/patch-026/test-postpatch.log` (`1 passed`).

**Pre-patch divergence (asserted, FAIL):** `disk(type = backup, …)` is an unknown
disk type, so `CREATE TABLE` throws. Exact observed error
(`tmp/patch-026/test-prepatch.log`):

> `Code: 137. DB::Exception: … DiskFactory: the disk '__tmp_internal_…' has unknown
> disk type: backup: While processing disk(type = backup, …). … (UNKNOWN_ELEMENT_IN_CONFIG)`

The pair was produced via the single-axis worktree-flip with a forced `IDisk.h`
include-closure rebuild on **both** flips (testing-suites §6 + build-and-test §7);
see §6.

### Strengthened invariant — physical survival of soft-deleted blobs (Fix A)

After Fix A (see §3 / Q6) the test asserts the **actual** soft-delete invariant, in
addition to the pre-existing deletion-marker assertion: after `DROP TABLE … SYNC`
and after the background `BlobKillerThread` has drained the shared removal queue,
the underlying object-storage blob files **physically still exist** under the
wrapped `object_storage_type=local` path. The strengthened assertion (verbatim):

```python
missing = sorted(set(marked_under_obj) - surviving)
assert not missing, (
    "backup-wrapped object-storage blobs were physically deleted despite the "
    "soft-delete markers (the inner BlobKillerThread defeated the backup "
    f"layer); missing blobs: {missing}"
)
```

`marked_under_obj` is the set of object keys (under the wrapped object-storage path)
that the backup killer recorded a marker for; `surviving` is re-listed from the
wrapped path after a test-side poll that gives the (pre-Fix-A) inner killer ample
scheduling rounds to act. No server-side sleeps are used — the wait is pure
filesystem polling. With `disk_transaction_wait_for_blob_removal` defaulting to
`true`, `DROP TABLE … SYNC` drains the queue synchronously via the backup disk's
killer (`DiskObjectStorageTransaction::waitBlobRemoval` loops `triggerAndWait`), so
the divergence is deterministic.

**Evidence-of-causation for Fix A specifically** (fix-isolating flip; only the three
fix files reverted, the test held constant across both binaries):

- **Post-fix PASS** (`tmp/patch-026/test-fixA-postfix.log`):
  `1 passed, 3 warnings in 19.76s` — markers present AND blobs physically survive.
- **Pre-fix FAIL** (`tmp/patch-026/test-fixA-prefix.log`): `1 failed, 4 warnings in
  4.15s` on exactly the new assertion —
  `AssertionError: backup-wrapped object-storage blobs were physically deleted
  despite the soft-delete markers (the inner BlobKillerThread defeated the backup
  layer); missing blobs: [...]` (12 blobs listed). The marker assertion still passes
  pre-fix (the backup killer runs); only the physical-survival assertion flips —
  pinning the failure to the inner killer's raw `unlink`, which Fix A removes.

The flip reverted `DiskObjectStorage.cpp` to the staged D1+guard version and
`BlobKillerThread.{h,cpp}` to clean upstream (the `detachWrapped`/`disable` methods
and their two call sites vanish together — internally consistent), rebuilt, ran the
test (FAIL), then re-applied `tmp/patch-026/fixA.diff`, rebuilt, and re-confirmed
PASS. All three builds were plain incremental (`EXIT=0`, 29 / 28 / 24 `Building CXX
object` lines + relink — NOT the ~800-TU `IDisk.h` closure rebuild, because Fix A
touches no struct layout or vtable: it adds two methods and drops a `const`
qualifier on a private member). Postcondition verified: `git diff` of the three fix
files equals `tmp/patch-026/fixA.diff`; HEAD never moved.

### Extended coverage — physical survival across ALL normal removal paths (merge / mutation / TTL)

The DROP test above proves physical survival for the **DROP** path only. The
data-safety guarantee Aiven needs is broader: **no normal ClickHouse removal path
physically deletes a backup-wrapped blob.** By construction every blob removal on a
`DiskObjectStorage` funnels through `DiskObjectStorageTransaction` → the single
shared `metadata_storage` removal queue → the per-disk `BlobKillerThread`
(`DiskObjectStorageTransaction::waitBlobRemoval` loops `blob_killer->triggerAndWait`
under the default synchronous `disk_transaction_wait_for_blob_removal`; the same
queue is drained by the background killer for async removals). Fix A makes the
backup disk's killer the **sole drainer** of that one queue, so the survival
guarantee should hold for *every* removal path — not just DROP. Three new test
functions in the same `test.py` prove it empirically for the remaining normal paths:

| Test function | Removal path | Trigger | What is captured / asserted |
|---|---|---|---|
| `test_backup_disk_merge_soft_delete` | **Merge** | `OPTIMIZE TABLE … FINAL` (two source parts → one) | captures the source parts' object keys, then asserts the orphaned source blobs physically survive |
| `test_backup_disk_mutation_soft_delete` | **Mutation** | `ALTER TABLE … DELETE WHERE …` (`mutations_sync = 2`) | captures the old part's object keys, then asserts the orphaned old blobs physically survive |
| `test_backup_disk_ttl_soft_delete` | **TTL-delete** | row `TTL d + INTERVAL 1 SECOND DELETE`, materialized by `OPTIMIZE … FINAL` | captures the source part's object keys, then asserts the orphaned source blobs physically survive |

**Determinism knobs.** Outdated/source/old parts are normally removed only after
`old_parts_lifetime` (default 480s). Each new table sets, **per-table** (not global
config, to avoid perturbing the DROP test), `old_parts_lifetime = 1`,
`cleanup_delay_period = 1`, `cleanup_delay_period_random_add = 1`, so the background
cleanup removes the orphaned parts within a few seconds. The tests then **poll**
(test-side, no server-side sleeps): `system.parts` until the orphaned parts are gone
(blobs enqueued + drained at that point); the marker directory until deletion markers
are recorded for the captured keys (proves the killer actually drained the queue, so
a survival pass is real evidence and not a "killer never ran" artifact); and finally
the filesystem (`node.exec_in_container(['find', …])`) for survival. The mutation
test additionally polls `system.mutations` for `is_done = 1`. Merges are held with
`SYSTEM STOP MERGES` while the source parts are captured so the operation has a
genuine multi-part input.

**Honest scoping (load-bearing).** Two refinements keep the assertion a real
evidence-of-causation and not a tautology:

1. **Per-part capture.** The captured object keys are scoped (via the blob's
   `local_path` part directory in `system.remote_data_paths`) to the *source parts*.
   This excludes the persistent table-level blob (`format_version.txt`, which lives
   directly under `store/<uuid>/` and is never removed by any of these paths) — it
   would otherwise never get a deletion marker and falsely fail the marker gate.
2. **`removed` = source keys no longer referenced after the operation.** The survival
   assertion targets only the source blobs that the operation actually orphaned
   (`source_blobs − referenced_after`). A blob still referenced afterwards (e.g. a
   column blob hardlinked into a mutation's rewritten part) survives trivially via
   metadata refcount and is *not* evidence for the backup guarantee, so it is excluded
   from both the marker gate and the survival assertion.

The strengthened assertion (verbatim, shared by all three new paths):

```python
missing = sorted(removed - surviving)
assert not missing, (
    f"backup-wrapped object-storage blobs orphaned by the {path_label} path "
    "were physically deleted despite the soft-delete markers (the inner "
    f"BlobKillerThread defeated the backup layer); missing blobs: {missing}"
)
```

**Evidence-of-causation (fix-isolating flip; only the three Fix-A files reverted in
the worktree via `git apply -R tmp/patch-026/fixA.diff`, the index/staged fix and
HEAD untouched, the test held constant across both binaries):**

- **Post-fix PASS** (`tmp/patch-026/test-paths-postfix.log`):

  ```
  test_aiven_backup_disk/test.py::test_backup_disk_soft_delete PASSED        [ 25%]
  test_aiven_backup_disk/test.py::test_backup_disk_merge_soft_delete PASSED  [ 50%]
  test_aiven_backup_disk/test.py::test_backup_disk_mutation_soft_delete PASSED [ 75%]
  test_aiven_backup_disk/test.py::test_backup_disk_ttl_soft_delete PASSED    [100%]
  =================== 4 passed, 3 warnings in 74.86s (0:01:14) ===================
  ```

- **Pre-fix FAIL** (`tmp/patch-026/test-paths-prefix.log`):

  ```
  test_aiven_backup_disk/test.py::test_backup_disk_soft_delete FAILED        [ 25%]
  test_aiven_backup_disk/test.py::test_backup_disk_merge_soft_delete FAILED  [ 50%]
  test_aiven_backup_disk/test.py::test_backup_disk_mutation_soft_delete FAILED [ 75%]
  test_aiven_backup_disk/test.py::test_backup_disk_ttl_soft_delete FAILED    [100%]
  ======================== 4 failed, 4 warnings in 11.95s ========================
  ```

  Each new path fails on exactly the new physical-survival assertion (the marker
  gate still PASSES pre-fix — both killers drain the shared queue, so markers appear
  — only survival flips, which pins the failure to the inner killer's raw `unlink`):

  - merge: `AssertionError: backup-wrapped object-storage blobs orphaned by the merge
    path were physically deleted despite the soft-delete markers …; missing blobs: [20 blobs]`
  - mutation: `… orphaned by the mutation path … missing blobs: [10 blobs]`
  - TTL: `… orphaned by the TTL path … missing blobs: [11 blobs]`

  (The pre-existing DROP test also FAILs pre-fix on its survival assertion, consistent
  with the earlier Fix-A evidence pair.)

Both flip builds were **plain incremental**, `EXIT=0`: `build_026_paths_prefix.log`
and `build_026_paths_restore.log` each have **24** `Building CXX object` lines +
relink — NOT the ~800-TU `IDisk.h` closure rebuild, because this dispatch reverts
only the three Fix-A source files (no header that changes struct layout / vtable is
touched). Postcondition verified: `git diff` of the three fix files is empty
(worktree == staged index); HEAD never moved (`75dc24a6780`).

**Conclusion.** All four normal ClickHouse blob-removal paths (DROP, merge, mutation,
TTL-delete) funnel through the one shared `metadata_storage` removal queue, now
drained solely by the backup disk's `BlobKillerThread`. With Fix A every path
soft-deletes (markers) and never physically unlinks a backup-wrapped blob; without
Fix A every path physically deletes it. The external-GC contract holds across the
removal surface, not just for DROP.

## 5. Rollback considerations

- **Revert safety:** the change is additive (a new disk type + a new
  `supportsLayers()` virtual). Reverting requires no schema migration and no
  on-disk format change for ordinary disks. A deployment that actually configured a
  `backup` disk would, after revert, fail to load that disk config (`Unknown disk
  type: backup`) — i.e. revert is only safe if no live config references the type.
- **State surviving restart:** yes — deletion-marker files persist on local disk
  under the backup base path (default `<clickhouse>/disks/<name>/backup/`). They are
  the feature's intended durable state and are consumed by the external Aiven GC.
- **Disable without rebuild:** remove the `type=backup` wrapper from the disk
  configuration; the underlying object-storage disk then deletes normally.

## 6. Per-uplift notes

### 25.8-aiven (historical)

Original carry, applied from `0059-Backup_disk.patch`. Object-storage-level layering
with in-place `object_storage` mutation, `getCacheLayersNames` → `getLayersNames`
rename, and `IObjectStorage::getLayerName`/`getWrappedObjectStorage` additions.

### 26.3-aiven (this uplift)

- Cherry-pick was: **rewritten** (`still-needed-but-rewrite`). Not a `git
  cherry-pick`; re-implemented against 26.3's disk-level layering.
- 10-file plan executed (3 new + 7 edited intended):
  - NEW `src/Disks/DiskObjectStorage/ObjectStorages/Backup/BackupObjectStorage.h`
  - NEW `src/Disks/DiskObjectStorage/ObjectStorages/Backup/BackupObjectStorage.cpp`
  - NEW `src/Disks/DiskObjectStorage/ObjectStorages/Backup/registerDiskBackup.cpp`
  - EDIT `src/CMakeLists.txt` (add `Backup` source dir next to `Cached`)
  - EDIT `src/Disks/registerDisks.cpp` (declare + call `registerDiskBackup` before `registerDiskCache`)
  - EDIT `src/Disks/IDisk.h` (`virtual bool supportsLayers() const { return false; }`)
  - EDIT `src/Disks/DiskEncrypted.h` (`supportsLayers()` forwards to delegate)
  - EDIT `src/Disks/ReadOnlyDiskWrapper.h` (`supportsLayers()` forwards to delegate)
  - EDIT `src/Disks/DiskObjectStorage/DiskObjectStorage.h` (`wrapWithBackup` decl + `supportsLayers() override { return true; }`)
  - EDIT `src/Disks/DiskObjectStorage/DiskObjectStorage.cpp` (`wrapWithBackup` impl + include + single-location `BAD_ARGUMENTS` guard, Q5; **+ Fix-A killer wiring**: `wrapWithBackup` now binds the new disk to `backup_disk`, calls `backup_disk->blob_killer->detachWrapped()` and `blob_killer->disable()` before returning, with a comment explaining the deferred-delete/shared-queue rationale — Q6)
  - EDIT `src/Disks/DiskObjectStorage/Replication/BlobKillerThread.h` (**Fix A**: declare `detachWrapped`/`disable`; drop `const` on the private `wrapped_blob_killer` member so `detachWrapped` can null it — Q6)
  - EDIT `src/Disks/DiskObjectStorage/Replication/BlobKillerThread.cpp` (**Fix A**: implement `detachWrapped` = `wrapped_blob_killer = nullptr`, `disable` = `enabled = false; task->deactivate()` — Q6)
  - EDIT `src/Storages/MergeTree/MergeTreeData.cpp` (gate flip `supportsCache()`→`supportsLayers()` + Q3 delegate branch) — **delivered as a patch file, see below**
- Dropped (obsolete on 26.3): `DiskObjectStorageCache.cpp` (`getLayersNames`
  rewrite), `IObjectStorage.h` (`getLayerName`/`getWrappedObjectStorage`),
  `CachedObjectStorage.h` (`getCacheConfigName`/`getWrappedObjectStorage` churn).
  `getCacheLayersNames` is NOT renamed.
- **Blocker (escalated):** the `MergeTreeData.cpp` edit could not be applied. The
  editing tool's pre-write hook (`deny-upstream-file-writes.sh`) is invoked with
  `spawn E2BIG` on this file because `MergeTreeData.cpp` is ~454 KB and the runtime
  passes the edit payload as a single argument/env entry that exceeds Linux's
  `MAX_ARG_STRLEN` (128 KB). The failure is deterministic and independent of edit
  size; smaller-file edits in this same dispatch all succeeded. `MergeTreeData.cpp`
  is NOT on the never-touch list (the hook would *allow* the path), so this is a
  tooling limitation, not a policy denial. The build is green WITHOUT this edit
  because the change is a runtime gate flip that depends only on symbols that
  already exist (`supportsCache`/`getCacheLayersNames`); the gate-flip is required
  for correct backup-/cache-layer enumeration in `getOrphanedPartsDisks`. The exact
  intended replacement (Allman braces, `getCacheLayersNames` kept) is delivered as
  `tmp/patch-026/mergetreedata-gateflip.patch`; the human applies it with
  `git apply --recount tmp/patch-026/mergetreedata-gateflip.patch && git add
  src/Storages/MergeTree/MergeTreeData.cpp` (fallback `patch -p1 --fuzz=3`). It is
  compile-verified by Dispatch 2's clean rebuild before any test runs.
- **Build correctness hazard (handled):** `IDisk.h` gained a new virtual
  (`supportsLayers`), changing the `IDisk` vtable layout. Per
  `docs/aiven/runbooks/build-and-test.md` §7, this `build/` has `#deps 0`, so an
  incremental build does NOT recompile every includer of `IDisk.h`. The forced
  `IDisk.h` include-closure rebuild (`python3 tmp/touch_includers.py` retargeted to
  `Disks/IDisk.h`, then `ninja -C build clickhouse`) was performed: post-patch by
  the parent, and by Dispatch 2 on **both** sides of the worktree flip. Each flip
  build was a closure recompile, not a relink — pre-patch `build_026_prepatch.log`
  had **821** `Building CXX object` lines (`BUILD_EXIT=0`); post-patch-restore
  `build_026_postpatch_restore.log` had **804** (`BUILD_EXIT=0`). The binary used
  for both evidence halves is vtable-consistent. Postcondition verified:
  `git diff -- <staged src set>` was empty (worktree == index) and the 11-file
  staged source set was unchanged; HEAD never moved.
- Test at: `tests/integration/test_aiven_backup_disk/test.py`
  (`test_backup_disk_soft_delete`); see §4 for shape and observables. The test now
  asserts BOTH the deletion markers AND physical survival of the soft-deleted blobs
  (Fix A invariant). Two evidence pairs exist:
  - **patch-026 vs pre-026** (`backup` disk type itself): `tmp/patch-026/test-postpatch.log`
    (PASS) and `tmp/patch-026/test-prepatch.log` (FAIL, `unknown disk type: backup`).
  - **Fix-A fix-isolating flip** (the deferred-delete defect, Q6):
    `tmp/patch-026/test-fixA-postfix.log` (PASS, `1 passed … 19.76s`) and
    `tmp/patch-026/test-fixA-prefix.log` (FAIL, `1 failed … 4.15s` on the
    physical-survival assertion). The Fix-A delta is saved at `tmp/patch-026/fixA.diff`;
    incremental build logs `build_026_fixA.log` / `build_026_prefix.log` /
    `build_026_postfix_restore.log` (all `EXIT=0`, plain incremental).
  - **Extended-coverage flip** (merge / mutation / TTL removal paths; same Fix-A
    delta isolated via `git apply -R tmp/patch-026/fixA.diff`):
    `tmp/patch-026/test-paths-postfix.log` (`4 passed … 74.86s`) and
    `tmp/patch-026/test-paths-prefix.log` (`4 failed … 11.95s`, each new path failing
    on the physical-survival assertion). Incremental build logs
    `build_026_paths_prefix.log` / `build_026_paths_restore.log` (both `EXIT=0`, 24
    `Building CXX object` lines + relink — plain incremental, NOT the `IDisk.h`
    closure: only the three Fix-A files are reverted, no layout-changing header).
    See §4 "Extended coverage" for the per-path table, scoping rationale, and the
    verbatim PASS/FAIL evidence.
- Time-to-port: warm-cache incremental build, `clickhouse` link exit 0.
- Anything surprising: the `MergeTreeData.cpp` `spawn E2BIG` editing block (above).

## 7. Patch-fix (2026-07-15) — idempotent `removeObjectImpl`

Source: `v25.8.26.11-lts-aiven` `e01a164dd6b` ("BackupObjectStorage: make
`removeObjectImpl` idempotent"), a post-fork-point addition on the Aiven 25.8 branch
(see the 2026-07-15 source-branch re-check note in `inventory.md`). Status: **port
applied to 26.3, uncommitted.**

### Problem (invariant it protects)

The soft-delete marker is written with `FS::createFile`, which opens
`O_WRONLY | O_CREAT | O_EXCL` (`src/Common/filesystemHelpers.cpp`). `O_EXCL` makes a
second write for the *same* object key throw `CANNOT_CREATE_FILE` (`EEXIST`). That
violates the idempotency contract of `removeObjectsIfExist` / `removeObjectIfExists`
("removing an already-removed object is a no-op"): the marker is **presence-only**
(its contents are never read — `isSoftDeleted` is just `FS::exists`), so re-marking an
already-marked object must succeed silently. Upstream symptom: `SYSTEM RESTORE
REPLICA` fails when its internal retry re-removes an object it already soft-deleted.

### Fix

`removeObjectImpl` now opens `::open(removed_marker_path, O_WRONLY | O_CREAT, 0666)`
(no `O_EXCL`) and reports genuine failures via
`ErrnoException::throwFromPath(ErrorCodes::CANNOT_CREATE_FILE, …)`. Byte-identical
logic to `e01a164`; the only adaptations are the 26.3 file location
(`src/Disks/DiskObjectStorage/ObjectStorages/Backup/BackupObjectStorage.cpp`, so it is
hand-applied rather than cherry-picked) and the added includes
(`<fcntl.h>`, `<Common/ErrnoException.h>` — note `ErrnoException` lives in its own
header in 26.3, not `<Common/Exception.h>`) plus the local `ErrorCodes` extern.

### Why no new integration test

Re-removing an already-removed object is **not deterministically reachable from SQL**:
backup-disk object keys are derived per-table (each table has a distinct UUID → distinct
metadata paths → distinct keys), so no normal DDL/DML sequence causes the same key to be
removed twice. The trigger is an abnormal retry/restore path (`SYSTEM RESTORE REPLICA`
internal retry), which cannot be induced deterministically without contrived transient
faults or sleeps — hence the upstream fix itself shipped testless. Rather than commit a
flaky, timing-dependent test (against the repo's no-flaky/no-sleep-for-races policy), we
rely on the existing `test_aiven_backup_disk` coverage: all four removal paths
(DROP/merge/mutation/TTL, §4) exercise `removeObjectImpl` on the happy path and therefore
guard the fixed function against regression. If belt-and-suspenders coverage is later
desired, a `ReplicatedMergeTree` + Keeper `SYSTEM RESTORE REPLICA` reproduction is the
candidate — flagged here as flakiness-prone and deferred.

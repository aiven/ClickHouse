# Patch 048 — zero-copy-fixes

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `c6d9323ba2` | Tilman Moeller (author), co-authored by Joe Lynch, 2026-01-07 | (the version we are porting FROM) |
| 26.3-aiven | `patch-port(048)` (see inventory for SHA; STAGED — HEAD unmoved) | parent agent, 2026-06-04 | **`partial`** — Leg 1 dropped (`obsoleted-by-upstream`), Leg 2 ported (`still-needed`) |

The source commit `c6d9323ba2` bundles two independent fixes. They are
dispositioned separately on the 26.3 base.

## 1. Purpose

The original commit carried two unrelated zero-copy fixes:

- **Leg 1 — drop-table validation.** Removed a `ZERO_COPY_REPLICATION_ERROR`
  throw in `MergeTreeData::dropAllData` that blocked dropping a table when the
  table directory was non-empty after parts were removed (occurs when an
  external restore tool — Astacus — leaves leftover files).
- **Leg 2 — sharding race with zero-copy locks.** Zero-copy part locks are
  keyed by `table_shared_id` (the table UUID), which is identical across the
  shards of the same `Replicated`-database table. Two shards committing a
  same-named part (e.g. the first part `all_0_0_0` of a partition)
  concurrently race to create the shared lock-ancestor znode
  `.../zero_copy_<disk>/<uuid>/<part_name>`, and the loser hits a hard
  `LOGICAL_ERROR`. Especially prevalent on `SYSTEM RESTORE REPLICA` of the
  first partition. The fix pre-creates that ancestor idempotently before the
  commit transaction is assembled.

## 2. Upstream-drift / validity findings (per leg)

> Mandatory section. Verify each leg is still SEMANTICALLY needed against
> `v26.3.10.62-lts`.

### Leg 1 — `obsoleted-by-upstream`. DROPPED. No change to `MergeTreeData.cpp`.

Upstream `25b0406c35c` (Alexey Milovidov, 2026-02-15, "Fix table drop getting
permanently stuck with zero-copy replication"; **verified ancestor of HEAD**)
reworked the exact block the old patch deleted. On the 26.3 base,
`MergeTreeData::dropAllData` (`src/Storages/MergeTree/MergeTreeData.cpp`
~4067-4092) no longer throws `ZERO_COPY_REPLICATION_ERROR` on a non-empty
directory; instead it:

```cpp
if (!isSharedStorage() && !disk->isDirectoryEmpty(relative_data_path)
    && supportsReplication() && disk->supportZeroCopyReplication()
    && (*settings_ptr)[MergeTreeSetting::allow_remote_fs_zero_copy_replication])
{
    ... LOG_WARNING(...);
    disk->removeSharedRecursive(relative_data_path, /*keep_all_shared_data*/ true, {});
}
else
{
    LOG_INFO(log, "dropAllData: removing table directory recursive to cleanup garbage");
    disk->removeRecursive(relative_data_path);
}
```

This already solves Leg 1's intent (the drop no longer gets stuck) **and** is
strictly safer than the old patch: the leftover-files branch removes local
metadata via `removeSharedRecursive(..., keep_all_shared_data = true, {})`,
which preserves shared S3 objects that other replicas may still reference.

**Safety-regression note.** The old patch simply *deleted* the throwing block,
which on 26.3 would fall through to the unconditional
`disk->removeRecursive(relative_data_path)` — physically removing data that may
be shared with other replicas. Porting Leg 1 would therefore **delete the
shared-data-preserving branch and regress safety**. We make **no change** to
`MergeTreeData.cpp`.

### Leg 2 — `still-needed`. PORTED into `ReplicatedMergeTreeSink.cpp`.

**Reachability on 26.3 (race confirmed reachable).** The commit path is
unchanged on 26.3 and is non-idempotent:

- The sink assembles the shared-lock ops via
  `storage.getLockSharedDataOps(*part, zookeeper, /*replace_zero_copy_lock*/ false, {}, ops)`
  (`ReplicatedMergeTreeSink.cpp:940`).
- `getLockSharedDataOps` → `getZeroCopyLockNodeCreateOps`
  (`StorageReplicatedMergeTree.cpp:10309`) with `mode == Persistent`,
  `replace_existing_lock == false` calls
  `zookeeper->checkExistsAndGetCreateAncestorsOps(zookeeper_node, requests)`
  (`StorageReplicatedMergeTree.cpp:11225`).
- `ZooKeeper::checkExistsAndGetCreateAncestorsOps`
  (`ZooKeeper.cpp:566-589`) does a read-side `exists()` over the ancestor
  chain and, for each ancestor absent **at check time**, appends a
  *non-idempotent* `makeCreateRequest(..., Persistent)`. The leaf
  (`.../<part_name>/<id>/<replica_name>`) differs per shard (distinct blob
  `id`, distinct `replica_name`), but the **part-name ancestor**
  `.../zero_copy_<disk>/<uuid>/<part_name>` is shared across shards (same
  `table_shared_id`).
- Under concurrent same-name commit, two shards both observe the ancestor
  absent → both append a hard `CREATE` → the second multi gets `ZNODEEXISTS`.
- The sink classifies any op failure inside the shared-lock op range
  (`shared_lock_ops_id_begin <= failed_op_idx < shared_lock_op_id_end`) as a
  hard `LOGICAL_ERROR` with **no retry**
  (`ReplicatedMergeTreeSink.cpp:1099-1103`):
  `"Creating shared lock for part {} has failed with error: {}. It's a bug. No race is possible since it is a new part."`

No upstream fix for this race was found on the 26.3 base.

**`getZeroCopyPartPath` static-signature note.** The helper is `static` on
`StorageReplicatedMergeTree`:

```cpp
static Strings getZeroCopyPartPath(const MergeTreeSettings & settings, const std::string & disk_type,
    const String & table_uuid, const String & part_name, const String & zookeeper_path_old,
    const ContextPtr & local_context);   // StorageReplicatedMergeTree.h:977-978
```

It returns the part-level lock paths
`<remote_fs_zero_copy_zookeeper_path>/zero_copy_<disk>/<table_uuid>/<part_name>`
(default root `/clickhouse/zero_copy`). Pre-creating those is exactly the
ancestor that races.

## 3. C++ / security review

The ported hunk, inserted between `part->setName(...)` and
`retry_context.actual_part_name = part->name;`
(`ReplicatedMergeTreeSink.cpp:909-922`):

```cpp
const auto storage_settings = storage.getSettings();
if ((*storage_settings)[MergeTreeSetting::allow_remote_fs_zero_copy_replication]
    && part->getDataPartStorage().supportZeroCopyReplication())
{
    const auto zero_copy_lock_part_paths = StorageReplicatedMergeTree::getZeroCopyPartPath(
        *storage_settings, part->getDataPartStorage().getDiskType(), storage.getTableSharedID(),
        part->name, storage.zookeeper_path, storage.getContext());
    for (const auto & path : zero_copy_lock_part_paths)
    {
        zookeeper->createAncestors(path);
        zookeeper->createIfNotExists(path, "");
    }
}
```

plus `extern const MergeTreeSettingsBool allow_remote_fs_zero_copy_replication;`
added to the file-top `namespace MergeTreeSetting`.

- **In-scope variables verified** at the insertion point (inside
  `ReplicatedMergeTreeSinkImpl<async_insert>::commitPart`’s
  `commit_new_part_stage` lambda):
  - `storage` — `StorageReplicatedMergeTree &` (sink member; confirmed via
    `storage.zookeeper_path`, `storage.getSettings()`, `storage.allocateBlockNumber(...)`).
  - `zookeeper` — `const ZooKeeperWithFaultInjectionPtr &` (the `commitPart`
    parameter at `ReplicatedMergeTreeSink.cpp:697`). `createAncestors(path)`
    and `createIfNotExists(path, data)` exist on `ZooKeeperWithFaultInjection`
    (`ZooKeeperWithFaultInjection.h:196,200`).
  - `part` — the `MutableDataPartPtr`; `getDataPartStorage().getDiskType()` and
    `.supportZeroCopyReplication()` exist on `IDataPartStorage`.
- **Deviation from the source snippet (documented):** the source called
  `storage.getZeroCopyPartPath(...)` (static-through-instance). The repo
  enables `readability-*` clang-tidy checks, which flag
  `readability-static-accessed-through-instance`. The call is qualified as
  `StorageReplicatedMergeTree::getZeroCopyPartPath(...)` — functionally
  identical (no instance state read), keeps CI clang-tidy clean. Allman braces
  applied (opening brace on its own line) per repo style.
- **Idempotence / correctness.** `createAncestors` + `createIfNotExists` are
  idempotent: concurrent pre-creation across shards converges with no error.
  Forcing the shared ancestor to exist *before* `getLockSharedDataOps` runs its
  `exists()` check guarantees the racy hard `CREATE` is not appended to the
  commit multi (the `exists()` observes the node present). The per-replica leaf
  is still hard-created in the multi, but it is unique per shard so it never
  collides.
- **Reach / blast radius.** The block runs only when
  `allow_remote_fs_zero_copy_replication` is enabled for the table **and** the
  part's storage supports zero-copy. For non-zero-copy tables it is a no-op.
  Adds two ZK round-trips (`createAncestors`, `createIfNotExists`) per part
  commit on zero-copy tables — negligible vs. the commit multi itself, and the
  `createIfNotExists` is a no-op once the ancestor exists.
- **No new attack surface.** Pure ZK-path pre-creation under operator-enabled
  zero-copy; no untrusted input, no relaxed check.

## 4. Test design

Integration test `tests/integration/test_aiven_zero_copy_lock_race/`
(MinIO/S3 zero-copy disk), because the race is cluster-level (two shards
sharing a `table_shared_id`) and unreachable from a stateless `.sql` runner.

**Setup.** Two nodes in different shards (`s1`, `s2`) of one `Replicated`
database `zc_race`. The table `zc_race.tbl` is created once and propagated to
both shards by the Replicated DB, so it has the **same UUID** on both → same
zero-copy `table_shared_id`. `storage_policy = 's3'`,
`allow_remote_fs_zero_copy_replication = 1`, no `PARTITION BY` (single `all`
partition → first commit on each shard is part `all_0_0_0`).

**Deterministic interleaving without a production failpoint.** The TOCTOU
window between the commit multi's `exists()` check and `tryMultiNoThrow` is
sub-millisecond. We widen it with the **existing** test failpoint
`rmt_delay_commit_part` (registered at `Common/FailPoint.cpp:156`), which
sleeps 5s right before the commit multi (`ReplicatedMergeTreeSink.cpp:973`),
*after* the ops — including the `exists()` check — are built. Enabling it on
both shards (`SYSTEM ENABLE FAILPOINT rmt_delay_commit_part`) and launching the
two inserts concurrently makes both shards pass their `exists()` check before
either multi runs, so the collision is reproduced reliably. This is a
pre-existing failpoint already used by
`tests/integration/test_race_condition_for_replicated_merge_tree/test.py` for a
sibling race — **no production failpoint was added**.

`test_concurrent_same_name_part_commit_across_shards`: enable the failpoint on
both shards, run concurrent `INSERT`s, assert both succeed and that the shared
part-level znode `/clickhouse/zero_copy/zero_copy_s3/<uuid>/all_0_0_0`
materialized.

### Evidence (worktree-flip pair, 2026-06-04)

- **post-patch:**
  `test_concurrent_same_name_part_commit_across_shards PASSED` —
  `1 passed, 3 warnings in 22.59s`.
- **pre-patch** (worktree reverted to HEAD via
  `git restore --source=HEAD --worktree`, index kept the staged change, binary
  rebuilt and re-run; the binary is volume-mounted into the test nodes so no
  image rebuild was needed): `1 failed` —
  `AssertionError: concurrent same-name part commit across shards failed:
  node2: ... Code: 49 ... (LOGICAL_ERROR)`. The node2 server log shows the
  decisive exception verbatim:
  `Code: 49. DB::Exception: Creating shared lock for part all_0_0_0 has failed
  with error: Node exists. It's a bug. No race is possible since it is a new
  part. (LOGICAL_ERROR)`. Worktree restored from index and rebuilt to the
  post-patch binary afterward (worktree-vs-index diff = 0).

This is a genuine deterministic evidence-of-causation pair (not a flaky loop).

## 5. Rollback considerations

Leg 2 is a self-contained ~16-line additive block plus one `extern`
declaration in `ReplicatedMergeTreeSink.cpp`; reverting it restores the prior
(racy) behavior with no schema/submodule coupling. Leg 1 requires no rollback
(no change was made). The test directory is independently removable.

## 6. Per-uplift notes

### 25.8-aiven (historical)

Source `c6d9323ba2` (author Tilman Moeller, co-authored by Joe Lynch,
2026-01-07): both legs in one commit — the `MergeTreeData::dropAllData`
validation removal and the `ReplicatedMergeTreeSink.cpp` lock pre-creation.

### 26.3-aiven (this uplift)

- **Leg 1: dropped** — `obsoleted-by-upstream` (`25b0406c35c`); porting it would
  regress the shared-data-preserving drop branch. `MergeTreeData.cpp` untouched.
- **Leg 2: ported** — `still-needed`; applied as a direct edit (not a
  cherry-pick) at the relocated insertion point, with the static call qualified
  and Allman braces. Built clean (incremental, 1 TU + relink, EXIT=0).
- Verified with a MinIO integration test + a **deterministic** worktree-flip
  evidence pair using the existing `rmt_delay_commit_part` failpoint (§4).

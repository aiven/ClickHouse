# Patch 008 — replication-queue-size-limit

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | n/a | n/a | patch did not exist (introduced in 25.8) |
| 25.8-aiven | `d6e78ab9938674342224e2e58408fadccaae9ee7` | Tilman Moeller / committer Aliaksei Khatskevich | original carry |
| 26.3-aiven | (staged) | T3 worker (patch-008 dispatch) | conflict-resolved + drift-fixed + **augmented** (new default-off server guard) |

The current uplift's row stays "(staged)" until the human commits.

## 1. Purpose

Bound the growth of per-replica `ReplicatedMergeTree` replication queues so a slow
replica cannot grow its ZooKeeper/Keeper `queue/` without limit and OOM-crash
Keeper (from which recovery is almost impossible — Keeper often corrupts its state
when OOMing, and hand-pruning hundreds of thousands of queue items to a consistent
state is impractical). The patch reuses the existing "too many parts" insert
delay/throw infrastructure: a per-table background thread publishes the maximum
replica queue size (and a fleet-wide total), and `delayInsertOrThrowIfNeeded`
delays or throws inserts once those cross configurable thresholds. The "why" is
durable: protecting Keeper from unbounded replicated-queue growth is a standing
Aiven operational concern.

Source SHA on `v25.8.18.1-lts-aiven`: `d6e78ab9938674342224e2e58408fadccaae9ee7` (inventory row 008).
Original author: `tilman.moeller@aiven.io` (committer `alex.khatskevich@aiven.io`; co-author `kevin.michel@aiven.io`).
Original purpose (quoted from the commit body):

> If a replica is struggling to execute all tasks in its replication queue, the
> queue can grow to a huge size and becomes even slower to manage when it's larger
> (it's an std::list). This queue is also present in ZooKeeper, for each replica.
> [...] At some point, if the imbalance persists, ZooKeeper will crash because it
> won't have enough memory to hold all the queues. [...] To fix that, we reuse the
> infrastructure that delays insert queries when there are too many parts. The same
> logic is applied when any replica queue size is too large for the table, or the
> grand total of all replicas' queue sizes, over all tables, is too large. We also
> do the same to fail queries if either counter reaches a second threshold [...].
> In each replica, a thread is updating a local copy of the maximum queue size in
> all replicas. A map is added to the context to keep track of these per-storage
> maxima. [...] We do it like that to avoid adding many ZooKeeper queries in the
> hot path [...].

## 2. Upstream-drift findings

### Commands run

```bash
# identifier recount on HEAD (v26.3.10.62-lts-aiven-dev)
rg -c 'queue_size_monitor|queue_size_to_(delay|throw)_insert|queues_total_size_to_(delay|throw)_insert'
rg -c 'ReplicatedMergeTreeQueueSizeThread|ReplicatedQueuesTotalSize|setStorageReplicatedQueuesSize'
# signature checks
rg -n 'createTask' src/Core/BackgroundSchedulePool.h
rg -n 'delayInsertOrThrowIfNeeded' src/Storages/MergeTree/MergeTreeData.h
rg -n 'namespace ServerSetting' src/Storages/MergeTree/MergeTreeData.cpp src/Storages/StorageReplicatedMergeTree.cpp
rg -n 'setCurrentComponent|enforce_component_tracking' src/Common/ZooKeeper/ZooKeeperImpl.cpp
```

### Findings

- Upstream changes to touched files between 25.8 and 26.3:
  - `MergeTreeData.cpp`/`.h`: `delayInsertOrThrowIfNeeded` decl unchanged in the pre-patch 3-arg shape, BUT the delay-computation block was refactored (the integer/float division now needs explicit `static_cast<double>` to satisfy 26.3's `-Werror=implicit-int-float-conversion`); the file already has a `namespace ServerSetting` block. Conflict-resolved.
  - `StorageReplicatedMergeTree.cpp`: constructor initializer list and `startupImpl` drifted (neighboring members); NO `namespace ServerSetting` block (added by PC-2). Conflict-resolved.
  - `ReplicatedMergeTreeSink.cpp`: `onStart` gained component-guard + keeper-creation + quorum-replica validation upstream; the patch's `delayInsertOrThrowIfNeeded` call-site change was layered on top. Conflict-resolved.
  - `docs/en/operations/system-tables/metrics.md`: upstream reformatted the whole file from heading-entries to a Markdown table; the `ReplicatedQueuesTotalSize` entry was reinserted as a table row. Conflict-resolved.
  - `BackgroundSchedulePool.h`: `createTask` lost its 2-arg form; only the 3-arg `(StorageID, log_name, func)` remains.
  - `Common/ZooKeeper/ZooKeeperImpl.cpp`: 26.3 added `enforce_component_tracking` — every scope issuing ZK requests must call `Coordination::setCurrentComponent` first, else `LOGICAL_ERROR` "Current component is empty".
- Upstream changes that touched the patch's behavior (symbols, error codes, callers):
  - `queue_size_monitor`, the four `queue*_size_to_*_insert` thresholds, `ReplicatedMergeTreeQueueSizeThread`, `ReplicatedQueuesTotalSize`, `setStorageReplicatedQueuesSize`: all ABSENT on HEAD (count 0). No upstream-equivalent feature was merged. The feature is still needed.
  - `LIMIT_EXCEEDED` (290): unchanged real error code (`src/Common/ErrorCodes.cpp`).
- Conclusion: **`still-needed-but-rewrite`** — semantics unchanged; the cherry-pick needs manual conflict resolution + three 26.3 drift fixes (`createTask` 3-arg, `LoggerPtr`, `setCurrentComponent`). Augmented per the parent policy call with a default-off server guard (clause-(v) blast-radius resolution).

## 3. C++ review

Per `docs/aiven/skills/cpp-review-checklist.md`:

- 1 Lifetime + ownership: ✓ — the monitor task captures `this` (the `ReplicatedMergeTreeQueueSizeThread`, owned by `StorageReplicatedMergeTree`); the `BackgroundSchedulePool` task is stopped in shutdown before the storage is destroyed, so the captured `storage` reference never dangles.
- 2 Exception safety: ✓ — `run` wraps `iterate` in try/catch and `tryLogCurrentException`, then reschedules; a transient ZK failure cannot kill the thread or leak. The insert-gate throws cleanly before any part is committed (it is in `onStart`/pre-commit).
- 3 Thread-safety + concurrency: ✓ — the per-storage maximum is published via an atomic (`max_replicas_queue_size.store`) and the fleet map via the already-thread-safe `Context` accessors (`setStorageReplicatedQueuesSize`); no new lock taken under a `Context` mutex; no sleep-based synchronization in C++ (the 1000 ms reschedule is a poll interval, not a race fix).
- 4 Performance + memory: ✓ — the ZK polling is off the insert hot path (a 1 s background poll); the insert gate reads the cached atomic/map, adding no ZK call per insert. With the guard OFF the monitor thread never starts and `delayInsertOrThrowIfNeeded` neutralizes the queue inputs to 0 (no extra work).
- 5 Settings as public API: ✓ — new `ServerSetting` `aiven_enable_replication_queue_size_limit` declared in `src/Core/ServerSettings.cpp` (Bool, default **false** — safe new gate); the four carried thresholds remain in `Settings.cpp`/`MergeTreeSettings.cpp`. `aiven_` prefix per AGENTS §8.
- 6 Error handling: ✓ — throws the real `LIMIT_EXCEEDED` (290) with the Aiven-distinctive message `Too large replication queue (N). One of the replicas is too slow for inserts` (and `Too many item in replication queues ...` for the fleet total), distinguishable from upstream by the message substring (tested).
- 7 Upstream / vendored code: ✓ — no `contrib/**`, `.claude/**`, `.github/workflows/**`, or root `AGENTS.md` touched; refactor drift in `MergeTreeData.cpp`/`ReplicatedMergeTreeSink.cpp`/`metrics.md` reconciled (see §2).
- 8 Behavior under settings: ✓ — guard OFF ⇒ nothing observable: the monitor thread is not started (`startupImpl` gate), `ReplicatedQueuesTotalSize` stays 0, and the queue inputs are forced to 0 in `delayInsertOrThrowIfNeeded` so no extra comparison/throw path runs (also closes the `0 >= 0` footgun if a threshold ever resolved to 0). Verified by the guard-OFF control node in the integration test.

## 4. Test design

(a) **New integration test that exercises the gated path.**

- Test path: `tests/integration/test_aiven_replication_queue_size_limit/` (`test.py` + `configs/enable_queue_limit.xml`), per the `test_aiven_<slug>` convention.
- Topology: two replicas (`node_on` with `aiven_enable_replication_queue_size_limit=true`; `node_off` default-false) of one `ReplicatedMergeTree` over Keeper, identical tiny per-table thresholds (`queue_size_to_throw_insert = 5`). `SYSTEM STOP FETCHES` stalls `node_on`'s queue; 12 inserts via `node_off` inflate it; a bounded polled wait on `system.replicas.queue_size` and `system.metrics.ReplicatedQueuesTotalSize` synchronizes on the monitor poll (no bare sleep as sole sync).
- Evidence-of-causation pair (the two-node config differential IS the pre/post pair per PC-6):

  Guard-ON (`node_on`, "post"):

  ```text
  node_on  queue_size=14 ReplicatedQueuesTotalSize=14
  node_on INSERT error: Received exception from server (version 26.3.10):
  Code: 290. DB::Exception: Received from 172.18.0.6:9000. DB::Exception:
  Too large replication queue (14). One of the replicas is too slow for inserts:
  While executing WaitForAsyncInsert. [...] . (LIMIT_EXCEEDED)
  ```

  Guard-OFF (`node_off`, "pre"/control):

  ```text
  node_off ReplicatedQueuesTotalSize=0
  # control INSERT succeeded (assert inserted == 1)
  ======================== 1 passed, 3 warnings in 16.41s ========================
  ```

- Why this distinguishes the Aiven gate from upstream behavior (AGENTS §7): the assertion checks BOTH the error code (`LIMIT_EXCEEDED`) AND the Aiven-specific substring (`Too large replication queue`), and the guard-OFF control proves the SERVER guard — not just the thresholds — is what gates the feature (`ReplicatedQueuesTotalSize` stays 0 on the off node because the monitor never starts).

## 5. Rollback considerations

- Revert safety: safe. No schema migration and no on-disk format change. The feature only delays/throws inserts and runs a polling thread; reverting removes both.
- Surviving state: the only persistent state read is the existing ZooKeeper `queue/` childcounts (not created by this patch). The per-storage maxima live in a `Context` in-memory map and an atomic — both die with the process; no new ZK nodes or on-disk files.
- Disable without rebuild: set the server setting `aiven_enable_replication_queue_size_limit` to false (its default). This is a server-level setting (cannot be overridden per-session), so the gate is fully controlled by config. The inner `queue_size_monitor` query setting and the four thresholds remain as carried.

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — patch did not exist (introduced in 25.8).

### 25.8-aiven (historical, may be empty)

Original carry: `d6e78ab9938674342224e2e58408fadccaae9ee7`, author Tilman Moeller, co-author Kevin Michel, committer Aliaksei Khatskevich. Enabled by default (`queue_size_monitor` query setting defaulted true).

### 26.3-aiven (this uplift)

- Cherry-pick was: **conflict-resolved + rewritten + augmented.** Conflicts in `metrics.md` (table reformat), `MergeTreeData.cpp` (delay-block refactor + `static_cast<double>`), `ReplicatedMergeTreeSink.cpp` (`onStart` drift), `StorageReplicatedMergeTree.cpp` (ctor/startup drift) were resolved keeping upstream and re-layering the patch.
- Augmentation (parent policy call, ratified 2026-06-15 — see [`../proposals/2026-06-15-aiven-settings-naming-convention-and-queue-size-guard.md`](../proposals/2026-06-15-aiven-settings-naming-convention-and-queue-size-guard.md)): carry patch 008 verbatim and add ONE new default-off server master guard `aiven_enable_replication_queue_size_limit` (clause-(v) blast-radius resolution — a faithful as-is port would start a per-table background thread and add a fleet-wide insert-rejection path on by default). `queue_size_monitor` and the four thresholds are carried untouched (not renamed, not `MAKE_OBSOLETE`). Two gate points: (PC-2) `startupImpl` starts the monitor only when `guard && queue_size_monitor`; (PC-3) `delayInsertOrThrowIfNeeded` forces the queue-size inputs to 0 when the guard is off — preserving upstream behavior AND removing the `0 >= 0` footgun that would otherwise throw on every insert if a threshold ever resolved to 0. `aiven_enable_replication_queue_size_limit` is the first application of the AGENTS §8 `aiven_` naming convention for Aiven-introduced settings.
- Drift fixes (port, not redesign): (1) `createTask` 2-arg → 3-arg with `storage.getStorageID()` (the 2-arg form was removed upstream); (2) logger `Poco::Logger *` → `LoggerPtr` via `getLogger` (HEAD convention); (3) **a 26.3 `Coordination::setCurrentComponent("ReplicatedMergeTreeQueueSizeThread::run")` guard** in the monitor thread — 26.3 added `enforce_component_tracking`, so without the guard the monitor's ZK poll throws `LOGICAL_ERROR` "Current component is empty" every cycle. Drift fix (3) was discovered at integration-test time (the first test run failed: `ReplicatedQueuesTotalSize` stayed 0 because the monitor thread kept throwing); it is the same class as (1)/(2) — a 26.3 API requirement the 25.8 source could not anticipate. Cosmetic: `.h` decl indentation 5→4 spaces.
- Carried K&R brace: `StorageReplicatedMergeTree::updateMaxReplicasQueueSize() {` (source style) was left as-is per PC-4 (do not chase style in carried code); flag for the human if the CI style check rejects it.
- Build hazard: the new members in `StorageReplicatedMergeTree.h` shifted struct layout; the first incremental build linked but SIGSEGV'd at runtime in an unrelated stale TU (`ReplicatedMergeTreeRestartingThread.cpp.o`) reading `queue_updating_task` at the old offset — the `#deps 0` layout face (build-and-test.md §7). Fixed by force-recompiling the `StorageReplicatedMergeTree.h` include closure (~37 TUs), then a clean build + green test.
- Test added at: `tests/integration/test_aiven_replication_queue_size_limit/`.
- Time-to-port: multi-session (env hook outage mid-dispatch); build was **warm-cache** for the closure recompile (~3.2 min) and the post-fix incremental (~1.6 min); integration test ~17 s wall-clock.
- Anything surprising: the guard-off 0-footgun (PC-3) and the `setCurrentComponent` 26.3 requirement (only surfaced at runtime, not at compile time) were the two non-obvious correctness items.

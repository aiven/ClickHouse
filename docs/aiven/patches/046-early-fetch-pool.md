# Patch 046 — early-fetch-pool

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | n/a | n/a | patch did not exist (introduced in 25.8) |
| 25.8-aiven | `7e08e44e51324f1dde3df165ee59c9b9bc90ae88` | Tilman Moeller / committer Joe Lynch | original carry |
| 26.3-aiven | (staged) | Cursor agent (patch-046 dispatch) | conflict-resolved + drift-fixed + **renamed** (`aiven_` convention, MergeTreeSetting aliased) |

The current uplift's row stays "(staged)" until the human commits.

## 1. Purpose

When a fresh replica is added to an existing `ReplicatedMergeTree` cluster, it
enqueues one `GET_PART` task per pre-existing part to download the data that
existed before the replica was created. These initial-sync entries have an
**empty `source_replica`** (they are not driven by a specific source replica's
insert). They tend to be large and numerous, and on the single shared
`background_fetches_pool` they monopolize the fetch threads and starve the many
small insert-driven `GET_PART` tasks of ongoing replication — those then pile up
as ZooKeeper/Keeper znodes (the same failure family as patch 008, attacked from
the scheduling side rather than the insert side).

The patch splits the `GET_PART`/`ATTACH_PART` fetch path into two background
pools: a separate **early** pool for the empty-`source_replica` initial-sync
fetches, and the existing **normal** pool for ongoing insert-driven fetches. The
early pool is bounded by its own server setting so the big early parts cannot
crowd out normal replication.

Source SHA on `v25.8.18.1-lts-aiven`: `7e08e44e51324f1dde3df165ee59c9b9bc90ae88`
(inventory row 046). Original author `tilman.moeller@aiven.io` (committer
`joelynch112@gmail.com`; co-author `kevin.michel@aiven.io`). Author date
2026-01-06. 9 files, +86/-7.

## 2. Mandatory augmentation — `aiven_` rename (policy decision)

Both settings were introduced in the 25.8 fork (not net-new this uplift), but the
maintainer standardized them under the `aiven_` convention
(`docs/aiven/AGENTS.md` settings-naming rule):

| Source name | Ported (canonical) name | Tier | Type | Default | Legacy alias |
|---|---|---|---|---|---|
| `use_early_fetch_pool` | `aiven_use_early_fetch_pool` | `MergeTreeSetting` | `Bool` | `true` | **yes** — `use_early_fetch_pool` |
| `background_early_fetches_pool_size` | `aiven_background_early_fetches_pool_size` | `ServerSetting` | `UInt64` | `8` | **no** |

- **MergeTreeSetting — declared WITH a backward-compat alias.** In
  `src/Storages/MergeTree/MergeTreeSettings.cpp` (~1908) via
  `DECLARE_WITH_ALIAS(Bool, aiven_use_early_fetch_pool, true, R"(...)", 0, use_early_fetch_pool)`,
  mirroring the live precedent `enable_block_number_column` aliasing
  `allow_experimental_block_number_column` (~1830). Argument order
  (`Type, canonical, default, doc, flags, alias`) confirmed against the precedent
  before building. **Rationale:** a 25.8 table that persisted
  `SETTINGS use_early_fetch_pool = …` in its `.sql` metadata would otherwise hit
  `UNKNOWN_SETTING` on a 26.3 ATTACH and fail to load; the alias prevents that and
  carries the value forward, while `system.merge_tree_settings` still reports the
  canonical `aiven_` name.
- **ServerSetting — plain rename, no alias.** Server settings are read by known
  key from config, so there is no clean alias form and no bespoke loader code was
  added. A stale `<background_early_fetches_pool_size>` is silently ignored and the
  pool reverts to the default `8` (see §6).
- Every reference renamed: the `DECLARE_WITH_ALIAS` / `DECLARE`, the
  `extern const …` declarations (`MergeTreeData.cpp`,
  `StorageReplicatedMergeTree.cpp`, `Context.cpp`), the `loadSettingsFromConfig`
  allow-list and `dumpToSystemServerSettingsColumns` entries in
  `ServerSettings.cpp`, and all `[ServerSetting::…]` / `[MergeTreeSetting::…]` read
  sites. The alias is declared **only** at the `MergeTreeSettings.cpp` site; all
  code reads the canonical name.
- **Internal identifiers left unprefixed** (they are not user-facing settings):
  `early_fetch_executor`, `getEarlyFetchesExecutor`, `scheduleEarlyFetchTask`, the
  `CurrentMetrics` `BackgroundEarlyFetchesPoolTask` / `…PoolSize`, and the
  `"EarlyFetch"` executor thread label.

### PC-GATE — default kept `true` (test-neutral)

The intent is `aiven_use_early_fetch_pool` defaults `true`, conditioned on not
changing upstream test outcomes. The split changes only **which background pool**
runs a fetch — fetches still succeed and produce identical data — so
query/result-level tests are unaffected. The only golden-list impact is the new
setting **name** appearing in the merge-tree-settings history check; per the
runbook that is an expected list update, not a reason to disable. Resolved by
registering the setting in `src/Core/SettingsChangesHistory.cpp`
(`getMergeTreeSettingsChangesHistory`, 26.3 block):

```cpp
{"aiven_use_early_fetch_pool", false, true, "New Aiven setting: route initial-sync (empty source_replica) fetches to a separate early-fetch background pool. Renamed from use_early_fetch_pool (kept as a backward-compatible alias)."},
```

**Outcome: default kept `true`.**

## 3. Upstream-drift findings

Everything except two `StorageReplicatedMergeTree.cpp` hunks mirrors a live
sibling and applied with at most line-offset:

- **`Context.cpp` executor block** — `early_fetch_executor` clones the live
  `fetch_executor` `OrdinaryBackgroundExecutor` construction in
  `initializeBackgroundExecutorsIfNeeded`, plus the matching `SHUTDOWN(...)` line
  and the `getEarlyFetchesExecutor()` accessor. **Drift fix:** the 26.3
  `OrdinaryBackgroundExecutor` constructor takes a **`ThreadName` enum**, not a
  string literal as the 25.8 source did. Added a `MERGETREE_EARLY_FETCH` value
  (mapping to `"EarlyFetch"`) to `THREAD_NAME_VALUES` in
  `src/Common/setThreadName.h` and pass `ThreadName::MERGETREE_EARLY_FETCH`.
- **`Context.cpp` shutdown block** — cherry-pick conflicted at the background
  executors `SHUTDOWN(...)` block (HEAD reorganized it relative to 25.8); resolved
  by inserting the `early_fetch_executor` shutdown into HEAD's live block and
  dropping the duplicated source block.
- **`ServerSettings.cpp`** — `DECLARE` next to `background_fetches_pool_size`, plus
  the `loadSettingsFromConfig` allow-list and `dumpToSystemServerSettingsColumns`
  entries.
- **`BackgroundJobsAssignee.cpp/.h`** — `scheduleEarlyFetchTask` mirrors
  `scheduleFetchTask`.
- **`CurrentMetrics.cpp`**, **`MergeTreeSettings.cpp`**, **`MergeTreeData.cpp`**
  extern — landed adjacent to 008's new entries; trivial.

**The two real hunks (re-anchored against HEAD, not the source offsets):**

1. `scheduleDataProcessingJob` (HEAD ~4162): the `GET_PART || ATTACH_PART` branch
   hoists the `ExecutableLambdaAdapter` into a `fetch_task` local, then routes:

   ```cpp
   if (selected_entry->log_entry->source_replica.empty()
       && (*getSettings())[MergeTreeSetting::aiven_use_early_fetch_pool])
       assignee.scheduleEarlyFetchTask(fetch_task);
   else
       assignee.scheduleFetchTask(fetch_task);
   ```

   Uses `.empty()` (HEAD idiom), **not** the source's `== ""`. Keeps the 3-arg
   `ExecutableLambdaAdapter` form already correct on HEAD.

2. `canExecuteFetch` (HEAD ~4234): re-anchored to wrap **only** the
   pool-saturation check in the early/normal branch. The early branch uses
   `getEarlyFetchesExecutor()->getMaxTasksCount()` + `BackgroundEarlyFetchesPoolTask`
   and emits the distinctive `disable_reason`
   `"... because N early fetches already executing, max M."`; the `else` scopes
   `replicated_fetches_pool_size` inside itself (not referenced afterward on HEAD).
   HEAD's **throttler** block and the `entry.source_replica.empty()` **broken-part**
   block are left untouched and after the saturation branch.

## 4. C++ review

Per `docs/aiven/skills/cpp-review-checklist.md`:

- 1 Lifetime + ownership: ✓ — `early_fetch_executor` is owned by `ContextSharedPart`
  (a `shared_ptr`), constructed in `initializeBackgroundExecutorsIfNeeded` and torn
  down via the same `SHUTDOWN(...)` machinery as the other executors; the scheduled
  `fetch_task` lambda captures `this`/`selected_entry` exactly as the existing
  fetch path does.
- 2 Exception safety: ✓ — no new throwing path; routing is a pure branch over a
  bool setting and an empty-string check before `trySchedule`.
- 3 Thread-safety + concurrency: ✓ — the executor accessor takes the existing
  `SharedLockGuard(background_executors_mutex)`; pool occupancy is read via the
  existing `CurrentMetrics::values[...]` atomic with `relaxed` order, matching the
  normal-pool check. No new lock, no sleep-based synchronization.
- 4 Performance + memory: ✓ — one extra `OrdinaryBackgroundExecutor` (default 8
  threads); the routing decision is O(1) per scheduled fetch. With the setting off,
  behavior is byte-identical to upstream (all fetches go to the normal pool).
- 5 Settings as public API: ✓ — `aiven_use_early_fetch_pool` (MergeTreeSetting,
  aliased) and `aiven_background_early_fetches_pool_size` (ServerSetting) per the
  `aiven_` convention; registered in `SettingsChangesHistory.cpp`.
- 6 Error handling: ✓ — the early-pool saturation produces a distinctive
  `disable_reason` substring (`early fetches already executing`) distinguishable
  from the normal-pool message; this is the test's observable.
- 7 Upstream / vendored code: ✓ — no `contrib/**`, `.claude/**`,
  `.github/workflows/**`, or root `AGENTS.md` touched.
- 8 Behavior under settings: ✓ — `aiven_use_early_fetch_pool = 0` ⇒ initial-sync
  fetches fall back to the normal pool, so the early pool is never the gate (proved
  by the integration test's control table). The early pool is still constructed but
  idle.

## 5. Test design (PC-TEST) — evidence of causation

`tests/integration/test_aiven_early_fetch_pool/` (`test.py` +
`configs/early_fetch_pool.xml`), per the `test_aiven_<slug>` convention.

- **Topology.** Two nodes over Keeper. `node_source` holds the pre-existing data;
  `node_replica` (with `aiven_background_early_fetches_pool_size = 1`) attaches
  **after** the data exists, so its `GET_PART` entries have empty `source_replica`
  (= early fetches). Two tables differing **only** in the routing setting:
  - `t_early`  `SETTINGS aiven_use_early_fetch_pool = 1` → initial-sync fetches use
    the **early** pool (sized 1 on the syncing node).
  - `t_normal` `SETTINGS aiven_use_early_fetch_pool = 0` → the **same** fetches fall
    back to the **normal** pool (default 16).
- **Differential / evidence-of-causation (timing-free).** Both tables are filled
  with `N_PARTS = 40` small parts (merges stopped) before the fresh replica is
  attached. The single early-pool slot saturates immediately, so surplus `t_early`
  entries are postponed with the Aiven-specific reason — observed via
  `system.replication_queue.postpone_reason` by a bounded poll. The two pool
  messages are textually distinct, so the assertion pair is:
  - **post** (`t_early`, routing ON): some queued entry shows
    `postpone_reason` containing `early fetches already executing`.
  - **pre/control** (`t_normal`, routing OFF): no queued entry ever shows that
    reason (the same initial-sync fetches went to the normal pool). A faithful
    no-op port that ignored the setting could not produce this difference.
  - **corroboration**: `system.metrics.BackgroundEarlyFetchesPoolSize == 1` on the
    syncing node (the new metric reflects the new server setting).
  - **sanity**: both tables converge to the full row count (the early pool bounds
    concurrency without dropping data).
- **Result:** `1 passed in 22.37s` (job `Integration tests (amd_binary, 1/5)`).

## 6. Backward-compat / migration note

- **MergeTreeSetting.** The `DECLARE_WITH_ALIAS` legacy alias
  (`use_early_fetch_pool`) fully covers persisted table metadata: old `.sql`
  SETTINGS still parse, the value is carried forward, and **no table fails to
  ATTACH**. Verified locally: `CREATE TABLE … SETTINGS use_early_fetch_pool = 0`
  succeeds, and `system.merge_tree_settings` reports the canonical
  `aiven_use_early_fetch_pool`.
- **ServerSetting.** No alias. A 25.8 server config still referencing
  `<background_early_fetches_pool_size>` is **silently ignored** on 26.3 (server
  settings are read by known key; unknown keys do not throw), so the early-fetch
  pool reverts to the default `8` until the operator renames the key to
  `aiven_background_early_fetches_pool_size`. This is operator-owned config
  migration. Recorded as `REPL-3` in
  [`../uplifts/26.3/major-upstream-changes.md`](../uplifts/26.3/major-upstream-changes.md).

## 7. Rollback considerations

- Revert safety: safe. No schema migration, no on-disk format change. Routing only
  selects which in-process background pool runs a fetch; reverting removes the
  second pool and the routing branch.
- Surviving state: none new. The early pool is in-memory and dies with the process;
  no new ZK nodes or on-disk files. Tables that persisted `use_early_fetch_pool`
  (or `aiven_use_early_fetch_pool`) in `.sql` keep loading because the alias
  remains a live setting.
- Disable without rebuild: set `aiven_use_early_fetch_pool = 0` per table (or rely
  on the alias) — initial-sync fetches then use the normal pool, identical to
  upstream.

## 8. Per-uplift notes

### 25.3-aiven (historical)

n/a — patch did not exist (introduced in 25.8).

### 25.8-aiven (historical)

Original carry: `7e08e44e51324f1dde3df165ee59c9b9bc90ae88`, author Tilman Moeller,
co-author Kevin Michel, committer Joe Lynch. Settings named `use_early_fetch_pool`
(MergeTreeSetting, default true) and `background_early_fetches_pool_size`
(ServerSetting, default 8).

### 26.3-aiven (this uplift)

- Cherry-pick was: **conflict-resolved + drift-fixed + renamed.** The
  `Context.cpp` shutdown block conflicted (HEAD reorg); resolved keeping HEAD.
- Drift fixes (port, not redesign): (1) `OrdinaryBackgroundExecutor` now takes a
  `ThreadName` enum — added `MERGETREE_EARLY_FETCH` → `"EarlyFetch"` to
  `setThreadName.h` and updated the ctor call; (2) `.empty()` instead of `== ""`
  in both hunks (HEAD idiom); (3) `canExecuteFetch` re-anchored to wrap only the
  saturation check, leaving HEAD's throttler + broken-part blocks intact.
- `aiven_` rename + MergeTreeSetting alias + ServerSetting plain rename (§2).
- PC-GATE: default kept `true`; registered in `SettingsChangesHistory.cpp` for the
  02995 new-settings golden list.
- Test added at: `tests/integration/test_aiven_early_fetch_pool/`.
- **Anything surprising (test infra, not code):** the first two integration runs
  failed at `node_replica` startup (port 9000 never opened, zero server logs)
  while `node_source` started fine. Root cause was an **invalid XML comment** in
  `configs/early_fetch_pool.xml` — the comment contained a literal `--` (double
  hyphen), which is illegal inside XML comments, so Poco's SAX parser rejected the
  config-merge and the server died before logging. It passed `clickhouse local`
  earlier only because that validation used a different file without the prose
  comment. Fixed by removing the `--`; confirmed with `xmllint` and a foreground
  run of the integration image. The C++ change was correct throughout (a control
  run of `test_aiven_replication_queue_size_limit` passed in 16 s, and a local
  server with `aiven_background_early_fetches_pool_size` started cleanly with
  `BackgroundEarlyFetchesPoolSize` reflecting the config).

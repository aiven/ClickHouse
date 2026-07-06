# Patch 050 — refreshable-mv-zookeeper

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | n/a | n/a | feature did not exist on this line |
| 25.8-aiven | `63f05c4e12fc006bcffd87011c23aa62dd8317c0` | Tilman Moeller / Joe Lynch | original carry |
| 26.3-aiven | (staged) | T3.21 worker | byte-equivalent (clean cherry-pick) |

The current uplift's row stays "(staged)" until the human commits.

## 1. Purpose

ClickHouse's refreshable materialized views, when run in a coordinated setup
(a `Replicated`/`Shared` database), previously required the Keeper `MULTI_READ`
feature flag. `MULTI_READ` is implemented by ClickHouse Keeper but **not** by
standard Apache ZooKeeper, so customers running plain ZooKeeper clusters could
not create refreshable materialized views at all — `CREATE MATERIALIZED VIEW …
REFRESH …` threw `NOT_IMPLEMENTED` "Keeper server doesn't support multi-reads.".
This patch removes that requirement so refreshable materialized views work with
either coordination backend, which Aiven needs because not all managed clusters
use ClickHouse Keeper.

Source SHA on `v25.8.18.1-lts-aiven`: `63f05c4e12fc006bcffd87011c23aa62dd8317c0`
(from `docs/aiven/uplifts/26.3/inventory.md` row 050).
Original author: `tilman.moeller@aiven.io` (committer `joelynch112@gmail.com`).
Original purpose (quoted, not paraphrased):

> Allow refreshable materialized views when using ZooKeeper (rather than ClickHouse Keeper)
>
> Refreshable materialized views previously required the MULTI_READ feature which
> is only available in ClickHouse Keeper, not in standard Apache ZooKeeper. This
> prevented users with ZooKeeper clusters from using refreshable materialized views.
> […]
> This patch removes the MULTI_READ requirement by:
> - Replacing atomic `multi(ops)` with async `asyncTryCreateNoThrow()` calls
> - Removing MULTI_READ checks in constructor and readZnodesIfNeeded
> - Using the same async pattern already used elsewhere in ClickHouse
>   (e.g., StorageReplicatedMergeTree)

## 2. Upstream-drift findings

### Commands run

```bash
git log --oneline v25.8.18.1-lts..v26.3.10.62-lts -- src/Storages/MaterializedView/RefreshTask.cpp
grep -n "MULTI_READ\|multi(ops)\|asyncTryCreateNoThrow" src/Storages/MaterializedView/RefreshTask.cpp   # on HEAD, pre-patch
```

### Findings

- Upstream changes to touched files between prior and current LTS:
  - `src/Storages/MaterializedView/RefreshTask.cpp`: heavily churned upstream
    (~25 commits: flaky-test fixes, use-after-free fixes, AST refactors,
    background-schedule-pool introspection, etc.), but all changes are around
    the patched regions, not in them — pure line-number drift. The three
    patched regions exist byte-identically on HEAD (constructor signature
    `… bool attach, …` at `:82`; `MULTI_READ` guard + `multi(ops)` block at
    `:117-142`; `readZnodesIfNeeded` guard at `:964-965`).
- Upstream changes that touched the patch's behavior (symbols, error codes, callers):
  - `KeeperFeatureFlag::MULTI_READ`, `multi(ops)`, `isFeatureEnabled`: no
    changes — upstream 26.3 still gates coordinated refreshable MVs on
    `MULTI_READ` (the guards at `:118`, `:964` and `multi(ops)` at `:142`
    are all present pre-patch). No equivalent or superseding fix was merged.
  - Post-image APIs all present at HEAD with the exact signatures the patch
    uses: `ZooKeeper::asyncTryCreateNoThrow(path, data, mode)`, the
    `FutureCreate` alias, `Coordination::Error::{ZOK,ZNODEEXISTS}`,
    `Coordination::Exception`, `Coordination::errorMessage`.
- Conclusion: **`still-needed-applies-cleanly`** — upstream did not adopt an
  equivalent; the cherry-pick applied cleanly (Outcome A); `byte_equivalent: true`
  (`git patch-id --stable` MATCH, decomposition empty).

## 3. C++ review

Apply `docs/aiven/skills/cpp-review-checklist.md`. One bullet per section.

- 1 Lifetime + ownership: ✓ — `std::vector<zkutil::ZooKeeper::FutureCreate>
  futures` is a plain local whose `std::future`s are drained in the same scope
  via `future.get()`; the `zookeeper` `shared_ptr` outlives the loop. No new
  raw pointers, no long-/short-lived dependency introduced.
- 2 Exception safety: ✓ (load-bearing, see policy call 3) — the patch replaces
  one atomic `multi(ops)` with independent `asyncTryCreateNoThrow` creates whose
  results are checked in a loop that throws on any error other than `ZOK`/
  `ZNODEEXISTS`. The `/paused` znode (restore-from-backup only) is now created
  **non-atomically** with the coordination znodes. Analysis of the residual
  edge: the four creates are independent, but **`replica_path` existence is
  checked first** (`replica_path_existed`) and the whole block is skipped if it
  exists; on any create failure the loop throws → the `RefreshTask` constructor
  throws → `RefreshTask::create` / `StorageMaterializedView` construction fails
  → the MV does **not** start refreshing. On a retry, the already-created
  sibling znodes return `ZNODEEXISTS` (tolerated) → idempotent. A partial state
  "coordination znodes created but `/paused` missing" cannot leave an MV
  refreshing **during a restore**, because `/paused` is created in the same
  `futures` batch *before* the loop checks results: if `/paused`'s create fails,
  the loop throws and the MV is never constructed; if `/paused` succeeds but an
  earlier sibling failed, the loop still throws (order-independent) and the MV
  is never constructed. The MV only becomes live after all four creates are
  confirmed `ZOK`/`ZNODEEXISTS`. So no "refreshing without /paused during a
  restore" window exists. (The atomicity drop is acceptable per policy call: a
  partial set of persistent znodes is self-healing on the idempotent retry, and
  the ephemeral `running` znode — created elsewhere — is what actually
  serializes replicas.)
- 3 Thread-safety + concurrency: ✓ — no new shared state, no new lock; the
  `asyncTryCreateNoThrow` futures are created and consumed on the same thread.
  Replica serialization continues to rely on the ephemeral `running` znode, not
  on `multi` atomicity. No sleep-based synchronization.
- 4 Performance + memory: n/a (not a hot path) — this is one-time coordination
  setup at MV construction. If anything the async creates can pipeline (issue
  all, then drain) versus a single round-trip `multi`; either way it runs once
  per MV create/attach, not per row or per refresh.
- 5 Settings as public API: n/a — the patch reads/writes no setting. The
  behavior ships unconditionally (see §3 bullet 8 and clause-(v) rationale
  below), so there is no new gate.
- 6 Error handling: ✓ — `ErrorCodes::NOT_IMPLEMENTED` (removed guard) is real
  and already used in this TU; the new throw uses `Coordination::Exception` with
  `res.error` / `Coordination::errorMessage(res.error)`, the standard Keeper
  error-reporting path. The removed `attach` parameter's only use was the
  deleted guard, so it is correctly commented out (`bool /*attach*/`) to avoid
  `-Wunused-parameter`.
- 7 Upstream / vendored code: ✓ — single edit to
  `src/Storages/MaterializedView/RefreshTask.cpp`; no `contrib/**`, no
  `.claude/**`, no `.github/workflows/**`, no root `AGENTS.md`. Surrounding code
  drifted upstream but the patched regions are byte-identical (see §2).
- 8 Behavior under settings: n/a-with-note (clause (v), HUMAN-DECIDED: ship
  UNCONDITIONALLY, not gated) — behavior change 2 (atomic→async create) affects
  **all** coordinated refreshable MVs, including those on ClickHouse Keeper with
  `MULTI_READ`, not just ZooKeeper ones. The human reviewed the blast radius and
  chose to ship ungated: a default-off setting would defeat the purpose
  (ZooKeeper users would stay blocked), and the atomicity drop is self-healing
  (creates are `ZNODEEXISTS`-tolerant; `replica_path_existed` is checked first;
  a partial failure throws → MV construction fails → idempotent retry). No
  setting added; do not re-litigate.

## 4. Test design

(a) **New integration test that fails on the parent commit and passes after the patch.**

- Test path: `tests/integration/test_aiven_refreshable_mv_zookeeper/`
  (`test.py`, `configs/keeper_no_multi_read.xml`,
  `configs/keeper_with_multi_read.xml`), per the Aiven integration-test naming
  convention (`docs/aiven/runbooks/testing-suites.md §4.4`).
- Topology: two single-node instances, each embedding its **own** Keeper.
  `node` embeds a Keeper with `<feature_flags><multi_read>0</multi_read>` (plain
  ZooKeeper simulation) and hosts the differential; `node_mr` embeds a Keeper
  with `multi_read` enabled and hosts the control. Each node has a `<zookeeper>`
  client block pointing at its own embedded Keeper and global `{shard}`/
  `{replica}` macros so the `Replicated` database and `ReplicatedMergeTree`
  default paths expand.
- **Why an embedded per-node Keeper and not the shared `with_zookeeper=True`
  ensemble:** the cluster helper's feature-flag mechanism
  (`helpers/cluster.py:3463-3490`) can only **force-enable**
  (`keeper_required_feature_flags`) or **randomize**
  (`keeper_randomize_feature_flags`, default `True`) flags — it has **no
  force-OFF path** (when randomization is off it returns `1` for every flag).
  There is therefore no way to deterministically *disable* `multi_read` on the
  shared ensemble. Embedding the Keeper per node gives exact, deterministic
  control and side-steps the `keeper_randomize_feature_flags` flake documented
  in `integration-tests.md §7.4.a` entirely (the randomizer never runs against
  an embedded Keeper).
- Pre-patch run output (the FAIL — `tmp/patch-050/test-prepatch.log`):

  ```text
  test_aiven_refreshable_mv_zookeeper/test.py::test_refreshable_mv_without_multi_read FAILED
  E   helpers.client.QueryRuntimeException: Client failed! Return code: 48, ...
  E   Code: 48. DB::Exception: Received from 172.18.0.2:9000. DB::Exception:
        Keeper server doesn't support multi-reads.. (NOT_IMPLEMENTED)
  =================== 1 failed, 1 passed, 4 warnings in 13.76s ===================
  ```

- Post-patch run output (the PASS — `tmp/patch-050/test-postpatch.log`):

  ```text
  test_aiven_refreshable_mv_zookeeper/test.py::test_refreshable_mv_without_multi_read PASSED [ 50%]
  test_aiven_refreshable_mv_zookeeper/test.py::test_refreshable_mv_with_multi_read_still_works PASSED [100%]
  ======================== 2 passed, 3 warnings in 12.84s ========================
  ```

- Why this test distinguishes the Aiven gate from upstream behavior (per
  AGENTS.md §7): the observable that flips is **whether a fresh coordinated
  refreshable-MV `CREATE` succeeds against a Keeper without `MULTI_READ`** — a
  presence/absence-of-error differential (stronger than an error-code assert).
  Pre-patch the constructor's `MULTI_READ` guard throws `NOT_IMPLEMENTED`;
  post-patch the guard is gone and the async creates succeed. The
  `with_multi_read` control proves the atomic→async rewrite (behavior change 2)
  did not regress the `MULTI_READ`-enabled (ClickHouse Keeper) path — it drives
  a full `SYSTEM REFRESH VIEW` and waits for the data to land in the target,
  exercising the also-patched `readZnodesIfNeeded` read path. The control passes
  both pre- and post-patch (it is a control, not part of the FAIL→PASS pair).
- Refresh-wait note: the control's full-refresh assertion uses
  `assert_eq_with_retry` (60×1 s) on `SELECT count() FROM tgt` after
  `SYSTEM REFRESH VIEW`; this was stable in local runs (the refresh completes in
  well under a second). No flakiness was observed; if it ever flakes, it can be
  downgraded to "CREATE succeeds + MV registered" without weakening the
  differential (which lives entirely in the `without_multi_read` case).

## 5. Rollback considerations

- Revert safety: safe. The patch is a single-TU code change with no schema
  migration and no on-disk format change. Reverting re-introduces the
  `MULTI_READ` requirement; existing coordination znodes created by the patched
  code are ordinary persistent znodes that the reverted code reads via `multi`
  (which requires `MULTI_READ`) — so a revert would only re-block ZooKeeper
  users, not corrupt existing ClickHouse-Keeper deployments.
- State surviving restart: the coordination znodes (`<coordination.path>`,
  `…/replicas`, the replica znode, and `…/paused` for restores) persist in
  ZK/Keeper as before — created via `asyncTryCreateNoThrow` instead of `multi`,
  but byte-identical in content. No new on-disk or in-memory cache state is
  introduced.
- Disable without rebuild: none — the behavior ships unconditionally by human
  decision (no setting). To disable, revert the patch and rebuild.

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — the patch did not exist on this line.

### 25.8-aiven (historical, may be empty)

Original carry: authored by Tilman Moeller, committed by Joe Lynch on
2026-01-08 as `63f05c4e12fc006bcffd87011c23aa62dd8317c0` on
`v25.8.18.1-lts-aiven`.

### 26.3-aiven (this uplift)

- Cherry-pick was: clean (`git cherry-pick --no-commit -x`, Outcome A;
  auto-merge absorbed pure line-number drift, no conflict markers).
- Upstream-drift conclusion: `still-needed-applies-cleanly` (§2) — upstream 26.3
  still requires `MULTI_READ` for coordinated refreshable MVs.
- Byte-equivalence: `git patch-id --stable` MATCH (source and staged ids
  identical: `039371d2b0705a81b6f3f8e3a51e266bc092b67f`); decomposition empty →
  `byte_equivalent: true`.
- Test added at: `tests/integration/test_aiven_refreshable_mv_zookeeper/`
  (new standalone module; embedded-per-node Keeper to disable `multi_read`
  deterministically).
- Time-to-port: ~1 build (warm-cache incremental, ~44 s) + 2 incremental
  rebuilds (~43 s / ~30 s) + 2 Docker pytest runs (~13 s / ~14 s). The build
  directory was **warm-cache** (sccache hot from a prior dispatch).
- Anything surprising: the cluster helper has no force-OFF for Keeper feature
  flags; the embedded-per-node-Keeper pattern is the clean way to disable a flag
  deterministically (and as a bonus it avoids the §7.4.a randomizer flake).
```

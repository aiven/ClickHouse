# Patch 066 (+078) — MV refresh in a sharded environment

> **`v26.3.15.4-lts` reconciliation (staged, uncommitted) — RECONCILED.** Upstream
> `.15` had again rewritten the refresh loop (the #104051 keeper-connection-loss
> backport + `SYSTEM PAUSE VIEW`), producing a 17-hunk `RefreshTask.cpp` conflict
> against 066/078's own rewrite. The resolution kept upstream's two-task
> `doScheduling`/`executeRefresh` loop (so #104051's duplicate-refresh avoidance
> survives) and re-introduced 066's shard-leader/global-leader coordination +
> deferred UUID-keyed `EXCHANGE` inside it, preserving the five `.62` defect fixes
> below and PAUSE VIEW. **One reconciliation-specific, data-loss-relevant fix:**
> 066's coordination znode is *shared* across shards (`{uuid}`-keyed, not
> `{shard}`), but #104051 keys the running owner on
> `last_attempt_replica == replica_name`; stock `default_replica_name` (`{replica}`)
> collides between shards, so a peer shard could think it owned another shard's
> refresh and clobber it. Fixed by qualifying `replica_name` with the shard name
> (`<shard>/<replica>`) in the constructor — globally unique across the shared
> subtree, stable per process (so #104051's reconnect re-creation still works), used
> only as znode *data* / equality operand, never as a path segment; within-shard
> multi-replica election is unaffected. **One deliberate divergence from the `.62`
> port:** the split-out deferred `exchangeTargetTableAfterRefresh` is version-guarded
> on the root znode (mirroring #104051's CREATE-side check), stronger than the `.62`
> unguarded exchange. Build green; **6/6** stateless refreshable-MV neutrality tests
> pass and the 2-shard `test_aiven_mv_refresh_sharded` causation test passes
> end-to-end (per-shard retention `shard1.tgt={1,2,3}`, `shard2.tgt={10,20,30}`). The
> §11 open item (single-node vs coordinated error-reporting asymmetry) is carried
> forward unchanged. The `.62` write-up below is retained as the baseline.
>
> **Status (`v26.3.10.62-lts` port — baseline): single-node neutrality RESTORED.** The
> squashed 066+078 change is staged and builds clean, the ZooKeeper-compatibility
> requirement is satisfied, and **five** defects found during the port are fixed.
> The escalated "single-node hang" was confirmed via an A/B against a pre-066 base
> binary to be a **test-harness artifact** (un-redirected `stdin` on
> `INSERT … VALUES`), not a 066 regression (§8). Fixing the harness invocation
> unmasked **three real single-node regressions** in the refresh state-machine
> rewrite, now fixed (§7.3–§7.5). All eight upstream stateless refreshable-MV
> tests pass, and the added 2-shard causation test forms a complete
> evidence-of-causation pair: it **PASSES with 066** and **FAILS without 066**
> (pre-066 base binary in a separate worktree — shard2 loses its data via the
> cross-shard `EXCHANGE` clobber), proving it is a true discriminator (§10,
> `tmp/patch-066/neutrality-results.md`, `build/test_066_sharded_integration.log`,
> `tmp/patch-066/test_066_sharded_WITHOUT.log`). One
> **open item** — a single-node vs coordinated error-reporting asymmetry introduced
> by fix §7.3 — is left for maintainer decision (§11; **deferred**).

## 0. Lineage

| LTS uplift | Source SHA(s) on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | 066 `3866708bab7753e89d93842acb11eb39ca3a6662`; 078 `ea9c5d6420f7d53ccaa51bc30401f2e4a1938eb9` | Aliaksei Khatskevich / committer Joe Lynch | original carry (two commits) |
| 26.3-aiven (`.10.62`) | (staged, one squashed commit) | Cursor agent (patch-066 dispatch) | conflict-resolved + 5 defect fixes; **single-node neutrality restored** |
| 26.3-aiven (`.15.4`) | (re-resolved on intra-LTS rebase; staged, uncommitted) | Cursor agent (patch-066 reconcile) | re-merged against #104051 + PAUSE VIEW; `replica_name` shard-qualified (shared-znode ownership fix); deferred exchange version-guarded; **6/6 neutrality + sharded causation test pass** |

066: "Fix MV refresh in sharded environment", author `alex.khatskevich@aiven.io`,
2025-12-22, 4 files / +1011 loc (inventory row 066).
078: "Fix MV refresh task race condition", same author, 2026-05-07, 2 files /
+69 loc (inventory row 078). 078's body says *"On version upgrade squash with
commit 'Fix MV refresh in sharded environment'"* — it only tunes 066's
race-recovery timing and cannot apply without 066, so the two are landed as one
squashed commit `patch-port(066,078)`.

## 1. Purpose — the data-loss bug 066 fixes

Refreshable materialized views already coordinate **across the replicas of one
shard** (the `coordinated` / `CoordinationZnode` / `RunningOnAnotherReplica`
machinery, the `…/replicas` Keeper tree). That layer exists on both the 25.8 base
and the 26.3 HEAD.

In a sharded `DatabaseReplicated`, each shard ran the refresh **independently**
and finished it with a replicated `EXCHANGE`/`DROP`. Because DDL in a Replicated
database propagates to **all** shards, one shard's `EXCHANGE` swapped the target
table for a temporary table that held **no data on the other shards**, deleting
their freshly written rows. This is a real data-loss bug.

## 2. Mechanism — the shard-coordination layer 066 adds

066 adds an **orthogonal shard-level layer on top of** the replica-level one:

- The Keeper tree gains a `…/shards/<shard_name>` subtree and a **global-leader /
  shard-leader** hierarchy: `tryBecomeGlobalLeader`, `tryBecomeShardLeader`,
  `markShardFinished`, `checkAllShardsFinished`, `getAllShardsCount`, the
  `is_global_leader` / `is_shard_leader` flags.
- The target-table `EXCHANGE` is **deferred until all shards report finished**
  and is keyed by the **temporary table UUID** (the global leader stores the
  table id; shard leaders read it via `getOrWaitForTemporaryTableID`), so
  DDL-replication timing can no longer make a shard swap the wrong table.
- `StorageMaterializedView::prepareRefresh` is **split** into
  `prepareTableForInsert(append, ctx)` (creates the temp table) +
  `prepareRefresh(ctx, target_table)` (builds the `INSERT … SELECT`), so the
  temp table is created first, then coordination happens, then the insert runs.

Upstream 26.3 has **no** shard-level coordination, so this is
**`still-needed-but-rewrite`**, not obsoleted by upstream.

## 3. 078 — race-recovery timing (squashed in)

078 adds `RefreshTask::isCurrentRefreshStillActive` and calls it once per second
inside `getOrWaitForTemporaryTableID`'s wait loop, so a shard leader waiting for
the global leader's temp-table id bails out early when the running/root znode no
longer points at its refresh dir (the "race recovery … was waiting too long to
fire"). **078 is coordinated-only**: `isCurrentRefreshStillActive` returns `true`
immediately when `!coordination.coordinated`, and its only caller is the
coordinated-only `getOrWaitForTemporaryTableID`. It therefore has **no effect on
single-node behavior** and is not implicated in the neutrality failure (§8).

## 4. Gating decision: SHIP UNGATED

Per the dispatch this is a data-loss correctness fix and is **not** placed behind
a default-off `aiven_` setting (gating it off would leave the corruption active).
The obligation is **test-neutrality** for single-shard / single-replica
refreshes — now **met** after the §7.3–§7.5 fixes (§8, §10).

## 5. Reconciliation with the 26.3 base

### 5.1 Load-bearing conflict — constructor znode creation (050 region)

Committed 050 (`04cf308298b`) had already rewritten the constructor's
znode-creation block from `multi(ops)` to `asyncTryCreateNoThrow` futures and
**dropped the `KeeperFeatureFlag::MULTI_READ` check**, to support real ZooKeeper.
066 was authored on top of 050 in the 25.8 fork, so the region aligned: the
resolution **keeps 050's `asyncTryCreateNoThrow` async-create pattern and the
absence of the `MULTI_READ` check**, and applies 066's `…/shards` tree on top.
066 does **not** reintroduce `multi(ops)` or an `isFeatureEnabled` gate here.

### 5.2 26.3 type / API drift in the refresh execution path

- `ASTInsertQuery` is held via `boost::intrusive_ptr`, not `std::shared_ptr`
  (`make_intrusive`, `boost::dynamic_pointer_cast`).
- `DB::QueryScope` (value, via `QueryScope::create`) replaces
  `std::unique_ptr<CurrentThread::QueryScope>`.
- `ProcessList::insert` takes the trailing `is_internal` argument.
- `InterpreterRenameQuery` no longer exposes `setInternal`, so the
  `interpreter.setInternal(true)` call in `exchangeTargetTable` is **dropped**
  (the only remaining build break after conflict resolution).

### 5.3 Drift already satisfied on HEAD (preserved)

- `createTask` stays 3-arg.
- `Coordination::setCurrentComponent` is kept at every `RefreshTask` entry point;
  audit confirms 066's new helpers issue their Keeper ops from inside
  `refreshTask` (which already sets the component) — no watch-callback thread
  issues ZK ops without the guard, so the 008 `Current component is empty`
  `LOGICAL_ERROR` does not recur.
- Logger is already `getLogger`/`LoggerPtr`.

## 6. ZooKeeper-compatibility verdict (the 050 point) — COMPATIBLE

Audit of the resolved `RefreshTask.cpp` shard-coordination ops:

- **No multi-reads.** There is no `->multi(` used for reads and no
  `isFeatureEnabled`/`MULTI_READ` gate. `getOrWaitForTemporaryTableID`,
  `checkAllShardsFinished`, `getAllShardsCount`, `isCurrentRefreshStillActive`
  use individual `tryGet` / `exists` / `getChildren` calls, which real ZooKeeper
  supports.
- **Multi-writes only where used.** `tryMulti` appears only for write batches
  (set/create/check), which Apache ZooKeeper supports.
- The constructor uses 050's `asyncTryCreateNoThrow` futures, not `multi`.

Verdict: **the new shard-coordination Keeper ops are ZooKeeper-compatible.**

## 7. Five defects found and fixed during the port

All are fixes layered on top of the faithful carry (documented for review).
§7.1–§7.2 are build/crash hazards found first; §7.3–§7.5 are the single-node
behavior regressions unmasked once the harness artifact in §8 was ruled out.

### 7.1 NULL-deref in `createRefreshDirectory` (single-node crash)

In non-coordinated (single-node) mode the Keeper handle is intentionally null
(`refreshTask` only sets `zookeeper` when `coordination.coordinated`). Every
sibling helper (`cleanupOldRefreshDirectories`, `tryBecomeGlobalLeader`,
`tryBecomeShardLeader`, …) begins with `if (!coordination.coordinated) return …;`
— but `createRefreshDirectory` was **missing that guard** (identical to the
25.8 source). Since `tryBecomeGlobalLeader` returns `true` for non-coordinated,
the unguarded `zookeeper->tryCreate` dereferenced a null pointer and a
single-shard refresh **segfaulted** (caught by `03327_permissions_on_refresh_mv`).
Fix: add the same `if (!coordination.coordinated) return;` guard, restoring the
patch's own "non-coordinated mode never touches Keeper" invariant. `…h
current_refresh_dir` stays empty in non-coordinated mode, which is what the
downstream code already expects (it reads it only inside `coordinated` branches).

### 7.2 `#deps 0` layout staleness (garbage `system.view_refreshes` progress)

066 grows `CoordinationZnode` (adds `refresh_dir`, `target_table_id`, …), which
is embedded **by value** in `RefreshTask::Info`. Several `Info` consumers
(`StorageSystemViewRefreshes.cpp`, `StorageSystemProcesses.cpp`,
`RefreshSet.cpp`) live in libraries whose CMake header-dependency tracking is
disabled (`#deps 0`), so ninja did **not** recompile them when `RefreshTask.h`
changed. They read the **old** `Info` layout while `RefreshTask.cpp` writes the
new one → shifted fields → `system.view_refreshes` reported garbage
`read_rows`/`progress` (a value ≈ `2^64 − elapsed_ns`), `total_rows=0`,
`written_rows=0` (caught by `03221_refreshable_matview_progress`). Fix:
force-recompile the `RefreshTask.h` / `StorageMaterializedView.h` include closure
(touch the consumer `.cpp`s, rebuild). After the rebuild the progress columns are
correct (`4 4 1`). **This was exactly the hazard called out in the dispatch §4.**

### 7.3 Failed refreshes not recorded in the znode (stale/empty error)

The rewrite's inner `catch (Exception &)` in `refreshTask` re-threw
**unconditionally**, so a normal single-node refresh failure skipped the shared
znode-update block and landed in the outer `catch (...)`. The outer catch sets
`scheduling.unexpected_error` but never writes `last_attempt_error` /
`last_attempt_succeeded`. Two user-visible regressions followed: `SYSTEM WAIT
VIEW` reported the generic empty-error fallback `Replica went away` instead of the
real error (broke `03258`'s `ACCESS_DENIED` assertion), and
`system.view_refreshes.exception` was never cleared after a later **successful**
refresh (broke `02932_1`). The pre-066 state machine recorded `last_attempt_*` on
every terminal transition. Fix: re-throw from the inner catch **only in
coordinated mode**; in non-coordinated mode fall through to the shared
znode-update block, which records the error on failure and clears it on success.

### 7.4 Cancelled view could never refresh again

The pre-066 loop reset `execution.interrupt_execution` to `false` at the top of
every scheduling cycle (the flag applies only to the in-flight refresh). The
rewrite dropped that reset, so after `SYSTEM CANCEL VIEW` the flag stayed `true`
and every subsequent attempt immediately re-threw `QUERY_WAS_CANCELLED`; the view
never reached `Running` again (hung `02932_2`). Fix: restore
`execution.interrupt_execution.store(false)` at the top of the scheduling loop.

### 7.5 Failed refresh queries missing from `system.query_log`

`executeRefreshUnlocked` created the refresh context with an **empty
`log_comment`** and had **no `try/catch`** around query execution, so refresh
queries were unfindable by `log_comment LIKE 'refresh of db.v%'` and a failed
refresh logged neither `ExceptionBeforeStart` nor `ExceptionWhileProcessing`
(broke `03258`'s query-log assertion). Fix, mirroring pre-066: set
`log_comment` to `refresh of <db>.<view>`, and wrap the `prepareRefresh` →
interpret → execute span in a `try/catch` that logs the failure
(`logQueryException` once the query has started, else `logExceptionBeforeStart`
for a pre-start failure such as the definer's `ACCESS_DENIED`) and re-throws so
the scheduling loop still records the znode error and reschedules.

- **Lifetime invariant:** `process_list_entry` is declared **outside** the
  `try`. The catch's `logQueryException` → `Context::getProcessListElement` holds
  only a `weak_ptr` into that entry; declaring it inside the `try` lets it be
  destroyed during stack unwinding, so the `weak_ptr` expires and logging throws
  `Weak pointer to process_list_elem expired … (LOGICAL_ERROR)`. The pre-066 code
  keeps it at function scope for exactly this reason; the rewrite must too.

## 8. Resolved — the escalated "single-node hang" was a harness artifact

The escalation reported that `02932_1/2` and `03258` hung to a 600s timeout, with
view `a` apparently going "dormant" after a failed-then-recovered out-of-schedule
refresh. **A/B against a pre-066 base binary disproved that diagnosis.**

A pre-066 base binary was built from `HEAD` (`880cc049101`, patch-port 046) via an
in-place working-tree swap of the five 066/078 source files (staged squash and the
maintainer's uncommitted docs left untouched), then confirmed via `llvm-nm` to
contain none of the 066 coordination symbols. The base binary **also hung** the
same tests at the same step. Root cause was therefore **not 066**: under the test
harness, `clickhouse-client -q "INSERT … VALUES (…)"` blocks waiting for more rows
on `stdin` unless `stdin` is redirected. `clickhouse-test` forwards its own
`stdin`, so running it with `< /dev/null` (as the CI runner does) makes the
inserts return immediately and the base binary passes `02932_1` in ~9s. The
"dormant task" was an artifact of an un-redirected `stdin`, not a scheduling bug.

With the harness invocation fixed (`< /dev/null`), the **real** 066 regressions
surfaced and were fixed in §7.3–§7.5. After those fixes, all eight upstream
stateless refreshable-MV tests pass (§10). Full A/B and root-cause evidence is in
`tmp/patch-066/neutrality-results.md`.

## 9. Backward-compat note — znode layout (document, do not block)

066 changes the coordination znode layout from `…/replicas` to
`…/shards/<shard>`. For Aiven's deployment path 066 is present in **both** 25.8
prod and the 26.3 target, so it is shards→shards (no migration). For the record:
a refreshable MV whose coordination znodes were created by an upstream-`replicas`-
layout server (26.3 base **without** this patch) would not interoperate — this is
irrelevant for Aiven (always-066) but stated explicitly. See `REPL-4` in
`major-upstream-changes.md`.

## 10. Tests

- **Causation (added, PASSES end-to-end):**
  `tests/integration/test_aiven_mv_refresh_sharded/test.py` — a 2-shard
  `DatabaseReplicated` refreshable MV with disjoint per-shard source data
  (`shard1.src={1,2,3}`, `shard2.src={10,20,30}`), asserting **per-shard** target
  content after one coordinated `SYSTEM REFRESH VIEW` (`shard1.tgt=={1,2,3}` AND
  `shard2.tgt=={10,20,30}`) — the data-itself differential, not "it ran". Run via
  `python -m ci.praktika run "integration" --test test_aiven_mv_refresh_sharded`
  against the locally-built 066 binary (`build/programs/clickhouse`): **`1 passed
  in 12.43s`** (log: `build/test_066_sharded_integration.log`). This is the
  WITH-066 leg.

- **Evidence-of-causation pair (both legs run, worktree-flip):** the *same*
  `test.py` + `configs/default_replica_path.xml` (binary-independent) were run
  against a **pre-066 base binary** built in a separate git worktree at HEAD
  `880cc049101` (patch-port 046, none of 066 — confirmed by `llvm-nm`: no
  `tryBecomeShardLeader` / `checkAllShardsFinished` symbols). Result: **`1 failed
  in 145.68s`** (log preserved at `tmp/patch-066/test_066_sharded_WITHOUT.log`;
  produced in the temporary `ClickHouse-patch-066-base` worktree, since removed).
  The failing assertion is the **shard2** check (`test.py:130`): expected
  `10\n20\n30`,
  **got empty** —

  ```
  E   AssertionError: '10\n20\n30' != ''
  E   @@ -1,3 +0,0 @@
  E   -10
  E   -20
  E   -30
  ```

  All setup queries ran cleanly (CREATE DATABASE×2, one replicated CREATE TABLE,
  SYNC, disjoint INSERTs, CREATE MV, `SYSTEM REFRESH VIEW` — no exceptions), and
  the shard1 assertion (`test.py:122`) passed (`shard1.tgt=={1,2,3}`) before shard2
  failed. So **shard2 lost its `{10,20,30}`**: shard1's refresh finished with a
  replicated `EXCHANGE` that, because DDL propagates to all shards in a Replicated
  database, swapped shard2's `tgt` for a temp table empty on shard2 — exactly the
  cross-shard clobber of §1. Which shard loses is a race (here shard2; the symptom
  is "some shard ends up with an empty `tgt`"). **FAIL-without-066 + PASS-with-066
  on the identical test = a valid, non-vacuous discriminator** (the test genuinely
  exercises the bug 066 fixes; it is not merely asserting completion).

  Three **test-scaffolding** bugs were found and fixed while making the test run
  to its assertions (none implicate 066 — all were `CREATE`-time setup errors):
  1. The setup created `src`/`tgt` **once per node**; `CREATE TABLE` in a
     `Replicated` database is itself a replicated DDL, so the second shard's
     `CREATE` hit `TABLE_ALREADY_EXISTS`. Fixed: create each table **once**, then
     `SYSTEM SYNC DATABASE REPLICA` on the other shard before its local `INSERT`.
  2. Bare `ReplicatedMergeTree()` under the stock `default_replica_path =
     /clickhouse/tables/{database}/{table}` resolves **both shards to the same
     path+replica** (no per-shard data). Fixed via a `configs/` drop-in.
  3. A name-keyed path (`…/{shard}/{database}/{table}`) then failed at refresh
     time with `REPLICA_ALREADY_EXISTS`: the refresh's temp inner table is a
     fresh-UUID **copy of the target's CREATE**
     (`StorageMaterializedView::prepareTableForInsert`), so a name-keyed path made
     it reuse the target's znode. Fixed with the standard **`{uuid}`-keyed** path
     `/clickhouse/tables/{uuid}/{shard}` (`configs/default_replica_path.xml`):
     `{uuid}` (kept unexpanded in metadata by `TableZnodeInfo::resolve`) re-expands
     to the temp table's new UUID → distinct znode; `{shard}` gives per-shard data.
  A missing `__init__.py` (required for pytest collection) was also added.
- **Neutrality (run, all green):** all eight upstream stateless refreshable-MV
  tests pass on the patched 066 build (`< /dev/null`):
  `02932_refreshable_materialized_views_1` (10.6s),
  `02932_refreshable_materialized_views_2` (17.2s),
  `03221_refreshable_matview_progress` (0.1s),
  `03258_refreshable_mv_misc` (1.0s),
  `03327_permissions_on_refresh_mv` (0.8s),
  `01910_view_dictionary_check_refresh` (24.3s),
  `03362_merge_tree_with_background_refresh` (1.7s),
  `03760_refreshable_mv_local` (0.5s).
  Full evidence in `tmp/patch-066/neutrality-results.md`.

## 11. Known asymmetry / open question (maintainer decision)

Fix §7.3 changed the **non-coordinated (single-node)** failure path: the inner
`catch (Exception &)` now re-throws **only when `coordination.coordinated`**, so a
single-node failure falls through to the shared znode-update block that records
`last_attempt_error` / `last_attempt_succeeded`. This is what makes
`system.view_refreshes.exception` reflect the real error and, crucially, get
**cleared on a later successful refresh** (restoring pre-066 behavior and fixing
`02932_1` / `03258`).

The **coordinated (sharded/multi-replica)** path was left as the rewrite has it:
failures still propagate to the outer `catch (...)`, which routes them through
`scheduling.unexpected_error` rather than the shared `last_attempt_*` znode block.
So the two paths now report and clear refresh errors **differently**:

- single-node: error recorded in `last_attempt_error`, cleared on next success;
- coordinated: error surfaced via `scheduling.unexpected_error`.

This asymmetry is **intentional and minimal for this port** — §7.3 deliberately
restricts itself to the single-node path to avoid changing the shard-coordination
error semantics 066 introduced (which interact with the global-/shard-leader
recovery and 078's race timing). Whether the coordinated path should be brought to
the same "record-then-clear in the shared znode" behavior is an **open item for
maintainer decision**; it is not implemented on either side here. If unified, the
change belongs in the coordinated branch of the inner `catch` in
`RefreshTask::refreshTask` and must be re-validated against the sharded causation
test (§10) and the coordinated-recovery failpoint
`refresh_task_stop_racing_for_running_refresh`.

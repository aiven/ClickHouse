# Patch 049 — refreshable-mv-shard-macro-expansion

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `cc745f53f9` | Tilman Moeller (author), Joe Lynch (co-author) | original carry |
| 26.3-aiven | `patch-port(049)` | T3.13 worker (source + scaffold), T3.14 worker (test redesign + evidence) | byte-equivalent cherry-pick; integration test with verified pre/post evidence pair (T3.14 redesign per (iv) reachability proof) |

The 26.3-aiven carry is its `patch-port(049)` commit (find it with `git log --grep '^patch-port(049)'`); per `commit-hygiene.md` §4 the `-aiven-dev` branch is resliced at each LTS transition, so we pin the stable subject handle, not a SHA that would re-hash.
## 1. Purpose

Refreshable materialized views (RMV) created inside a `DatabaseReplicated`
database coordinate their refresh-state across replicas through a ZooKeeper
path expanded from the server setting `default_replica_path` (default value
contains `{uuid}`, `{shard}`, and similar macros). The pre-patch
`RefreshTask::RefreshTask` constructor builds a `Macros::MacroExpansionInfo`
containing only `info.table_id` — `info.shard` is left default — and asks
`Macros::expand` to substitute the path. When `{shard}` is in
`default_replica_path` AND the server config provides no global `<shard>`
macro fallback, expansion throws `Code: 139 NO_ELEMENTS_IN_CONFIG`.

The patch closes the gap: it looks up the containing database via
`DatabaseCatalog::instance().getDatabase(...)`, and if the database is a
`DatabaseReplicated` it assigns `info.shard = replicated_db->getShardName()`
so `{shard}` expands to the engine's literal shard name. The "why" is
durable: Aiven customers running `DatabaseReplicated` with the per-database
shard name as the authoritative source (and `<shard>` intentionally absent
from the global config) cannot otherwise issue `CREATE MATERIALIZED VIEW ...
REFRESH ...` against such a database.

Source SHA on `v25.8.18.1-lts-aiven`: `cc745f53f9`.
Original author: Tilman Moeller `<tilman.moeller@aiven.io>`, 2026-01-07.
Co-author: Joe Lynch `<joe.lynch@aiven.io>`.

## 2. Upstream-drift findings

### Commands run

```bash
# (i) Insertion-point context at line 99-104.
sed -n '99,104p' src/Storages/MaterializedView/RefreshTask.cpp

# (iii) Identifier check — getShardName signature.
grep -n 'getShardName' src/Databases/DatabaseReplicated.h

# Header already included.
grep -n 'DatabaseReplicated.h' src/Storages/MaterializedView/RefreshTask.cpp

# Tier 2 patch-id check.
git show cc745f53f9 > tmp/patch-049/src.patch
git diff --cached     > tmp/patch-049/new.patch
git patch-id < tmp/patch-049/src.patch
git patch-id < tmp/patch-049/new.patch
```

Logs: `tmp/patch-049/cherry-pick.log`, `tmp/patch-049/diff-cached-stat.log`,
`tmp/patch-049/src.patch`, `tmp/patch-049/new.patch`.

### Findings

- Identifier inventory (all present on 26.3):
  - `DatabaseCatalog::instance().getDatabase(...)` — standard CH API
    (`src/Interpreters/DatabaseCatalog.h`).
  - `DatabaseReplicated` — class at `src/Databases/DatabaseReplicated.h`.
  - `getShardName()` — public method at
    `src/Databases/DatabaseReplicated.h:113`, signature
    `String getShardName() const { return shard_name; }`.
  - `info.shard` — existing field on `Macros::MacroExpansionInfo`.
  - `<Databases/DatabaseReplicated.h>` — already included at line 6 of
    `RefreshTask.cpp`; no new include needed.
- Upstream-equivalent grep across `src/Storages/MaterializedView/` for
  `info.shard = ...` setters: ZERO matches. The fix has not been
  independently merged upstream despite ~20+ upstream commits to
  `RefreshTask.cpp` between the LTS tags. Patch still does meaningful work.
- Hunk-context byte-stability: lines 99-104 of
  `src/Storages/MaterializedView/RefreshTask.cpp` are byte-identical between
  25.8 and 26.3 (verified in Step 1). The pre-patch line at 102 is
  `info.table_id = view->getStorageID();` — the patch's `+` lines are
  inserted immediately after.
- Tier 2 patch-id: BOTH source and staged diff produce patch-id
  `98be050c54304f8d8f468d9549e1d9c84398545c`. `byte_equivalent: true`.
- Conclusion: **`still-needed-and-applies`** for cherry-pick purposes.
  The source change is structurally sound on 26.3.

## 3. C++ review

Per `docs/aiven/skills/cpp-review-checklist.md`. One bullet per checklist section.

- 1 Lifetime + ownership: `✓` — `DatabaseCatalog::instance().getDatabase(...)`
  returns a `DatabasePtr` (`shared_ptr<IDatabase>`); the local `database`
  variable holds the shared reference for the lifetime of the `if`-block.
  `dynamic_cast<const DatabaseReplicated *>(database.get())` produces a
  non-owning raw pointer used only for the synchronous `getShardName()` call;
  the `shared_ptr` outlives that read.
- 2 Exception safety: `note — DatabaseCatalog::getDatabase` throws
  `UNKNOWN_DATABASE (Code: 81)` if the database is absent. This is
  effectively unreachable in the RMV constructor's context — the storage is
  being constructed BY this database — but the new exception path is a real
  one. The existing `Macros::expand(...)` calls on the next two lines also
  throw on macro-expansion failure; the surrounding code already runs under
  an exception-aware constructor frame.
- 3 Thread-safety + concurrency: `n/a` — the patched code runs inside
  `RefreshTask::RefreshTask`, which is invoked from
  `StorageMaterializedView::StorageMaterializedView` on the thread executing
  the DDL. No new lock, no thread-pool work.
- 4 Performance + memory: `✓` — one `DatabaseCatalog::getDatabase` lookup
  + one `dynamic_cast` + (conditionally) one `String` copy of the shard
  name. Cost is sub-microsecond and fires once per `CREATE MATERIALIZED
  VIEW ... REFRESH ...` (cold path, not per-row, not per-refresh).
- 5 Settings as public API: `n/a` — no setting added or consumed.
- 6 Error handling: `note — see §3.2 above`. The patch shrinks the
  trigger surface for the pre-patch `NO_ELEMENTS_IN_CONFIG` failure but
  introduces no new error code into the public surface.
- 7 Upstream / vendored code: `✓` — only `src/Storages/MaterializedView/RefreshTask.cpp`
  is touched. No `contrib/**`, no `.claude/**`, no workflows.
- 8 Behavior under settings: `n/a` — patch is not setting-gated.
- 9 Parent preflight checklist: `✓` — parent's (i)/(ii)/(iii) priors all
  re-verified by the worker (line 102 pre-context byte-stable; `getShardName`
  signature matches; header already included). Tier 1 + Tier 2 PASS.

## 4. Test design

### Decision: integration test with `ReplicatedMergeTree` target (T3.14 redesign, verified)

The T3.14 test trigger is a refreshable materialized view created inside
a `DatabaseReplicated` database, with a `ReplicatedMergeTree()` target
table (no explicit engine args — DR rewrites them internally). The server
config intentionally omits the `<shard>` macro; only `<replica>` is set
(`configs/no_shard_macro.xml`). This is the same setup the source patch
was authored against on 25.8.

### (iv) reachability proof (parent preflight, applied before drafting T3.14)

Reading `src/Storages/StorageMaterializedView.cpp:200-246`:

- Line 200 guards the whole sanity-check block with
  `if (mode < LoadingStrictnessLevel::ATTACH && !fixed_uuid)`. For a
  fresh `CREATE MATERIALIZED VIEW ... REFRESH ...` (non-APPEND,
  non-ATTACH), this is true.
- Line 221: `is_replicated_table = inner_engine.starts_with("Replicated")
  || inner_engine.starts_with("Shared")`.
- Line 222: `if (is_replicated_table && !is_replicated_db) throw ...` —
  gate A. With `is_replicated_db = true` (DR), does NOT fire.
- Line 224: `if (!is_replicated_table && refresh_coordinated) throw ...`
  — gate B (this is the gate T3.13 hit). With
  `is_replicated_table = true` (target engine starts with `Replicated`),
  does NOT fire.
- Line 246: `RefreshTask::create(this, getContext(), ...)` is reached —
  calls `std::make_shared<RefreshTask>(...)` at `RefreshTask.cpp:161` —
  hits the patched constructor body at `RefreshTask.cpp:103-105`.

Conclusion: DR + `ReplicatedMergeTree` target + non-APPEND MV passes
BOTH `BAD_ARGUMENTS` gates and reaches the patched code. Verified by
parent reading, not by guess.

### Why the target table's creation is not itself the differential

`ReplicatedMergeTree()` with no explicit args inside DR is the canonical
pattern. `DatabaseReplicated` rewrites the args internally to
`ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')`
and sets `database_replicated_allow_replicated_engine_arguments = 3` in
the rewritten context (see `src/Databases/DatabaseReplicated.cpp:1559`).
The macros are then expanded by DR's OWN expansion code at
`DatabaseReplicated.cpp:1191-1199`, which already sets both
`info.shard = getShardName()` AND `info.replica = getReplicaName()`. The
target table's creation therefore succeeds on BOTH pre-patch and
post-patch builds — by design; patch 049 does not touch DR's expansion.

The differential lives one frame deeper: the RMV's coordination path
is computed in `RefreshTask::RefreshTask` at `RefreshTask.cpp:103-107`,
independently of the target table's path. The macros there come from
`default_replica_path` (default `/clickhouse/tables/{uuid}/{shard}`) and
`default_replica_name` (default `{replica}`). PRE-PATCH: `info.shard` is
unset → `Macros::expand` falls through to global config → no `<shard>`
macro present → throws `Code: 139 NO_ELEMENTS_IN_CONFIG`. POST-PATCH:
`info.shard = replicated_db->getShardName() = "aiven_shard_x"` →
expansion succeeds.

### T3.14 redesign verification

Worktree-flip per `docs/aiven/runbooks/integration-tests.md` §7.3. Logs
at `tmp/patch-049-t314/test-prepatch.log` and
`tmp/patch-049-t314/test-postpatch.log`.

**Pre-patch run (binary built with patch absent from worktree)**:

```
FAILED test_aiven_refreshable_mv_shard_macro_expansion/test.py::test_refreshable_mv_in_replicated_database_expands_shard_macro
E   helpers.client.QueryRuntimeException: Client failed! Return code: 139, stderr: Received exception from server (version 26.3.10):
E   Code: 139. DB::Exception: Received from 172.18.0.5:9000. DB::Exception: No macro 'shard' in config while processing substitutions in '/clickhouse/tables/{uuid}/{shard}' at '27' or macro is not supported here. Stack trace:
E   . (NO_ELEMENTS_IN_CONFIG)
E   (query: CREATE MATERIALIZED VIEW testdb.mv REFRESH EVERY 1 HOUR TO testdb.target AS SELECT 1 AS a)
```

The error fires precisely where the patch defends: `Macros::expand` of
`default_replica_path` in `RefreshTask::RefreshTask`. Reachability
demonstrated.

**Post-patch run (patch flipped back into worktree, rebuilt)**:

```
test_aiven_refreshable_mv_shard_macro_expansion/test.py::test_refreshable_mv_in_replicated_database_expands_shard_macro PASSED [100%]
============================ 1 passed, 2 warnings in 12.31s =====================
```

Evidence-of-causation pair verified: same trigger, opposite outcomes,
attributable to the 3-line patched constructor body.

### One Keeper-feature-flag pin required for determinism

The test instance pins `keeper_required_feature_flags=["multi_read"]`.
Without this pin, `helpers/cluster.py` randomizes the Keeper feature
flags per-run (`keeper_randomize_feature_flags=True` default), and when
`MULTI_READ` lands as `disabled` the patched code path bails at
`RefreshTask.cpp:117-119` with `Code: 48 NOT_IMPLEMENTED` ("Keeper server
doesn't support multi-reads.") before the multi-write at line 142. Pinning
`multi_read` matches the precedent in
`tests/integration/test_refreshable_mv_skip_old_temp_table_ddls/test.py:22`
and is documented inline in the test for future reference. The pin does
NOT affect pre-patch behavior — pre-patch fails at line 106
(`Macros::expand`) which is upstream of the `multi_read` check.

### T3.13 escalation history (preserved for lineage)

T3.13's original test body used a non-replicated target table
(`ENGINE = MergeTree() ORDER BY a`) inside the same DR setup. That
combination is rejected by the gate at
`StorageMaterializedView.cpp:222-225` (gate B above) with
`Code: 36 BAD_ARGUMENTS` BEFORE `RefreshTask::create` runs:

```cpp
if (!is_replicated_table && refresh_coordinated)
    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "This combination doesn't work: refreshable materialized view, "
        "no APPEND, replicated database, non-replicated table. ...");
```

In a `Replicated` database, `StorageMaterializedView.cpp:193-197` forces
`refresh_coordinated = true` for non-APPEND MVs. The combination
"Replicated DB + non-replicated target + non-APPEND" is therefore
structurally unreachable for the patched code. T3.13's worktree-flip
showed identical `Code: 36` on both pre-patch and post-patch — the
patch is OBSERVATIONALLY INERT against that trigger because the
trigger fires earlier in the stack. T3.13 escalated with
`test_design_blocked` per the dispatch's explicit "FIXED" directive,
and T3.14's parent prompt redesigned the trigger after applying the
(iv) reachability proof above.

The gate at `StorageMaterializedView.cpp:222-225` is NOT new in 26.3:
`git log -L 225,225:src/Storages/StorageMaterializedView.cpp` shows it
landed in upstream commit `48ec505823e` ("Refreshable MV: coordination
and Replicated DB support") on 2024-03-09, ~22 months before the source
patch was authored. The same blocker exists on 25.8 — yet the patch's
authoring tests on 25.8 worked because the authors used a
`ReplicatedMergeTree` target (per the source commit message and
existing 25.8 work), the same shape T3.14 now adopts on 26.3. This is
also the resolution of T3.13's open question about whether the patch
was load-bearing on 25.8: it was; the 25.8 test design used the same
shape we now use here.

### Other candidates considered (not selected for T3.14)

Recorded for posterity; T3.14 selected option 1.

1. **(SELECTED) `ReplicatedMergeTree()` target inside DR.** DR rewrites
   the engine args using its own macro-expansion code which is
   independent of the patched code path. Macros relevant to the patch
   are still in `default_replica_path` and still need
   `info.shard = replicated_db->getShardName()` from the patched
   constructor.
2. **`ReplicatedMergeTree` target with literal ZK path** —
   `ENGINE = ReplicatedMergeTree('/clickhouse/tables/target/r', 'r1')`.
   Functionally equivalent for the patch's purposes, but requires
   `database_replicated_allow_replicated_engine_arguments` to be set
   permissively. Option 1 is simpler.
3. **APPEND mode** —
   `CREATE MATERIALIZED VIEW testdb.mv REFRESH EVERY 1 HOUR APPEND TO testdb.target ...`.
   With APPEND, gate B does not fire because `refresh_coordinated` is
   not forced. However, this changes the semantics under test — the
   patch's target scenario is the non-APPEND case where coordination is
   mandatory.
4. **Direct stateless test against `RefreshTask::RefreshTask` via a
   non-coordinated path.** Not viable — in a non-Replicated DB,
   `dynamic_cast<const DatabaseReplicated *>` returns null, so BOTH
   the bug AND the fix no-op. No differential.

### Test files (now staged)

- `tests/integration/test_aiven_refreshable_mv_shard_macro_expansion/__init__.py` (empty marker)
- `tests/integration/test_aiven_refreshable_mv_shard_macro_expansion/configs/no_shard_macro.xml` (only `<replica>` macro)
- `tests/integration/test_aiven_refreshable_mv_shard_macro_expansion/test.py` (single test, self-documenting via docstring)

## 5. Rollback considerations

- Revert is safe: no schema migration, no on-disk format change. The patch
  is a 3-line additive change inside a constructor; reverting it restores
  the pre-patch macro-expansion behavior.
- State that survives `clickhouse-server` restart: the patched code only
  affects the ZK coordination path computed for newly-created RMVs. RMVs
  created post-patch with `{shard}` in their coordination path will, on
  restart, attempt to ATTACH against the same `{shard}`-bearing path; if
  the patch is reverted the ATTACH will again hit `NO_ELEMENTS_IN_CONFIG`
  unless `<shard>` is then present in the global config. This is the
  symmetric failure mode customers were seeing pre-patch; no new on-disk
  state is created.
- Setting to disable the new behavior without rebuilding: none — the patch
  is unconditional code. To disable, revert and rebuild.

## 6. Per-uplift notes

### 25.8-aiven (historical, may be empty)

n/a — original carry (`cc745f53f9`, Tilman Moeller + Joe Lynch, 2026-01-07).
No 25.8 dossier exists; this is the patch's first dossier.

### 26.3-aiven (this uplift)

- Cherry-pick was: **clean** (`git cherry-pick --no-commit -x cc745f53f9`
  produced exactly 3 insertions in `src/Storages/MaterializedView/RefreshTask.cpp`
  at line 102-105; no conflict markers; `Auto-merging` reported but the
  diff applied verbatim).
- Patch-id semantic verification: **byte-equivalent**. Both source and
  staged diff produce `98be050c54304f8d8f468d9549e1d9c84398545c`. Tier 2 PASS.
- Build: post-patch full build succeeded (`ninja -C build clickhouse`
  exit 0, ~97 s wall, warm cache). Pre-patch flipped-out incremental
  rebuild succeeded (~45 s). Post-patch flipped-back incremental rebuild
  succeeded (~44 s). All Tier 3a builds PASS.
- Upstream-drift conclusion: `still-needed-and-applies` (per §2).
- Regression sweep on touched-file's stateless companions:
  - `03760_refreshable_mv_local`: **OK** (0.38 s).
  - `03258_refreshable_mv_misc`: **timeout** (600 s; hung during initial
    `CREATE USER / CREATE DATABASE / CREATE TABLE Engine Memory / INSERT /
    GRANT` setup, well before any RMV / RefreshTask code). The test has
    multiple prior "Fix test" / "Probably fix flaky test" upstream commits
    in its history. Hang is unrelated to the patched code path. Per
    dispatch policy ("both pass OR both pre-existing-flaky with the patched
    file untouched-by-effect"), this is acceptable.
- Tier 3b status: **PASS — integration test with verified
  evidence-of-causation pair (T3.14 redispatch)**. Test at
  `tests/integration/test_aiven_refreshable_mv_shard_macro_expansion/test.py`;
  pre-patch produces `Code: 139 NO_ELEMENTS_IN_CONFIG` ("No macro 'shard'"),
  post-patch passes. Logs at `tmp/patch-049-t314/test-prepatch.log` and
  `tmp/patch-049-t314/test-postpatch.log`. See §4 for the full
  reachability proof, redesign rationale, and T3.13 escalation history.
- T3.13 → T3.14 redispatch flow: T3.13 cherry-picked + built + ran a
  regression sweep + authored the test scaffolding but escalated
  `test_design_blocked` because the originally-specified trigger
  (`MergeTree` target inside DR) is gated out by
  `StorageMaterializedView.cpp:222-225` before reaching the patched
  code. The parent applied a new (iv) preflight clause — "verify the
  test trigger reaches the patched code, not just that the patched code
  exists" — by reading `StorageMaterializedView.cpp:200-246` directly
  and confirming that a `ReplicatedMergeTree` target passes both gates
  and reaches `RefreshTask::create` at line 246. T3.14 inherited
  T3.13's staged source change + dossier scaffold and only redesigned
  the test trigger and re-ran the worktree-flip. This is the first
  T3.x dispatch in the system that inherits a partially-staged INDEX
  from a prior escalated dispatch.
- Parent-preflight (i)/(ii)/(iii) discipline: priors verified end-to-end
  (line context byte-stable, identifier inventory complete, include
  present). The discipline performs as expected for source-only
  verification.
- `test_aiven_<slug>/` convention rule-of-three counter: T3.13 followed
  the naming convention exactly; T3.14 reused the directory unchanged
  for the redesigned test body. Convention promotion 2/3 → 3/3 is on
  the basis of the final committed shape (this dossier's test), not on
  the T3.13 intermediate state.
- Time-to-port (subagent wall-clock): T3.13 ~30 min, T3.14 ~25 min
  (test redesign + 1 build cycle to discover the keeper-feature-flag
  randomization, 1 retry to pin `multi_read`, 2 final worktree-flip
  builds + tests, dossier updates). Warm-cache build dir.
- Anything surprising:
  - (T3.13 finding, preserved): the upstream
    `StorageMaterializedView.cpp:222-225` check is a structural gate
    that routes the originally-specified test design around the
    patched code. For future RMV-coordination-path patches the
    preflight should include the `StorageMaterializedView` validation
    gates as a checked invariant.
  - (T3.14 finding): the redesigned trigger demonstrates the patched
    code is REACHABLE on 26.3 AND DOES change observable behavior —
    confirming the patch is load-bearing on 26.3 (not vacuously-true
    or obsoleted-by-upstream).
  - (T3.14 finding): the integration-test framework's default
    `keeper_randomize_feature_flags=True` masks the differential ~50% of
    runs by toggling `MULTI_READ` off, which causes the patched code to
    bail at `RefreshTask.cpp:117-119` (`Code: 48 NOT_IMPLEMENTED`)
    BEFORE the multi-write at line 142. Pinning
    `keeper_required_feature_flags=["multi_read"]` in `add_instance` is
    required for deterministic post-patch behavior. The existing
    `test_refreshable_mv_skip_old_temp_table_ddls/test.py` has the same
    pin for the same reason.

## 7. Follow-ups

1. **Parent-preflight (iv) clause: "verify the test trigger reaches the
   patched code, not just that the patched code exists".** T3.14 is the
   FIRST concrete application of this clause: the parent read
   `StorageMaterializedView.cpp:200-246` and confirmed that a
   `ReplicatedMergeTree` target inside DR passes both BAD_ARGUMENTS
   gates and reaches `RefreshTask::create` at line 246 BEFORE drafting
   the T3.14 dispatch prompt. Application succeeded: the redesigned
   trigger produced the expected `Code: 139` pre-patch and PASS
   post-patch on the first attempt (modulo a Keeper-feature-flag pin
   discovered during the test run; see §6 for the surprise log).
   Recommended formalization: promote (iv) into the standing
   parent-preflight discipline after a third confirming use (T3.14 is
   the first; rule-of-three pending).

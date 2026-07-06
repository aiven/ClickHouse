<!-- subagentStop archive | source=transcript | status=completed | type=general-purpose | ts=2026-06-16T12:22:26Z -->

Here is my exit report.

---
outcome: escalate
patch_slug: mv-refresh-sharded
source_sha: 3866708bab7753e89d93842acb11eb39ca3a6662 (066); ea9c5d6420f7d53ccaa51bc30401f2e4a1938eb9 (078)
proposed_commit:
  staged_files:
    - src/Storages/MaterializedView/RefreshTask.cpp
    - src/Storages/MaterializedView/RefreshTask.h
    - src/Storages/StorageMaterializedView.cpp
    - src/Storages/StorageMaterializedView.h
    - src/Common/FailPoint.cpp
    - tests/integration/test_aiven_mv_refresh_sharded/test.py
    - docs/aiven/patches/066-mv-refresh-sharded.md
    - docs/aiven/uplifts/26.3/inventory.md
    - docs/aiven/uplifts/26.3/major-upstream-changes.md
  commit_message: |
    See tmp/patch-066/commit-message.txt (subject `patch-port(066,078): Fix MV
    refresh in sharded environment`, both provenance lines, Original-author line,
    Co-authored-by). NOT to be committed until the neutrality blocker is resolved.
  byte_equivalent: false
tests:
  added: yes
  kind: integration + upstream-existing
  paths:
    - tests/integration/test_aiven_mv_refresh_sharded/test.py
  upstream_reference:
    - tests/queries/0_stateless/02932_refreshable_materialized_views_1.sh
    - tests/queries/0_stateless/03221_refreshable_matview_progress.sql
  pre_patch_fail_verified: false
  post_patch_pass_verified: false
  justification: |
    Causation integration test is shipped but NOT run to completion (needs the
    praktika/Docker harness, and is moot while single-node is wedged). Neutrality
    run is RED: 02932_1/2 and 03258 hang to 600s; 03221 and 03327 pass only after
    my two fixes. Evidence in tmp/patch-066/neutrality-results.md.
escalation_reason: test_fail_ambiguous

## Tier results

- Tier 1 (textual cherry-pick): **pass** — 066 applied 3-way; conflicts in `RefreshTask.cpp`/`.h` + `StorageMaterializedView.cpp`/`.h` resolved; 078 applied on top.
- Tier 2 (semantic patch-id): **pass (with intended deltas)** — reshaped by design (26.3 type drift + 2 defect fixes + include dedup), not a faithful byte-copy; documented in the dossier.
- Tier 3 (build + test): **build pass / test FAIL** — clean `ninja` (incl. include-closure rebuild); single-node neutrality fails (3 tests hang).

## Evidence

- ZK-compat: no `->multi(` reads, no `isFeatureEnabled`/`MULTI_READ`; constructor keeps 050's `asyncTryCreateNoThrow`; coordination reads use individual `tryGet`/`exists`/`getChildren`, writes use `tryMulti`. **Verdict: ZooKeeper-compatible.**
- Defect 1 (fixed): `createRefreshDirectory` lacked the `if (!coordination.coordinated) return;` guard every sibling helper has → single-node `zookeeper->tryCreate` null-deref → SIGSEGV in `03327`. Fixed; `03327` now PASS.
- Defect 2 (fixed): `#deps 0` staleness — 066 grew `CoordinationZnode` (embedded by value in `RefreshTask::Info`); `StorageSystemViewRefreshes.cpp.o` etc. were not recompiled → garbage `system.view_refreshes` progress (`read_rows ≈ 2^64 − elapsed_ns`). Forced include-closure rebuild → `03221` now PASS (`4 4 1`).
- **Unresolved blocker:** `02932_refreshable_materialized_views_1` FAIL 600.02s (Timeout), reproducible with `--jobs 1`. After failed out-of-schedule refresh → recover → faked-clock advance, view `a`'s refresh task goes **dormant** (won't refresh even on forced `SYSTEM REFRESH`; `system.stack_trace` has no refresh frames → not a deadlock; `BackgroundSchedulePoolTask=1`/512 → pool not saturated); dependent `b` stuck `WaitingForDependencies` forever. Same class: `02932_2`, `03258`.

## What I did

- Squashed 066+078 into one staged change; preserved 050 async-create + no-MULTI_READ; reconciled 26.3 drift (`boost::intrusive_ptr<ASTInsertQuery>`, `DB::QueryScope`, `ProcessList::insert` is_internal, dropped `InterpreterRenameQuery::setInternal`, `prepareTableForInsert`/`prepareRefresh` split); re-added `refresh_task_stop_racing_for_running_refresh` failpoint; deduped 3 includes.
- Confirmed single-node mechanisms work in isolation (basic refresh, progress, dependency chains, failure→recover→advance), but the full upstream sequence wedges.
- Wrote dossier 066, REPL-4, inventory annotations for 066 & 078, commit message, neutrality results. HEAD unchanged (`880cc049101`); maintainer's uncommitted docs untouched; no cherry-pick state left.

## Proposed next step

Need a maintainer decision: this is a data-loss fix decided to ship **ungated**, but it currently breaks single-node refresh test-neutrality (the explicit stop-and-report trigger). Recommended: (1) A/B the 3 hanging tests against the **pre-066 26.3 base** binary to confirm the regression originates here (base is expected to pass — these are upstream tests); (2) bisect the 870-line refresh-loop state resets on the **failure / out-of-schedule** path (where `a` stops rescheduling) with instrumented logging. Do not commit `tmp/patch-066/commit-message.txt` until single-node behavior is restored.

A note on process, per the mentor rule: the two bugs I fixed are worth internalizing because they recur in this codebase. The null-deref is an **invariant-symmetry** bug — when a patch establishes an invariant ("non-coordinated mode never touches Keeper") it must hold in *every* helper; one un-guarded sibling is enough to crash. The progress-garbling is the **`#deps 0` layout hazard** — when a struct that is embedded *by value* in a cross-TU type (`CoordinationZnode` inside `RefreshTask::Info`) grows, every consumer must be recompiled, and this repo's disabled header-dependency tracking won't do it for you.

**Today you learned:** a single-node hang can hide behind two unrelated fixable bugs (crash, then garbled telemetry) before the real neutrality regression surfaces — fix forward, but keep testing past the first green. **Rule of thumb:** "works in isolation, hangs in sequence" almost always means leftover *state*, not leftover *logic* — hunt the reset, not the computation. **Next rabbit hole:** instrument `scheduling.out_of_schedule_refresh_requested` / `refresh_task->schedule()` across the failure path to find why an overdue task stops rescheduling.

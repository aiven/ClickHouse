# T3.13 / T3.14 — patch 049 dispatch arc retrospective

> **What this is:** The reflection-after-cost step (spec §10 step 12) applied to the **patch 049 dispatch arc**, which spanned **two worker dispatches across two parent-session passes** — T3.13's initial port that staged source + scaffold and then escalated `test_design_blocked`, and T3.14's redispatch that inherited T3.13's staged INDEX, redesigned the test trigger using the `(iv) reachability` discipline (first named at T3.13's escalation review), and landed a clean pre/post evidence pair. This arc is the **n=1 case** for the `(iv) reachability check` discipline as a parent-preflight item.
> **Date:** 2026-05-28.
> **Dispatch references:**
>   - T3.13: `log.md` row at `2026-05-28T11:45:49Z`; subagent id `toolu_017G6Sxog7w15j7wi61nPbj6`; report archive `docs/aiven/uplifts/26.3/reports/toolu_017G6Sxog7w15j7wi61nPbj6.md` (204 lines; **backfilled** offline from on-disk JSONL `0d03c4e6-fa1d-484e-91c8-46f7526303dc.jsonl` due to hook v3 third regression — see retro 14). **Outcome: escalate, escalation_reason: `test_design_blocked`.**
>   - T3.14: `log.md` row at `2026-05-28T12:36:32Z`; subagent id `toolu_01Ngku5yMGP4YVcVxAQ3ND3G`; report archive `docs/aiven/uplifts/26.3/reports/toolu_01Ngku5yMGP4YVcVxAQ3ND3G.md` (146 lines; **backfilled** from on-disk JSONL `c3739ede-5668-4339-b6a0-7b4645c3c6d0.jsonl`). **Outcome: success.**
> **Outcome of the arc:** `success`. Five files committed in `7c2b55ed346`: `src/Storages/MaterializedView/RefreshTask.cpp` (+3 / -0 lines), `tests/integration/test_aiven_refreshable_mv_shard_macro_expansion/{__init__.py, test.py, configs/no_shard_macro.xml}` (new), `docs/aiven/patches/049-refreshable-mv-shard-macro-expansion.md` (new dossier, 410 lines). Tier 1 cherry-pick: clean. Tier 2 patch-id: `byte_equivalent: true` (`98be050c54304f8d8f468d9549e1d9c84398545c` matches between source `cc745f53f9` and staged diff). Tier 3 evidence pair: pre-patch FAIL with `Code: 139 NO_ELEMENTS_IN_CONFIG` at `RefreshTask.cpp:103` (stack frame proves reachability), post-patch PASS.

## Headline

The arc is the **first explicit application of the `(iv) reachability check` discipline** as a parent-preflight item. T3.13's worker had honored the dispatch's "FIXED test design" directive and produced a pre/post FAIL/FAIL pair — both failures fired at `Code: 36 BAD_ARGUMENTS` from `StorageMaterializedView.cpp:222-225` ("This combination doesn't work: refreshable materialized view, no APPEND, replicated database, non-replicated table"), which is structurally UPSTREAM of the patched `RefreshTask::RefreshTask` constructor body. The patched code was therefore unreachable for the chosen test trigger, regardless of whether the patch was applied. The worker correctly escalated `test_design_blocked` rather than rationalizing a "the patch must be observationally inert on 26.3" conclusion.

Parent's review of the escalation independently re-derived the (iv) reachability question: **"can the patched code at `RefreshTask.cpp:103-105` be reached at all on 26.3, given current upstream sanity checks?"** Answer: YES, via a `ReplicatedMergeTree()` target (rather than `MergeTree()`). The gate at `StorageMaterializedView.cpp:222-225` (added 2024-03-09 in upstream commit `48ec505823e`, ~22 months before the source patch) blocks `MergeTree()` targets in DR but explicitly permits `ReplicatedMergeTree()` targets. T3.14 redispatched with the corrected target and produced a clean evidence pair on the first attempt.

**This is NOT analogous to patch 060** (retro 11). Patch 060's saga ended with "ship without test" because the patched code was observationally inert on BOTH 25.8 and 26.3 via the reachable triggers (wider-scope drift defeated the predicate equally on both LTSes). Patch 049's patched code is reachable on BOTH 25.8 and 26.3, just via a different trigger than T3.13 first tried — and the 25.8 authoring tests (per the source commit message) used the same `ReplicatedMergeTree()` shape T3.14 adopted. The escalation was a correctly-caught test-design error, not a structural impossibility.

**Three rule-of-three counters advanced.** The `(iv) reachability check` discipline reached **n=1 of 3 of CRYSTALLIZED** (the discovery moment when the discipline was first named and applied). The `test_aiven_<slug>/` integration-test convention reached **3 of 3 of WORKING** (patch 006 + patch 005 + patch 049 — VERIFIED). The "inherited-staged-INDEX redispatch flow" reached **n=1 of 3** (a new workflow pattern where the redispatched worker inherits the prior worker's staged source change + dossier).

## What worked

1. **T3.13 worker escalated `test_design_blocked` rather than rationalize FAIL/FAIL as no-regression.** The worker's diagnostic correctly named the gate (`StorageMaterializedView.cpp:222-225`) and observed that the failure mode was identical pre- and post-patch (`Code: 36 BAD_ARGUMENTS` in both cases, same line). A less-disciplined worker could have argued "both LTSes block this trigger → no-regression on 26.3 → ship without test" by analogy to patch 060 — but the T3.13 worker recognized the analogy was false: the trigger blockage was a test-design error, not a structural inertness of the patch. The escalation preserved the parent's option to redesign.
2. **Parent's (iv) reachability proof was constructed explicitly.** When reviewing the T3.13 escalation, parent went deep: searched `src/Storages/StorageMaterializedView.cpp` for all gates between `CREATE MATERIALIZED VIEW` and `RefreshTask::create`; identified gate A (`StorageMaterializedView.cpp:193-197` forces `refresh_coordinated=true` in DR for non-APPEND MVs) and gate B (lines 222-225, the BAD_ARGUMENTS rejection); verified that `ReplicatedMergeTree()` target satisfies both gates (it's `is_replicated_table`, so the rejection at 222-225 doesn't fire); traced the call chain through to `RefreshTask::create` at line 246, then `std::make_shared<RefreshTask>` at `RefreshTask.cpp:161`, then the patched ctor body at lines 103-105. **The proof is written into the dossier's §4 "Why the trigger pivot," not just in the dispatch prompt** — durable institutional knowledge.
3. **T3.14 inherited T3.13's staged INDEX cleanly.** T3.13 had staged the source change + dossier (the dossier was authored with the FAIL/FAIL pair documented as the pre-T3.14 state). T3.14's worker received instructions to NOT re-do cherry-pick or dossier — only author the new test files + update §4 of the dossier with the T3.14 evidence pair. The two-pass workflow's structural advantages (context-locality, re-dispatchability, parent-control checkpoint) generalized from T3.9's decoupled-cherry-pick pattern to T3.14's inherited-INDEX pattern.
4. **T3.14 evidence pair was textbook reachability proof.** Pre-patch: `Code: 139 NO_ELEMENTS_IN_CONFIG` at `RefreshTask.cpp:103` — the stack frame literally names the patched line, proving the trigger reaches the code under test. Post-patch: PASS. Pre-patch fails because `info.shard` is unset → `Macros::expand` of `default_replica_path` falls through to global config → no `<shard>` macro present → throws. Post-patch succeeds because `info.shard = replicated_db->getShardName() = "aiven_shard_x"` → expansion succeeds. The differential is precisely the 3 lines patched.
5. **The Keeper-feature-flag pin issue was surfaced and resolved in-session.** T3.14's first test run post-patch hit `Code: 999 KEEPER_EXCEPTION` (intermittent); the worker investigated, traced to `helpers/cluster.py`'s `keeper_randomize_feature_flags=True` default which can land `MULTI_READ` as `disabled` and cause the patched code path to bail at `RefreshTask.cpp:117-119` with `Code: 48 NOT_IMPLEMENTED` before reaching the `multi_read` call at line 142. Worker pinned `keeper_required_feature_flags=["multi_read"]` (precedent: `tests/integration/test_refreshable_mv_skip_old_temp_table_ddls/test.py:22`) and the test became deterministic. Documented inline in the test file + dossier §4.

## What surfaced

### A. `(iv) reachability check` discipline — first naming + first application (n=1 of 3 CRYSTALLIZED)

**Symptom (positive):** T3.13's escalation was the moment the `(iv)` check was named. Patch 060's saga (retro 11) had hit the same failure mode three times but did not crystallize the discipline because each redesign attempt focused on the specific gate, not on the general question "does the trigger reach the patched code?" T3.13's escalation forced the parent to ask the general question — and the answer revealed a discipline that should have existed at parent-preflight from T3.10 onward.

**Diagnosis:** the discipline has two sub-checks:
  - **(iv-a) Code-path reachability**: the test trigger's call path actually reaches the patched function body on HEAD, not gated by an upstream sanity check that returns early or throws before the patched code runs.
  - **(iv-b) Differential observability**: the test trigger produces a DIFFERENT observable outcome pre-patch vs. post-patch (i.e., the patched behavior is *demonstrable*, not silently fixed under default config OR identical-on-both-LTSes due to wider-scope drift).

Patch 060's three escalations were a mix of (iv-a) and (iv-b) failures (T3.10 = iv-a `checkProperties` gate; T3.11 = iv-b wider-scope drift defeats predicate; T3.12 = iv-a `checkAlterIsPossible` gate). Patch 049's T3.13 = iv-a `StorageMaterializedView.cpp:222-225` gate.

**Decision:** add the (iv) check to `docs/aiven/skills/dispatch-prompt-template.md` "Parent preflight checklist" as a fourth bullet alongside (i)/(ii)/(iii). Mark as PROVISIONAL pending second proactive use. The codification target text:

> **(iv) Reachability proof**: given the chosen test trigger T and the patched function F on HEAD, write out the call chain `T → ... → F`. For each intermediate frame, verify no upstream sanity check rejects T BEFORE reaching F (iv-a), and verify the observable produced by T differs between pre-patch and post-patch (iv-b). If either sub-check fails, redesign T or escalate `test_design_blocked` BEFORE the worker dispatch.

**Rule-of-three count:** **n=1 of 3 CRYSTALLIZED**. Patch 060's saga = n=0 (PROBLEM, three observations not yet named); T3.13 = n=1 (NAMED + applied to patch 049 to derive T3.14's design); T3.15 patch 042 = n=2 (proactive use); next dispatch = n=3 (codification target).

### B. `test_aiven_<slug>/` convention rule-of-three: 3 of 3 → VERIFIED-with-discipline

**Symptom (positive):** the convention introduced in T3.6 patch 006 (1/3) and applied in T3.9 patch 005 (2/3) reached its third application at T3.14 patch 049: `tests/integration/test_aiven_refreshable_mv_shard_macro_expansion/`. Three distinct test shapes under the same naming:
  - Patch 006: single-node DDL trigger inside DR, no restart.
  - Patch 005: multi-node 3-Keeper cluster, full kill-and-restart cycle.
  - Patch 049: single-node DR with macro-expansion trigger, no restart.

**Diagnosis:** the convention's three properties (prefix collision-avoidance, slug-matching, ownership signal) held under all three applications. The convention is **VERIFIED**.

**Decision:** codify VERIFIED status in `docs/aiven/runbooks/integration-tests.md §6` by promoting the rule-of-three counter from 2/3 to 3/3. (The codification is part of Phase C of the current packaging plan; defer the runbook edit to that phase.)

**Rule-of-three count:** 3 of 3 → VERIFIED.

### C. Inherited-staged-INDEX redispatch flow (n=1 of 3 WORKING)

**Symptom:** T3.14 was the first dispatch in T3.x to **inherit a prior worker's staged INDEX** rather than start from a clean cherry-pick. The dispatch prompt explicitly instructed T3.14's worker: "DO NOT re-do `git cherry-pick`. T3.13 has staged `src/Storages/MaterializedView/RefreshTask.cpp` + the dossier. Your job is to author the integration test, update the dossier §4 with the T3.14 evidence pair, and re-stage."

**Diagnosis:** the inherited-INDEX flow has three structural properties that distinguish it from a fresh dispatch:
  1. **No Tier-1 cherry-pick step**: the worker reads the existing staged diff (`git diff --cached`) as a given and proceeds to Tier-2/3 work.
  2. **Dossier is partially-authored**: §1 (Purpose), §2 (Upstream drift), §3 (C++ review) carry over from T3.13. §4 (Test design) gets a new sub-section ("T3.14 redesign verification") with the new evidence pair.
  3. **Final invariant check is stricter**: the worker must verify the staged INDEX is exactly the prior staged state + the new test files (not + a partial revert OR + an inadvertent re-edit of the existing source change).

The flow is a workflow-pattern complement to T3.9's decoupled cherry-pick + test-authoring shape: T3.9 splits ONE patch's work across two dispatches by intent (pre-arranged); T3.14 splits it across two dispatches by necessity (escalation-driven redispatch). Both share the "second worker inherits first worker's INDEX" property.

**Decision:** document the inherited-INDEX flow in `docs/aiven/runbooks/integration-tests.md §7` (or a new sibling section) as test-shape D ("Inherited-INDEX redispatch after escalation"). Mark PROVISIONAL pending second occurrence. Track for next escalation-driven redispatch.

**Rule-of-three count:** 1 of 3.

### D. Keeper-feature-flag determinism issue (n=1 of 3 ENVIRONMENTAL)

**Symptom:** T3.14's first post-patch test run hit `Code: 999 KEEPER_EXCEPTION` (intermittent). Worker investigated and traced to `helpers/cluster.py`'s `keeper_randomize_feature_flags=True` default, which randomizes Keeper feature flags per-run; on the affected run, `MULTI_READ` landed as `disabled` and the patched code path bailed at `RefreshTask.cpp:117-119` with `Code: 48 NOT_IMPLEMENTED` ("Keeper server doesn't support multi-reads.") before reaching the multi-write at line 142.

**Diagnosis:** integration tests under `helpers/cluster.py` are subjected to feature-flag randomization by default. For tests that exercise a code path requiring specific Keeper features (`multi_read`, `filtered_list`, `check_not_exists`, `create_if_not_exists`, `remove_recursive`), the test MUST pin the required features explicitly via `keeper_required_feature_flags=[...]` on the `add_instance` call. Pre-existing precedent: `tests/integration/test_refreshable_mv_skip_old_temp_table_ddls/test.py:22`.

**Resolution:** worker added `keeper_required_feature_flags=["multi_read"]` to the test's `add_instance` call. Test became deterministic. The pin does NOT affect pre-patch behavior — pre-patch fails at line 106 (`Macros::expand`) which is upstream of the `multi_read` check, so the test still correctly exhibits the FAIL/PASS differential.

**Decision:** document the Keeper-feature-flag determinism gotcha in `docs/aiven/runbooks/integration-tests.md` (Phase C "Known gotchas" subsection). Mark as 1 of 3. Track for next integration test that exercises Keeper-feature-gated code paths.

**Rule-of-three count:** 1 of 3.

### E. Hook v3 third regression occurred TWICE in this arc

**Symptom:** both T3.13 and T3.14 hook fires delivered `unknown / unknown` rows to `log.md`. The on-disk JSONLs were intact and parseable, but the hook input lacked the assistant content.

**Diagnosis:** see retro 14 for the consolidated thread. T3.13 + T3.14 are two of six explicit instances of the regression manifesting on real dispatches (T3.9 + T3.10 + T3.11 + T3.12 + T3.13 + T3.14 + T3.15 = seven instances; the probe block landed at T3.10 captured 6 of those).

**Resolution:** both rows backfilled offline in Phase A of the current packaging cleanup. Archives at `reports/toolu_017G6Sxog7w15j7wi61nPbj6.md` (T3.13) and `reports/toolu_01Ngku5yMGP4YVcVxAQ3ND3G.md` (T3.14) carry `backfilled=true` and cite their source JSONL UUIDs.

**Forward signpost:** retro 14.

## Forward decisions for T3.15+

- **`(iv) reachability check` discipline**: at n=1 of CRYSTALLIZED. T3.15 (retro 13) is the n=2 proactive use. The codification target (`dispatch-prompt-template.md` "Parent preflight checklist") is documented in retro 13. Defer the actual edit to Phase C of the packaging plan.
- **`test_aiven_<slug>/` convention**: VERIFIED at 3/3. Codify the rule-of-three update in `integration-tests.md §6` (Phase C).
- **Inherited-staged-INDEX redispatch flow**: at n=1. Track for next escalation-driven redispatch.
- **Keeper-feature-flag determinism**: at n=1. Track for next Keeper-feature-gated integration test.

## Learning log

**Today you learned:** `test_design_blocked` and `postpatch_fail` are distinct escalation types with distinct parent responses. `test_design_blocked` ⇒ redesign the trigger (the patched code IS reachable, your trigger doesn't reach it). `postpatch_fail` ⇒ investigate the wider call-chain (the trigger reaches the patched code but the observable doesn't differ; either the patch is silently defeated by wider-scope drift OR your assertion is testing the wrong thing). T3.13 was a clean `test_design_blocked` because the gate was upstream of the patched code; T3.11/T3.12 (retro 11) were `postpatch_fail` because the predicate was defeated by `AlterCommands::apply` divergence.

**Rule of thumb:** when a test FAILS pre-patch with the same error code AND at the same line as it fails post-patch, the trigger is gated upstream of the patch. That's `test_design_blocked`. When pre and post fail with DIFFERENT outcomes but neither matches the expected differential, the patch is reaching the code but the observable is wrong. That's `postpatch_fail`. The escalation type tells the parent what kind of redesign is needed.

**Next rabbit hole:** the `keeper_required_feature_flags` pin discipline is an example of "environmental coupling that the test runner randomizes by default." There are likely OTHER such randomization knobs (network latency injection, Keeper election timing, query-cancellation injection) that could affect future integration tests. Worth a one-shot audit of `helpers/cluster.py`'s randomization defaults at the next integration-test patch dispatch.

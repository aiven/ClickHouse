# T3.9 — patch 005 dispatch retrospective

> **What this is:** The reflection-after-cost step (spec §10 step 12) applied to the **patch 005 dispatch arc**, which spanned **two parent-session passes** — the T3.8 worker run (`53d78719-fd0c-44b1-bad3-03d8cdb130c2`) that staged the source change + dossier and then escalated `policy_call`, and the T3.9 follow-up run (`cdeba357-fbf1-4867-9307-52b953a0ed12`) that authored the integration test, produced the evidence pair, and re-staged. The patch 001 drop also happened in the T3.8 parent session; it is covered separately in retro 09 because it had no worker dispatch (parent-only preflight).
> **Date:** 2026-05-27.
> **Dispatch references:**
>   - T3.8 patch-005 worker: NO `log.md` row (hook v3 third regression first manifested here — see retro 14). On-disk JSONL `53d78719-fd0c-44b1-bad3-03d8cdb130c2.jsonl` (mtime `2026-05-27T09:37Z`). Cursor-internal `subagent_id` not captured by the hook; it can be reconstructed by inspecting the parent transcript's `Task` tool-call results around that timestamp if needed. **Outcome: escalate, escalation_reason: `policy_call`.**
>   - T3.9 patch-005 worker: NO `log.md` row at completion either (same regression). On-disk JSONL `cdeba357-fbf1-4867-9307-52b953a0ed12.jsonl` (mtime `2026-05-27T11:00Z`). One spurious row at `2026-05-27T10:26:31Z` (mid-T3.9-runtime, ~34 minutes before completion) populated as `unknown / unknown` and was discarded by parent inspection. **Outcome: success.**
> **Outcome of patch 005 arc:** `success`. Four files committed in `a591b4331d8`: `src/Common/ZooKeeper/ZooKeeperImpl.cpp` (+9 / -1 lines), `tests/integration/test_aiven_zk_connect_retry/` (new — `__init__.py`, `test.py`, `configs/connect_retries_2.xml`), `docs/aiven/patches/005-tolerate-zk-restart-with-exponential-backoff.md` (new dossier, ~330 lines). Tier 1 cherry-pick: manual on hunk 1, textual on hunks 2-3. Tier 2 patch-id: `byte_equivalent: false` with empty decomposition (only context-line shift). Tier 3 evidence pair: pre-patch FAIL (`restart_clickhouse(kill=True)` raises `Cannot start ClickHouse`), post-patch PASS (CH restarts cleanly within the patch's 6.3s retry window).

## Headline

T3.9 was the **second integration test in the uplift** (after T3.6 patch 006) and the **first dispatch where the test-authoring step was decoupled into a separate parent-session pass** from the cherry-pick step. The T3.8 worker correctly executed cherry-pick + dossier but recognized that the patch's defended behavior — `ZooKeeper::connect` retrying with exponential backoff during transient ZK unavailability — is NOT reachable from the stateless test runner (single shared persistent ZK; no per-test kill primitive). It escalated `policy_call` rather than improvise a test, surfacing the question: **"is the schema's `tests.added: no_trigger_on_current_lts` outcome the right answer for this patch?"**

Parent's answer (informed by T3.6 patch 006's `test_aiven_<slug>/` precedent): **No — write a custom integration test that isolates the connection-retry layer.** T3.9's worker received a fully-specified test design (Shape A: startup-while-ZK-down, with `<num_connection_retries>2</num_connection_retries>` to expose the patch's `min_num_tries=6` floor + exponential backoff) and landed it cleanly.

**Three rule-of-three counters advanced.** The `test_aiven_<slug>/` integration-test convention reached **2 of 3 of working** (patch 006 = 1, patch 005 = 2). The decoupled cherry-pick + test-authoring pattern is **n=1** (track for second occurrence). The `policy_call` escalation as a legitimate worker outcome is **n=1** (track for next occurrence — though I suspect this one will be rare; most patches' test design is decidable at parent preflight).

## What worked in T3.9

1. **T3.8 worker honored "escalate rather than improvise."** When confronted with a schema option (`tests.added: no_trigger_on_current_lts`) that didn't fit the patch's defended-behavior shape, the worker did NOT silently pick the closest-matching enum value or hand-roll a degenerate stateless test. It emitted a clean `policy_call` escalation with a 7-bullet diagnostic that named the call site (`ZooKeeperImpl.cpp::connect`), distinguished it from the operation-retry layer (`ZooKeeperRetries.h`), pointed to the existing-but-insufficient `test_inserts_with_keeper_retries/` suite, and asked the parent for a direction. The integration-tests runbook (`docs/aiven/runbooks/integration-tests.md`) is largely a transcription of that diagnostic's reasoning into a re-usable rule.
2. **Decoupled cherry-pick + test-authoring produced a cleaner dispatch.** T3.8 staged the source change + dossier with `tests.added` left as a question. T3.9 received a fully-specified test design and authored it without revisiting the cherry-pick. The two-pass shape has structural advantages:
   - T3.8's worker context is dominated by `git cherry-pick` mechanics + Tier 1/2/3 validation.
   - T3.9's worker context is dominated by integration-test mechanics (`helpers/cluster.py`, `restart_clickhouse`, `pytest.fixture(scope="module")`, ZK-kill primitives).
   
   Keeping them in separate dispatches means each worker reads exactly the prompt they need without context bloat.
3. **`test_aiven_<slug>/` convention generalized cleanly.** Patch 006 introduced the prefix; patch 005 applied it to a different shape (startup-while-ZK-down vs. patch 006's macro-expansion-on-attach). Test name: `test_aiven_zk_connect_retry/`. The convention's three properties — (a) `test_aiven_` prefix avoids collision with upstream test_inserts/test_keeper_*/, (b) `<slug>` matches the dossier slug for grep-cross-referencing, (c) clear ownership signal in the test directory listing — held under the second application.
4. **The integration-tests runbook (§7 Patterns) landed in the same session.** The T3.8 escalation produced the structural understanding ("mechanism-isolation is the difference between a defended layer and a coincidentally-tested layer"), and the §7 of `docs/aiven/runbooks/integration-tests.md` was authored from that understanding. Three sub-templates documented:
   - Test-shape A: startup-while-X-fails.
   - Test-shape B: mechanism-isolation by config-tuning (e.g., `<num_connection_retries>2</num_connection_retries>`).
   - Test-shape C: decoupled cherry-pick / test-authoring dispatch.
   
   These are codified as patterns, not yet at rule-of-three — but the runbook landing means the next dispatch's parent preflight has a reusable reference.
5. **Pre/post evidence pair has the exact diagnostic shape we wanted.** Pre-patch: `restart_clickhouse(kill=True)` raises `Cannot start ClickHouse` (the 3 tight retries with no inter-attempt sleep give up at ~300ms, before ZK reappears at 3s). Post-patch: `restart_clickhouse(kill=True)` succeeds (6 retries with exponential backoff give a total wait window of `100 + 200 + 400 + 800 + 1600 + 3200 = 6300ms` = 6.3s, so ZK's 3s reappearance is caught on the 5th retry). The pre-patch FAIL message is *specifically* the `Cannot start ClickHouse` error — not a flake, not a timeout — which makes the evidence unambiguous.

## What surfaced

### A. `policy_call` escalation as a first-class worker outcome (n=1 of WORKING mitigation)

**Symptom:** the T3.8 worker, mid-dispatch, found that the schema's `tests.added` enum (`yes`, `no_trigger_on_current_lts`, `pre_existing_upstream`, `no_test_needed`) had no value matching "patch defends a behavior reachable only via per-test ZK-kill, which the stateless runner cannot provide, AND existing integration tests mask the layer via operation-retry settings." The closest match — `no_trigger_on_current_lts` — would have been a lie (the trigger IS reachable, just not via the stateless runner). The worker escalated `policy_call` instead of choosing.

**Diagnosis:** `policy_call` is the schema-level analogue of `test_design_blocked` — both signal "I cannot proceed without parent input," but they differ in WHAT the parent needs to decide:
  - `test_design_blocked` = "the test I designed cannot reach the patched code on HEAD; redesign the test."
  - `policy_call` = "the schema offers no valid enum for this patch's situation; pick one OR extend the schema OR change the dispatch outcome."

The parent's reply was a design specification ("write integration test, here's the shape"), which T3.9 implemented. An alternative would have been a schema extension (`tests.added: integration` as a new enum value) which we deferred — the existing schema has `tests.kind` for `stateless | integration` which covers the test type, and `tests.added: yes` covers the existence; adding a third dimension for "added via separate-pass dispatch" felt premature at n=1.

**Decision:** document `policy_call` in `docs/aiven/schema/halt-and-escalate.md` as a recognized escalation type alongside `test_design_blocked` and `postpatch_fail`. The schema file already has a section on escalation_reason values; add `policy_call` with the T3.8 example as the canonical case. Defer the formal codification until a second `policy_call` instance surfaces (n=2).

**Rule-of-three count:** 1 of 3. First explicit `policy_call`. Track for next dispatch where the worker hits a schema-doesn't-fit situation.

### B. Decoupled cherry-pick + test-authoring dispatch (n=1 of WORKING pattern)

**Symptom:** patch 005 was completed across TWO parent-session-passes (T3.8 + T3.9). Patches 002, 007, 010, 040, 042, 049, 060, 073, 077 were all completed in ONE pass each. The two-pass shape was driven by the `policy_call` escalation, not by deliberate workflow choice.

**Diagnosis:** the two-pass shape has THREE structural advantages even when not forced by a `policy_call`:
  1. **Context-locality**: pass-1 worker focuses on cherry-pick + Tier 1/2/3; pass-2 worker focuses on test design + evidence pair. Neither reads the other's mechanics.
  2. **Re-dispatchability**: if pass-2's test fails (pre-patch passes, or post-patch fails), the parent can redispatch pass-2 alone with a new test design. Pass-1's work is durable.
  3. **Parent-control checkpoint**: between passes, the parent can review the dossier and the staged source change, decide whether to proceed, and craft a test design without time pressure.

The cost: total wall-clock is roughly 1.5x to 2x a one-pass dispatch. For patches where the test design is obvious from the patch surface (stateless, single-table, single-trigger), the cost outweighs the benefits. For patches where the test design requires careful thought (integration, multi-mechanism-layer, environmental-coupling), the benefits dominate.

**Decision:** document the two-pass shape in `docs/aiven/runbooks/integration-tests.md §7 Patterns` as test-shape C ("Decoupled cherry-pick / test-authoring dispatch"). Already landed in the patterns-doc commit (`54d219cadab`). Mark as 1 of 3 — codify trigger conditions ("when to choose two-pass") after a second deliberate two-pass dispatch.

**Rule-of-three count:** 1 of 3. First two-pass dispatch in the T3.x series. Track for next dispatch where the parent preflight identifies a similar non-trivial test design (likely candidates: future integration-test patches, patches with environmental coupling like Keeper feature flags).

### C. `test_aiven_<slug>/` convention rule-of-three: 2 of 3 (WORKING)

**Symptom (positive):** the convention introduced in T3.6 patch 006 (`test_aiven_replicated_database_attach_with_shard_macro/`) generalized cleanly to T3.9 patch 005 (`test_aiven_zk_connect_retry/`). Two distinct test shapes (single-node DDL trigger vs. multi-node ZK-kill cluster) under the same naming convention.

**Diagnosis:** the convention has three properties that held under both applications:
  - **Prefix collision-avoidance**: `test_aiven_` doesn't collide with upstream `test_keeper_*/`, `test_inserts_*/`, `test_replicated_*/`. Future Aiven-specific tests will continue to populate this namespace.
  - **Slug-matching**: the test directory slug matches the patch dossier slug for cross-referencing (`grep -r 'zk_connect_retry' tests/integration/ docs/aiven/patches/` produces both files in one query).
  - **Ownership signal**: a maintainer scanning `ls tests/integration/` immediately identifies Aiven-only tests by the `test_aiven_*` prefix.

**Decision:** keep PROVISIONAL at 2 of 3. Promote to VERIFIED after the third occurrence. The next integration-test patch is the calibration slot.

**Rule-of-three count:** 2 of 3. T3.6 patch 006 = 1, T3.9 patch 005 = 2. (T3.14 patch 049 = 3 — but it's covered in retro 12 because the count is per-application chronologically.)

### D. Hook v3 third regression: first manifestation (n=1 of REGRESSION)

**Symptom:** the `log.md` row at `2026-05-27T10:26:31Z` populated with `unknown / unknown / unknown / unknown / unknown / n/a` mid-T3.9-runtime (~34 minutes before T3.9's actual completion). The worker was still running. The row was discarded by parent inspection at end-of-session. The T3.9 actual completion (~11:00 UTC) never produced an auto-row at all — log.md gained NO new row for T3.9's completion. The T3.8 worker (cherry-pick + dossier dispatch, completed ~09:37 UTC) ALSO never produced an auto-row.

**Diagnosis:** the symptom is the **third regression** of the `subagentStop` hook, after the v1 → v2 transition (T3.6 Finding G) and the v2 → v3 refinement (T3.7 Finding B). Diagnosis-at-the-time was incomplete — the parent verified the hook script was healthy under synthetic input (correct YAML extraction from on-disk JSONLs `53d78719*.jsonl` + `cdeba357*.jsonl`) but could not explain why the hook fired with an `unknown / unknown` shape on `2026-05-27T10:26:31Z` (mid-runtime, not at completion).

**Resolution (deferred to subsequent dispatches):** the probe block landed in `acb4d88fc70` to capture raw `$input` on subsequent hook fires. Six probe captures (T3.10 through T3.15) confirmed that **Cursor never sends the assistant transcript content in the `subagentStop` hook input** (`message_count: 0` in every captured payload). The v3 JSONL-fallback is therefore the permanent path, and intermittent `unknown / unknown` rows correspond to hook fires where the JSONL was not fully flushed. Full diagnostic chain consolidated in retro 14.

**Decision:** the T3.9 archive (and the T3.8 archive) were backfilled offline from the on-disk JSONLs. The log.md row for T3.9 completion is filled in retroactively. Bootstrap retro covers the regression as a single thread (retro 14).

**Rule-of-three count (regression occurrences):** 1 of 3 in this T3.x sub-thread (T3.7's v3 fix was the 0th — that's the calibration moment, not a regression instance under v3). T3.9 = 1st instance under v3. T3.10/T3.11/T3.12 + T3.13/T3.14/T3.15 added more — see retro 14.

## Forward decisions for T3.10+

- **`policy_call` escalation type**: document in `docs/aiven/schema/halt-and-escalate.md` opportunistically. Codify trigger conditions after second occurrence.
- **Decoupled cherry-pick / test-authoring**: documented in `integration-tests.md §7 test-shape C`. Mark PROVISIONAL. Don't force two-pass dispatch for one-pass-suitable patches.
- **`test_aiven_<slug>/` convention**: at 2/3. Keep using; calibrate on T3.14's third application.
- **Hook v3 third regression**: probe block landing is the next step. Track in retro 14.

## Learning log

**Today you learned:** integration tests are the *resolution path* for patches whose defended behavior is gated by environmental coupling that the stateless runner cannot provide. The mental model: stateless tests assume a single ZK + a single CH server + a single database; if your patch defends a behavior that requires any deviation from that triple (multi-node, ZK-kill, server-restart, version-asymmetric replication), the test belongs in `tests/integration/test_aiven_<slug>/`.

**Rule of thumb:** when designing a test for a patch, ask: "is the defended behavior reachable from a single SQL session against a single CH server with a single shared ZK?" If yes → stateless `9NNN_*.sql`. If no → integration `test_aiven_<slug>/`. The decision is binary and should be made at parent preflight, before the worker dispatch — not mid-worker (otherwise you get a `policy_call` escalation).

**Next rabbit hole:** the §7 patterns in `integration-tests.md` are at n=1 (each pattern observed once). The natural calibration cycle is: each pattern needs to be applied to a SECOND patch before it can be considered durable. Watch for the second application of test-shape B (mechanism-isolation by config-tuning) and test-shape C (decoupled dispatch). The third application promotes them to VERIFIED-with-discipline.

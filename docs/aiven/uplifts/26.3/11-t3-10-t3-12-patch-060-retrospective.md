# T3.10 / T3.11 / T3.12 — patch 060 ship-without-test saga retrospective

> **What this is:** The reflection-after-cost step (spec §10 step 12) applied to the **three-dispatch verification saga for patch 060** ("Fix alter order by", `93c2be960f`, Tilman Moeller + Aliaksei Khatskevich, 2026-01-13). T3.10 was the initial port + first stateless-test attempt; T3.11 was the parent-redesigned `ADD COLUMN + MODIFY ORDER BY` retry; T3.12 was the parent-redesigned `RENAME COLUMN` retry. All three test attempts FAILED to produce evidence-of-causation on 26.3 — each blocked by a different upstream sanity-check gate or by a wider-scope `AlterCommands::apply` divergence that defeats the patch's predicate. The eventual outcome was the **first ship-without-test no-regression port** in the T3.x series.
> **Date:** 2026-05-27 through 2026-05-28.
> **Dispatch references:**
>   - T3.10: `log.md` row at `2026-05-27T13:24Z`; on-disk JSONL `fb19f8f6-52fe-4df5-8e5b-c2d9b0c37122.jsonl`. Outcome: `test_design_blocked`.
>   - T3.11: `log.md` row at `2026-05-28T09:15Z`; on-disk JSONL `d9c8c3a6-8501-492b-805e-59c1311dc0ed.jsonl`; report at `docs/aiven/uplifts/26.3/reports/T3.11-postpatch-fail.md`. Outcome: `postpatch_fail`.
>   - T3.12: `log.md` row at `2026-05-28T11:16Z`; on-disk JSONL `8663995e-c9b8-453b-89b3-2c729baeefc3.jsonl`; report at `docs/aiven/uplifts/26.3/reports/T3.12-postpatch-fail.md`. Outcome: `postpatch_fail`.
> **Outcome of the saga:** `success-without-test`. Two files committed in `492866d292b`: `src/Storages/StorageReplicatedMergeTree.cpp` (+2 / -1 line) and `docs/aiven/patches/060-alter-order-by-sorting-key-zk-metadata.md` (new dossier, 198 lines). Tier 1 cherry-pick: clean auto-merge; the staged diff is byte-equivalent to the source patch (Tier 2: `byte_equivalent: true`, patch-id `27037b121ff8df19a7d61bcd20cfed16a53364b2`). Tier 3 evidence pair: **NOT PRODUCED** on 26.3 by any of the three attempts. Substitute audit trail: the dossier's §4 "No-regression proof vs 25.8" argument + the two `T3.1{1,2}-postpatch-fail.md` reports.

## Headline

Patch 060 is the **first patch in the T3.x series shipped without a test file**. The decision is justified by a three-part no-regression argument:
  1. The cherry-pick is byte-equivalent to the 25.8 source (patch-id match).
  2. The ±5 context lines around the patched site are byte-identical to 25.8.
  3. The wider-scope `AlterCommands::apply` MODIFY_ORDER_BY divergence (at `src/Storages/AlterCommands.cpp:653-666`) that defeats the patch's predicate on the reachable code paths has been in upstream code since **2020-06-12** (commit `465c4b65b72`), ~5.5 years before the source patch was authored. **The patch was observationally inert on direct `MODIFY ORDER BY` on 25.8 too** — whatever fix-scenario it has, if any, lives on an ALTER path neither LTS's verification attempts reach.

The saga is the first instance of **three iterative test redesigns** in T3.x, each hitting a DIFFERENT upstream sanity-check gate. It established the empirical foundation for the `(iv) reachability` discipline introduced in T3.13. Without the three-attempt sequence on patch 060, T3.13's escalation would have been a one-off observation; with it, T3.13 became the second case where "the patched code is on HEAD but the test trigger doesn't reach it" — and the discipline crystallized.

**Three rule-of-three counters surfaced.** "Three test redesigns for one patch" reached **n=1 of 3** for the productive-failure pattern. "Ship-without-test no-regression port" reached **n=1 of 3** as a documented outcome class. "Upstream-history archaeology as no-regression substantiation" reached **n=1 of 3** as a parent investigation pattern.

## What worked

1. **Worker honored "escalate on postpatch_fail rather than commit a green-but-meaningless test."** Both T3.11 and T3.12 hit gates that would have allowed a trivially-passing test — T3.11 could have shipped a test where pre and post both read `actual=1` and called it green if the worker had assumed "post equals reference"; T3.12's RENAME-COLUMN scenario produces a tautological pass (no `sorting_key.definition_ast` mutation; both LTSes write `0`). In each case the worker emitted a structured `postpatch_fail` escalation with a full root-cause analysis instead.
2. **Two `T3.1{1,2}-postpatch-fail.md` reports are durable institutional knowledge.** Each is a ~200-line worker-authored diagnostic that names the gate (`MergeTreeData::checkProperties:1010-1014` for T3.10/T3.11 column-newness; `AlterCommands.cpp:653-666` for T3.11 predicate defeat; `MergeTreeData::checkAlterIsPossible:4445-4452` for T3.12 RENAME rejection), cites the source-code line numbers, and shows the literal pre/post test output. Future maintainers debugging "why doesn't ALTER ORDER BY produce a metadata-shape diff in stateless tests?" can read these reports and get the answer in 20 minutes.
3. **Upstream-history archaeology produced the load-bearing finding.** Parent's investigation between T3.11 and T3.12 used `git log -L 653,666:src/Storages/AlterCommands.cpp` to trace the MODIFY_ORDER_BY branch's current shape back to commit `465c4b65b72` on 2020-06-12, with a cosmetic comment-only follow-up in `2c9ce0f3fa4` on 2020-06-15. This **5.5-year prior** establishes that the patch was equally inert on the 25.8 baseline. The no-regression argument relies entirely on this finding — without it, "ship without test" would have been an act of faith.
4. **The dossier captures the saga as audit trail.** §4 of `docs/aiven/patches/060-alter-order-by-sorting-key-zk-metadata.md` contains the verification record (all three SQL bodies + escalation outcomes), the no-regression argument, and a §7 "Follow-ups" listing unexplored candidates for the patch's actual fix-scenario (none currently promising; deferred until concrete production divergence). The dossier is the substitute for the evidence pair we couldn't produce.
5. **`(i)/(ii)/(iii)` discipline correctly predicted "still-needed-and-applies."** Parent's preflight at T3.10 wrote out the three checks; (i) and (ii) passed (byte-stable patched line + context); (iii) passed (identifiers `isPrimaryKeyDefined`, `ReplicatedMergeTreeTableMetadata`, `ast_to_str`, `future_metadata_in_zk` all present on HEAD). The dispatch went ahead correctly. The discipline did NOT predict the gate-blocked test design — that's the dimension the T3.13 escalation later named `(iv) reachability`.

## What surfaced

### A. `(i)/(ii)/(iii)` discipline is insufficient — a fourth check is needed (n=0 of 3, the discovery moment)

**Symptom:** the parent's `(i)/(ii)/(iii)` discipline was applied correctly at T3.10 preflight. All three checks passed. The patch's source change applied byte-equivalently. AND the test design failed to reach the patched code three times — once at T3.10 (initial `MODIFY ORDER BY` rejected by `checkProperties`), once at T3.11 (the test reached `StorageReplicatedMergeTree::alter` but the patched predicate evaluated to `true` due to a wider-scope divergence in `AlterCommands::apply`), once at T3.12 (the alternative trigger rejected by `checkAlterIsPossible`).

**Diagnosis:** the existing `(i)/(ii)/(iii)` discipline answers "is the patched code on HEAD and applicable?" but not "does the test trigger reach the patched code?". These are independent questions:
  - (i)/(ii)/(iii) = state-of-the-code question. The patch is APPLICABLE on HEAD.
  - (iv) = state-of-the-test question. The test design REACHES the patched line.
  
  Patch 060 demonstrates that (iv) can fail in three structurally-distinct ways:
    - (iv-a) Upstream sanity check rejects the trigger before reaching the patched code (T3.10's `checkProperties`).
    - (iv-b) Wider-scope code change defeats the patch's predicate at the patched site (T3.11's `AlterCommands::apply` divergence).
    - (iv-c) The chosen alternative trigger is statically un-usable as a differential (T3.12's RENAME COLUMN of a non-sort column producing a tautological pass).

**Decision (deferred to T3.13):** the patch-060 saga did NOT immediately produce a `(iv) reachability` discipline. The three failure modes were diagnosed inline in the dossier and the worker reports. **T3.13's escalation, which independently named the same failure mode on a different patch, was the second observation that crystallized the discipline.** Retro 12 documents the codification step.

**Rule-of-three count (the discovery is n=0 of 3 — this is the calibration moment, like T3.7 Finding A's discovery of the prose-vs-column contradiction):** patch 060 = 0/3 (PROBLEM discovered); T3.13 patch 049 = 1/3 (SAME PROBLEM observed second time, crystallized into the discipline name); T3.15 patch 042 = 2/3 (DISCIPLINE applied proactively first time). T3.x next is the third proactive use.

### B. Ship-without-test no-regression port as a first-class outcome (n=1 of 3 WORKING)

**Symptom:** after three failed test attempts, the parent chose to ship the source change without a regression test. This is a new outcome class — distinct from "ported with test" (the default for T3.1-T3.7, T3.9), "ported without test (no_trigger_on_current_lts)" (T3.x earlier hypothesized but not used), and "dropped (obsoleted-by-upstream)" (T3.8 patch 001).

**Diagnosis:** ship-without-test no-regression is justified ONLY when three conditions are met:
  1. The source change is byte-equivalent (Tier 2 PASS).
  2. The ±5 context lines around the patched site are byte-stable across LTSes.
  3. There exists a wider-scope drift finding that establishes the patch was observationally inert on the source LTS too on the reachable code paths, so shipping introduces no observable behavioral regression.

The third condition is the load-bearing one and requires upstream-history archaeology (see Finding C below). It is NOT sufficient to say "we couldn't find a working test" — that would justify dropping or deferring, not shipping. The substantive argument is "the patch's reachable behavior on this LTS is provably-identical to its reachable behavior on the prior LTS."

**Decision:** document the outcome class in the dispatch-prompt template at `docs/aiven/skills/dispatch-prompt-template.md` "Outcomes" section. Mark as PROVISIONAL pending second occurrence. The likely codification target is a new sub-section under §4 Test design: "Outcome E: ship-without-test no-regression port (when (a)+(b)+(c) hold)."

**Rule-of-three count:** 1 of 3. Track for a second patch that exhibits the same three conditions. If a third occurs, codify as a standard outcome.

### C. Upstream-history archaeology as no-regression substantiation (n=1 of 3 WORKING)

**Symptom:** the load-bearing finding for patch 060's no-regression argument was a `git log -L 653,666:src/Storages/AlterCommands.cpp` invocation that traced the MODIFY_ORDER_BY branch back to commit `465c4b65b72` on 2020-06-12. This was discovered as part of parent's investigation between T3.11 and T3.12, NOT during T3.10 preflight.

**Diagnosis:** the `(i)/(ii)/(iii)` discipline scans for upstream commits matching topical greps (`sorting_key`, `isPrimaryKeyDefined`, `alter.*metadata`) within the patched file. It does NOT scan for upstream changes to **other** files that interact with the patch's predicate. The wider-scope drift in `AlterCommands.cpp` was missed at T3.10 preflight because the patch doesn't touch that file and `KeyDescription::getKeyFromAST` isn't an identifier in the patch text.

**Productive failure shape:** the iterative test attempts FORCED the parent to investigate the call-chain end-to-end. T3.11's `postpatch_fail` was the trigger for the deep `git log -L` archaeology that surfaced the 5.5-year-prior divergence. Without T3.11's failure, the no-regression argument would not have been constructed and the patch would have been dropped (or shipped on faith).

**Decision:** add a "Wider-scope drift" sub-check to the dispatch-prompt template's parent preflight. Specifically: after (iii) identifier inventory, add (iii-b) "for the patched predicate / function, identify the upstream caller and verify the caller's behavior hasn't drifted in a way that defeats the predicate's invariant." This is a defensive check against the (iv-b) failure mode (predicate-defeated by wider-scope change).

Defer codification until a second patch exhibits the same wider-scope drift defeat. Track as 1 of 3.

**Rule-of-three count:** 1 of 3.

### D. Three-attempt iterative test redesign as a productive-failure pattern (n=1 of 3)

**Symptom:** the saga consumed three worker dispatches + ~30 minutes of parent investigation between attempts (cumulative). Total wall-clock: ~75 minutes. Net output: source change + dossier + ship-without-test classification + no-regression argument. NO test file shipped.

**Diagnosis:** the cost-benefit of three attempts is favorable IF the wider-scope drift it surfaces is institutional knowledge that prevents future analogous patches from going wrong. Patch 060's saga produced:
  - The `(iv) reachability` discipline (codified in T3.13 escalation, retro 12).
  - The "ship-without-test no-regression port" outcome class (this retro, Finding B).
  - The "upstream-history archaeology" check (Finding C).
  - Two durable T3.11 + T3.12 worker reports as institutional knowledge.

If the saga had succeeded on T3.10's first attempt, none of these would have surfaced. The "iterative redesign as productive failure" pattern is therefore valuable — but only when the redesigns lead to substantive findings, not when they thrash on the same root cause.

**Decision:** when a worker escalates `postpatch_fail` or `test_design_blocked`, the parent's redesign attempt SHOULD be guided by an upstream-history scan of the gate that blocked the test. If the second attempt fails on a structurally-different gate, escalate to "investigation pause" — go deep on the call-chain history BEFORE the third attempt. Track this guidance as 1 of 3.

**Rule-of-three count:** 1 of 3 (n=1 productive failure with substantive output).

### E. Three different gate-keepers in `MergeTreeData` block "natural" ALTER ORDER BY tests

**Symptom:** each of the three test attempts hit a DIFFERENT upstream gate:
  - T3.10: `MergeTreeData::checkProperties:1010-1014` — rejects MODIFY ORDER BY referencing a pre-existing non-new column. Code 36 BAD_ARGUMENTS.
  - T3.11: `AlterCommands::apply:653-666` — the patch's predicate `future_metadata.isPrimaryKeyDefined()` evaluates to true post-MODIFY_ORDER_BY because the apply step writes `primary_key.definition_ast` without nulling it.
  - T3.12: `MergeTreeData::checkAlterIsPossible:4445-4452` — rejects RENAME COLUMN on a sort-key column. Code 524 ALTER_OF_COLUMN_IS_FORBIDDEN.

**Diagnosis:** the `ALTER ORDER BY` surface in `MergeTree` is heavily defended against accidental mis-use. Tests that want to exercise the metadata-shape divergence the patch fixes need either (a) a non-standard trigger (e.g., `MODIFY COLUMN <type>` where the new type is permitted; `DROP COLUMN` of a non-sort column whose sort expression references it indirectly; materialized-view / projection DDL on `Replicated*MergeTree`), or (b) an integration-shaped test that constructs the ZK znode divergence by direct manipulation pre-patch and asserts post-patch ALTER no longer rewrites the line.

**Decision:** document the gate-keeper map in the patch dossier's §7 "Follow-ups." Re-open only on concrete production evidence. This is **the canonical example of a patch whose fix-scenario is genuinely difficult to construct as a test** — a useful counter-example for "all patches should have a test."

**Rule-of-three count:** 1 of 3 (n=1 patch with a genuinely-difficult fix-scenario).

## Forward decisions for T3.13+

- **`(iv) reachability` discipline**: this saga is the n=0 (PROBLEM discovery). T3.13 is the n=1 (DISCIPLINE-CRYSTALLIZED moment). Retro 12 documents the codification.
- **Ship-without-test no-regression port**: documented in this retro as outcome class. Mark PROVISIONAL. Add to dispatch-prompt template after second occurrence.
- **Upstream-history archaeology as a (iii-b) check**: not yet codified. Track for next wider-scope-drift-defeat. Defer to n=3.
- **Iterative test redesign as productive failure**: n=1. The parent's heuristic — "if second attempt hits a structurally-different gate, pause and go deep on call-chain history before third attempt" — is documented here and applied implicitly in subsequent dispatches.

## Learning log

**Today you learned:** the existence of a patch on HEAD (the question `(i)/(ii)/(iii)` answers) is not the same as the reachability of the patch's behavior from a stateless test trigger (the question `(iv)` answers). A patch can be byte-equivalent + still-needed-and-applies AND yet have NO reachable code path on the source LTS either — in which case shipping it is a no-regression port, not a fix. The substantive question to ask before any verification dispatch is: "What is the test trigger? What is the call-chain from trigger to patched line? What gates are between them on this LTS?"

**Rule of thumb:** when a worker escalates `postpatch_fail`, do NOT immediately redesign the test. INVESTIGATE the wider call-chain first. `git log -L <range>:<file>` on every function between the trigger and the patched line. If you find a wider-scope divergence that defeats the patch's invariant, you have a no-regression argument (the patch was equally inert on the prior LTS). If you don't, redesign the test with a different trigger.

**Next rabbit hole:** patch 060's §7 "Follow-ups" lists three unexplored candidates for the patch's actual fix-scenario. Re-open if a future production observation surfaces metadata-shape divergence between CREATE-shaped and ALTER-shaped ZK metadata that affects replica join behavior. Until then, the patch is a documented no-op on the reachable paths — a useful institutional fact, not a regression.

# T3.7 — patch 010 dispatch retrospective

> **What this is:** The reflection-after-cost step (spec §10 step 12) applied to the seventh real patch dispatch — the smallest patch in the inventory (1 file / 1 line), the first dispatch since the T3.6 Finding G hook fix landed, the first dispatch consuming the dispatch-prompt template at v1 + parent-preflight refinement, and the first dispatch where a parent-preflight prior was empirically contradicted by the worker without compromising the dispatch outcome.
> **Date:** 2026-05-26.
> **Dispatch reference:** `docs/aiven/uplifts/26.3/log.md` row at `2026-05-26T15:22:38Z`; subagent id `toolu_01FgRHtjZGkmh4h6Z7DZaceE`; report archive `docs/aiven/uplifts/26.3/reports/toolu_01FgRHtjZGkmh4h6Z7DZaceE.md` (28 KB; auto-archived by the fixed hook).
> **Outcome of T3.7:** `success` (worker). Four files staged for human commit: `src/Databases/DatabaseReplicatedSettings.cpp` (+1 / −1), `tests/queries/0_stateless/9010_default_logs_to_keep.{sql,reference}` (new), `docs/aiven/patches/010-default-logs-to-keep.md` (new dossier, 251 lines). Tier 1 cherry-pick: clean after mechanical context-line resolution. Tier 2 patch-id: `byte_equivalent: false`; decomposition empty (only context-line shift). Tier 3 evidence pair: post-patch `OK`, pre-patch `FAIL` with `expected 300 / actual 1000`.

## Headline

T3.7 was the fastest dispatch yet end-to-end: worker wall-time ~10 minutes (two incremental builds ~80s combined, two test runs ~0.13s each, the rest dispatch overhead). Total parent + worker session time ~25 minutes including parent preflight (which surfaced a much larger finding — that patch 020 had an unported dependency 019 — and required pivoting the choice mid-session).

**The T3.6 Finding G hook fix is now empirically validated on a real patch dispatch.** The `log.md` row at `2026-05-26T15:22:38Z` populated all eight columns (`slug=default-logs-to-keep`, `outcome=success`, `escalation_reason=none`, `report=[link]`) with the worker's report archived verbatim. No manual backfill. First 1-of-N for the WORKING mitigation; T3.6's `rule-of-three: 3 of 3 → RESOLVED` is now ground-truthed with 1 of N empirical confirmations.

**Two parent-preflight discipline findings surfaced** (Findings A and B) — both pre-existing dispatch-prompt template authorship hazards that T3.7's small surface made unusually visible. Both are now actionable for T3.8.

## What worked in T3.7

1. **Preflight caught the patch-020 dependency BEFORE dispatch.** The original session opener proposed patch 020 (`Add CHECK TABLE to default privileges`) as the next target. The parent's first preflight action was to inspect the source SHA against the current 26.3 dev tree and immediately found that patch 020's hunk-context (the `GRANT DEFAULT REPLICATED DATABASE PRIVILEGES` privilege list) didn't exist in the current tree because its dependency, patch 019 (`05d8148a57`, 14 files / 287 LOC), is unported. Parent surfaced this to the human BEFORE drafting the dispatch prompt, presented three alternatives via structured question, the human picked the cheapest (patch 010), and the session proceeded without wasting worker context on an impossible cherry-pick. This is the dispatch-prompt template §5 "parent preflight" working as designed — and the catch generalizes: every cheap-looking patch may have a load-bearing dependency that's not obvious from the inventory subject line alone.
2. **The hook fix is empirically working in production.** The `subagentStop` hook auto-populated `log.md` with all 8 columns correctly (`completed | default-logs-to-keep | general-purpose | toolu_01FgRHtjZGkmh4h6Z7DZaceE | success | none | [report]`) and archived the 28 KB report to `reports/<subagent_id>.md`. First real-patch validation of the path-resolution + all-turn-concatenation fix from T3.6 Finding G. The parent did NOT have to backfill anything. The fix is forward-compatible (preferred-branch logic) and resilient (all-turn join survives YAML-in-non-last-turn outputs).
3. **The dispatch-prompt template (v1) produced a 606-line prompt in ~20 minutes.** Reuse from the patch-040 backbone + template substitution + patch-specific Test design block. The worker had everything it needed in one read; the prompt's §5 ("Test design — FIXED, implement verbatim") prevented redesign and produced a passing test on first try.
4. **The single-line patch produced an honest pre/post evidence pair.** `system.zookeeper` SELECT pattern works perfectly for setting-default-value triggers. Reference: `300\n` (post-patch); pre-patch FAIL line: `expected 300 / actual 1000`. This pattern is now the canonical recipe for "Aiven default-value tuning" patches and should be documented in `runbooks/testing-suites.md` as a recurring shape (see Finding D).
5. **Worker honored "command literals first" implicitly.** Worker emitted the `proposed_commit.commit_message` verbatim in YAML, including the `Original author: Tilman Moeller <tilman.moeller@aiven.io>, 2025-12-08.` line, the `Co-authored-by: Khatskevich <khatskevich@aiven.io>` trailer, and the `(cherry picked from commit ...)` line. The human's commit will copy-paste this block directly. T3.5's mitigation (Findings B/C/G) is now at **2-of-N for the WORKING mitigation** (1-of-N was T3.6); track rule-of-three for the working pattern.
6. **Hook archive is searchable and durable.** The 28 KB report at `reports/toolu_01FgRHtjZGkmh4h6Z7DZaceE.md` contains the worker's full halt-and-escalate block, the tier results, the evidence excerpts (cherry-pick log, post/pre-test logs, worktree-restoration verification, final-staged-state snapshot), and the worker's narrative. Future retrospectives or audit questions ("what did the T3.7 worker actually do?") can be answered by reading one file.

## What surfaced

### A. Parent preflight prior contradicted by worker on cherry-pick cleanliness

**Symptom:** parent's preflight prior in the dispatch prompt (lines 28-32) asserted: "**5 commits touched the file between 25.8 and 26.3** ... but NONE of them change the default value `1000`. **Hunk should apply cleanly.**" The inventory's `cherry_pick_clean=no` column was acknowledged in the patch-specific context table (line 138) but the prose narrative overrode it. Worker hit a cherry-pick conflict on the first attempt at Step 2 — `git cherry-pick --no-commit -x` reported `CONFLICT (content): Merge conflict in src/Databases/DatabaseReplicatedSettings.cpp` and exited non-zero.

**Diagnosis:** the conflict was **mechanical**, not semantic. Commits `956465b4691` (adds three `default_replica_{path,shard_name,name}` settings) and `4eeadb9c352` (adds `internal_replication`) inserted four unrelated `DECLARE` lines between `logs_to_keep` and the source diff's lower context anchor `DECLARE_SETTINGS_TRAITS`. The patched line itself was unambiguous (`-1000` / `+300` on the same DECLARE), so the resolution was: accept HEAD's structure verbatim, change only the literal `1000` → `300` on the `logs_to_keep` line. Worker validated this by running Tier 2 patch-id decomposition (`+`/`-` lines byte-identical to source); the decomposition was empty, confirming the resolution was the unique mechanically-correct one.

The parent's prose interpretation slipped two adjacent concepts:
- "**No upstream commit moved the default value**" — TRUE; this is a semantic claim about whether the patch is still meaningful.
- "**Hunk should apply cleanly**" — FALSE; this is a textual claim about whether `git apply` accepts the diff as-is. The inventory's `cherry_pick_clean=no` was the correct textual claim and the parent ignored it.

These are independent: a patch can be semantically still-needed yet textually conflict-prone whenever neighboring lines drift in the context window.

**Decision:** add to `docs/aiven/skills/dispatch-prompt-template.md` "Parent preflight checklist" a discipline-three-way check:

  - **(i) Patched-line stability** — is the line(s) the patch targets unchanged on HEAD? (Worker's `1d. Hunk-line verification` step.)
  - **(ii) Context-window stability** — within the patch's `@@ -N,M +N',M' @@` window, are any neighboring lines on HEAD different from the source patch's context? Run `diff <(git show <sha>:<file> | sed -n 'N-3,N+3p') <(sed -n 'N-3,N+3p' <file>)` — non-empty output means context drift, expect a cherry-pick conflict.
  - **(iii) Semantic-equivalence on HEAD** — does the change still mean what it meant on the source branch? (Worker's drift-superseded check, when applicable.)

(i) + (ii) together predict whether cherry-pick goes clean; (iii) predicts whether it should be carried at all. The dispatch-prompt template's preflight prose should explicitly answer all three, not collapse (i)+(ii) into "should apply cleanly" or "shouldn't apply cleanly" based on the inventory column alone.

**Rule-of-three count: 1 of 3.** First explicit case where the parent's prose preflight contradicted the inventory's `cherry_pick_clean` column. Earlier dispatches (T3.2 with `cherry_pick_clean=yes`, T3.3 with `=yes`, T3.4 with `=yes` and conflict-on-Site-2, T3.5 with `=no` and clean cherry-pick) had not hit this specific calibration error in the prose-vs-column tension. Defer the dispatch-prompt-template addition to a Bootstrap commit unless a second occurrence happens before T3.10. In the meantime, T3.8 parent preflight should explicitly write out (i)/(ii)/(iii) as a discipline.

### B. Hook regression resolution validated empirically — and refined in-session

**Symptom (positive — what we wanted to see):** the `log.md` row for `2026-05-26T15:22:38Z` populated with `slug=default-logs-to-keep`, `subagent_type=general-purpose`, `subagent_id=toolu_01FgRHtjZGkmh4h6Z7DZaceE`, `outcome=success`, `escalation_reason=none`, `report=reports/toolu_01FgRHtjZGkmh4h6Z7DZaceE.md` — all 8 columns correct. Path-resolution fallback worked (`.agent_transcript_path` returned `null`; hook derived the JSONL via `dirname(.transcript_path)/subagents/<youngest>.jsonl`). All-turn YAML extraction worked (the YAML lived in turn 58 of 59; the awk parser found it via column-0 anchors).

**Symptom (negative — surfaced by post-dispatch inspection):** the archived report file `reports/toolu_01FgRHtjZGkmh4h6Z7DZaceE.md` came in at **354 lines, with the `outcome:` YAML line at line 170** — meaning **169 lines of worker thinking-prose preceded the actual report**. Compare T3.4's archive `toolu_01LpNLkS2cPr4HQjgmvDdTKf.md`: 178 lines total, `outcome:` at line 4. The T3.7 hook fix captured the YAML correctly (good) AND every preceding "I need to read…", "Let me check…", "All required reading is complete." narration turn (bad).

**Diagnosis:** the T3.6 v2 fix concatenated *every* assistant text block to guarantee YAML capture wherever the worker placed it. T3.6's synthetic verification probes happened to be terse (the synthetic worker emitted only the YAML); T3.7's real worker emitted 58 short narration turns plus turn 58 with the 184-line YAML report. The all-turn join captured the union. The awk parser still extracted the correct outcome/slug/reason (column-0 anchors are specific enough) — but the archive's *readability* collapsed: a reader opening the file to audit the dispatch had to scroll past 169 lines of intermediate planning before reaching the structured report.

**Decision (landed in this same session, hook v3):** filter to turns containing the column-0 YAML anchors (`(^|\n)outcome:|(^|\n)patch_slug:|(^|\n)escalation_reason:`) — the SAME anchors the awk parser already uses, so the filter cannot drop a turn the parser would have read. Fall back to all-turn join only when no turn carries the YAML (read-only subagents like `explore` / `cursorGuide`, or aborted runs).

The fix is forward-compatible: if a future worker splits the YAML across two turns (e.g., `commit_message` body in one turn, the rest in another), both turns carry column-0 anchors, both get selected, joined with a blank line; awk reads the first occurrence of each anchor and still produces the right outcome/slug/reason. The fix is also backward-compatible: terse workers (T3.4-style) match exactly one YAML-bearing turn and produce identical output to v1 (last-turn-only).

T3.7's archive was re-extracted in-session from the on-disk worker JSONL using the v3 filter; new size 187 lines (vs 354 lines pre-fix; the saved 167 lines were pure worker-narration noise). `outcome:` now sits at line 4 (matches T3.4 precedent).

**Forward signpost:** the hook now carries a v1/v2/v3 history comment block above the jq filter. If a v4 is ever needed (e.g., Cursor changes the JSON shape again, or workers find a new way to hide YAML), the rationale chain is in the source.

**Rule-of-three counts:**
- **Hook fix v3 (current WORKING archive scheme):** 1 of 3. T3.7 is the calibration point; T3.8 / T3.9 are the validation slots. If three consecutive archives come in with `outcome:` at line ~4 and no prose-noise leakage, the v3 mitigation is GA-class.
- **Hook fix v2 → v3 calibration was triggered by synthetic-vs-real-worker divergence.** Synthetic probes were terse; real workers narrate. This is the second time (T3.6 was the first) that real-patch dispatch surfaced a hook defect that synthetic verification missed. **Lesson: every real dispatch's archive should be eyeballed for prose-noise before committing.** Defer codifying this as a parent-checklist item until the third occurrence.

### C. Patch 020 has unported dependency (patch 019); inventory subject-line scan is insufficient

**Symptom:** session opener proposed patch 020 (`Add CHECK TABLE to default privileges`) as the next dispatch. Parent's preflight immediately found: patch 020's hunk targets a privilege list block in `src/Interpreters/Access/InterpreterGrantQuery.cpp` line 533 (`OPTIMIZE, ... SELECT, ... SHOW, ...`). This block does NOT exist in the current 26.3 dev tree — it's introduced by patch 019 (`Allow avnadmin creating database using sql`, 14 files / 287 LOC), which is unported. Patch 020 alone would land its `+ "CHECK, "` change in empty context (the SQL syntax it enhances doesn't exist).

**Diagnosis:** the inventory has columns for `files_count`, `loc_changed`, `cherry_pick_clean`, and `subject` but no column for `depends_on`. The proposal heuristic used by the parent during the prior session-close — "small patches are cheap to dispatch" — does not account for dependency chains. Patch 020 is the 1-LOC tail of a 288-LOC two-patch logical unit.

**Decision:** at next-patch proposal time, the parent's heuristic must include a quick check: search for the patch's diff context on the current tree (`rg '<context line>' src/`), and if absent, scan adjacent inventory rows by topic/file/author/date for the likely dependency. This adds ~2 minutes to per-patch selection. Document in `docs/aiven/skills/dispatch-prompt-template.md` "How to use" section as a new step 0 ("dependency sanity check") before the existing steps 1-9.

Longer-term improvement: extend the inventory schema with a `depends_on` column. Patches whose `cherry_pick_clean=no` plus topical alignment with adjacent rows are candidates. Defer the inventory schema change to a Bootstrap commit; it requires running the T2 classifier with an additional pass.

**Rule-of-three count: 1 of 3.** First explicit dependency-chain miss in T3.x proposals. Earlier dispatches happened to pick independently-scopable patches. Defer schema change; track for T3.8/T3.9.

### D. `system.zookeeper` SELECT as canonical Aiven-default-value test pattern

**Symptom:** patch 010 is the smallest patch in the inventory: changing a single literal default value in a settings struct. The test design parent + worker converged on was a clean SQL pattern that should generalize:

```sql
-- Tags: no-parallel

DROP DATABASE IF EXISTS replicated_<NNN>_<slug>;
CREATE DATABASE replicated_<NNN>_<slug>
  ENGINE = Replicated('/test/aiven_<NNN>/' || currentDatabase() || '/replicated_<NNN>_<slug>', 'shard_1', 'replica_1');
SELECT value FROM system.zookeeper
WHERE path = '/test/aiven_<NNN>/' || currentDatabase() || '/replicated_<NNN>_<slug>'
  AND name = '<setting_name>';
DROP DATABASE replicated_<NNN>_<slug>;
```

Reference is the new default's literal value. Pre-patch returns the OLD literal; post-patch returns the NEW literal. The pattern works because `DatabaseReplicated::createDatabaseNodesInZooKeeper` (in `src/Databases/DatabaseReplicated.cpp`) calls `zkutil::makeCreateRequest(zookeeper_path + "/<setting_name>", std::to_string(<setting_value>), ...)` — every DatabaseReplicatedSetting that lands as a child ZK node is observable this way.

**Diagnosis:** this is a reusable recipe. Aiven's tuning patches that change default values for `DatabaseReplicatedSetting` fields are common in the inventory; each one is a candidate for this pattern.

**Decision:** add this pattern to `docs/aiven/runbooks/testing-suites.md` as §3.X "Recipe — DatabaseReplicatedSetting default-value trigger via `system.zookeeper`". Document the load-bearing conditions: (a) the setting must land in ZK at CREATE time (true for all `DatabaseReplicatedSetting` fields that the C++ code writes via `makeCreateRequest`); (b) `no-parallel` is needed because of Replicated DB cluster-state interaction (mirrors `02710_*`); (c) the path namespace `'/test/aiven_<NNN>/' || currentDatabase()` is collision-free per-test; (d) the reference holds the POST-patch literal.

Defer the runbook addition to a Bootstrap commit on T3.8 or whenever the second similar-shape patch appears. **Rule-of-three count: 1 of 3.** Single use today; need two more confirming uses (or one more + a deliberate runbook codification).

### E. Throughput observation: T3.7 is a candidate calibration point

**Symptom:** session wall-clock from "user said pick 010" to "worker returned success" was approximately 25 minutes. Parent preflight ~15 minutes (including the patch-020 dependency-rejection detour), dispatch-prompt draft ~5 minutes, worker run ~5 minutes (much smaller than the 45-minute budget). The worker's full wall-time was dominated by two incremental builds (~80s combined) and dispatch overhead (file reads, tool round-trips).

**Diagnosis:** at the 1-LOC end of the patch-size spectrum, the dispatch is dominated by parent preflight + build time, not by LOC or test complexity. The 45-minute budget allocated to T3.7 in the dispatch prompt was 9x the actual worker time. The system is over-budgeted for small patches and (per T3.6) appropriately budgeted for restart-class integration tests.

The throughput math: 7 patches over ~6 working days = ~1.2 patches/day cadence. To complete remaining ~70 patches in 3 weeks (~15 working days), we'd need ~5 patches/day cadence — a 4x leverage gain over the current pace.

**Decision:** small-patch dispatches like T3.7 are an opportunity to **bundle** — dispatch multiple small independent patches to one worker per session. Defer concretely until the next patch family with 2-3 unbundled-but-trivially-independent SHAs (typical pattern: a typo fix + a comment update + a small constant change in unrelated files). Track for T3.8 selection. Do not bundle patches with cross-dependencies (T3.7's patch-020 finding shows the parent must verify independence first).

**Rule-of-three count: n/a.** Observation, not a failure mode.

## Concrete decisions for T3.8+

| Decision | Driver | Action |
|---|---|---|
| **Hook fix refined in-session from v2 to v3** — v3 prefers turns containing column-0 YAML anchors over all-turn concat. Confirmed by re-extracting T3.7's archive (354 → 187 lines; `outcome:` from line 170 → line 4). Track for 2 more dispatches (T3.8, T3.9) before claiming GA durability. | Finding B | Landed; watch T3.8/T3.9 archives for `outcome:` near top of file. If any archive surfaces prose-noise again, that's the v4 trigger. |
| **Parent preflight discipline (i/ii/iii)** — write out patched-line stability, context-window stability, semantic-equivalence as three independent checks in the dispatch prompt prose. Don't collapse them into "should apply cleanly". | Finding A | Mental discipline for T3.8; defer dispatch-prompt-template additions to a Bootstrap commit on second occurrence. |
| **Dependency sanity check at proposal time** — search for the patch's diff context on the current tree (`rg`) and scan adjacent inventory rows when the context is absent. | Finding C | Add to next-patch-selection workflow for T3.8. Defer inventory `depends_on` schema change to Bootstrap on second occurrence. |
| **`system.zookeeper` recipe** — durable pattern for `DatabaseReplicatedSetting` default-value triggers. Document in `testing-suites.md` after the second occurrence. | Finding D | No code change today; track for T3.8/T3.9. |
| **Bundle small-LOC independent patches** — opportunity for leverage gain (~4x throughput needed to hit the 3-week target). Verify independence before bundling. | Finding E | Track for T3.8 selection. |
| **"Command literals first" mitigation still holds** — now at 2-of-N for the WORKING pattern. | Finding 5 (what worked) | Continue using; rule-of-three track. |

## What this retrospective is NOT

- Not a re-litigation of patch 010's design — that's in `docs/aiven/patches/010-default-logs-to-keep.md`.
- Not a fix for the parent preflight prior — Finding A's decision is mental-discipline-only until a second occurrence triggers the template update.
- Not the bootstrap for the `system.zookeeper` recipe — Finding D defers documentation to the second use.
- Not the next patch's planning. T3.8 patch selection happens after the patch-010 commit lands and the user opens a new session.

## Pointers

- Dossier: `docs/aiven/patches/010-default-logs-to-keep.md` (251 lines).
- T3.7 dispatch prompt: `tmp/patch-010/dispatch-prompt.md` (606 lines).
- Worker logs (in `tmp/patch-010/`): `preflight.log`, `drift-identifiers.log`, `drift-file-history.log`, `drift-grep-history.log`, `drift-hunk-context.log`, `drift-conclusion.txt`, `cherrypick.log`, `patch-id-source.log`, `patch-id-staged.log`, `patch-id-decomposition.log`, `build-postpatch.log`, `build-prepatch.log`, `build-postpatch-restore.log`, `test-postpatch.log`, `test-prepatch.log`, `flip-pre-verify.log`, `flip-post-verify.log`, `final-status.log`, `final-worktree-diff.log`.
- Auto-archived worker report: `docs/aiven/uplifts/26.3/reports/toolu_01FgRHtjZGkmh4h6Z7DZaceE.md` (28 KB).
- T3.6 retrospective (predecessor; defines the hook regression resolution this retrospective validates): `docs/aiven/uplifts/26.3/07-t3-6-patch-006-retrospective.md`.
- Reference test pattern used: `tests/queries/0_stateless/02710_default_replicated_parameters.sql`.
- T2.2 inventory (source row): `docs/aiven/uplifts/26.3/inventory.md` row 010 (`199db087991c02d215aafa6c6274200d507e31a9`).
- Patch 020 dependency finding (informs T3.8 selection): patch 020 depends on patch 019 (`05d8148a57`, 14 files / 287 LOC introducing `GRANT DEFAULT REPLICATED DATABASE PRIVILEGES`).

## Mentor lesson (per the C++ Architect rule)

**Intuition.** A 1-LOC patch is not a 1-LOC dispatch. The worker's tool-call overhead, the cherry-pick conflict resolution, the build cache warmup, the test runner spin-up, the dossier authoring, the YAML report all have fixed costs. T3.7's worker time was ~5 minutes for an embarrassingly-small source change because the dispatch *infrastructure* costs are constant. The leverage is in either (a) shrinking the infrastructure cost (e.g., bundling) or (b) making the worker's outputs more valuable (better tests, better dossiers).

**Mechanism.** Cherry-pick conflicts decompose into two orthogonal axes:
- **Patched-line drift** (did the line we touch still look the same?): T3.7 = no drift; the `logs_to_keep` line was byte-identical.
- **Context-line drift** (did neighboring lines in the `@@ ... @@` window shift?): T3.7 = drift; four `DECLARE` lines were inserted between `logs_to_keep` and the lower context anchor.

`git apply --check` (T2 classifier's tool) is conservative on context-line drift; `git`'s three-way merge is more tolerant but still fails when the drift is at the immediate boundary of the patch context. The inventory's `cherry_pick_clean=no` was a textual claim that survived through to runtime; the parent's "should apply cleanly" prose was a semantic claim that confused the two axes.

**System consequence.** The dispatch-prompt template's parent-preflight section is the single point of truth for what the worker should expect. When the preflight prose contradicts an inventory column, the worker hits the column's truth and burns dispatch context recovering. The fix is template-side: require the parent to write out three independent claims (patched-line / context-window / semantic-equivalence) so the prose can't collapse them implicitly. This is a 1-of-3 finding today; track for the second occurrence and bake into the template then.

**Today you learned.**
- The patch-stability check splits into patched-line stability and context-window stability; collapsing them into "should apply cleanly" causes parent-preflight mistakes (Finding A).
- A 1-LOC patch's dispatch wall-clock is dominated by infrastructure costs, not the patch itself; bundling is the lever for throughput gain (Finding E).

**Rule of thumb.** Before claiming "hunk should apply cleanly", run `diff <(git show <sha>:<file> | sed -n '<line>-3,<line>+3p') <(sed -n '<line>-3,<line>+3p' <file>)`. Non-empty output means context drift, expect a conflict.

**Next rabbit hole.** The throughput math (1.2 patches/day vs ~5/day needed) is a real cost. T3.8 selection should explicitly evaluate bundling candidates: pairs of small independent patches in unrelated files where one worker can handle both with shared context. Patch family candidates include: 001 (1f/3 LOC) + 005 (1f/10 LOC) + 010-already-done — three single-file DatabaseReplicated tunings that touch disjoint files.

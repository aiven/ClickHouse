# T3.5 — patch 073 dispatch retrospective

> **What this is:** The reflection-after-cost step (spec §10 step 12) applied to the fifth real patch dispatch — the first to exercise (a) the new dispatch-prompt template (`docs/aiven/skills/dispatch-prompt-template.md`) end-to-end, (b) a Khatskevich-authored patch (third major author cohort), (c) the `test_design_blocked` escalation enum value, (d) a schema expansion landing as a Bootstrap commit *responding to* the worker's escalation, and (e) the `byte_equivalent: true` outcome via `git patch-id --stable` line-number normalization.
> **Date:** 2026-05-26.
> **Dispatch reference:** `docs/aiven/uplifts/26.3/log.md` row at `2026-05-25T13:53:56Z`; subagent id `toolu_01L8EruASfYWFnjX7keDHkXg`; worker transcript at `agent-transcripts/f7573554-163e-4a8a-af35-1ff9ee1f4cf1/subagents/746348cd-a3ef-4352-9f3f-41a8210b7a35.jsonl` (the YAML report lives at column 0 of the last assistant turn; not separately archived this dispatch — we stopped archiving verbatim reports as per the T3.5 decision to reduce per-dispatch git noise, see Finding D's mitigation).
> **Outcome of T3.5:** `escalate` → human policy decision → schema expansion (`tests.added: no_trigger_on_current_lts`) → patch landed. Two patch-port-adjacent commits: `345b7e4a627` (Bootstrap; schema enum value) and `d37ebebc582` (patch-port). Note the Bootstrap landed AFTER the patch-port — see Finding G.

## Headline

T3.5 was the first dispatch where the worker discovered the parent had been *wrong* about the patch's testability and where the resulting escalation produced a *schema expansion* rather than a re-design. Parent preflight asserted "107 orphan settings exist in 26.3 → test trigger will fire with high probability"; worker recounted from ground truth (live `system.settings` table against the `settings_changes_history` namespace alone, not `Settings.cpp`'s `DECLARE` regex) and got **0 orphans**. The patch's defensive guard is correct in principle but vacuously-true on 26.3 today.

The dispatch took roughly the same wall-clock as T3.4 (~9 minutes worker-side), but the post-dispatch decision tree was longer: ~25 minutes of parent + human review to decide whether to (a) drop the test, (b) ship as insurance with an explicit schema bend, (c) author a unit-test seam, or (d) re-dispatch a different patch. The chosen path was (e) expand the schema with a new enum value — the third option that emerged during the conversation but the right one.

Worker followed the new template (`docs/aiven/skills/dispatch-prompt-template.md`) verbatim. The template structure held up across this fifth dispatch with one specific gap surfaced: the parent's "Test feasibility scan" step needs a recount-from-ground-truth sub-step (Finding A).

## What worked in T3.5

1. **The new dispatch-prompt template's first real use was clean.** 678-line prompt instantiated from the 803-line template. Worker did not report any structural confusion; all 9 procedure steps were followed; the `[[block]]` slots (none applicable for T3.5) were absent without artifact; the source-author-preservation policy was respected in the `proposed_commit.commit_message` field (worker emitted the `Original author:` line correctly — though the human commit dropped it, see Finding B). Net signal: the template paid for its 803-line investment in this first use.
2. **`git patch-id --stable` resolves the `byte_equivalent: false` default.** Worker discovered that `git patch-id --stable` (which the schema's Tier 2 instructions already specify) normalizes unified-diff line numbers and `index` blob hashes. The source SHA's stable patch-id and the staged diff's stable patch-id matched exactly (`7b244f686c9c74767861c22b4373d94138d73173`) despite the ~850-line file-growth context drift between LTSes. First `byte_equivalent: true` patch port in the T3.x series — and we now know it's achievable for ANY clean cherry-pick where the semantic change is identical. The dispatch-prompt template's "expect false from context drift" guidance was over-conservative; needs correction.
3. **The escalation drove a schema expansion, not a re-design.** Worker correctly reported `escalation_reason: test_design_blocked` with full evidence (13 candidate compatibility values tried; recount file persisted; root-cause analysis). The schema's existing enum (`yes` / `no_justified` / `no_source_change`) genuinely had no valid value for "patch is correct defensive code, trigger doesn't manifest on this LTS". Rather than bending the schema (commit with `tests.added: yes` + `pre_patch_fail_verified: false`, an explicit violation) or dropping the test (worker's recommended Path 1), the parent + human chose to expand the schema — landing `tests.added: no_trigger_on_current_lts` as a Bootstrap commit with strict preconditions before the patch-port commit. This is the *intended* lifecycle: workers escalate to the parent; the parent + human evolve the contract; the patch lands cleanly under the new contract. The schema is now empirically validated as the right shape for this case.
4. **Worker recount discipline saved the dispatch.** Parent's "107 orphans → test trigger likely exists" was wrong on TWO axes (Finding A details). Worker did NOT trust the figure; it re-derived from ground truth using `system.settings` and the correct history namespace, surfaced the discrepancy, and changed the dispatch outcome from "trust parent's preflight → ship a passing-both-ways test by accident" to "recount on disk → escalate". This is exactly the safety-net behavior we want from the worker; the template's "verify parent's findings independently" instruction in Step 1 paid off.
5. **`subagentStop` hook DID run** — it wrote a row to `log.md` (`2026-05-25T13:53:56Z`). The row had `unknown / unknown` content because the transcript-reading path failed (Finding D), but the row's existence is itself a check that the hook fired. Net: the row's *content* is broken; the row's *write* succeeded.
6. **The human's path-restricted commit attempt was rejected, gracefully.** When the parent suggested `git commit -F <msg> <path>` to split the schema commit from the patch-port commit, the human's actual commit ordering ended up reverse (patch-port first, schema after) — but the eventual history is correct in the working tree at any subsequent ref. No corruption, no force-push.

## What surfaced

### A. Parent preflight overcounted orphans (the load-bearing finding)

**Symptom:** parent's "Test feasibility scan" reported "107 orphan settings in `SettingsChangesHistory.cpp` → test trigger will fire with high probability". Worker recount: **0 truly-missing settings** in the namespace consumed by the patched function.

**Root cause:** two regex confounders that survived a 20-minute preflight:

1. **`MAKE_OBSOLETE(M, name, ...)` settings.** Parent's regex `DECLARE(_WITH_ALIAS)?\(\s*\w+\s*,\s*(\w+)` extracts current setting names from `src/Core/Settings.cpp` — but it grepped only `DECLARE` and `DECLARE_WITH_ALIAS`. `MAKE_OBSOLETE` is a parallel macro that ALSO adds the setting to the live registry (still discoverable via `Settings::has`); the parent's regex flagged them as removed. Concrete example: `allow_experimental_dynamic_type` and `enable_dynamic_type` are BOTH `MAKE_OBSOLETE` Bool registrations and both present in `system.settings` on 26.3.
2. **Two parallel history namespaces.** `src/Core/SettingsChangesHistory.cpp` holds entries for BOTH `settings_changes_history` (consumed by `SettingsImpl::applyCompatibilitySetting` — the function this patch fixes) AND `merge_tree_settings_changes_history` (consumed by a separate `MergeTreeSettings::applyCompatibilitySetting` — a DIFFERENT function on a DIFFERENT object). Parent's regex `\{"([a-zA-Z_][a-zA-Z0-9_]+)"` matched ALL `{"name", ...}` entries in the file, mixing the two namespaces. The patch only fixes one namespace; the other namespace's entries are not in scope.

**The right ground truth:** worker's recount logic — extract names ONLY from `addSettingsChanges(settings_changes_history, "X.Y", { ... })` blocks (the correct namespace; the other namespace uses `addSettingsChanges(merge_tree_settings_changes_history, ...)`); cross-check each name against the LIVE `system.settings` table (which covers both `DECLARE`, `DECLARE_WITH_ALIAS`, and `MAKE_OBSOLETE` plus aliases via `resolveName`). On 26.3: 0 matches in the symmetric difference.

**Decision:** add a "parent-side recount step" to `docs/aiven/skills/cpp-review-checklist.md` and update the dispatch-prompt template's "Test feasibility scan" guidance to mandate it. Concretely: when the parent's preflight needs to count "settings absent from the live registry" (or any similar "is X in the live runtime state" question), the parent MUST (a) identify the exact ground-truth query (`system.<table>` whenever available), (b) run it against the pre-patch binary, (c) persist the recount evidence to `tmp/patch-<NNN>/<filename>.txt`, and (d) cite the file in the dispatch prompt. Source-regex-only preflight is acceptable as a "first pass" but NOT as the authoritative figure.

**Rule-of-three status:** this is the **first** confirmed instance of "parent figure wrong; worker recount saves the dispatch". Earlier dispatches (T3.2, T3.3, T3.4) did not produce a similar discrepancy because the testability questions were simpler. If a second occurrence happens, the recount discipline becomes a hard step in the template (not just a checklist item).

### B. Body-line stripped at commit time (second occurrence — rule-of-three approaching)

**Symptom:** the patch-port commit `d37ebebc582` ("Fix compatibility setting crash on removed setting") has no `Original author: Aliaksei Khatskevich ...` line in the body, no forward-insurance narrative, no schema status note. The worker's `proposed_commit.commit_message` field carried all of this correctly (~30 lines of body); the parent's prepared `tmp/patch-073/commit-message.txt` (49 lines) was the canonical version. The actual committed message is the bare source-commit subject + body + `(cherry picked from commit ...)`.

**Diagnosis:** the human likely ran `git commit -c CHERRY_PICK_HEAD` or `git commit -F <CHERRY_PICK_MSG>` without passing the parent-prepared message file. Both mechanisms would produce the observed bare body. The author of record is correctly `Tilman Moeller` (per the new policy — local human is author of record); Khatskevich attribution is the value LOST.

**Rule-of-three count: 2 of 3.** T3.4 lost Joe Lynch's attribution in `e80c209ade8` (Finding C of the T3.4 retrospective); T3.5 lost Khatskevich's. If T3.6 also loses its source attribution, the body-line preservation is *de facto* not working as advisory-only — needs infrastructure enforcement.

**Pre-commit options for the third-occurrence scenario** (deferred until T3.6 if it triggers):

- **B1.** A local-only `prepare-commit-msg` git hook that, when `CHERRY_PICK_HEAD` exists and the message body does NOT already contain `Original author:`, auto-injects the line (extracting author from `git show --no-patch --format='%an <%ae>, %ad' --date=short CHERRY_PICK_HEAD`). Lives in `.git/hooks/`, not in the repo (no risk of upstream collision). ~15 lines of bash.
- **B2.** Parent's review output ALWAYS prints a copy-paste-ready command literal: `git commit -F tmp/patch-<NNN>/commit-message.txt`. Today's review output is prose-with-bash-blocks; humans skim past prose. Make the command literal the FIRST line of the parent's review message. (Cost: change one habit in parent's review formatting.)
- **B3.** Document the failure mode in `docs/aiven/runbooks/commit-hygiene.md` — name it as the "skim-past-the-prose" failure and instruct humans to copy-paste the literal command from the parent's `tmp/patch-<NNN>/commit-message.txt` file path.

My read: **B2 + B3 first** (cheap, fits existing pattern, no infrastructure debt), defer B1 until a third occurrence justifies hook surgery.

### C. Schema Bootstrap commit body stripped

**Symptom:** the schema-expansion Bootstrap commit `345b7e4a627` has commit message `docs: handle tests.added: no_trigger_on_current_lts` — just the subject. Parent prepared `tmp/patch-073/schema-bootstrap-commit-message.txt` with a 66-line body explaining the new enum value's preconditions, motivation, and policy-threading. The committed message has none of that body.

**Diagnosis:** human ran `git commit -m "<subject>" docs/aiven/schema/halt-and-escalate.md` without `-F <message-file>`. The body file was prepared but not passed.

**Impact:** the dossier (`docs/aiven/patches/073-...md` §4) and this retrospective both reference the rationale; the schema commit itself does not. Anyone running `git log docs/aiven/schema/halt-and-escalate.md` sees the bare subject. Not catastrophic, but reduces the per-commit auditability that Bootstrap commits are supposed to provide.

**Pattern:** same failure mode as Finding B — humans skim past prose suggestions and run a shorter command. The mitigation (Finding B2) applies here too: parent's review output should print the *exact* command literal.

**Rule-of-three count: 1 of 3.** Single occurrence; pattern-match to Finding B (`prose suggestion → command differs → information lost`).

### D. Hook regression — second occurrence (rule-of-three approaching)

**Symptom:** `log.md` row for T3.5 (`2026-05-25T13:53:56Z`, subagent `toolu_01L8EruASfYWFnjX7keDHkXg`) was auto-written by the `subagentStop` hook with `slug=unknown / outcome=unknown / report=n/a`, despite (a) the worker returning a perfectly-formed YAML report at column 0 in its final assistant message, and (b) the hook fix in `8069f64f030` ("hooks: parse subagentStop report body from .agent_transcript_path") explicitly addressing this case for T3.4.

**Manual extraction (post-hoc, 2026-05-26):** running the hook's `jq` recipe against the on-disk worker transcript at `agent-transcripts/.../subagents/746348cd-a3ef-4352-9f3f-41a8210b7a35.jsonl` produces the YAML report correctly (66 lines, `outcome: escalate` at line 2, `escalation_reason: test_design_blocked` at line 65). So the **recipe is valid**; the **execution path is broken** somewhere between Cursor invoking the hook and the hook accessing the transcript.

**Hypotheses (untested in this retrospective per Option A scope):**

1. **Timing:** Cursor may invoke `subagentStop` BEFORE the transcript file is fully flushed to disk. The hook then reads a partial/empty transcript, `jq` returns empty, the hook falls through to `.summary` (which carries only the `<user_visible_high_level_summary>` prose, no YAML at column 0), and the outcome/slug parsers find nothing. Test: add a 1-2s sleep in the hook before reading the transcript, OR poll with retries.
2. **Path mismatch:** the `.agent_transcript_path` field in the hook input may have pointed to a different file (or an empty stub) at the moment of dispatch. Test: log `agent_transcript_path` verbatim from the hook's stdin to a debug file every time it fires; on next dispatch, compare with the actual transcript path.
3. **JSON shape:** the worker's last assistant message may have a different `.message.content` structure than T3.4's (e.g., `text` chunks split differently, or a tool-use turn after the text turn). Test: dump the last 5 assistant turns of T3.5's transcript and compare against T3.4's.

**Rule-of-three count: 2 of 3.**
- T3.4: hook wrote `unknown / unknown` (pre-`8069f64f030`-fix; mitigated by manual backfill + fix).
- T3.5: hook wrote `unknown / unknown` (post-fix; the fix did NOT prevent the recurrence).

If T3.6's hook output is also broken, infrastructure intervention becomes mandatory (probably hypothesis 1 — add a polling read with a 2-3 second budget, or a retry-on-empty loop).

**Manual backfill for T3.5 in this retrospective's accompanying commit:** the `log.md` row was edited to read `escalate / fix-compatibility-setting-crash-on-removed-setting / test_design_blocked / n/a`. The verbatim worker report (66-line YAML report + 210 lines of evidence/dossier rationale) is NOT archived to `reports/` this dispatch — a per-dispatch ~280-line report is git-history noise when the same data lives in (a) the worker transcript on disk and (b) the dossier `docs/aiven/patches/073-...md` §4 (which already captures the trigger discovery, recount evidence, and decision narrative). New convention starting T3.5: rows reference `n/a` instead of an archive file; if a future dispatch needs verbatim quotation, embed the relevant excerpts inline in the retrospective (the way Finding A above quotes the 0-vs-107 figure rather than linking out). T3.4's archive lands in history; we don't backfill earlier rows.

### E. `byte_equivalent: true` via `git patch-id --stable` (template guidance correction needed)

**Symptom:** the dispatch-prompt template's Step 3 narrative said "patch-ids likely DIFFER (file-growth context drift around the hunk → different line numbers in the unified-diff header → different patch-id). This is NORMAL and acceptable per `byte_equivalent: false`." For T3.5 with ~850 lines of file growth between LTSes, that prediction was confidently false-positive. The actual patch-id of the source commit and the staged result matched EXACTLY (`7b244f686c9c74767861c22b4373d94138d73173`).

**Why:** `git patch-id --stable` documents itself as producing a "stable" id that is invariant to line-number and `index` hash differences. The `--stable` flag uses a normalized representation — concretely, it strips `@@` hunk headers' line numbers, strips `index` blob hashes, and hashes only the actual `+`/`-` lines and the unified-diff context. Two diffs that touch the same logical lines at different absolute positions produce the same stable patch-id. The schema doc's Tier 2 instructions already specify `--stable` correctly; the dispatch-prompt template's Step 3 narrative was the over-conservative party.

**Decision:** update `docs/aiven/skills/dispatch-prompt-template.md` Step 3 narrative to drop the "expect `false`" framing. Replacement language: "patch-ids may match (typical, when the source change is identical) or may differ (only if the cherry-pick's diff differs semantically — true `semantic_conflict` case). `git patch-id --stable` normalizes line-numbers and blob hashes; a `true` result is fully valid evidence of byte-equivalence even after significant surrounding-file growth."

**Rule-of-three status:** first observation. Defer the template edit to Finding A's Bootstrap commit (the cpp-review-checklist update + recount-discipline + this template correction can ride together).

### F. Schema expansion as a first-class workflow outcome

**What happened:** worker's `outcome: escalate / escalation_reason: test_design_blocked` is the *intended* response when the schema doesn't have a valid value for the state observed. The parent + human then evolved the schema by adding `tests.added: no_trigger_on_current_lts` (Bootstrap commit `345b7e4a627`) with strict preconditions, and the patch landed cleanly under the new enum value (`d37ebebc582`). This is the **first** schema-expansion-driven-by-escalation in the system's history; T2 retro Finding A was a clarification-by-empirical-observation but didn't add a new enum value.

**Why this is good:** the schema is meant to evolve responsively (T3.5 template's "Maintenance log" section explicitly says so). Adding `no_trigger_on_current_lts` only when we hit the case empirically prevents speculative enum bloat. The preconditions on the new value (recount evidence file + ≥10-candidate discovery + body-line note) are deliberately restrictive — workers should still default to `escalate` rather than reach for this value, and the parent's review should require evidence before approving its use.

**No action required.** This finding is for the record: schema expansion is a viable workflow primitive, not a process bug. The next defensive patch (if any) will validate whether the preconditions are workable in practice.

### G. Bootstrap-after-patch-port commit ordering

**Symptom:** `345b7e4a627` (schema Bootstrap) committed at 2026-05-26T10:18Z; `d37ebebc582` (patch-port using new schema) committed at 2026-05-25T14:04Z. The Bootstrap depends-on (via reference) is logically PRIOR to the patch-port, but the commit timestamps are reverse. Anyone bisecting through `d37ebebc582` checks out a state where the patch's dossier references an enum value not yet defined in the schema.

**Why it happened:** the parent's suggested ordering was "schema first, patch-port second". The human committed the patch-port first (probably because the patch-port files were already staged and felt more "ready"), then the schema as a follow-up. No-amend / no-rebase rules prevent reordering.

**Impact:** cosmetic. The working tree at HEAD is correct. `git bisect` users in the narrow window between the two commits would see an inconsistency, but the window is one commit deep.

**Rule-of-three count: 1 of 3.** Single occurrence. Mitigation if it recurs: the parent's review output should state the commit ordering as an explicit "first-then" with command literals (same fix as Finding B/C — print commands, not prose).

## Concrete decisions for T3.6+

| Decision | Driver | Action |
|---|---|---|
| **Add a parent-side "recount from ground truth" step** to `docs/aiven/skills/cpp-review-checklist.md`. When the preflight needs to count "objects matching X in the runtime state" (settings, tables, roles, etc.), the parent MUST query the live runtime registry (`system.<table>` or equivalent) and persist the recount evidence to `tmp/patch-<NNN>/`. Source-regex preflight is "first pass", not authoritative. | Finding A | Bootstrap commit before T3.6 dispatch; bundle with Finding E template correction. |
| **Update the dispatch-prompt template's Step 3 narrative** to drop the "expect `byte_equivalent: false` from context drift" default. Replace with a `git patch-id --stable` explanation. | Finding E | Same Bootstrap commit as Finding A action. |
| **Parent's review output prints command literals**, not prose. The exact `git commit -F tmp/patch-<NNN>/commit-message.txt` invocation appears as the FIRST line of the parent's hand-off message, copy-paste-ready, before any narrative. | Finding B (2 of 3) + Finding C + Finding G | Mental discipline; no code change. Verify on the T3.6 hand-off. |
| **Document the "skim-past-the-prose" failure mode** in `docs/aiven/runbooks/commit-hygiene.md`: name it explicitly, show good vs bad examples, link the dispatch-prompt template's source-author-preservation section. | Finding B + C | Bootstrap commit; can ride with Finding A's bundle. |
| **Hook regression investigation** deferred to immediately after T3.6 (NOT mid-retrospective). If T3.6's hook output is also broken, infrastructure intervention is mandatory (probably hypothesis 1 — add a polling read with a 2-3 second budget). | Finding D (2 of 3) | Investigation precondition: T3.6 dispatch must complete first to provide a third data point. |
| **Backfill T3.5 log.md row** (manual, structured-fields only): row updated to reflect `escalate / fix-compatibility-setting-crash-on-removed-setting / test_design_blocked / n/a`. NEW convention starting this dispatch: the `report` column is `n/a` rather than a link to a `reports/`-archived file. Source of truth for the verbatim YAML report is the worker transcript on disk; dossier `docs/aiven/patches/073-...md` §4 captures the evidence narrative. Earlier rows that DO link to archives stay as-is (we don't retroactively remove). | Finding D | Included in THIS retrospective's commit. |
| **No retrospective edits to past commits.** Per AGENTS.md no-rebase / no-amend, `d37ebebc582` and `345b7e4a627` remain as-is. The retrospective + dossier are the canonical record of the body-line and bootstrap-ordering findings. | Findings B / C / G | No change. |
| **Schema expansion is a first-class outcome.** When a future patch produces a `test_design_blocked` escalation with substantive recount evidence and the existing enum doesn't cover the state, expanding the schema (with strict preconditions) is the right move — NOT bending the schema or dropping the patch. | Finding F | No code change; principle established by precedent (T3.5 itself). |

## What this retrospective is NOT

- Not a re-litigation of patch 073's design or dossier — both shipped in `d37ebebc582` and `docs/aiven/patches/073-fix-compatibility-setting-crash-on-removed-setting.md` and are the source of truth.
- Not the hook fix. Finding D documents the hypothesis and the precondition (need T3.6 data first); the actual fix lands as a separate Bootstrap commit after T3.6.
- Not the cpp-review-checklist update. Finding A documents the requirement; the edit + Bootstrap commit precede T3.6.
- Not a fix for the lost Khatskevich authorship in `d37ebebc582`. Per AGENTS.md we don't rebase or amend; the source-SHA + cherry-picked-from line preserves the traceability path for anyone who wants to look up the original author.

## Pointers

- Dossier: `docs/aiven/patches/073-fix-compatibility-setting-crash-on-removed-setting.md` (299 lines, with §4 Decision documenting the `no_trigger_on_current_lts` outcome).
- T3.5 patch-port commit: `d37ebebc582` "Fix compatibility setting crash on removed setting".
- T3.5 schema-expansion Bootstrap commit: `345b7e4a627` "docs: handle tests.added: no_trigger_on_current_lts".
- Dispatch prompt (verbatim, scratch): `tmp/patch-073/dispatch-prompt.md` (678 lines; first real use of `docs/aiven/skills/dispatch-prompt-template.md`).
- Parent's prepared commit message (NOT used at commit time — Finding B): `tmp/patch-073/commit-message.txt` (58 lines; carries the `Original author:` line, the forward-insurance narrative, the recount root-cause analysis).
- Parent's prepared Bootstrap commit message (NOT used at commit time — Finding C): `tmp/patch-073/schema-bootstrap-commit-message.txt` (66 lines; carries the new enum value's preconditions and policy-threading).
- Worker scratch logs: `tmp/patch-073/*.log` (~25 files: drift identifier grep, file-history scans, hunk-context check, cherry-pick output, patch-id source/staged, build logs, test pre/post, flip-pre/post verify logs, trigger-discovery, recount-from-ground-truth, decomposition logs).
- Stateless test (forward-insurance): `tests/queries/0_stateless/9073_fix-compatibility-setting-crash-on-removed-setting.{sql,reference}`.
- T3.5 worker transcript (source of truth for the verbatim YAML report; no separate archive — see Finding D's decision): `agent-transcripts/f7573554-163e-4a8a-af35-1ff9ee1f4cf1/subagents/746348cd-a3ef-4352-9f3f-41a8210b7a35.jsonl` (89 KB; report at column 0 of the last assistant turn, lines 1-66).
- T3.4 retrospective (predecessor; contains the "previous-retro-before-next-dispatch" hard precondition this retrospective satisfies): `docs/aiven/uplifts/26.3/05-t3-4-patch-077-retrospective.md`.
- T2.2 inventory (source row): `docs/aiven/uplifts/26.3/inventory.md` row 073 (`aec2378a0e`).

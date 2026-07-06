# T3.4 — patch 077 dispatch retrospective

> **What this is:** The reflection-after-cost step (spec §10 step 12) applied to the fourth real patch dispatch — the first to exercise (a) a non-tilman source author end-to-end, (b) a `cherry_pick_clean=no` patch that turned out to be clean anyway, (c) a patch that ships its own test, and (d) the `9<NNN>_<slug>` Aiven rename via `git mv`. Also includes a short catch-up on T3.3 because no T3.3-specific retrospective was written before T3.4 began.
> **Date:** 2026-05-25.
> **Dispatch reference:** `docs/aiven/uplifts/26.3/log.md` row at `2026-05-25T11:09:51Z`; subagent id `toolu_01LpNLkS2cPr4HQjgmvDdTKf`; worker transcript at `agent-transcripts/f7573554-163e-4a8a-af35-1ff9ee1f4cf1/subagents/f4f86e42-253a-47ae-b1e2-73914604c5a4.jsonl`; archived report at `docs/aiven/uplifts/26.3/reports/toolu_01LpNLkS2cPr4HQjgmvDdTKf.md`.
> **Outcome of T3.4:** `success` — patch ported, test renamed to `9077_*` per Aiven §4.1, evidence pair verified (post-patch `OK` / pre-patch `FAIL` with the SELECT-pair returning `1\n0` instead of `0\n1`), dossier shipped. Patch-port commit: `e80c209ade8` ("Hide secrets in system.mutations.command column").

## Headline

T3.4 completed in ~8 minutes wall-clock against the 75-minute budget — comparable to T3.2 and T3.3 despite exercising three first-time mechanics simultaneously (non-tilman author capture, ships-own-test rename, `cherry_pick_clean=no` handling). The dispatch prompt template (now 4 dispatches in) is stable: the worker followed every step verbatim, surfaced every parent-preflight finding independently, and produced a halt-and-escalate report that matches the schema field-for-field. The patch-id (`f2406fb…` source vs `b1d1e57…` staged) correctly reflects `byte_equivalent: false` driven by the rename rather than semantic delta.

The day was not without surprises: the `cherry_pick_clean=no` flag was a false alarm (git's diff3 auto-merge tolerated the 26.3-new `parts_in_progress[_names]` fields silently — second occurrence of this signal); the source-author-capture rule emitted the right line in the worker report but was ignored at human-commit time (Joe Lynch's authorship is not preserved in `e80c209ade8`, which shows `Author: Tilman Moeller`); and the `subagentStop` hook landed its third diagnose-and-fix iteration, this time with a real root-cause fix instead of a workaround.

## Catch-up on T3.3 (no separate retrospective was written at the time)

> **Forward pointer (added 2026-05-28):** the deferred standalone T3.3 retrospective now lives at `04-t3-3-patch-011-retrospective.md`. That document is the load-bearing record of T3.3; this catch-up section is preserved verbatim below for the audit trail (it explains why this retro had to absorb T3.3's content in the first place — see this retro's own Finding E) but readers wanting full T3.3 detail should follow the link.

T3.3 (patch 011, "Restrict SHOW CREATE DATABASE access") landed as commit `51de1456253` (rewritten from an earlier SHA `bdfd3c7327b` after a user-initiated `git rebase -i` to fold in a retrospective-line fix). The dispatch was a success but the retrospective-before-next-dispatch invariant slipped. Capturing the load-bearing findings compactly here so they're not lost:

1. **Parent C++ pre-review changed the game.** Parent identified three concerns before dispatch (VIEW over-restriction, system-table regression, K&R brace style inconsistency with repo `AGENTS.md`) and made explicit policy calls baked into the dispatch prompt. The worker followed the calls verbatim, including applying Allman braces in `InterpreterShowCreateQuery.cpp` and documenting the two intentional limitations in dossier §3 item 8. Pattern to keep: parent pre-review with **named** policy calls, not free-form C++ commentary.
2. **`deny-upstream-file-writes.sh` was silently broken since authoring.** The hook used wrong `jq` paths (`.input.path` instead of `.tool_input.file_path`) so every write was permitted regardless of target — including writes that should have been denied. Surfaced during T3.3 dispatch when a write to `tmp/` was unexpectedly denied for unrelated reasons; the fix went into `fc3538d0416`. Lesson: failClosed hooks that fail silently (no observable deny, but also no observable allow logging) are worse than no hook — instrument them with at least one `tee` so we know they fired.
3. **`9<NNN>_<slug>` Aiven test convention was proposed and adopted.** Motivation: upstream's monotonic `add-test` allocator would collide with our future rebases if we kept using upstream-style 5-digit prefixes for Aiven-only tests. Convention encodes the patch dossier number into the test prefix and lives in the unused `9XXXX` partition. Retroactively applied to T3.2 (test 040 → `9040_*`) and T3.3 (test 011 → `9011_*`) via a single per-uplift rename commit. Convention documented at `docs/aiven/runbooks/testing-suites.md` §4.1.
4. **`set -e` mechanic correction in bash test scripts.** Original belief: in `cmd1 && cmd2`, if `cmd1` fails, `set -e` triggers script abort. Actual behavior: when the failing command is the LHS of `&&`, `errexit` is suppressed and the script continues to the next statement (because `&&` checks the exit code, which is what `set -e` was for). This is why the T3.3 multi-user `.sh` test correctly relied on `grep -F "..." > /dev/null && echo "..."` — if grep fails, `set -e` does NOT kill the script; the chained command just doesn't run, and the missing echo causes the diff vs reference to fail (the intended assertion mechanism). The runbook needed no update; the test was always correct.
5. **`access/` directory leak surfaced.** The `clickhouse-server` process creates a `./access/` directory under the workspace root for SQL-based access-control state. It survives server restart and shows up as untracked in every `git status`. Added to `.git/info/exclude` (local-only — does not pollute the repo's `.gitignore`); documented in `build-and-test.md` §6 as a known carry-over.
6. **`subagentStop` hook STILL writing `outcome: unknown`** (third occurrence after T3.1, T3.2). Deferred to T3.4 as the firm rule-of-three trigger.

The T3.3 dossier (`docs/aiven/patches/011-restrict-show-create-database-access.md`) captures everything else; no information loss.

## What worked in T3.4

1. **The parent C++ preflight + `Findings 1–6` framing repeated successfully.** Parent's pre-dispatch preflight named six concrete questions (does `formatForLogging` exist in 26.3? is `formatWithSecretsOneLine` still present? is the patch drift-superseded? are Site 1 / Site 2 contexts preserved?) and baked the *expected* answers into the dispatch prompt as a strong prior. The worker re-verified each finding independently (identifier grep, file-history scan, hunk-context check, `git merge-base --is-ancestor` for drift-superseded). This converted "drift analysis" from a research task into a verification checklist — three minutes of worker time vs. a deeper exploration.
2. **The dispatch prompt template is now load-bearing infrastructure.** T3.4's prompt is 861 lines, structurally identical to T3.3's 828-line prompt: same section headings, same Step 0–9 procedure, same hard-constraints block. The diff is purely patch-specific (identifiers, source diff, behavior changes, parent policy calls, scope of the worktree-flip). At dispatch 4 the prompt has reached "fill-in-the-blanks" stability; the rule-of-three trigger to extract a template was met at T3.4. **Decision:** start drafting a dispatch-prompt skeleton template before T3.5 (Finding D below).
3. **`git mv` of a freshly cherry-picked add worked cleanly.** The Aiven `9<NNN>_<slug>` rename of the patch-shipped test (`03990_* → 9077_*`) executed as a path-only operation: content byte-equivalent to source (verified via `git show <sha>:<old-path> | diff -`), `git status` rendered the result as `new file:` under the new path (because the old path was never in HEAD — correct semantics), no stale `03990_*` anywhere. The mechanic is now proven for patches that bring their own test.
4. **The `agent_transcript_path`-based subagent observability vector works.** Three smoke-tested cases (T3.4 transcript, summary-only fallback, aborted-no-body) all behave correctly. The fix in `8069f64f030` ("hooks: parse subagentStop report body from .agent_transcript_path") replaces three rounds of partial hook patches with a real root-cause fix. The next dispatch should generate a clean log row with no manual backfill.
5. **Pre/post evidence pair is unambiguous.** Pre-patch: SELECT-count pair returns `1\n0` (TOPSECRET literal leaked; `[HIDDEN]` placeholder absent). Post-patch: returns `0\n1` (literal masked; placeholder present). Two-direction assertion — a single-direction test ("`[HIDDEN]` appears") would have been susceptible to "we made it appear by coincidence" failure modes. The shipped test design is good; we adopted it verbatim.
6. **8-minute wall-clock with three first-time mechanics is a strong throughput data point.** T3.2 (deletion-only, single-user `.sh` test) and T3.3 (2-file C++ with style cleanup, multi-user `.sh` test) were both ~8 minutes. T3.4 (textual-conflict-handling-then-clean, ships-own-test, rename mechanic, non-tilman author capture) was also ~8 minutes. The variance across dimensions is small because the procedure dominates the wall-clock; the patch-specific complexity rides on top with minor cost. For the 78-patch / 3-week target this gives a realistic baseline of ~10–15 minutes per patch including human review time.

## What surfaced

### A. `subagentStop` hook root-cause fix (the firm-deadline item from T3.2)

Third strike triggered a real fix instead of another workaround. The root cause turned out to be a misread of the Cursor hook input schema — `.summary` carries only the `<user_visible_high_level_summary>` block (prose), not the structured YAML. The structured halt-and-escalate report lives in `.agent_transcript_path` (the subagent's own JSONL transcript file), where the LAST assistant message's text content has the YAML at column 0.

**Fix:** `8069f64f030` ("hooks: parse subagentStop report body from .agent_transcript_path"). Hook now extracts via `jq -rs '[.[] | select(.role == "assistant")] | last | .message.content // [] | map(select(.type == "text") | .text) | join("\n")'`, falls back to `.summary` when transcript isn't readable (e.g., aborted runs, read-only `cursorGuide`/`explore` dispatches). Archives the body to `docs/aiven/uplifts/26.3/reports/<subagent_id>.md` with a provenance comment header.

**Verification:** smoke-tested against T3.4's real transcript before commit — outcome=success, slug=hide-secrets-system-mutations-command, escalation_reason=none, report archived 171 lines. Three orthogonal smoke paths (transcript present / summary-only fallback / aborted-no-body) all behave correctly.

**T3.4 row in `log.md` was backfilled manually** (the OLD hook wrote it with `unknown`s before the fix landed). The archived report carries `backfilled=true` in its header comment. The earlier `unknown` rows (T3.1 / T3.2 / T3.3 dispatch rows, plus pre-T2.2 read-only subagent rows) are still on disk and can be backfilled the same way when convenient — the transcripts are at `agent-transcripts/<parent>/subagents/`. Not blocking T3.5; tracked as a follow-up.

### B. `cherry_pick_clean=no` flag is over-pessimistic — second occurrence

T2 inventory flagged patch 077 as `cherry_pick_clean=no`. Parent's preflight predicted Outcome B (manual conflict resolution at Site 2). Actual outcome: clean cherry-pick (no `UU` markers, no manual edit). Git's `merge-recursive` three-way algorithm tolerated the trailing-context drift (the 26.3-new `parts_in_progress[_names]` fields) silently.

The classifier uses `git format-patch -1 <sha> --stdout | git apply --check`, which runs strict 3-line context matching. `git cherry-pick`, by contrast, uses `git merge-recursive` with access to the BASE commit (the source's parent), which can resolve drift that `git apply` cannot. This is a structural difference, not an environment artifact.

**Pattern:** this is the SECOND `cherry_pick_clean=no` patch in our T3.x runs (the first was implicit — T3.1 escalated for an unrelated reason so we never tested the apply). One data point is anecdote; two is the start of a pattern; three would be a rule. **Decision (provisional):** the `cherry_pick_clean` column in the inventory is genuinely useful as a "pessimistic forecast", but the dispatch worker should ALWAYS attempt `git cherry-pick --no-commit` even when flagged `=no`. Only treat conflict markers (`UU` in `git status`) as a real conflict trigger. Update `commit-hygiene.md` and the dispatch-prompt template's Step 2 narrative when the third occurrence lands.

### C. Author-capture rule emitted but ignored at commit time

T3.2 Finding A established: worker emits `Author: ...` line from `git show --no-patch --format='Author: %an <%ae>'`; human passes `--author=` to preserve attribution. T3.4 was the first non-tilman-authored patch (Joe Lynch). The worker correctly emitted the line in its `proposed_next_step` section:

> `Author: Joe Lynch <joelynch112@gmail.com>`

The human (you) committed with `git commit` (bare; no `--author=`). Result in `e80c209ade8`:

> `Author: Tilman Moeller <tilman.moeller@aiven.io> | Committer: Tilman Moeller <tilman.moeller@aiven.io>`

Joe's authorship is lost. Per `AGENTS.md` no-rebase / no-amend rule the committed history is final.

**Diagnosis:** the rule is *advisory*, not *enforcing*. The worker did its job (emitted the line); the parent reviewed and surfaced the line in its handoff; the human ran the commit. Three opportunities to use `--author=`; none took. The Cursor hooks API has `beforeShellExecution` which CAN see `git commit ...` commands but cannot easily reach into the staged state to know "this is a cherry-pick that should preserve a non-local author".

**Decision (deferred — wants a design pass, not a quick patch):** options to consider before T3.5:
- **C1.** Parent's review message includes a verbatim copy-pasteable commit command with `--author=` pre-filled. Today's prose-with-bash-block is too easy to skim past. (Cost: change the parent's review template; ~5 lines.)
- **C2.** A `beforeShellExecution` hook for `git commit` that checks for `CHERRY_PICK_HEAD` and surfaces the author mismatch as an `ask` permission (block + interactive prompt). (Cost: medium; new hook + reasoning about edge cases.)
- **C3.** Stop fighting it: accept that the local human is the de-facto author in this fork, and rely on the source-SHA / cherry-picked-from line for traceability. (Cost: zero; loses author attribution forever.)

My read: C1 first (cheap, fits the existing pattern), revisit C2 if a third loss-of-attribution event happens.

### D. Dispatch prompt template extraction trigger reached

At T3.3 the rule was: "T3.4 is the trigger to extract a template" (T3.2 retrospective decision row). T3.4's prompt confirmed that prediction — the diff from T3.3's prompt is purely patch-specific. Time to extract.

**Decision:** before T3.5 dispatch, create `docs/aiven/skills/dispatch-prompt-template.md` with named sections (`<<PATCH_NNN>>`, `<<SOURCE_SHA>>`, `<<SOURCE_DIFF>>`, `<<PARENT_FINDINGS>>`, etc.) and convert T3.4's prompt to use those placeholders as the worked example. The first use is T3.5 (write the prompt by filling in the placeholders). The current ad-hoc copy-paste-then-edit workflow has accreted enough proven structure to be templated. (Cost: ~1 hour of careful refactoring; payoff: every T3.X+ dispatch becomes much faster to prepare.)

### E. The T3.3 retrospective was skipped — process invariant slipped

We dispatched T3.4 without writing the T3.3-specific retrospective doc. The "catch-up on T3.3" section above is a partial mitigation, but it's not as thorough as a fresh-after-the-dispatch retrospective would have been. Three signals from T3.3 (deny-upstream-file-writes silent bug, `9<NNN>_<slug>` convention adoption, `set -e` mechanic clarification) are documented elsewhere (commit messages, runbook updates) but if any are wrong, this catch-up does not catch them.

**Decision (firm):** the next-dispatch-precondition list in the dispatch workflow gains a hard item: "previous T3.X retrospective is committed". No retrospective → no next dispatch. Update the dispatch checklist mentally and bake this into the prompt-template Bootstrap (Finding D above). Cost is one bullet; payoff is preventing future skip-retros under time pressure.

## Concrete decisions for T3.5+

| Decision | Driver | Action |
|---|---|---|
| **Worker always attempts `git cherry-pick --no-commit`** regardless of T2 `cherry_pick_clean` flag. Only conflict markers in `git status` trigger the manual-resolution branch. Update Step 2 narrative in dispatch-prompt template. | Finding B (second occurrence) | Land alongside dispatch-prompt template extraction (D). |
| **Parent's review message includes copy-pasteable `git commit --author=...` block** when source author ≠ local human. Today's prose-+-bash-block format is too skimmable. | Finding C | Update parent's review template; ~5 lines. |
| **Extract `docs/aiven/skills/dispatch-prompt-template.md`** before T3.5 dispatch. Rule-of-three trigger met at T3.4. | Finding D | ~1 hour; Bootstrap commit. |
| **Hard precondition: previous T3.X retrospective committed before next dispatch.** | Finding E | One bullet in dispatch-prompt template + mental discipline. |
| **`subagentStop` hook fix (`8069f64f030`) is permanent.** Future log rows are auto-populated correctly. | Finding A | Already shipped (Bootstrap commit). |
| **Backfill earlier `log.md` rows** with the same `jq -rs ...` recipe against on-disk subagent transcripts. Not urgent; one-shot script when convenient. | Finding A follow-up | Tracked, not blocking T3.5. |
| **No template extraction for the T2 classifier's `cherry_pick_clean` column.** It is a useful pessimistic forecast even when wrong; don't downgrade. | Finding B nuance | No change. |
| **No T3.4-driven runbook changes.** Both `testing-suites.md` and `build-and-test.md` were exercised on a `.sql` test path for the first time (T3.2 / T3.3 were `.sh`); both held up. No promotion / demotion. | T3.4 was the first .sql test path | No change. |

## What this retrospective is NOT

- Not a re-litigation of patch 077's design or the dossier — both shipped in `e80c209ade8` and `docs/aiven/patches/077-hide-secrets-system-mutations-command.md` and are the source of truth.
- Not a full T3.3-specific retrospective — the "catch-up on T3.3" section is best-effort compression after the fact. A future reviewer who needs full T3.3 detail should read the T3.3 dossier (`docs/aiven/patches/011-restrict-show-create-database-access.md`) and the relevant commits (`51de1456253` patch, `fc3538d0416` `deny-upstream-file-writes` fix, `12ccb680f2b` test-naming convention doc).
- Not the dispatch-prompt template itself. That's Finding D's deliverable, a separate Bootstrap commit before T3.5.
- Not a fix for the lost Joe-Lynch authorship in `e80c209ade8`. Per `AGENTS.md` we don't rebase or amend; the source-SHA + cherry-picked-from line preserves the traceability path for anyone who wants to look up the original.

## Pointers

- Dossier: `docs/aiven/patches/077-hide-secrets-system-mutations-command.md` (138 lines).
- Patch-port commit: `e80c209ade8` "Hide secrets in system.mutations.command column".
- Hook root-cause-fix commit: `8069f64f030` "hooks: parse subagentStop report body from .agent_transcript_path".
- Dispatch prompt (verbatim, scratch): `tmp/patch-077/dispatch-prompt.md` (861 lines).
- Worker scratch logs: `tmp/patch-077/*.log` (~25 files: drift identifier grep, file-history scans, hunk-context checks, cherry-pick output, patch-id source/staged, build logs, test pre/post, flip-pre/post verify logs, final-status logs, decomposition logs, equivalence logs for both sites + both test files).
- Stateless test: `tests/queries/0_stateless/9077_system_mutations_command_mask_secrets.{sql,reference}`.
- Archived worker report: `docs/aiven/uplifts/26.3/reports/toolu_01LpNLkS2cPr4HQjgmvDdTKf.md` (171 lines; backfilled).
- T3.3 patch-port commit: `51de1456253` "Restrict SHOW CREATE DATABASE access".
- T3.3 hook fix commit: `fc3538d0416` "hooks: fix deny-upstream-file-writes to read .tool_input.file_path".
- T3.3 test-naming convention doc commit: `12ccb680f2b` "docs(aiven): document 9<NNN>_<slug> test-naming convention".
- T3.2 retrospective (predecessor): `docs/aiven/uplifts/26.3/03-t3-2-patch-040-retrospective.md`.
- T2.2 inventory (source row): `docs/aiven/uplifts/26.3/inventory.md` row 077 (`a25b337024`).

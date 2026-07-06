# T3.3 — patch 011 dispatch retrospective

> **What this is:** The reflection-after-cost step (spec §10 step 12) applied to the third real patch dispatch — the first to exercise (a) a multi-file C++ port with named parent policy calls, (b) a parent-instructed Allman-brace style cleanup on top of a clean cherry-pick, (c) a multi-user `.sh` test pattern (3 grant scenarios), (d) the `set -e + && echo` short-circuit idiom that distinguishes pre/post by *missing-line-in-diff* rather than by *exit code*, and (e) the discovery of the `deny-upstream-file-writes.sh` silent-allow bug.
>
> **Date:** 2026-05-28 (backfilled).
>
> **Why this retro is backfilled.** T3.4 dispatched without writing a T3.3-specific retrospective — the process invariant "every dispatch gets its own reflection-after-cost doc before the next one starts" slipped (T3.4 retrospective Finding E acknowledged the slip). The "Catch-up on T3.3" section in `05-t3-4-patch-077-retrospective.md` (lines 14-25) captured five compressed findings; this document is the deferred standalone retro that re-derives those findings at full fidelity plus the load-bearing pieces the catch-up could not cover from memory.
>
> **Dispatch reference:** No `log.md` row was auto-populated (the `subagentStop` hook was still on v1 at dispatch time; the row would be `unknown / unknown` and is among the rows the header's "earlier `unknown` rows can be backfilled the same way" disclaimer refers to). Wall-clock from `tmp/patch-011/dispatch-start.txt`: `2026-05-24T19:41:28Z`. Wall-clock from `tmp/patch-011/dispatch-end.txt`: `2026-05-24T19:52:47Z`. **Dispatch duration: ~11 minutes** (slightly longer than the "~8 minutes" remembered in the catch-up section — the catch-up was approximate).
>
> **Outcome of T3.3:** `success` — patch ported with parent-instructed Allman-brace cleanup applied to `InterpreterShowCreateQuery.cpp` (so `byte_equivalent: false` driven by style, not semantics), test added with honest pre/post evidence pair, dossier shipped. The patch-port commit was rewritten twice by the user during commit hygiene: original SHA `bdfd3c7327b` (2026-05-25) → current SHA `51de1456253` (2026-05-25, via user-initiated `git rebase -i` to fold in a retrospective-line fix). The two cited Bootstrap commits driven by this dispatch are `fc3538d0416` (`deny-upstream-file-writes` hook fix, 2026-05-24, landed *during* the dispatch) and `12ccb680f2b` (`9<NNN>_<slug>` test-naming convention doc, 2026-05-25, landed *after* the dispatch but renaming T3.3's own test).

## Headline

T3.3 was the dispatch where the **parent C++ pre-review** went from "free-form commentary" to "three named policy calls baked into the dispatch prompt" — and the worker followed them verbatim, including a non-trivial Allman-brace cleanup that produced ~10 LOC of stylistic delta on top of the textual cherry-pick. The dispatch finished in ~11 minutes wall-clock against a 75-minute budget, and the only surprises were operational (a hook bug, a test-naming convention proposal, a stray `access/` directory) rather than C++ correctness. The patch itself is a security hardening — `SHOW CREATE DATABASE` access restriction so main-service users cannot read DSN credentials embedded in engine settings — and the test design is the first multi-user `.sh` shape in the T3.x series (three `clickhouse-client --user=<name>` invocations across three grant scenarios, asserting both presence of expected outputs and presence of specific Aiven-gate substrings in error messages).

Three side-discoveries from T3.3 each became durable infrastructure: (i) `deny-upstream-file-writes.sh` had been silently broken since authoring (wrong `jq` paths) — fixed mid-dispatch in `fc3538d0416`; (ii) upstream's `tests/queries/0_stateless/add-test` allocator would collide with our future rebases — the `9<NNN>_<slug>` Aiven convention was proposed during this dispatch and ratified after in `12ccb680f2b` (with retroactive rename of T3.2's `04206_*` → `9040_*` and T3.3's `04207_*` → `9011_*`); (iii) `clickhouse-server`'s `./access/` directory leaks into the worktree on every run — added to `.git/info/exclude` locally and documented in `build-and-test.md` §6.

## What worked in T3.3

1. **Parent C++ pre-review with three NAMED policy calls held up under worker execution.** Parent identified three concerns before dispatch (1: VIEW over-restriction shipped as-is; 2: system-table regression shipped as-is; 3: Allman-brace style cleanup applied during port) and embedded them in the prompt as numbered, scoped instructions. The worker did not re-litigate; the dossier's §3 item 8 captures the three documented limitations verbatim. This is the pattern T3.4+ retrospectives later cite as "parent's preflight + named policy calls" — T3.3 is where it was first proven.

2. **Allman-brace cleanup scoped strictly to the cherry-picked hunk did not leak.** Per parent policy call 3, the worker rewrote K&R braces to Allman braces ONLY in the hunk it just cherry-picked (`InterpreterShowCreateQuery.cpp`). It did NOT touch surrounding code that already used the wrong style — that would be scope creep. Net: ~7 lines added in `InterpreterShowCreateQuery.cpp` (stat `5 files changed, 330 insertions(+), 1 deletion(-)`); the style cleanup is contained, attributable to the patch, and clearly separable on review. This is the pattern Step 2.5 `[[STEP_2_5_STYLE_CLEANUP]]` later codified.

3. **Multi-user `.sh` test with three grant scenarios produced an unambiguous diff-based assertion.** The test runs `clickhouse-client --user=priv` and `clickhouse-client --user=nonpriv` against three queries each (`SHOW CREATE TABLE`, `SHOW CREATE DATABASE`, `SELECT create_table_query FROM system.tables`). Reference is 6 lines (the expected ordered emission across both users); test body emits ONLY when the right assertion holds (via `grep -F "..." > /dev/null && echo "..."`). Pre-patch: 3 lines emit (priv queries succeed) + 1 extra `0` line (nonpriv `system.tables` row was non-empty) → 4 actual vs 6 expected → diff non-empty → FAIL. Post-patch: 6 lines emit in order → diff empty → PASS. Pattern reference: `tests/queries/0_stateless/02561_temporary_table_grants.sh` (lifted-and-adapted).

4. **The `set -e + && echo` idiom proved robust for "missing-line distinguishes pre/post" tests.** A test shape this assertion-dense (3 users × 3 queries = 9 assertions, but only 6 emissions because some queries fire on both users) needs a way to fail-soft on a single missed assertion without aborting the whole script. The `cmd1 && cmd2` operator is the right tool: if `cmd1` fails, `cmd2` is skipped (the missing emission shows up in the diff) and `set -e` does NOT trigger (because the failing command is the LHS of `&&`, not a bare command). The test always runs to completion; the diff vs `.reference` is the ground truth. Worker discovered this contradicted the dispatch prompt's prose ("set -e kills the test on first failure"); test is still correct because the diff-vs-reference is what the runner checks (not the script's exit code).

5. **The patch-id verification correctly recorded `byte_equivalent: false` with a non-semantic justification.** Source-SHA stable patch-id ≠ staged-diff stable patch-id, because the worker rewrote Egyptian → Allman braces in the InterpreterShowCreateQuery hunk. The decomposition check (`diff <(git show <sha>) <(git diff --cached) | grep -E '^[-+]' | grep -v '^[-+]{3}'`) shows the differences are ALL whitespace/style, no semantic delta. Dossier §6 records `byte_equivalent: false` with the justification "style cleanup per parent policy call 3". This is the first T3.x dispatch where `byte_equivalent: false` had a non-conflict-resolution justification (T3.5 later did the same for the LOC-shift case via `git patch-id --stable`).

6. **Warm-cache build numbers held to the runbook's predictions.** From `tmp/patch-011/build-cache-state.txt`: post-patch full build 29 s, pre-patch incremental rebuild 17 s, post-patch incremental restore rebuild 18 s. These match the warm-cache reference in `build-and-test.md` §3 (T3.2 was 66 s / 22 s / 14 s) — the patch-011 build is FASTER than T3.2 because the patched files are smaller compilation units than T3.2's `HTTPHandlerFactory.cpp`. **Strong signal:** for the 78-patch / 3-week target, warm-cache incremental builds dominate the procedure cost and stay sub-30-second per build. The wall-clock variance across the T3.x series is dominated by escalations and review, not by builds.

7. **Schema constraint 6 (`tests.added: yes` evidence requirement) held without amendment.** Worker produced `paths` (the two test files at the dispatch-time prefix `04207_*`), `pre_patch_fail_verified: true`, `post_patch_pass_verified: true`, and Evidence section with verbatim excerpts of both logs. Same as T3.2 — every field of the schema's `tests.added: yes` branch populated correctly on first contact. T3.3 was the first multi-user-test demonstration of this; the schema generalized cleanly.

8. **The single-axis worktree-flip technique held under two-file pre/post.** First T3.x dispatch with `$PATCHED_FILES` containing TWO files (`InterpreterShowCreateQuery.cpp` + `StorageSystemTables.cpp`). The procedure (`git restore --worktree --source=HEAD $PATCHED_FILES` → rebuild → run pre-test → `git restore --worktree $PATCHED_FILES` → rebuild → continue) executed without surprises. `tmp/patch-011/final-worktree-diff.log` is empty (worktree matches index post-flip-back); `tmp/patch-011/flip-pre-verify.log` shows both files restored to HEAD content for the pre-patch run. The closed-set discipline (derive `PATCHED_FILES` once, never re-glob) prevented the worktree-flip from touching the new test files.

## What surfaced

### A. `deny-upstream-file-writes.sh` was silently broken since authoring

**Symptom:** during T3.3 dispatch the worker attempted to write a file inside `tmp/patch-011/` and was unexpectedly denied by the hook (with an opaque error). Parent investigated: every prior dispatch's writes should have been denied if the hook's logic were correct, but they weren't — meaning the hook had been silently *allowing* every write since its authoring (including writes that should have been denied to the never-touch list).

**Root cause:** the hook used `.input.path` to extract the target path from the `Edit/Write/MultiEdit` tool input. Cursor's actual JSON shape is `.tool_input.file_path`. `jq '.input.path // ""'` evaluates to `""` on every input; the subsequent `case "" in ...) deny ;; esac` never matches any of the deny patterns; the hook exits 0; the write proceeds. The hook had no observable side-effect on success, so the silent allow went undetected.

**Decision:** fixed in `fc3538d0416` ("hooks: fix deny-upstream-file-writes to read .tool_input.file_path"). Wrote a `tee` to the hook's debug log on EVERY firing, so future regressions surface as "no log entry → hook didn't fire" rather than as "no deny → must be allowing correctly".

**Lesson:** fail-closed hooks that fail silently (no observable deny, but also no observable allow logging) are worse than no hook — they create false confidence. Pattern to apply going forward: every `failClosed: true` hook MUST emit at least one observable line per firing (a `tee` to a per-hook log file under `tmp/hook-log/` or equivalent), so that "hook didn't fire" is distinguishable from "hook fired and allowed".

**Rule-of-three count:** 1 of 3. This is the first silent-allow-hook discovery. The probe-block pattern landed in T3.8-T3.12 (acb4d88fc70) for the `subagentStop` hook's third regression is the next-instance candidate — but probe-blocks are diagnostic, not preventive. A second silent-allow-on-a-different-hook discovery would lift the counter; codify the "every failClosed hook tees on every firing" rule into the hook-authoring template when n=3.

### B. `9<NNN>_<slug>` Aiven test-naming convention was proposed and adopted mid-dispatch

**Symptom:** worker invoked `tests/queries/0_stateless/add-test restrict_show_create_access.sh` per the dispatch prompt's instructions. The allocator scanned the directory, found the highest existing prefix (`04205_*` at the time), and produced `04207_restrict_show_create_access.{sh,reference}` (T3.2's `04206_*` was already allocated). The names worked but parent immediately flagged a future-rebase problem.

**Root cause:** upstream's `add-test` is a strict monotonic allocator. The moment any Aiven test exists at a higher prefix than the upstream max, every subsequent `add-test` call (upstream OR downstream) would jump there — polluting upstream's allocator for the rest of the rebase. On the next LTS rebase, upstream's allocator would eventually claim numbers we used downstream, forcing per-rebase renames.

**Decision (made during T3.3, ratified after):** allocate Aiven tests in the `9<NNN>_<slug>` partition where `<NNN>` is the patch dossier number. The `9XXXX` range is virgin territory upstream-wide (0 of ~20,500 tests use a non-`0` leading digit on this branch); it provides a permanent reservation with no need to modify upstream's `add-test`. The runner accepts the non-monotonic prefix via try/except fallback at `tests/clickhouse-test:3121`. Retroactively renamed T3.2's `04206_disable_replicas_status_default → 9040_disable_replicas_status_default` and T3.3's `04207_restrict_show_create_access → 9011_restrict_show_create_access` in the same per-uplift cleanup commit that the user folded into `51de1456253`. Convention documented in `12ccb680f2b` (`docs(aiven): document 9<NNN>_<slug> test-naming convention`) and now lives in `docs/aiven/runbooks/testing-suites.md §4.1`.

**Lesson:** allocators that scan-and-increment are friendly to single-fork repos but adversarial to multi-fork ones. The right mitigation is partition-reservation (claim a high prefix range that upstream demonstrably doesn't use), not allocator modification (which fights upstream every rebase).

**Rule-of-three count:** VERIFIED at the time of T3.3 retroactive rename (n=2 simultaneous uses: T3.2's test + T3.3's test, both renamed in the same commit). All subsequent T3.x stateless tests (T3.5 patch 073, T3.7 patch 010, T3.15 patch 042) used the convention without re-litigation. The integration-test analogue `test_aiven_<slug>/` (`docs/aiven/runbooks/testing-suites.md §4.4`) followed the same partition-reservation logic at T3.6 and reached its own VERIFIED-with-discipline 3/3 at T3.14 (see Phase C codification commit `6e97bd782ed`).

### C. `set -e` mechanic correction — `cmd1 && cmd2` does NOT trigger errexit on cmd1 failure

**Symptom:** parent's dispatch prompt described the test mechanism as "if `grep` fails, `set -e` kills the test and the runner detects the failure". Worker's test, written from this prose, used `grep -F "..." > /dev/null && echo "..."`. The test produced the expected pre-patch FAIL diff (missing lines) — but the script DID NOT abort on the first failing grep; it continued and produced subsequent emissions, and the diff vs reference was what failed.

**Root cause:** bash semantics. When the failing command is the LHS of `&&`, `errexit` is suppressed (because `&&` checks the exit code, which is the use case `set -e` was originally for). The script continues past the failing grep; the `echo` is skipped; the next grep+echo statement runs normally; etc. The diff at the end compares actual stdout vs `.reference` and that's the ground truth.

**Decision (no code change required):** the test design is correct — it just operates via diff-vs-reference, not via exit code. The dispatch prompt's prose was misleading; corrected for T3.4+ to read "the missing emissions show up in the diff" rather than "`set -e` kills the test". No runbook change needed (`testing-suites.md` §3 already describes diff-vs-reference as the assertion mechanism).

**Lesson:** when designing a test, the OBSERVABLE that the runner consumes is the assertion. For stateless tests, that observable is the stdout-vs-reference diff; the script's exit code is incidental. Internal control flow (`set -e`, `&&`, `;`, `||`) is for the script's own correctness, not for the runner's failure detection.

**Rule-of-three count:** n/a — this is a clarification of bash semantics, not a discipline. The mistaken prose was corrected in-place.

### D. `clickhouse-server`'s `./access/` directory leaks into the worktree on every run

**Symptom:** every `git status` after a server start shows `Untracked files: access/`. The directory survives server restart and accumulates contents (SQL-based access-control state — user profiles, grants, etc.).

**Root cause:** `clickhouse-server` creates `./access/` (relative to its CWD) for the SQL-based access-control storage backend when no explicit path is configured. The integration test suite, the stateless test runner, and ad-hoc local server starts all use the repository root as CWD by default → `./access/` lands there.

**Decision:** added `access/` to `.git/info/exclude` (local-only — does NOT pollute the repo's `.gitignore` which is upstream-owned). Documented as a known carry-over in `docs/aiven/runbooks/build-and-test.md` §6 alongside the `cmake --fresh` recipe and the `~/.local/lib/python3.13/site-packages/` reset.

**Lesson:** server-side scratch directories that land in the worktree on default-config runs are best handled in `.git/info/exclude` (local) rather than `.gitignore` (upstream-shared). The latter is the never-touch list per `docs/aiven/AGENTS.md` §3.

**Rule-of-three count:** n/a — environmental quirk with mechanical resolution, no discipline to propagate.

### E. User-initiated `git rebase -i` rewrote the patch-port commit (`bdfd3c7327b` → `51de1456253`)

**Symptom:** the dispatch landed as commit `bdfd3c7327b` on 2026-05-25. Later the same day, the user ran `git rebase -i` to fold a retrospective-line fix into the patch-port commit body, producing the rewritten SHA `51de1456253`. Both SHAs are visible in `git log --all --oneline --grep='Restrict SHOW CREATE'` (5 SHAs total — the predecessor chain across the user's interactive rebase rounds).

**Why this is worth noting:** `docs/aiven/AGENTS.md` and the workspace's project `AGENTS.md` both prohibit AGENT rebases ("When working with a branch, do not use rebase or amend — add new commits instead"). The user CAN rebase their own work — that's not the prohibition. But the chain of rewritten SHAs means any downstream reference to "patch 011's port commit" must distinguish between (a) the SHA when the work first landed and (b) the current SHA after the user's rebase. The patch dossier at `docs/aiven/patches/011-restrict-show-create-database-access.md` doesn't currently record the rewrite — only the source SHA on 25.8-aiven is recorded.

**Decision:** add a "Lineage rewrite" note to the dossier §0 (table cell `26.3-aiven > First-carry SHA on aiven branch`) capturing both SHAs and the rebase reason. Mechanical, done as part of this retrospective's commit (the dossier currently reads `(staged; to be filled in by human at commit time)` which the user has since updated to a single SHA — but the human-edit retained only the post-rebase SHA, not both).

**Lesson:** if a patch-port commit is rewritten between landing and the retrospective, the dossier should record BOTH SHAs (predecessor → current) so future archeologists don't waste time wondering why their `git log -1 <retro-cited-SHA>` returns no such commit. The retrospective is the right place to capture this audit trail, since the retrospective is written AFTER the rewrites have settled.

**Rule-of-three count:** 1 of 3. Future patches with user-rebased SHAs would lift the counter; the codification target is the patch-dossier template's §0 (`docs/aiven/skills/patch-dossier-template.md`) — add a "Predecessor SHAs (if rewritten)" line to the table schema. Defer until n=3.

### F. The retrospective-before-next-dispatch invariant slipped

**Symptom:** T3.4 dispatched on 2026-05-25 with no T3.3 retrospective on disk. The "Catch-up on T3.3" section in T3.4's retro (lines 14-25) was a partial mitigation written from memory; T3.4's own Finding E flagged the slip explicitly and recommended writing a standalone T3.3 retro "when convenient". "When convenient" turned out to be three days later, during Phase B of the packaging plan.

**Root cause:** the discipline "every dispatch gets its own retro before the next one starts" was a human-enforced norm, not a tool-enforced check. The parent agent didn't track "retro for T3.X exists" as a precondition for dispatching T3.X+1. The human was occupied with the immediate next dispatch and the retrospective slipped.

**Decision:** this retrospective IS the deferred resolution. Beyond that, two forward-looking options:

- **Option 1 (lightweight):** add a "Step 0 preflight" item to the dispatch-prompt template — "parent verifies retros/<NN-tN.X-prev-dispatch>.md exists before assembling the prompt for tN.X+1". Human-enforced; no tool support. Cheap to add, easy to forget.
- **Option 2 (tool-enforced):** add a check to the parent's pre-dispatch routine that scans `docs/aiven/uplifts/<lts>/*-retrospective.md` for the most-recent T3.X tag and refuses to dispatch T3.X+1 if there's no matching file. Harder to wire up; harder to bypass.

**Recommendation:** defer the codification until rule-of-three. T3.3-slip was the first instance; one more slip in a future uplift would justify Option 1; a third would justify Option 2. For now, this retrospective's existence + the retro-04 Finding E narrative form the cultural record.

**Rule-of-three count:** 1 of 3 of skipped-retros. T3.3 is the only known instance; T3.4-T3.15 all got retros (some bundled into multi-dispatch retros 11/12, but each dispatch's findings are represented).

## What this retro does NOT do

- **Does not re-author the dossier.** `docs/aiven/patches/011-restrict-show-create-database-access.md` is the source of truth for the patch's C++ analysis, drift findings, test design, and per-uplift outcome. This retro adds the meta-process record around that dossier; the dossier itself is not re-litigated.
- **Does not amend the patch.** Commits `bdfd3c7327b` (predecessor) and `51de1456253` (current) are the patch-port commits; this retrospective only adds a forward-pointing trace in the retros series.
- **Does not retroactively backfill `log.md` row for T3.3.** That's a separate hygiene task — Phase A backfilled T3.13/T3.14/T3.15 from on-disk JSONLs; the same procedure could be applied to T3.1/T3.2/T3.3/T3.8-T3.12 when an operator wants completeness. Out of scope here.
- **Does not codify the silent-hook rule (Finding A) or the user-rebase-lineage rule (Finding E)** into runbooks. Each is at counter 1/3; per the project's rule-of-three discipline, codification waits for n=3.

## Pointers

- **Patch-port commits**: predecessor `bdfd3c7327b` (2026-05-25) → current `51de1456253` (2026-05-25, post user rebase). Both visible in `git log --all --oneline --grep='Restrict SHOW CREATE'`.
- **Dossier**: `docs/aiven/patches/011-restrict-show-create-database-access.md` (260 lines, full §1-§6 coverage).
- **Tests**: `tests/queries/0_stateless/9011_restrict_show_create_access.{sh,reference}` (post-rename; original dispatch-time name was `04207_*`, renamed in `12ccb680f2b`).
- **Dispatch scratch**: `tmp/patch-011/` — full procedure artifacts (`drift-*.log`, `cherrypick.log`, `build-*.log`, `flip-*.log`, `final-status.log`, `final-worktree-diff.log`, `dispatch-prompt.md`).
- **Related Bootstrap commits driven by this dispatch**:
  - `fc3538d0416` (2026-05-24) — `hooks: fix deny-upstream-file-writes to read .tool_input.file_path` (Finding A).
  - `12ccb680f2b` (2026-05-25) — `docs(aiven): document 9<NNN>_<slug> test-naming convention` (Finding B).
- **Catch-up source**: `05-t3-4-patch-077-retrospective.md` lines 14-25 ("Catch-up on T3.3 (no separate retrospective was written)"). This retro replaces that section's role as the load-bearing T3.3 record; the catch-up section stays as a forward-pointer to here for readers who land there first.
- **Predecessor retro**: `03-t3-2-patch-040-retrospective.md` (T3.2 patch 040).
- **Successor retro**: `05-t3-4-patch-077-retrospective.md` (T3.4 patch 077; T3.3 was skipped at the time).
- **Runbooks promoted/exercised**: `docs/aiven/runbooks/testing-suites.md` §3 (`.sh` anatomy), §4.1 (the convention this dispatch proposed), §5 (pre/post evidence pair), §6 (worktree-flip — first 2-file `$PATCHED_FILES` use). `docs/aiven/runbooks/build-and-test.md` §3 (build env), §6 (`access/` carry-over note added).

## Mentor lesson (per the C++ Architect rule)

**Intuition.** A C++ pre-review that produces THREE NAMED policy calls + a worker that follows them VERBATIM is more efficient than a C++ pre-review that produces FREE-FORM commentary + a worker that has to translate commentary into action. Named policy calls compress decision authority into the prompt; free-form commentary leaves the worker to re-derive intent under time pressure.

**Mechanism.** Each named policy call is a constraint on the worker's degrees of freedom (e.g., "DO NOT touch surrounding K&R braces; ONLY rewrite the hunk you just cherry-picked"). Constraints are cheap for the parent to enumerate (the parent already did the C++ review) and expensive for the worker to re-derive (the worker doesn't share the parent's context window). The dispatch prompt is the right place to make this transfer: write down the constraints once, in numbered form, and the worker can execute them without spawning sub-decisions.

**System consequence.** The 8-section C++ review checklist (`docs/aiven/skills/cpp-review-checklist.md`) is the parent's tool; the named-policy-call protocol is the parent's *output*. The protocol scales: T3.4 had multi-outcome cherry-pick policy calls; T3.7 had ship-without-test policy calls; T3.10 had ship-without-test no-regression policy calls; T3.13 had `test_design_blocked` policy escalations. Each policy class was named and embedded; each worker executed against the constraint, not the prose.

**Today you learned.** That `cmd1 && cmd2` suppresses `set -e` on `cmd1` failure — which sounds like a bug but is the correct behavior given that `&&` already checks the exit code. The right mental model: `set -e` covers UNguarded statements; `&&`, `||`, `;` are all guards.

**Rule of thumb.** When a discipline first emerges (parent C++ pre-review with named policy calls, in this case), write the canonical version into the next dispatch's prompt — don't wait for rule-of-three to extract into a template. The rule-of-three is for *codifying into runbooks*; the prompt-level prose is cheap to evolve in place and the worker reads it once per dispatch anyway.

**Next rabbit hole.** Tool-enforced retrospective-before-next-dispatch (Finding F Option 2). The current human-enforced norm has one observed slip in 15 dispatches (T3.3) and one near-slip mitigated by the "Catch-up" section (T3.4). At what counter does the tool support pay for its complexity? Probably n=3 actual slips OR n=1 slip with material findings lost — whichever comes first.

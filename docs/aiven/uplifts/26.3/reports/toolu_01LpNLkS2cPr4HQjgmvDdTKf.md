<!-- subagentStop archive | source=transcript | status=completed | type=general-purpose | ts=2026-05-25T11:09:51Z | backfilled=true -->

---
outcome: success
patch_slug: hide-secrets-system-mutations-command
source_sha: a25b337024f16cfe7a87f0d59413c7d788dd9972
proposed_commit:
  staged_files:
    - docs/aiven/patches/077-hide-secrets-system-mutations-command.md
    - src/Storages/MergeTree/ReplicatedMergeTreeQueue.cpp
    - src/Storages/StorageMergeTree.cpp
    - tests/queries/0_stateless/9077_system_mutations_command_mask_secrets.reference
    - tests/queries/0_stateless/9077_system_mutations_command_mask_secrets.sql
  commit_message: |
    Hide secrets in system.mutations.command column

    (cherry picked from commit a25b337024f16cfe7a87f0d59413c7d788dd9972)

    Test: renamed 03990_* to 9077_* per Aiven test-naming convention
    (docs/aiven/runbooks/testing-suites.md §4.1).
  byte_equivalent: false
tests:
  added: yes
  kind: stateless
  paths:
    - tests/queries/0_stateless/9077_system_mutations_command_mask_secrets.sql
    - tests/queries/0_stateless/9077_system_mutations_command_mask_secrets.reference
  upstream_reference:
  pre_patch_fail_verified: true
  post_patch_pass_verified: true
  justification:
escalation_reason: none
---

## Tier results

- Tier 1 (textual cherry-pick): **pass — clean (Outcome A)**. Git's diff3 auto-merge tolerated the 26.3-only `parts_in_progress[_names]` field drift at both Site 1 and Site 2; no `UU` markers; no manual edit needed. T2 inventory's `cherry_pick_clean=no` flag was a false alarm from the strict 3-line context check.
- Tier 1.5 (test rename): **pass** — renamed `03990_*` to `9077_*` per Aiven §4.1 via `git mv`; renamed test content byte-equivalent to source (verified `diff` against `git show a25b337024:...` — both `.sql` and `.reference` empty diff).
- Tier 2 (semantic patch-id): **pass with `byte_equivalent: false`** — patch-ids differ (`f2406fb75c218bfc8412830439559a8cab7e66ff` vs `b1d1e5756ca328a1f40fb22f76337358fcb0549d`) due to (a) the test rename and (b) context-only delta at both sites from the 26.3-only `parts_in_progress[_names]` fields. Schema-runbook decomposition (`grep -E '^[-+]' | grep -v '^[-+]\{3\}'`) shows zero difference between source and staged +/- lines.
- Tier 3 (build + test): **pass** — full post-patch build 41 s warm-cache (22 ninja steps); incremental pre-patch rebuild 17 s (19 steps); incremental post-patch restore rebuild 18 s (19 steps). Post-patch test `9077_*: OK 0.13 sec`; pre-patch test `9077_*: FAIL 0.13 sec` with diff `1\n0` vs `0\n1`.

## Evidence

### Cherry-pick outcome (`tmp/patch-077/cherrypick.log`)

```text
Auto-merging src/Storages/MergeTree/ReplicatedMergeTreeQueue.cpp
Auto-merging src/Storages/StorageMergeTree.cpp
---STATUS---
Changes to be committed:
	modified:   src/Storages/MergeTree/ReplicatedMergeTreeQueue.cpp
	modified:   src/Storages/StorageMergeTree.cpp
	new file:   tests/queries/0_stateless/03990_system_mutations_command_mask_secrets.reference
	new file:   tests/queries/0_stateless/03990_system_mutations_command_mask_secrets.sql
```

### Semantic decomposition (`tmp/patch-077/pm-decomposition.log`)

```text
(empty — source and staged +/- content byte-identical)
```

(Full per-site diff in `tmp/patch-077/site1-equivalence.log` and `tmp/patch-077/site2-decomposition.log` shows only metadata, `@@` line numbers, and trailing-context-line identifier differences — i.e., `parts_in_progress` / `parts_in_progress_names` context lines that exist only in 26.3.)

### Rename preserved content (`tmp/patch-077/test-sql-equivalence.log` and `test-ref-equivalence.log`)

```text
(both files empty — diff of `git show a25b337024:tests/queries/0_stateless/03990_*` vs `tests/queries/0_stateless/9077_*` shows zero difference for both .sql and .reference)
```

### Post-patch test PASS (`tmp/patch-077/test-postpatch.log`)

```text
Using queries from '/home/tilman.moeller/projects/ClickHouse/tests/queries' directory
Connecting to ClickHouse server... OK
Connected to server 26.3.10.1 @ 3f2c4441653b8fcee3f1ee66270433af0c869008 v26.3.10.62-lts-aiven-dev
Found 1 parallel tests and 0 sequential tests
Running about 1 stateless tests (Process-3).
9077_system_mutations_command_mask_secrets:                             [ OK ] 0.13 sec.

1 tests passed. 0 tests skipped. 0.15 s elapsed (Process-3).
All tests have finished.
```

### Pre-patch test FAIL (`tmp/patch-077/test-prepatch.log`)

```text
9077_system_mutations_command_mask_secrets:                             [ FAIL ] 0.13 sec.
Reason: result differs with reference:
--- .../tests/queries/0_stateless/9077_system_mutations_command_mask_secrets.reference
+++ .../tests/queries/0_stateless/9077_system_mutations_command_mask_secrets.stdout
@@ -1,2 +1,2 @@
-0
 1
+0

Having 1 errors! 0 tests passed. 0 tests skipped. 0.14 s elapsed (Process-3).
```

Interpretation: pre-patch first SELECT (`%TOPSECRET%`) returns 1 (literal key leaked); second SELECT (`%[HIDDEN]%`) returns 0 (no mask placeholder). Post-patch they swap to 0 and 1 respectively. Both SELECTs change → assertion pair is robust.

### Worktree-flip postcondition (`tmp/patch-077/flip-post-verify.log`)

```text
--- worktree-vs-index (must be EMPTY) ---
(empty — postcondition satisfied)
--- staged-stat (must show both files still staged) ---
 src/Storages/MergeTree/ReplicatedMergeTreeQueue.cpp | 5 +----
 src/Storages/StorageMergeTree.cpp                   | 2 +-
 2 files changed, 2 insertions(+), 5 deletions(-)
```

## What I did

- **Step 0 — Preflight.** Confirmed HEAD on `v26.3.10.62-lts-aiven-dev`; `git status --ignore-submodules` shows only the allowed `M .gitignore` and `?? access/`; source SHA exists. Submodule-state entries (`M contrib/NuRaft …`) are git's report of untracked content inside contrib submodules, not actual content changes — benign and ignored per `git status --ignore-submodules` confirmation.
- **Step 1 — Drift analysis.** All identifiers present on HEAD (including `formatForLogging` at `IAST.h:356`, `formatWithSecretsOneLine` at `IAST.h:358`, and `parts_in_progress_names` with 3 occurrences in `StorageMergeTree.cpp`). 127 commits touched `ReplicatedMergeTreeQueue.cpp` and 955 touched `StorageMergeTree.cpp` between 25.8 and 26.3, but none added `formatForLogging` to or modified `getMutationsStatus` in either file (verified by pickaxe `-S`). 0 commits matched the topic grep. Hunk-context check confirmed Site 1's 4-line shape and Site 2's targeted line are preserved verbatim; Site 2's trailing context has `parts_in_progress_names,` between `block_numbers_map,` and `parts_to_do_names,`. Conclusion: `still-needed-applies-with-conflict-at-site-2` (revised to `still-needed-applies-cleanly` after Step 2).
- **Step 2 — Cherry-pick.** `git cherry-pick --no-commit -x a25b337024…` exit 0; git auto-merge handled the trailing-context drift at both Site 1 (different field name on the last context line) and Site 2 (`parts_in_progress_names` insertion) without producing conflict markers. All four files staged cleanly.
- **Step 2.5 — Rename.** `git mv 03990_system_mutations_command_mask_secrets.{sql,reference} 9077_…`; rendered in `git status` as `new file: 9077_…` (since the source path was never tracked in HEAD, the rename collapses to a fresh `add` under the new path — correct git semantics). Verified no stale `03990_*` on disk or in the staged set.
- **Step 3 — Patch-id verification.** Patch-ids differ as expected (test rename + context drift). Schema-runbook decomposition (`grep -E '^[-+]' | grep -v '^[-+]\{3\}'`) shows zero difference between source and staged +/- lines. Renamed test content byte-equivalent to source (`diff` of `git show a25b337024:…03990_*` vs the new `9077_*` — empty for both `.sql` and `.reference`).
- **Step 4 — Build.** Post-patch full build 41 s, 22 ninja steps; both Site 1 and Site 2 `.o`s rebuilt; final `clickhouse` binary relinked. Warm cache (recorded in `tmp/patch-077/build-cache-state.txt`).
- **Step 5 — Test design.** Adopted the shipped test verbatim. Recorded the replicated-path coverage gap (test uses non-replicated MergeTree → exercises Site 2 only; Site 1 verified by code review) and the other-secret-types gap (only `encrypt(...)` is tested; `s3(...)`, named-collection passwords, etc. not exercised but the underlying masking framework treats all `is_secret_arg` AST nodes uniformly).
- **Step 6 — Evidence pair.**
  - 6a: post-patch `9077_*: OK 0.13 sec`.
  - 6b: `git restore --worktree --source=HEAD $PATCHED_FILES` (two files); worktree shows inverse of staged diff.
  - 6c: incremental rebuild 17 s; both `.o`s rebuilt (mtime 13:03:35; ninja's TTY-overwriting status output isn't fully captured by `tee` to file for one of the steps, but mtime confirms rebuild).
  - 6d: restart server (initial attempt hit stale-process + port-9234 collision after my `kill -INT` killed the wrong PID — the bash wrapper instead of the actual server child; recovered with `pkill -KILL -f`). Pre-patch `9077_*: FAIL 0.13 sec` with diff `1\n0` vs `0\n1`.
  - 6e: `git restore --worktree $PATCHED_FILES`; `git diff -- $PATCHED_FILES` empty; staged set intact.
  - 6f: incremental rebuild 18 s; binary restored to post-patch.
  - Stopped local server cleanly.
- **Step 7 — Dossier.** Authored `docs/aiven/patches/077-hide-secrets-system-mutations-command.md` filling all 7 sections per the template; staged with `git add`.
- **Step 8 — Verification.** Five expected staged entries present; no stale `03990_*` anywhere; `git diff -- $PATCHED_FILES` empty; source files in post-patch state at both call sites with `parts_in_progress_names` preserved.

## Proposed next step

**Ready for human commit.**

**Source author/committer** (per T3.2 Finding A — LOAD-BEARING because author ≠ local human):

```text
Author: Joe Lynch <joelynch112@gmail.com>
Committer: Joe Lynch <joelynch112@gmail.com>
```

To preserve attribution, the human should use:

```bash
git commit --author="Joe Lynch <joelynch112@gmail.com>" -F - <<'EOF'
Hide secrets in system.mutations.command column

(cherry picked from commit a25b337024f16cfe7a87f0d59413c7d788dd9972)

Test: renamed 03990_* to 9077_* per Aiven test-naming convention
(docs/aiven/runbooks/testing-suites.md §4.1).
EOF
```

A bare `git commit -c CHERRY_PICK_HEAD` would credit `--author` correctly (since `CHERRY_PICK_HEAD` carries Joe's authorship) but the human should verify with `git log -1 --format='%an <%ae> | %cn <%ce>'` after commit. If the human wants to drop the `(cherry picked from …)` provenance line they may; per the `-x` flag this line was added by git automatically into `CHERRY_PICK_HEAD`'s message template.

**Open items the human may want to consider** (none are blockers):

1. **Local-only `.gitignore` drift** (`M .gitignore`) is unchanged from the dispatch precondition — not part of this patch, not staged.
2. **`access/` untracked dir** (known server-leak from T3.2) is unchanged — not part of this patch.
3. **Contrib submodules show `(untracked content)`** in `git status` — these are untracked files inside the submodule directories (e.g., `asio/` inside `contrib/NuRaft`), not content changes; `git status --ignore-submodules` confirms there is no real drift here. Likely build-artifact residue. Not in scope.
4. **Test runtime artifacts** `tests/queries/0_stateless/9077_*.{stdout,stderr}` exist on disk from the pre-patch test run; they are not tracked and not shown in `git status` Untracked (the test-runner's `.gitignore` covers them). Not in scope; cleaning them up is optional.
5. **Runbook update suggestion** (for the future): T3.4's cherry-pick was clean despite parent's pre-flight predicting Outcome B. Git's diff3 auto-merge is more tolerant of trailing-context drift than the 3-line strict-context check `cherry_pick_clean` uses. This is the second false-alarm-from-cherry_pick_clean signal worth tracking in the T3.X retrospective — if it accumulates, the T2 inventory's `cherry_pick_clean` flag could be downgraded from "expect conflict" to "expect possible conflict, run cherry-pick to find out".

**Learning log:**

- **Today you learned:** (1) `IAST::formatForLogging` is the canonical secret-masking AST formatter — the docstring at `src/Parsers/IAST.h:351-355` makes the "always hides secrets" contract explicit, and the inconsistent naming (`formatWithSecretsOneLine` is the *leaky* variant despite the name reading either way) is a known smell upstream knows about. (2) Git's diff3 auto-merge can tolerate trailing-context drift that the `cherry_pick_clean` 3-line strict-context check rejects — running the cherry-pick is sometimes cheaper than parent's textual pre-flight predicts.
- **Rule of thumb:** when porting a patch that touches a struct initializer, check whether the struct has gained new fields in the new LTS; new trailing-context fields will look like a conflict to strict checks but auto-merge handles them silently.
- **Next rabbit hole:** survey the 10+ remaining `formatWithSecretsOneLine` call sites in the codebase to ask "which of them are also user-observable in a system table?" — there may be sibling leaks worth tracking as future Aiven patches.

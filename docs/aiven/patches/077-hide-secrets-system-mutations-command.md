# Patch 077 — hide-secrets-system-mutations-command

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | (unknown — to be researched at 25.X dossier merge) | (unknown) | (original carry, or "ported from 24.x") |
| 25.8-aiven | `a25b337024f16cfe7a87f0d59413c7d788dd9972` | Joe Lynch &lt;joelynch112@gmail.com&gt; (author and committer) | (the version we're porting FROM) |
| 26.3-aiven | `patch-port(077)` | T3.4 worker (Claude Opus 4.7) | `byte-equivalent: false` — test renamed to `9077_*` per Aiven §4.1; context-only delta at both sites from 26.3-only `parts_in_progress[_names]` fields |

The 26.3-aiven carry is its `patch-port(077)` commit (find it with `git log --grep '^patch-port(077)'`); per `commit-hygiene.md` §4 the `-aiven-dev` branch is resliced at each LTS transition, so we pin the stable subject handle, not a SHA that would re-hash.
## 1. Purpose

Hide secrets in the `command` column of `system.mutations`. Without this patch, `SELECT command FROM system.mutations` leaks literal secret arguments (encryption keys, S3 credentials, named-collection passwords, and anything else marked `is_secret_arg` on the AST) to any role with `READ` on `system.mutations`. With the patch, those literals render as `[HIDDEN]`, matching the masking already applied to `system.query_log.query` since 22.x.

The patch swaps the AST formatter at the two `getMutationsStatus` call sites — one in `ReplicatedMergeTreeQueue` (replicated path) and one in `StorageMergeTree` (non-replicated path) — from helpers that include secrets (`formatWithSecretsOneLine` and a manual `WriteBufferFromOwnString` / `IAST::FormatSettings(/*one_line=*/true)` / `IAST::format` triple) to the canonical `IAST::formatForLogging`, whose docstring at `src/Parsers/IAST.h:351-355` guarantees it always hides secrets. No setting is introduced; the change is unconditional, intentionally matching the unconditional masking of `system.query_log.query`.

Source SHA on `v25.8.18.1-lts-aiven`: `a25b337024f16cfe7a87f0d59413c7d788dd9972` (from `docs/aiven/uplifts/26.3/inventory.md` row 077).
Original author: `Joe Lynch <joelynch112@gmail.com>` (per `git log --format='%an <%ae>'`).
Original purpose (verbatim source commit body): `Hide secrets in system.mutations.command column` (one-line subject, no body, no trailers).

## 2. Upstream-drift findings

### Commands run

```bash
git grep -c -- '<identifier>' -- 'src/Storages/' 'src/Parsers/'   # for each of: ReplicatedMergeTreeQueue, StorageMergeTree, MergeTreeMutationStatus, MutationCommand, WriteBufferFromOwnString, FormatSettings, formatForLogging, formatWithSecretsOneLine, parts_in_progress_names

git log v25.8.18.1-lts..v26.3.10.62-lts -- src/Storages/MergeTree/ReplicatedMergeTreeQueue.cpp --oneline   # 127 commits
git log v25.8.18.1-lts..v26.3.10.62-lts -- src/Storages/StorageMergeTree.cpp --oneline                    # 955 commits

git log v25.8.18.1-lts..v26.3.10.62-lts --grep 'mutations\.command\|system\.mutations.*mask\|mutations.*secret\|formatForLogging.*mutation' --oneline   # 0 matches
git log v25.8.18.1-lts..v26.3.10.62-lts -p -S'formatForLogging'   -- src/Storages/MergeTree/ReplicatedMergeTreeQueue.cpp src/Storages/StorageMergeTree.cpp   # 0 matches
git log v25.8.18.1-lts..v26.3.10.62-lts -p -S'getMutationsStatus' -- src/Storages/MergeTree/ReplicatedMergeTreeQueue.cpp src/Storages/StorageMergeTree.cpp   # 0 matches

rg -n -B 1 -A 14 'for \(const MutationCommand & command : entry\.commands\)' src/Storages/MergeTree/ReplicatedMergeTreeQueue.cpp
rg -n -B 1 -A 12 'result\.push_back\(MergeTreeMutationStatus' src/Storages/StorageMergeTree.cpp
```

### Findings

- **Identifier inventory**: every identifier the patch references is present on `v26.3.10.62-lts` HEAD.
  - `formatForLogging`: present in `src/Parsers/IAST.h:356` and `src/Parsers/IAST.cpp` (and used in ~10 other places already). The patch can call it safely from both sites.
  - `formatWithSecretsOneLine`: still present in `src/Parsers/IAST.h:358` and used in 10+ other call sites. The patch removes ONLY the `StorageMergeTree::getMutationsStatus` call; the other uses are out of scope.
  - `parts_in_progress_names`: PRESENT (3 occurrences in `src/Storages/StorageMergeTree.cpp` and 1 in `src/Storages/MergeTree/MergeTreeMutationStatus.h`). This is a 26.3-only field added to `MergeTreeMutationStatus` between `block_numbers_map` and `parts_to_do_names` — the trailing-context drift parent's pre-flight predicted.

- **Upstream changes to touched files** between `v25.8.18.1-lts` and `v26.3.10.62-lts`:
  - `src/Storages/MergeTree/ReplicatedMergeTreeQueue.cpp`: 127 commits — substantial activity, but none touched `getMutationsStatus`. Line drift only (Site 1 moved from L2538 to L2646).
  - `src/Storages/StorageMergeTree.cpp`: 955 commits — busy file. None touched the `getMutationsStatus` swap line itself; one or more added the `parts_in_progress[_names]` fields to `MergeTreeMutationStatus`, drifting the trailing context (Site 2 moved from L957 to L1099).

- **Upstream-equivalent search**: 0 commits matched the topic grep; 0 commits introduced `formatForLogging` or modified `getMutationsStatus` in either file via pickaxe (`-S`). The four `Hide secrets in system.mutations.command column` SHAs from history are all Aiven-side variants; none are ancestors of `v26.3.10.62-lts`. Patch is NOT drift-superseded.

- **Hunk-context verification**:
  - Site 1 (`ReplicatedMergeTreeQueue.cpp` L2646): the 4-line pre-patch shape (`WriteBufferFromOwnString buf;` / `IAST::FormatSettings format_settings(/*one_line=*/true);` / `command.ast->format(buf, format_settings);` / `buf.str(),`) is present verbatim. Target shape preserved.
  - Site 2 (`StorageMergeTree.cpp` L1102): the targeted line `command.ast->formatWithSecretsOneLine(),` is present verbatim. Trailing context has `parts_in_progress_names,` (L1105) between `block_numbers_map,` (L1104) and `parts_to_do_names,` (L1106).

- **Conflict resolution narrative** (Outcome A from Step 2): the cherry-pick ran cleanly. Git's auto-merge (3-line context fuzz plus the diff3 algorithm) tolerated the trailing-context drift at BOTH sites — Site 1 had a similar `parts_in_progress` field inserted into the `ReplicatedMergeTree` struct initializer in 26.3, but only the trailing context line of the patch's @@ window differed (`parts_to_mutate` vs `parts_in_progress`), which auto-merge handled silently. `git status` reported the four files as cleanly staged with no `UU` markers; the T2 inventory's `cherry_pick_clean=no` flag was a false alarm from the strict 3-line context check, not a real conflict. The semantic content of the cherry-pick is byte-identical to the source — verified in Step 3 by the decomposition runbook (`grep -E '^[-+]' | grep -v '^[-+]\{3\}'` shows zero difference between source and staged +/- lines).

- **Conclusion**: `still-needed-applies-cleanly` (a special case of `still-needed-applies-with-conflict-at-site-2` where auto-merge's fuzz saved the day). Proceed with the cherry-pick as staged.

## 3. C++ review

Apply `docs/aiven/skills/cpp-review-checklist.md` to this patch. One bullet per checklist section. `n/a` is allowed; `✓` requires one-sentence evidence.

- 1 Lifetime + ownership: `n/a` — patch replaces one method call with another on the same object (`command.ast`). No ownership change, no allocator change.
- 2 Exception safety: `✓` — `IAST::formatForLogging` is the canonical, exception-safe formatter (same throw shape as the methods it replaces; declared at `src/Parsers/IAST.h:356`, same signature class as `formatWithSecretsOneLine`/`formatForErrorMessage`).
- 3 Thread-safety + concurrency: `✓` — both call sites already operate under the mutex of `getMutationsStatus`'s caller (the mutations-status read path); no new concurrency surface, no new lock acquired.
- 4 Performance + memory: `~` — `formatForLogging` internally constructs `FormatSettings(one_line=true, show_secrets=false)` and runs the same AST traversal as `formatWithSecretsOneLine`. Net: equal traversal cost, marginally less code duplication at the call site. Site 1 ALSO eliminates the manual `WriteBufferFromOwnString` / `FormatSettings` / `format` / `buf.str()` four-call dance → small reduction in stack frames per row in `system.mutations`. Negligible at realistic mutation queue sizes (typically O(10) entries; even O(1k) is bounded by `MergeTreeSettings::finished_mutations_to_keep` default 100).
- 5 Settings as public API: `n/a` — no setting introduced. There is intentionally no escape hatch; masking `system.mutations.command` is consistent with `system.query_log.query`, which has been masked since 22.x and also has no opt-out.
- 6 Error handling: `✓` — no new error paths. `formatForLogging` cannot fail in any way the replaced calls could not.
- 7 Upstream / vendored code: `✓` — both files are upstream-owned `src/Storages/*` paths; patch is a documented Aiven security hardening. The commit body is terse (one-line subject) but the intent is unambiguous: hide secrets in `system.mutations.command`. The previously-named helper `formatWithSecretsOneLine` was the leaky path.
- 8 Behavior under settings: The upstream `src/Parsers/IAST.h:351-355` docstring is the load-bearing comment: *"`formatForLogging` and `formatForErrorMessage` always hide secrets. This inconsistent behaviour is due to the fact such functions are called from Client which knows nothing about access rights and settings. Moreover, the only use case for displaying secrets are backups, and backup tools use only direct input and ignore logs and error messages."* `formatForLogging` ALWAYS hides secrets, regardless of the caller's `show_secrets` setting. This is by design — system tables should never display unmasked secrets. Aiven should consider proposing the rename of `formatWithSecretsOneLine` (a name that defensibly reads as both "with secrets [included]" and "with secrets [handled]") upstream, but that is out of scope for this patch.

## 4. Test design

Option **(a)** — new test that fails on the parent commit and passes after the patch.

- Test path: `tests/queries/0_stateless/9077_system_mutations_command_mask_secrets.{sql,reference}` per the Aiven test-naming convention (`docs/aiven/runbooks/testing-suites.md` §4.1). Renamed from upstream-shipped `03990_system_mutations_command_mask_secrets.*` because the test was authored by Aiven (Joe Lynch) and ships with this patch — no upstream history of these paths.

- **Pre-patch run output** (the FAIL):

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

  Interpretation: the first SELECT (`command LIKE '%TOPSECRET%'`) returns `1` because pre-patch `formatWithSecretsOneLine` includes the literal key `TOPSECRET-key-16`. The second SELECT (`command LIKE '%[HIDDEN]%'`) returns `0` because no `[HIDDEN]` placeholder is emitted.

- **Post-patch run output** (the PASS):

  ```text
  9077_system_mutations_command_mask_secrets:                             [ OK ] 0.13 sec.

  1 tests passed. 0 tests skipped. 0.15 s elapsed (Process-3).
  ```

- **Why this distinguishes pre-patch from post-patch** (per AGENTS.md §7): the test inserts an `ALTER TABLE ... UPDATE` mutation whose AST contains an `encrypt('aes-128-ecb', toString(id), 'TOPSECRET-key-16')` call — the third argument is flagged `is_secret_arg` on the `ASTFunction` node. Pre-patch, `StorageMergeTree::getMutationsStatus` renders via `formatWithSecretsOneLine`, which leaks the literal `TOPSECRET-key-16` into the `command` column. Post-patch, the same call site renders via `formatForLogging`, which always sets `show_secrets=false` and emits `[HIDDEN]` in place of the literal. The two SELECTs (`%TOPSECRET%` count → 0; `%[HIDDEN]%` count → 1) form a positive+negative assertion pair that distinguishes the two behaviors unambiguously — a test asserting only one would be susceptible to "we made it disappear by coincidence" failure modes.

- **Known coverage limitations** (recorded verbatim):
  - **Replicated-path coverage gap**: the shipped test uses non-replicated `MergeTree`, exercising only Site 2 (`StorageMergeTree::getMutationsStatus`). Site 1 (`ReplicatedMergeTreeQueue::getMutationsStatus`) requires `ReplicatedMergeTree` + Keeper; the shipped test does not cover it. We accept this gap because both sites use the same `formatForLogging` helper; Site 1's adoption is verified by code review.
  - **Other-secret-types gap**: the test uses an `encrypt(...)` key as the secret. Other secret-bearing AST shapes (S3 credentials in `s3(...)` table functions, named-collection passwords, dictionary source DSNs) are not exercised. The IAST masking framework treats all `ASTLiteral`/`ASTFunction` flagged with `is_secret_arg` uniformly, so a single example is sufficient to verify the helper works — but if Aiven adds new secret-bearing AST nodes in the future, this test will not catch a regression on those new nodes.

- **Mutation race annotation**: the test uses `mutations_sync = 0` and immediately SELECTs `system.mutations`. After completion, `system.mutations` keeps the row for the default retention (configurable via `MergeTreeSettings::finished_mutations_to_keep`, default 100); the test relies on this default to ensure the row is still visible when the SELECT runs. On a slow build (ASan/TSan) the mutation may not yet have started — also fine, the command text is recorded at queue-insert time, not at completion. No known races; document the dependency.

- **Tags**: none. The test as shipped has no `-- Tags:` line. Do NOT add `no-*` tags per repo policy.

## 5. Rollback considerations

- **Revert safety**: safe. The patch makes no schema change, no on-disk format change, no metadata change, no Keeper-state change. The only visible effect of a revert is that `system.mutations.command` would once again leak secret literals — a security regression, not a stability or data-integrity regression.
- **State surviving restart**: none. The change is purely in the read path of `system.mutations`; no row is written, no ZK node is added, no file on disk records the masking choice.
- **Re-enabling pre-patch behavior**: there is NO setting that re-enables unmasked rendering. This is intentional and consistent with `system.query_log.query`, which has been unconditionally masked since 22.x. If an operator needs to inspect unmasked mutation commands (e.g., for debugging a stuck mutation that involves a secret-bearing AST), they can use `EXPLAIN AST` with `show_secrets=1` on the raw mutation file/znode payload, or read the `mutation_*.txt` file under the table's data directory directly — both remain available out-of-band.

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — researched at 25.X dossier merge time.

### 25.8-aiven (historical, may be empty)

n/a — this is the version we're porting FROM. The patch landed as commit `a25b337024f16cfe7a87f0d59413c7d788dd9972` on `v25.8.18.1-lts-aiven` (author and committer Joe Lynch). Outcome on 25.8-aiven: not documented in this dossier (was created at T3.4).

### 26.3-aiven (this uplift)

- Cherry-pick was: **clean (Outcome A)**. Git's 3-line context fuzz + diff3 auto-merge tolerated the 26.3-only `parts_in_progress[_names]` fields inserted into `MergeTreeMutationStatus` at both sites; no `UU` markers, no manual edit needed. The T2 inventory's `cherry_pick_clean=no` flag was a false alarm caused by the strict context check, not a real conflict.
- Upstream-drift conclusion: `still-needed-applies-cleanly` (a softer variant of the predicted `still-needed-applies-with-conflict-at-site-2`).
- Test added at: `tests/queries/0_stateless/9077_system_mutations_command_mask_secrets.{sql,reference}` (renamed from upstream-shipped `03990_*` per Aiven §4.1 via `git mv`; content byte-equivalent to source).
- Time-to-port: ~8 minutes wall-clock (Step 0 start 11:00:01Z → Step 7 end ~11:08Z). **Cache state: warm** (per `tmp/patch-077/build-cache-state.txt`). Full post-patch build: 41 s / 22 ninja steps. Pre-patch incremental rebuild: 17 s / 19 steps. Post-patch incremental restore rebuild: 18 s / 19 steps. Cold-cache equivalents would be substantially larger (multiple minutes); these numbers are not transferable to cold-cache budgets.
- Anything surprising: the cherry-pick going through cleanly was the surprise — parent's preflight predicted Outcome B (manual Site 2 conflict resolution) based on the strict 3-line context check, but git's diff3 auto-merge handled the drift silently at both sites. The first-time `git mv` rename of a freshly-cherry-picked-as-add file rendered in `git status` as a plain "new file" under the new path (rather than as `renamed:`), because the source path was never tracked in HEAD; this is correct git semantics but worth recording so a future dossier reviewer isn't confused by the absence of a `renamed:` line. The `formatWithSecretsOneLine` name smell (§3 item 8) and the replicated-path coverage gap (§4) are noted as future-work for upstream contribution / integration-test expansion respectively.

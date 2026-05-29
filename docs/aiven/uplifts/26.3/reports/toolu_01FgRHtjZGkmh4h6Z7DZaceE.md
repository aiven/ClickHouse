<!-- subagentStop archive | source=transcript | status=completed | type=general-purpose | ts=2026-05-26T15:22:38Z -->

---
outcome: success
patch_slug: default-logs-to-keep
source_sha: 199db087991c02d215aafa6c6274200d507e31a9
proposed_commit:
  staged_files:
    - src/Databases/DatabaseReplicatedSettings.cpp
    - tests/queries/0_stateless/9010_default_logs_to_keep.sql
    - tests/queries/0_stateless/9010_default_logs_to_keep.reference
    - docs/aiven/patches/010-default-logs-to-keep.md
  commit_message: |
    Change default logs_to_keep from 1000 to 300 for DatabaseReplicated

    Reduce the default value of  setting from 1000 to 300 for
    DatabaseReplicated databases. This reduces ZooKeeper resource consumption
    by ~70% while maintaining a 6x safety margin over max_replication_lag_to_enqueue (50).

    Context:
    Previously, we implemented a server-level setting (replicated_database_logs_to_keep)
    to centralize control of this value. However, after analysis, we determined that:
    1. Customers do not have ALTER_DATABASE_SETTINGS permission, so they cannot
       modify database settings via ALTER DATABASE MODIFY SETTING
    2. The simpler approach of changing the database-level default is sufficient
    3. No additional readonly checks are needed since access control already
       prevents customer modifications

    This change affects only newly created databases. Existing databases retain
    their current logs_to_keep value stored in ZooKeeper.

    The default value of 300 provides adequate recovery buffer while significantly
    reducing ZooKeeper memory usage in multi-database managed provider environments.

    Co-authored-by: Khatskevich <khatskevich@aiven.io>

    Original author: Tilman Moeller <tilman.moeller@aiven.io>, 2025-12-08.

    (cherry picked from commit 199db087991c02d215aafa6c6274200d507e31a9)
  byte_equivalent: false
tests:
  added: yes
  kind: stateless
  paths:
    - tests/queries/0_stateless/9010_default_logs_to_keep.sql
    - tests/queries/0_stateless/9010_default_logs_to_keep.reference
  upstream_reference: []
  pre_patch_fail_verified: true
  post_patch_pass_verified: true
  justification: ""
escalation_reason: none
---

## Tier results

- Tier 1 (textual cherry-pick): pass — conflict reported by `git cherry-pick`, resolved mechanically as a pure context-line shift (four upstream-added `DECLARE` lines below `logs_to_keep`); the only `+`/`-` change is the literal `1000 → 300` token, identical to the source patch.
- Tier 2 (semantic patch-id):    pass — patch-ids differ (source `6f11b897bbe…`, staged `3b6a95beb3f…`) but decomposition runbook output is empty: the `+`/`-` lines (excluding `+++`/`---` headers) are byte-identical between source and staged; only context lines drifted. `byte_equivalent: false` per the schema.
- Tier 3 (build + test):         pass — post-patch build exit 0; post-patch test `9010_default_logs_to_keep` PASS; pre-patch test FAIL with the unified diff `expected 300 / actual 1000`; worktree restored, post-patch binary rebuilt for the human reviewer.

## Evidence

Cherry-pick conflict + mechanical resolution, staged diff (from `tmp/patch-010/cherrypick.log`):

```
Auto-merging src/Databases/DatabaseReplicatedSettings.cpp
CONFLICT (content): Merge conflict in src/Databases/DatabaseReplicatedSettings.cpp
error: could not apply 199db087991... Change default logs_to_keep from 1000 to 300 for DatabaseReplicated

(after mechanical resolution: keep HEAD intact, change only 1000 -> 300 on the logs_to_keep line)

diff --git a/src/Databases/DatabaseReplicatedSettings.cpp b/src/Databases/DatabaseReplicatedSettings.cpp
index 99cd01fcbdd..87703f6503c 100644
--- a/src/Databases/DatabaseReplicatedSettings.cpp
+++ b/src/Databases/DatabaseReplicatedSettings.cpp
@@ -22,7 +22,7 @@ extern const int UNKNOWN_SETTING;
     DECLARE(Bool, check_consistency, true, "Check consistency of local metadata and metadata in Keeper, do replica recovery on inconsistency", 0) \
     DECLARE(UInt64, max_retries_before_automatic_recovery, 10, "Max number of attempts to execute a queue entry before marking replica as lost recovering it from snapshot (0 means infinite)", 0) \
     DECLARE(Bool, allow_skipping_old_temporary_tables_ddls_of_refreshable_materialized_views, false, "If enabled, when processing DDLs in Replicated databases, it skips creating and exchanging DDLs of the temporary tables of refreshable materialized views if possible", 0) \
-    DECLARE(NonZeroUInt64, logs_to_keep, 1000, "Default number of logs to keep in ZooKeeper for Replicated database.", 0) \
+    DECLARE(NonZeroUInt64, logs_to_keep, 300, "Default number of logs to keep in ZooKeeper for Replicated database.", 0) \
```

Patch-id decomposition (`tmp/patch-010/patch-id-decomposition.log`): empty — `+`/`-` lines byte-identical between source and staged; the patch-id difference is 100% context-line shift.

Post-patch test (`tmp/patch-010/test-postpatch.log`):

```
Connected to server 26.3.10.1 @ f03db74f48ee9568b59a930189e297d5007c88f2 v26.3.10.62-lts-aiven-dev
Found 0 parallel tests and 1 sequential tests
Running 1 stateless tests (MainProcess).
9010_default_logs_to_keep:                                              [ OK ] 0.13 sec.
1 tests passed. 0 tests skipped. 0.17 s elapsed (MainProcess).
---runner exit: 0---
```

Pre-patch test (`tmp/patch-010/test-prepatch.log`):

```
Running 1 stateless tests (MainProcess).
9010_default_logs_to_keep:                                              [ FAIL ] 0.13 sec.
Reason: result differs with reference:
--- /home/tilman.moeller/projects/ClickHouse/tests/queries/0_stateless/9010_default_logs_to_keep.reference   2026-05-26 17:14:18 +0200
+++ /home/tilman.moeller/projects/ClickHouse/tests/queries/0_stateless/9010_default_logs_to_keep.stdout      2026-05-26 17:17:31 +0200
@@ -1 +1 @@
-300
+1000

Database: test_3ujixnm7
Having 1 errors! 0 tests passed.
---runner exit: 1---
```

Worktree restoration verification (`tmp/patch-010/flip-post-verify.log`):

```
---worktree post-patch verify (line 25 must be 300)---
    DECLARE(NonZeroUInt64, logs_to_keep, 300, "Default number of logs to keep in ZooKeeper for Replicated database.", 0) \
---worktree-vs-index diff (must be EMPTY)---
(empty)
---index-vs-HEAD numstat (must show 1 ins / 1 del)---
 src/Databases/DatabaseReplicatedSettings.cpp | 2 +-
 1 file changed, 1 insertion(+), 1 deletion(-)
```

Final staged file set (`tmp/patch-010/final-status.log`):

```
A  docs/aiven/patches/010-default-logs-to-keep.md
M  src/Databases/DatabaseReplicatedSettings.cpp
A  tests/queries/0_stateless/9010_default_logs_to_keep.reference
A  tests/queries/0_stateless/9010_default_logs_to_keep.sql
4 files changed, 265 insertions(+), 1 deletion(-)
```

## What I did

- **Preflight (Step 0).** Verified branch `v26.3.10.62-lts-aiven-dev`, source SHA exists. `git status --porcelain` showed 8 `contrib/*` "untracked content" rows (build artifacts inside the upstream-owned submodule trees); `git diff` and `git diff --cached` both clean. Documented as environmental noise (contribs are in the never-touch list and untouched by this dispatch); proceeded rather than escalating spurious `policy_call`. Recorded reasoning in `tmp/patch-010/preflight.log`.
- **Drift analysis (Step 1).** All 6 identifiers present on HEAD; 5 commits touched the file between LTSes; `git log --grep 'logs_to_keep'` yielded only the type-change commit and its merge; pre-image line present verbatim at line 25. Conclusion: `still-needed-and-applies`.
- **Cherry-pick (Step 2).** `git cherry-pick --no-commit -x 199db087991c…` reported a textual conflict — a surprise vs the parent's prior of "Hunk should apply cleanly". Inspection showed the conflict was pure context-line shift: `956465b4691` (adds three `default_replica_*` settings) and `4eeadb9c352` (adds `internal_replication`) inserted four unrelated `DECLARE` lines between `logs_to_keep` and the source diff's lower context anchor `DECLARE_SETTINGS_TRAITS`. The patch surface itself (the `logs_to_keep` line) was unambiguous. Per `docs/aiven/AGENTS.md` §6 the STOP condition is "non-trivial conflict" and per the schema's Tier-2 decomposition runbook, context-line shifts are explicitly the benign case (`byte_equivalent: false` with green); resolved mechanically by keeping all of HEAD's structure intact and changing only the `1000 → 300` token on the `logs_to_keep` line. Validated immediately via the decomposition runbook (Step 3 below) — empty diff confirmed the resolution is the only mechanically-correct one.
- **Patch-id (Step 3).** Source and staged patch-ids differ as expected; the decomposition diff (lines starting with `+`/`-` excluding `+++`/`---`) was empty. Tier 2 GREEN with `byte_equivalent: false`.
- **Post-patch build (Step 4).** `ninja -C build clickhouse` exit 0 in ~47 seconds (warm-cache).
- **Test authoring (Step 5).** Created `tests/queries/0_stateless/9010_default_logs_to_keep.{sql,reference}` directly per Aiven `9<NNN>_<slug>` convention (no `add-test`). The `.sql` mirrors `02710_default_replicated_parameters.sql`'s shape: `CREATE DATABASE … ENGINE = Replicated(…)` with a `currentDatabase()`-namespaced ZK path, then `SELECT value FROM system.zookeeper` to read the persisted `logs_to_keep` node, then `DROP`. Reference: `300\n` (4 bytes verified by `od -c`). Tagged `no-parallel` (matches reference test for the same Replicated-DB-creation reason).
- **Evidence pair (Step 6).** Started server (`tmp/ch-smoke/`-rooted); ran post-patch test → `[ OK ] 0.13 sec`. Flipped worktree to pre-patch via `git restore --worktree --source=HEAD src/Databases/DatabaseReplicatedSettings.cpp`; verified line 25 reads `1000` again and the index still has the `+300` change staged. Rebuilt incrementally (~34s, exit 0). Restarted server with the pre-patch binary (after `pkill -KILL` because `-INT` didn't return promptly during the first restart attempt, which produced a transient `Connection refused` before settling). Pre-patch test → `[ FAIL ] 0.13 sec` with `expected 300 / actual 1000`. Flipped back via `git restore --worktree …`, verified worktree-vs-index empty, re-built post-patch binary (exit 0, ~34s).
- **Dossier (Step 7).** Wrote `docs/aiven/patches/010-default-logs-to-keep.md` with all sections filled, including the new §9 (parent preflight discipline) and a candid "Anything surprising" note feeding back the preflight refinement: cherry-pick cleanliness depends on context-window stability, not just patched-line stability.
- **Final verification (Step 8).** Four files staged exactly as expected; worktree-vs-index diff for the source file is empty.
- **Server cleanup.** Stopped the local server with `pkill -INT`; ports 9000/8123 free.

## Proposed next step

Ready for human commit. Suggested:

```bash
git commit -F <(cat <<'EOF'
Change default logs_to_keep from 1000 to 300 for DatabaseReplicated

Reduce the default value of  setting from 1000 to 300 for
DatabaseReplicated databases. This reduces ZooKeeper resource consumption
by ~70% while maintaining a 6x safety margin over max_replication_lag_to_enqueue (50).

Context:
Previously, we implemented a server-level setting (replicated_database_logs_to_keep)
to centralize control of this value. However, after analysis, we determined that:
1. Customers do not have ALTER_DATABASE_SETTINGS permission, so they cannot
   modify database settings via ALTER DATABASE MODIFY SETTING
2. The simpler approach of changing the database-level default is sufficient
3. No additional readonly checks are needed since access control already
   prevents customer modifications

This change affects only newly created databases. Existing databases retain
their current logs_to_keep value stored in ZooKeeper.

The default value of 300 provides adequate recovery buffer while significantly
reducing ZooKeeper memory usage in multi-database managed provider environments.

Co-authored-by: Khatskevich <khatskevich@aiven.io>

Original author: Tilman Moeller <tilman.moeller@aiven.io>, 2025-12-08.

(cherry picked from commit 199db087991c02d215aafa6c6274200d507e31a9)
EOF
)
```

Notes for the reviewer:

- A small environmental side-effect: the local server start leaked an `access/` directory at the repo root (similar to the documented `preprocessed_configs/` quirk in `docs/aiven/runbooks/build-and-test.md` §4). It is untracked, not staged, and `.gitignore` already covers `/access/`-style server artifacts. Safe to remove with `rm -rf access/` if desired.
- The cherry-pick conflict was a surprise relative to the parent's preflight prior ("Hunk should apply cleanly"). The dossier's §6 "Anything surprising" feeds back the refinement: `cherry_pick_clean=no` from the inventory was correct; the prose interpretation was the slip. A future preflight should treat "no insertions/deletions in the source diff's context window on the target" as a separate check from "no semantic change to the patched line".

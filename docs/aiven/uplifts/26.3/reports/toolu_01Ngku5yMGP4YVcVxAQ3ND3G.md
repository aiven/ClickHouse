<!-- subagentStop archive | source=transcript | status=completed | type=general-purpose | ts=2026-05-28T12:36:32Z | backfilled=true | jsonl=c3739ede-5668-4339-b6a0-7b4645c3c6d0.jsonl | reason=hook-v3-third-regression -->

---
outcome: success
patch_slug: refreshable-mv-shard-macro-expansion
source_sha: cc745f53f96746a66d8e095901ce495563e1bdf8
proposed_commit:
  staged_files:
    - src/Storages/MaterializedView/RefreshTask.cpp
    - tests/integration/test_aiven_refreshable_mv_shard_macro_expansion/__init__.py
    - tests/integration/test_aiven_refreshable_mv_shard_macro_expansion/configs/no_shard_macro.xml
    - tests/integration/test_aiven_refreshable_mv_shard_macro_expansion/test.py
    - docs/aiven/patches/049-refreshable-mv-shard-macro-expansion.md
  commit_message: |
    Fix refreshable materialized views where there is a shard macro in the target table

    Refreshable materialized views use ZooKeeper coordination paths that are
    expanded from server settings like default_replica_path and default_replica_name.
    These paths can contain macros such as {shard}, {database}, {table}, and {replica}
    that need to be expanded to actual values.

    When a refreshable materialized view is created in a DatabaseReplicated database,
    the coordination path may contain the {shard} macro. However, the macro expansion
    was not including the shard name in MacroExpansionInfo, causing the {shard} macro
    to remain unexpanded in the coordination path.

    This fix:
    - Retrieves the database from DatabaseCatalog
    - Checks if it's a DatabaseReplicated database
    - If so, sets info.shard to the shard name from the database
    - This ensures {shard} macros are properly expanded in coordination paths

    Without this fix, refreshable materialized views in DatabaseReplicated databases
    would fail to coordinate correctly across replicas when the coordination path
    contains shard macros, leading to incorrect ZooKeeper paths and coordination
    failures.

    Original author: Tilman Moeller <tilman.moeller@aiven.io>, 2026-01-07.
    Co-authored-by: Joe Lynch <joe.lynch@aiven.io>
    (cherry picked from commit cc745f53f96746a66d8e095901ce495563e1bdf8)
  byte_equivalent: true
tests:
  added: yes
  kind: integration
  paths:
    - tests/integration/test_aiven_refreshable_mv_shard_macro_expansion/test.py
  pre_patch_fail_verified: true
  post_patch_pass_verified: true
escalation_reason: none
---

## Tier results

- Tier 1 (textual cherry-pick): pass — inherited verbatim from T3.13's `git cherry-pick --no-commit -x cc745f53f9`; 3-line additive change at `RefreshTask.cpp:103-105`, no conflict markers.
- Tier 2 (semantic patch-id): pass — T3.13 confirmed both source and staged diff produce patch-id `98be050c54304f8d8f468d9549e1d9c84398545c`; staged source change is unchanged in T3.14.
- Tier 3 (build + test): pass — `ninja -C build clickhouse` exit 0 on all four T3.14 builds (post-patch sanity, pre-patch flipped-out, post-patch retry after `multi_read` fix, post-patch restored). Pre-patch integration test fails with `Code: 139 NO_ELEMENTS_IN_CONFIG`; post-patch passes.

## Evidence

`git diff --cached --stat` (final 5-file staged state):

```
 .../049-refreshable-mv-shard-macro-expansion.md    | 409 +++++++++++++++++++++
 src/Storages/MaterializedView/RefreshTask.cpp      |   3 +
 .../__init__.py                                    |   0
 .../configs/no_shard_macro.xml                     |   5 +
 .../test.py                                        |  83 +++++
 5 files changed, 500 insertions(+)
```

Pre-patch test FAIL (`tmp/patch-049-t314/test-prepatch.log:66-76`, edited for brevity):

```
E   helpers.client.QueryRuntimeException: Client failed! Return code: 139, stderr: Received exception from server (version 26.3.10):
E   Code: 139. DB::Exception: Received from 172.18.0.5:9000. DB::Exception: No macro 'shard' in config while processing substitutions in '/clickhouse/tables/{uuid}/{shard}' at '27' or macro is not supported here. Stack trace:
E   5. ./build/./src/Common/Macros.cpp:155:19: DB::Macros::expand(String const&, DB::Macros::MacroExpansionInfo&) const @ 0x000000001175ef9c
E   6. ./build/./src/Storages/MaterializedView/RefreshTask.cpp:103:37: DB::RefreshTask::RefreshTask(...) @ 0x000000001a393bb8
E   . (NO_ELEMENTS_IN_CONFIG)
E   (query: CREATE MATERIALIZED VIEW testdb.mv REFRESH EVERY 1 HOUR TO testdb.target AS SELECT 1 AS a)
```

The stack frame `RefreshTask.cpp:103` is exactly the patched line — reachability of the patched code is observationally proven.

Post-patch test PASS (`tmp/patch-049-t314/test-postpatch.log:22, 52, 66`):

```
test_aiven_refreshable_mv_shard_macro_expansion/test.py::test_refreshable_mv_in_replicated_database_expands_shard_macro
PASSED                                                                   [100%]
======================== 1 passed, 2 warnings in 12.31s ========================
```

Patch-presence assertion after every flip:

```
$ grep -c 'replicated_db->getShardName' src/Storages/MaterializedView/RefreshTask.cpp
# post-patch sanity: 1
# pre-patch (after git restore --worktree --source=HEAD): 0
# post-patch restored (after git checkout --): 1
$ git diff src/Storages/MaterializedView/RefreshTask.cpp | wc -l
# final: 0
```

## What I did

- **Step 1** — Verified the inherited staged state (`git diff --cached --stat`: 2 files, 355 insertions; staged patch present in worktree; HEAD on `v26.3.10.62-lts-aiven-dev`).
- **Step 2** — Inspected the inherited test scaffolding. Reused `__init__.py` (empty) and `configs/no_shard_macro.xml` (single `<replica>` macro) verbatim.
- **Step 3** — Cleaned `_instances/` and `__pycache__/` from T3.13's run.
- **Step 4-5** — Sanity build with patch in worktree (incremental relink, exit 0); confirmed patched line present.
- **Step 6** — Overwrote `test.py` with the corrected design (`ReplicatedMergeTree()` target instead of T3.13's `MergeTree()`). Added one improvement beyond the verbatim dispatch body: `keeper_required_feature_flags=["multi_read"]` on the instance — required for determinism (see surprise below).
- **Step 7** — Worktree-flip cycle:
  1. Flipped out patch, rebuilt (exit 0), ran pre-patch test → `Code: 139` at `RefreshTask.cpp:103` ✓ — reachability proof confirmed empirically.
  2. Initial post-patch run produced `Code: 999 KEEPER_EXCEPTION` (transient ZK session expiry mid-multi-write); retry produced `Code: 48 NOT_IMPLEMENTED` ("Keeper server doesn't support multi-reads") at `RefreshTask.cpp:119`.
  3. **Root-caused** to `helpers/cluster.py:1986` defaulting `keeper_randomize_feature_flags=True`. When `MULTI_READ` rolled `disabled`, the patched code bailed at the feature-flag check at line 117-119 before reaching the multi-write at line 142. Found precedent in `tests/integration/test_refreshable_mv_skip_old_temp_table_ddls/test.py:22` which pins the same flag for the same reason.
  4. Added `keeper_required_feature_flags=["multi_read"]` to `add_instance` with an inline comment.
  5. Re-ran pre-patch → still `Code: 139` (pre-patch fails upstream of the feature-flag check, so the pin doesn't change pre-patch behavior). Re-ran post-patch → PASSED.
  6. Final worktree-flip cycle yielded the clean evidence-of-causation pair recorded above.
- **Step 8** — Staged the three test files. Verified `_instances/` and `__pycache__/` are NOT staged.
- **Step 9** — Updated dossier in place:
  - §0 row for 26.3-aiven: outcome changed to "byte-equivalent cherry-pick; integration test with verified pre/post evidence pair (T3.14 redesign per (iv) reachability proof)".
  - §4: replaced T3.13's escalation decision with the verified T3.14 design. Added "(iv) reachability proof" subsection (reading `StorageMaterializedView.cpp:200-246`), "Why the target table's creation is not itself the differential" subsection, "T3.14 redesign verification" subsection with both error excerpts, and "One Keeper-feature-flag pin required for determinism" subsection. Preserved T3.13's analysis as "T3.13 escalation history (preserved for lineage)". Renamed "Candidate redesigns" to "Other candidates considered (not selected for T3.14)" and marked option 1 as selected.
  - §6: Tier 3b status changed from "escalated `test_design_blocked`" to "PASS — integration test with verified evidence-of-causation pair (T3.14 redispatch)". Added a "T3.13 → T3.14 redispatch flow" bullet describing the first-of-its-kind inherited-staged-INDEX flow. Updated the "Anything surprising" bullet to retain T3.13's `StorageMaterializedView.cpp:222-225` finding and add the (a) T3.14 reachability finding and (b) keeper feature-flag randomization finding.
  - §7: Removed items 1, 2, 4 (resolved by T3.14). Kept item 3 (the (iv) preflight clause) and reworded: T3.14 is the FIRST concrete application of (iv); recommended formalization after a third confirming use (rule-of-three pending).
- **Step 10** — This report.

## Proposed next step

Ready for human commit. Suggested:

```bash
git commit -F tmp/patch-049-t314/commit-message.txt
```

(`commit-message.txt` not yet written by me — the human can `git commit -F-` the body above or compose the file from `proposed_commit.commit_message`. The body line `Original author: Tilman Moeller <tilman.moeller@aiven.io>, 2026-01-07.` preserves source-author provenance per the schema; do NOT use `git commit --author=` or `git commit -c CHERRY_PICK_HEAD`.)

Follow-ups (now lighter than T3.13's):

1. Promote the parent-preflight (iv) clause to standing discipline after a third confirming use (T3.14 is the first; T3.13 documented the need but did not itself apply it). Recorded as the sole §7 item.
2. The `keeper_randomize_feature_flags=True` interaction surfaced by T3.14 may be worth a single-paragraph note in `docs/aiven/runbooks/integration-tests.md` so future workers writing tests that exercise Keeper-multi-read code paths know to pin the flag preemptively. This is a separate small follow-up, not blocking this commit.

## Learning log

- **Today you learned:**
  - The parent (iv) preflight clause ("verify the test trigger reaches the patched code, not just that the patched code exists") is what unblocks inherited-state mid-port redispatches — applying it once before the redispatch saves an entire worker round-trip.
  - Integration-test infrastructure can randomize Keeper feature flags by default (`keeper_randomize_feature_flags=True`), which can intermittently mask any code path gated on `KeeperFeatureFlag::MULTI_READ` or similar. Pinning required flags via `keeper_required_feature_flags=[...]` matches existing precedent and is the deterministic fix.
- **Rule of thumb:** When a redesigned test produces different errors on consecutive runs of the same binary, suspect randomized cluster infrastructure (feature flags, ports, sysctl tunables) before suspecting the patch.
- **Next rabbit hole:** What other Aiven patches gate on Keeper-server feature flags that get randomized? A grep for `KeeperFeatureFlag::` usages in `src/Storages/**` could surface a small list worth a documentation pass.

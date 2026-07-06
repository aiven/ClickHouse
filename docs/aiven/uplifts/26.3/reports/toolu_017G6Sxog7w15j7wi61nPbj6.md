<!-- subagentStop archive | source=transcript | status=completed | type=general-purpose | ts=2026-05-28T11:45:49Z | backfilled=true | jsonl=0d03c4e6-fa1d-484e-91c8-46f7526303dc.jsonl | reason=hook-v3-third-regression -->

---

```yaml
outcome: escalate
patch_slug: refreshable-mv-shard-macro-expansion
source_sha: cc745f53f96746a66d8e095901ce495563e1bdf8
proposed_commit:
  staged_files:
    - src/Storages/MaterializedView/RefreshTask.cpp
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

    NOTE (T3.13 dispatch): Source change ships staged. Integration test design as
    specified by the dispatch is structurally unreachable on 26.3 — see
    docs/aiven/patches/049-refreshable-mv-shard-macro-expansion.md §4 for the
    StorageMaterializedView.cpp:222-225 BAD_ARGUMENTS gate that routes around
    RefreshTask::RefreshTask. Pending parent redesign of the test trigger before
    the evidence-of-causation pair can be produced.

    Original author: Tilman Moeller <tilman.moeller@aiven.io>, 2026-01-07.
    Co-authored-by: Joe Lynch <joe.lynch@aiven.io>
    (cherry picked from commit cc745f53f96746a66d8e095901ce495563e1bdf8)
  byte_equivalent: true
tests:
  added: no_justified
  kind: integration
  paths: []
  upstream_reference: []
  pre_patch_fail_verified: false
  post_patch_pass_verified: false
  justification: |
    The dispatch's FIXED integration test design (non-replicated MergeTree target
    inside a Replicated database, non-APPEND MV) cannot reach the patched
    `RefreshTask::RefreshTask` constructor on 26.3. The upstream sanity check at
    `StorageMaterializedView.cpp:222-225` (added 2024-03-09 in `48ec505823e`,
    predating both 25.8 and 26.3) throws `Code: 36 BAD_ARGUMENTS` for the
    combination "Replicated database + non-replicated target + no APPEND" BEFORE
    `RefreshTask::create` is called. Worktree-flip evidence: pre-patch and
    post-patch test runs both fail with the same `BAD_ARGUMENTS` error — neither
    reaches the patched code path. Integration test files are present on disk
    under `tests/integration/test_aiven_refreshable_mv_shard_macro_expansion/`
    (matching the dispatch's verbatim spec, with `__init__.py`,
    `configs/no_shard_macro.xml`, `test.py`) but NOT in `staged_files` because
    the design is structurally blocked. Per the dispatch: "Test body is FIXED —
    implement verbatim. ... If reality diverges from this design, STOP and
    escalate with `test_design_blocked` or `policy_call`." This is that
    escalation. Candidate redesigns (literal-path `ReplicatedMergeTree` target;
    APPEND mode) are listed in dossier §4 for parent decision.
escalation_reason: test_design_blocked
---
```

## Tier results

- Tier 1 (textual cherry-pick): **pass** — `git cherry-pick --no-commit -x cc745f53f9` produced 3 insertions, 0 deletions in `src/Storages/MaterializedView/RefreshTask.cpp`, no conflict markers.
- Tier 2 (semantic patch-id): **pass** — both source and staged diff yield `git patch-id` `98be050c54304f8d8f468d9549e1d9c84398545c`. `byte_equivalent: true`.
- Tier 3 (build + test): **build pass; test design blocked** — `ninja -C build clickhouse` exit 0 on all three rebuilds (post-patch, pre-patch flipped-out, post-patch flipped-back). Regression sweep: `03760_refreshable_mv_local` OK; `03258_refreshable_mv_misc` timed out during initial DDL setup (unrelated to RefreshTask, prior flaky-fix history). Integration test cannot produce evidence-of-causation — `StorageMaterializedView.cpp:222-225` gate routes around the patched line.

## Evidence

### Step 1 — preflight priors (all verified)

```
=== (i) Lines 99-104 of RefreshTask.cpp ===
        const auto & server_settings = context->getServerSettings();
        const auto macros = context->getMacros();
        Macros::MacroExpansionInfo info;
        info.table_id = view->getStorageID();
        coordination.path = macros->expand(server_settings[ServerSetting::default_replica_path], info);
        coordination.replica_name = context->getMacros()->expand(server_settings[ServerSetting::default_replica_name], info);

=== (iii) getShardName signature ===
113:    String getShardName() const { return shard_name; }

=== Include check ===
6:#include <Databases/DatabaseReplicated.h>
```

### Step 2 — Tier 1 cherry-pick (clean)

```
$ git cherry-pick --no-commit -x cc745f53f9
Auto-merging src/Storages/MaterializedView/RefreshTask.cpp

$ git diff --cached --stat
 src/Storages/MaterializedView/RefreshTask.cpp | 3 +++
 1 file changed, 3 insertions(+)
```

### Step 3 — Tier 2 patch-id (byte-equivalent)

```
src patch-id: 98be050c54304f8d8f468d9549e1d9c84398545c cc745f53f96746a66d8e095901ce495563e1bdf8
new patch-id: 98be050c54304f8d8f468d9549e1d9c84398545c 0000000000000000000000000000000000000000
```

### Step 4 — Tier 3a build (post-patch, pre-patch, restored) all exit 0

```
[15/18] Building CXX object src/CMakeFiles/dbms.dir/Storages/MaterializedView/RefreshTask.cpp.o
[16/18] Linking CXX static library src/libdbms.a
[17/18] Linking CXX executable programs/clickhouse
exit: 0
```

### Step 5 — Regression sweep

```
03760_refreshable_mv_local:                                             [ OK ] 0.38 sec.
03258_refreshable_mv_misc:                                              [ FAIL ] 600.02 sec.
Reason: Timeout! Processes left in process group 2671188:
... clickhouse-client ... -q "     create user user_03258_test_qdtkdn4v;
     create database test_qdtkdn4v_03258;     create table a (x String) engine Memory; ..."
```
(hung during initial `CREATE USER`/`GRANT` setup, well before any RMV code; test has documented prior flaky-fix history)

### Step 7 — Worktree-flip pre/post evidence (the blocker)

**Pre-patch test run** (binary built with patch absent from worktree):

```
FAILED test_aiven_refreshable_mv_shard_macro_expansion/test.py::test_refreshable_mv_in_replicated_database_expands_shard_macro
E   helpers.client.QueryRuntimeException: Client failed! Return code: 36, stderr: Received exception from server (version 26.3.10):
E   Code: 36. DB::Exception: Received from 172.18.0.5:9000. DB::Exception: This combination doesn't work: refreshable materialized view, no APPEND, replicated database, non-replicated table. The refresh would be done on one replica, but the table would be replaced on other replicas too (with empty tables). Refusing to create.
======================== 1 failed, 3 warnings in 17.78s ========================
```

**Post-patch test run** (patch flipped back in, rebuilt):

```
FAILED test_aiven_refreshable_mv_shard_macro_expansion/test.py::test_refreshable_mv_in_replicated_database_expands_shard_macro
E   helpers.client.QueryRuntimeException: Client failed! Return code: 36, stderr: Received exception from server (version 26.3.10):
E   Code: 36. DB::Exception: Received from 172.18.0.5:9000. DB::Exception: This combination doesn't work: refreshable materialized view, no APPEND, replicated database, non-replicated table. The refresh would be done on one replica, but the table would be replaced on other replicas too (with empty tables). Refusing to create.
======================== 1 failed, 3 warnings in 11.85s ========================
```

**Critical signal:** both pre- and post-patch runs throw the *same* `Code: 36 BAD_ARGUMENTS` — predicted error was `Code: 139 NO_ELEMENTS_IN_CONFIG`. The actual error originates at `src/Storages/StorageMaterializedView.cpp:225` (sanity check, runs in `StorageMaterializedView::StorageMaterializedView` BEFORE `RefreshTask::create`). The patched line at `RefreshTask.cpp:103-105` is never reached on either side of the flip.

### Final staged state

```
 .../049-refreshable-mv-shard-macro-expansion.md    | 352 +++++++++++++++++++++
 src/Storages/MaterializedView/RefreshTask.cpp      |   3 +
 2 files changed, 355 insertions(+)
```

Untracked (intentionally NOT staged): `tests/integration/test_aiven_refreshable_mv_shard_macro_expansion/` (contents: `__init__.py`, `configs/no_shard_macro.xml`, `test.py`, plus gitignored `_instances/` and `__pycache__/`). `git diff src/Storages/MaterializedView/RefreshTask.cpp | wc -l == 0` (worktree matches index).

## What I did

- Verified HEAD is on `v26.3.10.62-lts-aiven-dev`; confirmed parent's (i)/(ii)/(iii) preflight priors at lines 99-104 of `RefreshTask.cpp`, the `getShardName` signature at `DatabaseReplicated.h:113`, and the pre-existing `<Databases/DatabaseReplicated.h>` include at line 6.
- Cherry-picked `cc745f53f9` cleanly (`--no-commit -x`); confirmed Tier 1 (3 insertions, no conflicts) and Tier 2 (matching patch-id `98be050c5430...`).
- Built post-patch `clickhouse` binary (`ninja -C build clickhouse` exit 0; warm cache; ~97 s).
- Ran regression sweep against running smoke server. `03760_refreshable_mv_local` OK in 0.38 s. `03258_refreshable_mv_misc` timed out at 600 s during initial setup — well before reaching any RMV code; documented prior flaky-fix history; not in RefreshTask machinery.
- Authored the integration test directory verbatim per the dispatch's FIXED Step 6 spec (`__init__.py`, `configs/no_shard_macro.xml`, `test.py`).
- Saved the staged patch to `tmp/patch-049/staged-patch.diff`, flipped the patch out with `git restore --worktree --source=HEAD -- src/.../RefreshTask.cpp` (index untouched), rebuilt, ran the integration test → FAILED with `Code: 36 BAD_ARGUMENTS` (predicted: `Code: 139 NO_ELEMENTS_IN_CONFIG`).
- Investigated the divergence: `StorageMaterializedView.cpp:222-225` throws `BAD_ARGUMENTS` for "Replicated database + non-replicated target + no APPEND" combinations, gated by line 196 (`refresh_coordinated = true` is forced in non-APPEND mode inside a Replicated DB). `git log -L 225,225:...` shows this check was added in upstream commit `48ec505823e` ("Refreshable MV: coordination and Replicated DB support") on 2024-03-09 — predates both 25.8 and 26.3.
- Flipped the patch back with `git checkout -- src/.../RefreshTask.cpp`, rebuilt, ran the integration test again → STILL FAILED with the same `Code: 36 BAD_ARGUMENTS`. Confirms the test design as-specified routes around the patched code path on both sides of the flip.
- Authored the dossier (`docs/aiven/patches/049-refreshable-mv-shard-macro-expansion.md`) with every section filled; staged it.
- Decided NOT to stage the integration test files — they implement the dispatch's FIXED design verbatim but produce no evidence-of-causation pair on 26.3. Per dispatch: "Test body is FIXED — implement verbatim. ... If reality diverges from this design, STOP and escalate with `test_design_blocked` or `policy_call`." Files remain on disk (untracked) so the parent can inspect.
- Stopped the local smoke server cleanly before the worktree-flip; verified `git diff src/Storages/MaterializedView/RefreshTask.cpp | wc -l == 0` (worktree matches index in the final state).

## Proposed next step

**Parent decision needed: redesign the test trigger.** The dispatch's verbatim test design is blocked by the `StorageMaterializedView.cpp:222-225` BAD_ARGUMENTS gate (added upstream 2024-03-09, predates both LTS releases). Four candidate redesigns are listed in dossier §4; the most-promising candidate for T3.13.1 dispatch:

> Switch the target table from `ENGINE = MergeTree() ORDER BY a` to `ENGINE = ReplicatedMergeTree('/clickhouse/tables/target/r', 'r1') ORDER BY a` — keeping the target's path LITERAL (no `{shard}` macro) so its creation does not need shard-macro expansion, but the RMV's coordination path still uses `default_replica_path` which DOES contain `{shard}` and therefore still exercises the patched `RefreshTask::RefreshTask` code. Parent must verify that `database_replicated_allow_replicated_engine_arguments` (per patch 006 §4) permits explicit engine args inside a `Replicated` database on 26.3 before redispatching.

Alternatives in §4: APPEND mode (changes semantics); set `default_replica_path` explicitly in config (already the case by default — structural blocker remains); non-Replicated DB (patch + bug both no-op — not a viable differential).

Side findings worth recording in the next dispatch's preflight:
- The parent-preflight (i)/(ii)/(iii) discipline as-defined verified correctly on this patch, but does NOT cover **test-reachability gating in adjacent code paths**. Recommendation: add a (iv) clause — "verify the test trigger reaches the patched code, not just that the patched code exists" — likely a one-line grep against upstream throw sites for the predicted error code.
- Sub-shape C ("fresh-state DDL trigger, no restart") remains a valid classification — the patched code DOES fire during CREATE without a restart, IF a test design that reaches it is constructed. The runbook follow-up to document Sub-shape C in `integration-tests.md §7.1` should be deferred to the dispatch that successfully produces the evidence pair.
- `test_aiven_<slug>/` convention rule-of-three counter: T3.13 followed the directory/naming convention verbatim; the design WITHIN the convention is what failed. Parent decides whether T3.13 counts toward the 2/3 → 3/3 promotion or whether to pause until the evidence pair lands.

---

**Today you learned:** Test-design "fixed verbatim" assumptions can be invalidated by upstream sanity checks in adjacent (un-patched) files that fire BEFORE the patched code; the safest pre-dispatch check for an evidence-pair test is "does the trigger reach the patched line?" not "does the patched line exist?".
**Rule of thumb:** When a pre-patch test fails with an error code DIFFERENT from the predicted one, the test is broken (or the prediction is) — never "tweak the test to match the new code" without first confirming the patched code was actually reached.
**Next rabbit hole:** Whether the original 25.8 patch was tested with `ReplicatedMergeTree` target (most likely) — the source-commit message references coordination correctness, which only manifests when the target ITSELF is replicated; the dispatch's `MergeTree` choice was a parent miss, not a 26.3 regression.

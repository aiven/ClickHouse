---
description: 'Dossier for the Aiven patch that coordinates refreshable materialized view refreshes across the shards of a Replicated database'
sidebarTitle: '066: Refreshable MVs in sharded Replicated databases'
slug: '/aiven/patches/066-mv-refresh-sharded'
title: 'Patch 066: Refreshable materialized views in a sharded Replicated database'
doc_type: 'reference'
aiven_identity: '066'
aiven_absorbs: ['078']
aiven_changelog_category: 'Critical Bug Fix'
aiven_settings: []
---

# Patch 066 — refreshable materialized views in a sharded Replicated database {#patch-066-mv-refresh-sharded}

## Summary {#summary}

In a `Replicated` database with more than one shard, every refresh of a non-`APPEND` refreshable
view deletes the target table's rows on all shards but one. The patch makes one replica lead
each refresh for all shards: every shard inserts its own rows into the same new table, and the
leader swaps it in once, after all shards are done.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.8 | 066 `3866708bab7`; 078 `ea9c5d6420f` | original; 078 tunes 066's wait for the leader's table |
| 26.3 | `patch-port(066,078)` `b86c0c1b517`; `patch-fix(050)` `825f7b06b05` (a 066 fix: take the shard before publishing the refresh directory); `patch-fix(066)` `69f13676f85` (FLEECH-776, #67: parse znodes written by 25.8) | port onto the rewritten refresh loop, five port defects fixed |
| 26.8 | `patch-port(066)` | `still-needed-but-rewrite`, one commit with all three 26.3 commits; absorbs 078 |

## Background (ClickHouse) {#background}

A refreshable materialized view (RMV) re-runs its query on a schedule. Without `APPEND`, a
refresh creates a new table, inserts the result, `EXCHANGE`s it with the target table and drops
the old one. In a `Replicated` database every DDL — including that `CREATE`, `EXCHANGE` and
`DROP` — runs on all replicas of all shards through the database's DDL queue, while an `INSERT`
stays in the shard that ran it.

The replicas of a coordinated view share state in Keeper: a root znode with the schedule state
and the replica running the current attempt, an ephemeral `/running` znode held by that replica,
and `/paused`. Upstream builds the path from `default_replica_path`, which normally contains
`{shard}`, so each shard coordinates and refreshes on its own.

## Component tour {#component-tour}

- `src/Storages/MaterializedView/RefreshTask.{h,cpp}` — the refresh state machine.
  `doScheduling` reads Keeper and decides what to do; `executeRefresh` runs a refresh on another
  thread; `executeRefreshUnlocked` creates the table, runs the `INSERT ... SELECT` and does the
  `EXCHANGE`. `CoordinationZnode` is the root znode, written as `name: value` lines.
- `src/Storages/StorageMaterializedView.cpp` — `prepareRefresh` creates the new table and
  builds the insert query.

## Problem {#problem}

With two shards, each shard refreshes on its own schedule. Shard 1 creates `.tmp...`, fills it
with its rows and `EXCHANGE`s it with the target. The `CREATE` and `EXCHANGE` also run on shard
2, where `.tmp...` is empty, so shard 2's target table becomes empty. Shard 2's own refresh does
the same to shard 1. Which shard loses its rows is a race; usually someone does.

## Decisions {#decisions}

### Carry the 26.3 design {#decision-carry-design}

- **Chosen:** one coordination znode for all shards, at the 25.8/26.3 path
  `/clickhouse/tables/{uuid}/mv_refresh_qrLUb5TgIJ`, so views keep their state across the
  upgrade. The replica that wins upstream's `/running` election is the global leader. It
  creates `_refresh_<ms>` under the root, takes its own shard there, then publishes the
  directory name in the root znode. It creates the new table and writes its UUID to
  `<dir>/temporary_table`. In each other shard one replica takes `<dir>/<shard>` (ephemeral),
  inserts its shard's rows into that table by UUID and creates `<dir>/finished/<shard>`. The
  leader waits for all shards (up to 2 hours, or until cancelled), then does the one
  `EXCHANGE`. `APPEND` views work the same way, minus the new table and the `EXCHANGE`.
- **Rejected:** redesigning the protocol on this uplift; it has run in production since 25.8.
- **Rung:** edit to an upstream hot path (rung 5). Upstream has no shard-level coordination;
  this is an upstream candidate (rung 2).

### Pass the refresh directory explicitly {#decision-explicit-dir}

- **Chosen:** the helpers take the directory as an argument. The leader reads it from the
  znode of its attempt, a shard participant from the root znode it has just read.
- **Rejected:** 26.3's shared `current_refresh_dir` / `is_global_leader` / `is_shard_leader`
  fields, written and read on two threads and kept apart by a special case in
  `readZnodesIfNeeded`.

### Fold 078 and the 26.3 fixes into one commit {#decision-absorb}

- **Chosen:** one identity. 078 only tunes 066's wait (check every second that the refresh is
  still live) and cannot exist without it; it was already squashed into 066 on 26.3. Both 26.3
  fixes, `825f7b06b05` and `69f13676f85`, are fixes to 066.

## Approach {#approach}

- Constructor: the shared path; `replica_name` becomes `<shard>/<replica>`, because replica
  names repeat across shards and the root znode identifies the running replica by name. No
  `/replicas` subtree; `drop` removes the refresh directories and the root.
- `doScheduling`: after winning the election (with `refresh_dir` empty), removes stale refresh
  directories, creates the new one, takes its shard and only then publishes `refresh_dir`. If
  another replica is running a refresh with a published directory, this replica tries to take
  its shard and insert its rows (`participateInRefresh`), unless that shard is already marked finished (a claim
  lost with a Keeper session must not insert the rows twice). This runs on the scheduling thread,
  as on 26.3; shutdown and `SYSTEM STOP VIEW` interrupt it like a normal refresh. A repeated
  unexpected error backs off 5 s instead of busy-looping (FLEECH-776).
- `executeRefreshUnlocked`: the leader publishes the new table right after creating it, marks
  its shard finished after the insert and waits for the others before the `EXCHANGE`. A shard
  participant runs only the insert part, into the leader's table.
  `executeRefresh` now sets a Keeper component (`setCurrentComponent`), because it talks to
  Keeper; upstream's did not, and CI aborts on Keeper calls without one.
- `CoordinationZnode`: new `refresh_dir` field, written right after `refresh_running` (where 26.3
  servers look for it during a rolling upgrade) and before the fields upstream added in 26.8.
  `parse` looks fields up by name (FLEECH-776), so 25.8, 26.3 and 26.8 layouts all parse;
  `target_table_id` from older znodes is ignored. `updateCoordinationState` clears `refresh_dir`
  whenever it clears `refresh_running`.
- `StorageMaterializedView::prepareRefresh`: optional `shard_target` — insert into that table
  instead of creating one.

## Concept {#concept}

In a `Replicated` database DDL is global but data is per shard. Any operation that publishes
data with DDL (`EXCHANGE`, `REPLACE`, `DROP`) has to wait until every shard has written its part.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-but-rewrite`

- Upstream 26.8 still refreshes each shard on its own; the bug is unchanged.
- The refresh loop changed a lot since 26.3 (dependencies overhaul, the missing-feature-flags
  give-up path, a cancellation check before the `EXCHANGE`, `NORMALIZED_QUERY_HASH` quotas, temp
  table handling). The patch was re-applied onto upstream's code, keeping it, instead of
  resolving 26.3's diff: 26.3 rewrote `executeRefreshUnlocked`, 26.8 keeps upstream's and adds
  branches.
- Dropped from 26.3, because on 26.8 they were dead or wrong:
  - the wait for every replica after the `EXCHANGE`: with the default
    `distributed_ddl_output_mode` the DDL returns a pulling pipeline, which 26.3 never ran, so
    it never waited;
  - `tryBecomeGlobalLeader` and the `refresh_task_stop_racing_for_running_refresh` failpoint:
    unused since 26.3;
  - the stale-directory cleanup matched `refresh_` instead of `_refresh_`, so it never ran; the
    new one removes all old directories once the leader holds `/running`;
  - the wait for the leader's table skipped `.tmp_replace_*` names; 26.8 names that table
    `_tmp_replace_*` (`TemporaryReplaceTableName`);
  - the leader removed every refresh directory after its refresh; it now removes only its own,
    so a leader that lost its session cannot delete a newer refresh's directory.
- 26.3's single-node defect fixes (§7.3–7.5 of its dossier) are not needed: upstream's own
  error handling and query logging are kept.

## Customer impact {#customer-impact}

- Sharded `Replicated` databases: refreshes keep every shard's rows. Same as 25.8 and 26.3.
- Single-shard databases: one extra refresh directory per refresh in Keeper, otherwise as
  upstream.
- `system.view_refreshes.last_refresh_replica` shows `<shard>/<replica>`.
- Known limits, unchanged from 26.3:
  - if a shard's insert fails, the leader waits up to 2 hours before failing the refresh;
  - `DROP DATABASE` on one replica removes the shared coordination znode, and the other
    replicas' views retry on `ZNONODE` until they restart.
- Upstream tests that fail by design: `test_refreshable_mv::test_refreshable_mv_in_replicated_db`
  looks for the znode at the per-shard path; cases of `test_refreshable_mv_no_multi_read` read
  `/clickhouse/tables/{uuid}/1/running`. The fork's test lanes run only `aiven_*` suites.

```sql
-- Two shards of `db`; db.src holds 1,2,3 on shard 1 and 10,20,30 on shard 2.
CREATE MATERIALIZED VIEW db.mv REFRESH EVERY 1 HOUR TO db.tgt AS SELECT a FROM db.src;
SYSTEM REFRESH VIEW db.mv;
SELECT a FROM db.tgt;  -- on shard 2
-- Before: empty (shard 1's EXCHANGE replaced it).  After: 10, 20, 30
```

## Security review {#security-review}

n/a — coordination in Keeper. The insert on a shard runs with the view's SQL security context,
as upstream's refresh does.

## Tests {#tests}

- **Paths:** `tests/integration/test_aiven_mv_refresh_sharded/`,
  `src/Storages/tests/gtest_refresh_task_coordination_znode.cpp`
- **Proves:** with two shards, a refresh and a second one keep each shard's rows (fails without
  the patch on 26.3: shard 2's target ends empty); an `APPEND` view appends each shard's rows; a
  root znode in the 25.8 or 26.3 layout is read, the view refreshes and the znode is written
  back in the current layout. The gtest parses all three layouts and rejects missing and
  duplicate fields.
- **Run:** `python -m ci.praktika run "integration" --test test_aiven_mv_refresh_sharded`;
  `unit_tests_dbms --gtest_filter='RefreshTaskCoordinationZnode*'`
- **Evidence:** the integration suite runs on Buildkite (no Docker here). By hand on a local
  arm64 debug build, two servers as two shards of one database (2026-11-19): refreshes led from
  either shard keep `tgt` = 1,2,3 on shard 1 and 10,20,30 on shard 2, with and without `APPEND`;
  after the root znode was rewritten in the 25.8 and then the 26.3 layout, the view refreshed
  and wrote it back in the 26.8 layout; no errors in the server logs. Upstream's 27 stateless
  refreshable-view tests pass on one server (`05361`, which matches the name filter, needs a
  local S3 and is unrelated). The gtest was only compiled (`-fsyntax-only`); the local build
  has tests off and the Buildkite lanes do not run gtests.

## Rollback {#rollback}

- **Revert:** sharded `Replicated` databases lose rows on every refresh again. Views get new,
  per-shard coordination znodes and lose their schedule state once.
- **Persistent state:** the shared root znode with `refresh_dir`, and `_refresh_*` directories
  while a refresh runs.
- **Disable without rebuild:** none.

## Retirement {#retirement}

Upstream coordinating refreshes across the shards of a `Replicated` database (offer this
patch).

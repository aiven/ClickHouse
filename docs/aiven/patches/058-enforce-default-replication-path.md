---
description: 'Reject a new ReplicatedMergeTree definition whose ZooKeeper path or replica name differs from the server defaults'
sidebarTitle: '058: Enforce default replication path'
slug: '/aiven/patches/058-enforce-default-replication-path'
title: 'Patch 058: Enforce the default replication path'
doc_type: 'reference'
aiven_identity: '058'
aiven_absorbs: []
aiven_changelog_category: 'Improvement'
aiven_settings:
  - 'aiven_enforce_default_replication_path'
---

# Patch 058 — Enforce the default replication path {#patch-058-enforce-default-replication-path}

## Summary {#summary}

A `ReplicatedMergeTree` table keeps its replication state under a ZooKeeper path and a
replica name, which upstream lets the user choose. With the server setting
`aiven_enforce_default_replication_path`, a new definition must use the server's
`default_replica_path` and `default_replica_name`: omitting both arguments, or writing the
defaults literally, is accepted; anything else is rejected with `BAD_ARGUMENTS`. Aiven
carries it so that every table's replication state lives where the server puts it, and no
table can be pointed at Keeper state that belongs to something else. Tables that already
exist keep loading. Nothing changes unless the server setting is enabled, which the managed
configuration does.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.8 | `b1b76407e50` | carried, always on, checked on every load path |
| 26.3 | `patch-drop(058)` `1592a945ce2`, then `patch(058)` `b0c09890e35` | dropped in favour of an upstream setting, then re-ported behind the new default-off `aiven_enforce_default_replication_path`, unchanged otherwise |
| 26.8 | `patch-port(058)` | `still-needed-but-rewrite`: same setting and messages, checked only for new definitions |

The 26.3 drop relied on upstream's `database_replicated_allow_replicated_engine_arguments`
rejecting explicit arguments. That setting is session-level, applies only inside a
`Replicated` database, and the managed configuration sets it to `1` (allow, with a warning),
so it never replaced the patch; the re-port reversed the drop.

## Background (ClickHouse) {#background}

`ReplicatedMergeTree(zookeeper_path, replica_name, ...)` takes the two arguments from the
query or, when they are omitted, from the server settings `default_replica_path` (default
`/clickhouse/tables/{uuid}/{shard}`) and `default_replica_name` (default `{replica}`). At
`CREATE`, `{database}` and `{table}` are substituted and the result is stored in the table's
metadata; `{uuid}`, `{shard}` and `{replica}` are stored as written and expanded every time
the table loads.

The same parsing function runs on several paths: a new definition (`CREATE`, an `ATTACH` with
a full definition, `RESTORE`), and paths that re-read a definition the server already holds:
startup load (`FORCE_ATTACH`), a short `ATTACH TABLE t`, the replay of a `Replicated`
database's DDL log on the other replicas (`SECONDARY_CREATE`), recovery of a lost replica, and
the backup collector reading a table's path back. Upstream 26.8 tells them apart for its own
path-component validation.

## Component tour {#component-tour}

- **`checkDefaultReplicationPath`** (new, `registerStorageMergeTree.cpp`). Compares the
  requested path and replica name with the server defaults after substituting `{database}`
  and `{table}`, and throws `BAD_ARGUMENTS` naming the default.
- **`extractZooKeeperPathAndReplicaNameFromEngineArgs`** (upstream). Takes a new
  `enforce_default_replication_path` argument and calls the check in its `expand_macro`
  lambda, which both the explicit-arguments and the omitted-arguments branches use.
- **`registerStorageMergeTree`** (upstream). Computes the argument: the server setting is on,
  and the definition is new (`CREATE`, a full-definition `ATTACH`, or `RESTORE`) and not a
  recovery from stored metadata.
- **`extractZooKeeperPathFromReplicatedTableDef`** (upstream, backups). Passes `false`.
- **Setting.** `aiven_enforce_default_replication_path` in `ServerSettings.cpp`.

## Problem {#problem}

An explicit path lets one table's replication state be placed anywhere in Keeper, including
where another table's or another component's state lives. Upstream's guard can be turned off
per session and does not cover tables outside `Replicated` databases.

## Decisions {#decisions}

### The patch stays {#decision-the-patch-stays}

- **Chosen:** carry the server-level check on 26.8.
- **Rejected:** upstream's `database_replicated_allow_replicated_engine_arguments`, as the
  26.3 drop tried; see Lineage.
- **Rung:** 5, a check inside upstream's argument parsing. Rung 1 cannot serve: no upstream
  setting enforces the path for every table at server level.

### Only new definitions are checked {#decision-scope}

- **Chosen:** check `CREATE`, a full-definition `ATTACH` and `RESTORE`; skip startup load,
  short `ATTACH`, DDL-log replay, recovery from stored metadata and the backup read-back
  (user, 2026-11-05).
- **Rejected:** checking every path, as 25.8 and 26.3 did. Those paths re-read a definition
  the server already accepted, so checking them adds no protection, and a table whose stored
  path differs from the current default (created before the check was enabled, or before a
  change of `default_replica_path`) would fail to load at restart, could not be re-attached,
  and would be dropped silently from a backup's replicated-data coordination.
- **`RESTORE` stays checked** although upstream's own predicate skips it: a backup is input
  from outside the server, and a backup of a table on the default path passes.
- **Own flag:** computed at the call site rather than reusing `validate_substitutions`, so an
  upstream change to that predicate cannot silently change this patch's scope.

## Approach {#approach}

The check runs before upstream resolves the path, so a rejected definition creates no
Keeper state. Both sides of the comparison substitute only `{database}` and `{table}`, as
stored metadata does: a table created with omitted arguments stores the default with
`{uuid}` unexpanded, so a later `CREATE ... AS` or `RESTORE` of that definition still equals
the default. Defaults that contain `{database}` or `{table}` are compared after substitution
for the new table, so copying such a definition to another name is rejected; the managed
default contains neither.

## Concept {#concept}

A guard belongs where input enters the system, not everywhere state is re-read. When one
function serves both, pass the guard an explicit "this is new input" signal from the caller
that knows.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-but-rewrite`

- The anchor in `extractZooKeeperPathAndReplicaNameFromEngineArgs` is unchanged in shape; it
  gained a `validate_substitutions` argument.
- Upstream added a "fresh definition" predicate and a "must never reject" contract for the
  backup read-back, which the 26.3 shape would break.
- The 26.3 helper's comment claimed `{uuid}` is expanded with a nil UUID; special-only
  expansion never expands `{uuid}`. The port drops the helper's nil UUID and the comment.

## Customer impact {#customer-impact}

- **Who:** every service whose configuration enables `aiven_enforce_default_replication_path`,
  which is every managed service.
- **What they see**, from the integration test, with the managed defaults
  `/clickhouse/tables/{uuid}/{shard}` and `{replica}`:

```sql
-- Accepted: arguments omitted; the stored definition shows the defaults.
CREATE TABLE db.events (a Int64) ENGINE = ReplicatedMergeTree ORDER BY a;

-- Accepted: the defaults written out.
CREATE TABLE db.events2 (a Int64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY a;

-- Rejected, inside or outside a Replicated database:
CREATE TABLE db.events3 (a Int64)
ENGINE = ReplicatedMergeTree('/somewhere/else/{shard}', '{replica}') ORDER BY a;
-- Code: 36. DB::Exception: Setting ZooKeeper path to /somewhere/else/{shard} is not allowed,
-- please omit it or set equal to /clickhouse/tables/{uuid}/{shard}. (BAD_ARGUMENTS)

CREATE TABLE db.events4 (a Int64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', 'other_{replica}') ORDER BY a;
-- Code: 36. DB::Exception: Setting replica name to other_{replica} is not allowed,
-- please omit it or set equal to {replica}. (BAD_ARGUMENTS)
```

- **Existing tables** with a non-default path keep working: they load at restart, accept
  inserts, can be detached and re-attached with `ATTACH TABLE t`, and can be backed up.
  Restoring such a backup is rejected with the ZooKeeper path message.
- **Unchanged:** `ENGINE = MergeTree` in a `Replicated` database with patch 004 (converted with
  omitted arguments); `RESTORE` of a table on the default path; a restore that replays stored
  definitions as `ATTACH TABLE ... UUID ...` with the default arguments. A table with a
  non-default path cannot be restored either way, as on 25.8 and 26.3.
- **How to restore upstream behaviour:** disable the server setting.
- **New versus 26.3:** existing tables with a non-default path load, re-attach and back up,
  where 26.3 rejected them.
- **Upstream tests edited:** none.

## Security review {#security-review}

The check is the boundary that keeps a table's replication state at the path the server
assigns. Every way to supply a new definition is checked: `CREATE` (including
`CREATE ... AS`, `ON CLUSTER` and `Replicated` databases), a full-definition `ATTACH`, and
`RESTORE`. The skipped paths only re-read definitions stored by the server; a user cannot
supply them except by first creating them through a checked path. A session cannot change
the server setting.

## Tests {#tests}

- **Paths:** `tests/integration/test_aiven_enforce_default_replication_path/`
- **Proves:** with the setting on, omitted and explicit default arguments pass; a foreign
  path and a foreign replica name are rejected inside a `Replicated` database even with
  `database_replicated_allow_replicated_engine_arguments = 1`; a foreign path is rejected
  outside a `Replicated` database and in a full-definition `ATTACH`; a full-definition
  `ATTACH` with the default arguments and an explicit UUID, the form a restore that replays
  stored definitions uses, passes and can be restored with `SYSTEM RESTORE REPLICA`; a converted `MergeTree`
  passes; `RESTORE` of a default-path table passes. A table created with a foreign path
  before the setting was enabled loads after restart in both an `Atomic` and a `Replicated`
  database, accepts inserts, re-attaches and backs up, while restoring its backup is rejected.
  With the setting off a foreign path is accepted.
- **Run:** `python -m ci.praktika run "integration" --test test_aiven_enforce_default_replication_path`,
  or `pytest` from `tests/integration` with `CLICKHOUSE_TESTS_SERVER_BIN_PATH` set.
- **Evidence:** on the parent commit every test errors at setup, because the server rejects
  the unknown `aiven_enforce_default_replication_path` element (`UNKNOWN_ELEMENT_IN_CONFIG`);
  with the patch all 11 pass (2026-11-05).

## Rollback {#rollback}

- **Downgrade:** not supported; services only move forward.
- **Revert in a later release:** no on-disk or Keeper format of its own.
- **Disable without rebuild:** set `aiven_enforce_default_replication_path` to false and
  restart.

## Retirement {#retirement}

An upstream server-level setting that enforces the default replication path for every
`ReplicatedMergeTree` definition. Re-check upstream on every uplift; an upstream
contribution would make `database_replicated_allow_replicated_engine_arguments` enforceable at
server level and cover tables outside `Replicated` databases. The release that drops the
patch keeps `aiven_enforce_default_replication_path` declared with `SettingsTierType::OBSOLETE`;
an unknown element stops the server from starting.

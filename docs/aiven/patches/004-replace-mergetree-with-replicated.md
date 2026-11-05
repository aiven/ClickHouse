---
description: 'Create a non-replicated MergeTree-family table in a Replicated database with its Replicated engine, so its data replicates'
sidebarTitle: '004: MergeTree to Replicated in Replicated databases'
slug: '/aiven/patches/004-replace-mergetree-with-replicated'
title: 'Patch 004: Replace MergeTree with ReplicatedMergeTree in Replicated databases'
doc_type: 'reference'
aiven_identity: '004'
aiven_absorbs: []
aiven_changelog_category: 'Improvement'
aiven_settings:
  - 'aiven_replace_mergetree_with_replicated'
---

# Patch 004 — Replace MergeTree with ReplicatedMergeTree in Replicated databases {#patch-004-replace-mergetree-with-replicated}

## Summary {#summary}

In a `Replicated` database, `CREATE TABLE ... ENGINE = MergeTree` (or any engine of the
`MergeTree` family) creates the table with its `Replicated*` engine instead, so the table's
data replicates like its definition. Aiven carries it because a `Replicated` database
replicates only definitions: a plain `MergeTree` table there holds different data on every
replica, and the managed configuration routes `Distributed` inserts to a single replica, which
for such a table loses data on the others. Nothing changes unless the server setting
`aiven_replace_mergetree_with_replicated` is enabled, which the managed configuration does.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.8 | `a79091a04c0` (carry of the original `226ed6cc31`) | carried, always on, no guards |
| 26.3 | `patch-port(004)` `70124fbd668`; follow-up `29015bd1d36` | `still-needed-but-rewrite`: gated behind the new default-off `aiven_replace_mergetree_with_replicated`, `ATTACH` excluded, rewrite only to a registered engine. The follow-up re-keyed the gate on `is_replicated_database_internal` after a rebase made the original `SECONDARY_QUERY` check a silent no-op |
| 26.8 | `patch-port(004)` | `still-needed`, one commit: the 26.3 shape after its follow-up |

## Background (ClickHouse) {#background}

A `Replicated` database keeps a DDL log in Keeper. A `CREATE` on any replica is written to the
log, and every replica, the initiator included, applies the entry with
`ClientInfo::is_replicated_database_internal` set. The database replicates definitions only:
whether a table's data replicates depends on its engine. `ReplicatedMergeTree` replicates
through Keeper; `MergeTree` does not.

The `internal_replication` setting of a `Replicated` database (a `DatabaseReplicatedSettings`
key, default `false`) decides how a `Distributed` table over the database's cluster inserts:
with `true` it writes to one replica per shard and relies on the table to replicate. That is
the right choice for replicated tables, and loses data for non-replicated ones.

Upstream has two related mechanisms and neither converts at `CREATE`:
`database_replicated_allow_only_replicated_engine` (default `false`) rejects a non-replicated
table that stores data on disk, and the `convert_to_replicated` flag converts an existing table
of an `Atomic` or `Ordinary` database at the next server start.

## Component tour {#component-tour}

- **`StorageFactory::get`** (upstream, DDL path). Resolves the engine of every table that is
  created or attached. The patch calls `rewriteUnreplicatedMergeTreeEngine` once, after the
  `ENGINE required` check and before the engine name is read.
- **`StorageFactory::rewriteUnreplicatedMergeTreeEngine`** (new, private). Renames
  `storage_def->engine->name` in place when all of these hold: the server setting is on, the
  query is not an `ATTACH`, the name ends with `MergeTree` and does not start with `Replicated`,
  the context is the replicated-database internal one, the database engine is `Replicated`, and
  `Replicated` + name is a registered engine.
- **`DatabaseReplicated::commitCreateTable`** (upstream). Writes the same `ASTCreateQuery` to
  Keeper and to local metadata, so the stored definition carries the `Replicated*` engine.
- **`InterpreterCreateQuery::doCreateTable`** (upstream). Runs the
  `database_replicated_allow_only_replicated_engine` check on the storage that was built, so a
  converted table passes it.
- **Setting.** `aiven_replace_mergetree_with_replicated` in `ServerSettings.cpp`.

## Problem {#problem}

A user who writes `ENGINE = MergeTree` in a `Replicated` database gets a table that exists on
every replica and holds different data on each. Inserts through a `Distributed` table land on
one replica; reads through it pick any replica, so results change from query to query, and a
replica's data is lost when it is replaced. Nothing warns the user.

## Decisions {#decisions}

### The patch stays {#decision-the-patch-stays}

- **Chosen:** carry the conversion on 26.8.
- **Rejected:** replacing it with upstream's reject setting. Rejecting breaks every client and
  migration script that writes `ENGINE = MergeTree`; converting keeps them working.
- **Rung:** 5, an edit to `StorageFactory::get`, the upstream path of every table creation,
  kept to one call into a private method. Rung 1 cannot serve: no upstream setting converts.
  Rung 2 is open as an upstream `database_replicated_*` setting that converts instead of
  rejecting.

### The rewrite stays in `StorageFactory` {#decision-location}

- **Chosen:** rename the engine where the storage is resolved, on the replica that applies the
  DDL log entry.
- **Rejected:** `InterpreterCreateQuery::setEngine`, upstream's place for choosing a default
  engine. It also runs on the initiator before the entry is written and on the `ON CLUSTER`
  preflight, so the log entry would change shape; more surface, no gain.

### The gate keys on `is_replicated_database_internal` {#decision-signal}

- **Chosen:** the flag upstream itself uses to detect the DDL-log path
  (`InterpreterCreateQuery::assertOrSetUUID`).
- **Rejected:** `query_kind == SECONDARY_QUERY`, the 25.8 check. Upstream stopped setting it on
  this path during 26.3, which silently disabled the conversion. The flag is drift-prone too:
  the positive conversion test is what catches a future move.

### Non-`MergeTree` disk engines are out of scope {#decision-scope}

- **Chosen:** convert only the `MergeTree` family; leave `Log`, `File`, `Set`, `Join` and the
  like to upstream's reject setting.
- **Rejected:** rejecting them in this patch. That changes behaviour for existing users of such
  tables and duplicates an upstream setting.
- **Consequence:** the managed configuration does not enable
  `database_replicated_allow_only_replicated_engine` today, so such tables are accepted and do
  not replicate. Enabling it is tracked as a configuration follow-up; the test proves the two
  compose.

## Approach {#approach}

The rename happens on the `ASTCreateQuery` the interpreter later stores, so the local metadata,
the database's metadata in Keeper and `SHOW CREATE TABLE` all show the `Replicated*` engine,
and every replica derives the same definition from the same log entry. A replica that recovers
from Keeper reads that definition and needs no rewrite.

The conversion uses the engine's default replication path and replica name, so the table gets
the arguments upstream recommends for a `Replicated` database. Engine arguments that are not
replication paths, such as the version column of `ReplacingMergeTree(v)`, are kept.

A materialized view's inner table is created with a copy of the view's context, so it converts
too. A refreshable view in `APPEND` mode converts the same way; its coordinated refresh then
runs on one replica and the result replicates. A refreshable view without `APPEND` is still
refused when it names a non-replicated engine: upstream checks the written engine before the
inner table exists, and the patch does not change that check.

The setting is read on every `CREATE`, from the server configuration loaded at startup.

## Concept {#concept}

A database that replicates definitions and an engine that replicates data are two separate
promises. When users cannot be expected to know the difference, the safe default is to make the
definition imply the data, at the single point every replica passes through, and to apply it
deterministically so every replica stores the same result.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed`

- The `StorageFactory::get` anchor, `ClientInfo::is_replicated_database_internal` and its
  setter `setQueryKindReplicatedDatabaseInternal` are unchanged.
- `database_replicated_allow_only_replicated_engine` still runs after the storage is built.
- All eight `*MergeTree` engines on 26.8 have a registered `Replicated*` engine, including
  `CoalescingMergeTree`, which is new since 26.3.

## Customer impact {#customer-impact}

- **Who:** every service whose configuration enables `aiven_replace_mergetree_with_replicated`,
  which is every managed service.
- **What they see:** `CREATE TABLE ... ENGINE = MergeTree` in a `Replicated` database reports
  `ReplicatedMergeTree` in `system.tables` and `SHOW CREATE TABLE`, and its data replicates.
  Tables in other databases, attached tables and engines outside the `MergeTree` family are
  unchanged.
- **How to restore upstream behaviour:** create the table in a non-`Replicated` database, or
  disable the server setting.
- **New versus 26.3:** none; the 26.8 behaviour is the 26.3 behaviour after its follow-up.
- **Upstream tests edited:** none.
- **Example**, from the integration test, on a server with the setting on:

```sql
CREATE TABLE rdb_on.t_mergetree (a Int64) ENGINE = MergeTree ORDER BY a;
SELECT engine FROM system.tables WHERE database = 'rdb_on' AND name = 't_mergetree';
-- ReplicatedMergeTree
```

## Security review {#security-review}

n/a — the patch picks an engine for a table the user is already allowed to create. The
replicated engine uses the server's default replication path, built by the server; the user
supplies no Keeper path.

## Tests {#tests}

- **Paths:** `tests/integration/test_aiven_replace_mergetree_with_replicated/`
- **Proves:** with the setting on, `MergeTree`, `SummingMergeTree`, `ReplacingMergeTree(v)` and
  `CoalescingMergeTree` convert on both replicas and their data replicates; a materialized
  view's inner table and an `APPEND` refreshable view's inner table convert; a refreshable view
  without `APPEND` keeps upstream's refusal; a replica that joins later recovers the converted
  definitions and the data; upstream's reject setting accepts converted tables and rejects
  `Log`; a table in an `Atomic` database, an engine without a replicated twin and an `ATTACH`
  are not converted; with the setting off the same `CREATE` behaves as upstream.
- **Run:** `python -m ci.praktika run "integration" --test test_aiven_replace_mergetree_with_replicated`,
  or `pytest` from `tests/integration` with `CLICKHOUSE_TESTS_SERVER_BIN_PATH` set.
- **Evidence:** on the parent commit every test errors at setup, because the server rejects the
  unknown `aiven_replace_mergetree_with_replicated` element (`UNKNOWN_ELEMENT_IN_CONFIG`); with
  the patch all 13 pass (2026-11-05).

## Rollback {#rollback}

- **Downgrade:** not supported; services only move forward.
- **Revert in a later release:** no on-disk or Keeper format of its own. Converted tables stay
  `Replicated*`, since that is what their metadata says.
- **Disable without rebuild:** set `aiven_replace_mergetree_with_replicated` to false and
  restart. Only future `CREATE`s are affected.

## Retirement {#retirement}

An upstream setting that converts non-replicated `MergeTree` tables in a `Replicated` database
at `CREATE`. Re-check upstream on every uplift; this is also the patch to offer upstream. The
server setting lives in deployed configurations, so the release that drops the patch keeps
`aiven_replace_mergetree_with_replicated` declared with `SettingsTierType::OBSOLETE`; an
unknown element stops the server from starting.

---
description: 'Lower the default logs_to_keep of new Replicated databases from 1000 to 300 to reduce the DDL log kept in ZooKeeper'
sidebarTitle: '010: Default logs_to_keep 300'
slug: '/aiven/patches/010-default-logs-to-keep'
title: 'Patch 010: Default logs_to_keep 300 for Replicated databases'
doc_type: 'reference'
aiven_identity: '010'
aiven_absorbs: []
aiven_changelog_category: 'Improvement'
aiven_settings:
  - 'logs_to_keep (Replicated database setting, default changed)'
---

# Patch 010 — Default `logs_to_keep` 300 for `Replicated` databases {#patch-010-default-logs-to-keep}

## Summary {#summary}

Lowers the default of the `Replicated` database setting `logs_to_keep` from 1000 to 300. A new
`Replicated` database keeps its last 300 DDL log entries in ZooKeeper instead of 1000, about 70% fewer
nodes per database. Managed services often have many databases, and the ZooKeeper ensemble keeps every
node in memory. Existing databases are not affected: the value is stored in ZooKeeper when a database is
created.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.8 | `199db087991c` | original; replaced an earlier design with a server setting |
| 26.3 | `1d008a142a8` | cherry-pick, with a stateless test |
| 26.8 | `patch-port(010)` | `still-needed`, ported unchanged; the in-source engine documentation now states the new default |

## Background (ClickHouse) {#background}

A `Replicated` database replicates DDL through a log in ZooKeeper under the database path: one node per
entry under `log`, a `max_log_ptr` counter and a `logs_to_keep` node. The first replica to create the
database writes `logs_to_keep` from its `DatabaseReplicatedSettings` (`DatabaseReplicated::buildDatabaseNodesInZooKeeper`).
From then on the value in ZooKeeper is the one that counts: `DatabaseReplicatedDDLWorker` reads it, deletes
older entries, and treats a replica whose `log_ptr` is more than `logs_to_keep` behind `max_log_ptr` as
lost. A lost replica recovers from a snapshot of the metadata of another replica instead of replaying the
log.

The defaults of `DatabaseReplicatedSettings` come from the code, then the `<database_replicated>` section
of the server configuration (since 25.9), then the `SETTINGS` clause of `CREATE DATABASE`.

## Component tour {#component-tour}

- **`DatabaseReplicatedSettings.cpp`**: the `logs_to_keep` declaration; the patch changes its default.
- **`DatabaseReplicated.cpp`**: writes `<path>/logs_to_keep` at creation; its `Documentation` string,
  the source of the engine reference page, lists the settings and their defaults.
- **`DatabaseReplicatedWorker.cpp`**: reads `<path>/logs_to_keep`; the lost-replica check and log cleanup.

## Problem {#problem}

With the upstream default, every `Replicated` database keeps up to 1000 log entries in ZooKeeper, each
with the full DDL text. On a service with many databases this is a large share of the ensemble's memory,
while the entries a replica actually needs to catch up are far fewer.

## Decisions {#decisions}

- **Code default, not configuration.** Since 25.9 the `<database_replicated>` server configuration section
  gives the same effect for new databases without a code change, and explicit `SETTINGS` still win. It was
  considered for this uplift and not taken; the patch is ported as is. Moving to configuration is the
  retirement path.
- **300.** Six times `max_replication_lag_to_enqueue` (default 50), beyond which a replica already refuses
  new queries. A replica that falls further behind recovers from a snapshot, which is slower than replaying
  but correct.
- **New databases only.** The stored value is not rewritten. Lowering it for existing databases is an
  operational task, done directly in ZooKeeper, outside the server.
- **In-source documentation updated.** The settings table in the `Documentation` string said 1000; it now
  says 300, so `system.database_engines` matches the binary. The generated reference page under
  `docs/reference/` is left to the documentation generator.

## Approach {#approach}

One default in the `DECLARE` list, and the matching row in the documentation string.

## Concept {#concept}

A database-level setting that is persisted in ZooKeeper at creation behaves like a format parameter: its
code default only seeds new objects. Changing it never migrates existing ones.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed`

- 26.8 and upstream keep the default 1000 and the `NonZeroUInt64` type.
- The creation path, the worker's read of `<path>/logs_to_keep` and the lost-replica check are unchanged.
- The `<database_replicated>` configuration section exists, so the patch could be replaced by
  configuration (see Retirement).

## Customer impact {#customer-impact}

- **Who:** every service, for `Replicated` databases created on 26.8.
- **What they see:** nothing in normal operation. `SELECT value FROM system.zookeeper` on the database
  path shows `logs_to_keep` 300. A replica that is down long enough to miss more than 300 DDL entries
  recovers from a snapshot instead of replaying them.
- **How to restore upstream behaviour:** `CREATE DATABASE ... ENGINE = Replicated(...) SETTINGS
  logs_to_keep = 1000`, or set it in `<database_replicated>`.
- **New versus 26.3:** none.
- **Upstream tests edited:** none. `test_database_replicated_settings` passes unchanged: it only checks
  the configured value and settings given in the query.

## Security review {#security-review}

n/a — the patch changes a default; it adds no input, privilege or path.

## Tests {#tests}

- **Paths:** `tests/queries/0_stateless/aiven_010_default_logs_to_keep.{sql,reference}`. The 26.3 test,
  renamed to the 26.8 scheme. It now uses the per-test database name instead of a fixed one, so it no
  longer needs `no-parallel`, and carries the `zookeeper` tag.
- **Proves:** a new `Replicated` database created without `SETTINGS` writes `logs_to_keep` 300 to ZooKeeper.
- **Run:** `python3 -m ci.praktika run "Stateless tests (amd_debug, parallel)" --test aiven_010_default_logs_to_keep`.
- **Evidence:** on the parent commit the test fails with 1000 instead of 300; with the patch it passes,
  three runs (2026-11-17). Upstream `test_database_replicated_settings` passes with the patch.

## Rollback {#rollback}

- **Downgrade:** not supported; services only move forward.
- **Revert in a later release:** safe. Databases created with 300 keep it, because it is stored in
  ZooKeeper; new databases would get 1000.
- **Disable without rebuild:** set `logs_to_keep` in `<database_replicated>`, or in the `SETTINGS` of
  `CREATE DATABASE`.

## Retirement {#retirement}

Replace with configuration: the managed configuration sets `logs_to_keep` 300 in the
`<database_replicated>` section it already writes, then a later uplift drops this patch with
`patch-drop(010)`. Nothing is declared by the patch, so dropping it needs no compatibility shim.

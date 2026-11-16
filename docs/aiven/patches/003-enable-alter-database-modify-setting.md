---
description: 'Dropped: ALTER DATABASE ... MODIFY SETTING for Replicated databases, written to change cluster_secret at runtime, which is not a database setting'
sidebarTitle: '003: ALTER DATABASE MODIFY SETTING (dropped)'
slug: '/aiven/patches/003-enable-alter-database-modify-setting'
title: 'Patch 003: ALTER DATABASE MODIFY SETTING for Replicated databases (dropped)'
doc_type: 'reference'
aiven_identity: '003'
aiven_absorbs: []
aiven_changelog_category: 'Improvement'
aiven_settings: []
---

# Patch 003 — `ALTER DATABASE ... MODIFY SETTING` for `Replicated` databases (dropped) {#patch-003-enable-alter-database-modify-setting}

## Summary {#summary}

The patch let `ALTER DATABASE ... MODIFY SETTING` change the settings of a `Replicated` database, so that
`cluster_secret` could be changed without recreating the database. Since 22.8 the `ALTER` has had no effect
on the secret, and since 24.3 it is rejected, because `cluster_secret` is no longer a database setting:
the secret comes from a named collection. It is not carried on 26.8; nothing uses it.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 21.11 | package patch set | original, with a companion patch that added `cluster_secret` as a database setting; the `ALTER` changed the secret in memory |
| 22.8 | package patch set | companion rebased onto upstream's named collections, copying the setting only at construction; the `ALTER` is accepted and has no effect |
| 23.8 | package patch set | unchanged |
| 24.3 | package patch set | companion removed (the secret comes from a named collection); the `ALTER` on `cluster_secret` fails with `BAD_ARGUMENTS` |
| 25.3 | `f26bb5e6d60` | applied from the package patch |
| 25.8 | `1055b0defeb8` | re-applied |
| 26.3 | `924255927119` | cherry-pick; the test uses `max_broken_tables_ratio` |
| 26.8 | `patch-drop(003)` | dropped, `irrelevant-by-removal` |

## Background (ClickHouse) {#background}

A `Replicated` database builds a cluster of its replicas for `ON CLUSTER`-style work and for the
`cluster` and `clusterAllReplicas` functions. Since upstream 22.4 the credentials of that cluster, including
`cluster_secret`, come from a named collection in the server configuration, referenced by the database
setting `collection_name`. `DatabaseReplicated` reads the collection once, when the database is constructed
(`fillClusterAuthInfo`), and builds the cluster from that copy.

The base `IDatabase::applySettingsChanges` throws `NOT_IMPLEMENTED`; upstream overrides it only for engines
whose settings can change (for example `MaterializedPostgreSQL`, which validates all changes first and
writes the database metadata).

## Problem {#problem}

The patch was written before named collections, when the secret was a database setting and could only be
changed by recreating the database. With named collections that problem no longer exists in that form: the
secret lives in the server configuration, and a database picks up a new one when it is constructed again
(restart, or `DETACH`/`ATTACH DATABASE`).

## Decisions {#decisions}

### Drop the patch {#decision-drop}

- **Chosen:** do not carry it.
- **Rejected:** porting it as on 26.3. Its only working effect is an in-memory change of tunables such as
  `max_broken_tables_ratio` on the replica that runs the `ALTER`: not written to the database metadata,
  not sent to the other replicas, lost on restart, and written while the DDL worker reads the settings
  without the database lock. Nothing uses it.
- **Rejected:** rewriting it as a runtime secret change (re-read the named collection, rebuild the cluster).
  There is no requirement for it, and a change on one server at a time would break authentication between
  servers until all have the new secret.

## Concept {#concept}

A patch can lose its purpose without breaking the build: when upstream moves the data a patch acts on, a
rebase that only fixes compilation keeps the code and loses the effect. Re-test the stated purpose after such
a rebase, with the query the patch was written for.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `irrelevant-by-removal`

- `DatabaseReplicatedSettings` has no `cluster_secret`; the credentials come from the named collection
  (`collection_name`), read at construction.
- Upstream has no `DatabaseReplicated::applySettingsChanges` override.

## Customer impact {#customer-impact}

- **Who:** none. The managed admin role is not allowed to run `ALTER DATABASE ... MODIFY SETTING`, and the
  control plane does not use it; it supplies the secret through the named collection.
- **What changes against 26.3:** for a user with that grant, `ALTER DATABASE <replicated> MODIFY SETTING ...`
  fails with `NOT_IMPLEMENTED` instead of changing the setting in memory until restart.
- **Upstream tests edited:** none.

## Security review {#security-review}

Dropping the patch removes a way to change database settings at runtime and adds nothing. How the cluster
authenticates is unchanged: the secret still comes from the named collection.

## Retirement {#retirement}

Retired on 26.8. If changing the secret without a restart becomes a requirement, it needs a new design
(re-reading the named collection, and accepting both secrets while servers switch), not this patch.

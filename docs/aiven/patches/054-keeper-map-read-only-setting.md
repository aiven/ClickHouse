---
description: 'A read_only table setting for KeeperMap that rejects writes, used to fence a table while its data is read from Keeper'
sidebarTitle: '054: KeeperMap read_only setting'
slug: '/aiven/patches/054-keeper-map-read-only-setting'
title: 'Patch 054: KeeperMap read_only setting'
doc_type: 'reference'
aiven_identity: '054'
aiven_absorbs: []
aiven_changelog_category: 'New Feature'
aiven_settings:
  - 'read_only (KeeperMap table setting)'
---

# Patch 054 — `KeeperMap` `read_only` setting {#patch-054-keeper-map-read-only-setting}

## Summary {#summary}

Adds a table setting `read_only` to `KeeperMap`. While it is on, `INSERT`, `TRUNCATE`, `DELETE`,
`ALTER UPDATE` and `ALTER DELETE` fail with `TABLE_IS_READ_ONLY`; `SELECT` works. It is set with
`CREATE TABLE ... SETTINGS read_only = 1` or `ALTER TABLE ... MODIFY SETTING read_only = 1`, and cleared with
`MODIFY SETTING read_only = 0` or `RESET SETTING read_only`. The backup tool uses it to stop writes while it
reads a table's data directly from Keeper.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.x | `4c16ca65e99e` | original |
| 26.3 | `e2bb2a5649c9` | cherry-pick; constructor merge and three API adaptations |
| 26.8 | `patch-port(054)` | `still-needed`, rewritten on upstream's `StorageEmbeddedRocksDB` settings pattern; fence in the sink; `Replicated` database routing fixed |

## Background (ClickHouse) {#background}

A `KeeperMap` table keeps its rows as nodes under a Keeper path; several tables, on one or more servers,
can share the path. The table's columns and primary key are also stored in Keeper and compared when the
table attaches.

Upstream `KeeperMap` has no table settings and allows only comment `ALTER`s
(`IStorage::checkAlterIsPossible`). `StorageEmbeddedRocksDB` is the upstream example of an engine with its
own settings: a `RocksDBSettings` class, `checkAlterIsPossible` allowing comment and settings `ALTER`s and
validating the values (#88443), and `alter` that rebuilds the settings from defaults into a `MultiVersion`.

An `INSERT` into `KeeperMap` collects the block in a sink and writes it to Keeper as one `multi` in
`finalize`. `INSERT` holds the table lock for share and `ALTER` holds the alter lock; they do not exclude
each other.

In a `Replicated` database `DatabaseReplicated::shouldReplicateQuery` keeps `KeeperMap` mutations local
(the data is shared, so they run once) and sends only comment `ALTER`s through the database queue,
because they change the table's metadata.

## Component tour {#component-tour}

- **`KeeperMapSettings`** (new, `src/Storages/KeeperMapSettings.{h,cpp}`). One setting, `read_only`
  (`Bool`, default `false`). `loadFromQuery` reads the `SETTINGS` clause of `CREATE`; `checkCanSet`
  validates a change; `hasBuiltin` feeds the engine registration.
- **`StorageKeeperMap`** (upstream).
  - The settings live in a `MultiVersion<KeeperMapSettings>`; `checkNotReadOnly` reads the current
    version.
  - `write`, `truncate` and `mutate` call it when the query starts. The sink's `finalize` and the three
    `DELETE` `multi` paths call it again inside their retry loops, right before the request to Keeper.
  - `checkAlterIsPossible` allows comment and settings `ALTER`s only and validates the settings.
  - `alter` applies the commands to the metadata, stores the new definition, and builds a fresh
    `KeeperMapSettings` from the stored changes.
  - `create` loads the settings and keeps the `SETTINGS` clause in the metadata, so that `ALTER` can
    edit it.
  - The registration sets `supports_settings` and `has_builtin_setting_fn`; the engine documentation
    describes the setting.
- **`DatabaseReplicated::shouldReplicateQuery`** (upstream). Settings `ALTER`s on `KeeperMap` go
  through the database queue, as comment `ALTER`s do.
- **Failpoint `keeper_map_sink_pause_before_multi`** (new, `PAUSEABLE_ONCE`). Pauses the sink before its
  Keeper write; used by the test.

## Problem {#problem}

To back up a `KeeperMap` table the backup tool reads its nodes straight from Keeper. Writes that land
during the read make the copy inconsistent. Upstream has no way to stop writes to one table without
changing grants or Keeper ACLs.

## Decisions {#decisions}

### Rewrite on the `StorageEmbeddedRocksDB` pattern {#decision-rocksdb-pattern}

- **Chosen:** follow upstream's engine-settings pattern. This fixes three defects in the 26.3
  implementation:
  1. With `read_only` off, every `ALTER` was accepted. Column changes were then applied to the local
     metadata only, which no longer matches the metadata in Keeper, so the table could fail to attach.
     Now only comment and settings `ALTER`s are allowed, as upstream allows only comment `ALTER`s.
  2. `RESET SETTING read_only` updated the stored definition but not the setting in memory, so the table
     stayed read-only until restart. The settings are now rebuilt from defaults.
  3. `ALTER` assigned the settings in place while `INSERT` read them, a data race. They are now a
     `MultiVersion`.
- Setting values are validated before the definition is written, so a bad value cannot leave a
  definition that fails to attach.
- **Rejected:** porting the 26.3 code as is.
- **Rung:** 4, a new setting in an upstream storage.

### Fence in the sink {#decision-fence}

- **Chosen:** check `read_only` again right before each Keeper write. An `INSERT` that started before the
  `ALTER` and has not written yet fails.
- **Rejected:** a per-table lock that makes the `ALTER` wait for in-flight writes. It is an exact fence,
  but it is new locking in an upstream storage, and the `ALTER` would wait for long `INSERT`s.
- **Rejected:** checking only when the query starts (26.3). A long `INSERT` commits after the `ALTER` has
  returned.

### Replicate settings `ALTER`s in `Replicated` databases {#decision-replicated}

- **Chosen:** send settings `ALTER`s on `KeeperMap` through the database queue, like comment `ALTER`s.
  Every replica then applies the setting to its own table, and the new definition is committed to the
  database's metadata in Keeper.
- **Rejected:** keeping the upstream rule, which runs every non-comment `KeeperMap` `ALTER` locally. For a
  settings `ALTER` that hits an assertion in debug builds. In release builds only the initiating replica
  changes; its local digest no longer matches Keeper, so the replica is treated as lost the next time
  replication initialises. The 26.3 implementation has this bug.

### Keep the name `read_only` {#decision-name}

- **Chosen:** `read_only`, without the `aiven_` prefix. It is a per-engine setting, not a server or
  query setting, and the backup tool already uses this name.

## Approach {#approach}

`RESET SETTING` of the last setting would leave an empty `SETTINGS` clause, which the generic metadata
code writes out and then cannot parse back. `alter` keeps `read_only = false` in the clause instead. The
generic behaviour is an upstream candidate.

The fence leaves a residual window: a write that has passed the check and is in flight to Keeper
completes after the `ALTER` returns. The window is one Keeper request, not a whole `INSERT`.

With parallel `INSERT` each sink writes its own `multi`, as upstream. A fence that arrives between two
sinks' writes rejects the later ones; the earlier ones stay committed.

The setting is per table. Another table on the same Keeper path, on this or another server, is not
fenced. In a `Replicated` database every replica's table is fenced; in an `Atomic` database the `ALTER` has
to run on every server that has the table.

The sink still writes a block as one `multi` with one request per key. Large blocks can exceed
ZooKeeper's request limit (`jute.maxbuffer`, 1 MB by default). This is upstream behaviour, not changed by
this patch.

## Concept {#concept}

A flag checked when work starts is not a fence for work already started. The check belongs next to the
side effect it guards, here the write to Keeper.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed`

- `KeeperMap` on 26.8 still has no settings, no `checkAlterIsPossible` or `alter`, and no equivalent of
  `read_only`.
- The registration now carries a `Documentation` block; the setting is described there, and the
  reference page is generated from it.
- `override_metadata` in the constructor and `create` is upstream code (shared catalog, under
  `CLICKHOUSE_CLOUD`), not part of this patch; the 26.3 port note had it wrong.
- Upstream fixes to strict-mode `DELETE` and metadata canonicalisation are in the same functions; the
  guards sit next to them unchanged.

## Customer impact {#customer-impact}

- **Who:** services with `KeeperMap` tables.
- **What they see:** during a backup, writes to `KeeperMap` tables fail with `TABLE_IS_READ_ONLY`, as
  before. With 26.8, an `INSERT` that started before the fence and had not written yet also fails, where
  26.3 let it commit into the backup window.
- **Fixed against 26.3:** `ALTER TABLE ... ADD COLUMN` and other column `ALTER`s are rejected with
  `NOT_IMPLEMENTED`, as upstream; 26.3 accepted them and could leave the table unable to attach.
  `RESET SETTING read_only` takes effect immediately. In `Replicated` databases the setting reaches every
  replica, and the replica no longer loses its digest.
- **Existing tables:** a definition with `SETTINGS read_only = ...` written by 26.3 attaches unchanged.
- **How to restore upstream behaviour:** `RESET SETTING read_only`, or never set it.
- **Upstream tests edited:** none.

## Security review {#security-review}

The setting only restricts: it adds a rejection on the write paths and does not let any query do more
than before. Setting it requires the `ALTER MODIFY SETTING` grant on the table, so a user who can write to
a table and change its settings can turn the fence off. It is a consistency tool for the operator, not
access control.

The patch also narrows what `KeeperMap` accepts: column `ALTER`s that 26.3 accepted are rejected again, and
setting values are validated before anything is stored. The `Replicated` database change sends one more
kind of `ALTER` through the queue; the query is unchanged and every replica runs the same storage checks.

## Tests {#tests}

- **Paths:** `tests/queries/0_stateless/aiven_054_keeper_map_read_only_setting.sh`
- **Proves:**
  - Without the setting, a column `ALTER` is `NOT_IMPLEMENTED`; an unknown setting and an invalid value
    are rejected before anything is stored.
  - With `read_only = 1`: `INSERT`, `TRUNCATE`, `DELETE`, `ALTER DELETE` and `ALTER UPDATE` fail with
    `TABLE_IS_READ_ONLY`; `MODIFY COMMENT` and `SELECT` work. The setting survives `DETACH`/`ATTACH`.
  - `RESET SETTING` re-enables writes at once and after `DETACH`/`ATTACH`; it is set on `CREATE`.
  - Fence: an `INSERT` paused at `keeper_map_sink_pause_before_multi`, then `read_only` turned on, then
    resumed, fails, and its row is not stored.
  - In a `Replicated` database `MODIFY SETTING read_only = 1` succeeds and blocks `INSERT`.
- **Run:** `python -m ci.praktika run "Stateless tests (amd_debug, parallel)" --test aiven_054_keeper_map_read_only_setting`
- **Evidence (2026-11-06):**
  - RED on the parent commit `81b84f909fab`: `ALTER TABLE ... MODIFY SETTING read_only = 1` fails with
    `NOT_IMPLEMENTED`.
  - GREEN with the patch: OK, five runs.
  - The 21 upstream `KeeperMap` stateless tests pass with the patch.

## Rollback {#rollback}

- **Downgrade:** not supported; services only move forward.
- **Revert in a later release:** a table whose definition has a `SETTINGS` clause fails to attach on a
  server without the patch, because upstream `KeeperMap` accepts no settings. `RESET SETTING` does not
  remove the clause (it keeps `read_only = false`), so a revert has to strip it from the definitions.
- **Disable without rebuild:** `RESET SETTING read_only` on the table.

## Retirement {#retirement}

An upstream way to make a single table read-only. A good upstream contribution candidate, together with
writing no empty `SETTINGS` clause after `RESET SETTING`.

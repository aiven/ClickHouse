---
description: 'Dossier for the Aiven patch that resolves the shard macro when a replicated table is attached at server startup'
sidebarTitle: '006: Attach with shard macro'
slug: '/aiven/patches/006-replicated-database-attach-with-shard-macro'
title: 'Patch 006: Replicated database attach with shard macro'
doc_type: 'reference'
aiven_identity: '006'
aiven_absorbs: []
aiven_changelog_category: 'Bug Fix'
aiven_settings: []
---

# Patch 006 — Replicated database attach with shard macro {#patch-006-replicated-database-attach-with-shard-macro}

## Summary {#summary}

When the server starts and attaches a `ReplicatedMergeTree` table that lives in
a `Replicated` database, the patch lets `{shard}` and `{replica}` come from that
database, as `CREATE` already does, instead of requiring the server to define
them as macros. Aiven carries it because a server that hosts `Replicated`
databases has no single correct global `shard` macro, and without one an
ordinary restart fails to load such tables and the server does not start. With
the patch the restart succeeds; nothing else changes.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.3 | `if (is_replicated_database \|\| query.attach)` on `v25.3.14.14-lts-aiven` | carried |
| 25.8 | `22e03c9d9d6cf9929aec824b724e09ea5c58653f` on `v25.8.18.1-lts-aiven` | ported |
| 26.3 | `patch-port(006): Replicated database attach with shard macro` at `8363bc6a7f08d275c24b7b0fdf10ca82ee449786` | ported verbatim (one line) |
| 26.8 | `patch-port(006): resolve the shard macro when attaching a replicated table` | `still-needed-but-rewrite`, one commit; ported first on 26.8.2.7 and re-implemented on 26.8.34.13 |

The 26.3 commit is itself a cherry-pick of `d2f78fd`. `TableZnodeInfo.cpp` and
`DatabaseReplicatedHelpers.cpp` are byte-identical between
`v25.8.31.9-lts-aiven` and `v26.3.26.3-lts-aiven`, so those lines share one
behaviour. On 26.3 up to `v26.3.10.62-lts-aiven` the patch is absent, so those
builds have the startup failure.

## Background (ClickHouse) {#background}

A `ReplicatedMergeTree` table's ZooKeeper path and replica name are engine
arguments that may contain macros. They are resolved in two passes:

1. `{uuid}`, `{database}` and `{table}` are expanded first, and that result is
   written into the table's metadata file.
2. `{shard}` and `{replica}` are expanded afterwards and deliberately not
   stored, so that a metadata file can be copied between replicas.

So every `ATTACH`, including the one the server performs for each table at
startup, resolves `{shard}` again. It has two possible sources: a global
`<shard>` macro in the server configuration, or the shard name in the
`Replicated` database's engine arguments. For a table inside a `Replicated`
database the second is the intended source, but it is used only when the server
defines no `shard` macro: configured macros take precedence. The configuration
cannot simply supply the first, because a macro is per server while a shard name
is per database: one server can host several `Replicated` databases on
different shards.
Upstream's default `default_replica_path` is `/clickhouse/tables/{uuid}/{shard}`,
so a table created without engine arguments keeps `{shard}` in its stored path.

## Component tour {#component-tour}

`src/Storages/TableZnodeInfo.cpp` is the storage layer's resolver for a
replicated table's Keeper path, shared by `CREATE` and `ATTACH`. The entry point
is `TableZnodeInfo::resolve`, called from
`extractZooKeeperPathAndReplicaNameFromEngineArgs` in `registerStorageMergeTree.cpp`
for every `Replicated*MergeTree`. On the startup path it runs inside the table's
load job, so an exception here fails server startup, not one query. The patch
changes the block that fills `MacroExpansionInfo::shard` and `::replica`.

`src/Common/Macros.cpp` is the expander. `Macros::expand` looks up the
configured macros first. Only for a name the configuration does not define does
it use `MacroExpansionInfo::shard` or `::replica`, if set, and otherwise it throws
`NO_ELEMENTS_IN_CONFIG`. So leaving `info.shard` unset makes a configured
`shard` macro mandatory.

`src/Databases/DatabaseReplicatedHelpers.cpp` holds the two free functions the
unpatched block called, `getReplicatedDatabaseShardName` and
`getReplicatedDatabaseReplicaName`. Each downcasts with
`assert_cast<const DatabaseReplicated *>` and no other check. The patch stops
calling them here; Kafka and Paimon still call the replica one.

## Problem {#problem}

A `Replicated` database contains a `ReplicatedMergeTree` table whose stored path
holds `{shard}`, and the server defines no global `shard` macro. On startup the
loader attaches the table. `TableZnodeInfo::resolve` fills `info.shard` from the
database only when `is_replicated_database` holds, and that flag requires
`context->isDDLOrOnClusterInternal()`: true for DDL-queue and `ON CLUSTER` work,
false for the loader's local `ATTACH`. So `Macros::expand` looks for a
configured `shard` macro, finds none, and throws. The load job fails and the
server does not come up:

```text
Code: 139. DB::Exception: No macro 'shard' in config while processing
substitutions in '/clickhouse/tables/{uuid}/{shard}' at '27' or macro is not
supported here: Cannot attach table `testdb`.`t` from metadata file ...
(NO_ELEMENTS_IN_CONFIG)
```

The blast radius is a whole server, triggered by a plain restart.

## Decisions {#decisions}

### The cast is the guard {#decision-the-cast-is-the-guard}

- **Chosen:** enter the block on `is_on_cluster || query.attach`, and inside it
  take the names only when `dynamic_cast<const DatabaseReplicated *>` succeeds.
- **Rejected:** the 26.3 shape, `is_replicated_database || query.attach` with the
  helper calls. `query.attach` says nothing about the database engine, so on
  every startup a `ReplicatedMergeTree` in a plain `Atomic` database reaches
  `assert_cast<const DatabaseReplicated *>` with a `DatabaseAtomic`. That cast
  is unchecked (see the next Decision), and `DatabaseReplicated` extends
  `DatabaseAtomic`, so `getShardName` reads past the end of the real object.
  Also rejected: keeping the helpers behind a second check on
  `getEngineName() == "Replicated"`. It is sound today, but the defect being
  fixed is a guard drifting away from the cast it protects, and a separate
  condition can drift the same way.
- **Rung:** 2, upstream contribution. This is a plain bug fix with no Aiven
  policy in it, and upstream `master` still has the unpatched block (checked
  against a fetch from 2026-09-02). Rung 1 does not serve: no single global
  `shard` macro is right for a server with several `Replicated` databases. Until
  an upstream fix lands, the fork carries one hunk in an upstream file.
- **Supersedes:** the 26.3 one-line widening.

### The type check is absent on every build type {#decision-type-check-absent}

- **Chosen:** treat `getReplicatedDatabaseShardName` on a non-`Replicated`
  database as undefined behaviour on every build type, and rely on the
  `dynamic_cast` instead.
- **Rejected:** the 26.8.2.7 dossier's explanation that the check was missing
  only because that build passed `-DNDEBUG`. On this line's Debug build there is
  no `-DNDEBUG`, and the check is still gone. `assert_cast.h` tests
  `DEBUG_OR_SANITIZER_BUILD` but does not include `base/defines.h`, which defines
  it, and `DatabaseReplicatedHelpers.cpp` includes `assert_cast.h` first. Running
  that file's compile command with `-E` shows `assert_cast` reduced to
  `return static_cast<To>(from);`. Six files in `src/` include `assert_cast.h`
  first. Fixing the header is an upstream contribution in its own right, not
  part of this patch.
- **Rung:** n/a, this is a finding, not a mechanism.
- **Supersedes:** the 26.8.2.7 explanation.

### The harness's shard macro is switched off {#decision-harness-shard-macro}

- **Chosen:** the test's instances set `with_remote_database_disk=False`, and the
  fixture asserts that no node has a `shard` macro.
- **Rejected:** keeping the test as on 26.8.2.7. The Aiven integration lane runs
  the `db disk` job, where the harness adds `<shard>default</shard>` to every
  instance with a remote database disk. With that macro present, the unpatched
  build attaches the table through the fallback and every case passes, so the
  test proved nothing on this lane. The fixture assertion makes a future
  harness change fail loudly instead.
- **Rung:** n/a, test design.

## Approach {#approach}

The block that fills `info.shard` and `info.replica` now runs on
`is_on_cluster || query.attach`, and takes both names from the database only
when it is a `DatabaseReplicated`:

```cpp
if (is_on_cluster || query.attach)
{
    auto database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    if (const auto * replicated = dynamic_cast<const DatabaseReplicated *>(database.get()))
    {
        info.shard = replicated->getShardName();
        info.replica = replicated->getReplicaName();
    }
}
```

Taking the `nullptr` branch for a database that is not `Replicated` is correct,
not a fallback: such a database has no shard name, and the configured macros are
the right source for its tables.

Two details a reviewer might misread:

- The `DatabasePtr` is a named local on purpose. A temporary in the `if`
  condition is destroyed at the end of the condition, before the body runs, so
  `dynamic_cast<...>(getDatabase(name).get())` would leave `replicated`
  dangling.
- `dynamic_cast`, not `typeid_cast`: `typeid_cast` matches the exact type, so a
  future subclass of `DatabaseReplicated` would return `nullptr` and silently
  bring this failure back. This runs once per table attach, so the cost of the
  hierarchy walk does not matter.

`is_replicated_database` stays, because it also feeds `allow_uuid_macro` and
that meaning must not move. Deleting it would be a correct simplification, but
it widens the diff into upstream code for no behavioural change. The include of
`DatabaseReplicatedHelpers.h` is replaced by `DatabaseReplicated.h`; the helpers
stay for their other callers.

The `is_on_cluster || query.attach` precondition is probably wider than needed:
DDL against a `Replicated` database already runs with the internal flag set. It
is kept because the non-DDL paths (restore, materialized-view inner tables,
`ATTACH ... FROM`) cannot be cheaply enumerated, and a narrow change is worth
more here than a tidy one.

Non-goals:

- Making `getReplicatedDatabaseShardName` fail closed. That changes shared code
  for callers this patch does not own.
- Setting the DDL-internal flag on the startup loader's context. That flag also
  gates `allow_uuid_macro` and the DDL guards, so the blast radius is far wider.
- Expanding `{shard}` into the metadata file at `CREATE` time. That is the root
  cause, but fixing it there destroys the replica portability the two-pass
  design exists for and changes on-disk metadata.

## Concept {#concept}

A value deliberately left as a template has to stay resolvable wherever it is
read back. ClickHouse keeps `{shard}` symbolic so metadata stays portable, which
means resolution happens again on every load. Any input that only exists during
DDL then becomes a startup failure. When you see a stored template, ask which
contexts will expand it later and whether all of them can reach its inputs.

The narrower lesson: when a defect is "the guard and the guarded operation can
drift apart", make them one expression, as `if (const auto * p = dynamic_cast<T *>(x))`
does, rather than adding a better guard. And never trust a check that depends
on a macro without proving the macro is defined in that file: an `#ifdef` in a
header that does not include its own definition is decided by include order.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-but-rewrite`

- Retarget from 26.8.2.7 to 26.8.34.13: the only upstream change to the touched
  files between the two releases is a comment in `TableZnodeInfo.h` (from the
  `ATTACH AS REPLICATED` path validation backport). The block and the
  `is_replicated_database` definition are unchanged, so the 26.8.2.7 design
  applies as is. The patch was re-implemented by hand rather than cherry-picked.
- New caller: `DatabaseOrdinary::checkReplicaPathIsSafe` resolves the path of an
  `Ordinary` or `Atomic` table being converted with `ATTACH AS REPLICATED`. That
  database is not `Replicated`, so the `dynamic_cast` declines and behaviour is
  unchanged.
- Same gap, not ported: `StorageKafkaUtils.cpp` and the Paimon path in
  `ObjectStorage/Utils.cpp` fill `info.replica` only under `is_replicated_database`,
  so a startup `ATTACH` takes `{replica}` from the server config. Both skip
  `{shard}` on purpose. The result differs only if a database's replica name
  differs from the server's `replica` macro, which the managed configuration does
  not do. Recorded, not fixed.
- No superseding upstream fix: upstream `master` still has the unpatched block.

## Customer impact {#customer-impact}

- **Who:** a server whose `Replicated` database holds a `ReplicatedMergeTree`
  table with `{shard}` in its path, and whose configuration defines no global
  `shard` macro.
- **What they see:** the restart succeeds and the table is queryable. Before the
  patch the server error log showed
  `No macro 'shard' in config ... Cannot attach table ... (NO_ELEMENTS_IN_CONFIG)`
  and the server did not start.
- **How to restore the previous behaviour:** none needed; no setting, metadata
  or Keeper state changes.
- **Unchanged precedence:** a server that does define a `shard` macro still
  resolves `{shard}` from it, with or without the patch, even when the database's
  shard name differs. Adding or removing that macro on such a server therefore
  changes the path its tables resolve to on the next restart.
- **Versus 26.3:** a `ReplicatedMergeTree` in a database that is not `Replicated`
  no longer goes through the unchecked downcast on every startup. No symptom of
  that was ever observed: the reads past the object happened to be harmless in
  every case tested.
- **Versus 26.8.2.7:** same behaviour.

## Security review {#security-review}

`n/a — the patch changes which of two existing sources supplies {shard} and {replica} on attach; the values it now uses are the same ones the DDL path already uses for the same table, so no new input reaches the Keeper path and no access check is involved.`

## Tests {#tests}

- **Paths:** `tests/integration/test_aiven_replicated_database_attach_with_shard_macro/`
- **Proves:** a `ReplicatedMergeTree` table created without engine arguments in
  a `Replicated` database, on servers with a `replica` macro but no `shard`
  macro, attaches after a restart (case 1); and a `ReplicatedMergeTree` in a
  plain `Atomic` database still attaches after a restart (case 2).
- **Why integration:** the trigger needs a server configuration without a
  `shard` macro and a real restart through the startup loader. Neither is
  available from a stateless test on the shared server.
- **How it asserts:** `async_load_databases` is disabled, so a failed `ATTACH`
  fails the startup load job and `restart_clickhouse` raises instead of returning
  a queryable server. Case 2 runs on its own instance, so a failed restart in one
  case cannot fail the other. The fixture asserts that no node has a `shard`
  macro.
- **Run:** `./.buildkite/run_aiven_integration.sh`, which runs every
  `test_aiven*` module in the Aiven integration job.

### Evidence {#evidence}

Local Debug build, aarch64, 2026-11-03. Three binaries from the same tree:
"base" has no 006, "26.3 shape" is `is_replicated_database || query.attach` with
the helper calls, "this port" is the `dynamic_cast` form.

| Case | Base | 26.3 shape | This port |
|---|---|---|---|
| 1, `Replicated` database, `{shard}` in path | **FAIL** | PASS | PASS |
| 2, plain `Atomic` database | PASS | PASS | PASS |

Case 1 on base fails because `node1` never comes back from the restart: the
harness gives up after five start attempts. The cause was captured from
`node1`'s `clickhouse-server.err.log` when the same test first ran on 26.8.2.7:
`Code: 139 ... No macro 'shard' in config while processing substitutions in
'/clickhouse/tables/{uuid}/{shard}' ... Cannot attach table testdb.t`.

Case 2 does not tell the 26.3 shape from this port, on any build type: the
`assert_cast` it reaches is unchecked everywhere (see
[the type check is absent](#decision-type-check-absent)). It is a regression
guard on the skip path, not evidence against the 26.3 shape. An ASan build might
report the read past the `DatabaseAtomic` object; that is untested.

The first run of this test on 26.8.34.13 passed on base in every case, because
the harness had added a `shard` macro. That is why the test now disables it and
checks for it.

### No-regression argument {#no-regression-argument}

Write `D` for `isDDLOrOnClusterInternal()`, `A` for `query.attach`, `R` for "the
database is `Replicated`". The old block filled the names on `D ∧ R`, the new one
on `(D ∨ A) ∧ R`. The difference is exactly `¬D ∧ A ∧ R`, the startup attach
this patch fixes. That equivalence needs `getEngineName() == "Replicated"` and
`dynamic_cast<const DatabaseReplicated *>` to agree, and they do:
`DatabaseReplicated` is the only database returning that name, and nothing
derives from it.

One behaviour does change outside that set: `getDatabase` now runs whenever `A`
holds, and it throws `UNKNOWN_DATABASE` for an absent database. The loader
registers a database before attaching its tables, so this does not occur.

### Regression run {#regression-run}

Upstream `tests/integration/test_replicated_database/`, which covers
`Replicated` database DDL, attach, recovery and metadata repair, run on this
port: 44 passed, none failed.

## Rollback {#rollback}

- **Revert:** safe on a running fleet. Reverting brings back the startup failure
  for the affected configuration. Reverting to the 26.3 shape instead would bring
  back the unchecked downcast.
- **Persistent state:** none. No on-disk metadata, Keeper state or setting
  changes.
- **Disable without rebuild:** none, by design.

## Retirement {#retirement}

Retire when upstream resolves `{shard}` and `{replica}` from a `Replicated`
database on the attach path. This is a plain bug fix and should be offered
upstream; link the pull request here when it exists. Re-check upstream `master`
on the next uplift.

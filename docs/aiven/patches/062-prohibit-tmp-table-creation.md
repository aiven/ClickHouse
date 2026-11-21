---
description: 'Reserve table names starting with .tmp for the temporary tables of the engine'
sidebarTitle: '062: Prohibit .tmp table creation'
slug: '/aiven/patches/062-prohibit-tmp-table-creation'
title: 'Patch 062: Prohibit .tmp table creation'
doc_type: 'reference'
aiven_identity: '062'
aiven_absorbs: ['063', '064']
aiven_changelog_category: 'Improvement'
aiven_settings:
  - 'aiven_prohibit_tmp_table_creation'
---

# Patch 062 — Prohibit `.tmp` table creation {#patch-062-prohibit-tmp-table-creation}

## Summary {#summary}

The engine creates temporary tables under real names: a refreshable materialized view
builds its next result in `.tmp.inner_id.<uuid>`, and `CREATE OR REPLACE` and
`CREATE ... AS SELECT` build the new table under a temporary name before publishing it.
The backup tool skips tables whose name starts with `.tmp` in `Replicated` databases,
because those are temporaries. With the server setting `aiven_prohibit_tmp_table_creation`,
that prefix is reserved: users cannot give a table such a name, so every `.tmp*` table
really is a temporary and no user data is silently left out of a backup. The engine's own
`CREATE OR REPLACE` temporary is renamed into the reserved space. Tables that already exist
keep working. Nothing changes unless the server setting is enabled, which the managed
configuration does.

## Lineage {#lineage}

| LTS | How to find | Outcome |
|---|---|---|
| 25.8 | `e79384374f2`, 2026-01-29 (`062`); `063` 2026-02-02; `064` 2026-02-05 | **origin.** The name check, the `.tmp_replace_` prefix, and the exemption of the engine's own temporary |
| 26.3 | `patch-port(062,063,064)` at `8cf2003d28b` on `v26.3.60.2-lts-aiven` | one commit, gated behind the new default-off `aiven_prohibit_tmp_table_creation` |
| 26.8.2.7 | — | not ported |
| 26.8.34.13 | `patch-port(062):` | `still-needed-but-rewrite`: one commit absorbing `063` and `064`, keyed on upstream's fresh-definition predicate |

The three identities ship as one commit because each intermediate state is broken: the
check without `064` rejects every `CREATE OR REPLACE`.

## Background (ClickHouse) {#background}

- **`CREATE OR REPLACE` and `CREATE ... AS SELECT`.** `doCreateOrReplaceTable` creates the
  table under a name from `TemporaryReplaceTableName`, populates it, then publishes it with a
  `RENAME` or `EXCHANGE`. On 26.8 a plain `CREATE ... AS SELECT` in an `Atomic` or
  `Replicated` database takes this path too, so a failed populate leaves no table behind.
- **Refreshable materialized views.** A full refresh runs `CREATE OR REPLACE TABLE
  .tmp.inner_id.<uuid>` as an internal query, inserts into it, and exchanges it with the
  target table. In a `Replicated` database the exchange is a replicated `RENAME`.
- **Loading modes.** The same `doCreateTable` serves a user's `CREATE`, a full-definition
  `ATTACH`, startup load, a short `ATTACH TABLE t`, the replay of a `Replicated` database's
  DDL log (`SECONDARY_CREATE`), `RESTORE`, and recovery of a lost replica. Upstream 26.8
  tells new input apart with `isFreshTableDefinition` and
  `Context::isRecoveryFromStoredMetadata`.

## Component tour {#component-tour}

- **`checkTableNameIsNotReserved`** (new, `InterpreterCreateQuery.cpp`). Throws
  `BAD_ARGUMENTS` for a name starting with `.tmp` when the setting is on, the definition is
  fresh, and the context is not a recovery from stored metadata.
- **`InterpreterCreateQuery::doCreateTable`** (upstream). Calls the check after the existence
  checks, unless the query is internal or creates the temporary table of
  `doCreateOrReplaceTable`.
- **`InterpreterCreateQuery::doCreateOrReplaceTable`** (upstream). Calls the check on the
  final name unless the query is internal, and marks the interpreter that creates its
  temporary table with the new member `creates_temporary_replace_table`.
- **`InterpreterRenameQuery::executeToTables`** (upstream). On the initiator of a
  `Replicated` database, rejects a new name starting with `.tmp`: the destination name, and
  for an exchange the source name too. The new `setInternal` exempts engine renames.
- **`StorageMaterializedView::exchangeTargetTable`** (upstream). Marks the refresh's
  exchange internal.
- **`TemporaryReplaceTableName`** (upstream, `063`). Prefix `.tmp_replace_` instead of
  `_tmp_replace_`, in both `toString` and the parsing pattern.
- **Setting.** `aiven_prohibit_tmp_table_creation` in `ServerSettings.cpp`.

## Problem {#problem}

The backup tool cannot tell a user table named `.tmpdata` from an engine temporary, so it
skips both. A user table with that prefix exists, replicates and answers queries, but is
missing from every backup, and nothing warns about it. The engine's temporaries can also
collide with user tables of the same name.

## Decisions {#decisions}

### Only fresh definitions are checked {#decision-scope}

- **Chosen:** check `CREATE` (including `CREATE OR REPLACE`, `CREATE ... AS SELECT` and
  views) and a full-definition `ATTACH`; skip startup load, short `ATTACH`, DDL-log replay,
  `RESTORE` and recovery from stored metadata, through upstream's predicates.
- **Why:** the skipped paths re-create a table the server already holds. A table created
  before the setting was enabled must keep loading, replicating and being recoverable on a
  new replica.
- **`RESTORE` is skipped**, as the internal create of the restore always was: the backup
  tool does not back up `.tmp*` tables, so its restores contain none.

### The engine's temporary table has its own exemption {#decision-own-flag}

- **Chosen:** a dedicated member `creates_temporary_replace_table` on the interpreter that
  creates the temporary table.
- **Rejected:** `setInternal(true)` on that interpreter. `internal` also skips other checks
  that apply to user statements, such as the table-count limit, and the temporary table of a
  `CREATE ... AS SELECT` becomes the user's table.

### Renames are checked on the initiator of a `Replicated` database {#decision-rename}

- **Chosen:** check in the branch that enqueues the rename to the database's DDL log, before
  anything is enqueued. Engine renames set `setInternal(true)`.
- **Why:** the backup tool's exclusion applies to `Replicated` databases, and a rename that
  the initiator rejects never reaches the other replicas.

### Keep `.tmp_replace_` {#decision-prefix}

- **Chosen:** carry `063`.
- **Why:** each replica derives the temporary name with its own binary, so 26.3 and 26.8
  replicas must agree during a rolling upgrade; and a temporary left by a failed replace
  falls under the reserved prefix and is skipped by the backup tool.

## Approach {#approach}

The check sits after the existence checks, so `CREATE TABLE IF NOT EXISTS` on an existing
`.tmp*` table stays a no-op. It runs before the storage is created, and before the
populating `SELECT` of `CREATE ... AS SELECT`, so a rejected statement leaves nothing behind.

## Concept {#concept}

A name convention shared with an external tool is a contract. Reserve the name space where
names are introduced, and give the engine's own users of that name space an explicit
exemption rather than a broad "internal" bypass.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-but-rewrite`

- Plain `CREATE ... AS SELECT` now goes through `doCreateOrReplaceTable`, which publishes
  under the final name; the check therefore runs on that name as well as in `doCreateTable`.
- Upstream added `isFreshTableDefinition` and `isRecoveryFromStoredMetadata`. The check uses
  both, so recovery of a lost replica needs no change in `DatabaseReplicated`.
- The refresh's exchange in a `Replicated` database is a replicated `RENAME`; it is marked
  internal.
- Upstream still names the temporary `_tmp_replace_*`, and its unit test
  `gtest_tmp_replace_table_name.cpp` and two stateless tests pin that prefix.

## Downstream contract {#downstream-contract}

- **The backup tool** skips tables whose name starts with `.tmp` in `Replicated` databases.
- **The managed configuration** enables `aiven_prohibit_tmp_table_creation`.
- **The control plane** expects `BAD_ARGUMENTS` for a user `CREATE` of a `.tmp*` table and
  for a `RENAME` to such a name.

## Customer impact {#customer-impact}

- **Who:** every service whose configuration enables `aiven_prohibit_tmp_table_creation`,
  which is every managed service.
- **What they see**, from the integration test:

```sql
CREATE TABLE ".tmpdata" (a UInt64) ENGINE = MergeTree ORDER BY a;
-- Code: 36. DB::Exception: Table name '.tmpdata' is invalid: names starting with '.tmp'
-- are reserved for internal use. (BAD_ARGUMENTS)

RENAME TABLE rdb.events TO rdb.".tmpdata";   -- rdb is a Replicated database
-- Code: 36. DB::Exception: Table name '.tmpdata' is invalid: names starting with '.tmp'
-- are reserved for internal use. (BAD_ARGUMENTS)
```

- **Existing `.tmp*` tables** keep loading, accept inserts, replicate, and are created on a
  new replica. They can be dropped, or renamed to a name outside the reserved prefix, which
  brings them back into backups.
- **Unchanged:** `CREATE OR REPLACE`, `CREATE ... AS SELECT` and refreshable materialized
  views, in `Atomic` and `Replicated` databases; session `TEMPORARY` tables of any name.
- **Visible rename:** the temporary of `CREATE OR REPLACE` is `.tmp_replace_*` instead of
  `_tmp_replace_*`, whether the setting is on or off.
- **How to restore upstream behaviour:** disable the server setting; the prefix stays.
- **Upstream tests edited:** `gtest_tmp_replace_table_name.cpp`,
  `04613_create_or_replace_view_over_dictionary`,
  `04626_create_or_replace_drop_privilege_preflight` (the temporary's prefix).

## Known residual {#known-residual}

- A `RENAME` in a database that is not `Replicated` is not checked; the backup tool's
  exclusion only applies to `Replicated` databases.

## Security review {#security-review}

The check protects the completeness of backups, not access control: a user can only affect
their own tables. Every way to introduce a table name with a new definition passes through
it: `CREATE` in all its forms and a full-definition `ATTACH`; for `Replicated` databases,
`RENAME` and `EXCHANGE` too. The skipped paths re-create tables the server already holds,
which a user can only have introduced through a checked path or before the setting was
enabled. The exemptions are set only by engine code: `internal` on create and rename
interpreters, and `creates_temporary_replace_table` inside `doCreateOrReplaceTable`. A
session cannot change the server setting.

## Tests {#tests}

- **Paths:** `tests/integration/test_aiven_prohibit_tmp_table/`;
  `src/Common/tests/gtest_tmp_replace_table_name.cpp`.
- **Proves:** with the setting on, `CREATE`, `CREATE ... AS SELECT`, `CREATE OR REPLACE`,
  `CREATE VIEW` and a full-definition `ATTACH` of a `.tmp*` name are rejected and leave no
  table; so are `CREATE`, `RENAME` and `EXCHANGE` in either order in a `Replicated` database.
  `.tmp*` tables created before the setting was enabled load after restart, accept
  `CREATE TABLE IF NOT EXISTS` and inserts, can be renamed out of the prefix, and are created
  with their data on a new replica. `CREATE OR REPLACE`, `CREATE ... AS SELECT` and
  refreshable materialized views work in `Atomic` and `Replicated` databases and leave no
  temporaries. A server with the setting off accepts every rejected statement.
- **Run:** `python -m ci.praktika run "integration" --test test_aiven_prohibit_tmp_table`.
- **Evidence (2026-11-21):** with the patch 10 of 10 pass, three runs in a row. On the parent
  commit, with `skip_check_for_incorrect_settings` added so that the unknown setting does not
  stop the server, the 7 rejection tests fail ("expected to be failed but succeeded") and the
  3 tests of existing behaviour pass. Without the internal mark on the refresh's exchange, the
  refreshable materialized view in the `Replicated` database fails to refresh with the
  reserved-name error. The unit test passes 6 of 6. The upstream stateless tests of
  `CREATE OR REPLACE`, `CREATE ... AS SELECT` and refreshable materialized views that inspect
  the temporary pass, 12 of 12.

## Rollback {#rollback}

- **Downgrade:** not supported; services only move forward.
- **Revert in a later release:** no on-disk or Keeper format of its own. Reverting `063`
  while a `CREATE OR REPLACE` is in flight across versions would make replicas derive
  different temporary names.
- **Disable without rebuild:** set `aiven_prohibit_tmp_table_creation` to false and restart.

## Retirement {#retirement}

An upstream reservation of a name prefix for engine temporaries, or a marker on those
tables that the backup tool can read instead of the name. Re-check upstream on every uplift.
The release that drops the patch keeps `aiven_prohibit_tmp_table_creation` declared with
`SettingsTierType::OBSOLETE`; an unknown element stops the server from starting.

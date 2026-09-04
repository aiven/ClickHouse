---
description: 'Dossier for the Aiven patch that requires a create privilege before showing a create statement'
sidebarTitle: '011: Restrict SHOW CREATE'
slug: '/aiven/patches/011-restrict-show-create-access'
title: 'Patch 011: Restrict SHOW CREATE access'
doc_type: 'reference'
---

# Patch 011 — Restrict `SHOW CREATE` access {#patch-011-restrict-show-create-access}

## Lineage {#lineage}

| LTS | How to find | Outcome |
|---|---|---|
| 26.3 | `patch-port(011): Restrict SHOW CREATE DATABASE access` at `2e2ca06b0fa0f823cba9302a0262f077796f7cae` | ported |
| 26.8 | `patch-port(011): Restrict SHOW CREATE DATABASE access` | `still-needed-but-rewrite` |

The 26.3 commit is itself a double cherry-pick, from `654f61e` and `931e184`.
Co-authored by Kevin Michel.

## Background {#background}

ClickHouse RBAC grants are **implied** as well as explicit. Granting a user any
access to a database implies `SHOW DATABASES` on it; granting `SELECT` on a
table implies `SHOW COLUMNS` for the granted columns. A service user therefore
holds the `SHOW` privileges as a side effect of being able to work at all.

Upstream authorizes `SHOW CREATE TABLE` with `SHOW COLUMNS` and
`SHOW CREATE DATABASE` with `SHOW DATABASES`. Both are implied grants, so in
practice every service user can read the full create statement of every object
they can see.

## Component tour {#component-tour}

`src/Interpreters/InterpreterShowCreateQuery.cpp` is a **statement
interpreter**: one class per SQL statement kind, resolved through
`InterpreterFactory`, whose `executeImpl` turns a parsed AST into a
`QueryPipeline`. `SHOW CREATE` has no query plan and no analyzer stage — it
resolves a name, checks access, fetches metadata, formats one `String` cell,
and returns a `SourceFromSingleChunk`. If you are looking for where a
`SHOW`-family statement is authorized, this is the only gate; nothing behind it
re-checks.

`src/Storages/System/StorageSystemTables.cpp` is a **system table**: a virtual
storage whose rows are produced on read by `TablesBlockSource::generate`. It
answers the same question as `SHOW CREATE TABLE` — through the columns
`create_table_query`, `engine_full`, and `as_select`, all rendered from one
`getRenderedCreateTableQuery` call — but it is a `SELECT`, so it never enters
the interpreter above.

`src/Storages/System/StorageSystemDatabases.cpp` is the same shape
(`fillData`), and its `engine_full` column renders the database engine clause
from `getCreateDatabaseQuery`.

Three files, three doors, one piece of information.

## Problem {#problem}

Any user who can see an object can read its full definition: column schema and
codecs, engine parameters, replication paths in Keeper, `ORDER BY` and
`PARTITION BY` keys, TTLs, remote hostnames and ports, and object-storage
paths. On a managed fleet that is topology and tenancy disclosure between
tenants of the same cluster.

Note what this is **not**, on 26.8. The 26.3 commit message says the create
statement is shown "unredacted" and can "leak credentials". That is no longer
true by default: `formatWithPossiblyHidingSecrets` masks secrets unless the
`format_display_secrets_in_show_and_select` setting (default `false`), the
matching server setting, and the `displaySecretsInShowAndSelect` privilege all
agree. The residual exposure is everything that is not classified as a secret,
which is still the whole physical design of the table.

## Approach {#approach}

Require a **non-implied** privilege in addition to the implied `SHOW` one:
`CREATE TABLE` for a table's create statement, `CREATE DATABASE` for a
database's. The rule is "you may read the definition of an object you could
have created", which lands the capability on operator-shaped roles without
inventing an Aiven-specific privilege.

Apply it at each user-facing door, with the failure mode that door can survive:

- `checkAccess` in the interpreter, which **throws** `ACCESS_DENIED`. Correct
  for a single-object statement — the user asked a direct question.
- `isGranted` in the system tables, which **returns a bool**. The cell degrades
  to an empty string. Throwing here would abort the whole `SELECT`, so one
  inaccessible table would hide every accessible row.

In `system.tables` all three rendered columns must be gated together, composed
into the existing `can_expose_metadata` flag. In `system.databases`,
`engine_full` becomes an empty string. Neither system table gains an error
path, so no existing query starts failing.

Reversibility needs no new setting: an operator restores the old behavior for
any object by granting `CREATE TABLE` or `CREATE DATABASE`. That is per-object,
uses existing RBAC, and needs no rebuild.

Deliberate non-goals:

- `SHOW CREATE DICTIONARY` keeps its `SHOW DICTIONARIES`-only check. The 26.3
  patch edited exactly that `if`/`else` and left the dictionary branch alone.
- The temporary-tables path in `StorageSystemTables` is unchanged. Temporary
  tables are session-owned, so their creator already knows the definition.
- No gating inside the shared renderers (`RenderedCreateQuery`,
  `formatWithPossiblyHidingSecrets`). `SystemLog` and `getTableOverride` call
  the same machinery with server-internal contexts that hold no user grants; a
  check there would need an "internal, skip it" escape hatch, which is hidden
  policy in a formatting layer.

## Known residual {#known-residual}

`CREATE TABLE mine AS other.tab` checks only `SHOW COLUMNS` on the source
(`InterpreterCreateQuery.cpp`) and inherits the source's engine, `ORDER BY`,
`PARTITION BY`, TTL, and settings. A user who holds `SHOW COLUMNS` on another
tenant's table and can create tables of their own can copy the definition and
then read it back legitimately from their own table. `CLONE AS` is a sibling of
the same path.

This patch is therefore a **cost increase for casual disclosure, not a
confidentiality boundary**. Closing the residual would mean requiring a create
privilege on the source of an `AS` clause, which changes long-standing upstream
behavior for every operator; it is out of scope here and recorded so nobody
reads the patch as a guarantee it does not provide.

## Concept {#concept}

**Metadata in ClickHouse is normally reachable through at least two doors: a
`SHOW` interpreter and a `system.*` table.** They share no code path, so a
guard on one is not a guard on the information. You find the full set by
grepping the *metadata accessor* — `getCreateTableQuery`,
`getCreateDatabaseQuery`, `getRenderedCreateTableQuery` — not the statement
name, because a system table never mentions `SHOW CREATE`.

The second half of the concept is the mechanism split. `checkAccess` throws and
`isGranted` returns a bool, and the choice between them is decided by what the
caller can survive rather than by preference: a statement may refuse, a scan
must degrade. Putting `checkAccess` inside a system-table loop is the classic
bug in this area — it converts partial visibility into a failed query.

Finally, having found the doors, sort them into user-facing and
server-internal. Only the former may carry a grant check.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-but-rewrite`.

- The two interpreter anchors survive; the checks are still two lines. The
  surrounding code was rewritten for dictionary existence-oracle masking and
  `StorageAlias` target checks, and now documents that access is checked on the
  *requested* identifier before any lookup, which the added checks respect.
- The `StorageSystemTables` hunk is gone. `tryGetCreateTableQuery` was replaced
  by `getRenderedCreateTableQuery` behind a new `can_expose_metadata` flag —
  upstream independently invented the gate the patch needed, so the port
  composes into it instead of nulling an AST.
- The blast radius grew from two rendered columns to three (`as_select` is
  new). The 26.3 patch covered them incidentally by nulling one AST; the port
  must cover them deliberately.
- `system.databases.engine_full` is a third door the 26.3 patch never opened.
  Closed here as a recorded deviation from that patch's file scope.
- Secret redaction is now default-on upstream, which removes the credential
  half of the original motivation (see Problem).
- Upstream `04365_show_create_describe_no_column_leak` now denies `SHOW CREATE`
  under partial column grants. That narrows the gap but does not close it: a
  user with table-level `SHOW COLUMNS` and no create privilege still reads the
  full DDL. That test also has a closing section asserting that a *full* table
  `SELECT` grant makes `SHOW CREATE` work again, which this patch falsifies —
  `SELECT` does not carry `CREATE TABLE`. The port therefore grants
  `CREATE TABLE` alongside `SELECT` in that one section, so the test keeps
  checking the no-leak property it exists for instead of the added
  requirement. Its reference file is unchanged.
- The `system.tables` gate is a local flag composed from `can_expose_metadata`
  and scoped to the block that renders the three columns, not a change to
  `can_expose_metadata` itself. That flag also guards `data_paths`,
  `total_rows`, `metadata_version`, the partition key and the storage policy,
  none of which is a create statement; widening it would restrict far more than
  this patch is about.

## Customer impact {#customer-impact}

A user who only holds implied `SHOW` privileges now gets `ACCESS_DENIED` on
`SHOW CREATE TABLE` and `SHOW CREATE DATABASE`, with a message that names the
missing `CREATE TABLE` or `CREATE DATABASE` grant. The same user still sees
rows in `system.tables` and `system.databases`, but
`create_table_query`, `engine_full`, and `as_select` on the table, and
`engine_full` on the database, render as empty strings. `SELECT`,
`SHOW TABLES`, `SHOW DATABASES`, and `DESCRIBE` are unchanged.
`SHOW CREATE DICTIONARY` is unchanged.

Restore the previous behavior per object by granting `CREATE TABLE` or
`CREATE DATABASE`. No new setting and no rebuild.

Versus 26.3 `011`: the two statement denials and the
`system.tables.create_table_query` / `engine_full` blanks are the same. New
on 26.8: `system.tables.as_select` and `system.databases.engine_full` also
go empty. Control-plane queries that read `engine_full` under a
non-operator role will see blanks; the row itself remains.

`CREATE TABLE … AS` / `CLONE AS` still inherit the source definition behind
only `SHOW COLUMNS` on the source — see Known residual.

## Tests {#tests}

- Path: `tests/queries/0_stateless/aiven_011_restrict_show_create_access.sh`
  and its reference file. Stateless is sufficient — no cluster, restart, or
  external service is involved.
- The 26.3 test asserted two surfaces. This one must assert all four, in both
  directions: `SHOW CREATE TABLE`, `SHOW CREATE DATABASE`, the three
  `system.tables` columns, and `system.databases.engine_full`. A future
  refactor that reroutes one column then fails loudly.
- Assert an Aiven-relevant message substring for the denied statements (the
  grant name in the `ACCESS_DENIED` text), not the error code alone.
- Evidence: FAIL on parent / PASS with patch, recorded in the halt report.
- The fixtures exist to keep the assertions from passing vacuously: `engine_full`
  is legitimately empty for a database engine that renders no clause and for an
  ordinary view, and `as_select` is empty for a plain table. So the test creates
  its own database with an explicit engine, a `MergeTree` table, and a view, and
  asserts the privileged user sees all four columns **non-empty** before
  asserting the non-privileged one sees them empty.

## Rollback {#rollback}

A code revert restores upstream behavior and needs no data or metadata
migration; the patch creates no on-disk or Keeper state. Without a rebuild, an
operator restores access per object by granting `CREATE TABLE` or
`CREATE DATABASE`.

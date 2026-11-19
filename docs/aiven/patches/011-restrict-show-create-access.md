---
description: 'Dossier for the Aiven patch that requires a create privilege before showing a create statement'
sidebarTitle: '011: Restrict SHOW CREATE'
slug: '/aiven/patches/011-restrict-show-create-access'
title: 'Patch 011: Restrict SHOW CREATE access'
doc_type: 'reference'
aiven_identity: '011'
aiven_absorbs: []
aiven_changelog_category: 'Improvement'
aiven_settings: []
---

# Patch 011 — Restrict `SHOW CREATE` access {#patch-011-restrict-show-create-access}

## Summary {#summary}

The patch makes the full create statement of a table, view or database require
the non-implied `CREATE TABLE` or `CREATE DATABASE` privilege, both through
`SHOW CREATE` and through the `system.tables` and `system.databases` columns
that render it. Aiven carries it because the platform creates objects inside
customer services, such as integration tables, whose definitions carry
infrastructure detail meant for operators only, while the customer's users
hold every implied `SHOW` privilege on them. A user without the create
privilege gets `ACCESS_DENIED` from the statement and empty cells in the
system tables; queries, `DESCRIBE`, `SHOW TABLES` and the key columns of
`system.tables` are unchanged.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| before 24.3 | none in this repository: applied as build-time patches from 2022-07-04 | carried |
| 24.3, 24.8, 25.3 | `Restrict "SHOW CREATE database"`, first committed 2024-12-03 | ported |
| 25.8 | carried forward | ported |
| 26.3 | `patch-port(011): Restrict SHOW CREATE DATABASE access` at `2e2ca06b0fa0f823cba9302a0262f077796f7cae` (26.3.26.3) and `a6101cb9aa90` (26.3.60.2) | ported |
| 26.8 | `patch-port(011): Restrict SHOW CREATE DATABASE access`, first at `2685e00cae06` on 26.8.2.7, re-implemented on 26.8.34.13 | `still-needed-but-rewrite`, one commit |

The 2022 reason was that the create statement "shows unwanted information and
should only be accessible to internal superusers, not customer admins". The
2024 commit message added that it "can leak credentials". Co-authored by Kevin
Michel, who wrote the 2022 version.

## Background (ClickHouse) {#background}

ClickHouse grants are **implied** as well as explicit. Any grant on a table
implies `SHOW TABLES` and `SHOW COLUMNS` on it; any grant inside a database
implies `SHOW DATABASES` on it. A user who can query a table therefore holds
the `SHOW` privileges as a side effect of being able to work at all.

Upstream authorizes `SHOW CREATE TABLE` with `SHOW COLUMNS` and
`SHOW CREATE DATABASE` with `SHOW DATABASES`, and renders
`system.tables.create_table_query` for any table the user can list. Secret
arguments (passwords, secret keys, user info in URLs) are masked by
`formatWithPossiblyHidingSecrets` unless the user also holds
`displaySecretsInShowAndSelect`; everything else in the definition is shown.

## Component tour {#component-tour}

`src/Interpreters/InterpreterShowCreateQuery.cpp` is a **statement
interpreter**: `executeImpl` resolves the name, checks access, fetches the
create AST and returns it as a single `String` cell. It serves
`SHOW CREATE TABLE`, `SHOW TABLE`, `SHOW CREATE VIEW`, `SHOW CREATE DICTIONARY`
and `SHOW CREATE DATABASE`. Access is checked on the *requested* identifier
before any lookup, so an existing hidden object and a missing name produce the
same error. An `Alias` table additionally needs grants on its target, because
its create statement renders the target's definition.

`src/Storages/System/StorageSystemTables.cpp` is a **system table**. Its rows
are produced on read by `TablesBlockSource::generate`; the columns
`create_table_query`, `engine_full` and `as_select` come from one
`getRenderedCreateTableQuery` call behind the upstream `can_expose_metadata`
flag. It is a `SELECT`, so it never enters the interpreter above.
`information_schema.views` is a view over it.

`src/Storages/System/StorageSystemDatabases.cpp` has the same shape in
`fillData`; its `engine_full` column renders the database engine clause.

Three files, three ways to the same piece of information.

## Problem {#problem}

Any user who can see an object can read its whole definition: engine
parameters, remote hostnames and ports, topics, buckets, usernames, access key
IDs, Keeper and object-storage paths, and the `SELECT` of a view. On a managed
service that includes objects the platform created for the customer, such as
the tables behind a Kafka or PostgreSQL integration. The customer's users can
query those tables, so they hold the implied `SHOW` privileges, but the
definitions describe the platform's side of the integration.

Secrets themselves are not the problem on this line. A run on the unpatched
26.8.34.13 binary, with the managed server configuration and a user without
`displaySecretsInShowAndSelect`, showed the Kafka SASL password, S3 secret,
MySQL and PostgreSQL passwords and URL user info masked in every statement and
system-table column. `Replicated` databases on 26.8 take cluster credentials
only through a named collection, so they never appear in the DDL. Dictionary
definitions are the exception; see
[dictionaries are out of scope](#decision-dictionaries-out-of-scope).

## Decisions {#decisions}

### The patch stays, as an explicit security boundary {#decision-patch-stays}

- **Chosen:** carry 011 for the 2022 reason, operator-only definitions, and
  keep it as one explicit patch whose Ways in table below is reviewed on every
  uplift.
- **Rejected:** dropping it because upstream now masks secrets. That retires
  only the 2024 credential reason; nothing upstream hides the non-secret
  infrastructure detail, and the control plane still depends on a customer's
  user getting `ACCESS_DENIED` or an empty `create_table_query` for a
  platform-created integration table, and `ACCESS_DENIED` for
  `SHOW CREATE DATABASE default`.
- **Rung:** 5, an edit to upstream hot paths. Rung 1 does not serve: no
  setting or grant gives `SHOW CREATE` a non-implied requirement. Rung 2 does
  not either: the policy changes upstream semantics for every user. Rung 4
  would need one funnel, and the three surfaces share none.
- **Supersedes:** the credential rationale of the 2024 commit.

### The gate is the create privilege the user could have used {#decision-create-privilege-gate}

- **Chosen:** require `CREATE TABLE` for a table's or view's definition and
  `CREATE DATABASE` for a database's: you may read the definition of an object
  you could have created.
- **Why this privilege:** on a managed service it draws exactly the line the
  platform needs, with grants that already exist:
  - `avnadmin` holds `CREATE TABLE` in every customer database, through
    `aiven_admin_role` for the databases the platform creates (such as
    `default`) and directly for the databases it creates itself, as part of
    `DEFAULT REPLICATED DATABASE PRIVILEGES`. So it reads the definitions of
    its own tables and views, with secrets still masked.
  - On the databases behind service integrations, `avnadmin` is granted only
    `SELECT` and `INSERT`, so those table definitions stay hidden.
  - Nobody on the customer side holds `CREATE DATABASE`. `avnadmin` creates
    databases through the server's indirect-creation path
    (`aiven_user_with_indirect_database_creation`), which builds the
    `Replicated` database on its behalf and grants the curated set in place of
    `CREATE DATABASE`. So no customer user reads a database's engine clause,
    which carries the Keeper path, not even for a database they created.
  - The internal admin user holds every privilege and keeps full access for
    backup and restore.
- **Rejected:** an Aiven-specific privilege or a server setting. Either adds a
  fork-local surface to maintain, and a setting would be one more place for the
  protection to be switched off. Also rejected: a stricter gate such as
  `CREATE DATABASE` or `displaySecretsInShowAndSelect` for table definitions.
  `avnadmin` holds neither, so it could not read the definitions of its own
  tables, which the control plane expects it to do.
- **Rung:** as above.

### Hide the statement, show the fragments {#decision-minimal-scope}

- **Chosen:** hide exactly the rendered create statement: `create_table_query`,
  `engine_full` and `as_select` in `system.tables` (and so
  `information_schema.views.view_definition`), and `engine_full` in
  `system.databases`. Every other column stays as upstream: `sorting_key`,
  `primary_key`, `partition_key`, `sampling_key`, `unique_key`,
  `skipping_indices_types`, `data_paths`, `metadata_path`, `storage_policy`,
  `definer`, `comment`, the target columns, `system.columns` and
  `system.data_skipping_indices`.
- **Rejected:** hiding every fragment that could be reassembled into a
  definition. The fragments describe the logical design a user needs to write
  efficient queries, carry no credentials, and are read by client tools; hiding
  them breaks those tools for no protection the 2022 requirement asked for.
- **Rung:** n/a, scope.

### A statement throws, a scan blanks {#decision-statement-throws-scan-blanks}

- **Chosen:** `checkAccess` in the interpreter, which throws `ACCESS_DENIED`
  before the name is looked up; `isGranted` in the system tables, which leaves
  the cell empty.
- **Rejected:** one mechanism for both. Throwing from a scan aborts the whole
  `SELECT`, so one hidden table would hide every row the user may see. Blanking
  in the statement would answer a direct question with a misleading empty
  definition.
- **Rung:** n/a, mechanism.

### An alias also needs the privilege on its target {#decision-alias-target}

- **Chosen:** for an `Alias` table, require `CREATE TABLE` on the target as
  well as on the alias, in both the statement and `system.tables`.
- **Rejected:** checking the alias only, as the 26.3 port did. The alias's
  create statement renders the target's definition, so a user could create an
  alias to an object they may not read and read it through the alias.
- **Rung:** n/a, scope.
- **Supersedes:** the 26.3 behaviour.

### `system.databases.engine_full` is gated {#decision-databases-engine-full}

- **Chosen:** gate `system.databases.engine_full` with `CREATE DATABASE`.
- **Rejected:** keeping the 26.3 file scope, which never touched
  `system.databases`. The column is the same engine clause
  `SHOW CREATE DATABASE` prints, so leaving it open would make the statement
  denial meaningless.
- **Rung:** n/a, scope.

### A local flag, not a wider `can_expose_metadata` {#decision-local-flag}

- **Chosen:** compose a new `can_expose_create_query` from
  `can_expose_metadata` and the create-privilege checks, used only for the
  three rendered columns.
- **Rejected:** adding the check to `can_expose_metadata`. That flag also
  guards `data_paths`, `total_rows`, `metadata_version`, the partition key and
  the storage policy, none of which is a create statement.
- **Rung:** n/a, mechanism.

### Dictionaries are out of scope {#decision-dictionaries-out-of-scope}

- **Chosen:** leave `SHOW CREATE DICTIONARY` on its upstream
  `SHOW DICTIONARIES` check. The dictionary's row in `system.tables` is blanked
  by this patch like any other row, because it is gated on `CREATE TABLE`.
- **Rejected:** gating dictionaries here. Some dictionary source definitions
  are not masked by upstream's secret masking on this line, so the right fix is
  masking, which protects every reader including privileged ones. That is a
  separate patch, tracked in the tracker.
- **Rung:** n/a, scope.

## Approach {#approach}

In `InterpreterShowCreateQuery::executeImpl`, the non-dictionary branch checks
`CREATE TABLE` on the requested name right after `SHOW COLUMNS`, the alias
check adds `CREATE TABLE` on the target, and the database branch checks
`CREATE DATABASE` after `SHOW DATABASES`. All three run before the object is
looked up, so the denial reveals nothing about whether the object exists.

In `TablesBlockSource::generate`, `can_expose_create_query` is
`can_expose_metadata`, plus `CREATE TABLE` on the table, plus `CREATE TABLE` on
the target for an alias. When it is false, the block renders the three
columns from an empty AST, which yields empty strings. In
`StorageSystemDatabases::fillData`, `engine_full` is rendered only when the
user holds `CREATE DATABASE` on the database.

Neither system table gains an error path, so no existing `SELECT` starts
failing. An operator restores the old behaviour per object by granting the
create privilege.

Non-goals:

- The temporary-tables path in `StorageSystemTables` is unchanged. Temporary
  tables are session-owned, so their creator already knows the definition.
- No check in the shared renderers (`getRenderedCreateTableQuery`,
  `formatWithPossiblyHidingSecrets`). `SystemLog` and other server-internal
  callers use them with contexts that hold no user grants.
- `CREATE TABLE … AS` and `CLONE AS` are unchanged; see
  [Known residual](#known-residual).

## Known residual {#known-residual}

`CREATE TABLE mine AS other.tab` checks only `SHOW COLUMNS` on the source and
inherits the source's engine, keys, TTL and settings. A user who can see
another table and can create tables of their own can copy its definition and
read it back from their own table. `CLONE AS` takes the same path. The copy
also needs the `TABLE ENGINE` grant for the source's engine, so it is open only
for engines the user may create anyway: `avnadmin` holds the source grants for
`URL`, `S3`, `AZURE`, `PostgreSQL` and `MySQL` but not `Kafka`, so a Kafka
integration table cannot be copied.

This patch is therefore a **cost increase for casual disclosure, not a
confidentiality boundary** against a user who can create tables. Closing it
would mean requiring a create privilege on the source of an `AS` clause, which
changes long-standing upstream behaviour for every operator. Out of scope, and
recorded so nobody reads the patch as a guarantee it does not provide.

A user who holds `CREATE DICTIONARY` but not `CREATE TABLE` sees an empty
`create_table_query` for their own dictionary in `system.tables`.
`SHOW CREATE DICTIONARY` still works for them. Accepted.

## Concept {#concept}

**Metadata in ClickHouse is reachable through at least two ways: a `SHOW`
interpreter and a `system.*` table, plus any view over that table.** They share
no code path, so a guard on one is not a guard on the information. Find the
full set by searching for the metadata accessor (`getCreateTableQuery`,
`getCreateDatabaseQuery`, `getRenderedCreateTableQuery`), not the statement
name, because a system table never mentions `SHOW CREATE`.

The mechanism follows from what the caller can survive: `checkAccess` throws
and `isGranted` returns a bool. A statement may refuse; a scan must degrade.
And an implied privilege can never be a boundary, because every grant carries
it.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-but-rewrite`

- The interpreter now checks access on the requested identifier before any
  lookup, and checks `Alias` targets. The added checks sit in that order, and
  the alias rule is extended rather than bypassed.
- The 26.3 `system.tables` hunk is gone: upstream replaced
  `tryGetCreateTableQuery` with `getRenderedCreateTableQuery` behind
  `can_expose_metadata`. The port composes a local flag with it.
- `as_select` is a rendered column on this line; it is gated with the other
  two.
- `system.databases.engine_full` was never gated on 26.3 and is now.
- New `system.tables` columns since 26.3 (`unique_key`,
  `skipping_indices_types`, `target_database`, `target_table`) are fragments
  and stay visible.
- Secret masking now covers the table and database engine credentials that
  were probed on this line, which retires the 2024 rationale (see Problem).
- Retarget from 26.8.2.7 to 26.8.34.13: re-implemented from the decisions
  above rather than cherry-picked; the alias target rule is new.

## Customer impact {#customer-impact}

- **Who:** any user without `CREATE TABLE` on a table or view, or without
  `CREATE DATABASE` on a database. That includes users who can `SELECT` from it.
- **What they see:** `ACCESS_DENIED` from `SHOW CREATE TABLE`, `SHOW TABLE`,
  `SHOW CREATE VIEW` and `SHOW CREATE DATABASE`, with a message naming the
  missing grant. Empty `create_table_query`, `engine_full` and `as_select` in
  `system.tables`, empty `view_definition` in `information_schema.views`, and
  empty `engine_full` in `system.databases`. The rows themselves remain, and
  `SELECT`, `DESCRIBE`, `SHOW TABLES`, `SHOW DATABASES` and the key columns are
  unchanged. `SHOW CREATE DICTIONARY` is unchanged.
- **How to restore:** grant `CREATE TABLE` or `CREATE DATABASE` on the object,
  and on an alias's target too. No setting, no rebuild.
- **Versus 26.3:** the statement denials and the
  `create_table_query`/`engine_full` blanks are the same. New:
  `as_select`, `information_schema.views.view_definition` and
  `system.databases.engine_full` go empty, and an `Alias` also needs
  `CREATE TABLE` on its target.
- **Control-plane expectations this keeps:** `avnadmin` reads the masked
  definition of a table in `default`; it gets `ACCESS_DENIED` naming
  `CREATE TABLE` (or an empty `create_table_query`) for a table behind a
  Kafka integration, while the internal admin user still reads it; and
  `SHOW CREATE DATABASE default` is refused for `avnadmin`.
- **Upstream tests edited:** `04365_show_create_describe_no_column_leak`,
  `03636_storage_alias_metadata_access_rights`,
  `04613_remote_database_engine_access` and
  `05218_explain_hide_secrets_definer_view_join` each grant `CREATE TABLE`
  where they assume a `SELECT` or `SHOW` grant is enough to read a definition.
  Their reference files are unchanged, so each still checks what it is about.
- **Example**, as a user with `SELECT ON shop.*` and no create privilege:

```sql
SHOW CREATE TABLE shop.orders;
-- Before: CREATE TABLE shop.orders (`id` UInt64, `amount` Decimal(10, 2)) ENGINE = MergeTree ORDER BY id ...
-- After:  Code: 497. DB::Exception: analyst: Not enough privileges. To execute this query,
--         it's necessary to have the grant CREATE TABLE ON shop.orders. (ACCESS_DENIED)

SELECT name, create_table_query, engine_full, sorting_key FROM system.tables WHERE database = 'shop';
-- After:  orders    ''    ''    id
```

## Security review {#security-review}

- **Threat model:** a customer SQL user, including the service's main user,
  reads the definition of an object they did not create: a platform-created
  integration table or another team's table in the same service. They gain
  infrastructure detail, not data and not credentials (those are masked, see
  Problem). This patch closes the direct reads. If it is bypassed, the next
  layer is secret masking, so the worst case is the upstream exposure of
  non-secret detail, the state before this patch.
- **Identity:** every check runs as the authenticated session user, through
  the query context's access. No identity comes from the query text. The system
  tables are evaluated with the reader's context, so a view over them, such as
  `information_schema.views`, inherits the reader's grants.
- **Ways in:**

| Path | Kind | Covered by |
|---|---|---|
| `SHOW CREATE TABLE`, `SHOW TABLE`, `SHOW CREATE VIEW` | `SHOW` interpreter | `CREATE TABLE` on the requested name, before lookup |
| `SHOW CREATE DATABASE` | `SHOW` interpreter | `CREATE DATABASE`, before lookup |
| `SHOW CREATE` on an `Alias` | `SHOW` interpreter | also `CREATE TABLE` on the target |
| `system.tables` `create_table_query`, `engine_full`, `as_select` | `system.*` table | `can_expose_create_query`, including the alias target |
| `information_schema.views` | view over `system.tables` | inherits the `system.tables` gate |
| `system.databases.engine_full` | `system.*` table | `CREATE DATABASE` |
| A `Remote` database proxying another server | `SHOW` interpreter | checked on the proxy name; the remote server applies its own checks to the connection's user |
| Implied `SHOW` grants | implicit grants | not sufficient by design; `CREATE` privileges are never implied by `SELECT` or `SHOW` |
| Key columns, `system.columns`, `system.data_skipping_indices` | `system.*` table | visible by decision, see [hide the statement, show the fragments](#decision-minimal-scope) |
| `CREATE TABLE … AS`, `CLONE AS` | DDL | not covered, see [Known residual](#known-residual) |
| `SHOW CREATE DICTIONARY`, `system.dictionaries` | `SHOW` interpreter, `system.*` table | not covered, see [dictionaries are out of scope](#decision-dictionaries-out-of-scope) |
| Query text of a `CREATE` in `system.query_log` | `system.*` table | not covered; reading other users' queries needs a grant on `system.query_log` |
| `BACKUP` | backup | not covered; needs the `BACKUP` privilege, which operators hold |
| `ON CLUSTER`, DDL replay, replication | — | `SHOW CREATE` and `SELECT` are not replicated or replayed |
| `clickhouse-local` | — | n/a; the local process owns the metadata files |

- **Default:** protected with no configuration. The rule is in code and uses
  existing privileges.
- **Lifecycle:** no state. Grants are ordinary access entities, so the rule
  holds across restart, `SYSTEM RELOAD CONFIG` and replicated access storage.
- **Preservation:** no flag exists that a statement could clear. Only a grant
  changes the outcome, and grants already need access management.
- **Release build:** no `assert_cast` or `chassert`; the checks are plain
  `checkAccess` and `isGranted` calls on every build type.
- **Over-blocking:** see [Regression run](#regression-run).
- **Untrusted input:** object names from the query reach only access checks
  and catalog lookups; nothing is joined into a path or generated SQL.
- **Accepted risks:** the `AS`/`CLONE AS` residual and the dictionary-owner
  case in [Known residual](#known-residual); dictionary definitions until the
  masking patch lands, tracked in the tracker.

## Tests {#tests}

- **Paths:** `tests/queries/0_stateless/aiven_011_restrict_show_create_access.sh`
  and its reference file. Stateless is enough: no cluster, restart or external
  service is involved.
- **Proves:** without the patch, a user holding only the implied `SHOW`
  privileges reads every create statement and rendered column, and an `Alias`
  exposes its target to a user without `CREATE TABLE` on it.
- **How it asserts:** a privileged user (with `CREATE TABLE` and
  `CREATE DATABASE`) first sees every gated column **non-empty**, so the
  emptiness checks cannot pass vacuously; a view and a database with an
  explicit engine exist for that reason. Then a non-privileged user is denied
  each statement, with the missing grant named in the message, and sees each
  column empty. The test also asserts that an alias needs the target grant,
  that a URL password stays masked for the privileged user, and that
  `sorting_key` stays visible.
- **Run:** `./tests/clickhouse-test aiven_011_restrict_show_create_access`

### Evidence {#evidence}

Local Debug build, aarch64, 2026-11-04. On the parent (26.8.34.13 without 011),
the test fails with exactly the 011 lines: the alias statement and its
`create_table_query`, the three statement denials, and the five empty-column
checks of the non-privileged user. Every other line already matches. With the
patch, it passes.

### Regression run {#regression-run}

90 stateless tests: every upstream test that pairs `SHOW CREATE`,
`create_table_query`, `engine_full`, `as_select` or `view_definition` with a
`GRANT`, plus the access-control and system-table tests. They ran on the same
minimal server configuration, once without and once with the patch. Apart from
`aiven_011_restrict_show_create_access`, the failures with the patch are the
failures without it. The exceptions are two tests that failed on one run each
because a randomized setting asked for a `parallel_replicas` cluster this
configuration does not define. The shared failures need the CI server
configuration (test clusters, remote servers). The four edited upstream tests
pass both with and without the patch.

A second run covered all 82 upstream tests about secrets, masking and
credentials, because 011 sits next to secret masking in the same output. 45
pass with the patch. The other 37 fail the same way without it, except one that
failed on the randomized `parallel_replicas` setting. The shared failures need
external services (S3, Kafka, NATS, MySQL, PostgreSQL), backups or the CI
server configuration. No secret test changes result because of this patch.

## Rollback {#rollback}

- **Revert:** safe on a running fleet; a revert restores upstream visibility.
- **Persistent state:** none. No on-disk, Keeper or access-entity state.
- **Disable without rebuild:** grant `CREATE TABLE` or `CREATE DATABASE` on
  the affected objects.

## Retirement {#retirement}

Retire if upstream gains a way to make definitions require a non-implied
privilege, or if the platform stops creating objects whose definitions are
meant for operators only. Re-check on every uplift for new ways in: new
`system.tables` columns that render the definition, new `SHOW` statements, and
new views over the system tables.

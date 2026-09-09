---
description: 'What tenants, operators and control-plane queries observe from the Aiven ClickHouse 26.8 patch set'
sidebarTitle: 'Customer impact'
slug: '/aiven/uplifts/26.8/customer-impact'
title: 'Aiven 26.8 uplift customer impact'
doc_type: 'reference'
---

# Aiven 26.8 uplift customer impact {#aiven-26-8-uplift-customer-impact}

Everything the 26.8 patch set changes for a tenant, an operator, or a
control-plane query, in one place. Support, documentation and the control plane
read this; nobody reads 70 dossiers.

Each entry is the `Customer impact` section of a patch's dossier under
[`../../patches/`](../../patches/), condensed. The dossier stays authoritative
for detail and reasoning; this file exists so the fleet-visible delta can be
reviewed as a whole.

## How this file stays true {#how-this-stays-true}

**Append at port time, not at the end.** A patch is not done until its impact
appears here — see the
[definition of done](execution-plan.md#definition-of-done). An
end-of-uplift aggregation task would never be written, and by then nobody would
remember which observable belonged to which patch.

Only landed patches appear. Absence from this file means either "not ported yet"
or "nothing user-visible"; the [inventory](inventory.md) says which.

## Landed changes {#landed-changes}

### `040` — default HTTP endpoints hardened {#040}

Seven built-in HTTP endpoints are no longer registered by default and now
return `404`: `/replicas_status`, `/binary`, `/merges`, `/jemalloc`,
`/clickstack`, `/schema`, and `/processors-profile`. `/replicas_status` is also
no longer advertised in the not-found response, and links to the disabled UIs
are gone from the landing page.

Unchanged and still served: `/`, `/ping`, the HTTP query API, `/play`,
`/dashboard`, `/docs`, and the `/js/` assets those UIs need. `/webterminal`
continues to return `403 Forbidden` under Aiven's `enable_webterminal=false`.

Restore any denied endpoint with an explicit `http_handlers` rule — no rebuild.
Handler implementations, embedded resources, SQL functionality and jemalloc
profiling are untouched; only automatic registration changed. Policy and
per-endpoint reasoning: [`http-endpoint-inventory.md`](http-endpoint-inventory.md).

> The `040` dossier predates the dossier template's `Customer impact` section,
> so this entry was derived from the endpoint inventory. Backfill the section
> when that dossier is next touched.

### `011` — `SHOW CREATE` requires a `CREATE` grant {#011}

A user holding only implied `SHOW` privileges now gets `ACCESS_DENIED` on
`SHOW CREATE TABLE` and `SHOW CREATE DATABASE`, naming the missing
`CREATE TABLE` or `CREATE DATABASE` grant. That user still sees rows in
`system.tables` and `system.databases`, but `create_table_query`, `engine_full`
and `as_select` on the table, and `engine_full` on the database, render as
empty strings rather than raising.

`SELECT`, `SHOW TABLES`, `SHOW DATABASES`, `DESCRIBE` and
`SHOW CREATE DICTIONARY` are unchanged. Restore per object by granting
`CREATE TABLE` or `CREATE DATABASE` — no new setting, no rebuild.

**Control-plane note:** queries reading `system.tables.as_select` or
`system.databases.engine_full` under a non-operator role now see blanks where
26.3 returned values. The rows themselves remain. These two columns are new
exposure relative to the 26.3 version of this patch.

### `073` — `compatibility` survives a dangling history entry {#073}

No user-visible change on a consistent build, which is the state 26.8 ships in.

If a future port ever introduces a `SettingsChangesHistory` entry naming a
setting the build lacks, the behavior is: `MergeTree` access keeps working
instead of raising a repeated exception in every session, and the server logs
once per process, under the `Settings` or `MergeTreeSettings` logger, that the
setting is recorded in history but absent from the build. The affected setting
is not restored to its historical value, so a tenant relying on `compatibility`
for that one setting silently gets current semantics — the cost of staying
available. Operators get a line to grep instead of an outage.

### `006` — server starts without a global `shard` macro {#006}

A server whose `Replicated` database holds a `ReplicatedMergeTree` table with
`{shard}` in its path, and whose configuration defines no global `<shard>`
macro, now starts instead of failing its table-load job with
`No macro 'shard' ... (NO_ELEMENTS_IN_CONFIG)`. Nothing to do on upgrade: a
restart that previously failed simply succeeds.

Also fixed, with no known user-visible symptom: a `ReplicatedMergeTree` in a
database that is *not* `Replicated` no longer enters the shard-resolution block,
where 26.3 took an unchecked cast on every attach. A latent-defect fix rather
than something a tenant would notice, closed because the alternative is
shipping undefined behavior on the restart path.

## Rollup for release notes {#rollup}

The tenant-visible surface so far, for anyone drafting the 26.8 release note:

| Change | Who notices | Restore without a rebuild |
|---|---|---|
| Seven HTTP endpoints return `404` | anyone probing those paths | `http_handlers` rule per endpoint |
| `SHOW CREATE` needs a `CREATE` grant | users with only `SHOW` privileges | grant `CREATE TABLE` / `CREATE DATABASE` |
| `as_select` and `engine_full` blank in system tables | control-plane queries under non-operator roles | grant `CREATE TABLE` / `CREATE DATABASE` |
| Startup no longer needs a global `shard` macro | operators of `Replicated` databases | n/a — strictly fewer failures |

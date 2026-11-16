---
description: 'Dossier for the Aiven patch that expands the {shard} macro in a refreshable materialized view coordination path inside a Replicated database'
sidebarTitle: '049: RMV shard macro'
slug: '/aiven/patches/049-refreshable-mv-shard-macro-expansion'
title: 'Patch 049: Refreshable MV shard macro expansion'
doc_type: 'reference'
aiven_identity: '049'
aiven_absorbs: []
aiven_changelog_category: 'Bug Fix'
aiven_settings: []
---

# Patch 049 — refreshable MV shard macro expansion {#patch-049-refreshable-mv-shard-macro-expansion}

## Summary {#summary}

The patch gives the `{shard}` macro the `Replicated` database's own shard name when a
refreshable materialized view builds its Keeper coordination path. Aiven servers have no
global `<shard>` macro; each `Replicated` database carries its shard name. Without the
patch, `CREATE MATERIALIZED VIEW ... REFRESH` in such a database fails with
`NO_ELEMENTS_IN_CONFIG`.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.8 | `cc745f53f9` | original |
| 26.3 | `patch-port(049)` `3f14018e8e6` | cherry-pick, integration test |
| 26.8 | `patch-port(049)` | `still-needed-and-applies`; `tryGetDatabase` instead of `getDatabase` |

## Background (ClickHouse) {#background}

A refreshable MV in a `Replicated` database is *coordinated*: replicas agree through a
Keeper path built from the server setting `default_replica_path` (for example
`/clickhouse/tables/{uuid}/{shard}`) and `default_replica_name`. `Macros::expand` takes
`{shard}` from `MacroExpansionInfo::shard` if set, otherwise from the server's `<macros>`.

## Component tour {#component-tour}

- `src/Storages/MaterializedView/RefreshTask.cpp`, the `RefreshTask` constructor — storage
  layer, runs at `CREATE` and `ATTACH` of the view; expands the coordination path before
  any Keeper call.

## Problem {#problem}

Tables in a `Replicated` database already get the database's shard name for `{shard}`
(`DatabaseReplicated` sets it), but the refreshable MV does not, so on servers without a
global `<shard>` macro every coordinated refreshable MV fails to create.

## Decisions {#decisions}

### Look the database up without throwing {#decision-try-get}

- **Chosen:** `tryGetDatabase`, then `dynamic_cast` to `DatabaseReplicated` on the owned
  pointer; the cast is the only guard.
- **Rejected:** `getDatabase` as on 26.3; it throws if the database is not registered,
  which would turn a macro lookup into a load failure.
- **Rung:** edit to an upstream hot path (rung 5), three lines; upstream candidate (rung 2).

## Approach {#approach}

If the view's database is `Replicated`, set `info.shard` from it; otherwise expansion is
unchanged.

## Concept {#concept}

`{shard}` has two sources, the server `<macros>` and the `Replicated` database; every
place that expands a Keeper path for a table in such a database must pass the second.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-and-applies`

- Upstream still expands the path with only `table_id` set.
- Upstream 26.8 requires the Keeper-only `MULTI_READ` and `CREATE_IF_NOT_EXISTS` flags for
  coordinated views, which the test harness randomizes. `050`, which lands before this patch,
  removes that requirement, so the test needs no flag pinning.

## Customer impact {#customer-impact}

Same as 26.3: coordinated refreshable MVs can be created in `Replicated` databases on
servers without a global `<shard>` macro. Upstream tests edited: none.

## Security review {#security-review}

n/a — the shard name comes from the database's own definition; no new input or access.

## Tests {#tests}

- **Paths:** `tests/integration/test_aiven_refreshable_mv_shard_macro_expansion/`
- **Proves:** `CREATE MATERIALIZED VIEW ... REFRESH` in a `Replicated` database succeeds
  with no global `<shard>` macro; without the patch it fails with `NO_ELEMENTS_IN_CONFIG`.
- **Run:** `python -m ci.praktika run "integration" --test test_aiven_refreshable_mv_shard_macro_expansion`
- **Evidence:** PASS on the 26.8 lane build (Buildkite amd_asan_ubsan). On the parent `CREATE
  MATERIALIZED VIEW` fails with `NO_ELEMENTS_IN_CONFIG`; shown on 26.3, not re-run on 26.8.

## Rollback {#rollback}

- **Revert:** safe; existing views keep their stored Keeper path, but new ones fail again.
- **Persistent state:** coordination znodes under the expanded path.
- **Disable without rebuild:** none needed.

## Retirement {#retirement}

Upstream passing the database's shard name when expanding the coordination path.

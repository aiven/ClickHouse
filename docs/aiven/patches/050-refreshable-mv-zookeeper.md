---
description: 'Dossier for the Aiven patch that keeps coordinated refreshable materialized views working on Apache ZooKeeper'
sidebarTitle: '050: Refreshable MVs on ZooKeeper'
slug: '/aiven/patches/050-refreshable-mv-zookeeper'
title: 'Patch 050: Refreshable materialized views on Apache ZooKeeper'
doc_type: 'reference'
aiven_identity: '050'
aiven_absorbs: ['N02']
aiven_changelog_category: 'Bug Fix'
aiven_settings: []
---

# Patch 050 — refreshable materialized views on Apache ZooKeeper {#patch-050-refreshable-mv-zookeeper}

## Summary {#summary}

The patch lets coordinated refreshable materialized views run on Apache ZooKeeper, which lacks
the ClickHouse-Keeper-only `MULTI_READ` and `CREATE_IF_NOT_EXISTS` extensions that upstream
requires. Aiven runs ClickHouse on ZooKeeper, so without it every refreshable view in a
`Replicated` database fails to create, and existing ones stop for good after the upgrade. On
ClickHouse Keeper behaviour is unchanged.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.8 | `63f05c4e12f` | original: removed the `MULTI_READ` requirement |
| 26.3 | `patch-port(050)` `81966a10989`; `patch-new(N02)` `edc5cd1ccd6` | port; `N02` added because a backport made the `/running` create a Keeper-only op |
| 26.8 | `patch-port(050)` | `still-needed-but-rewrite`, one commit; absorbs `N02`; upstream's new missing-flags give-up path neutralized at its single check |

## Background (ClickHouse) {#background}

A refreshable materialized view in a `Replicated` database is *coordinated*: its replicas share
state in Keeper under a path built from `default_replica_path`. The root znode holds the
schedule state, an ephemeral `/running` znode marks the replica that is refreshing, `/paused`
pauses all replicas, and `/replicas/<name>` registers each replica. ClickHouse Keeper offers
extensions plain ZooKeeper lacks, among them `MULTI_READ` (several reads in one atomic request)
and `CREATE_IF_NOT_EXISTS` (a create that succeeds if the znode exists). The ClickHouse client
reads the server's feature flags on connect; `multiRead` falls back to separate reads when
`MULTI_READ` is missing.

## Component tour {#component-tour}

All changes are in `src/Storages/MaterializedView/RefreshTask.cpp`, the storage-layer task that
schedules and runs refreshes:

- `coordinationFeatureFlagsMissing` — upstream's single check that both flags are present. It
  feeds the constructor (fresh `CREATE` throws `NOT_IMPLEMENTED`; `ATTACH` marks the view
  "coordination unavailable", which disables it permanently), the unpause path and the
  re-check in `doScheduling`.
- The `RefreshTask` constructor — creates the coordination znodes on first use, now through
  the new helper `createCoordinationZnodes`.
- `readZnodesIfNeeded` — reads the three znodes on the scheduling thread.
- `updateCoordinationState` — the `multi` that updates the root znode and creates or removes
  `/running`.

## Problem {#problem}

On Apache ZooKeeper, stock 26.8:

- rejects `CREATE MATERIALIZED VIEW ... REFRESH ...` in a `Replicated` database with
  `NOT_IMPLEMENTED` ("Keeper server doesn't have all feature flags required by refreshable MV:
  MULTI_READ, CREATE_IF_NOT_EXISTS");
- at startup marks every existing coordinated view "coordination unavailable": it shows
  `Disabled` in `system.view_refreshes` and `SYSTEM START VIEW` cannot bring it back. Upgrading
  a ZooKeeper-backed 26.3 service stops all its refreshable views;
- would, if those checks were simply removed, send the `/running` create as `CreateIfNotExists`,
  which ZooKeeper cannot parse inside a `multi`. ClickHouse then treats the reply as a transport
  failure and drops the shared session, so every replicated table on the node goes read-only,
  over and over.

## Decisions {#decisions}

### Neutralize the give-up path at its single check {#decision-single-check}

- **Chosen:** `coordinationFeatureFlagsMissing` returns `false`, because after this patch nothing
  coordination needs is missing on ZooKeeper. Upstream's give-up machinery stays compiled and
  still runs under its failpoints.
- **Rejected:** deleting the give-up path (the 26.3 approach removed the checks one by one). On
  26.8 the path spans the constructor, unpause and `doScheduling`; deleting it is a large,
  conflict-prone diff for no behavioural gain.
- **Rung:** edit to an upstream hot path (rung 5). Config cannot add Keeper features to ZooKeeper,
  and migrating the fleet to ClickHouse Keeper is out of scope. Upstream candidate (rung 2):
  upstream CI only runs Keeper.

### Keep ClickHouse Keeper on upstream's code path {#decision-keeper-unchanged}

- **Chosen:** use the Keeper-only operation when the server advertises it, emulate it only when
  it does not: the coordination znodes are created with upstream's atomic `multi` on Keeper and
  one by one on ZooKeeper; `/running` uses `CreateIfNotExists` on Keeper and an existence check
  plus a plain create on ZooKeeper.
- **Rejected:** the 26.3 `050` choice of always creating the znodes one by one. It changes
  Keeper behaviour without need.

### Absorb `N02` {#decision-absorb-n02}

- **Chosen:** one identity. `N02` fixed the same requirement — refreshable views on ZooKeeper —
  for a code path a backport had changed after `050` was written.

## Approach {#approach}

- `coordinationFeatureFlagsMissing` returns `false`, so a fresh `CREATE`, a view loaded at
  startup and an unpause all proceed on ZooKeeper.
- `createCoordinationZnodes`: on ZooKeeper, each persistent znode is created separately and an
  existing one is accepted, which is what `ignore_if_exists` means. Atomicity is not needed: a
  replica reads the znodes only after all exist.
- `readZnodesIfNeeded`: the `MULTI_READ` throw is removed; `tryGet` sends separate reads on
  ZooKeeper. The reads are not atomic there; a stale read is corrected by the watches set just
  before, and the root znode's version check on every write.
- `updateCoordinationState`: without `CREATE_IF_NOT_EXISTS`, `/running` is created only if an
  `exists` check finds it absent. The check comes before the `multi` because a plain create
  failing with `ZNODEEXISTS` would roll back the root-znode update in the same `multi`.

## Concept {#concept}

Upstream CI runs only ClickHouse Keeper, so Keeper-only operations slip into code that must also
work on ZooKeeper. Every such use needs a feature-flag gate with a ZooKeeper fallback, and the
test must use a Keeper with *all* extensions off, not just the one under suspicion.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-but-rewrite`

- Upstream added a permanent "coordination unavailable" state, failpoints and re-checks for
  missing flags (`coordinationFeatureFlagsMissing`, `markCoordinationUnavailable`). On ZooKeeper
  it disables every coordinated view at startup — a new, worse failure than on 26.3.
- The constructor's creates and the `/running` create both use `ignore_if_exists`; both gated.
- `readZnodesIfNeeded` still throws without `MULTI_READ`; removed as on 26.3.

## Customer impact {#customer-impact}

- On ZooKeeper-backed services, coordinated refreshable views can be created and keep refreshing
  across restarts and the upgrade from 26.3. Same as 26.3.
- On ClickHouse Keeper: none.
- Upstream tests edited: none. Two cases of `test_refreshable_mv_no_multi_read`
  (`test_refreshable_mv_attach_without_multi_read`,
  `test_refreshable_mv_attach_feature_flag_propagation_race`) assert that a view on a Keeper
  without `MULTI_READ` ends `Disabled`; with this patch it keeps running, so they fail by design.
  Its failpoint-driven cases force the flags missing and should still pass (not run locally).
  The fork's test lanes run only `aiven_*` suites.
- Aiven's own `004` test (`test_aiven_replace_mergetree_with_replicated`) has refreshable-view
  cases that fail without this patch whenever the harness randomly disables either flag.

```sql
-- On a ZooKeeper-backed server:
CREATE MATERIALIZED VIEW rdb.mv REFRESH EVERY 1 HOUR TO rdb.tgt AS SELECT a FROM rdb.src;
-- Before: Code: 48. DB::Exception: Keeper server doesn't have all feature flags required by refreshable MV: MULTI_READ, CREATE_IF_NOT_EXISTS
-- After:  Ok.
SELECT status FROM system.view_refreshes WHERE view = 'mv';  -- after a restart
-- Before: Disabled   After: Scheduled
```

## Security review {#security-review}

n/a — changes how coordination znodes are created and read; no access control, secrets or new
input.

## Tests {#tests}

- **Paths:** `tests/integration/test_aiven_refreshable_mv_zookeeper/`
- **Proves:** on a Keeper with every Keeper-only extension off (it reports the ZooKeeper-compatible
  API version), a coordinated view can be created, refreshes twice, still refreshes after a
  restart, and the replicated tables stay writable. The same scenario on a default Keeper is the
  control. Without the patch the `CREATE` fails with `NOT_IMPLEMENTED`.
- **Run:** `python -m ci.praktika run "integration" --test test_aiven_refreshable_mv_zookeeper`
- **Evidence:** the integration suite was not run locally (no Docker); runs on Buildkite. The same
  scenario was reproduced by hand on a local arm64 debug build with that Keeper configuration:
  the parent build fails the `CREATE` with code 48; the patched build passes create, two refreshes
  and a restart; restarting the parent binary on the patched build's data leaves the view
  `Disabled`.

## Rollback {#rollback}

- **Revert:** on ZooKeeper, every coordinated refreshable view is disabled at the next startup.
- **Persistent state:** coordination znodes; identical in content to what upstream writes.
- **Disable without rebuild:** none.

## Retirement {#retirement}

Upstream supporting refreshable views on Apache ZooKeeper (offer this patch), or the fleet moving
to ClickHouse Keeper.

---
description: 'Run the fetches of a replica catching up in their own background pool, so they do not delay the replication of new inserts'
sidebarTitle: '046: Early fetch pool'
slug: '/aiven/patches/046-early-fetch-pool'
title: 'Patch 046: Early fetch pool'
doc_type: 'reference'
aiven_identity: '046'
aiven_absorbs: []
aiven_changelog_category: 'Improvement'
aiven_settings:
  - 'aiven_use_early_fetch_pool'
  - 'aiven_background_early_fetches_pool_size'
---

# Patch 046 — Early fetch pool {#patch-046-early-fetch-pool}

## Summary {#summary}

A new or recovering `ReplicatedMergeTree` replica copies every existing part from the other
replicas, while new inserts keep producing small parts it must also fetch. Upstream runs both
in the one pool sized by `background_fetches_pool_size`, so the large catch-up fetches can take
every slot while the small fetches, each one a queue entry in Keeper, pile up behind them. With
the MergeTree setting `aiven_use_early_fetch_pool` (on by default), the catch-up fetches run in
a separate pool sized by the server setting `aiven_background_early_fetches_pool_size`
(default 8), and the normal pool stays free for new inserts. Query results are unchanged; only
the pool that runs a fetch differs.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.3 | `68fbdb513d0` | original, setting `use_early_fetch_pool`, server setting `background_early_fetches_pool_size` |
| 25.8 | `7e08e44e513` | carried; lost the removal of a dropped table's tasks from the early pool |
| 26.3 | `patch-port(046)` `9823b972788`, `patch-fix(046)` `a24990eef8f` | renamed to `aiven_use_early_fetch_pool` (old name kept as alias) and `aiven_background_early_fetches_pool_size`; the fix restored the removal |
| 26.8 | `patch-port(046)` | `still-needed`: 26.3 and its fix, plus the pool resize on config reload |

## Background (ClickHouse) {#background}

Each replica keeps a replication queue in Keeper. A `GET_PART` entry asks it to fetch a part
from another replica; it normally names the replica that created the part (`source_replica`).
`StorageReplicatedMergeTree::scheduleDataProcessingJob` hands `GET_PART` and `ATTACH_PART`
entries to the fetches executor, and `canExecuteFetch` postpones an entry while that executor
has no free slot, recording the reason in `system.replication_queue.postpone_reason`.

Two paths create entries without a source replica: `cloneReplica`, when a new replica (or one
marked lost) copies the parts of another, and `removePartAndEnqueueFetch`, when a broken part
is fetched again.

## Component tour {#component-tour}

- **Early fetches executor** (new, `Context`). An `OrdinaryBackgroundExecutor` next to the
  fetches executor, with metrics `BackgroundEarlyFetchesPoolTask` and
  `BackgroundEarlyFetchesPoolSize`, thread name `EarlyFetch`, and the fetch profile events. It
  is shut down with the other executors.
- **`BackgroundJobsAssignee`** (upstream). New `scheduleEarlyFetchTask`; `finish` also removes
  the table's tasks from the early executor.
- **`scheduleDataProcessingJob`** and **`canExecuteFetch`** (upstream). An entry without a
  source replica, on a table with `aiven_use_early_fetch_pool`, goes to the early executor
  and is postponed with "early fetches already executing" when it is full; every other fetch
  is unchanged.
- **Config reload** (upstream, `Server.cpp`). Raises the early pool like the other pools.
- **Settings.** `aiven_use_early_fetch_pool` in `MergeTreeSettings.cpp` with alias
  `use_early_fetch_pool`; `aiven_background_early_fetches_pool_size` in `ServerSettings.cpp`,
  reported `IncreaseOnly` in `system.server_settings`.

## Problem {#problem}

When a cluster gains a replica while inserts continue, the new replica's queue holds one
large catch-up entry per existing part and a stream of small entries for new parts. With more
existing parts than fetch slots, the small entries wait for the large downloads, and the queue
can grow to many thousands of Keeper nodes for one table. Enlarging the fetch pool does not
help, because a large table always has more existing parts than any reasonable pool size.

## Decisions {#decisions}

### Port as is {#decision-port}

- **Chosen:** carry the 26.3 shape with its fix.
- **Rejected:** dropping it; upstream 26.8 still runs all fetches in one pool.
- **Rung:** 5, routing inside upstream's replication queue scheduling. Rung 1 cannot serve:
  no upstream setting gives these fetches their own slots.

### Keep the old table setting name as an alias {#decision-alias}

- **Chosen:** `DECLARE_WITH_ALIAS(..., use_early_fetch_pool)`.
- **Why:** server configs written for versions before 26.3 set `use_early_fetch_pool` in
  `<replicated_merge_tree>`, and tables whose definition sets it store the old name. Without
  the alias the first fails server startup and the second fails to load (`UNKNOWN_SETTING`).
- **Not aliased:** the server setting's old name `background_early_fetches_pool_size`, as on
  26.3. It is rejected at startup (`UNKNOWN_ELEMENT_IN_CONFIG`); the managed configuration
  never sets it.

### Resize on config reload {#decision-reload}

- **Chosen:** raise the early pool in the config reload, like the four sibling pools.
- **Why:** 26.3 already reported the setting as `IncreaseOnly` but never resized the pool, so
  `system.server_settings` described a reload that did nothing.

### Smaller differences from 26.3 {#decision-small}

- Not added to the list of pool settings that may also come from the default profile: that
  list exists for settings that used to be profile settings, which this never was.
- The `SettingsChangesHistory` row is a rename (`true` to `true`) in the 26.8 block, like
  patch 008's renamed settings.

## Approach {#approach}

The routing predicate is the same in both places that need it, so a fetch is checked against
the pool that will run it. Broken-part re-fetches also have no source replica and use the
early pool, as on 26.3; they are recovery traffic like the catch-up. All other checks in
`canExecuteFetch` (cancelled fetches, network throttling, waiting for a broken part to be
removed) apply to both pools.

## Concept {#concept}

When two kinds of work with very different sizes share a fixed number of slots, the large
kind decides the latency of the small kind. A separate pool is the simplest isolation: each
kind can only exhaust its own slots.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed`

- All anchors (`Context` executors, `BackgroundJobsAssignee`, `scheduleDataProcessingJob`,
  `canExecuteFetch`, the `ThreadName` list) are unchanged in shape.
- Upstream 26.8 lowers `background_pool_size` on small-memory hosts; this applies to merges
  only, so the early pool is not lowered, like the fetch pool.

## Customer impact {#customer-impact}

- **Who:** every replicated table, since the setting is on by default.
- **What they see:** while a replica catches up, its catch-up fetches show in
  `system.replication_queue` with the reason below, and fetches of newly inserted parts keep
  running:

```sql
SELECT type, new_part_name, postpone_reason
FROM system.replication_queue
WHERE table = 'events' AND postpone_reason != '';
-- GET_PART  all_12_12_0  Not executing fetch of part all_12_12_0 because 8 early fetches already executing, max 8.

SELECT metric, value FROM system.metrics WHERE metric LIKE 'BackgroundEarlyFetchesPool%';
-- BackgroundEarlyFetchesPoolTask  8
-- BackgroundEarlyFetchesPoolSize  8
```

- **Configuration:** `aiven_use_early_fetch_pool` can be set per table (`SETTINGS`) or in
  `<merge_tree>` / `<replicated_merge_tree>`; the old name works in both places.
  `aiven_background_early_fetches_pool_size` can be raised without restart.
- **How to restore upstream behaviour:** `ALTER TABLE ... MODIFY SETTING aiven_use_early_fetch_pool = 0`,
  or set it to `0` in `<replicated_merge_tree>`.
- **New versus 26.3:** raising the pool size takes effect on config reload.
- **Upstream tests edited:** none.

## Security review {#security-review}

No new input or privilege: the settings are MergeTree and server settings with the usual
access rules, and the routing only chooses between two executors for work the replica already
had to do. Removing a dropped table's tasks from the early executor keeps a fetch from running
against a table that no longer exists.

## Tests {#tests}

- **Paths:** `tests/integration/test_aiven_early_fetch_pool/`
- **Proves:** the pool size comes from the server setting; with the table setting on, a
  replica catching up postpones its surplus fetches with "early fetches already executing",
  with it off never; the old name in `<replicated_merge_tree>` changes the default and the
  server starts; a table using the old name routes to the early pool and re-attaches;
  dropping a table while it catches up empties the early pool and the server keeps
  running; raising the pool size on config reload takes effect.
- **Run:** `python -m ci.praktika run "integration" --test test_aiven_early_fetch_pool`, or
  `pytest` from `tests/integration` with `CLICKHOUSE_TESTS_SERVER_BIN_PATH` set.
- **Evidence:** on the parent commit every test errors at setup, because the server rejects
  the unknown `aiven_background_early_fetches_pool_size` element (`UNKNOWN_ELEMENT_IN_CONFIG`);
  with the patch all 6 pass, three runs in a row (2026-11-05). In the drop case the table was
  dropped after 12 of its 40 parts had been fetched.

## Rollback {#rollback}

- **Downgrade:** not supported; services only move forward.
- **Revert in a later release:** no on-disk or Keeper format of its own; tables keep their
  queue entries and fetch them in the normal pool.
- **Disable without rebuild:** set `aiven_use_early_fetch_pool = 0` per table or in the server
  config.

## Retirement {#retirement}

An upstream way to prioritise or isolate fetches of new parts over a replica's catch-up.
Re-check upstream on every uplift. The release that drops the patch keeps both settings
declared with `SettingsTierType::OBSOLETE`, and the alias; an unknown element stops the
server from starting and an unknown table setting stops the table from loading.

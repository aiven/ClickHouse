---
description: 'Delay and then reject inserts into ReplicatedMergeTree tables while a replica replication queue in Keeper is too large'
sidebarTitle: '008: replication queue size limit'
slug: '/aiven/patches/008-replication-queue-size-limit'
title: 'Patch 008: Replication queue size limit'
doc_type: 'reference'
aiven_identity: '008'
aiven_absorbs: []
aiven_changelog_category: 'Improvement'
aiven_settings:
  - 'aiven_enable_replication_queue_size_limit'
  - 'queue_size_to_delay_insert -> aiven_queue_size_to_delay_insert'
  - 'queue_size_to_throw_insert -> aiven_queue_size_to_throw_insert'
  - 'queues_total_size_to_delay_insert -> aiven_queues_total_size_to_delay_insert'
  - 'queues_total_size_to_throw_insert -> aiven_queues_total_size_to_throw_insert'
  - 'queue_size_monitor -> aiven_queue_size_monitor'
---

# Patch 008 — Replication queue size limit {#patch-008-replication-queue-size-limit}

## Summary {#summary}

Inserts into a `ReplicatedMergeTree` table are delayed, and then rejected with
`LIMIT_EXCEEDED`, while the largest replication queue among the table's replicas, or the
sum of those maxima over all replicated tables of the server, is above a threshold. Aiven
carries it because a replica that falls behind grows its queue in Keeper without bound,
and a Keeper that runs out of memory is very hard to recover. Nothing changes unless the
server setting `aiven_enable_replication_queue_size_limit` is enabled, which the managed
configuration does; then a lagging replica slows writers down instead of taking Keeper
with it.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.8 | `d6e78ab9938674342224e2e58408fadccaae9ee7` | introduced, always on |
| 26.3 | `patch-port(008)` `ee5c47ee13c` | `still-needed-but-rewrite`; gated behind the new default-off server setting `aiven_enable_replication_queue_size_limit` |
| 26.8 | `patch-port(008)` | `still-needed-but-rewrite`, one commit. The insert gate is re-implemented for 26.8's split throw/delay checks; the total no longer lives in a `Context` map. Renamed: `queue_size_to_delay_insert`, `queue_size_to_throw_insert`, `queues_total_size_to_delay_insert`, `queues_total_size_to_throw_insert` and `queue_size_monitor` to their `aiven_` names, the old names kept as aliases |

## Background (ClickHouse) {#background}

Every `ReplicatedMergeTree` replica has a replication queue: the log entries (fetch a part,
merge, mutate) it has copied from the shared log but not executed yet. The queue exists
twice, in memory and as one child node per entry under `<table path>/replicas/<replica>/queue`
in Keeper. A replica that cannot keep up, because fetches are slow, stopped, or failing,
keeps queuing while every insert on any replica adds another entry.

Upstream already has a back-pressure mechanism for a different symptom:
`MergeTreeData::delayInsertOrThrowIfNeeded` delays and then rejects inserts when a
partition has too many parts (`parts_to_delay_insert`, `parts_to_throw_insert`). It has
nothing for queue size: `max_replicated_merges_in_queue` and its siblings only limit what
a replica *schedules*, not what other replicas pile up for it.

## Component tour {#component-tour}

- **`ReplicatedMergeTreeQueueSizeThread`** (new, background). A `BackgroundSchedulePool`
  task per table. Each second, `iterate` lists the table's replicas and reads the child
  count of every replica's `queue` node in one batched `exists` request, then `publish`
  stores the maximum in an atomic and applies the difference to the
  `ReplicatedQueuesTotalSize` metric. The metric is therefore the server-wide total, and
  each table owns exactly its share of it.
- **`ReplicatedMergeTreeRestartingThread`** (upstream, background). It activates a
  replica's replication tasks once the replica is active; the queue size task starts
  there, next to the cleanup and part check threads.
- **`StorageReplicatedMergeTree`** (upstream, storage). Owns the thread.
  `partialShutdown`, which runs whenever the replica goes read-only and on shutdown, stops
  it; `shutdown` removes the table's share of the total. `getReplicationQueueSizesForInsert`
  answers the insert gate.
- **`MergeTreeData::delayInsertOrThrowIfNeeded`** (upstream, insert path). Calls the
  virtual `getReplicationQueueSizesForInsert`, empty for every other engine, and folds the
  result into its existing throw checks and delay computation.
- **`checkReplicationQueueSizesForInsert`** (new, insert path). Resolves the thresholds,
  a query-level value winning when set, throws, or returns the delay.
- **Settings.** The server switch in `ServerSettings.cpp`; four thresholds each as session
  setting (`Settings.cpp`) and table setting (`MergeTreeSettings.cpp`); the session setting
  `aiven_queue_size_monitor`; history rows in `SettingsChangesHistory.cpp`.

## Problem {#problem}

When one replica lags, its Keeper queue grows by one node per insert on any replica, and
nothing slows the writers down. Keeper holds the whole tree in memory, so a sustained lag
ends with Keeper running out of memory. That takes down replication and DDL for every
table on the service, and a Keeper that crashed this way may not restart cleanly;
hand-pruning hundreds of thousands of queue nodes back to a consistent state is not
practical. The queue is also slower to process the longer it gets, so the lag feeds
itself.

## Decisions {#decisions}

### The patch stays {#decision-the-patch-stays}

- **Chosen:** carry the limit on 26.8.
- **Rejected:** dropping it as covered by upstream. 26.8 has no gate on queue size.
- **Rung:** 5, an edit to the upstream insert gate, kept to one virtual call: the logic
  lives in new files. Rung 1 cannot serve, there is no upstream mechanism to configure.
  Rung 2 is open: the feature has no Aiven-specific policy in it and protects any Keeper,
  so it is a candidate for an upstream contribution once this shape has settled.

### The gate is a virtual hook, not new parameters {#decision-virtual-hook}

- **Chosen:** `MergeTreeData::getReplicationQueueSizesForInsert`, overridden by
  `StorageReplicatedMergeTree`, read inside `delayInsertOrThrowIfNeeded`.
- **Rejected:** the 26.3 shape, which added two optional parameters to
  `delayInsertOrThrowIfNeeded` and passed them from one call site in the sink. 26.8 calls
  the gate from three places in `ReplicatedMergeTreeSink` (throw-only at construction,
  delay-only in `onStart` and between blocks) and for the destination of
  `MOVE PARTITION ... TO TABLE`; every one of them would need the parameters, and a missed
  one silently skips the limit.
- **Supersedes:** the 26.3 signature change.

### Queue throws follow `allow_throw` {#decision-allow-throw}

- **Chosen:** the queue checks throw only where the parts checks throw, and delay only
  where they delay.
- **Rejected:** the 26.3 checks, which threw unconditionally. They were correct there only
  because a single call site passed the queue sizes. On 26.8 they would throw from the
  delay-only calls, in the middle of a multi-block insert.

### The total is a metric, and each table removes its share {#decision-total-as-metric}

- **Chosen:** each table publishes into `ReplicatedQueuesTotalSize` by difference and
  subtracts its share on shutdown, also from the thread's destructor if shutdown threw.
- **Rejected:** the 26.3 map in `Context` keyed by storage UUID. Only `DROP` cleared it, so
  a detached table kept its last queue size in the total, which could reject inserts into
  every table of the server; and tables without a UUID (an `Ordinary` database) shared one
  key.

### Threshold 0 rejects every insert {#decision-threshold-zero}

- **Chosen:** keep the 26.3 semantics. With the switch on, a threshold is compared with
  `>=`, so 0 means "always".
- **Rejected:** "0 disables", the convention of upstream's newer insert gates. It would
  change what an existing 0 means on upgrade.
- With the switch off nothing is compared at all. On 26.3 a 0 threshold rejected inserts
  even with the switch off; that is fixed.

### Fork settings are renamed, old names kept as aliases {#decision-rename-with-aliases}

- **Chosen:** the `aiven_` names from the
  [naming rule](../runbooks/porting-doctrine.md#setting-naming), with the old names as
  `DECLARE_WITH_ALIAS` aliases.
- **Rejected:** no aliases, as the naming rule asks. These are session and table settings,
  not only config keys: on 26.3 they could be stored in `CREATE TABLE ... SETTINGS` and
  in SQL settings profiles, and a table whose metadata names an unknown setting does not
  load. An alias maps the old name onto the same setting, so a stored value keeps
  applying; it is not the silent fallback the rule guards against.
- **Rejected:** marking the old names obsolete. Metadata would load, but stored values
  would be ignored without a trace.

### The session copies stay {#decision-session-settings}

- **Chosen:** the four thresholds and `aiven_queue_size_monitor` remain session settings
  as well as table settings.
- **Rejected:** table settings only. The managed settings catalogue lists the session
  settings, and removing them would be a second rename on the same upgrade.

## Approach {#approach}

The poll runs only while the replica is active: the restarting thread starts it with the
other replication tasks and `partialShutdown` stops it, so a read-only replica does not
poll a Keeper it cannot reach. Its last value stays in the total until the table shuts
down, which keeps the limit conservative during a Keeper outage.

The gate adds the queue terms to the upstream checks. With `allow_throw` it throws
`LIMIT_EXCEEDED` (`Too large replication queue (N)` for the per-table maximum,
`Too many items in replication queues (N)` for the server total), counted in
`RejectedInserts`. With `allow_delay` it returns a delay between `min_delay_to_insert_ms`
and `max_delay_to_insert`, growing linearly from the delay threshold to the throw
threshold, and the insert sleeps for the larger of this and the parts delay.

`aiven_enable_replication_queue_size_limit` and `aiven_queue_size_monitor` are read when
the table object is created, the latter from the default profile, so changing either
takes effect for a table on its next attach or server restart. With the switch off, the
thread is never created and `getReplicationQueueSizesForInsert` is empty, so inserts take
the upstream path.

The insert never queries Keeper: it reads two atomics.

## Concept {#concept}

When upstream gives a function mode flags (`allow_throw`, `allow_delay`), every addition
inside it has to honour them; a check that was correct because of who called it stops
being correct when upstream moves the calls. And state that a component publishes into a
shared total must be withdrawn by the same component, on every exit path, or the total
outlives its contributors.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-but-rewrite`

- `delayInsertOrThrowIfNeeded` gained `allow_delay`, and `ReplicatedMergeTreeSink` now
  checks for throw at construction, rethrows in `onStart`, and only delays in `onStart`
  and `consume`. The 26.3 hunk in the gate does not apply.
- The delay computation merges parts and dead-blob delays into one factor; the queue
  delay is combined with it by `max`.
- `03999_stateless_settings_history` requires a history row for every new session and
  table setting.
- `BackgroundSchedulePool::createTask(StorageID, name, function)` and Keeper component
  tracking are as on 26.3.

## Customer impact {#customer-impact}

- **Who:** every service whose configuration enables
  `aiven_enable_replication_queue_size_limit`, which is every managed service.
- **What they see:** while a replica lags by more than `aiven_queue_size_to_delay_insert`
  entries (50000) inserts slow down, up to `max_delay_to_insert` each; at
  `aiven_queue_size_to_throw_insert` (100000) they fail with `LIMIT_EXCEEDED`. The server
  totals are 100000 and 200000. `ReplicatedQueuesTotalSize` in `system.metrics` shows the
  total the gate compares.
- **How to restore upstream behaviour:** raise the thresholds per table
  (`ALTER TABLE ... MODIFY SETTING`), per query, or in `<merge_tree>`; or disable the
  server switch.
- **New versus 26.3:**
  - The settings have `aiven_` names; the old names still work as aliases.
  - Detaching a table removes its queue from the server total. On 26.3 it stayed until
    the table was attached again or dropped.
  - `MOVE PARTITION ... TO TABLE` into a replicated table is now subject to the limit, and
    a multi-block insert is delayed between blocks, like the parts limit.
  - With the switch off, a 0 threshold no longer rejects inserts.
- **Upstream tests edited:** none.
- **Example**, from the integration test, as any user, on a server with the switch on and
  a replica that stopped fetching:

```sql
SELECT value FROM system.metrics WHERE metric = 'ReplicatedQueuesTotalSize';
-- 11
INSERT INTO stalled VALUES (100);
-- Code: 290. DB::Exception: Too large replication queue (11). One of the replicas is too
-- slow for inserts. (LIMIT_EXCEEDED)
```

## Security review {#security-review}

n/a — an availability limit on the insert path. It reads queue sizes from Keeper with the
server's own session, takes no identifiers from SQL into a Keeper path (the table's
replica path is server-built), and any user who can insert can already raise the
thresholds for their own queries or tables, which only affects their own service.

## Tests {#tests}

- **Paths:** `tests/integration/test_aiven_replication_queue_size_limit/`
- **Proves:** with the switch on, a stalled replica first delays and then rejects inserts;
  a query-level threshold overrides the table's; detaching the table removes its share of
  the total; a stored pre-rename setting name still applies; with the switch off the same
  inserts behave as upstream, also with a 0 threshold.
- **Run:** `python -m ci.praktika run "integration" --test test_aiven_replication_queue_size_limit`,
  or `pytest` from `tests/integration` with `CLICKHOUSE_TESTS_SERVER_BIN_PATH` set.
- **Evidence:** FAIL on the parent binary (the server rejects the unknown switch at
  startup), PASS with the patch (3 passed, 2026-11-05).

## Rollback {#rollback}

- **Downgrade:** not supported; services only move forward.
- **Revert in a later release:** no on-disk or Keeper format, and the only Keeper access is
  reads. The names are persistent state, though: table metadata and settings profiles can
  store any of the nine names, and the configuration carries the switch. A release that
  drops this patch must keep every name declared as obsolete (see Retirement), or such
  tables do not load and such servers do not start.
- **Disable without rebuild:** set `aiven_enable_replication_queue_size_limit` to false
  and restart, or raise the thresholds.

## Retirement {#retirement}

An upstream insert gate on replication queue size, or an upstream Keeper-side limit that
bounds per-replica queues. Re-check upstream on every uplift; this is also the patch to
offer upstream.

Retiring removes the mechanism, not the names: the `aiven_` session settings and their
old-name aliases go to `OBSOLETE_SETTINGS` (`MAKE_OBSOLETE`), the table settings to
`MAKE_OBSOLETE_MERGE_TREE_SETTING`, and the server switch stays declared with
`SettingsTierType::OBSOLETE`, as upstream does when it removes a setting.

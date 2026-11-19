---
description: 'Dossier for the Aiven patch that lets a Kafka table have zero consumers, so consumption can be paused without dropping the table'
sidebarTitle: '030: Kafka zero consumers'
slug: '/aiven/patches/030-kafka-num-consumers-zero'
title: 'Patch 030: Allow kafka_num_consumers = 0'
doc_type: 'reference'
aiven_identity: '030'
aiven_absorbs: []
aiven_changelog_category: 'Improvement'
aiven_settings: []
---

# Patch 030 — allow `kafka_num_consumers = 0` {#patch-030-kafka-num-consumers-zero}

## Summary {#summary}

The patch lowers the minimum of the `Kafka` engine setting `kafka_num_consumers` from 1
to 0. Aiven uses 0 to pause consumption of a Kafka integration table without dropping
it, and without relying on SQL a tenant could undo. A table with zero consumers starts
no consumer and never contacts the broker; everything else about the table is unchanged.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.8 | `37b61f443b` | original; also guards `StorageKafka2` |
| 26.3 | `patch-port(030)` `9eea1c9341a` | `still-needed`, clean cherry-pick |
| 26.8 | `patch-port(030)` | `still-needed-and-applies`; one context conflict next to upstream's new upper bound |

## Background (ClickHouse) {#background}

A `Kafka` table is a stream, not storage. On startup it creates `kafka_num_consumers`
librdkafka consumers in one consumer group; background tasks poll them and push blocks
into the materialized views attached to the table. There are two engines behind
`ENGINE = Kafka`: `StorageKafka` (offsets committed to Kafka) and `StorageKafka2`
(offsets kept in Keeper, chosen when `kafka_keeper_path` is set). Both read the same
settings and validate them in `registerStorageKafka`.

## Component tour {#component-tour}

- `src/Storages/Kafka/StorageKafkaUtils.cpp`, `registerStorageKafka` — the factory
  lambda that validates engine settings at `CREATE` and `ATTACH`. Upstream rejects
  `kafka_num_consumers < 1` here; on 26.8 it also rejects values above
  `MAX_KAFKA_NUM_CONSUMERS`.
- `src/Storages/Kafka/StorageKafka.cpp` and `StorageKafka2.cpp`, `getMaxBlockSize` —
  the default block size is `max_insert_block_size / num_consumers`, used when a
  consumer's configuration is built.
- `StorageKafka::startup` / `StorageKafka2::startup` create one consumer per slot in a
  `for (i < num_consumers)` loop; with 0 slots nothing is created. On 26.8,
  `StorageKafka2::threadFunc` returns early for a missing slot and a direct `SELECT`
  fails with `QUERY_NOT_ALLOWED` ("No Kafka consumers available for direct read").

## Problem {#problem}

Without the patch the only ways to stop a Kafka integration from consuming are
`DETACH`/`DROP` of the table or its views, or changing the broker side. Those are
per-table statements that a tenant can run too, and they change what the tenant sees in
the schema. The control plane needs a lever it owns that keeps the table and its views
in place: set the table's consumer count to zero, and back to the real count later.
Upstream refuses that with `Number of consumers can not be lower than 1`.

## Decisions {#decisions}

### Keep the setting name {#decision-keep-name}

- **Chosen:** no new setting; the patch only widens the range of the upstream
  `kafka_num_consumers`.
- **Rejected:** a separate `aiven_` pause switch. It would need its own plumbing in both
  engines and would not change stored DDL the control plane already writes.
- **Rung:** edit to an upstream hot path (rung 5), but four lines. Config (rung 1)
  cannot pause one table; this is a candidate for upstream (rung 2), since "0 means
  paused" is generic.

## Approach {#approach}

Remove the `< 1` check in `registerStorageKafka` and keep upstream's new upper bound.
Guard the division in both `getMaxBlockSize` implementations by treating 0 as 1. With 0
consumers no consumer is built, so that division is not reached today; the guard keeps a
later caller from dividing by zero.

## Concept {#concept}

A `Kafka` table's consumer count is its whole runtime footprint: the table, its views
and its offsets live on while it is 0, so 0 is a clean "paused" state.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-and-applies`

- 26.8 added `MAX_KAFKA_NUM_CONSUMERS` right after the removed check; it stays.
- `StorageKafka2` was reworked: slots are pre-sized and `threadFunc` skips empty ones,
  so zero slots is a supported state rather than a lucky one.

## Customer impact {#customer-impact}

- `CREATE TABLE ... ENGINE = Kafka SETTINGS kafka_num_consumers = 0` succeeds instead of
  failing with `BAD_ARGUMENTS`. The table consumes nothing until its count is positive
  again.
- A direct `SELECT` from a zero-consumer `StorageKafka2` table fails with
  `QUERY_NOT_ALLOWED`.
- Same as 26.3 otherwise. Upstream tests edited: none.

```sql
CREATE TABLE t (key UInt64) ENGINE = Kafka
SETTINGS kafka_broker_list = '127.0.0.1:9092', kafka_topic_list = 't',
         kafka_group_name = 'g', kafka_format = 'CSV', kafka_num_consumers = 0;
-- Before: Code: 36. DB::Exception: Number of consumers can not be lower than 1. (BAD_ARGUMENTS)
-- After:  Ok.
```

## Security review {#security-review}

n/a — the patch widens a numeric range on an existing setting; it reads no new input and
grants nothing.

## Tests {#tests}

- **Paths:** `tests/queries/0_stateless/aiven_030_kafka_num_consumers_zero.sql`
- **Proves:** both engines accept 0 consumers, and a zero-consumer `StorageKafka2`
  refuses a direct read instead of crashing. Without the patch both `CREATE`s fail with
  `BAD_ARGUMENTS`.
- **Run:** `./tests/clickhouse-test aiven_030_kafka_num_consumers_zero`
- **Evidence:** PASS on the 26.8 lane build (debug, local arm64 and Buildkite amd_debug). On the parent the `CREATE`s fail with `Number of consumers can not be
  lower than 1` (`BAD_ARGUMENTS`); shown on 26.3, not re-run on 26.8.

## Rollback {#rollback}

- **Revert:** safe, but a table stored with `kafka_num_consumers = 0` then fails to
  attach. Set every such table back to a positive count before downgrading.
- **Persistent state:** the value in stored DDL.
- **Disable without rebuild:** none needed; the patch does nothing unless 0 is used.

## Retirement {#retirement}

Upstream accepting 0 (or adding a pause switch the control plane switches to).

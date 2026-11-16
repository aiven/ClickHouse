---
description: 'Dossier for the Aiven patch that starts a Kafka consumer group a given duration back in time when it has no committed offset'
sidebarTitle: '076: Kafka offset reset by duration'
slug: '/aiven/patches/076-kafka-offset-reset-by-duration'
title: 'Patch 076: Kafka offset reset by duration'
doc_type: 'reference'
aiven_identity: '076'
aiven_absorbs: []
aiven_changelog_category: 'New Feature'
aiven_settings:
  - 'kafka_auto_offset_reset_by_duration_ms (Kafka table setting)'
---

# Patch 076 — Kafka offset reset by duration {#patch-076-kafka-offset-reset-by-duration}

## Summary {#summary}

The patch adds the `Kafka` table setting `kafka_auto_offset_reset_by_duration_ms`. When it
is non-zero, a partition without a committed offset starts at the first message newer than
now minus that duration, instead of at the earliest or latest message. Aiven's Kafka
integrations offer it so a new integration can skip old data without losing recent data;
at the default 0 nothing changes.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.8 | `7e09c63352` | original |
| 26.3 | `patch-port(076)` `be111ef1f39` | `still-needed-but-rewrite`: value passed straight to the consumers; assignment callback mutates the partition list in place |
| 26.8 | `patch-port(076)` | `still-needed-and-applies`: context conflicts in `KafkaConsumer` only |

## Background (ClickHouse) {#background}

librdkafka can map a timestamp to the first offset at or after it (`offsetsForTimes`). The
two Kafka engines assign partitions differently: `StorageKafka` lets the consumer group
rebalance and gets an assignment callback; `StorageKafka2` assigns partitions itself from
offsets kept in Keeper, through `KafkaConsumer2::updateOffsets`. In both, a partition
without a committed offset starts where `auto.offset.reset` (patch `032`) says.

## Component tour {#component-tour}

- `src/Storages/Kafka/KafkaSettings.cpp` — the setting.
- `StorageKafka::popConsumer` and `StorageKafka2::createKafkaConsumer` — pass the value to
  the consumer they create.
- `src/Storages/Kafka/KafkaConsumer.cpp`, the assignment callback in `createConsumer` —
  for `StorageKafka`. cppkafka calls `assign()` with the same list after the callback, so
  the callback changes offsets in that list instead of calling `assign()` itself.
- `src/Storages/Kafka/KafkaConsumer2.cpp`, `updateOffsets` — for `StorageKafka2`;
  partitions whose Keeper offset is missing get the resolved offset before `assign`.

## Problem {#problem}

With only `earliest`/`latest`, a new integration on a topic with long retention either
re-reads everything or misses everything already in the topic.

## Decisions {#decisions}

### Fall back rather than fail {#decision-fallback}

- **Chosen:** if resolving fails, or a partition has no message newer than the target,
  log a warning and leave the partition to `auto.offset.reset`.
- **Rejected:** failing the assignment; a broker hiccup would stall consumption.

## Approach {#approach}

Only partitions without a committed offset are touched; committed offsets always win. The
whole block is skipped when the setting is 0.

## Concept {#concept}

In cppkafka's rebalance callback, change the offsets in the list you are given; an
`assign()` call inside the callback is overwritten when the callback returns.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-and-applies`

- `KafkaConsumer` gained `waited_for_assignment` and in-flight block rewind
  (`block_start_offsets`) next to the patched code; both kept.
- cppkafka's assignment-callback contract is unchanged between the 26.3 and 26.8 pins.

## Customer impact {#customer-impact}

- Same as 26.3.
- Known limitation, also on 26.3: if a block is aborted before a partition's first commit,
  the consumer rewinds via committed offsets, finds none, and that partition falls back to
  `auto.offset.reset`.
- Upstream tests edited: none.

## Security review {#security-review}

n/a — chooses a start offset for the table's own consumer group; no access, secrets or new
input paths.

## Tests {#tests}

- **Paths:** `tests/queries/0_stateless/aiven_076_kafka_offset_reset_by_duration.sql`
- **Proves:** the setting parses, round-trips through the stored definition and rejects
  non-numbers. Without the patch `CREATE` fails with `UNKNOWN_SETTING`. The resolution
  itself needs a broker; the control plane's integration tests cover it end to end.
- **Run:** `./tests/clickhouse-test aiven_076_kafka_offset_reset_by_duration`
- **Evidence:** PASS on the 26.8 lane build (debug, local arm64 and Buildkite amd_debug). On the parent `CREATE` fails with `UNKNOWN_SETTING` (not re-run).

## Rollback {#rollback}

- **Revert:** tables stored with the setting stop attaching.
- **Persistent state:** the setting in stored table definitions; consumer offsets already
  committed stay where they are.
- **Disable without rebuild:** set it to 0.

## Retirement {#retirement}

Upstream adding a time-based start position for the Kafka engine.

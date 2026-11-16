---
description: 'Dossier for the Aiven patch that adds per-table Kafka producer tuning settings and keeps the old compression setting names'
sidebarTitle: '033: Kafka producer settings'
slug: '/aiven/patches/033-kafka-extra-settings'
title: 'Patch 033: Extra Kafka producer settings'
doc_type: 'reference'
aiven_identity: '033'
aiven_absorbs: []
aiven_changelog_category: 'Improvement'
aiven_settings:
  - 'kafka_producer_batch_size (Kafka table setting)'
  - 'kafka_producer_batch_num_messages (Kafka table setting)'
  - 'kafka_producer_linger_ms (Kafka table setting)'
  - 'kafka_producer_queue_buffering_max_messages (Kafka table setting)'
  - 'kafka_producer_queue_buffering_max_kbytes (Kafka table setting)'
  - 'kafka_producer_request_required_acks (Kafka table setting)'
  - 'kafka_producer_compression_codec (alias of kafka_compression_codec)'
  - 'kafka_producer_compression_level (alias of kafka_compression_level)'
---

# Patch 033 — extra Kafka producer settings {#patch-033-kafka-extra-settings}

## Summary {#summary}

The patch adds six `Kafka` table settings that tune the producer used by `INSERT INTO` a
Kafka table (batching, linger, queue limits, acks), and keeps the old names
`kafka_producer_compression_codec`/`_level` as aliases of upstream's compression settings.
Aiven's integrations expose these knobs and store them in table definitions. Left at their
defaults, librdkafka's own defaults apply.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.8 | `6384eccfac` | original: six knobs plus a compression enum |
| 26.3 | `patch-port(033)` `f67b8c739c8` | partial: knobs kept, compression became aliases of upstream's settings |
| 26.8 | `patch-port(033)` | `still-needed-and-applies` with context conflicts; the compression aliases registered with the new named-collection override check |

## Background (ClickHouse) {#background}

`INSERT INTO` a `Kafka` table produces messages through a librdkafka producer configured
by `KafkaConfigLoader::getProducerConfiguration`. Since 26.8 a table built on a named
collection may replace one of the collection's keys only with `SHOW NAMED COLLECTIONS
SECRETS`, and never a key marked `NOT OVERRIDABLE`. `NamedCollectionsHelpers.cpp` knows which
flat `kafka_*` settings overwrite which librdkafka property.

## Component tour {#component-tour}

- `src/Storages/Kafka/KafkaSettings.cpp` — the six settings and the `ALIAS` names on
  `kafka_compression_codec`/`kafka_compression_level`.
- `src/Storages/Kafka/KafkaConfigLoader.cpp`, `getProducerConfiguration` — sets the six
  properties before the server config and table-level settings are applied;
  `setKafkaConfigValue` skips `0` for the five properties whose librdkafka minimum is 1.
- `src/Storages/NamedCollectionsHelpers.cpp`, `getKafkaProperty` — the flat-setting →
  librdkafka property list used by the override check.

## Problem {#problem}

Upstream has no per-table producer tuning, and does not know the old compression names
that Aiven integration tables store, so those tables fail to attach with `UNKNOWN_SETTING`.

## Decisions {#decisions}

### Register only the compression aliases with the named-collection override check {#decision-override-check}

- **Chosen:** list `kafka_producer_compression_codec`/`_level` in `getKafkaProperty`. They
  are applied after a collection's keys, so without an entry they would replace them without
  the privilege and past `NOT OVERRIDABLE`.
- **Rejected:** listing the six producer settings as well. They are set before the
  collection's keys, so the collection wins; listing them would refuse statements that change
  nothing.
- **Rung:** edit to an upstream hot path (rung 5); no cheaper rung carries per-table
  producer settings.

## Approach {#approach}

The knobs are set before `updateConfigurationFromConfig`, so the server's `<kafka>`
section and named-collection keys still win over them, as they did on 26.3.
`request.required.acks` is always set; its default `-1` equals librdkafka's.

## Concept {#concept}

A new flat setting that maps onto a librdkafka property has to be added to the
named-collection override list, or it silently bypasses that check.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-and-applies`

- Conflicts were context only (new includes and settings around ours).
- New door: upstream's named-collection override check; covered as above.

## Customer impact {#customer-impact}

- Same settings as 26.3.
- New versus 26.3: on a table built on a named collection, replacing a stored compression
  key with the old `kafka_producer_compression_*` names needs `SHOW NAMED COLLECTIONS SECRETS`
  and fails on a `NOT OVERRIDABLE` key, like upstream's `kafka_compression_*`.
- Upstream tests edited: none.

## Security review {#security-review}

The patch adds settings that override named-collection keys.

- **Threat model:** a user allowed to use a named collection but not to see or change its
  secrets replaces a key the owner fixed.
- **Ways in:**

  | Path | Kind | Covered by |
  |---|---|---|
  | `Kafka(nc) SETTINGS kafka_producer_compression_*` | table settings over a named collection | `getKafkaProperty` entries |
  | `Kafka(nc) SETTINGS kafka_producer_<knob>` | table settings over a named collection | not an override: applied before the collection |

- The producer settings are not listed: the collection's keys win over them.
- The settings carry no secrets.

## Tests {#tests}

- **Paths:** `aiven_033_kafka_extra_settings.sql`,
  `aiven_033_kafka_extra_settings_named_collection.sh`
- **Proves:** the six settings and two aliases parse; replacing a collection's compression
  key with an alias needs the privilege and respects `NOT OVERRIDABLE`, and a producer
  setting is not treated as an override. Without the patch the
  first fails with `UNKNOWN_SETTING`.
- **Run:** `./tests/clickhouse-test aiven_033_`
- **Evidence:** PASS on the 26.8 lane build (debug, local arm64 and Buildkite amd_debug). On the parent `CREATE` fails with `UNKNOWN_SETTING` (not re-run).

## Rollback {#rollback}

- **Revert:** tables stored with these settings stop attaching.
- **Persistent state:** the settings in stored table definitions.
- **Disable without rebuild:** leave them at defaults.

## Retirement {#retirement}

Upstream adding per-table producer settings; the aliases retire once no stored table uses
the old compression names.

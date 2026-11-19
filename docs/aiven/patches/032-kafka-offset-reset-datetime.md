---
description: 'Dossier for the Aiven patch that adds a per-table Kafka auto.offset.reset setting and keeps the kafka_date_time_input_format alias'
sidebarTitle: '032: Kafka offset reset'
slug: '/aiven/patches/032-kafka-offset-reset-datetime'
title: 'Patch 032: Kafka per-table offset reset and date-time format alias'
doc_type: 'reference'
aiven_identity: '032'
aiven_absorbs: []
aiven_changelog_category: 'Improvement'
aiven_settings:
  - 'kafka_auto_offset_reset (Kafka table setting)'
  - 'kafka_date_time_input_format (Kafka table setting, alias of date_time_input_format)'
---

# Patch 032 — Kafka per-table offset reset and date-time format alias {#patch-032-kafka-offset-reset-datetime}

## Summary {#summary}

The patch adds the `Kafka` table setting `kafka_auto_offset_reset`, which chooses where a
consumer group without a committed offset starts (`earliest` by default, as upstream
hardcodes, or `latest`), and keeps `kafka_date_time_input_format` as an alias of the
format setting `date_time_input_format`. Aiven's integrations expose both and store them
in table definitions. A table that sets neither behaves exactly as upstream.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.8 | `9f769ed901` | original |
| 26.3 | `patch-port(032)` `cc64f730444` | partial: offset reset as an enum; date-time setting kept as an alias, forwarded only when set |
| 26.8 | `patch-port(032)` | `still-needed-and-applies`; clean on top of `029` |

## Background (ClickHouse) {#background}

librdkafka's `auto.offset.reset` decides where a consumer starts on a partition for which
its group has no committed offset. Upstream sets it to `earliest` in
`KafkaConfigLoader::getConsumerConfiguration`, before the server `<kafka>` section and named
collection keys are applied, so those can change it but a table cannot.

## Component tour {#component-tour}

- `src/Core/SettingsEnums.{h,cpp}` — `KafkaAutoOffsetReset`, a standard case-sensitive
  setting enum with librdkafka's six spellings.
- `src/Storages/Kafka/KafkaSettings.{h,cpp}` — the two settings.
- `KafkaConfigLoader::getConsumerConfiguration` — sets `auto.offset.reset` from the table
  instead of the literal `earliest`.
- `StorageKafkaUtils.cpp`, `createSettingsAdjustments` — forwards the alias to
  `date_time_input_format` when the table set it.

## Problem {#problem}

Without the patch a table cannot start a new consumer group at the end of a topic, and
Aiven tables stored with either setting fail to attach with `UNKNOWN_SETTING`.

## Decisions {#decisions}

### Forward the alias only when set {#decision-alias-changed}

- **Chosen:** forward `kafka_date_time_input_format` only if the table set it.
- **Rejected:** always forwarding, as 25.8 did. Its default would overwrite a canonical
  `date_time_input_format` the table also set.

### Not part of the named-collection override check {#decision-no-override-check}

- **Chosen:** do not list `kafka_auto_offset_reset` in `getKafkaProperty`.
- **Why:** it is set before the server config and named collection, so it never replaces
  a collection's key; listing it would refuse statements that change nothing.

## Approach {#approach}

The default `EARLIEST` maps to `earliest`, the value upstream hardcodes.

## Concept {#concept}

In `KafkaConfigLoader`, the order of `conf.set` calls is the precedence: values set before
`updateConfigurationFromConfig` are defaults the config can change; values set after win.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-and-applies`

- Upstream still hardcodes `earliest`.

## Customer impact {#customer-impact}

- Same as 26.3. `kafka_auto_offset_reset` accepts `smallest`, `earliest`, `beginning`,
  `largest`, `latest`, `end` (lowercase only); others fail with `BAD_ARGUMENTS`.
- Upstream tests edited: none.

## Security review {#security-review}

n/a — chooses a start offset and a parsing mode; no access, secrets or new input paths.

## Tests {#tests}

- **Paths:** `tests/queries/0_stateless/aiven_032_kafka_offset_reset_datetime.sql`
- **Proves:** both settings parse, the enum is case-sensitive and rejects junk. Without the
  patch `CREATE` fails with `UNKNOWN_SETTING`. That the consumer honours the value needs a
  broker and is covered by reading the code only.
- **Run:** `./tests/clickhouse-test aiven_032_kafka_offset_reset_datetime`
- **Evidence:** PASS on the 26.8 lane build (debug, local arm64 and Buildkite amd_debug). On the parent `CREATE` fails with `UNKNOWN_SETTING` (not re-run).

## Rollback {#rollback}

- **Revert:** tables stored with either setting stop attaching.
- **Persistent state:** the settings in stored table definitions.
- **Disable without rebuild:** leave the defaults.

## Retirement {#retirement}

Upstream adding a per-table offset-reset setting; the alias retires once no stored table
uses it.

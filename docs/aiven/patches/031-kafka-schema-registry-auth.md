---
description: 'Dossier for the Aiven patch that keeps the kafka_format_avro_schema_registry_url table setting and keeps schema-registry credentials out of logs'
sidebarTitle: '031: Kafka schema registry'
slug: '/aiven/patches/031-kafka-schema-registry-auth'
title: 'Patch 031: Kafka schema-registry URL alias and credential-safe logging'
doc_type: 'reference'
aiven_identity: '031'
aiven_absorbs: []
aiven_changelog_category: 'Improvement'
aiven_settings:
  - 'kafka_format_avro_schema_registry_url (Kafka table setting, alias of format_avro_schema_registry_url)'
---

# Patch 031 — Kafka schema-registry URL alias and credential-safe logging {#patch-031-kafka-schema-registry-auth}

## Summary {#summary}

The patch keeps `kafka_format_avro_schema_registry_url` as a `Kafka` table setting that
feeds the upstream format setting `format_avro_schema_registry_url`, masks its credentials
wherever the canonical name is masked, and strips `user:password@` from the registry URL in
the schema-fetch trace log. Aiven's integrations store the `kafka_` name in table
definitions, so without it those tables would not attach. Operators see no credentials in
`SHOW CREATE`, `system.tables` or server logs.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.8 | `7d48793fc5` | original: per-table URL, basic auth, log stripping |
| 26.3 | `patch-port(031)` `b8faa0019e9` | partial: auth dropped (upstream does it better), URL kept as an alias, log stripping kept |
| 26.8 | `patch-port(031)` | `still-needed-but-rewrite`: log fix moved to `AvroConfluentSchemaRegistry.cpp`; alias added to the Kafka secret-masking list |

## Background (ClickHouse) {#background}

`AvroConfluent` reads a schema id from each message and fetches the schema from a
Confluent-compatible registry at `format_avro_schema_registry_url`, which may carry basic
auth as `user:password@`. A `Kafka` table accepts any format setting in its `SETTINGS`
clause and passes it to its input format. Settings that hold secrets are masked when a
statement is printed: query-level settings through `CoreSettings::SETTINGS_TO_HIDE`, engine
settings through each engine's `SETTINGS_TO_HIDE` (for Kafka, `Kafka_fwd.h`).

## Component tour {#component-tour}

- `src/Storages/Kafka/KafkaSettings.cpp` declares the alias; `createSettingsAdjustments` in
  `StorageKafkaUtils.cpp` forwards it to `format_avro_schema_registry_url`. The generic
  pass-through skips `kafka_*` names, so the forward is explicit and runs after it.
- `src/Storages/Kafka/Kafka_fwd.h`, `Kafka::SETTINGS_TO_HIDE` — read by `ASTSetQuery` when a
  statement is formatted for logs, `SHOW CREATE` and `system.tables`.
- `src/Processors/Formats/Impl/AvroConfluentSchemaRegistry.cpp`,
  `ConfluentSchemaRegistry::fetchSchema` — the HTTP fetch; on 26.8 this code moved out of
  `AvroRowInputFormat.cpp`.

## Problem {#problem}

Aiven integration tables are stored with `kafka_format_avro_schema_registry_url`; upstream
does not know that name and `ATTACH` fails with `UNKNOWN_SETTING`. Separately, upstream
logs the full registry URL, credentials included, at trace level on every schema fetch.

## Decisions {#decisions}

### Mask the alias like the canonical setting {#decision-mask-alias}

- **Chosen:** add the alias to `Kafka::SETTINGS_TO_HIDE`, delegating to upstream's masking of
  `format_avro_schema_registry_url`, so only the credentials are hidden.
- **Rejected:** hiding the whole value. The canonical name shows the host, and the two
  names should look the same.
- **Rung:** runtime guard in a list we extend (rung 4); the alias itself is rung 5.

### Fix the log line here, offer it upstream {#decision-log-upstream}

- **Chosen:** strip user info in the `LOG_TRACE`; it is a generic bug fix and a candidate
  for an upstream pull request (rung 2), which would retire this hunk.

## Approach {#approach}

The alias is forwarded only when non-empty. If a table sets both names, the alias wins
because it is appended last. The fetch still uses the full URL; only the logged copy loses
its user info.

## Concept {#concept}

Every name under which a secret can be written needs its own entry in the masking lists;
an alias is a second name.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-but-rewrite`

- The schema-registry client moved to `AvroConfluentSchemaRegistry.cpp`; the log line is
  unchanged there.
- Upstream now masks `format_avro_schema_registry_url` credentials in printed statements,
  but not the `kafka_` alias, which this port adds.

## Customer impact {#customer-impact}

- `kafka_format_avro_schema_registry_url` works in `Kafka` table `SETTINGS`, same as 26.3.
- New versus 26.3: its credentials show as `[HIDDEN]` in `SHOW CREATE`, `system.tables`
  and `system.query_log`.
- Upstream tests edited: none.

```sql
CREATE TABLE t (key UInt64) ENGINE = Kafka SETTINGS ...,
    kafka_format_avro_schema_registry_url = 'http://user:secret@registry:8081';
SELECT create_table_query FROM system.tables WHERE name = 't';
-- Before (26.3): ... kafka_format_avro_schema_registry_url = 'http://user:secret@registry:8081'
-- After:        ... kafka_format_avro_schema_registry_url = 'http://[HIDDEN]@registry:8081'
```

## Security review {#security-review}

The patch hides a credential in output.

- **Threat model:** someone who can read server logs, `system.query_log` or a table
  definition learns the registry password.
- **Ways in:**

  | Path | Kind | Covered by |
  |---|---|---|
  | `SHOW CREATE`, `system.tables`, `system.query_log` | printed statement | `Kafka::SETTINGS_TO_HIDE` |
  | schema fetch trace log | server log | stripped user info |
  | `ENGINE = Kafka(nc, kafka_format_avro_schema_registry_url = ...)` | engine argument | not masked, same as upstream for the canonical name |

- **Accepted risks:** the engine-argument form is not masked for either name; tracked.

## Tests {#tests}

- **Paths:** `tests/queries/0_stateless/aiven_031_kafka_schema_registry_auth.sql`
- **Proves:** the alias is accepted and its password does not appear in
  `system.tables.create_table_query`. Without the patch `CREATE` fails with
  `UNKNOWN_SETTING`. The log fix is covered by reading the code.
- **Run:** `./tests/clickhouse-test aiven_031_kafka_schema_registry_auth`
- **Evidence:** PASS on the 26.8 lane build (debug, local arm64 and Buildkite amd_debug). On the parent `CREATE` fails with `UNKNOWN_SETTING` (not re-run).

## Rollback {#rollback}

- **Revert:** tables stored with the alias stop attaching; do not revert on a fleet with
  Avro integrations.
- **Persistent state:** the alias in stored table definitions.
- **Disable without rebuild:** none.

## Retirement {#retirement}

The alias retires once the control plane writes `format_avro_schema_registry_url` and no
stored table uses the old name. The log fix retires when upstream accepts it.

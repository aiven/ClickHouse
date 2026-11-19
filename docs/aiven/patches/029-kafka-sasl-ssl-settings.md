---
description: 'Dossier for the Aiven patch that types the Kafka engine security settings as enums and adds the SSL file and endpoint-identification settings'
sidebarTitle: '029: Kafka SASL/SSL settings'
slug: '/aiven/patches/029-kafka-sasl-ssl-settings'
title: 'Patch 029: Kafka SASL and SSL settings as enums'
doc_type: 'reference'
aiven_identity: '029'
aiven_absorbs: []
aiven_changelog_category: 'Improvement'
aiven_settings:
  - 'kafka_ssl_endpoint_identification_algorithm (Kafka table setting)'
  - 'kafka_ssl_ca_location (Kafka table setting)'
  - 'kafka_ssl_certificate_location (Kafka table setting)'
  - 'kafka_ssl_key_location (Kafka table setting)'
---

# Patch 029 — Kafka SASL and SSL settings as enums {#patch-029-kafka-sasl-ssl-settings}

## Summary {#summary}

The patch turns the `Kafka` engine settings `kafka_security_protocol` and
`kafka_sasl_mechanism` from free strings into enums, and adds four per-table TLS settings:
`kafka_ssl_ca_location`, `kafka_ssl_certificate_location`, `kafka_ssl_key_location` and
`kafka_ssl_endpoint_identification_algorithm`. Aiven's Kafka integrations write these
settings into every integration table, so a table needs no server-level Kafka config to
talk TLS to its own broker. Bad values fail at `CREATE` with `BAD_ARGUMENTS`, and a table
that does not set them gets `PLAINTEXT`, `GSSAPI` and no broker host-name check. Upstream's AWS MSK IAM
login, which would sign with the server's own AWS identity, is refused.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.3 | enums on `v25.3.14.14-lts-aiven` | original; SSL locations always set |
| 25.8 | `f45b8fb6fb` | SSL locations set only when non-empty |
| 26.3 | `patch-port(029)` `f29a00cdc2d` | `still-needed-but-rewrite`: upstream had grown string `kafka_security_protocol`/`kafka_sasl_mechanism`; flipped back to enums |
| 26.8 | `patch-port(029)` | `still-needed-but-rewrite`: 26.3 design kept; upstream's new `AWS_MSK_IAM` refused (not in the enum, rejected from server config and named collections); the `kafka_ssl_*` settings registered with the new named-collection override check |

## Background (ClickHouse) {#background}

A `Kafka` table builds a librdkafka configuration from three layers, applied in order by
`updateConfigurationFromConfig`: the server's `<kafka>` config section (and a named
collection's nested `kafka.*` keys), then the table's own `kafka_*` settings. librdkafka
reads `security.protocol`, `sasl.mechanism`, `ssl.*.location` and
`ssl.endpoint.identification.algorithm` (`https` checks the broker's certificate against
its host name; `none` only checks the chain). librdkafka's own default for the last one
is `https`.

## Component tour {#component-tour}

- `src/Core/SettingsEnums.{h,cpp}` — the three enums with hand-written traits: parsing is
  case-insensitive, and `SASL-SSL`/`SASL-PLAINTEXT` are accepted for the underscore forms.
- `src/Storages/Kafka/KafkaSettings.{h,cpp}` — the setting declarations and the enum
  types registered with the Kafka settings class.
- `src/Storages/NamedCollectionsHelpers.cpp`, `getKafkaProperty` — maps each flat
  `kafka_ssl_*` setting to its librdkafka property, so replacing a named collection's nested
  key needs `SHOW NAMED COLLECTIONS SECRETS` and respects `NOT OVERRIDABLE`.
- `src/Storages/Kafka/KafkaConfigLoader.cpp`, `updateConfigurationFromConfig` — the
  storage-layer step that turns table settings into librdkafka properties for both
  consumers and producers, for both `StorageKafka` and `StorageKafka2`.

## Problem {#problem}

Upstream has `kafka_security_protocol` and `kafka_sasl_mechanism` only as strings, and
no per-table TLS file or host-name settings at all. Without the patch every Aiven Kafka
integration table using TLS would fail at `ATTACH` with `UNKNOWN_SETTING`, because the
control plane stores `kafka_ssl_*` settings in the table definition. Typos in the
protocol or mechanism would only surface at connection time.

## Decisions {#decisions}

### Keep the 26.3 enum design for now {#decision-keep-enums}

- **Chosen:** carry 26.3's enums and its "always apply" wiring unchanged, including the
  `none` default for host-name checking (Aiven, 2026-11-16).
- **Rejected for this port:** upstream's string settings plus validation at `CREATE`,
  applied only when the table sets them. That is the smaller merge surface and keeps
  server-config and named-collection values; it is to be reconsidered, see
  [Retirement](#retirement).
- **Rung:** edit to an upstream hot path (rung 5). Config (rung 1) cannot carry per-table
  certificates.

### Register the TLS settings with the named-collection override check {#decision-override-check}

- **Chosen:** list the four `kafka_ssl_*` settings in `getKafkaProperty`.
- **Rejected:** leaving them out. A table could then replace a collection's CA, client
  certificate or host-name check without the privilege, and past `NOT OVERRIDABLE`.
- **Note:** this covers settings a table names. The always-applied defaults still replace
  collection values without a check (see Drift).

### Refuse AWS MSK IAM {#decision-aws-msk-iam}

- **Chosen:** Aiven does not offer AWS MSK IAM (Aiven, 2026-11-16). The mechanism enum has
  no `AWS_MSK_IAM`, so a table or a named collection's flat `kafka_sasl_mechanism` fails at
  `CREATE`. A nested `kafka.sasl_mechanism` or server-config value of `AWS_MSK_IAM` that the
  table does not override is refused with `SUPPORT_IS_DISABLED` when the configuration is
  built, and upstream's MSK setup (`AWSMSKIAMAuth::setupAuthentication`) is no longer called.
- **Why:** MSK IAM signs tokens with the server's own AWS credentials (environment, `~/.aws`,
  instance profile, ECS, web identity), gated only by `kafka.use_environment_credentials`.
  There is no per-table or per-user credential, so on Aiven it either cannot authenticate or,
  with that switch on, would act as Aiven's VM role.
- **Layers:** the enum, the refusal at the single place the configuration is built, and the
  switch left off in managed configuration, so no single key enables it.
- **Rejected:** keeping it working, as upstream does. The control plane does not offer it,
  and it is one config key away from a privilege escalation.
- **Rung:** runtime guard in code we already own here (rung 4); the enum is the patch itself.
  Revisit only as a product feature with per-customer credentials.

## Approach {#approach}

`security.protocol`, `sasl.mechanism` and
`ssl.endpoint.identification.algorithm` are always set from the table, so their defaults
win over the server config. The three `ssl.*.location` properties are set only when
non-empty. `sasl.username` and `sasl.password` are unchanged from upstream.

## Concept {#concept}

A typed setting with a default is never "unset": if it is always applied, its default
silently beats every lower configuration layer. Prefer applying only `.changed` values
when lower layers exist.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-but-rewrite`

- New door: upstream added `AWS_MSK_IAM` as a mechanism, read from the table, a named
  collection or the server config in `updateConfigurationFromConfig`; refused, see Decisions.
- Upstream added a check that overriding a named collection's keys from table settings
  needs `SHOW NAMED COLLECTIONS SECRETS` (`NamedCollectionsHelpers.cpp`). It assumes a
  table setting overrides only when the table sets it. With this patch the defaults of
  `kafka_security_protocol`, `kafka_sasl_mechanism` and
  `kafka_ssl_endpoint_identification_algorithm` replace a collection's nested
  `kafka.security_protocol`, `kafka.sasl_mechanism` and
  `kafka.ssl_endpoint_identification_algorithm` even when the table does not mention
  them. The same holds on 26.3.

## Customer impact {#customer-impact}

- Tables may set `kafka_ssl_ca_location`, `kafka_ssl_certificate_location`,
  `kafka_ssl_key_location` and `kafka_ssl_endpoint_identification_algorithm`.
- `kafka_security_protocol`, `kafka_sasl_mechanism` and
  `kafka_ssl_endpoint_identification_algorithm` reject unknown values at `CREATE` with
  `BAD_ARGUMENTS` and a list of allowed values.
- A table that leaves them unset uses `PLAINTEXT`, `GSSAPI` and `none` even when the
  server config or its named collection says otherwise.
- New versus upstream 26.8: `AWS_MSK_IAM` fails at `CREATE` (`BAD_ARGUMENTS`) or, when it
  comes from a nested named-collection key or the server config, on first use with
  `SUPPORT_IS_DISABLED` ("AWS MSK IAM authentication is not available on this service").
- Same as 26.3 otherwise. Upstream tests edited: none; `04218_kafka_aws_msk_iam_auth_settings`
  fails by design and is not part of the Aiven test lanes.

```sql
CREATE TABLE t (v String) ENGINE = Kafka SETTINGS kafka_broker_list = 'localhost:19092',
    kafka_topic_list = 't', kafka_group_name = 'g', kafka_format = 'CSV',
    kafka_security_protocol = 'NOPE';
-- Before: Ok. (fails later, at connection)
-- After:  Code: 36. DB::Exception: Unexpected value of KafkaSecurityProtocol: 'NOPE'. Must be one of ['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL']. (BAD_ARGUMENTS)
```

## Security review {#security-review}

The patch configures TLS and authentication for outgoing broker connections.

- **Threat model:** a network attacker between the server and a Kafka broker. Without a
  host-name check, any certificate from the trusted CA is accepted for any broker. Second, a
  tenant who can create a `Kafka` table choosing AWS MSK IAM, which signs with the server's
  AWS identity; this patch refuses it.
- **Identity:** the connection authenticates as the table's SASL user or client
  certificate; the patch adds file paths for the latter, read with the server's
  permissions.
- **Ways in:**

  | Path | Kind | Covered by |
  |---|---|---|
  | `CREATE`/`ATTACH TABLE ... ENGINE = Kafka SETTINGS ...` | table settings | enum validation; applied in `updateConfigurationFromConfig` |
  | `Kafka(nc) SETTINGS kafka_ssl_* = ...` | explicit override of a named collection | `getKafkaProperty` entries: privilege and `NOT OVERRIDABLE` |
  | `ENGINE = Kafka(named_collection)` without the settings | named collection | defaults override nested keys (see Drift) |
  | `<kafka>` server config | config | overridden by table defaults |
  | `kafka_sasl_mechanism = 'AWS_MSK_IAM'` (table or flat collection key) | MSK IAM | enum: `BAD_ARGUMENTS` at `CREATE` |
  | nested `kafka.sasl_mechanism` / `<kafka><sasl_mechanism>` = `AWS_MSK_IAM` | MSK IAM | `SUPPORT_IS_DISABLED` when the configuration is built |

- **Default:** host-name checking is off unless the table sets `https`. The control plane
  always sets the value explicitly, so the default affects hand-made tables only.
- **Untrusted input:** `kafka_ssl_*_location` are server file paths taken from SQL and
  passed to librdkafka unchecked; any user who can create a `Kafka` table can point them
  at any file the server can read. librdkafka only parses them as PEM, and errors do not
  echo file contents.
- **Accepted risks:** the `none` default and the override of named-collection values,
  both kept from 26.3 by decision and listed for review in the tracker.

## Tests {#tests}

- **Paths:** `tests/queries/0_stateless/aiven_029_kafka_sasl_ssl_settings.sql`,
  `aiven_029_kafka_sasl_ssl_settings_named_collection.sh` (override needs the privilege;
  `NOT OVERRIDABLE` holds), `aiven_029_kafka_aws_msk_iam_refused.sh` (MSK IAM refused from a
  named collection, flat and nested; the server-config door shares the nested key's code path)
- **Proves:** the four `kafka_ssl_*` settings exist, enum values parse case-insensitively,
  `aws_msk_iam` and other invalid values fail at `CREATE`. Without the patch the
  first `CREATE` fails with `UNKNOWN_SETTING`.
- **Run:** `./tests/clickhouse-test aiven_029_`
- **Evidence:** PASS on the 26.8 lane build (debug, local arm64 and Buildkite amd_debug). On the parent the first `CREATE`
  fails with `UNKNOWN_SETTING` (not re-run). Upstream `05317_named_collection_kafka_nested_override_access`
  passes on the local build; `04218_kafka_aws_msk_iam_auth_settings` fails by design.

## Rollback {#rollback}

- **Revert:** a table stored with a `kafka_ssl_*` setting then fails to attach with
  `UNKNOWN_SETTING`; the control plane writes them, so do not revert on a fleet with TLS
  integrations.
- **Persistent state:** the settings in stored table definitions.
- **Disable without rebuild:** none.

## Retirement {#retirement}

To reconsider on this line: switch to upstream's string settings with validation at
`CREATE` and apply only values the table sets. That restores named-collection and
server-config precedence, and keeps every name the control plane writes. The patch
retires fully if upstream adds the `kafka_ssl_*` table settings.

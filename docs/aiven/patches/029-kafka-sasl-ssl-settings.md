# Patch 029 — kafka-sasl-ssl-settings

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | enums present on `v25.3.14.14-lts-aiven` (`ca`/`cert`/`key` set *unconditionally*) | Tilman Moeller / Kevin Michel | (earlier variant) |
| 25.8-aiven | `f45b8fb6fb` (author Tilman Moeller, co-author Kevin Michel, 2025-12-22) | — | (the version we are porting FROM) |
| 26.3-aiven | `patch-port(029)` (STAGED — HEAD unmoved at `94dd391d108`; see inventory) | parent agent, 2026-06-04 | **`still-needed-but-rewrite`** — faithful enum port, two adaptations |

## 1. Purpose

Carry the Aiven Kafka SASL/SSL configuration surface as **enum-typed settings**:

- `kafka_security_protocol` — enum `KafkaSecurityProtocol` (`PLAINTEXT`/`SSL`/`SASL_PLAINTEXT`/`SASL_SSL`), default `PLAINTEXT`.
- `kafka_sasl_mechanism` — enum `KafkaSASLMechanism` (`GSSAPI`/`PLAIN`/`SCRAM-SHA-256`/`SCRAM-SHA-512`/`OAUTHBEARER`), default `GSSAPI`.
- `kafka_ssl_endpoint_identification_algorithm` — enum `KafkaSSLEndpointIdentificationAlgorithm` (`none`/`https`), default `NONE`.
- `kafka_ssl_ca_location`, `kafka_ssl_certificate_location`, `kafka_ssl_key_location` — `String` file paths (empty default).

The enum traits implement **case-insensitive** `fromString` (and, for the security
protocol, hyphen→underscore tolerance: `SASL-SSL` → `SASL_SSL`). The values are
mapped to the `librdkafka` property names in `KafkaConfigLoader` via
`toCppKafkaString` overloads (lowercase for `security.protocol` /
`ssl.endpoint.identification.algorithm`; verbatim uppercase for `sasl.mechanism`).

`security.protocol`, `sasl.mechanism`, and `ssl.endpoint.identification.algorithm`
are **always** applied to the `cppkafka::Configuration`; the three SSL file
locations are applied **only when non-empty**.

## 2. Upstream-drift / validity findings — why this is NOT obsoleted-by-upstream

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`.

Upstream 26.3 *does* now ship `kafka_security_protocol` and `kafka_sasl_mechanism`
— but as plain `String` settings (`KafkaSettings.cpp:48-49`), wired with an
empty-default + conditional `set` at `KafkaConfigLoader.cpp:364-367`. It does **not**
ship `kafka_ssl_endpoint_identification_algorithm` or the three `ssl_*_location`
settings at all.

A plain `String` is **not a behavior-equivalent replacement** for the Aiven enum
contract that both 25.3 and 25.8 shipped:

1. **DDL-time validation.** The enum rejects a bad value at `CREATE TABLE` with a
   helpful `BAD_ARGUMENTS` message listing the allowed values. The `String` accepts
   anything at DDL and defers rejection to `librdkafka` at consumer/producer
   creation (connection time). Verified against the patched binary — see §4.
2. **Normalization.** The enum `fromString` is case-insensitive and maps
   `SASL-SSL`/`SASL-PLAINTEXT` (hyphen) to the underscore form `librdkafka`
   expects. The `String` passes the value through verbatim.
3. **Default application.** The enum defaults `ssl.endpoint.identification.algorithm`
   to `none` and applies it **unconditionally**. `librdkafka`'s own default is
   `https` (`contrib/librdkafka/src/rdkafka_conf.c:890`, `.vdef =
   RD_KAFKA_SSL_ENDPOINT_ID_HTTPS`), so a `String` with an empty default would
   silently leave hostname verification **on** — a behavior change for existing
   Aiven deployments that rely on `none` (self-signed / `::1` brokers).

Because both 25.3 and 25.8 shipped the enum, the enum *is* the user-facing contract
Aiven has carried across two LTS lines; preserving it is a backward-compatibility
requirement ("the 25.8 settings interface cannot change from outside"), not a
preference. Disposition: **`still-needed-but-rewrite`** (faithful enum port).

### librdkafka cross-check (informs the design)

- `security.protocol`, `sasl.mechanism`, `ssl.endpoint.identification.algorithm` are
  `_RK_C_S2I` enum properties whose value match is **case-insensitive**
  (`rdkafka_conf.c:1890`, `rd_strcasecmp`). So the `toCppKafkaString` lowercasing is
  defensive, not strictly required — any casing the enum emits would be accepted.
- The s2i tables store lowercase tokens (`"none"`, `"https"`, `"plaintext"`,
  `"sasl_ssl"`, …); the hyphen form is *not* a valid `librdkafka` token, which is
  why the enum's hyphen→underscore normalization is a genuine convenience the
  `String` path lacks.

## 3. C++ / security review

- **Type-flip blast radius is self-contained.** The only 26.3 C++ that reads
  `kafka_security_protocol` / `kafka_sasl_mechanism` is `KafkaConfigLoader.cpp`
  (extern decls + wiring). Flipping `String → enum` there + the settings machinery
  is the whole change; no other reader depends on these being `String`.
- **`endpoint = none` default preserved deliberately.** This keeps TLS **hostname
  verification off by default** for the SSL/`SASL_SSL` paths — matching 25.3/25.8.
  This is an intentional behavior-preservation choice (the original patch comment:
  avoid hostname-verification failure for self-signed / `::1` certs), flagged here
  so it is a conscious posture, not an oversight. Operators that want verification
  set `kafka_ssl_endpoint_identification_algorithm = 'https'`.
- **No new untrusted-input parsing.** Enum `fromString` runs on operator/DDL input
  at `CREATE TABLE` time, not on query data. It throws `BAD_ARGUMENTS` on unknown
  values (a tightening relative to the permissive `String`).
- **Secrets unaffected.** `sasl.username`/`sasl.password` remain `String`, applied
  only when non-empty; masking behavior (`test_mask_sensitive_info`) is unchanged.

## 4. Test design

Stateless `.sql` test
`tests/queries/0_stateless/9029_kafka_sasl_ssl_settings.sql` (Aiven `9<NNN>_<slug>`
convention; `NNN = 029`). `Kafka` DDL with `kafka_num_consumers = 0` does not
connect to a broker, so the test is deterministic without one.

It asserts three things:

1. Canonical enum values + the three SSL file locations are accepted at `CREATE`.
2. Case/hyphen variants normalize and are accepted (`sasl-ssl`, `scram-sha-512`,
   `HTTPS`).
3. Invalid enum values are **rejected** at DDL with `serverError BAD_ARGUMENTS`
   (`kafka_ssl_endpoint_identification_algorithm = 'bogus'` and
   `kafka_security_protocol = 'NOPE'`) — the guardrail a plain `String` would not
   provide, so it is the key behavioral evidence for choosing the enum.

### Evidence (against the patched binary, 2026-06-04)

The four query behaviors the test asserts were verified directly against the freshly
built (patched) `clickhouse` binary via `clickhouse local` (same engine + settings
code path as the server):

```
canonical values        -> ok
case/hyphen variants     -> variants ok
endpoint = 'bogus'       -> Code: 36 ... Unexpected value of KafkaSSLEndpointIdentificationAlgorithm: 'bogus'. Must be one of ['none', 'https']. (BAD_ARGUMENTS)
security_protocol='NOPE' -> Code: 36 ... Unexpected value of KafkaSecurityProtocol: 'NOPE'. Must be one of ['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL']. (BAD_ARGUMENTS)
```

The `clickhouse-test` harness will run the same `.sql` in CI. A throwaway local
server was *not* used for the harness run: 26.3's default `programs/server/config.xml`
fails to initialize the temporary database disk (`DiskSelector` /
`database_disk`) under an ad-hoc `--path`, which the CI harness handles with its
bespoke test configs. This is unrelated to the patch; the `clickhouse local`
evidence above exercises the identical settings-validation path.

## 5. Rollback considerations

Five files, one cohesive change. To revert: drop the three enums + traits from
`SettingsEnums.{h,cpp}`, remove the three `M(CLASS_NAME, …)` registrations in
`KafkaSettings.h`, restore the two `String` DECLAREs and remove the four SSL
DECLAREs in `KafkaSettings.cpp`, and restore the upstream extern decls + wiring
block in `KafkaConfigLoader.cpp`. No submodule, schema, or cross-patch coupling.
Reverting only `KafkaConfigLoader.cpp` while keeping the enum settings would leave
the enum types unwired (still validated at DDL, but not applied) — revert together.

## 6. Per-uplift notes

### 25.3-aiven (historical)

Enums present on `v25.3.14.14-lts-aiven`. Difference from 25.8: the three SSL file
locations were set **unconditionally** (passing empty strings to `librdkafka`), and
the enum→string conversion used `SettingFieldKafka…::toString` directly rather than
a lowercasing helper.

### 25.8-aiven (the version ported FROM)

Source `f45b8fb6fb`. Refines 25.3: makes `ssl.ca/certificate/key.location`
**conditional** on non-empty, and introduces the `toCppKafkaString` overloads
(lowercasing `security.protocol` / endpoint algorithm). It plumbed the values
through a `KafkaConfigLoader::LoadConfigParams` struct, touching
`StorageKafka.cpp` / `StorageKafka2.cpp` / `KafkaConfigLoader.h`.

### 26.3-aiven (this uplift)

`still-needed-but-rewrite`. The enum machinery + traits are carried **verbatim** from
`f45b8fb6fb`. Two deliberate adaptations:

1. **Direct-settings wiring (no params-struct re-plumb).** 26.3's
   `updateGlobalConfiguration` already reads `storage.getKafkaSettings()` directly,
   so the enum/SSL values are read there. `StorageKafka.cpp`, `StorageKafka2.cpp`,
   and `KafkaConfigLoader.h` are **not touched** — smaller, safer diff with the same
   runtime behavior.
2. **25.8 logic (conditional SSL locations), not 25.3.** `ssl.ca/certificate/key`
   are applied only when non-empty; `security.protocol` / `sasl.mechanism` /
   `ssl.endpoint.identification.algorithm` are always applied.

Files changed: `src/Core/SettingsEnums.h`, `src/Core/SettingsEnums.cpp`,
`src/Storages/Kafka/KafkaSettings.h`, `src/Storages/Kafka/KafkaSettings.cpp`,
`src/Storages/Kafka/KafkaConfigLoader.cpp`. Full `clickhouse` build exit 0 (the
`SettingsEnums.h` edit forces a wide recompile of its includers). Verified via the
`clickhouse local` evidence in §4 and stateless test `9029_kafka_sasl_ssl_settings`.

# Patch 031 — kafka-schema-registry-auth

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `7d48793fc5` (author Tilman Moeller, co-author Kevin Michel, 2025-12-22) | — | (the version we are porting FROM) |
| 26.3-aiven | `patch-port(031)` (STAGED — HEAD unmoved at `94dd391d108`, commit deferred to maintainer) | parent agent, 2026-06-05 | **`partial`** — Legs A+B obsoleted-by-upstream; Leg A re-added as a backward-compat alias by request; Leg C (log credential-stripping) is the only genuine residual |

## 1. Purpose (original patch)

Three legs:

- **Leg A — per-table schema registry URL.** Add a `kafka_format_avro_schema_registry_url`
  Kafka setting and propagate it to the `format_avro_schema_registry_url` format
  setting via `createSettingsAdjustments`, so each `Kafka` table can point at its
  own Confluent schema registry.
- **Leg B — schema registry basic auth.** In
  `AvroConfluentRowInputFormat::SchemaRegistry::fetchSchema`, parse `user:password`
  from the registry URL's user-info and attach `Poco::Net::HTTPBasicCredentials` to
  the HTTP request.
- **Leg C — credential-safe logging.** Strip the user-info from the registry URL
  before it is written to the `LOG_TRACE` "Fetching schema id …" message.

## 2. Upstream-drift / validity findings

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`.

- **Leg A — `obsoleted-by-upstream` (functionally).** 26.3's `KafkaSettings` folds
  `LIST_OF_ALL_FORMAT_SETTINGS` into the engine's settings list
  (`KafkaSettings.cpp:70`), and `getFormatSettings` forwards every non-`kafka_`-prefixed
  setting into `createSettingsAdjustments` (`StorageKafkaUtils.cpp:524-525`). So a
  user can already set the canonical `format_avro_schema_registry_url` directly in a
  `Kafka` table's `SETTINGS` clause (it is a real format setting,
  `FormatFactorySettings.h:825`, type `URI`). The bespoke `kafka_`-prefixed setting
  is redundant for *new* tables.
- **Leg B — `obsoleted-by-upstream`, and upstream is superior.** HEAD's
  `AvroRowInputFormat.cpp:1109-1127` already parses `user:password` from the registry
  URL and authenticates — and it additionally **URL-decodes** the username/password
  (`Poco::URI::decode`, via `HTTPCredentials::fromUserInfo`), which the Aiven patch's
  raw `find(':')` did not. Porting the patch would regress this. **Dropped.**
- **Leg C — `still-needed`.** HEAD still logs the full URL **including credentials**
  at `LOG_TRACE` (`AvroRowInputFormat.cpp:1095`, `url.toString()`). Upstream did not
  carry the patch's sanitization. A registry URL with embedded `user:pass@` therefore
  leaks into logs at trace level. This is the only genuine residual.

### Backward-compatibility decision (Leg A re-added as an alias)

Although Leg A is functionally redundant, on 26.3 `kafka_format_avro_schema_registry_url`
is **not** a declared Kafka setting, so a 25.3/25.8 table that carries that
`kafka_`-prefixed name in its stored DDL would fail to parse on 26.3 with
`UNKNOWN_SETTING` on `ATTACH`/`CREATE`. To preserve the user-facing settings interface
("settings from 25.8 cannot change from outside"), Leg A is re-added as a thin
**backward-compat alias** that forwards to the canonical format setting.

## 3. C++ / security review

- **Leg A alias is a minimal, self-contained shim.** Because `getFormatSettings`
  skips `kafka_`-prefixed names, the alias is forwarded explicitly in
  `createSettingsAdjustments` (`emplace_back("format_avro_schema_registry_url", …)`),
  placed *after* the generic passthrough so the explicit alias wins if a table somehow
  sets both names. **The original patch's `StorageKafka::format_avro_schema_registry_url`
  member was vestigial** (write-only; propagation runs through `createSettingsAdjustments`
  reading the setting directly, not the member) — confirmed absent from later 25.8
  sources — so it is intentionally **not** carried (no dead code).
- **Leg C is defense-in-depth, not a functional gate.** It only changes a log string;
  the request URL is unchanged. No behavior depends on it.
- **Leg B not touched.** Upstream owns the auth path; no new attack surface from this
  port.

## 4. Test design

Stateless `.sql` test
`tests/queries/0_stateless/9031_kafka_schema_registry_url_alias.sql` (Aiven
`9<NNN>_<slug>` convention; `NNN = 031`). `Kafka` DDL with
`kafka_num_consumers = 0` does not connect to a broker, so it is deterministic.

- Asserts the backward-compat alias `kafka_format_avro_schema_registry_url` is
  accepted at `CREATE TABLE`.
- **Leg C** (credential-stripped logging) is covered by **inspection** (§3), not by
  this test — exercising it would require a live schema registry fetch plus a
  trace-log scrape, which a stateless test cannot do deterministically.

### Evidence (against the patched binary, 2026-06-05)

Verified via `clickhouse local` (same engine + settings code path as the server):

```
kafka_format_avro_schema_registry_url = '…'  -> alias accepted
format_avro_schema_registry_url       = '…'  -> canonical accepted (generic passthrough)
```

The `clickhouse-test` harness runs the same `.sql` in CI (the throwaway local server
used for other patches is unnecessary; the `clickhouse local` path exercises the
identical DDL/settings code). Full `clickhouse` build exit 0.

## 5. Rollback considerations

Three files, two cohesive changes. Leg A alias: drop the `DECLARE` in
`KafkaSettings.cpp` and the extern + `emplace_back` in `StorageKafkaUtils.cpp` (this
removes the `kafka_`-prefixed name; the canonical `format_avro_schema_registry_url`
continues to work). Leg C: revert the `sanitized_url` block in `AvroRowInputFormat.cpp`
(re-introduces the credential-in-log leak — do not revert unless intended). No
submodule, schema, or cross-patch coupling.

## 6. Per-uplift notes

### 25.8-aiven (the version ported FROM)

Source `7d48793fc5`: all three legs, plus a vestigial `StorageKafka` member. Notably,
25.8 already had the generic format-settings passthrough
(`LIST_OF_ALL_FORMAT_SETTINGS` + `getFormatSettings`), so the canonical
`format_avro_schema_registry_url` likely already worked there too; the `kafka_`-prefixed
setting was the Aiven-blessed name.

### 26.3-aiven (this uplift)

`partial`. **Leg B dropped** (upstream superior). **Leg A re-added as a
backward-compat alias** (per maintainer decision), minus the vestigial member.
**Leg C carried** (the residual credential-hygiene fix). Files changed:
`src/Storages/Kafka/KafkaSettings.cpp`, `src/Storages/Kafka/StorageKafkaUtils.cpp`,
`src/Processors/Formats/Impl/AvroRowInputFormat.cpp`. Build exit 0; verified via the
`clickhouse local` evidence in §4 and stateless test
`9031_kafka_schema_registry_url_alias`.

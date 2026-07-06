# Patch 033 — kafka-extra-settings

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `6384eccfac` (author Tilman Moeller, 2025-12-23) | — | (the version we are porting FROM) |
| 26.3-aiven | `patch-port(033)` (STAGED — HEAD unmoved at `94dd391d108`, commit deferred to maintainer) | parent agent, 2026-06-05 | **`partial`** — Leg 1 (six producer throughput knobs) still-needed and rewritten the 26.3 way; Leg 2 (compression) obsoleted-by-upstream but kept as backward-compat aliases (no enum) |

## 1. Purpose (original patch)

The original commit added per-table Kafka **producer** tuning knobs to improve
write throughput from the `Kafka` table engine, plus a typed compression codec.
Two cohesive legs:

- **Leg 1 — six producer throughput knobs.** New `UInt64`/`Int64` settings that
  map to librdkafka producer properties:
  - `kafka_producer_batch_size` → `batch.size`
  - `kafka_producer_batch_num_messages` → `batch.num.messages`
  - `kafka_producer_linger_ms` → `linger.ms`
  - `kafka_producer_queue_buffering_max_messages` → `queue.buffering.max.messages`
  - `kafka_producer_queue_buffering_max_kbytes` → `queue.buffering.max.kbytes`
  - `kafka_producer_request_required_acks` → `request.required.acks`
- **Leg 2 — typed compression.** A `kafka_producer_compression_codec` setting
  backed by a new `KafkaCompressionCodec` enum (`none`/`gzip`/`snappy`/`lz4`/`zstd`)
  plus `kafka_producer_compression_level` (`Int64`), mapping to
  `compression.codec`/`compression.level`.

All settings defaulted to `0` / `-1` so that, left untouched, librdkafka uses its
own defaults — i.e. the change was meant to be backward-compatible for existing
tables.

## 2. Upstream-drift / validity findings

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`.

- **Leg 1 — `still-needed`.** HEAD's `KafkaSettings` ships none of the six
  `kafka_producer_*` throughput knobs, and `getProducerConfiguration`
  (`KafkaConfigLoader.cpp`) does not set `batch.size`, `batch.num.messages`,
  `linger.ms`, `queue.buffering.max.*`, or `request.required.acks`. There is no
  per-table way to tune producer batching/buffering on 26.3 — the leg is a real
  residual. **Ported.**
- **Leg 2 — `obsoleted-by-upstream`.** HEAD already ships
  `kafka_compression_codec` (`String`, `KafkaSettings.cpp:57`) and
  `kafka_compression_level` (`Int64`, `KafkaSettings.cpp:58`), wired to
  `compression.codec` / `compression.level` in `updateGlobalConfiguration`
  (`KafkaConfigLoader.cpp:408-412`). The upstream codec is a `String`, not a
  typed enum, so the patch's `KafkaCompressionCodec` enum is **not** carried —
  re-introducing it would duplicate functionality and add a redundant enum/type
  to the settings traits. **Enum dropped.**

### Backward-compatibility decision (Leg 2 kept as aliases)

A 25.3/25.8 table may carry the old `kafka_producer_compression_codec` /
`kafka_producer_compression_level` names in its stored DDL; on 26.3 those names
do not exist as declared Kafka settings, so `ATTACH`/`CREATE` would fail with
`UNKNOWN_SETTING`. To preserve the user-facing settings interface, the two
upstream compression settings are declared with `DECLARE_WITH_ALIAS` (the
`ALIAS` second macro parameter of `KAFKA_RELATED_SETTINGS`), registering the old
`kafka_producer_*` names as backward-compat **aliases** that resolve to the
canonical upstream settings. No new enum or type is introduced — the alias rides
on the existing `String`/`Int64` declarations.

## 3. C++ / security review

- **26.3 direct-settings wiring (no params-struct churn).** The original patch
  threaded the eight values through `KafkaConfigLoader::ProducerConfigParams`
  (new struct fields) and populated them from `getProducerConfiguration` in both
  `StorageKafka.cpp` and `StorageKafka2.cpp`. On 26.3 the idiomatic pattern
  (used by patch 029's SASL/SSL wiring in `updateGlobalConfiguration`) is to read
  `storage.getKafkaSettings()` directly. So the six knobs are applied inside the
  templated `KafkaConfigLoader::getProducerConfiguration` by reading the storage
  settings directly — `StorageKafka.cpp`, `StorageKafka2.cpp`, and
  `KafkaConfigLoader.h` are **untouched** (no new `ProducerConfigParams` fields).
- **`getKafkaSettings` accessor.** Returns `const KafkaSettings &`
  (`StorageKafka.h:101`, `StorageKafka2.h:112`); bound by `const auto &` to avoid
  a copy. Same accessor already used in `updateConfigurationFromConfig`.
- **The `0`-skip guard in `setKafkaConfigValue`.** librdkafka's minimum for
  `batch.size`, `batch.num.messages`, `linger.ms`,
  `queue.buffering.max.messages`, and `queue.buffering.max.kbytes` is `>= 1`.
  These settings default to `0` meaning "leave at librdkafka default", so a
  literal `"0"` must not be forwarded (it would be rejected, or clobber the
  default). A `static const std::unordered_set<String> non_zero_properties`
  short-circuits the `set` for those five dotted keys when `value == "0"`.
  `request.required.acks` is intentionally **not** in the set: `-1`/`0`/`1` are
  all valid and meaningful, so it is always set via `conf.set` directly.
- **Key spelling — dotted, not underscored.** `setKafkaConfigValue` converts
  `_`→`.` (for XML-config friendliness). The six keys are passed already dotted
  (`"batch.size"`, …) which have no underscores, so the conversion is a no-op and
  the keys reach librdkafka unchanged. Underscore-form keys are deliberately not
  used.
- **No new attack surface.** Pure producer-side tuning forwarded to librdkafka;
  no credential handling, no new I/O path.

## 4. Test design

Stateless `.sql` test
`tests/queries/0_stateless/9033_kafka_producer_settings.sql` (Aiven
`9<NNN>_<slug>` convention; `NNN = 033`). `Kafka` DDL with
`kafka_num_consumers = 0` does not start consumer threads / connect to a broker,
so it is deterministic without a live broker.

- Asserts all six new `kafka_producer_*` knobs are accepted at `CREATE TABLE`
  (non-zero values: `batch_size = 16384`, `batch_num_messages = 1000`,
  `linger_ms = 10`, `queue_buffering_max_messages = 100000`,
  `queue_buffering_max_kbytes = 1048576`, `request_required_acks = 1`).
- Asserts the backward-compat compression aliases are accepted:
  `kafka_producer_compression_codec = 'gzip'`,
  `kafka_producer_compression_level = 5`.
- Asserts the canonical upstream names still work:
  `kafka_compression_codec = 'zstd'`, `kafka_compression_level = 3`.
- The `0`-skip guard in `setKafkaConfigValue` is covered by **inspection** (§3),
  not by this test — observing it requires a live broker producer config dump,
  which a stateless test cannot do deterministically.

### Evidence (against the patched binary, 2026-06-05)

Verified via `clickhouse local` (same engine + settings code path as the server;
the full `clickhouse-test` harness server fails to start here due to an unrelated
26.3 `DiskSelector` / `database_disk` config quirk shared with patches 029/031):

```
kafka_producer_* knobs accepted
kafka_producer_compression_* aliases accepted
kafka_compression_* canonical names accepted
```

Output matches the `.reference` exactly; `clickhouse local` exit 0. Full
`clickhouse` build exit 0.

## 5. Rollback considerations

Three source files, two cohesive changes. Leg 1: drop the six `DECLARE`s in
`KafkaSettings.cpp`, the six extern declarations in `KafkaConfigLoader.cpp`, and
the six `setKafkaConfigValue` / `conf.set` lines in `getProducerConfiguration`
(plus the `<unordered_set>` include and the `non_zero_properties` guard if Leg 1
is fully removed). Leg 2 aliases: change the two `ALIAS(...)` back to plain
`DECLARE(...)` in `KafkaSettings.cpp` (removes the `kafka_producer_compression_*`
backward-compat names; the canonical `kafka_compression_*` settings keep
working). No `SettingsEnums.{h,cpp}`, no `KafkaSettings.h` `M(CLASS_NAME, …)`
list change, no `StorageKafka*.cpp`, no `KafkaConfigLoader.h`, no submodule or
schema coupling.

## 6. Per-uplift notes

### 25.8-aiven (the version ported FROM)

Source `6384eccfac`: eight settings (six throughput knobs + two compression),
the new `KafkaCompressionCodec` enum (`SettingsEnums.{h,cpp}`, `KafkaSettings.h`
`M(CLASS_NAME, KafkaCompressionCodec)`), eight new `ProducerConfigParams` fields
(`KafkaConfigLoader.h`), and producer-config population in both `StorageKafka.cpp`
and `StorageKafka2.cpp` plus `getProducerConfiguration`. The `0`-skip guard in
`setKafkaConfigValue` was already present in the source.

### 26.3-aiven (this uplift)

`partial`. **Leg 1 carried** (six throughput knobs), **rewritten the 26.3 way**:
applied via a direct `storage.getKafkaSettings()` read inside the templated
`KafkaConfigLoader::getProducerConfiguration`, so there is **no
`ProducerConfigParams` struct change and no `StorageKafka.cpp` / `StorageKafka2.cpp`
churn** (mirrors patch 029's direct-settings wiring). **Leg 2 (compression)
obsoleted-by-upstream** — upstream's `kafka_compression_codec` (`String`) /
`kafka_compression_level` (`Int64`) already exist and are wired; the patch's
`KafkaCompressionCodec` **enum is dropped** (`SettingsEnums.{h,cpp}` and the
`KafkaSettings.h` type list are untouched). The two compression settings are kept
as `DECLARE_WITH_ALIAS` / `ALIAS` so the old `kafka_producer_compression_*` names
remain accepted (backward compat for 25.3/25.8 DDL). The `0`-skip guard in
`setKafkaConfigValue` is carried (librdkafka minimum `>= 1` for the five
buffering/batching properties). **No dependency on patch 032** — the six knobs
are placed after `kafka_ssl_key_location` rather than after
`kafka_auto_offset_reset` (which 032 introduces and is not yet present). Files
changed: `src/Storages/Kafka/KafkaSettings.cpp`,
`src/Storages/Kafka/KafkaConfigLoader.cpp`, plus stateless test
`tests/queries/0_stateless/9033_kafka_producer_settings.{sql,reference}`. Build
exit 0; verified via the `clickhouse local` evidence in §4 and stateless test
`9033_kafka_producer_settings`.

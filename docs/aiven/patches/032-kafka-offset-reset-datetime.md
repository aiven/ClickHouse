# Patch 032 — kafka-offset-reset-datetime

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `9f769ed901` (author Tilman Moeller, co-author Kevin Michel, 2025-12-23) | — | (the version we are porting FROM) |
| 26.3-aiven | `patch-port(032)` (STAGED — HEAD unmoved at `94dd391d108`, commit deferred to maintainer) | parent agent, 2026-06-05 | **`partial`** — Leg 1 (`kafka_auto_offset_reset`) still-needed, faithful enum port; Leg 2 (`kafka_date_time_input_format`) obsoleted-by-upstream, re-added as a `.changed`-guarded backward-compat alias |

## 1. Purpose (original patch)

Two independent legs:

- **Leg 1 — `kafka_auto_offset_reset`.** `auto.offset.reset` was hardcoded to
  `earliest` in the Kafka consumer configuration, with no per-table way to set it.
  The patch adds a `KafkaAutoOffsetReset` enum setting (values `smallest`,
  `earliest`, `beginning`, `largest`, `latest`, `end`; default `EARLIEST`) so each
  `Kafka` table can choose its reset policy. Default `EARLIEST` preserves the old
  hardcoded behavior bit-for-bit.
- **Leg 2 — `kafka_date_time_input_format`.** A per-table `DateTimeInputFormat`
  Kafka setting forwarded to the canonical `date_time_input_format` format setting,
  so the date/time parsing mode can be pinned in stored DDL (instead of relying on
  the global config or context settings, which do not survive replication/restore).

## 2. Upstream-drift / validity findings

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`.

- **Leg 1 — `still-needed` (faithful enum port).** HEAD hardcodes
  `conf.set("auto.offset.reset", "earliest")` at `KafkaConfigLoader.cpp:532` inside
  the templated `getConsumerConfiguration`. There is no upstream `kafka_auto_offset_reset`
  and no other per-table mechanism to override `auto.offset.reset`. Both 25.3 and
  25.8 shipped this as the `KafkaAutoOffsetReset` enum, so it is ported faithfully as
  an enum — same rationale as patch 029: DDL-time validation, case-insensitive
  parsing, and a stable interface contract for stored DDL that a plain `String`
  cannot provide. Default `EARLIEST` preserves today's hardcoded `earliest`.
- **Leg 2 — `obsoleted-by-upstream`, re-added as a backward-compat alias.** 26.3's
  generic format-settings passthrough already forwards `date_time_input_format` when
  it is set directly on a `Kafka` table: `KafkaSettings` folds
  `LIST_OF_ALL_FORMAT_SETTINGS` into the engine's settings
  (`KafkaSettings.cpp:74-77`), and `getFormatSettings` emits every non-`kafka_`-prefixed
  setting into `createSettingsAdjustments` (`StorageKafkaUtils.cpp:525-526`). So the
  functionality exists under the canonical name. But 25.3/25.8 stored DDL carries the
  `kafka_`-prefixed name, which would fail `UNKNOWN_SETTING` on `ATTACH`/`CREATE`. To
  preserve the user-facing settings interface, the `kafka_date_time_input_format`
  setting is re-added as a thin alias — mirroring the patch-031
  `kafka_format_avro_schema_registry_url` shim already in `createSettingsAdjustments`.

## 3. C++ / security review

- **Leg 1 — direct-settings wiring (divergence from the original).** The original
  threaded the value through a `ConsumerConfigParams::auto_offset_reset` field, which
  required edits to `KafkaConfigLoader.h`, `StorageKafka.cpp`, and `StorageKafka2.cpp`.
  This port instead reads the setting directly inside the templated
  `KafkaConfigLoader::getConsumerConfiguration`:

  ```cpp
  conf.set("auto.offset.reset",
      SettingFieldKafkaAutoOffsetResetTraits::toString(
          storage.getKafkaSettings()[KafkaSetting::kafka_auto_offset_reset].value));
  ```

  This mirrors the 029/033 direct-settings pattern (the same function already reads
  `storage.getKafkaSettings()` for the SASL/SSL knobs), so **`KafkaConfigLoader.h`,
  `StorageKafka.cpp`, and `StorageKafka2.cpp` are untouched** — a smaller, more
  self-contained diff with no `ProducerConfigParams`/`ConsumerConfigParams` struct
  churn. **Invariant:** `SettingFieldKafkaAutoOffsetResetTraits::toString` returns the
  canonical lowercase token (`earliest`, `largest`, …), which is exactly what
  `librdkafka` expects, and the enum guarantees a valid token (no raw string can leak
  through to `librdkafka`). Default `EARLIEST` ⇒ `"earliest"` is byte-identical to the
  previous hardcoded behavior.

  **Case-sensitivity note (and divergence from the spec's assumption).** Unlike the
  hand-written `KafkaSASLMechanism` / `KafkaSecurityProtocol` /
  `KafkaSSLEndpointIdentificationAlgorithm` traits in patch 029 (which call
  `Poco::toLower` for case-insensitive parsing), the standard `IMPLEMENT_SETTING_ENUM`
  macro used here does **not** lowercase the input — it is **case-sensitive**, so only
  the canonical lowercase tokens (`smallest`, `earliest`, …) are accepted at DDL time;
  `'LATEST'` is rejected with `BAD_ARGUMENTS`. This matches the original 25.8 patch,
  which used the same macro, so it is the **faithful** behavior and preserves the
  25.3/25.8 interface contract exactly. (The dispatch spec assumed the enum was
  case-insensitive; verified empirically that it is not. No custom case-insensitive
  trait was added, since that would diverge from both the verbatim port and the
  original.) `librdkafka` itself matches `auto.offset.reset` case-insensitively at
  runtime, but the DDL-time enum is the gate, and it admits only the lowercase tokens.

- **Leg 2 — `.changed`-guarded forward (intentional divergence from the original).**
  The original emitted the alias **unconditionally**
  (`result.emplace_back("date_time_input_format", …)` with no guard). On 25.8 that was
  harmless because there was no generic passthrough competing for the same key. On
  26.3 the generic `getFormatSettings` already emits a canonical
  `date_time_input_format` when the user sets it directly, so an unconditional alias
  emit would **clobber** the user's canonical value with the alias default (`Basic`)
  whenever the alias is left unset. The port therefore guards the forward on
  `.changed`:

  ```cpp
  if (kafka_settings[KafkaSetting::kafka_date_time_input_format].changed)
      result.emplace_back("date_time_input_format",
          kafka_settings[KafkaSetting::kafka_date_time_input_format].toString());
  ```

### `SettingsChanges` precedence (observed)

`createSettingsAdjustments` returns a `SettingsChanges` (a `std::vector<SettingChange>`),
consumed by `Context::applySettingsChanges` →
`Context::applySettingsChangesWithLock` (`Context.cpp:3068-3072`), which iterates the
vector in order and applies each change via `applySettingChangeWithLock`. There is **no
dedup**: a later same-key entry overwrites an earlier one (last-wins). The alias forward
is `emplace_back`-ed *after* the generic `getFormatSettings` insert
(`StorageKafkaUtils.cpp:525-526`), so when both are present the alias entry is applied
last and wins. Combined with the `.changed` guard this yields exactly the required
invariant:

- canonical-only set → canonical value used (alias not forwarded);
- alias-only set → alias value used (only entry for the key);
- both set → alias wins (the explicit downstream-named override, emitted last).

`DateTimeInputFormat` is already registered in the `KafkaSettings.h`
`KAFKA_SETTINGS_SUPPORTED_TYPES` `M(CLASS_NAME, …)` list (line 29), so no new
`M(CLASS_NAME, DateTimeInputFormat)` entry was needed (the original 032 likewise did
not add it).

## 4. Test design

Stateless `.sql` test
`tests/queries/0_stateless/9032_kafka_offset_reset_datetime.sql` (Aiven
`9<NNN>_<slug>` convention; `NNN = 032`). `Kafka` DDL with `kafka_num_consumers = 0`
does not connect to a broker, so it is deterministic without a live broker.

- **Leg 1** — asserts `kafka_auto_offset_reset` accepts canonical values
  (`smallest`, `earliest`, `latest`, `end`), is **case-sensitive** (`'LATEST'` is
  rejected with `serverError BAD_ARGUMENTS` — see the case-sensitivity note in §3),
  and **rejects** an invalid value (`'bogus'`) with `serverError BAD_ARGUMENTS` — the
  guardrail a plain `String` would lack.
- **Leg 2** — asserts the alias `kafka_date_time_input_format = 'best_effort'` is
  accepted at `CREATE TABLE`, and that the canonical `date_time_input_format =
  'best_effort'` is also accepted directly on the `Kafka` table (proves the generic
  passthrough still works alongside the alias).
- The runtime `auto.offset.reset` → `librdkafka` wiring (Leg 1) and the
  alias-vs-canonical precedence at apply time (Leg 2) are covered by **inspection**
  (§3); exercising them end-to-end would require a live broker, which a stateless
  test cannot do deterministically.

### Evidence (against the patched binary, 2026-06-05)

Verified via `clickhouse local` (same engine + settings code path as the server; the
full harness server start is blocked here by an unrelated 26.3 `DiskSelector` /
`database_disk` config quirk). See §verification in the final report. Full
`clickhouse` build exit 0.

## 5. Rollback considerations

- **Leg 1:** drop the `KafkaAutoOffsetReset` enum + trait
  (`SettingsEnums.h`/`SettingsEnums.cpp`), the `M(CLASS_NAME, KafkaAutoOffsetReset)`
  line (`KafkaSettings.h`), the `DECLARE` (`KafkaSettings.cpp`), and the extern +
  `conf.set("auto.offset.reset", …)` change (`KafkaConfigLoader.cpp`) — reverting the
  last line to the hardcoded `"earliest"` restores prior behavior exactly (the enum
  default is `EARLIEST`).
- **Leg 2:** drop the `DECLARE(DateTimeInputFormat, kafka_date_time_input_format, …)`
  (`KafkaSettings.cpp`) and the extern + `.changed`-guarded `emplace_back`
  (`StorageKafkaUtils.cpp`); the canonical `date_time_input_format` continues to work.
- No submodule, schema, or `StorageKafka*.cpp` / `KafkaConfigLoader.h` coupling.

## 6. Per-uplift notes

### 25.8-aiven (the version ported FROM)

Source `9f769ed901`: both legs. Leg 1 threaded `auto_offset_reset` through a
`ConsumerConfigParams` field (touching `KafkaConfigLoader.h`, `StorageKafka.cpp`,
`StorageKafka2.cpp`); Leg 2 emitted the `date_time_input_format` alias
**unconditionally** in `createSettingsAdjustments`.

### 26.3-aiven (this uplift)

`partial`. **Leg 1 carried as a faithful enum port**, but rewritten the 26.3 way with
**direct-settings wiring** (`storage.getKafkaSettings()` read inside the templated
`getConsumerConfiguration`), so `KafkaConfigLoader.h`, `StorageKafka.cpp`, and
`StorageKafka2.cpp` are **untouched** vs the original's params-struct plumbing.
**Leg 2 obsoleted-by-upstream** (the 26.3 generic format passthrough already forwards
the canonical `date_time_input_format`), re-added as a **`.changed`-guarded**
backward-compat alias — the guard is an **intentional divergence** from the original's
unconditional emit, required for coexistence with the 26.3 passthrough so the alias
default does not clobber a user's canonical `date_time_input_format`.

Files changed: `src/Core/SettingsEnums.h`, `src/Core/SettingsEnums.cpp`,
`src/Storages/Kafka/KafkaSettings.h`, `src/Storages/Kafka/KafkaSettings.cpp`,
`src/Storages/Kafka/KafkaConfigLoader.cpp`, `src/Storages/Kafka/StorageKafkaUtils.cpp`.
Build exit 0; verified via the `clickhouse local` evidence in §4 and stateless test
`9032_kafka_offset_reset_datetime`.

### Cross-patch dependency (076)

Patch 076 (`kafka_auto_offset_reset_ms`) **extends** Leg 1 of this patch (it adds a
companion time-based reset knob layered on the same `kafka_auto_offset_reset` enum
machinery). Therefore **032 lands first**; 076 depends on the `KafkaAutoOffsetReset`
enum and `kafka_auto_offset_reset` setting introduced here.

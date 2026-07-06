# Patch 076 — kafka-offset-reset-by-duration

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `7e09c63352` (author/committer Joe Lynch <joelynch112@gmail.com>, 2026-04-29) | — | (the version we are porting FROM) |
| 26.3-aiven | `patch-port(076)` (`33cd92d585b`) | parent agent, 2026-06-10 | **`still-needed-but-rewrite`** — faithful setting + time-based-reset semantics, but the value is threaded the 26.3 *direct-wiring* way (no `ConsumerConfigParams` struct) and the assignment callback is adapted to 26.3's `cppkafka` mutate-in-place rebalance contract |

> The commit *title* on the source abbreviates the setting as `kafka_auto_offset_reset_ms`;
> the actual `DECLARE`d name is `kafka_auto_offset_reset_by_duration_ms`, which is what
> this port carries verbatim.

## 1. Purpose (original patch)

Adds a `UInt64` Kafka setting `kafka_auto_offset_reset_by_duration_ms`. When it is
non-zero **and a partition has no committed offset**, the consumer starts from the offset
corresponding to `now() - kafka_auto_offset_reset_by_duration_ms` (resolved through
`librdkafka`'s offsets-for-times API) instead of the coarse `kafka_auto_offset_reset`
smallest/largest choice. `0` (the default) disables it and preserves the existing
`kafka_auto_offset_reset` behavior. When set, it takes precedence over
`kafka_auto_offset_reset` for partitions lacking a committed offset.

This is a companion knob layered on top of patch 032's `kafka_auto_offset_reset` enum
(see 032 §"Cross-patch dependency (076)"): 032 lands first; 076 extends it.

## 2. Upstream-drift / validity findings

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`.

- **`still-needed`.** 26.3 has no per-table time-based offset-reset mechanism. HEAD
  only exposes the coarse `auto.offset.reset` (smallest/largest) via patch 032's enum;
  there is no upstream way to say "on first consumption, start N ms in the past". The
  feature is therefore semantically needed and has no upstream equivalent.
- **Kafka subsystem drift since 25.8 (why this is a rewrite, not a cherry-pick).** Prior
  Aiven ports (029–033) converted the Kafka consumer settings plumbing to the 26.3
  *direct-wiring* pattern: settings are read straight from `storage.getKafkaSettings()`
  / `(*kafka_settings)[…]` at the consumer construction sites, and the
  `ConsumerConfigParams` / `ProducerConfigParams` struct fields the original 25.8 patch
  threaded the value through no longer exist on this branch. In addition, 26.3's
  `KafkaConsumer2` constructor already carries a `skip_bytes_` parameter (schema-registry
  drift) that the 25.8 source did not. So the source's `ConsumerConfigParams` field +
  constructor-arg-list edits do not apply; the value is wired directly instead.

## 3. C++ / security review

- **Setting declaration (faithful).** `DECLARE(UInt64, kafka_auto_offset_reset_by_duration_ms, 0, …)`
  in `KafkaSettings.cpp`, byte-identical to the source (name, type, default `0`,
  description). Default `0` ⇒ disabled ⇒ behavior is bit-for-bit the pre-patch
  `kafka_auto_offset_reset` path.

- **Direct-wiring (divergence from the original, matches 029–033).** The value is read at
  the two consumer-construction sites and passed straight to the consumer, with **no
  `ConsumerConfigParams` field**:
  - `StorageKafka::popConsumer` →
    `ret_consumer_ptr->createConsumer(consumer_config, (*kafka_settings)[KafkaSetting::kafka_auto_offset_reset_by_duration_ms].value)`;
  - `StorageKafka2::createKafkaConsumer` →
    `std::make_shared<KafkaConsumer2>(log, …, topics, getSchemaRegistrySkipBytes(), (*kafka_settings)[KafkaSetting::kafka_auto_offset_reset_by_duration_ms].value)`.
    Note the extra `getSchemaRegistrySkipBytes()` argument that the 25.8 source's
    `make_shared` did not have — 26.3's `KafkaConsumer2` ctor gained `skip_bytes_`, so the
    new `auto_offset_reset_ms_` is appended **after** it. The new ctor/method parameters
    are defaulted (`= 0`), so unrelated callers are unaffected.

- **`KafkaConsumer` (StorageKafka) — assignment-callback mutate-in-place.** The
  time-based resolution runs inside the `set_assignment_callback` lambda. **Invariant
  (verified against `contrib/cppkafka`):** `cppkafka::Consumer::handle_rebalance` invokes
  the assignment callback with a **non-const** `TopicPartitionList &` and then calls
  `assign()` with that **same** list after the callback returns. So offsets must be
  overridden by mutating `topic_partitions` **in place**; calling `consumer->assign()`
  from inside the callback would be overwritten. The lambda parameter type was widened
  from the prior `const TopicPartitionList &` to `TopicPartitionList &` accordingly. For
  each partition with **no committed offset** (`get_offsets_committed` →
  `RD_KAFKA_OFFSET_INVALID`), it resolves `now - duration` via `get_offsets_for_times`
  and sets the partition offset; partitions **with** a committed offset are left on their
  committed value (the duration only governs the no-committed-offset case, matching the
  setting's contract). `assignment` is recorded **after** the mutation.

- **`KafkaConsumer2` — `updateOffsets` resolution.** `KafkaConsumer2` does not use the
  rebalance-callback path; it drives offsets explicitly through `updateOffsets`. The same
  resolution is applied there: partitions whose stored offset is `INVALID_OFFSET` get the
  timestamp-resolved offset before the existing `assign`/seek logic runs.

- **Exception safety.** Both resolution blocks are wrapped in
  `try { … } catch (const cppkafka::HandleException & e)` and, on failure, log a warning
  and **fall through** to the normal `auto.offset.reset` behavior — a broker hiccup during
  offset resolution degrades gracefully rather than aborting the assignment. No new
  allocations on the hot poll path when the setting is `0` (the whole block is guarded by
  `auto_offset_reset_ms > 0`).

- **Negative/unresolvable offsets.** `get_offsets_for_times` can return a negative
  sentinel (e.g. `RD_KAFKA_OFFSET_END` when the timestamp is past the log end / partition
  empty); the code only applies the resolved offset when `>= 0` and otherwise logs and
  falls back, so no out-of-range offset is seeded.

## 4. Test design

Stateless `.sql` test `tests/queries/0_stateless/9076_kafka_offset_reset_by_duration.sql`
(Aiven `9<NNN>_<slug>` convention; `NNN = 076`). DDL-only: `kafka_num_consumers = 0`
keeps `CREATE` from connecting to a broker, so the run is deterministic without one. It
pins the **setting surface**:

- a non-zero value (`60000`) is accepted at `CREATE TABLE` and **round-trips** through
  the stored DDL (`extract(create_table_query, 'kafka_auto_offset_reset_by_duration_ms = [0-9]+')`);
- omitting it is accepted (defaults to `0`, disabled);
- a large value (`86400000`, one day) is accepted;
- it **coexists** with `kafka_auto_offset_reset` on the same table;
- a **non-numeric** value (`'notanumber'`) is rejected at DDL time with
  `serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED` (code 27) — the guardrail a plain
  `String` would lack.

The runtime time→offset resolution (committed-offset detection, `get_offsets_for_times`,
the mutate-in-place rebalance, and the `HandleException` fallback) is covered by
**inspection** (§3); exercising it end-to-end needs a live broker with a seeded
timestamped log, which a stateless test cannot do deterministically. (The 25.8 source
shipped a `test_kafka_auto_offset_reset_by_duration_ms` pytest integration test for that;
a broker-backed integration test is the follow-up if behavioral coverage is wanted.)

### Evidence (against the patched binary, 2026-06-10)

- Full `clickhouse` build exit 0 (after a build-dir recovery — see §6).
- `9076_kafka_offset_reset_by_duration: [ OK ] 0.13 sec.` via `clickhouse-test` against a
  freshly-built server (runbook §5).
- **Pre-patch FAIL evidence (causation):** the same DDL run against a binary **without**
  this change returns `Unknown setting 'kafka_auto_offset_reset_by_duration_ms': for
  storage Kafka. (UNKNOWN_SETTING)` — observed directly against a stale pre-patch server
  during this session. So the test passes only because the setting now exists.

## 5. Rollback considerations

- Drop the `DECLARE(UInt64, kafka_auto_offset_reset_by_duration_ms, …)` (`KafkaSettings.cpp`).
- Drop the two externs + the value passed at the consumer-construction sites
  (`StorageKafka.cpp`, `StorageKafka2.cpp`).
- Drop the `auto_offset_reset_ms_` parameter + member and the resolution block in
  `KafkaConsumer.{h,cpp}` (and revert the lambda parameter back to `const &`) and in
  `KafkaConsumer2.{h,cpp}` (`updateOffsets`).
- No submodule, schema, or `KafkaConfigLoader.h` coupling. Reverting restores the prior
  `kafka_auto_offset_reset`-only behavior exactly (the setting default is `0` = disabled).

## 6. Per-uplift notes

### 25.8-aiven (the version ported FROM)

Source `7e09c63352`: declared `kafka_auto_offset_reset_by_duration_ms`, threaded the value
through `ConsumerConfigParams`, and added the time→offset resolution in the consumers,
plus a `test_kafka_auto_offset_reset_by_duration_ms` pytest integration test.

### 26.3-aiven (this uplift)

`still-needed-but-rewrite`. Faithful setting + semantics, but: (i) the value is **direct-wired**
from `(*kafka_settings)[…]` at the consumer-construction sites (the `ConsumerConfigParams`
field the source used no longer exists — 029–033 drift); (ii) the `KafkaConsumer2`
`make_shared` argument list is adapted to 26.3's ctor, which gained a `skip_bytes_`
parameter, so `auto_offset_reset_ms_` is appended after it; (iii) the `KafkaConsumer`
assignment callback mutates `TopicPartitionList` **in place** (lambda widened to a
non-const ref) to match `cppkafka`'s `handle_rebalance` assign-after-callback contract,
verified in `contrib/cppkafka`. Test coverage is DDL-only (stateless `9076`), a
deliberate scope choice vs the source's broker-backed pytest.

Files changed: `src/Storages/Kafka/KafkaSettings.cpp`,
`src/Storages/Kafka/StorageKafka.cpp`, `src/Storages/Kafka/StorageKafka2.cpp`,
`src/Storages/Kafka/KafkaConsumer.h`, `src/Storages/Kafka/KafkaConsumer.cpp`,
`src/Storages/Kafka/KafkaConsumer2.h`, `src/Storages/Kafka/KafkaConsumer2.cpp`, plus the
stateless test `9076_kafka_offset_reset_by_duration.{sql,reference}`.

### Build-dir note (environment, not the patch)

The incremental build first failed at **link** with `undefined symbol:
DB::IAccessStorage::insertImpl(…)` / `removeImpl(…)` from the `UsersConfigAccessStorage`
and `LDAPAccessStorage` vtables. This was **pre-existing build-dir staleness**, not a 076
problem: the committed patch 022 changed those `IAccessStorage` virtual signatures, and in
this `build/` directory `ninja` has **no header-dependency records** (`#deps 0`, see
runbook §7), so the two subclasses 022 did not edit kept their stale pre-022 `.o`. Force-
recompiling `src/Access/*.cpp` (the `IAccessStorage.h` closure) cleared it. A clean CI
build is unaffected. This is worth a runbook §6 row (stale base-class virtual-signature
`.o` after a committed header change).

# Major upstream changes & backward-compatibility ledger (25.8 → 26.3)

## Purpose

A single place that collects the **architectural / interface / behavior changes**
between the 25.8-aiven and 26.3-aiven LTS lines that matter when integrating the new
version into the surrounding Aiven system (control plane, managed configs, operational
tooling, customer-facing settings). It is distilled from the per-patch dossiers and the
[`inventory.md`](inventory.md); each entry links back for full detail.

This is **not** the patch ledger (that is `inventory.md`). This document answers a
different question: *"When we drop 26.3 into the system, what behaves differently, what
existing customer state could break, and what must we do about it?"*

## How to use

- Before integration, read the **Integration action** of every entry tagged
  `⚠ external` or `⚠ operational` — those are the ones that change customer-visible
  behavior, stored-DDL compatibility, or required managed config.
- Entries tagged `internal` are recorded for context but need no system-side action.
- Append a new entry whenever a port uncovers a structural upstream change (use the
  template at the bottom).

## Recurring theme: settings backward-compatibility

A table created on 25.3/25.8 stores its `SETTINGS` verbatim. On 26.3, `ATTACH` / server
start re-parses that DDL; an unknown setting **name** throws `UNKNOWN_SETTING` and the
table fails to load. Therefore, whenever upstream provides an Aiven capability under a
**different name**, we keep the old name as a **`DECLARE_WITH_ALIAS`** alias (same type,
same default, same behavior). This preserves stored DDL with no behavior divergence.

Rule of thumb: *a setting name we ever shipped is part of the external contract — alias
it, don't drop it.*

## Quick reference

| Subsystem | Change (25.8 → 26.3) | Tag | Related patches |
|---|---|---|---|
| Object storage | Single `object_storage` → `ObjectStorageRouter` multi-location registry + disk-level `wrapped_disk` layering | ⚠ external | 026 |
| Object storage | Background `BlobKillerThread` physically removes blobs | ⚠ operational | 026 |
| Object storage | Azure disk code relocated to `…/ObjectStorages/AzureBlobStorage/` | internal | 024, 025 |
| Azure HTTP | Azure SDK Curl transport → Poco-based `PocoAzureHTTPClient` (`curl_options.CAInfo` gone) | internal | 013 |
| HTTP/SSL | Connection pool keyed by SSL context (trust isolation) | internal | 012 |
| WriteBuffer | `~WriteBuffer` no longer finalizes in destructor; `~WriteBufferFromS3` best-effort aborts | internal | 027 |
| MySQL/PG protocol | `mysql_require_secure_transport` / `postgresql_require_secure_transport` server settings exist upstream | ⚠ operational | 017 |
| Replicated DB | `internal_replication` for Replicated-DB auto-clusters is now a per-DB setting (default `false`); Aiven's forced-`true` patch dropped | ⚠ operational | 002, 004 |
| Replication queue | Queue-size monitor + insert delay/throw (patch 008) carried verbatim but wrapped in a default-**off** outer server setting `aiven_enable_replication_queue_size_limit`; production opts in additively (keeps `queue_size_monitor=1`) | ⚠ operational | 008 |
| Replication fetch | Early-fetch-pool settings (patch 046) renamed under `aiven_`, default-on, routing-only: `aiven_use_early_fetch_pool` (MergeTreeSetting, **aliased** for metadata compat) + `aiven_background_early_fetches_pool_size` (ServerSetting, **rename requires config update** — silent revert-to-default otherwise) | ⚠ operational | 046 |
| Kafka settings | Generic format-settings passthrough: all format settings are now valid per-table `Kafka` settings | ⚠ external | 031, 033 |
| Kafka settings | `kafka_security_protocol` / `kafka_sasl_mechanism` are `String` upstream (Aiven keeps enums) | ⚠ external | 029 |
| Kafka settings | `kafka_compression_codec` / `kafka_compression_level` exist upstream | ⚠ external | 033 |
| Kafka | Confluent schema-registry basic auth implemented upstream (with URL-decoding) | internal | 031 |
| Zero-copy | `dropAllData` reworked: `removeSharedRecursive(…, keep_all_shared_data=true)` instead of throwing | internal | 048 |

## Detailed entries

### OS-1 — Object storage: single → multiple object storages per disk  ⚠ external
- **Change.** 26.3 replaced the single `object_storage` member of `DiskObjectStorage`
  with an `ObjectStorageRouter` registry (N locations per disk) and introduced
  disk-level layering via a `wrapped_disk` pointer + `IDisk::supportsLayers`.
- **Why.** Upstream support for multiple object-storage locations behind one disk
  (replicated / tiered topologies).
- **Backward-compat impact.** Aiven's tiered-storage uses a **single** object-storage
  location per disk, which still works. The `backup` disk decorator (026) is **scoped to
  single-location disks**; a multi-location/replicated disk is rejected with
  `BAD_ARGUMENTS` (the soft-delete marker contract is unsound when background
  replication/GC cannot observe the markers).
- **Integration action.** If the system ever configures multi-location object-storage
  disks, the `backup` disk type is intentionally unavailable on them — do not assume it.
- **Refs.** [`patches/026-backup-disk-type.md`](../../patches/026-backup-disk-type.md).

### OS-2 — `BlobKillerThread` physical blob removal  ⚠ operational
- **Change.** 26.3 runs a background thread that physically deletes blobs from object
  storage based on a metadata-storage deletion queue.
- **Backward-compat impact.** The `backup` disk's soft-delete (write a deletion marker,
  let external GC decide) must not be undercut by this inner killer. Patch 026 disables
  the inner killer on backup-wrapped disks so only the backup-level (marker-writing)
  path is active; physical blob survival across DROP/merge/mutation/TTL-delete is
  asserted by `test_aiven_backup_disk`.
- **Integration action.** External GC remains the authority for physical deletion on
  backup disks. Ensure the GC component is wired in the new version.
- **Refs.** [`patches/026-backup-disk-type.md`](../../patches/026-backup-disk-type.md).

### KAFKA-1 — Generic format-settings passthrough  ⚠ external
- **Change.** 26.3 folds `LIST_OF_ALL_FORMAT_SETTINGS` into `KafkaSettings`
  (`KafkaSettings.cpp`), and `KafkaSettings::getFormatSettings` forwards **every
  non-`kafka_`-prefixed setting** into `createSettingsAdjustments`
  (`StorageKafkaUtils.cpp`). So any format setting (e.g. `format_avro_schema_registry_url`)
  is now settable per `Kafka` table directly.
- **Backward-compat impact.** Bespoke `kafka_<format-setting>` wrappers are redundant for
  new tables, but their **names** are still part of the contract — keep them as aliases
  (see KAFKA-2).
- **Integration action.** Documentation/tooling can point customers at canonical format
  setting names; old `kafka_`-prefixed names continue to work via aliases.
- **Refs.** [`patches/031-kafka-schema-registry-auth.md`](../../patches/031-kafka-schema-registry-auth.md).

### KAFKA-2 — Renamed Kafka settings → kept as aliases  ⚠ external
- **Change / compat.** Several Aiven Kafka settings now have an upstream equivalent under
  a different name. To preserve stored DDL, the Aiven names are kept as
  `DECLARE_WITH_ALIAS` aliases (same type/behavior):
  - `kafka_format_avro_schema_registry_url` → forwards to `format_avro_schema_registry_url` (031).
  - `kafka_producer_compression_codec` / `kafka_producer_compression_level` → aliases of
    upstream `kafka_compression_codec` / `kafka_compression_level` (033).
- **Integration action.** None required if aliases are present; verify with an `ATTACH`
  test of representative legacy DDL during integration.
- **Refs.** [`patches/031-…`](../../patches/031-kafka-schema-registry-auth.md), [`patches/033-…`](../../patches/033-kafka-extra-settings.md).

### KAFKA-3 — Security/SASL/SSL settings: `String` upstream, enum in Aiven  ⚠ external
- **Change.** Upstream ships `kafka_security_protocol` / `kafka_sasl_mechanism` as plain
  `String`. Aiven keeps them as validating **enums** (carried from 25.3/25.8) and adds
  enum/`String` SSL settings, because `String` loses DDL-time validation, hyphen→underscore
  normalization, and the always-applied `ssl.endpoint.identification.algorithm = none`
  default.
- **Backward-compat / security note.** `endpoint = none` is preserved **deliberately** —
  hostname verification is **off by default** for `SASL_SSL`/`SSL` Kafka (matches
  25.3/25.8 for self-signed / `::1` brokers). Operators opt in with `… = 'https'`.
- **Integration action.** If the platform wants verification on by default, set
  `kafka_ssl_endpoint_identification_algorithm = 'https'` in managed config/templates.
- **Refs.** [`patches/029-kafka-sasl-ssl-settings.md`](../../patches/029-kafka-sasl-ssl-settings.md).

### SEC-1 — TLS-required transport moved code → config  ⚠ operational
- **Change.** 26.3 ships `mysql_require_secure_transport` / `postgresql_require_secure_transport`
  server settings (default false). The 25.8 Aiven patch (017) hardcoded the rejection;
  it is dropped in favor of config.
- **Integration action.** Set `mysql_require_secure_transport=true` and
  `postgresql_require_secure_transport=true` in Aiven managed config to retain the prior
  security posture. **This is a required config change, not code.**
- **Refs.** [`patches/017-enforce-ssl-mysql-handler.md`](../../patches/017-enforce-ssl-mysql-handler.md).

### REPL-1 — Internal replication for Replicated-DB clusters moved code → setting  ⚠ operational
- **Change.** 26.3 ships `internal_replication` as a per-database setting
  `DatabaseReplicatedSettings::internal_replication` (default `false`, in base
  `v26.3.10.62-lts`, absent on 25.8). The value is threaded as an explicit
  `Cluster` constructor argument (`DatabaseReplicated.cpp:512`) into the same
  `addShard` insert-path machinery the 25.8 Aiven patch (002) added — that
  machinery is now byte-identical upstream (`Cluster.cpp:645,671-676`). Patch 002
  hard-coded `internal_replication=true` on the auto-built cluster only because
  the 25.8 base had no such setting; it is dropped in favor of config.
- **Backward-compat impact.** Doing nothing is a **silent behavior regression**:
  a `Distributed` table over a `Replicated` database would default to writing to
  **all** replicas (N× writes) instead of one-replica-plus-`ReplicatedMergeTree`
  async replication — the performance/consistency posture 25.8-aiven shipped is
  lost. Because the setting's value resolves from the server-config
  `<database_replicated>` section when the stored CREATE did not pin it
  (precedence: `DECLARE` default → server config → per-DB `CREATE … SETTINGS`,
  `DatabaseReplicated.cpp:2670-2673`), the prior behavior is restorable for
  **existing and new** DBs, all creation paths, with no code.
- **Integration action.** Set
  `<database_replicated><internal_replication>true</internal_replication></database_replicated>`
  in Aiven's managed server config. **This is a required config change, not code.**
  **Sequencing / safety:** `internal_replication=true` is safe **only** when every
  table behind a Replicated-DB auto-cluster is `ReplicatedMergeTree` — a plain
  `MergeTree` would receive a one-replica INSERT that never replicates (silent
  divergence). That invariant is enforced by patch **004**
  (`MergeTree`→`ReplicatedMergeTree` substitution); flip the config
  with/after 004 is in effect, not before. Do **not** use
  `ALTER DATABASE … MODIFY SETTING internal_replication` for this — patch 003's
  `applySettingsChanges` is in-memory-only and per-replica (non-durable).
- **Refs.** [`patches/002-enable-internal-replication-for-databasereplicated-clusters.md`](../../patches/002-enable-internal-replication-for-databasereplicated-clusters.md),
  [`inventory.md`](inventory.md) rows 002 / 004.

### REPL-2 — Replication-queue-size limiter re-gated behind a default-off `aiven_` server setting  ⚠ operational
- **Change.** Patch 008 ("Fix unbounded replication queue growth") ships a per-table
  background monitor thread (`ReplicatedMergeTreeQueueSizeThread`) plus insert delay/throw
  gating keyed on `ReplicatedMergeTree` replication-queue size, enabled in 25.8-aiven by
  `queue_size_monitor` — a **query setting defaulting to `true`**. On the 26.3 port, patch
  008 is carried **verbatim** (`queue_size_monitor` and the four thresholds —
  `queue_size_to_delay_insert`, `queue_size_to_throw_insert`,
  `queues_total_size_to_delay_insert`, `queues_total_size_to_throw_insert` — untouched), but
  the whole feature is wrapped in a **new default-`false` server setting**
  `aiven_enable_replication_queue_size_limit` acting as an **outer** master guard (clause
  (v): a default-on monitor + insert rejection for every `ReplicatedMergeTree` table is a
  broad-blast-radius default change and must not ship on by default).
- **Backward-compat impact.** Doing nothing is a **silent behavior regression** of the
  fleet's queue-overrun protection: a stock 26.3 build runs the limiter **off** (= upstream
  behavior, no monitor thread, no back-pressure, regardless of `queue_size_monitor`),
  whereas 25.8-aiven production ran it on. Because 008 is carried verbatim, legacy
  profiles/DDL that set `queue_size_monitor` load natively (it remains a live setting).
- **Integration action.** To retain the 25.8 protection, **add**
  `<aiven_enable_replication_queue_size_limit>true</aiven_enable_replication_queue_size_limit>`
  to Aiven's managed server config (keep the existing `queue_size_monitor=1` and the
  threshold values as-is — the change is purely additive). **This is a required config
  change, not code.**
- **Refs.** [`proposals/2026-06-15-aiven-settings-naming-convention-and-queue-size-guard.md`](../../proposals/2026-06-15-aiven-settings-naming-convention-and-queue-size-guard.md),
  [`inventory.md`](inventory.md) row 008.

### REPL-3 — Early-fetch-pool settings renamed under `aiven_`, default-on, routing-only  ⚠ operational
- **Change.** Patch 046 ("Add early fetch pool") splits the `ReplicatedMergeTree`
  `GET_PART`/`ATTACH_PART` fetch path into two background pools — a separate
  **early** pool for the empty-`source_replica` initial-sync fetches (a fresh
  replica downloading pre-existing data) and the existing **normal** pool for
  ongoing insert-driven fetches — so the big early parts cannot starve the small
  insert-driven tasks. Both 25.8 settings are renamed under the `aiven_`
  convention and the feature is carried **on by default** (it only changes which
  pool runs a fetch, not results):
  - `use_early_fetch_pool` → `aiven_use_early_fetch_pool` (`MergeTreeSetting`,
    `Bool`, default `true`), declared `DECLARE_WITH_ALIAS` with the old name kept
    as a backward-compatible alias.
  - `background_early_fetches_pool_size` → `aiven_background_early_fetches_pool_size`
    (`ServerSetting`, `UInt64`, default `8`), **plain rename, no alias**.
- **Backward-compat impact.**
  - *MergeTreeSetting:* the alias fully covers persisted table metadata — a 25.8
    `.sql` with `SETTINGS use_early_fetch_pool = …` still ATTACHes (no
    `UNKNOWN_SETTING`) and the value is carried forward; `system.merge_tree_settings`
    reports the canonical `aiven_use_early_fetch_pool`.
  - *ServerSetting:* **no alias.** A 25.8 server config still using
    `<background_early_fetches_pool_size>` is **silently ignored** on 26.3 (server
    settings are read by known key; unknown keys do not throw), so the early-fetch
    pool reverts to the default `8` until the key is renamed. Not a code regression,
    but a silent config drift if unnoticed.
- **Integration action.** Rename the server-config key to
  `<aiven_background_early_fetches_pool_size>` if the managed config pins a
  non-default early-pool size. The per-table setting needs no action (alias covers
  it). **This is a config rename, not code.**
- **Refs.** [`patches/046-early-fetch-pool.md`](../../patches/046-early-fetch-pool.md),
  [`inventory.md`](inventory.md) row 046.

### ZC-1 — Zero-copy `dropAllData` rework  internal
- **Change.** Upstream `25b0406c35c` reworked `MergeTreeData::dropAllData`: instead of
  throwing `ZERO_COPY_REPLICATION_ERROR`, it `LOG_WARNING`s and calls
  `removeSharedRecursive(…, keep_all_shared_data=true, {})`, which is safer (preserves
  shared data). The old Aiven guard (048 Leg 1) is therefore dropped; the cross-shard
  lock pre-creation race fix (048 Leg 2) is still carried.
- **Integration action.** None; recorded so a future "drop table stuck" investigation
  knows the 26.3 primitive is `keep_all_shared_data`, not a throw.
- **Refs.** [`patches/048-zero-copy-fixes.md`](../../patches/048-zero-copy-fixes.md).

### EXTDB-1 — PostgreSQL/MySQL client SSL has no upstream equivalent  internal
- **Change.** 26.3 still lacks client-side PG/MySQL SSL config; the Aiven patch (021)
  carries session settings (`postgresql_connection_pool_ssl_mode`/`_ssl_root_cert`,
  `SSLMode`/`MySQLSSLMode` enums) and a `mariadb-connector-c` fork pin.
- **Integration action.** The connector submodule fork must remain pinned
  (`aiven/mariadb-connector-c`, branch `aiven/clickhouse-v26.3.10.62`).
- **Refs.** [`patches/021-external-db-ssl.md`](../../patches/021-external-db-ssl.md).

## Entry template

```
### <ID> — <short title>  <tag: ⚠ external | ⚠ operational | internal>
- **Change.** What changed between 25.8 and 26.3.
- **Backward-compat impact.** What existing customer state / behavior is affected.
- **Integration action.** Concrete action for the system integration (or "none").
- **Refs.** Dossier / inventory links.
```

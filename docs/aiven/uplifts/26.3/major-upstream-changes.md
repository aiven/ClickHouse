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
| Refreshable MV | Aiven-only shard-level coordination added on top of upstream replica-level; Keeper layout `…/replicas` → `…/shards/<shard>`; fixes cross-shard data loss. ✓ **RECONCILED onto `v26.3.15.4`** (kept #104051's keeper-loss-safe loop + 066's shard coordination; `replica_name` shard-qualified to fix a shared-znode ownership collision — see REPL-4) | ⚠ operational | 066, 078 |
| Table namespace | `.tmp*` table-name namespace reserved (reject non-internal `CREATE`/`RENAME`) behind a new default-**off** server setting `aiven_prohibit_tmp_table_creation`; engine temporaries (`CREATE OR REPLACE`, refreshable-MV refresh) renamed under `.tmp*` and exempted | ⚠ operational | 062, 063, 064 |
| Replicated DB | Custom `ReplicatedMergeTree` ZK path / replica name already rejected upstream via `database_replicated_allow_replicated_engine_arguments=0` (default); Aiven's unconditional patch dropped — restore the **un-bypassable** guarantee with a `readonly` constraint pinning the setting | ⚠ operational | 058 |
| Replicated DB | Transparent `*MergeTree`→`Replicated*` engine substitution in `Replicated` databases (patch 004) carried behind a new default-**off** server setting `aiven_replace_mergetree_with_replicated`; production pairs it with the upstream `database_replicated_allow_only_replicated_engine=1` reject-backstop, sequenced with the REPL-1 `internal_replication=true` flip. **`.15.4` decoupled the replicated-DB DDL-log path from `query_kind == SECONDARY_QUERY` — it now sets only `ClientInfo::is_replicated_database_internal` (`DDLTask.cpp:663-667`); 004's gate was migrated to that flag (dossier §5.5), else the rewrite silently no-ops.** | ⚠ operational | 004 |

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

### REPL-4 — Refreshable-MV shard coordination + Keeper znode layout `…/replicas` → `…/shards`  ⚠ operational
- **Change.** Patches 066 ("Fix MV refresh in sharded environment") + 078 ("Fix MV
  refresh task race condition", squashed) add an Aiven-only **shard-level**
  coordination layer to refreshable materialized views, on top of the existing
  upstream **replica-level** coordination. Upstream 26.3 has no shard coordination:
  in a sharded `DatabaseReplicated` each shard ran the refresh independently and
  finished it with a replicated `EXCHANGE`/`DROP` that propagates to all shards,
  swapping in a temp table empty on the other shards and **deleting their data**.
  066 fixes this with a global-leader / shard-leader hierarchy, a
  `…/shards/<shard>` Keeper subtree, and a UUID-keyed `EXCHANGE` deferred until all
  shards finish.
- **Backward-compat impact.** The coordination znode layout changes from
  `…/replicas` to `…/shards/<shard>`. For Aiven this is shards→shards (066 is in
  both 25.8 prod and the 26.3 target — no migration). A refreshable MV whose
  coordination znodes were created by an upstream-`replicas`-layout server (26.3
  base **without** this patch) would not interoperate — irrelevant for Aiven
  (always-066), stated for the record. The new shard-coordination Keeper ops use
  only single reads/writes and multi-writes (no multi-read), so they work on real
  ZooKeeper, not just ClickHouse Keeper.
- **Integration action.** None for Aiven's always-066 path. Do not mix
  066-patched and unpatched servers against the same refreshable-MV coordination
  path.
- **Status (`v26.3.15.4-lts` rebase).** **RECONCILED — staged (uncommitted), to land as its own commit on `v26.3.15.4`.** Upstream `.15` had again rewritten the refreshable-MV refresh loop (the #104051 keeper-connection-loss backport + `SYSTEM PAUSE VIEW`), producing a 17-hunk `RefreshTask.cpp` conflict against 066/078's own refresh-loop rewrite. The resolution **kept upstream's two-task `doScheduling`/`executeRefresh` loop** (so #104051's "owner re-creates its ephemeral `running` znode after a brief Keeper blip" duplicate-refresh avoidance survives) and **re-introduced 066's shard-leader/global-leader coordination + deferred UUID-keyed `EXCHANGE` inside it**, preserving the five `.62` defect fixes and PAUSE VIEW. `StorageMaterializedView::exchangeTargetTable` re-adds 066's `block_io`/`CompletedPipelineExecutor` "wait for all replicas" block while keeping committed 062's `setInternal(true)`.
  - **Reconciliation-specific fix (data-loss-relevant).** 066's coordination znode is *shared* across shards (it is keyed by `{uuid}`, **not** `{shard}`), but #104051 keys the running owner on `last_attempt_replica == replica_name`. Stock `default_replica_name` is just `{replica}`, so every shard's sole replica expands to `replica1` and the identity collided — a peer shard could think it owned another shard's refresh and clobber it (the merge first failed the sharded test with `Cannot get temporary table without a current refresh directory`). Fixed by **qualifying `replica_name` with the shard name** (`<shard>/<replica>`) in the `RefreshTask` constructor: globally unique across the shared subtree, stable per process (so #104051's reconnect re-creation still works), and used only as znode **data** / equality operand — never as a path segment (paths use `shard_name`). Within-shard multi-replica election is unaffected (names stay distinct).
  - **Deliberate divergence from the `.62` port.** The split-out deferred `exchangeTargetTableAfterRefresh` is **version-guarded on the root znode** (mirroring #104051's CREATE-side version check), stronger than the `.62` port's unguarded exchange — closing a window #104051's model otherwise guards.
  - **Verification.** Build green; **6/6** stateless refreshable-MV neutrality tests pass (`02932_1`, `02932_2`, `03221`, `03258`, `03327`, `03760`) and the 2-shard `test_aiven_mv_refresh_sharded` causation test passes end-to-end (per-shard retention `shard1.tgt={1,2,3}`, `shard2.tgt={10,20,30}`).
  The `.62` history below is retained as the reconciliation baseline.
- **Status (`v26.3.10.62-lts` port, retained for reference).** **Single-node neutrality RESTORED.** The
  earlier escalation (the three tests "hung to a 600s timeout") was disproved by an
  A/B against a pre-066 base binary: it was a **test-harness artifact**
  (un-redirected `stdin` on `INSERT … VALUES`), not a 066 bug. **Five** port-time
  defects were fixed: a null-deref in `createRefreshDirectory`; a `#deps 0`
  layout-staleness garbling `system.view_refreshes` progress; and three single-node
  refresh-state-machine regressions (failed refreshes not recorded in the znode →
  `exception` never cleared; dropped per-cycle `interrupt_execution` reset →
  cancelled views never re-ran; missing `log_comment`/`try-catch` → failures absent
  from `system.query_log`). Neutrality: **8/8** stateless refreshable-MV tests pass.
  Causation: the added 2-shard integration test
  (`tests/integration/test_aiven_mv_refresh_sharded`) passes end-to-end against the
  locally-built 066 binary, asserting per-shard data retention (`shard1.tgt={1,2,3}`,
  `shard2.tgt={10,20,30}`). **Open item:** a single-node vs coordinated
  error-reporting asymmetry introduced by the failed-refresh fix is left for
  maintainer decision (dossier §11).
- **Refs.** [`patches/066-mv-refresh-sharded.md`](../../patches/066-mv-refresh-sharded.md),
  [`inventory.md`](inventory.md) rows 066 & 078.

### DDL-1 — `.tmp*` table-name namespace reserved behind a default-off `aiven_` server setting  ⚠ operational
- **Change.** The chain 062 ("Prohibit .tmp table creation") + 063 ("Use .tmp for all
  fake temporal tables") + 064 (the `CREATE OR REPLACE` internal exemption), squashed
  into one commit `patch-port(062,063,064)`. 062's two throws — non-internal `CREATE`
  of a `.tmp*` table (`InterpreterCreateQuery::doCreateTable`) and non-internal
  `RENAME … TO` a `.tmp*` name in a `Replicated` database
  (`InterpreterRenameQuery::executeToTables`) — are wrapped in a **new default-`false`
  server setting** `aiven_prohibit_tmp_table_creation`. 063 (rename the `CREATE OR
  REPLACE` temp prefix `_tmp_replace_` → `.tmp_replace_`) and 064 (mark that inner
  create internal so the guard exempts it) ship **unconditional**, as do the two
  internal-flips (`DatabaseReplicated::recoverLostReplica` create,
  `StorageMaterializedView::exchangeTargetTable` rename). The refresh temp
  `.tmp.inner_id.*` already lived under `.tmp*` upstream.
- **Backward-compat impact.** With the gate **off** (stock 26.3 default) behavior is
  byte-identical to upstream: a user can still create/rename `.tmp*` tables, exactly as
  before. 063's prefix change is engine-internal and ephemeral (the temp table is
  exchanged/dropped within the operation); no persisted DDL depends on it. The only
  observable change requires opting into the gate.
- **Integration action.** To reserve the `.tmp*` namespace in production (prevent user
  tables from colliding with engine temporaries and keep them backup-skippable by
  name), **add**
  `<aiven_prohibit_tmp_table_creation>true</aiven_prohibit_tmp_table_creation>` to
  Aiven's managed server config. **This is a config change, not code.** It is a
  server-level setting and cannot be overridden per-session.
- **Refs.** [`patches/062-prohibit-tmp-table-creation.md`](../../patches/062-prohibit-tmp-table-creation.md),
  [`inventory.md`](inventory.md) rows 062 / 063 / 064.

### REPL-5 — Custom replication parameters already rejected upstream; Aiven's hard guard moved code → config constraint  ⚠ operational
- **Change.** 26.3 ships `database_replicated_allow_replicated_engine_arguments`
  (session setting, `UInt64`, **default `0`**, `Settings.cpp:5637`). At
  `registerStorageMergeTree.cpp:263-294`, value `0` throws `BAD_ARGUMENTS` when a
  `ReplicatedMergeTree` in a `Replicated` database is created with an explicit
  `zookeeper_path` / `replica_name` that differs from the server defaults — the
  same rejection the 25.8 Aiven patch (058) enforced unconditionally. Patch 058 is
  dropped in favor of upstream + config.
- **Backward-compat impact.** A stock 26.3 server already rejects custom ZK
  path / replica name **by default**, but the upstream guard is a **session
  setting** a user can bypass (`SET database_replicated_allow_replicated_engine_arguments=1`),
  whereas 058 could not be bypassed. Doing nothing therefore *weakens* the
  guarantee from hard to soft. Two further deltas vs 058 (see dossier §2): upstream
  only enforces inside **Replicated databases** (058 also covered standalone
  replicated tables), and upstream compares against the **raw** setting template
  (which is *better* — it does not break `CREATE TABLE t1 AS t2`, whereas 058's
  expanded comparison could).
- **Integration action.** To restore 058's un-bypassable enforcement, pin the
  setting **non-overridable** in Aiven's managed config (it is already `0` by
  default), e.g. a `<constraints>` `<readonly/>` on
  `database_replicated_allow_replicated_engine_arguments` in **every** customer
  profile. **This is a required config change, not code.** **Open assumption:**
  this restores parity only if Aiven creates replicated tables exclusively inside
  Replicated databases (the 002/004 premise) — verify before relying on it; if not,
  the standalone-table scope gap must be revisited.
- **Refs.** [`patches/058-disallow-replication-parameter-customization.md`](../../patches/058-disallow-replication-parameter-customization.md),
  [`inventory.md`](inventory.md) row 058.

### REPL-6 — Transparent `MergeTree`→`ReplicatedMergeTree` conversion in Replicated DBs behind a default-off `aiven_` server setting  ⚠ operational
- **Change.** Patch 004 carries the transparent "write `ENGINE = MergeTree` in a
  `Replicated` database, get `ReplicatedMergeTree`" behavior. On the
  replica-execution (`SECONDARY_QUERY`) path, `StorageFactory::get` rewrites a
  non-replicated `*MergeTree` engine name (e.g. `MergeTree`, `SummingMergeTree`)
  to its registered `Replicated*` twin **before** engine instantiation, so every
  table in a Replicated database is self-replicating even if the caller wrote a
  plain `MergeTree` engine. On 26.3 the whole rewrite is wrapped in a **new
  default-`false` server setting** `aiven_replace_mergetree_with_replicated`
  acting as the master switch (clause (v): auto-transforming `MergeTree` for every
  Replicated database is a broad-blast-radius default change and must not ship on
  by default). The port also hardens the original 25.8 string-prepend with four
  gap fixes: gate via the setting; never rewrite on `ATTACH` (no silent re-engine
  of existing on-disk data); only rewrite to a `Replicated*` twin that is actually
  registered (no fabricated engine name); and preserve stored-DDL consistency (the
  rewrite mutates the same `ASTCreateQuery` that is persisted, so all replicas
  converge to identical `Replicated*` metadata).
- **Backward-compat impact.** With the gate **off** (stock 26.3 default) behavior
  is byte-identical to upstream: `ENGINE = MergeTree` stays `MergeTree`, no rewrite
  happens. The behavior is only live once the setting is opted into. The rewrite
  covers only the `*MergeTree` family; a `Log`/`StripeLog`/`TinyLog` disk engine in
  a Replicated database is **not** converted and would still diverge — that gap is
  closed by config, not code (see Integration action).
- **Integration action.** To restore the 25.8 transparent-convert UX in production,
  **add** `<aiven_replace_mergetree_with_replicated>true</aiven_replace_mergetree_with_replicated>`
  to Aiven's managed server config, and **keep** the upstream
  `database_replicated_allow_only_replicated_engine = 1` (its Cloud default) as the
  reject-backstop for the non-`MergeTree` disk engines 004 does not convert. **This
  is a config change, not code.** It is a server-level setting and cannot be
  overridden per-session.
  **REPL-1 coupling (load-bearing sequencing).** This setting is what makes the
  REPL-1 `internal_replication=true` flip safe: `internal_replication=true` is
  sound only when every table behind a Replicated-DB auto-cluster is
  `ReplicatedMergeTree`, otherwise a plain `MergeTree` receives a one-replica
  INSERT that never replicates (silent divergence). Enable
  `aiven_replace_mergetree_with_replicated=1` (and the reject-backstop)
  **before/with** flipping `internal_replication=true`, never after. Conversely, if
  004 is not enabled, do not flip REPL-1.
- **Open assumption.** This port assumes Aiven's control-plane / customers rely on
  the transparent-convert UX (rather than always writing `ReplicatedMergeTree`
  directly). If that assumption is false, the simpler disposition is to drop 004
  and rely on `database_replicated_allow_only_replicated_engine=1` alone (the
  002/058 pattern).
- **Refs.** [`patches/004-replace-mergetree-with-replicated.md`](../../patches/004-replace-mergetree-with-replicated.md),
  REPL-1 above, [`inventory.md`](inventory.md) row 004.

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

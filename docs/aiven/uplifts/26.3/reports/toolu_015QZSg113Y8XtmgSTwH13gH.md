<!-- subagentStop archive | source=transcript | status=completed | type=explore | ts=2026-06-12T09:46:58Z -->

I'll diff settings between v25.8.18.1-lts and v26.3.10.62-lts and scan all `*Settings.cpp` files for new gates.

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

# 26.3 vs 25.8 — New SETTINGS Gate Review (Aiven Security Screening)

Comparison bases: `v25.8.18.1-lts` → `v26.3.10.62-lts`. Primary source: `git diff … -- src/Core/Settings.cpp`, plus `ServerSettings.cpp`, `YTsaurusSettings.cpp`. No new `allow_*`/`enable_*` gates in `FormatFactorySettings.cpp`, `MergeTreeSettings.cpp`, or other `*Settings.cpp` files (MergeTree gained non-gate settings only).

Tier notation: `0` = PRODUCTION, `EXPERIMENTAL`, `BETA`, `SettingsTierType::OBSOLETE`.

---

## 1. New `allow_*` / `enable_*` / `allow_experimental_*` settings (present in 26.3, absent in 25.8)

### `src/Core/Settings.cpp` — 26 new session settings

| Setting | Default | Tier | DECLARE doc string (first line) |
|---------|---------|------|----------------------------------|
| `allow_calculating_subcolumns_sizes_for_merge_tree_reading` | `true` | PRODUCTION (`0`) | When enabled, ClickHouse will calculate the size of files required for each subcolumn reading for better task and block sizes calculation. |
| `allow_dynamic_type_in_join_keys` | `false` | PRODUCTION | Allows using Dynamic type in JOIN keys. Added for compatibility. It's not recommended to use Dynamic type in JOIN keys because comparison with other types may lead to unexpected results. |
| `allow_experimental_alias_table_engine` | `false` | EXPERIMENTAL | Allow to create table with the Alias engine. |
| `allow_experimental_database_paimon_rest_catalog` | `false` | EXPERIMENTAL | Allow experimental database engine DataLakeCatalog with catalog_type = 'paimon_rest' |
| `allow_experimental_expire_snapshots` | `false` | EXPERIMENTAL | Allow to execute experimental Iceberg command `ALTER TABLE ... EXECUTE expire_snapshots`. |
| `allow_experimental_json_lazy_type_hints` | `false` | EXPERIMENTAL | Enable experimental lazy type hints for JSON type. This feature allows optimizing JSON type conversions by deferring type hint evaluation. |
| `allow_experimental_nullable_tuple_type` | `false` | EXPERIMENTAL | Allows creation of Nullable Tuple columns in tables. |
| `allow_experimental_object_storage_queue_hive_partitioning` | `false` | EXPERIMENTAL | Allow to use hive partitioning with S3Queue/AzureQueue engines |
| `allow_experimental_polyglot_dialect` | `false` | EXPERIMENTAL | Enable polyglot SQL transpiler - transpiles SQL from 30+ dialects (MySQL, PostgreSQL, SQLite, Snowflake, DuckDB, etc.) into ClickHouse SQL. |
| `allow_fuzz_query_functions` | `false` | EXPERIMENTAL | Enables the `fuzzQuery` function that applies random AST mutations to a query string. |
| `allow_insert_into_iceberg` | `false` | BETA | Allow to execute `insert` queries into iceberg. *(alias: `allow_experimental_insert_into_iceberg`)* |
| `allow_nullable_tuple_in_extracted_subcolumns` | `false` | PRODUCTION | Controls whether extracted subcolumns of type `Tuple(...)` can be typed as `Nullable(Tuple(...))`. |
| `allow_special_serialization_kinds_in_output_formats` | `true` | PRODUCTION | Allows to output columns with special serialization kinds like Sparse and Replicated without converting them to full column representation. |
| `allow_statistics` | `true` | PRODUCTION | Allows defining columns with statistics and manipulate statistics. *(alias: `allow_experimental_statistics`)* |
| `enable_automatic_decision_for_merging_across_partitions_for_final` | `true` | PRODUCTION | If set, ClickHouse will automatically enable this optimization when the partition key expression is deterministic and all columns used in the partition key expression are included in the primary key. |
| `enable_full_text_index` | `true` | PRODUCTION | If set to true, allow using the text index. *(alias: `allow_experimental_full_text_index`)* |
| `enable_join_runtime_filters` | `true` | BETA | Filter left side by set of JOIN keys collected from the right side at runtime. |
| `enable_lazy_columns_replication` | `true` | PRODUCTION | Enables lazy columns replication in JOIN and ARRAY JOIN, it allows to avoid unnecessary copy of the same rows multiple times in memory. |
| `enable_materialized_cte` | `false` | EXPERIMENTAL | Enable materialized common table expressions, it will be preferred over enable_global_with_statement |
| `enable_positional_arguments_for_projections` | `false` | PRODUCTION | Enables or disables supporting positional arguments in PROJECTION definitions. |
| `enable_producing_buckets_out_of_order_in_aggregation` | `true` | PRODUCTION | Allow memory-efficient aggregation (see `distributed_aggregation_memory_efficient`) to produce buckets out of order. |
| `enable_time_time64_type` | `true` | PRODUCTION | Allows creation of Time and Time64 data types. *(alias: `allow_experimental_time_time64_type`)* |
| `filesystem_cache_allow_background_download` | `true` | PRODUCTION | Allow filesystem cache to enqueue background downloads for data read from remote storage. |
| `jemalloc_enable_profiler` | `false` | PRODUCTION | Enable jemalloc profiler for the query. Jemalloc will sample allocations and all deallocations for sampled allocations. |
| `parallel_replicas_allow_materialized_views` | `true` | PRODUCTION | Allow usage of materialized views with parallel replicas |
| `regexp_dict_allow_hyperscan` | `true` | PRODUCTION | Allow regexp_tree dictionary using Hyperscan library. |

### `src/Core/ServerSettings.cpp` — 4 new server settings

| Setting | Default | Tier | DECLARE doc string |
|---------|---------|------|-------------------|
| `allow_experimental_webassembly_udf` | `false` | EXPERIMENTAL | Enable experimental support for WebAssembly UDFs |
| `allow_impersonate_user` | `false` | OBSOLETE | Enable/disable the IMPERSONATE feature (EXECUTE AS target_user). The setting is deprecated. |
| `jemalloc_enable_global_profiler` | `false` *(via `Jemalloc::default_enable_global_profiler`)* | PRODUCTION | Enable jemalloc's allocation profiler for all threads. |
| `jemalloc_enable_background_threads` | `true` *(via `Jemalloc::default_enable_background_threads`)* | PRODUCTION | Enable jemalloc background threads. Jemalloc uses background threads to cleanup unused memory pages. |

### `src/Storages/YTsaurus/YTsaurusSettings.cpp` — 1 new engine setting

| Setting | Default | Tier | DECLARE doc string |
|---------|---------|------|-------------------|
| `enable_heavy_proxy_redirection` | `true` | PRODUCTION | Enable redirection to heavy proxies for heavy queries. See: https://ytsaurus.tech/docs/en/user-guide/proxy/http-reference#hosts |

---

## 2. Danger classification

### HIGH — external data / code-exec / network / filesystem / new engine access

| Setting | Why |
|---------|-----|
| **`allow_experimental_webassembly_udf`** (ServerSettings) | User-supplied WASM bytecode execution (`wasmtime`/`wasmedge`); classic code-exec surface. Guarded in `Context.cpp`. |
| **`allow_experimental_database_paimon_rest_catalog`** | Enables `DataLakeCatalog` with `catalog_type = 'paimon_rest'` — REST catalog + lake table access (`DatabaseDataLake.cpp`). |
| **`allow_insert_into_iceberg`** | Enables `INSERT` into Iceberg tables on object storage (writes external metadata/data). Renamed from `allow_experimental_insert_into_iceberg`, now BETA. |
| **`allow_experimental_expire_snapshots`** | Mutates Iceberg snapshot metadata via `ALTER TABLE … EXECUTE expire_snapshots` (`IcebergMetadata.cpp`). |
| **`allow_experimental_object_storage_queue_hive_partitioning`** | Enables hive-style partitioning on **S3Queue/AzureQueue** engines (`registerQueueStorage.cpp`) — external queue ingestion. |
| **`enable_heavy_proxy_redirection`** (YTsaurus) | Redirects heavy YTsaurus queries to alternate HTTP proxy hosts — network egress routing change when YTsaurus engine is used. |
| **`allow_fuzz_query_functions`** | Enables `fuzzQuery()` — random AST mutation of SQL strings; abuse/testing surface, not for production tenants. |

### MEDIUM — resource abuse / planner behavior / graduated exposure

| Setting | Why |
|---------|-----|
| **`filesystem_cache_allow_background_download`** | ON by default; triggers background remote-storage downloads outside query foreground — disk/network amplification. |
| **`enable_join_runtime_filters`** | ON by default (BETA); extra bloom-filter/set work per JOIN block; CPU/memory overhead, correctness sensitivity. |
| **`parallel_replicas_allow_materialized_views`** | ON by default; expands parallel-replicas to MV-backed queries — more distributed work/complexity. |
| **`enable_producing_buckets_out_of_order_in_aggregation`** | ON by default; doc warns "potentially higher memory usage" for skewed aggregations. |
| **`regexp_dict_allow_hyperscan`** | ON by default; Hyperscan regex in `regexp_tree` dictionaries — ReDoS/CPU abuse if untrusted patterns loaded. |
| **`allow_calculating_subcolumns_sizes_for_merge_tree_reading`** | ON by default; extra stat/size work per subcolumn read. |
| **`enable_lazy_columns_replication`** | ON by default; changes JOIN/ARRAY JOIN memory sharing semantics. |
| **`jemalloc_enable_profiler`** / **`jemalloc_enable_global_profiler`** | Allocation profiling overhead; `jemalloc_enable_global_profiler` exposes server-wide sampling. |
| **`jemalloc_enable_background_threads`** | ON by default (server); background memory page cleanup threads. |
| **`allow_experimental_polyglot_dialect`** | OFF by default; large Rust transpiler attack surface (30+ dialect parsers) before CH SQL execution — parser/transpiler bug class. |
| **`allow_experimental_alias_table_engine`** | OFF by default; `Alias` engine proxies all ops to a target table (`StorageAlias.cpp`) — potential indirection around table naming; grant-dependent. |

**Graduated existing settings now ON by default** (not new names, but security-relevant default flips — see §3):
`allow_statistics`, `allow_statistics_optimize`, `enable_full_text_index`, `enable_time_time64_type`, `enable_shared_storage_snapshot_in_query`, `enable_http_compression`.

### LOW — benign query-language / optimization / type features

| Setting | Why |
|---------|-----|
| `allow_experimental_json_lazy_type_hints` | JSON type-hint optimization only. |
| `allow_experimental_nullable_tuple_type` | New Nullable(Tuple) column type in DDL. |
| `allow_nullable_tuple_in_extracted_subcolumns` | Subcolumn nullability semantics; startup-profile only per doc. |
| `allow_dynamic_type_in_join_keys` | OFF; compatibility flag, doc discourages use. |
| `allow_special_serialization_kinds_in_output_formats` | Output-format optimization (Sparse/Replicated columns). |
| `enable_automatic_decision_for_merging_across_partitions_for_final` | FINAL query optimization heuristic. |
| `enable_materialized_cte` | OFF; CTE materialization planner choice. |
| `enable_positional_arguments_for_projections` | OFF; PROJECTION DDL syntax. |
| `enable_time_time64_type` | Graduated type enablement (LOW risk, but now always ON — see §3). |
| `allow_statistics` / `enable_full_text_index` | Graduated features; LOW direct security risk, behavior change. |
| `allow_impersonate_user` | OBSOLETE stub; deprecated IMPERSONATE, no effect. |

---

## 3. Defaults sanity (HIGH/MEDIUM only)

| Setting | Default | Safe? | Notes |
|---------|---------|-------|-------|
| `allow_experimental_webassembly_udf` | **OFF** | ✅ | Must explicitly enable code-exec. |
| `allow_experimental_database_paimon_rest_catalog` | **OFF** | ✅ | |
| `allow_insert_into_iceberg` | **OFF** | ✅ | |
| `allow_experimental_expire_snapshots` | **OFF** | ✅ | |
| `allow_experimental_object_storage_queue_hive_partitioning` | **OFF** | ✅ | |
| `allow_fuzz_query_functions` | **OFF** | ✅ | |
| `allow_experimental_polyglot_dialect` | **OFF** | ✅ | |
| `allow_experimental_alias_table_engine` | **OFF** | ✅ | |
| `enable_heavy_proxy_redirection` | **ON** | ⚠️ | **Already exposed** when YTsaurus table engine/settings are in use; redirects to heavy HTTP proxies. |
| `filesystem_cache_allow_background_download` | **ON** | ⚠️ | Background remote downloads enabled globally. |
| `enable_join_runtime_filters` | **ON** | ⚠️ | BETA feature active without opt-in. |
| `parallel_replicas_allow_materialized_views` | **ON** | ⚠️ | Only matters if parallel replicas already enabled. |
| `enable_producing_buckets_out_of_order_in_aggregation` | **ON** | ⚠️ | Memory trade-off, not a gate. |
| `regexp_dict_allow_hyperscan` | **ON** | ⚠️ | Hyperscan enabled for regexp_tree dicts by default. |
| `allow_calculating_subcolumns_sizes_for_merge_tree_reading` | **ON** | ⚠️ | Extra I/O; low security impact. |
| `enable_lazy_columns_replication` | **ON** | ⚠️ | Perf/memory semantics. |
| `jemalloc_enable_profiler` | **OFF** | ✅ | Per-query opt-in. |
| `jemalloc_enable_global_profiler` | **OFF** | ✅ | Server-wide profiler off. |
| `jemalloc_enable_background_threads` | **ON** | ⚠️ | Standard jemalloc behavior; perf not security. |

**Graduated defaults now ON (formerly experimental OFF)** — flag for Aiven policy review even though names aren't "new":
- `enable_time_time64_type`: `false` → **`true`**
- `allow_statistics` / `allow_statistics_optimize`: `false` → **`true`**, tier EXPERIMENTAL → PRODUCTION
- `enable_full_text_index`: `false` → **`true`**
- `enable_shared_storage_snapshot_in_query`: `false` → **`true`**
- `enable_http_compression`: `false` → **`true`**

---

## 4. Removed / graduated / obsolete settings (in 25.8, changed in 26.3)

### Removed active gates (replaced or obsolete)

| Old setting (25.8) | 26.3 fate |
|--------------------|-----------|
| `allow_experimental_time_time64_type` | **Graduated** → `enable_time_time64_type` (PRODUCTION, default **`true`**) |
| `allow_experimental_statistics` | **Graduated** → `allow_statistics` (PRODUCTION, default **`true`**) |
| `allow_experimental_full_text_index` | **Graduated** → `enable_full_text_index` (PRODUCTION, default **`true`**) |
| `allow_experimental_insert_into_iceberg` | **Renamed/graduated** → `allow_insert_into_iceberg` (BETA, still **`false`**) |
| `allow_statistics_optimize` | **Graduated** tier EXPERIMENTAL→PRODUCTION, default **`false`→`true`** (name retained) |
| `allow_experimental_parallel_reading_from_replicas` | **Graduated** tier BETA→PRODUCTION (default unchanged `0`) |
| `allow_experimental_live_view` | **Obsolete** — moved to `MAKE_OBSOLETE` in Settings.cpp |
| `allow_experimental_object_type` | **Obsolete** — moved to `MAKE_OBSOLETE` |
| `allow_not_comparable_types_in_order_by` | **Removed** — replaced by `use_variant_default_implementation_for_comparisons` |
| `allow_not_comparable_types_in_comparison_functions` | **Removed** — same replacement |
| `enable_zstd_qat_codec` | **Removed** |
| `enable_deflate_qpl_codec` | **Removed** |

### New obsolete marker
- `allow_impersonate_user` — added in ServerSettings as **OBSOLETE** tier (deprecated IMPERSONATE stub).

---

## 5. Cross-reference to engines / functions / catalogs

| Setting | Gated component | Aiven `REGISTER_*` correlation |
|---------|-------------------|-------------------------------|
| `allow_experimental_alias_table_engine` | **`Alias` table engine** (`registerStorageAlias` / `StorageAlias.cpp`) | Alias left **unconditional** in patch-052; only TimeSeries/ObjectStorage/DataLake wrapped |
| `allow_experimental_database_paimon_rest_catalog` | **`DataLakeCatalog`** engine, `catalog_type = 'paimon_rest'` | DataLake catalog family (`REGISTER_DATALAKE_*`) |
| `allow_insert_into_iceberg` | **Iceberg writes** (`INSERT` into Iceberg table functions / storages) | `REGISTER_DATALAKE_FUNCTION`, object-storage/lake flags |
| `allow_experimental_expire_snapshots` | **Iceberg maintenance** (`ALTER TABLE … EXECUTE expire_snapshots`) | Same lake/Iceberg stack |
| `allow_experimental_object_storage_queue_hive_partitioning` | **S3Queue / AzureQueue** engines with hive partitioning | Object-storage queue engines |
| `allow_experimental_polyglot_dialect` | **`polyglot` SQL dialect** (+ `polyglot_dialect` string setting, EXPERIMENTAL) | Build-time: `TARGET ch_rust::polyglot` in `configure_config.cmake` |
| `allow_fuzz_query_functions` | **`fuzzQuery()`** function (`Functions/fuzzQuery.cpp`) | No REGISTER flag; function always compiled |
| `allow_experimental_webassembly_udf` | **WebAssembly UDFs** (`Interpreters/WebAssembly/*`, `UserDefinedWebAssembly.*`) | Build-time: `USE_WASMTIME` / `USE_WASMEDGE` |
| `enable_heavy_proxy_redirection` | **YTsaurus** table engine HTTP client (`StorageYTsaurus.cpp`) | `REGISTER_YTSAURUS_*` family (patches 071+) |
| `allow_experimental_json_lazy_type_hints` | **JSON** type lazy hints | No separate engine |
| `enable_full_text_index` | **Text index** (MergeTree secondary index) | N/A |
| `allow_statistics` | **Column statistics** on MergeTree | N/A |

---

### Compact LOW-only list (22 session + 1 obsolete server)

`allow_experimental_json_lazy_type_hints`, `allow_experimental_nullable_tuple_type`, `allow_nullable_tuple_in_extracted_subcolumns`, `allow_dynamic_type_in_join_keys`, `allow_special_serialization_kinds_in_output_formats`, `enable_automatic_decision_for_merging_across_partitions_for_final`, `enable_materialized_cte`, `enable_positional_arguments_for_projections`, `enable_time_time64_type`, `allow_statistics`, `enable_full_text_index`, `enable_lazy_columns_replication`, `allow_impersonate_user` (OBSOLETE/no-op).

---

**Aiven action priorities:** (1) gate **`allow_experimental_webassembly_udf`** at server level if WASM contrib is built; (2) review **`enable_heavy_proxy_redirection`** default ON for YTsaurus deployments; (3) audit graduated **ON-by-default** flags (`enable_time_time64_type`, statistics, full-text index, `regexp_dict_allow_hyperscan`, `filesystem_cache_allow_background_download`, `enable_join_runtime_filters`); (4) keep existing OFF gates for Paimon REST catalog, Iceberg expire/insert, S3Queue hive partitioning, polyglot, fuzzQuery, Alias engine.

[REDACTED]

# New-surface gating screen (25.8 → 26.3)

## Purpose

The `REGISTER_*` engine/function expose-gate family (patches `045` → `051` → `052` →
`070` → `071` → `075`) is the **port** of Aiven's 25.8 gating mechanism onto 26.3. That
work is now complete. This document answers the *next* question:

> What is **new in 26.3** that gives a SQL user reachable, potentially-dangerous
> functionality (code execution, external data access, network egress, filesystem,
> new server endpoint), and is **not** covered by the ported `REGISTER_*` contract?

Crucially: none of the surfaces below have a corresponding 25.8-aiven patch — they did
not exist in 25.8. So they are **not port work**; they are **net-new gating decisions**
for the maintainer to take per managed-service policy. This screen surfaces and
classifies them; it does not author gates.

Method: pure-upstream diff `v25.8.18.1-lts` → `v26.3.10.62-lts` (no Aiven patches to
confound), across the registration files, `Settings.cpp`/`ServerSettings.cpp`,
`.gitmodules`, and `config.h.in` `USE_*`/`REGISTER_*` macros. Full investigation
threads: WASM UDFs, settings, and dependencies were each screened in depth (links at
the bottom). Every line/symbol cited below was re-verified against the working tree on
`v26.3.10.62-lts-aiven-dev`.

## Baseline: what the ported contract already covers

54 `REGISTER_*` flags in `src/Common/config.h.in` gate the *registration* of
already-compiled storages, table functions, dict sources, and dict layouts (S3, Azure,
HDFS, Iceberg, DataLake, ObjectStorage, TimeSeries, YTsaurus, ArrowFlight, MongoDB,
Redis, NATS, FileLog, URL/file/remote/executable functions, all dict sources, …). Each
defaults ON via `option(... ON)` in `src/configure_config.cmake`.

## Registration-level diff — essentially stable

| File | Added in 26.3 | Removed | Gating impact |
|---|---|---|---|
| `registerStorages.cpp` | `registerStorageAlias` | `registerStorageLiveView` | Alias left **unconditional** (line 149); LiveView had no flag → no dangling |
| `registerTableFunctions.cpp` | `Primes`, `MergeTreeAnalyzeIndexes`, `MergeTreeTextIndex` | — | local / introspection only — no external I/O or code-exec → out of scope |
| `registerDictionaries.cpp` | — | — | none |

So the registration surface barely moved. The real new surface is elsewhere.

## Net-new surfaces (no Aiven gate today)

Ranked by inherent risk; "default" = reachability on a stock Linux build.

| # | Surface | New artifacts | Compile gate (default) | Runtime gate (default) | Existing Aiven gate? | Class |
|---|---|---|---|---|---|---|
| 1 | **WebAssembly UDFs** — `CREATE FUNCTION … LANGUAGE WASM`, `system.webassembly_modules` | `src/Interpreters/WebAssembly/**`, `UserDefinedWebAssembly.*`, `WasmModuleManager.*` | `USE_WASMTIME` / `USE_WASMEDGE` (**ON** on Linux amd64/arm) | `allow_experimental_webassembly_udf` (server, **`false`**, EXPERIMENTAL) — `ServerSettings.cpp:1304` | **None** | **HIGH** |
| 2 | **Polyglot SQL transpiler** — `dialect = polyglot`, 30+ foreign SQL dialects | `src/Parsers/Polyglot/**`, Rust `polyglot` crate | `USE_POLYGLOT` (**ON** on Linux) | `allow_experimental_polyglot_dialect` (**`false`**) — `Settings.cpp:7641` | **None** | **MEDIUM** |
| 3 | **ClickStack web UI** — `GET/HEAD /clickstack/*` | `contrib/clickstack`, `ClickStackUIRequestHandler` | none (always linked) | **none** — handler unconditional, `HTTPHandlerFactory.cpp:370` | **None** | **MEDIUM** |
| 4 | **TOTP 2FA** — `OneTimePassword` auth path | `contrib/libcotp` | `USE_SSL && USE_LIBCOTP` (**ON**) | per-user in `users.xml`/RBAC (opt-in) | **None** | **MEDIUM (auth)** |
| 5 | **Alias table engine** — `ENGINE = Alias(target)` | `registerStorageAlias` (unconditional), `StorageAlias.cpp` | none | `allow_experimental_alias_table_engine` (**`false`**) — `Settings.cpp` | unconditional reg (052 left it so) | **LOW/MEDIUM** |
| 6 | **XRay instrumentation** — `SYSTEM INSTRUMENT …`, `system.instrumentation` | `USE_XRAY` | `ENABLE_XRAY` (**ON** Linux) | `SYSTEM_INSTRUMENT_*` privilege | none | **LOW** |
| 7 | new local TFs `primes` / `mergeTreeAnalyzeIndexes` / `mergeTreeTextIndex` | — | none | none | none | **LOW (out of scope)** |

### Notes that change the risk calculus

- **#1 WASM is the one genuine HIGH gap.** It is arbitrary (sandboxed) guest code
  execution with module upload via `INSERT INTO system.webassembly_modules` and disk
  persistence under `user_scripts/wasm/`. It is **distinct from** `REGISTER_EXECUTABLE_*`
  (those gate the `executable()` table function and the `Executable` table engine, not
  WASM). The only thing standing in front of it today is the experimental server setting
  defaulting OFF. WasmEdge (if `webassembly_udf_engine=wasmedge`) registers WASI, which
  can expose guest filesystem/network — wasmtime (the default) builds with
  `WASMTIME_FEATURE_WASI OFF`.
- **#3 ClickStack is less novel than it first looks.** Its handler is registered
  **exactly like the existing always-on `/play` (line 335), `/dashboard` (341), and
  `/binary` (348)** web UIs — `attachNonStrictPath`, unconditional, serving pre-gzipped
  static SPA assets. It is new *content*, but not a new *exposure pattern*: whatever
  Aiven already does to control `/play` reachability (network/proxy layer) covers it.
  Re-rated from the screen's initial HIGH to **MEDIUM** on that basis.
- **#5 Alias** was deliberately left unconditional during the `052` port (only
  TimeSeries/ObjectStorage/DataLake were wrapped); it is gated upstream by an
  experimental setting defaulting OFF.

## Graduated-to-default-ON settings (policy review, not a gate gap)

Formerly-experimental features that 26.3 turned **ON by default** (a behavior change,
not a new exposure to gate). Worth a managed-config policy decision:

| Setting (26.3 name) | 25.8 | 26.3 default | Note |
|---|---|---|---|
| `enable_time_time64_type` (was `allow_experimental_time_time64_type`) | OFF | **ON** | new `Time`/`Time64` types |
| `allow_statistics` (+`allow_statistics_optimize`) | OFF/EXP | **ON**/PROD | column statistics |
| `enable_full_text_index` (was `allow_experimental_full_text_index`) | OFF | **ON** | text secondary index |
| `enable_shared_storage_snapshot_in_query` | OFF | **ON** | |
| `enable_http_compression` | OFF | **ON** | |
| `regexp_dict_allow_hyperscan` | — | **ON** | Hyperscan in `regexp_tree` dicts — ReDoS surface if untrusted patterns |
| `filesystem_cache_allow_background_download` | — | **ON** | background remote fetches |
| `enable_join_runtime_filters` | — | **ON** (BETA) | extra per-JOIN work |

Several genuinely-dangerous new settings are correctly **OFF** by default and need no
action beyond *not enabling them*: `allow_experimental_database_paimon_rest_catalog`,
`allow_insert_into_iceberg`, `allow_experimental_expire_snapshots`,
`allow_experimental_object_storage_queue_hive_partitioning`, `allow_fuzz_query_functions`.
One to flag for YTsaurus deployments: `enable_heavy_proxy_redirection` (default **ON**,
changes proxy egress routing) — `YTsaurusSettings.cpp`.

## Recommendations (for maintainer decision — out of the port scope)

1. **WASM UDFs (HIGH).** Decide the managed-service posture. Options, cheapest → strongest:
   (a) rely on `allow_experimental_webassembly_udf=false` staying off in the control-plane
   config (already the upstream default); (b) build with `-DENABLE_WASMTIME=OFF
   -DENABLE_WASMEDGE=OFF` for defense-in-depth (stubs throw `SUPPORT_IS_DISABLED`; the C++
   glue still links); (c) author a **net-new** `REGISTER_WEBASSEMBLY_UDF`-style gate
   wrapping `Context::initWasmModuleManager()` (`Context.cpp:3696`) + the
   `attachSystemTables` block, if Aiven wants build-time gating consistent with the
   `REGISTER_*` family. Recommend (a)+(b) now; (c) only if the family convention is
   desired. If ever enabled: keep `webassembly_udf_engine=wasmtime`, force
   `webassembly_udf_max_fuel > 0` (0 = unbounded), and do not grant `CREATE_FUNCTION` to
   tenants. **→ Detailed design: [`REGISTER_WEBASSEMBLY_UDF` proposal](../../proposals/2026-06-12-webassembly-udf-register-gate.md)** (recommends option (c): a build gate at the single `initWasmModuleManager` choke point).
2. **Polyglot (MEDIUM).** OFF by default; consider `-DENABLE_POLYGLOT=OFF` in prod builds
   to drop the foreign-SQL transpiler attack surface entirely.
3. **ClickStack (MEDIUM).** Confirm it is covered by the same network/proxy control as
   `/play` and `/dashboard`; no code change needed if so. (Open follow-up: confirm whether
   the SPA only calls same-origin CH HTTP or phones out — that would raise the rating.)
4. **TOTP / libcotp (MEDIUM auth).** Acceptable if TOTP 2FA is desired; otherwise
   `-DENABLE_LIBCOTP=OFF`. No data-egress surface.
5. **Graduated-ON settings.** Review the table above against managed-config policy; pin
   the ones Aiven wants OFF in the control plane (these are config decisions, not patches).
6. **Out of scope:** Alias engine (gated OFF upstream), XRay (privileged), new local TFs.

None of the above is uplift-port work. If Aiven decides any should become a build gate,
that is a **new patch authored fresh against 26.3**, tracked separately from the
`045…075` family.

## Investigation threads

- WASM UDF deep-dive: [WASM screen](5b12109e-9df4-4efb-8476-ccc29a436233)
- Settings deep-dive: [settings screen](65b0c2b0-fa06-4c17-b17a-0acfd7b88e1b)
- Dependencies deep-dive: [contrib screen](95e4d576-2f73-412b-8b52-75cb746e6de2)

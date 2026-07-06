<!-- subagentStop archive | source=transcript | status=completed | type=explore | ts=2026-06-12T09:48:37Z -->

I'll compare the two LTS tags for new contrib submodules, `USE_*` macros, and what user-facing surface each unlocks.

[REDACTED]

Tracing each new `USE_*` macro to its cmake options, defaults, and user-facing registration points.

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

[REDACTED]

# 26.3 vs 25.8 — New optional dependencies & reachable surface

Comparison bases: `v25.8.18.1-lts` → `v26.3.10.62-lts` (pure upstream tags). Working tree read at 26.3 for call-site detail.

**Net `USE_*` delta:** +7 new, −2 removed (`USE_QPL`, `USE_QATLIB` — Intel QAT path dropped).

---

## 1. New contrib submodules in `.gitmodules`

| Submodule | One-line purpose |
|-----------|------------------|
| `contrib/wasmtime` | WasmTime WebAssembly VM/JIT (Rust); default engine for WASM UDFs |
| `contrib/wasmedge` | WasmEdge WebAssembly VM; alternate WASM UDF engine (registers WASI) |
| `contrib/spdlog` | Header-only logging used internally by WasmEdge build (`SPDLOG_FMT_EXTERNAL`) |
| `contrib/libcotp` | RFC 6238 TOTP/HOTP library for user password second-factor |
| `contrib/clickstack` | Pre-built static assets for embedded ClickStack observability UI |
| `contrib/StringZilla` | SIMD string search / memory helpers (internal perf) |
| `contrib/simdcomp` | SIMD integer bitpacking/compression (replaces `FastPFOR` on Linux amd64) |
| `contrib/base64` | Base64 codec fork replacing `contrib/aklomp-base64` (same role, new vendor) |

**Not new (URL/fork moves only):** `zstd`, `thrift`, `brotli`, `msgpack-c`, `xz`, `vectorscan`, `mongo-cxx-driver`, `crc32c`, AWS SDK submodules (reordered).

**Removed in 26.3:** `qpl`, `idxd-config`, `QAT-ZSTD-Plugin`, `qatlib`, `FastPFOR`, `simde`, `incbin`, `aklomp-base64`.

---

## 2. New `USE_*` compile macros (`config.h.in`)

| Macro | Library / gate | What it enables |
|-------|----------------|-----------------|
| `USE_WASMTIME` | `ch_rust::wasmtime` ← `contrib/wasmtime` + Rust crate | WasmTime backend for experimental WASM UDFs |
| `USE_WASMEDGE` | `ch_contrib::wasmedge` ← `contrib/wasmedge` | WasmEdge backend for WASM UDFs (WASI enabled in engine config) |
| `USE_POLYGLOT` | `ch_rust::polyglot` (Rust crate `polyglot`) | Foreign-SQL → ClickHouse SQL transpiler (`polyglot_transpile`) |
| `USE_LIBCOTP` | `ch_contrib::libcotp` ← `contrib/libcotp` | TOTP generation/validation for user OTP 2FA |
| `USE_XRAY` | LLVM XRay instrumentation (`cmake/xray_instrumentation.cmake`) | `SYSTEM INSTRUMENT ADD/REMOVE`, `system.instrumentation` |
| `USE_SIMDCOMP` | `ch_contrib::simdcomp` ← `contrib/simdcomp` | SIMD bitpacking in MergeTree text posting-list codec |
| `USE_SIMSIMD` | `ch_contrib::simsimd` (submodule existed in 25.8; macro is new) | SIMD paths in vector distance functions + USearch integration |

Set in `src/configure_config.cmake` when respective CMake targets exist (lines 170–172, 221–231).

**Submodules without `USE_*` but new in 26.3:**
- `clickstack` — always linked (`ch_contrib::clickstack_resources`), no macro
- `StringZilla` — always linked (`ch_contrib::stringzilla`), no macro
- `spdlog` — WasmEdge-only transitive dep

---

## 3. Reachable surface per new dependency

### HIGH priority

#### `USE_WASMTIME` + `USE_WASMEDGE` — WebAssembly UDFs

| Surface | Guard | File + symbol |
|---------|-------|---------------|
| Server setting | `allow_experimental_webassembly_udf` (default **false**) | `src/Core/ServerSettings.cpp` |
| Engine selection | `webassembly_udf_engine` = `"wasmtime"` \| `"wasmedge"` | `src/Core/ServerSettings.cpp` |
| Module manager init | `Context::initWasmModuleManager()` | `src/Interpreters/Context.cpp:3696` |
| Engine factory | `createEngine()` → `WasmTimeRuntime` / `WasmEdgeRuntime` | `src/Interpreters/WasmModuleManager.cpp:129` |
| Runtime compile/load | `WasmTimeRuntime::compileModule`, `WasmEdgeRuntime::compileModule` | `src/Interpreters/WebAssembly/WasmTimeRuntime.cpp`, `WasmEdgeRuntime.cpp` |
| Host API (guest imports) | `getHostFunction()` exports `clickhouse_log`, `clickhouse_throw`, `clickhouse_random`, `clickhouse_server_version` | `src/Interpreters/WebAssembly/HostApi.cpp:138` |
| DDL | `CREATE FUNCTION … LANGUAGE WASM` via `ParserCreateFunctionQuery` → `ASTCreateWasmFunctionQuery` | `src/Parsers/ParserCreateFunctionQuery.cpp:82` |
| UDF factory | `UserDefinedWebAssemblyFunctionFactory::addOrReplace` / `get` | `src/Functions/UserDefined/UserDefinedWebAssembly.cpp:647` |
| Query resolution | `UserDefinedWebAssemblyFunctionFactory::instance().get` | `src/Analyzer/Resolve/resolveFunction.cpp:1232` |
| System table (INSERT modules) | `StorageSystemWasmModules` attached when manager exists | `src/Storages/System/attachSystemTables.cpp:291` |
| Module persistence | `WasmModuleManager::saveModule` on `user_scripts` disk under `wasm/` | `src/Interpreters/WasmModuleManager.cpp:151` |

**WasmEdge note:** `getWasmEdgeVmConfig()` registers `WasmEdge_HostRegistration_Wasi` (`WasmEdgeRuntime.cpp:221`) — potential guest filesystem/network if engine switched to `wasmedge`. WasmTime build explicitly sets `WASMTIME_FEATURE_WASI OFF` (`rust/workspace/wasmtime/CMakeLists.txt:34`).

#### `contrib/clickstack` — new HTTP endpoint (no `USE_*`)

| Surface | Guard | File + symbol |
|---------|-------|---------------|
| HTTP GET/HEAD `/clickstack/*` | **None** (unconditional) | `HTTPHandlerFactory::addCommonDefaultHandlersFactory` → `ClickStackUIRequestHandler` at `src/Server/HTTPHandlerFactory.cpp:370` |
| Static asset handler | `ClickStackUIRequestHandler::handleRequest` | `src/Server/WebUIRequestHandler.cpp:156` |
| Build embed | `ch_contrib::clickstack_resources` | `contrib/clickstack-cmake/CMakeLists.txt`, linked from `src/CMakeLists.txt:370` |

Absent entirely in 25.8 (`HTTPHandlerFactory.cpp` has no `clickstack`).

---

### MEDIUM priority

#### `USE_POLYGLOT` — SQL dialect transpiler

| Surface | Guard | File + symbol |
|---------|-------|---------------|
| Session setting | `allow_experimental_polyglot_dialect` (default **false**) | `src/Core/Settings.cpp` |
| Dialect setting | `polyglot_dialect` (e.g. `sqlite`, `mysql`, `postgresql`) | `src/Core/Settings.cpp` |
| Parser path | `ParserPolyglotQuery::parseImpl` → `polyglot_transpile` / `polyglot_free_pointer` | `src/Parsers/Polyglot/ParserPolyglotQuery.cpp:74` |
| Query dispatch | `executeQuery` selects `ParserPolyglotQuery` when `dialect = polyglot` | `src/Interpreters/executeQuery.cpp:1180` |
| Client | `ClientBase` uses `ParserPolyglotQuery` | `src/Client/ClientBase.cpp:415` |

No `register*` symbol — bypasses normal lexer for foreign SQL, transpiles to CH SQL, then `ParserQuery` / `tryParseQuery`. Expands reachable SQL surface once enabled.

#### `USE_LIBCOTP` — TOTP second factor

| Surface | Guard | File + symbol |
|---------|-------|---------------|
| OTP verify | `checkOneTimePassword` → `getOneTimePassword` → `get_totp_at` (libcotp) | `src/Access/Common/OneTimePassword.cpp:148`, `:130` |
| Auth path | `Authentication::authenticate` calls `checkOneTimePassword` | `src/Access/Authentication.cpp:192` |
| Config parse | `UsersConfigParser` sets `OneTimePasswordSecret` | `src/Access/UsersConfigParser.cpp:181` |
| System introspection | `StorageSystemUsers` exposes OTP params | `src/Storages/System/StorageSystemUsers.cpp:174` |

Requires compile-time `USE_SSL && USE_LIBCOTP`. Not a new engine/format/protocol, but new auth verification path.

---

### LOW priority (internal / optimization)

| Macro / lib | Surface | Guard |
|-------------|---------|-------|
| `USE_SIMDCOMP` | `BitpackingBlockCodec` → `MergeTreeIndexTextPostingListCodec` | Internal MergeTree text-index on-disk format; no SQL registration |
| `USE_SIMSIMD` | `TupleOrArrayFunctionL2DistanceTransposed`, `CosineDistanceTransposed` in `distanceTransposed.cpp`; registered via `vectorFunctions.cpp` `registerFunction<…Transposed>` | Existing distance functions, faster SIMD path only |
| `USE_XRAY` | `InterpreterSystemQuery` cases `INSTRUMENT_ADD`/`REMOVE`; `attach<StorageSystemInstrumentation>` | Privilege `SYSTEM_INSTRUMENT_ADD/REMOVE`; dev/profiling |
| `StringZilla` | `StringSearcher.h`, `clickhouse_common_io` | Internal string ops |
| `spdlog` | WasmEdge compile only | No user surface |

---

## 4. Danger classification

| Item | Class | Justification |
|------|-------|---------------|
| **WASM UDFs** (`USE_WASMTIME`/`USE_WASMEDGE`) | **HIGH** | Arbitrary guest code execution in sandbox; module upload via `system.webassembly_modules` INSERT; persistence on `user_scripts` disk; WasmEdge path enables WASI host registration |
| **ClickStack** (`contrib/clickstack`) | **HIGH** | New always-on HTTP endpoint `/clickstack` on the same server port — new attack surface (static SPA may call back into CH APIs from browser context); no compile or runtime gate |
| **Polyglot** (`USE_POLYGLOT`) | **MEDIUM** | Transpiler/parser on untrusted foreign SQL input; output fed into full CH parser — amplification / logic-bypass risk; gated by `allow_experimental_polyglot_dialect` |
| **libcotp** (`USE_LIBCOTP`) | **MEDIUM** | New auth verification path (TOTP); crypto/parser on secrets; affects login boundary, not data egress |
| **XRAY** (`USE_XRAY`) | **LOW** | Internal instrumentation; privileged `SYSTEM` commands only |
| **simdcomp / simsimd / StringZilla** | **LOW** | Performance-only; no new protocols or egress |

---

## 5. Default-enabled? (standard Linux amd64, `ENABLE_LIBRARIES=ON`, no sanitizer)

Master switch: `option(ENABLE_LIBRARIES … ON)` in root `CMakeLists.txt:383`.

| Dependency | CMake option | Default ON in std Linux amd64? | Notes |
|------------|--------------|--------------------------------|-------|
| **WasmTime** | `ENABLE_WASMTIME` `${ENABLE_LIBRARIES}` | **Yes** | Needs `ENABLE_RUST`; skipped on MSAN, FreeBSD, S390X, `NO_ARMV81_OR_HIGHER` |
| **WasmEdge** | `ENABLE_WASMEDGE` `${ENABLE_LIBRARIES}` | **Yes** | Skipped on MUSL, Darwin, PPC64LE, S390X, FreeBSD |
| **Polyglot** | `ENABLE_POLYGLOT` `${ENABLE_LIBRARIES}` | **Yes** | Needs Rust workspace; FreeBSD skips all Rust |
| **libcotp** | `ENABLE_LIBCOTP` `${ENABLE_LIBRARIES}` | **Yes** | Requires `ENABLE_SSL`; skipped on FreeBSD |
| **simdcomp** | `ENABLE_SIMDCOMP` `${ENABLE_LIBRARIES}` | **Yes** | **Linux amd64 only** |
| **SimSIMD** | `ENABLE_SIMSIMD` `${ENABLE_LIBRARIES}` | **Yes** | |
| **XRay** | `ENABLE_XRAY` | **Yes** (explicit `ON`) | Forced **OFF** if sanitizer, MUSL, or not Linux amd64/aarch64 |
| **ClickStack** | *(none)* | **Always built** | `add_contrib(clickstack-cmake clickstack)` unconditional |

**Runtime defaults (HIGH/MEDIUM compile-on but off by default at runtime):**
- WASM UDFs: `allow_experimental_webassembly_udf = false`
- Polyglot: `allow_experimental_polyglot_dialect = false`
- ClickStack: **no runtime disable** — endpoint live whenever HTTP interface is up

---

## 6. Gating correlation vs Aiven `REGISTER_*`

Existing Aiven compile gates (from `src/configure_config.cmake:240–299`) cover engines (S3/Azure/HDFS/Iceberg/ObjectStorage/TimeSeries/YTsaurus/ArrowFlight/MongoDB/Redis/NATS/FileLog/…), table functions (url/file/remote/executable/S3/…), and dict sources — **none** of the new HIGH/MEDIUM surfaces below.

| New surface | Covered by existing `REGISTER_*`? | Gap |
|-------------|-------------------------------------|-----|
| **WASM UDFs** (`CREATE FUNCTION … LANGUAGE WASM`, `system.webassembly_modules`) | **No** | Separate from `REGISTER_EXECUTABLE_FUNCTION` / `REGISTER_EXECUTABLE_TABLE_ENGINE` (those gate `executable()` table function and `Executable` **table engine**, not WASM UDFs). Needs new flag(s) e.g. `REGISTER_WEBASSEMBLY_UDF` + compile `-DUSE_WASMTIME=0 -DUSE_WASMEDGE=0` and/or server-setting enforcement |
| **ClickStack `/clickstack` HTTP UI** | **No** | Unconditional handler registration; needs new compile gate or HTTP handler guard |
| **Polyglot dialect** | **No** | Only upstream `allow_experimental_polyglot_dialect`; needs `REGISTER_POLYGLOT` or `-DENABLE_POLYGLOT=OFF` in prod builds |
| **libcotp / TOTP 2FA** | **No** | Auth enhancement; disable via `-DENABLE_LIBCOTP=OFF` if fleet policy rejects TOTP in users.xml |
| **XRAY instrumentation** | **No** | Low priority; disable via `-DENABLE_XRAY=OFF` |
| **simdcomp / simsimd** | **No** | LOW; optimization-only |

**Removed surface (25.8 → 26.3):** Intel QAT (`USE_QPL`, `USE_QATLIB`) and `FastPFOR` removed — reduces hardware-accelerated codec attack surface, not a new gate concern.

---

## Summary for Aiven review (action order)

1. **WASM UDFs** — compile ON by default on Linux; runtime OFF by default. Highest risk: code exec + disk write for modules. No `REGISTER_*` coverage. WasmEdge + WASI is especially sensitive if `webassembly_udf_engine=wasmedge`.
2. **ClickStack** — compile-always, HTTP-always, **zero Aiven gate** — treat as new server endpoint.
3. **Polyglot** — compile ON; runtime experimental setting only — consider compile-time `-DENABLE_POLYGLOT=OFF` or new `REGISTER_*` for managed service.
4. **libcotp** — MEDIUM auth boundary; likely acceptable if SSL+TOTP desired, but not gated today.
5. **Internal libs** (simdcomp, simsimd, StringZilla, XRAY) — LOW; optional hardening via existing `ENABLE_*` switches.

---

**Today you learned:** 26.3 adds seven `USE_*` macros and eight submodules; only three open meaningful user surface (WASM, Polyglot, ClickStack), and ClickStack is the only one with no runtime experimental gate.

**Rule of thumb:** For Aiven, treat “new submodule” and “new `USE_*`” separately — `clickstack` proves a dependency can ship user-reachable HTTP surface without any `USE_*` macro at all.

**Next rabbit hole:** Trace what API endpoints the embedded ClickStack SPA calls (same-origin CH HTTP vs external telemetry) to refine the ClickStack HIGH classification.

[REDACTED]

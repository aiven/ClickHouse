<!-- subagentStop archive | source=transcript | status=completed | type=explore | ts=2026-06-12T09:47:26Z -->

I'll audit the WASM UDF subsystem for 26.3 vs 25.8: existence, reachability, compile/runtime gates, and registration points.

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

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

# WebAssembly UDF Subsystem — Aiven Security/Gating Review (v25.8 → v26.3)

## 1. Existence & scope

**Confirmed: absent in `v25.8.18.1-lts`, present in `v26.3.10.62-lts`.**

Evidence:
- `git show v25.8.18.1-lts:src/Interpreters/WebAssembly/` → *fatal: path exists on disk, but not in tag*
- `v26.3` adds ~3,616 lines across 20 new files (full diff stat for core paths)
- `v25.8` `ServerSettings.cpp` has **no** `webassembly`/`wasm` settings (`NO_MATCH_IN_25.8`)
- `v25.8` `src/Functions/UserDefined/` lacks `UserDefinedWebAssembly.{cpp,h}`

### Source locations (26.3)

| Area | Path |
|------|------|
| UDF implementation | `/home/tilman.moeller/projects/ClickHouse/src/Functions/UserDefined/UserDefinedWebAssembly.cpp` (817 lines), `.h` |
| Module manager | `/home/tilman.moeller/projects/ClickHouse/src/Interpreters/WasmModuleManager.cpp`, `.h` |
| WASM runtime engines | `/home/tilman.moeller/projects/ClickHouse/src/Interpreters/WebAssembly/` (`WasmTimeRuntime.*`, `WasmEdgeRuntime.*`, `WasmEngine.*`, `WasmMemory.*`, `HostApi.*`, `WasmTypes.*`) |
| SQL parser / AST | `/home/tilman.moeller/projects/ClickHouse/src/Parsers/ParserCreateFunctionQuery.cpp`, `ASTCreateWasmFunctionQuery.{cpp,h}` |
| Interpreter | `/home/tilman.moeller/projects/ClickHouse/src/Interpreters/InterpreterCreateFunctionQuery.cpp` |
| Query resolution | `/home/tilman.moeller/projects/ClickHouse/src/Analyzer/Resolve/resolveFunction.cpp` (lines ~1229–1232) |
| Factory integration | `/home/tilman.moeller/projects/ClickHouse/src/Functions/UserDefined/UserDefinedSQLFunctionFactory.cpp` |
| System table | `/home/tilman.moeller/projects/ClickHouse/src/Storages/System/StorageSystemWasmModules.{cpp,h}` |
| System table attach | `/home/tilman.moeller/projects/ClickHouse/src/Storages/System/attachSystemTables.cpp` (lines 291–295) |
| Context init | `/home/tilman.moeller/projects/ClickHouse/src/Interpreters/Context.cpp` (`initWasmModuleManager`, lines 3696–3728) |
| Settings | `/home/tilman.moeller/projects/ClickHouse/src/Core/ServerSettings.cpp`, `Settings.cpp`, `SettingsChangesHistory.cpp` |
| Contrib engines | `/home/tilman.moeller/projects/ClickHouse/contrib/wasmtime`, `/home/tilman.moeller/projects/ClickHouse/contrib/wasmedge` |
| CMake / macros | `/home/tilman.moeller/projects/ClickHouse/src/configure_config.cmake` (lines 227–231), `/home/tilman.moeller/projects/ClickHouse/src/Common/config.h.in` (lines 86–87) |
| Docs | `/home/tilman.moeller/projects/ClickHouse/docs/en/sql-reference/functions/wasm_udf.md` |
| Tests | `tests/queries/0_stateless/03206_wasm_module_manage.sql` et al., `tests/integration/test_webassembly_udf/` |

### SQL syntax

```82:89:/home/tilman.moeller/projects/ClickHouse/src/Parsers/ParserCreateFunctionQuery.cpp
        /** CREATE func LANGUAGE WASM
          *     ARGUMENTS (a Int32, b UInt8)
          *     RETURNS UInt64
          *     FROM 'some_module' [:: 'internal_func_name']
          *     SHA256_HASH 'cafe42...'
          *     ABI ROW_DIRECT
          *     SETTINGS key1 = value1, key2 = value2
          */
```

Supported ABIs: `ROW_DIRECT`, `BUFFERED_V1` (see `UserDefinedWebAssembly.h`).

**No `REGISTER_*` flag exists** for this subsystem (grep over `src/` returns no matches).

---

## 2. Reachability

### Workflow (when enabled)

1. **Upload module** — `INSERT INTO system.webassembly_modules (name, code[, hash]) VALUES (...)`
2. **Create UDF** — `CREATE [OR REPLACE] FUNCTION <name> LANGUAGE WASM ... FROM '<module>' [:: '<export>'] ...`
3. **Invoke** — `SELECT my_wasm_udf(col) FROM table` (resolved via analyzer → `UserDefinedWebAssemblyFunctionFactory`)

### Privileges

| Action | Privilege |
|--------|-----------|
| `CREATE FUNCTION` (SQL or WASM) | `AccessType::CREATE_FUNCTION` |
| `CREATE OR REPLACE FUNCTION` | `CREATE_FUNCTION` + `DROP_FUNCTION` |
| `DROP FUNCTION` | `DROP_FUNCTION` |
| `INSERT INTO system.webassembly_modules` | Standard `INSERT` on `system.webassembly_modules` (no WASM-specific grant) |
| `DELETE FROM system.webassembly_modules WHERE name = '...'` | Standard `ALTER DELETE` / mutation on that system table |

From `InterpreterCreateFunctionQuery.cpp`:

```36:37:/home/tilman.moeller/projects/ClickHouse/src/Interpreters/InterpreterCreateFunctionQuery.cpp
    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_FUNCTION);
```

### Catalog / system tables

- **`system.webassembly_modules`** — lists loaded modules (`name`, `hash`; `code` is write-only). Attached **only** when WASM is enabled.
- **`system.functions`** — WASM UDFs appear with their `CREATE FUNCTION ... LANGUAGE WASM` DDL (origin enum `WASM_USER_DEFINED = 3` is defined but **not used** in `fillData`; WASM functions are listed as `SQL_USER_DEFINED`).

### On by default?

**No.** Primary gate is a **server setting** defaulting to `false`:

```1304:1305:/home/tilman.moeller/projects/ClickHouse/src/Core/ServerSettings.cpp
    DECLARE(Bool, allow_experimental_webassembly_udf, false, R"(Enable experimental support for WebAssembly UDFs)", EXPERIMENTAL) \
    DECLARE(String, webassembly_udf_engine, "wasmtime", "The engine used to execute WebAssembly UDFs. Supported values are 'wasmtime' and 'wasmedge'.", EXPERIMENTAL) \
```

Integration test `tests/integration/test_webassembly_udf/test.py` (`test_disabled_not_available`) confirms without the flag:
- `system.webassembly_modules` does not exist
- `CREATE FUNCTION ... LANGUAGE WASM` → `"WebAssembly support is not enabled"`

Tests enable it via `tests/config/config.d/wasm_udf.xml` — **not** production default.

---

## 3. Compile gates

### Macros

From `src/Common/config.h.in`:

```86:87:/home/tilman.moeller/projects/ClickHouse/src/Common/config.h.in
#cmakedefine01 USE_WASMEDGE
#cmakedefine01 USE_WASMTIME
```

Set in `src/configure_config.cmake` when CMake targets exist:

```227:231:/home/tilman.moeller/projects/ClickHouse/src/configure_config.cmake
if (TARGET ch_contrib::wasmedge)
    set(USE_WASMEDGE 1)
endif()
if (TARGET ch_rust::wasmtime)
    set(USE_WASMTIME 1)
endif()
```

### CMake options & defaults

| Option | Default | Notes |
|--------|---------|-------|
| `ENABLE_LIBRARIES` | `ON` (`CMakeLists.txt:383`) | Parent default for contrib libs |
| `ENABLE_WASMTIME` | `${ENABLE_LIBRARIES}` → **ON** | Requires `ENABLE_RUST`; disabled on MSAN, FreeBSD, `NO_ARMV81_OR_HIGHER`, S390X |
| `ENABLE_WASMEDGE` | `${ENABLE_LIBRARIES}` → **ON** | Disabled on MUSL, Darwin, PPC64LE, S390X, FreeBSD, LoongArch64 |

Wasmtime explicitly disables WASI at build:

```34:37:/home/tilman.moeller/projects/ClickHouse/rust/workspace/wasmtime/CMakeLists.txt
set(WASMTIME_FEATURE_WASI OFF)
set(WASMTIME_FEATURE_LOGGING OFF)
set(WASMTIME_FEATURE_DISABLE_LOGGING ON)
set(WASMTIME_FEATURE_THREADS OFF)
```

### Is the feature absent if engine libs are not compiled?

**Partially.** The ClickHouse WASM **source is always compiled** (`clickhouse_interpreters_wasm` object lib at `src/CMakeLists.txt:295`; `UserDefinedWebAssembly.cpp` is unconditional). If `USE_WASMTIME`/`USE_WASMEDGE` are 0, engine stubs throw `SUPPORT_IS_DISABLED`:

```514:517:/home/tilman.moeller/projects/ClickHouse/src/Interpreters/WebAssembly/WasmTimeRuntime.cpp
std::unique_ptr<WasmModule> WasmTimeRuntime::compileModule(...) const
{
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Wasmtime support is disabled");
}
```

So: disabling both engines at compile time prevents execution, but **parser, factory, and interpreter code remain in the binary**. Runtime gate (`allow_experimental_webassembly_udf=false`) still prevents reachability without touching compile flags.

Contrib submodules `contrib/wasmtime` and `contrib/wasmedge` are present in the 26.3 tree.

---

## 4. Runtime gates

### Server settings (tier: `EXPERIMENTAL`)

| Setting | Default | Tier |
|---------|---------|------|
| `allow_experimental_webassembly_udf` | `false` | `EXPERIMENTAL` |
| `webassembly_udf_engine` | `"wasmtime"` | `EXPERIMENTAL` |

### User/query settings (tier: `EXPERIMENTAL`, added in 26.3 per `SettingsChangesHistory.cpp`)

| Setting | Default | Description |
|---------|---------|-------------|
| `webassembly_udf_max_fuel` | `100'000` | Instruction fuel per instance; **`0` = no limit** |
| `webassembly_udf_max_memory` | `128_MiB` | Per-instance memory cap |
| `webassembly_udf_max_input_block_size` | `0` | Rows per block (`0` = all rows at once) |
| `webassembly_udf_max_instances` | `32` | Parallel compartment pool size per function |

From `Settings.cpp`:

```7766:7778:/home/tilman.moeller/projects/ClickHouse/src/Core/Settings.cpp
    DECLARE(UInt64, webassembly_udf_max_fuel, 100'000, R"(
Fuel limit per WebAssembly UDF instance execution. Each WebAssembly instruction consumes some amount of fuel.
Set to 0 for no limit.
)", EXPERIMENTAL) \
    DECLARE(UInt64, webassembly_udf_max_memory, 128_MiB, R"(
Memory limit in bytes per WebAssembly UDF instance.
)", EXPERIMENTAL) \
    ...
    DECLARE(UInt64, webassembly_udf_max_instances, 32, R"(
Maximum number of WebAssembly UDF instances that can run in parallel per function.
)", EXPERIMENTAL) \
```

**No `allow_experimental_*` user-level setting** — only the server-level `allow_experimental_webassembly_udf`.

Master enable check in `Context::initWasmModuleManager()`:

```3703:3704:/home/tilman.moeller/projects/ClickHouse/src/Interpreters/Context.cpp
    if (!shared->server_settings[ServerSetting::allow_experimental_webassembly_udf])
        return nullptr;
```

When disabled, `getWasmModuleManager()` throws:

```3725:3726:/home/tilman.moeller/projects/ClickHouse/src/Interpreters/Context.cpp
    if (!shared->wasm_module_manager)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "WebAssembly support is not enabled");
```

---

## 5. Registration point

There is **no** single `registerWebAssemblyUDF()` analogous to `registerStorageX`. Registration is distributed:

| Hook | File | Lines | Role |
|------|------|-------|------|
| **`Context::initWasmModuleManager()`** | `Context.cpp` | 3696–3713 | **Master enable** — returns `nullptr` when flag off; creates `WasmModuleManager` + loads persisted WASM UDFs on `setUserScriptsPath` |
| **`attach<StorageSystemWasmModules>(...)`** | `attachSystemTables.cpp` | 291–295 | Conditionally exposes `system.webassembly_modules` |
| **`UserDefinedWebAssemblyFunctionFactory::addOrReplace()`** | `UserDefinedWebAssembly.cpp` | 648–688 | Registers WASM UDF in in-memory factory on `CREATE FUNCTION` / startup reload |
| **`UserDefinedSQLFunctionFactory::registerFunction()`** | `UserDefinedSQLFunctionFactory.cpp` | 163–164 | Dispatches WASM AST to factory above |
| **`registerInterpreterCreateFunctionQuery()`** | `InterpreterCreateFunctionQuery.cpp` | 76–83 | Always registered; WASM handled via `tryExecute<ASTCreateWasmFunctionQuery>` |
| **`resolveFunction.cpp`** | `resolveFunction.cpp` | 1229–1232 | Query-time resolution to `UserDefinedWebAssemblyFunctionFactory::get()` |

**Best Aiven `REGISTER_*` insertion point** (if ever desired): wrap the body of `Context::initWasmModuleManager()` (line 3703) and/or the `attachSystemTables` block (line 291). There is currently **no** existing `REGISTER_WEBASSEMBLY_UDF` or similar in `config.h.in` / `configure_config.cmake`.

---

## 6. Danger assessment

### Code execution
- **Yes — sandboxed arbitrary WASM execution** when enabled. Users upload compiled `wasm32-unknown-unknown` modules (no OS/stdlib; docs require freestanding target).
- Two engines: default **wasmtime** (WASI compiled off); optional **wasmedge** registers WASI (`WasmEdge_ConfigureAddHostRegistration(..., WasmEdge_HostRegistration_Wasi)` at `WasmEdgeRuntime.cpp:221`).

### Host API exposed to guest (`env` imports only)

From `HostApi.cpp` lines 140–145:
- `clickhouse_server_version() -> i64`
- `clickhouse_throw(ptr, size)` — raises `WASM_ERROR` exception
- `clickhouse_log(level, ptr, size)` — writes to server log; levels clamped to WARNING–TRACE (no FATAL/ERROR to avoid false alerts)
- `clickhouse_random(ptr, size)` — fills WASM memory with PRNG bytes

`linkHostFunctions()` only links imports from module `"env"` (`WasmModuleManager.cpp:203–211`). Unknown `env` imports → `RESOURCE_NOT_FOUND` at load. Non-`env` imports are skipped; with **wasmtime**, WASI imports fail at instantiation (`03208_wasm_import_export.sh` comment, lines 72–74).

### Filesystem
- **Indirect write**: `INSERT INTO system.webassembly_modules` writes `.wasm` files under `{user_scripts_path}/wasm/` on a local disk (`WasmModuleManager::saveModule`, line 194).
- **No guest filesystem API** under wasmtime. **Potential WASI filesystem/network** if `webassembly_udf_engine=wasmedge` and a module imports WASI (not blocked at link time).

### Network egress
- **No direct network** from host API or wasmtime (WASI off).
- **Risk if wasmedge + WASI-importing module** — not tested as safe; default engine is wasmtime.

### Resource abuse / limits

| Control | Enforcement |
|---------|-------------|
| Memory | `webassembly_udf_max_memory` per compartment (wasmtime store limiter; wasmedge max pages) |
| CPU/instructions | `webassembly_udf_max_fuel` (wasmtime fuel; wasmedge cost limit) — **but `0` disables fuel** |
| Parallelism | `webassembly_udf_max_instances` (compartment pool) |
| Cancellation | Epoch interruption + `StopToken` (wasmtime); query `max_execution_time` still applies (`03207_wasm_fault.sh`) |
| Module size | Name ≤128 chars; magic-byte validation; **no explicit max module byte size** found (large modules tested in `03926_wasm_limit_large_module.sql`) |
| Determinism | `isDeterministic() == false` (`UserDefinedWebAssembly.cpp:526`) |

### Other notes
- Modules persisted across restarts (integration test `test_enabled` verifies).
- Backup/restore can re-register WASM UDFs (`UserDefinedSQLFunctionFactory::restore`).
- `WASM_ERROR` error code 770 (`ErrorCodes.cpp`).

---

## 7. Gating recommendation

### Classification: **(a) — already fully gated by experimental server setting defaulting OFF**

**Justification:**
1. `allow_experimental_webassembly_udf` defaults **`false`** and is the sole master switch.
2. When off: no `WasmModuleManager`, no `system.webassembly_modules`, `CREATE FUNCTION LANGUAGE WASM` fails with `WebAssembly support is not enabled` — confirmed by integration test.
3. Aiven managed service can keep this server setting **absent/false** in production config; no `REGISTER_*` patch is strictly required for gating reachability.

### Defense-in-depth (optional, not required for default-off posture)

| Layer | Option | Notes |
|-------|--------|-------|
| **(b) Compile** | `-DENABLE_WASMTIME=OFF` (and/or `-DENABLE_WASMEDGE=OFF`) | Stubs throw `SUPPORT_IS_DISABLED`; WASM C++ code still linked |
| **(c) REGISTER_*** | New `REGISTER_WEBASSEMBLY_UDF` wrapping `initWasmModuleManager()` | Belt-and-suspenders if setting were ever mis-set |
| **(d) Runtime hardening** | If ever enabling: force `webassembly_udf_max_fuel > 0`, keep `webassembly_udf_engine=wasmtime`, deny `CREATE_FUNCTION` to tenants | `fuel=0` allows infinite loops until query timeout; wasmedge enables WASI registration |

### Aiven operational guidance
- **Do not set** `allow_experimental_webassembly_udf=true` in managed deployments.
- Ensure `CREATE_FUNCTION` is not granted to customer SQL users (existing UDF control).
- No new Aiven patch needed **unless** you want compile-time removal (b) or registration stub (c) as extra insurance against config mistakes.

---

### Uncertainties / where to look further
- Exact default `INSERT` access policy on `system.*` tables for Aiven's RBAC model — check your role templates in `src/Access/`.
- WasmEdge behavior with WASI-importing modules when `webassembly_udf_engine=wasmedge` — only wasmtime path is covered by `03208_wasm_import_export.sh`; worth a dedicated test if wasmedge is ever allowed.
- Whether `enableAllExperimentalSettings` or any Aiven config overlay could flip the flag — grep shows it is **not** in `enableAllExperimentalSettings.cpp`.

[REDACTED]

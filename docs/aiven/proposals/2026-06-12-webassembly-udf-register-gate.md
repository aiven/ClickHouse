# Proposal: build-time `REGISTER_WEBASSEMBLY_UDF` gate for the 26.3 WASM UDF subsystem

> **Document type:** Proposal / design sketch (awaiting maintainer ratification).
> **Date:** 2026-06-12.
> **Status:** DRAFT — proposed. Net-new gating work, **not** an uplift port. Decision needed before any code is authored.
> **Audience:** The maintainer who owns the Aiven `REGISTER_*` gating contract and the managed-service security posture; the engineer who would implement the gate.
> **One-sentence framing:** Add an Aiven build-time `REGISTER_WEBASSEMBLY_UDF` flag (default ON) that wraps the single master-enable choke point `Context::initWasmModuleManager`, so the managed service can compile-out the new-in-26.3 WebAssembly UDF code-execution subsystem as defense-in-depth, layered on top of the upstream experimental-setting and `USE_*` compile gates.

## 1. Goal & motivation

The 26.3 new-surface screen ([`new-surface-screening.md`](../uplifts/26.3/new-surface-screening.md))
identified WebAssembly (WASM) UDFs as the **one genuine HIGH-risk gap**: arbitrary
(sandboxed) guest-code execution, reachable from SQL via `CREATE FUNCTION … LANGUAGE WASM`
plus module upload through `INSERT INTO system.webassembly_modules`, with on-disk
persistence under `user_scripts/wasm/`. It is wholly new in 26.3 (absent in
`v25.8.18.1-lts`), so there is **no 25.8-aiven patch to port** — this is a fresh decision.

Today the subsystem is reachable only when an operator sets the experimental server
setting `allow_experimental_webassembly_udf = true` (default `false`). That is sufficient
to keep it off, but it is a single **runtime** switch: a control-plane config mistake, a
profile override, or a future default flip would expose code execution. Aiven's gating
philosophy for this class of feature is **build-time** removal (the `REGISTER_*` family),
which a runtime mis-config cannot defeat. This proposal brings WASM UDFs under that same
contract.

**Decision requested:** approve authoring a `REGISTER_WEBASSEMBLY_UDF` build gate
(Section 5), plus adopt the layered recommendations (Section 8).

## 2. Background — what we are gating

| Aspect | Detail (verified on `v26.3.10.62-lts-aiven-dev`) |
|---|---|
| DDL | `CREATE [OR REPLACE] FUNCTION f LANGUAGE WASM ARGUMENTS (…) RETURNS … FROM '<module>'[:: '<export>'] [SHA256_HASH …] [ABI …] [SETTINGS …]` — `src/Parsers/ParserCreateFunctionQuery.cpp:82` |
| Module store | `INSERT INTO system.webassembly_modules (name, code[, hash])`; persisted to `user_scripts/wasm/` — `src/Interpreters/WasmModuleManager.cpp` |
| Master enable (choke point) | `Context::initWasmModuleManager()` — `src/Interpreters/Context.cpp:3696` |
| System table | `StorageSystemWasmModules`, attached **only when the manager exists** — `src/Storages/System/attachSystemTables.cpp:291` |
| Resolution | `UserDefinedWebAssemblyFunctionFactory::get` — `src/Analyzer/Resolve/resolveFunction.cpp:1232` |
| Compile backends | `USE_WASMTIME` (default engine, WASI **off**), `USE_WASMEDGE` (registers WASI) — `src/Common/config.h.in:86-87`, set in `src/configure_config.cmake:227-231` |
| Runtime master gate | `allow_experimental_webassembly_udf` (server, default **`false`**, EXPERIMENTAL) — `src/Core/ServerSettings.cpp:1304` |
| Privilege | `CREATE FUNCTION` ⇒ `AccessType::CREATE_FUNCTION`; module upload ⇒ plain `INSERT` on the system table |

The host API exposed to the guest is narrow (`clickhouse_log`, `clickhouse_throw`,
`clickhouse_random`, `clickhouse_server_version`); only module `"env"` imports are linked.
Under the default `wasmtime` engine WASI is compiled off, so there is no guest
filesystem/network. The residual risk is (a) CPU/memory abuse (fuel/memory limits, but
`webassembly_udf_max_fuel = 0` disables the fuel cap), (b) WASI exposure **if** the engine
is switched to `wasmedge`, and (c) the general blast radius of shipping a code-exec
subsystem in a multi-tenant managed service.

## 3. Threat model (why a build gate, not just the setting)

- **Invariant we want:** in a managed-service build that does not offer WASM UDFs, the
  feature is **unreachable regardless of runtime configuration** — no setting, profile,
  backup-restore, or future upstream default change can turn it on.
- The upstream experimental setting protects against *accidental default exposure today*,
  but it is mutable at runtime and its default could graduate (we have already seen ~8
  formerly-experimental settings flip to default-ON in 26.3 — see the screen). A runtime
  switch is the wrong layer for a hard "this build cannot execute WASM" guarantee.
- A build flag is consistent with the rest of the Aiven contract: the `REGISTER_*` family
  gates *registration of compiled features* at compile time; WASM UDFs are exactly such a
  feature. This is the same orthogonality we already ship for YTsaurus
  (`USE_YTSAURUS` ⟂ `REGISTER_YTSAURUS_*`).

## 4. Design space

From the screen, cheapest → strongest:

| Option | Mechanism | Pros | Cons |
|---|---|---|---|
| (a) Rely on runtime default | keep `allow_experimental_webassembly_udf=false` in control plane | zero code | mutable; one config mistake = exposure; not a build guarantee |
| (b) Drop the backends | `-DENABLE_WASMTIME=OFF -DENABLE_WASMEDGE=OFF` | no WASM VM in binary | the C++ glue (parser/factory/interpreter/system table) **still links**; `CREATE FUNCTION … LANGUAGE WASM` still parses and the manager still initializes — it only throws `SUPPORT_IS_DISABLED` at *execution*; coarse (all-or-nothing per backend); not expressed in the Aiven flag contract |
| **(c) `REGISTER_WEBASSEMBLY_UDF` build gate** | wrap the master choke point | single-point, contract-consistent, defeats runtime mis-config, leaves backends free to vary | net-new code (small) |
| (d) Runtime hardening only | force `max_fuel>0`, pin `wasmtime`, deny `CREATE_FUNCTION` | needed *if ever enabled* | does not remove the surface |

**Recommendation (this proposal): (c) as the primary gate, with (a) and a hardening
posture as complements.** (c) is preferred over (b) because (b) leaves the whole subsystem
present-but-throwing and is not visible in the `REGISTER_*` contract, whereas (c) makes the
subsystem genuinely inert and audit-able via `config.h`, exactly like the rest of the family.

## 5. Proposed patch — `REGISTER_WEBASSEMBLY_UDF`

### 5.1 The flag

`src/Common/config.h.in` — add one `#cmakedefine01`. Placement: a new short
"user-defined functions" note adjacent to the `USE_WASMTIME`/`USE_WASMEDGE` lines (it is
neither a table engine, table function, nor dict source, so it does not belong in those
sub-blocks):

```c
#cmakedefine01 REGISTER_WEBASSEMBLY_UDF
```

`src/configure_config.cmake` — the matching default-ON `option()` (mandatory; an undefined
`#cmakedefine01` resolves to `0` and would silently disable the feature — the
"`#cmakedefine01` trap" we hit across the family). Placement: alongside the gate, not in
the engines/functions block:

```cmake
option(REGISTER_WEBASSEMBLY_UDF "Register the WebAssembly UDF subsystem (CREATE FUNCTION ... LANGUAGE WASM)" ON)
```

### 5.2 The single-point guard

`Context::initWasmModuleManager()` is the master enable: every downstream surface
(`getWasmModuleManager` → throws `SUPPORT_IS_DISABLED`; `StorageSystemWasmModules`
attachment, which is conditional on the manager existing; UDF creation and resolution)
depends on this returning a non-null manager. Gating its body therefore disables the
entire subsystem from one place — the WASM analogue of wrapping a single `registerStorageX`
call.

```cpp
// src/Interpreters/Context.cpp  (sketch)
WasmModuleManager * Context::initWasmModuleManager()
{
    std::lock_guard lock(shared->mutex);

    if (shared->wasm_module_manager)
        return shared->wasm_module_manager.get();

#if !REGISTER_WEBASSEMBLY_UDF
    return nullptr;   // Aiven build-time gate: subsystem compiled-out, independent of allow_experimental_webassembly_udf
#else
    if (!shared->server_settings[ServerSetting::allow_experimental_webassembly_udf])
        return nullptr;
    // … existing body (engine selection, disk, manager construction) …
    return shared->wasm_module_manager.get();
#endif
}
```

Effect when `REGISTER_WEBASSEMBLY_UDF = 0`: `initWasmModuleManager` returns `nullptr` even
if `allow_experimental_webassembly_udf = true` ⇒ `getWasmModuleManager` throws
`SUPPORT_IS_DISABLED`, `system.webassembly_modules` is **not** attached, and
`CREATE FUNCTION … LANGUAGE WASM` fails. The runtime switch becomes a no-op against the gate.

> **Open question for the maintainer:** do we gate at this single choke point only, or also
> add a hard guard in the parser / interpreter so the DDL is rejected earlier with a clearer
> message? The choke point alone is sufficient for the security invariant; an earlier guard
> is cosmetics. Recommendation: choke point only, to keep the diff minimal and the surface
> singular (one place to reason about).

### 5.3 Orthogonality (the key invariant)

`REGISTER_WEBASSEMBLY_UDF` (expose the subsystem) is orthogonal to
`USE_WASMTIME`/`USE_WASMEDGE` (which backend libs are compiled), just as
`REGISTER_YTSAURUS_*` ⟂ `USE_YTSAURUS`. The truth table:

| `REGISTER_WEBASSEMBLY_UDF` | `USE_WASMTIME`/`USE_WASMEDGE` | `allow_experimental_webassembly_udf` | Result |
|---|---|---|---|
| 1 (default) | ≥1 on | true | fully functional (= upstream default behavior when operator opts in) |
| 1 | ≥1 on | false (default) | unreachable (upstream default) — **no behavior change vs stock** |
| **0 (Aiven hardened)** | any | **any** | **compiled-out — unreachable regardless of setting** |
| 1 | both off | true | manager inits but execution throws `SUPPORT_IS_DISABLED` (upstream behavior) |

The default-ON value means a stock Aiven build behaves **identically to upstream**; the gate
only bites when Aiven explicitly sets `-DREGISTER_WEBASSEMBLY_UDF=0`.

## 6. Invariants the gate must protect

1. **No default-behavior change.** Default ON ⇒ a normal build is byte-for-behavior
   identical to upstream. (Mirrors the whole `REGISTER_*` family.)
2. **Flag↔option pairing.** The `#cmakedefine01` MUST have a matching `option(... ON)` or it
   silently resolves to 0 — verified by the same coverage check used in 045/051/052/071/075.
3. **Single choke point.** Disabling must flow from one location so there is exactly one
   place to audit and no second path to the subsystem.
4. **Orthogonal to backends and to the runtime setting** (Section 5.3).
5. **Exception, not crash.** With the gate off, attempts surface as the existing
   `SUPPORT_IS_DISABLED` exception (an exception, not a server crash, in the release build).

## 7. Verification plan

Build-system-only change ⇒ `no_justified` test posture (consistent with the family; no
evidence-of-causation pair is expressible for a compile-time toggle). Verify:

1. **`config.h`:** `REGISTER_WEBASSEMBLY_UDF = 1` on a default build.
2. **Default build (flag ON), setting ON:** `CREATE FUNCTION … LANGUAGE WASM` succeeds (if a
   backend is compiled), `system.webassembly_modules` present — i.e. unchanged from upstream.
3. **Hardened build (`-DREGISTER_WEBASSEMBLY_UDF=0`), setting forced ON:**
   - `SELECT count() FROM system.tables WHERE name='webassembly_modules'` → `0` (not attached);
   - `CREATE FUNCTION f LANGUAGE WASM …` → `SUPPORT_IS_DISABLED`;
   - server starts cleanly (no init regression).
4. **Coverage diff:** newly-added `config.h.in` flag == newly-added option (empty diff).

The hardened-build smoke is the load-bearing one — it proves the runtime setting cannot
defeat the gate.

## 8. Layered recommendation (defense-in-depth)

The build gate is the primary control. Adopt it **with** these complements:

1. **Keep `allow_experimental_webassembly_udf = false`** in the control-plane default
   (belt to the build gate's suspenders; also covers any build that ships with the flag ON).
2. **Optionally drop the backends** in the hardened build (`-DENABLE_WASMTIME=OFF
   -DENABLE_WASMEDGE=OFF`) to also remove the VM code/contrib from the binary. With the
   `REGISTER_` gate this is optional (the subsystem is already inert), but it reduces binary
   size and dependency surface.
3. **If WASM UDFs are ever offered:** pin `webassembly_udf_engine = wasmtime` (WASI off),
   enforce `webassembly_udf_max_fuel > 0` (0 = unbounded loops until query timeout), and do
   **not** grant `CREATE_FUNCTION` (and INSERT on `system.webassembly_modules`) to tenant
   roles.

## 9. Scope & non-goals

- **Net-new, not a port.** This is tracked separately from the `045 → 051 → 052 → 070 →
  071 → 075` `REGISTER_*` uplift family (all committed/staged). It would be authored fresh
  against 26.3, with its own dossier (`docs/aiven/patches/`) once approved.
- **Other screened surfaces are out of scope here** and remain config/policy decisions
  (see the screen): Polyglot (`-DENABLE_POLYGLOT=OFF` / experimental setting OFF), ClickStack
  (`/clickstack`, handled at the proxy layer like `/play`), libcotp TOTP, the graduated
  default-ON settings. If any later warrants a build gate, it gets its own proposal.
- **No change to the upstream experimental setting's default** — we layer on top of it.

## 10. Decision checklist

- [ ] Approve (c) `REGISTER_WEBASSEMBLY_UDF` as the primary gate (vs (a)/(b) only).
- [ ] Confirm the single-choke-point placement at `initWasmModuleManager` (vs also adding an
      earlier parser/interpreter guard).
- [ ] Confirm flag naming `REGISTER_WEBASSEMBLY_UDF` (alt: `REGISTER_WEBASSEMBLY_FUNCTION` —
      rejected: it is a UDF subsystem, not a table function).
- [ ] Confirm `config.h.in` / `configure_config.cmake` placement (adjacent to `USE_WASM*`,
      new "UDF" note — vs forcing it into the engines/functions block).
- [ ] Decide whether to also ship complement #2 (`-DENABLE_WASMTIME/WASMEDGE=OFF`) in the
      hardened build.

Once decided, the implementation is a ~4-file change (`config.h.in`,
`configure_config.cmake`, `Context.cpp`, dossier) following the family's
`no_justified` + build-system-verification template.

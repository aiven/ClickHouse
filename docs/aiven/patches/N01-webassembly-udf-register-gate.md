# Patch N01 (webassembly-udf-register-gate) — REGISTER_WEBASSEMBLY_UDF build-time gate

> **NET-NEW Aiven patch (`N01`) — NOT a port / NOT a cherry-pick.** This gate is
> authored fresh against `v26.3.10.62-lts`; there is no prior-LTS source commit, so
> `source_sha: none` and `byte_equivalent: n/a` throughout. `N01` is the per-cycle
> net-new handle (category D in `commit-hygiene.md` §1); commit subject
> `patch-new(N01):`. It is tracked
> separately from the `045 → 051 → 052 → 070 → 071 → 075` `REGISTER_*`
> engine/function uplift *port* family.

## 0. Lineage

| LTS uplift | First-carry handle on aiven branch | Authored by | Outcome |
|---|---|---|---|
| 26.3-aiven | `N01` (staged) | Aiven WASM-gate worker, 2026-06-12 | net-new authoring (no prior-LTS source) |

The current uplift's row stays "(staged)" until the human commits. There is no
25.3-aiven / 25.8-aiven row: the WebAssembly UDF subsystem did not exist before
26.3 (absent in `v25.8.18.1-lts`), so there is nothing to inherit.

## 1. Purpose

Add an Aiven build-time `REGISTER_WEBASSEMBLY_UDF` flag (default **ON**) that
wraps the single master-enable choke point `Context::initWasmModuleManager`, so a
managed-service build can compile out the new-in-26.3 WebAssembly UDF
code-execution subsystem (`CREATE FUNCTION … LANGUAGE WASM`, module upload via
`INSERT INTO system.webassembly_modules`, on-disk persistence under
`user_scripts/wasm/`) as **defense-in-depth**, layered on top of the upstream
experimental server setting `allow_experimental_webassembly_udf` and the
`USE_WASMTIME`/`USE_WASMEDGE` compile gates.

The durable motivation: WASM UDFs are the one genuine HIGH-risk new surface in
26.3 (arbitrary, sandboxed guest-code execution reachable from SQL). Upstream
gates it only at **runtime** via an experimental server setting (default off).
A runtime switch is the wrong layer for a hard "this build cannot execute WASM"
guarantee — a control-plane config mistake, a profile override, or a future
upstream default flip could expose it. Aiven's gating philosophy for this class
of feature is **build-time** removal (the `REGISTER_*` family), which a runtime
mis-config cannot defeat. This brings WASM UDFs under that same contract.

Source SHA on a previous LTS: **none — net-new Aiven patch authored against
`v26.3.10.62-lts`.**
Design + rationale: `docs/aiven/proposals/2026-06-12-webassembly-udf-register-gate.md`.
Screening that surfaced the gap: `docs/aiven/uplifts/26.3/new-surface-screening.md`
(surface #1, the one genuine HIGH gap).

## 2. Upstream-drift findings

> For a net-new patch there is no prior-LTS source to drift from. The relevant
> check is instead: does the chosen choke point + the gated surfaces exist and
> behave as the proposal assumes on `v26.3.10.62-lts`?

### Commands run

```bash
# Choke point exists and is the single master enable:
grep -n "Context::initWasmModuleManager"  src/Interpreters/Context.cpp     # -> 3696
grep -n "getWasmModuleManager"             src/Interpreters/Context.cpp     # -> throws SUPPORT_IS_DISABLED at 3731
# System table attached only when the manager exists:
grep -n "webassembly_modules"              src/Storages/System/attachSystemTables.cpp
# Backend / runtime gates orthogonal to this flag:
grep -nE "USE_WASMTIME|USE_WASMEDGE"       src/Common/config.h.in
grep -n "allow_experimental_webassembly_udf" src/Core/ServerSettings.cpp
```

### Findings

- `Context::initWasmModuleManager` (`Context.cpp:3696`) is the single point that
  constructs `shared->wasm_module_manager`. Every downstream surface depends on
  it returning non-null: `getWasmModuleManager` throws `SUPPORT_IS_DISABLED`
  (`Context.cpp:3731`) when the manager is null, `StorageSystemWasmModules` is
  attached **only when the manager exists**, and UDF creation/resolution route
  through the manager. Gating its body therefore disables the whole subsystem
  from one place.
- `USE_WASMTIME = 1` and `USE_WASMEDGE = 1` on this build (both backends
  compiled) — the gate is verified to be **orthogonal** to which VM is linked:
  with the flag at 0 the subsystem is inert even though both backends are
  present.
- `allow_experimental_webassembly_udf` is a server setting, default `false`
  (`ServerSettings.cpp`). The gate is verified to defeat it: with the setting
  forced ON the hardened build still rejects the subsystem.
- Conclusion: **still-needed-and-applies** (net-new authoring; the choke point
  and the gated surfaces exist on 26.3 exactly as the proposal assumes).

## 3. C++ review

The change is one preprocessor guard inside one function body plus two
build-system declarations (`#cmakedefine01` + `option(... ON)`); no runtime data
path is altered when the flag is ON (the default).

- 1 Lifetime + ownership: ✓ — with the flag at 0 the early `return nullptr;`
  precedes any construction; `shared->wasm_module_manager` simply stays empty.
  No object lifetime is shortened or extended; the surrounding `std::lock_guard`
  is unchanged.
- 2 Exception safety: ✓ — the gated branch is a bare `return nullptr;` (no
  throwing code added). Downstream `getWasmModuleManager` still throws the
  existing `SUPPORT_IS_DISABLED` exception — an exception, not a server crash, in
  the release build.
- 3 Thread-safety + concurrency: ✓ — the guard sits inside the existing
  `std::lock_guard lock(shared->mutex)`; the `#if` only changes which code the
  already-locked region runs. No new shared state, no new lock ordering.
- 4 Performance + memory: ✓ — strictly reductive when the flag is 0 (manager
  never constructed, system table never attached). Default build (flag ON) is
  behavior-identical to upstream — the `#else` branch is the verbatim upstream
  body.
- 5 Settings as public API: ✓ — no server *setting* added or changed;
  `REGISTER_WEBASSEMBLY_UDF` is a compile-time CMake option (default ON). The
  upstream `allow_experimental_webassembly_udf` setting is untouched. Default
  build ⇒ no public-API surface change.
- 6 Error handling: ✓ — disabling surfaces via the existing `SUPPORT_IS_DISABLED`
  ("WebAssembly support is not enabled") path; no new error code introduced.
- 7 Upstream / vendored code: ✓ — all edited files are ClickHouse-owned
  (`src/Common/config.h.in`, `src/configure_config.cmake`,
  `src/Interpreters/Context.cpp`); none under `contrib/**`.
- 8 Behavior under settings: ✓ — verified by the differential build below: flag
  ON ⇒ subsystem reachable regardless of nothing (upstream behavior, gated by
  the runtime setting); flag OFF ⇒ subsystem unreachable **regardless of** the
  runtime setting (the load-bearing invariant).

## 4. Test design

(b) **Documented justification — `no_justified` + build-system differential
verification.** This is a build-system-only change (one `#cmakedefine01`, one
`option(... ON)`, one `#if` guard inside a function body) and adds **no runtime
behavior on a default build**: `REGISTER_WEBASSEMBLY_UDF` defaults ON, so the
guard's `#else` branch (the verbatim upstream body) compiles — zero
default-behavior change. The behavior the patch *introduces* — eliding the WASM
UDF subsystem — is a **compile-time** property of the produced binary, gated by a
CMake flag, not a runtime setting. A stateless `.sql`/`.sh` test cannot observe it
on a default build (the subsystem is present), and producing the "disabled"
binary requires a second CMake configure (`-DREGISTER_WEBASSEMBLY_UDF=0`) +
rebuild that the stateless runner cannot express. A worktree/setting flip is the
wrong layer entirely — the flag is compile-time, so flipping the runtime setting
proves nothing. Per `docs/aiven/AGENTS.md` §7(b) the correct posture is
`no_justified` with build-system verification.

### Build-system differential verification (the correctness evidence)

The load-bearing proof is the **contrast** between a hardened build and a default
build, **with the experimental runtime setting forced ON in BOTH runs** (via a
server `config.d` snippet `<allow_experimental_webassembly_udf>1</…>`). Because
the runtime setting is identical (ON) in both runs, any difference is attributable
to the build flag alone.

| Build | `config.h` | `system.webassembly_modules` count | `CREATE FUNCTION … LANGUAGE WASM … FROM 'nope'` |
|---|---|---|---|
| Hardened `-DREGISTER_WEBASSEMBLY_UDF=0` | `REGISTER_WEBASSEMBLY_UDF = 0` (`USE_WASMTIME=1`, `USE_WASMEDGE=1`) | **0** (not attached) | **Code 344 `SUPPORT_IS_DISABLED`**: "WebAssembly support is not enabled" |
| Default `-DREGISTER_WEBASSEMBLY_UDF=ON` | `REGISTER_WEBASSEMBLY_UDF = 1` (`USE_WASMTIME=1`, `USE_WASMEDGE=1`) | **1** (attached) | **Code 674 `RESOURCE_NOT_FOUND`**: "WebAssembly module 'nope' not found" |

Interpretation:

- **Hardened** (`tmp/wasm-gate/smoke-hardened-systable.log` = `0`,
  `tmp/wasm-gate/smoke-hardened-create.log` = `SUPPORT_IS_DISABLED`): even with
  `allow_experimental_webassembly_udf=1` forced ON, the system table is **not
  attached** and the `CREATE` is rejected at the "not enabled" check. The runtime
  setting is defeated by the build gate — the security invariant.
- **Default** (`tmp/wasm-gate/smoke-default-systable.log` = `1`,
  `tmp/wasm-gate/smoke-default-create.log` = `RESOURCE_NOT_FOUND` for module
  `'nope'`): the system table **is** attached and the `CREATE` gets **past** the
  "not enabled" check, failing only later because module `'nope'` does not exist.
  That `RESOURCE_NOT_FOUND` (not `SUPPORT_IS_DISABLED`) **proves the subsystem is
  reachable** — i.e. default ON is behavior-identical to upstream.
- **`config.h`** (`tmp/wasm-gate/configh-hardened.log`,
  `tmp/wasm-gate/configh-default.log`): `REGISTER_WEBASSEMBLY_UDF` resolves to `0`
  on the hardened build and `1` on the default build, with `USE_WASMTIME=1` /
  `USE_WASMEDGE=1` in both — confirming the gate is independent of the compiled
  backends.
- **Coverage diff** (Step 0): the set of newly-added `config.h.in` flags equals
  the set of newly-added `option()` names, both exactly
  `{REGISTER_WEBASSEMBLY_UDF}` — no flag is left without a matching `ON` option
  (an undefined `#cmakedefine01` would silently resolve to 0).
- **Both rebuilds green**: editing `config.h.in`/`configure_config.cmake` forced
  a CMake reconfigure + broad rebuild (config.h is widely included);
  `tmp/wasm-gate/build-hardened.log` and `tmp/wasm-gate/build-default.log` both
  end with `ninja exit: 0` (~3278 / ~3272 steps respectively).

Why this distinguishes the Aiven gate from a generic upstream
`SUPPORT_IS_DISABLED`: the hardened error fires **with the experimental setting
explicitly ON**, a state in which upstream would NOT throw "not enabled" (it would
reach the manager). The default build proves exactly that (reaching
`RESOURCE_NOT_FOUND` instead). So the `SUPPORT_IS_DISABLED` in the hardened run is
demonstrably the Aiven build gate, not upstream's runtime gate.

### Note on backends

`USE_WASMTIME = 1` and `USE_WASMEDGE = 1` on this build, so the default build both
attaches the system table **and** has execution backends available. (Had both been
0, the manager would still be created in the default build — system table present —
but execution would throw a backend error; the gate proof, system-table presence +
the "not enabled" message contrast, would still hold.) The asserted invariants
(system table count 0↔1 and the `SUPPORT_IS_DISABLED`↔`RESOURCE_NOT_FOUND`
contrast) are unaffected by the backend choice.

## 5. Rollback considerations

- Revert safety: ✓ — pure compile-time guard + build-system option. Reverting
  removes the `#cmakedefine01`/`option()` lines and the `#if !REGISTER_WEBASSEMBLY_UDF
  return nullptr; #else … #endif` wrapper; no schema migration, no on-disk format
  change, no ZK state.
- State surviving restart: none introduced by this patch. (The subsystem's own
  on-disk modules under `user_scripts/wasm/` are upstream behavior, only reachable
  when the flag is ON.)
- Disabling the new behavior without rebuilding: `n/a` — the toggle is a
  compile-time CMake option. To make the subsystem inert, build with
  `-DREGISTER_WEBASSEMBLY_UDF=0`. The complementary runtime control remains
  `allow_experimental_webassembly_udf=false` (belt to the build gate's suspenders).

## 6. Per-uplift notes

### 26.3-aiven (this uplift)

- Net-new authoring (no cherry-pick; no prior-LTS source). The three edits were
  applied by hand before this dispatch:
  `src/Common/config.h.in:147` (`#cmakedefine01 REGISTER_WEBASSEMBLY_UDF`),
  `src/configure_config.cmake:308` (`option(REGISTER_WEBASSEMBLY_UDF … ON)` with a
  comment block), and `src/Interpreters/Context.cpp:3701` (the
  `#if !REGISTER_WEBASSEMBLY_UDF return nullptr; #else <upstream body> #endif`
  guard immediately after the `if (shared->wasm_module_manager) return …;`
  early-return).
- Orthogonality table (the key invariant; verified):

  | `REGISTER_WEBASSEMBLY_UDF` | `USE_WASMTIME`/`USE_WASMEDGE` | `allow_experimental_webassembly_udf` | Result |
  |---|---|---|---|
  | 1 (default) | ≥1 on | true | fully functional (= upstream when operator opts in) — **verified: systable=1, CREATE reaches `RESOURCE_NOT_FOUND`** |
  | 1 | ≥1 on | false (default) | unreachable (upstream default) — no behavior change vs stock |
  | **0 (Aiven hardened)** | any | **any** | **compiled-out — unreachable regardless of setting** — **verified: systable=0, CREATE → `SUPPORT_IS_DISABLED` even with setting ON** |
  | 1 | both off | true | manager inits but execution throws `SUPPORT_IS_DISABLED` (upstream behavior) |

- Single-choke-point rationale: `initWasmModuleManager` is the only constructor of
  `shared->wasm_module_manager`; gating it disables `getWasmModuleManager` (throws),
  the `StorageSystemWasmModules` attachment (conditional on the manager), and UDF
  creation/resolution — one place to audit, no second path.
- Orthogonality confirmed empirically: both backends compiled (`USE_WASMTIME=1`,
  `USE_WASMEDGE=1`) yet the flag still fully gates the subsystem.
- Default ON ⇒ no upstream behavior change (the `#else` branch is the verbatim
  upstream body; default smoke reaches the subsystem exactly as stock would).
- Test: no new stateless test — `no_justified`, build-system-only,
  default-behavior unchanged. Build-system differential verification recorded
  above (`tmp/wasm-gate/configh-{hardened,default}.log`,
  `tmp/wasm-gate/smoke-{hardened,default}-{systable,create}.log`,
  `tmp/wasm-gate/build-{hardened,default}.log`).
- Time-to-port: two broad rebuilds (config.h.in widely included), ~5.4 min
  hardened + ~4.0 min default; build dir warm-cache (sccache hot).
- Cross-references: design `docs/aiven/proposals/2026-06-12-webassembly-udf-register-gate.md`;
  screening `docs/aiven/uplifts/26.3/new-surface-screening.md` (surface #1).
  Family analogue for the `option()` + flag↔option-coverage pattern:
  `docs/aiven/patches/075-register-arrowflight-flags.md`.
- Anything surprising: the gate is a single-branch `#if` inside the function body
  (cf. the `REGISTER_*` family, which guards `registerX(factory)` *calls*) — the
  WASM analogue of wrapping a single `registerStorageX`, because the subsystem's
  master enable is one manager-construction point rather than a registration call.

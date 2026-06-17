# 26.3 execution plan — order & grouping

Derived per [`../../runbooks/execution-sequencing.md`](../../runbooks/execution-sequencing.md).
This is the suggested dispatch order for the remaining patches in
[`inventory.md`](inventory.md). It is **advisory** — dossiers and retrospectives
remain the authoritative per-patch record.

**Status (refreshed 2026-06-17, audited against git):** 77 of 78 adjudicated — 66 ported (`patch-port`, incl. 023 folded into `patch-port(015)`; **004 ported 2026-06-17** as `patch-port(004)`, gated behind default-off `aiven_replace_mergetree_with_replicated`) + 11 not-carried (6 `patch-drop` 001/007/017/043/044/009; 002 & 058 dropped `obsoleted-by-upstream`, replaced by production config; 027/028 dropped under `docs(...)` subjects — `6a02e965f41` / `75dc24a6780` — not the `patch-drop` convention; 074 dropped `obsoleted-by-upstream`, dossier-only); **1 remaining — the held clause-(v) patch 057** — see
[`00-introduction.md`](00-introduction.md). **Phase 1 is complete:** 035, 056
(clean warm-ups), 047 (rewrite + integration test), 053 (Replicated-DB
constraint security fix, pulled forward from Phase 3), 037 (sensor-init removal,
`test_design_blocked` — untestable hardware-dependent convenience) and 039
(ThreadFuzzer pthread-wrap disable, `no_justified` — compile-time toggle,
artifact-level nm evidence) are ported; 043 dropped
(`superseded-by-upstream-equivalent`), 044 dropped (`ineffective-no-op` — curl
IPv6 already enabled on the base since upstream 2020). 020 is deferred to
Phase 3 (depends on 019). **1 patch remains:** the held clause-(v) patch **057** (004 ported 2026-06-17 as `patch-port(004)`, gated behind default-off `aiven_replace_mergetree_with_replicated`, ledger REPL-6; 058 dropped `obsoleted-by-upstream` 2026-06-17 — 26.3's `database_replicated_allow_replicated_engine_arguments=0` already rejects custom ZK path/replica name; un-bypassability restored via a production `readonly` constraint, ledger REPL-5). The Phase-4 replication core is fully committed — **066 & 078** landed as `patch-port(066,078)` (`5aa0b87b7b4`) and the `.tmp` chain landed as `patch-port(062,063,064)` (`ebe166d40f7`); 008/046/050/003/072 committed earlier; 002/009 dropped. Phases 1–3 are now complete — including the Phase-3 engine/flag toggles (052/070/071/075 ported, 074 dropped); object storage fully done (027/028 dropped under `docs(...)` commits). One **net-new** patch also landed, outside the 78-row inventory: `patch-new(N01)` — the `REGISTER_WEBASSEMBLY_UDF` build-time gate (see [`inventory.md`](inventory.md) "Net-new patches" and the new-surface screen).

## Phased grouping

Phases run in order of increasing risk/coupling; within a phase the subsystem
groups are independent and parallelize across sessions. `A→B` denotes a
dependency chain (port A before B).

| Phase | Group | Patches | Notes |
|---|---|---|---|
| **1** | Warm-up — *testable* (small, clean, SQL/fs-observable) | ~~035~~ ✓, ~~056~~ ✓ | validated the full pipeline incl. the evidence-of-causation test. **020 removed — depends on 019** (see Phase 3). **047 reclassified out** — it was a rewrite (functions renamed `Size`→`Bytes`) needing an integration test, not a warm-up; ported as T3.18 |
| **1** | Warm-up — *build/config* (apply-clean but no evidence pair → likely `no_justified`) | ~~037~~ ✓(tdb), ~~039~~ ✓(nj), ~~043~~ ✗drop, ~~044~~ ✗drop — **PHASE 1 COMPLETE** | **043 dropped** (`superseded-by-upstream-equivalent`: upstream `545c27008d4` already in base). **044 dropped** (`ineffective-no-op`: `curl_config.h` has hardcoded `#define ENABLE_IPV6` since upstream 2020, and the patch's `option` is dead + compile-define redundant, so it never made IPv6 disableable — validity check, not just apply-check). **037/039 caveat**: runtime/instrumentation `.cpp` toggles that do NOT fit §7(b)'s build-system-only carve-out and have no reliable evidence pair (037 sensor metrics are hardware-dependent; 039 disables the very thread-fuzzer instrumentation a test would use) → expect `test_design_blocked`, not a quick port |
| **2** | Object storage (S3/Azure) | ~~012~~→~~013~~, ~~023~~(folded into 015)→~~024~~, ~~025~~, ~~027~~✗drop, ~~028~~✗drop, ~~015~~→~~016~~, ~~026~~, ~~048~~ — **DONE** | 027 (`6a02e965f41`) / 028 (`75dc24a6780`) dropped `obsoleted-by-upstream`, committed under `docs(...)` subjects (not `patch-drop`) |
| **2** | Kafka | ~~030~~, ~~029~~, ~~031~~, ~~032~~→~~076~~, ~~033~~ — **DONE** | — |
| **2** | Dictionaries / PostgreSQL | ~~045~~, ~~061~~, ~~036~~, ~~034~~ — **DONE** | — |
| **3** | TLS / SSL / access | ~~017~~✗drop, ~~018~~, ~~065~~, ~~067~~, ~~059~~, ~~014~~, ~~021~~, ~~019~~→~~020~~, ~~**022**~~ — **DONE** | 017 dropped (`mysql_require_secure_transport` upstream); rest committed |
| **3** | ZK/Keeper, system tables, settings | ~~038~~, ~~054~~, ~~069~~, ~~053~~, ~~055~~, ~~068~~, ~~041~~ — **DONE** | whole group committed |
| **3** | Engine/flag toggles | ~~052~~, ~~070~~→~~071~~, ~~074~~✗drop→~~075~~ — **DONE** | YTsaurus & ArrowFlight sibling pairs; 074 dropped (`obsoleted-by-upstream`, already in 26.3 base), 052/070/071/075 ported |
| **4** | Replication core (highest value + drift risk) — **DONE** | ~~002~~✗drop, ~~003~~, ~~008~~, ~~009~~✗drop, ~~046~~, ~~072~~, ~~050~~, ~~066~~, ~~078~~ | Deep-research drift/dependency map done 2026-06-12; 066 = 1011 loc — warm context + care. **003, 072, 050, 008, 046 COMMITTED** (008 = `2eb41d8686c`, 050 = `04cf308298b`, 046 = `880cc049101`; 008 adds the default-off `aiven_enable_replication_queue_size_limit` guard, 046 renames its two settings under the `aiven_` convention — `aiven_use_early_fetch_pool` default-on with a legacy alias). Replication-core leftovers: **066, 078** (MV-refresh pair). **002 DROPPED** (`obsoleted-by-upstream`, 2026-06-15): upstream ships `internal_replication` as a per-DB `DatabaseReplicatedSettings` knob (default `false`); restored in production via `<database_replicated><internal_replication>true</…>` config (ledger REPL-1), safe only with 004 — see dossier 002 §2/§5. **009 DROPPED** (`not-justified`, 2026-06-15): on single-shard `ReplicatedMergeTree` (Aiven's topology) `MOVE … TO TABLE` is leader-only, so no data benefit over table replication, while DDL-log routing amplifies a failed `MOVE` into a DB-DDL-queue stall + forced recovery — see dossier 009 §1b/§6 |
| **4** | `.tmp` family (dependency chain) — **DONE** | ~~062~~→~~063~~→~~064~~ | strict order; landed squashed as `patch-port(062,063,064)` (`ebe166d40f7`), 062's throw gated behind default-off `aiven_prohibit_tmp_table_creation` |

## Held — human gating decision first (clause v)

These trip (or may trip) the clause-(v) blast-radius screen
([`../../skills/dispatch-prompt-template.md`](../../skills/dispatch-prompt-template.md)).
Get the gating decision (a `policy_call` — typically a default-off server
setting) **before** any worker dispatch; schedule the code once the gate is
decided.

| Patch | Why held |
|---|---|
| ~~004~~ | **RESOLVED — ported `patch-port(004)`** (2026-06-17). The canonical clause-(v) case: gated behind a new default-off server setting `aiven_replace_mergetree_with_replicated` (gate OFF = byte-identical to stock 26.3), plus four gap fixes (gate, exclude `ATTACH`, missing-twin safety, stored-DDL consistency). **Gates the REPL-1 config flip:** the dropped-002 production setting `internal_replication=true` is safe only when this all-`ReplicatedMergeTree` invariant holds — sequence the config flip with/after 004 (dossier 004, ledger REPL-1 / REPL-6). |
| ~~058~~ | **RESOLVED — dropped `obsoleted-by-upstream`** (2026-06-17). 26.3's `database_replicated_allow_replicated_engine_arguments=0` (default) already rejects custom `ReplicatedMergeTree` ZK path/replica name; 058's only delta was un-bypassability → restored via a production `readonly` constraint, not code (dossier 058, ledger REPL-5). |
| **057** | Remove all cloud-specific settings (610 loc) — removes settings broadly |
| ~~062~~ | **RESOLVED — ported `patch-port(062,063,064)` (`ebe166d40f7`)**. Screened 2026-06-17: gated 062's throw behind a default-off `aiven_prohibit_tmp_table_creation` server setting (gate OFF = byte-identical to stock 26.3); 063/064 + internal-flips shipped unconditional. |
| ~~051~~ | **RESOLVED — ported `patch-port(051)`** (de-escalated by maintainer: carried as a no-default-change capability, all `REGISTER_*` flags default ON). |

## Deferred — broad "new experimental / dangerous surface" screening (post-family) — DONE 2026-06-12

**Completed:** the broad screen was carried out — see [`new-surface-screening.md`](new-surface-screening.md). Outcome: WebAssembly UDFs were the highest-risk new surface and are now gated by the net-new build flag `patch-new(N01)` (`REGISTER_WEBASSEMBLY_UDF`, default ON, single choke point at `Context::initWasmModuleManager`); Polyglot/ClickStack/libcotp triaged (ClickStack re-rated MEDIUM as a sibling of `/play`); new `allow_experimental_*` settings catalogued. The original task framing is kept below for the record.

Separate future task (to be grouped into one gating effort): systematically enumerate
features **added in 26.3 that did not exist in 25.8** and are experimental or
operationally dangerous, then decide a coherent disable strategy. The `REGISTER_*`
family (045/051/052/070/071/075) only covers storage-engine / table-function /
dictionary-source *factory registration*; the items below are orthogonal and need
their own mechanism (compile flag, server setting, or access control).

Seed findings (from the 25.8→26.3 registration audit, see `tmp/reg-audit/`):

| Surface | New in 26.3 | Current disable lever | Notes |
|---|---|---|---|
| **WebAssembly UDFs** | yes — `src/Interpreters/WebAssembly/*`, `Functions/UserDefined/UserDefinedWebAssembly.*`, `WasmModuleManager`; flags `USE_WASMTIME`/`USE_WASMEDGE` (0 in 25.8); settings `webassembly_udf_max_*`, server `webassembly_udf_engine` (EXPERIMENTAL) | upstream compile flags `USE_WASMTIME`/`USE_WASMEDGE` (built only if contrib `ch_rust::wasmtime`/`ch_contrib::wasmedge` present) — i.e. a contrib/build-config decision | executes user-supplied WASM bytecode — security-relevant |
| new table functions `primes`, `mergeTreeAnalyzeIndexes`, `mergeTreeTextIndex` | yes | none (unguarded) | local/introspection only — out of `REGISTER_*` scope; listed for completeness, likely no action |
| storage `Alias` (`registerStorageAlias`) | yes | none (unguarded) | local alias engine, benign; left unconditional in patch-port(052) |

Not a coverage gap for the `REGISTER_*` family: the only removed registration
(`LiveView`) had no flag; no risky connectivity engine/function was added that the
family misses. TODO for the deferred task: also sweep `allow_experimental_*`
settings added since 25.8 (e.g. `allow_experimental_ytsaurus_table_function`) and any
new `USE_*` contrib features, to decide a unified Aiven hardening posture.

## How to use this with the pipeline

- Pick the next group; within it follow the `→` dependency order and do the
  small/clean patches before the large ones.
- Independent Phase-2/3 groups can run concurrently in separate sessions.
- When a held patch's gating decision lands, slot its code wherever the relevant
  subsystem context is warm (e.g. 062 with the `.tmp` chain, 004/058 with the
  replication-core phase).

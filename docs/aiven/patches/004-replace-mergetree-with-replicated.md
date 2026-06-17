# Patch 004 — replace `MergeTree` with `ReplicatedMergeTree` in `Replicated` databases (Aiven-gated)

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| ≤25.8-aiven | `226ed6cc31` ("Replace MergeTree with ReplicatedMergeTree in Replicated databases", Tilman Moeller, committer `alex.khatskevich@aiven.io`) | original carry (≈12 re-applications back to ~25.3) | unconditional string-prepend, never upstreamed in this *convert* form |
| 26.3-aiven | (staged) | `patch-port(004)` dispatch | conflict-clean apply + **gated** (one new default-off server setting) + **four gap fixes**, landed as ONE commit `patch-port(004)` |

The current uplift's inventory row stays "(staged)" until the human commits.

Design ratified in
[`../proposals/2026-06-17-port-004-replace-mergetree-with-replicated-gated.md`](../proposals/2026-06-17-port-004-replace-mergetree-with-replicated-gated.md).

## 1. Purpose

In a `DatabaseReplicated`, a table created with a non-replicated engine
(`ENGINE = MergeTree`) is **not** replicated: an `INSERT` lands on one replica and
never propagates, so the database silently diverges. Patch 004 makes the
"every table in a Replicated database is self-replicating" invariant hold
**transparently**: on the replica-execution (`SECONDARY_QUERY`) path it rewrites a
non-replicated `*MergeTree` engine name to its `Replicated*` twin at
table-creation time, so callers need not know about replication
(`MergeTree`→`ReplicatedMergeTree`, `SummingMergeTree`→`ReplicatedSummingMergeTree`,
etc.).

This is the exact invariant that makes the **REPL-1** production flip
(`internal_replication=true`, the dropped-002 behavior) safe:
`internal_replication=true` is only sound when *every* table behind the
auto-cluster is `ReplicatedMergeTree` (ledger REPL-1, dossier 002 §5).

### 1.1 Upstream 26.3 has the same intent, different mechanism

`database_replicated_allow_only_replicated_engine` (`Bool`, default `false`,
**Cloud default `1`**) **rejects** a non-`Replicated` disk-storing table in a
`Replicated` DB — it does **not** convert. 004's transparent-convert UX is
genuinely absent upstream, so 004 is **not** obsoleted. The two are
complementary: 004 *converts* the `*MergeTree` family; the upstream reject-setting
*backstops* the non-`MergeTree` disk engines 004 cannot convert (§5.3 below). The
production posture (§7) uses both.

## 2. Policy decision — gate behind one default-off server setting (clause v)

Per `docs/aiven/AGENTS.md` §7 / clause (v), 004 changes default `CREATE TABLE`
behavior for *every* Replicated database — a broad blast radius — so it must not
ship on by default. The resolution (ratified in the proposal §4) is
**default-off-but-opt-in**, modelled on `enforce_https_for_url_storage`:

- New server setting `aiven_replace_mergetree_with_replicated` (`Bool`, default
  `false`), declared in `src/Core/ServerSettings.cpp` immediately after patch
  062's `aiven_prohibit_tmp_table_creation`. Third application of the AGENTS §8
  `aiven_` naming convention. Server-level, **not** session-overridable.
- The whole rewrite early-returns when the setting is off. **One switch, one
  place.** Gate OFF ⇒ a stock build never rewrites and is byte-for-behavior
  identical to upstream (test-neutral by construction).

## 3. Implementation (the gated port + 4 gap fixes)

Two source files, mirroring the original `226ed6cc31`:

- `src/Storages/StorageFactory.h` — new private method declaration
  `rewriteUnreplicatedMergeTreeEngines` under `Storages storages;`.
- `src/Storages/StorageFactory.cpp` — the method definition + the call site in
  `StorageFactory::get`, inserted right after the `ENGINE required` throw and
  immediately before `const ASTFunction & engine_def = *storage_def->engine;`
  (so `name = engine_def.name;` later picks up the rewritten name and the
  `storages.find(name)` resolution succeeds).

The method, in order, and the gap fixes it carries over the bare 25.8 source:

1. **Gate (§3.2 / proposal §4.2, gap fix).** Early-return when
   `local_context->getServerSettings()[ServerSetting::aiven_replace_mergetree_with_replicated]`
   is false.
2. **Exclude `ATTACH` (§5.1 gap fix).** `query.attach` is threaded in as a
   `bool is_attach` parameter; early-return when true. Re-engining an existing
   on-disk `MergeTree` as `Replicated*` would mis-adopt its local data. Upstream's
   own reject-check does the same (`!create.attach`). Invariant: *the rewrite only
   ever applies to a fresh `CREATE`, never to adopting existing data.*
3. **`*MergeTree` / not-already-`Replicated` shape check** (faithful to source):
   `endsWith(engine_name, "MergeTree") && !startsWith(engine_name, "Replicated")`.
4. **Replicated-DB + `SECONDARY_QUERY` scope** (faithful to source):
   `query_kind == ClientInfo::QueryKind::SECONDARY_QUERY` and the database engine
   is `Replicated`.
5. **Missing-twin safety (§5.4 gap fix).** Build the candidate
   `"Replicated" + engine_name` and only assign it if `storages.contains(candidate)`;
   otherwise leave the name unchanged and let normal resolution proceed (which
   yields the normal `UNKNOWN_STORAGE` for the original name). Invariant: *the
   rewrite never produces an unregistered engine name.* (The original used a bare
   `engine_name.insert(0, "Replicated")` with no existence check.)

**Stored-DDL consistency (§5.2 — verified already satisfied, not a code change).**
The rewrite mutates `storage_def->engine->name` on the **same** `ASTCreateQuery`
that `InterpreterCreateQuery::doCreateTable` later persists via
`database->createTable(…, query_ptr)` (`StorageFactory::get` is called from the
create interpreter, which then persists the same AST). Because the rewrite is
deterministic, every replica re-derives the same engine from the (still-`MergeTree`)
replicated DDL-log entry and converges to identical stored metadata (matching
`DatabaseReplicated` digest). So the gate point can **stay** inside
`rewriteUnreplicatedMergeTreeEngines`; this is downgraded to a **test assertion**
(`SHOW CREATE TABLE` reports `Replicated*` on both nodes — §5.1 of the test).

**Non-`MergeTree` disk engines (§5.3 — config, not code).** 004 only touches
`*MergeTree`. Production additionally sets the upstream
`database_replicated_allow_only_replicated_engine = 1` (Cloud default) to **reject**
any non-replicated disk engine 004 didn't convert. Documented in ledger REPL-6.

### 3.1 Includes added to `StorageFactory.cpp`

`<Databases/IDatabase.h>` (for `getEngineName`), `<Interpreters/ClientInfo.h>`
(for `ClientInfo::QueryKind::SECONDARY_QUERY`),
`<Interpreters/DatabaseCatalog.h>` (for `DatabaseCatalog::instance().getDatabase`),
and `<Core/ServerSettings.h>` (for `getServerSettings()` + the `ServerSetting::`
accessor). A `namespace ServerSetting { extern const ServerSettingsBool
aiven_replace_mergetree_with_replicated; }` forward-decl mirrors the
`InterpreterCreateQuery.cpp` pattern.

## 4. Invariants the design protects

1. **No default-behavior change.** Gate off ⇒ no rewrite, stock 26.3 behavior.
2. **Single server-only switch.** Not session-overridable; one audit point.
3. **`CREATE`-only.** Never rewrites `ATTACH` — existing data is never silently
   re-engined.
4. **Cross-replica convergence.** Stored DDL + metadata digest reflect the
   rewritten engine; all replicas agree.
5. **No fabricated engines.** Only rewrites to a registered `Replicated*` twin.
6. **Replicated-DB-scoped.** Only fires for `SECONDARY_QUERY` inside a
   `Replicated` database.

## 5. Upstream-drift findings

- **`StorageFactory::get` signature.** At 26.3 the `local_context` parameter is a
  `ContextMutablePtr` (matching the method's `const ContextMutablePtr &` param);
  `query.getDatabase()` and `query.attach` exist on `ASTCreateQuery`. The call
  site anchor (after the `ENGINE_REQUIRED` throw, before `engine_def`) is intact.
- **Registry member.** The private `Storages storages;` (an
  `unordered_map<string, Creator>`) is the existence-check source for the
  missing-twin fix. `clang-tidy` flagged `storages.find(c) != storages.end()` →
  used `storages.contains(c)` (C++20, equivalent, lint-clean).
- **Settings access.** Mirrors patch 062's `aiven_prohibit_tmp_table_creation`
  usage in `InterpreterCreateQuery.cpp`. No drift.
- Conclusion: **`still-needed-but-rewrite`** — semantics carried; the port adds the
  gate plumbing + three behavioral gap fixes. `byte_equivalent: false` (the gate
  and gap fixes are Aiven-introduced additions over the source).

## 6. C++ review

Per `docs/aiven/skills/cpp-review-checklist.md`:

- 1 Lifetime + ownership: ✓ — the method takes `const String & database_name`,
  `const ContextMutablePtr & local_context`, `bool is_attach`, `String &
  engine_name`; it borrows, mutates the caller-owned `engine_name` in place, and
  owns nothing. The `candidate` local is a plain `String`. No new ownership.
- 2 Exception safety: ✓ — the rewrite runs before engine instantiation and only
  reassigns a `String`; the only throwing call (`getDatabase`) is the same lookup
  the unmodified path performs immediately afterwards (it would throw there too).
  No partial state on throw.
- 3 Thread-safety + concurrency: ✓ — `storages` is populated at startup and read
  thread-safely thereafter (the unmodified `get` already reads it under no lock);
  the setting is read via the thread-safe `getServerSettings()` accessor. No new
  shared state, no lock, no sleep.
- 4 Performance + memory: ✓ — when the gate is off (the default) the cost is one
  `bool` server-setting read on the cold `CREATE` DDL path and an immediate
  return; when on, two `endsWith`/`startsWith` scans, one `getClientInfo`
  comparison, one catalog lookup, and one `unordered_map` probe — all on the cold
  DDL path, never on a data path. One short-lived `String` allocation for the
  candidate (only when the shape/scope checks pass).
- 5 Settings as public API: ✓ — new `ServerSetting`
  `aiven_replace_mergetree_with_replicated` (Bool, default **false** — safe new
  gate), `aiven_` prefix per AGENTS §8, server-level (cannot be overridden
  per-session).
- 6 Error handling: ✓ — the rewrite never throws of its own; the missing-twin
  branch deliberately leaves the name unchanged so the existing `UNKNOWN_STORAGE`
  path reports the **original** engine name (no fabricated `Replicated*` name, no
  `LOGICAL_ERROR`). Asserted by the missing-twin test.
- 7 Upstream / vendored code: ✓ — no `contrib/**`, `.claude/**`,
  `.github/workflows/**`, or root `AGENTS.md` touched. Only `StorageFactory.{h,cpp}`
  and `ServerSettings.cpp`.
- 8 Behavior under settings: ✓ — gate OFF ⇒ nothing observable (early return before
  any name change). Verified by the OFF-neutrality differential leg of the
  integration test (same DDL stays `MergeTree`, row does not replicate).

## 7. Production posture

- **Required config (not code):**
  ```xml
  <aiven_replace_mergetree_with_replicated>true</aiven_replace_mergetree_with_replicated>
  ```
  plus the upstream backstop `database_replicated_allow_only_replicated_engine = 1`
  (Cloud default) for the non-`MergeTree` disk-engine gap (§5.3).
- **REPL-1 coupling (load-bearing sequencing).** This setting is what makes the
  REPL-1 `internal_replication=true` flip safe. Enable
  `aiven_replace_mergetree_with_replicated=1` (and the reject-backstop)
  **before/with** flipping `internal_replication=true`, never after. If 004 is not
  enabled, do not flip REPL-1. See ledger REPL-1 / REPL-6.

## 8. Test design

Because `aiven_replace_mergetree_with_replicated` is a **server** setting (cannot
be set per-session) and the property is genuinely cluster-level (cross-replica
data flow), the proof is an **integration** test:
`tests/integration/test_aiven_replace_mergetree_with_replicated/` (`test.py` +
`configs/enable_replace.xml`). 4 instances + ZooKeeper: a setting-ON Replicated-DB
pair (`node_on1`/`node_on2`, `rdb_on`) and a setting-OFF pair
(`node_off1`/`node_off2`, `rdb_off`). **8/8 PASS** against the locally-built
patched binary (`8 passed in 30.30s`):

1. `test_on_conversion_and_real_replication` — setting ON: `ENGINE = MergeTree` →
   `ReplicatedMergeTree` on **both** nodes (`system.tables.engine` + `SHOW CREATE`
   agree, §5.2), and an INSERT on `node_on1` is visible on `node_on2` (REAL
   replication, the evidence of causation — not a cosmetic rename).
2. `test_on_variant_engines_convert_and_replicate[SummingMergeTree|ReplacingMergeTree]`
   — the variants convert to their `Replicated*` twins and replicate.
3. `test_on_non_replicated_database_never_converts` — even with the setting ON, a
   `MergeTree` table in an `Atomic` database stays `MergeTree` (Replicated-DB-scoped).
4. `test_on_missing_twin_not_rewritten` — `ENGINE = FooMergeTree` (no registered
   `ReplicatedFooMergeTree`) is left unchanged: the error is the normal
   `UNKNOWN_STORAGE` naming `FooMergeTree` (not the fabricated twin), no
   `LOGICAL_ERROR`. This is a real exercise of the §5.4 missing-twin branch via SQL
   (no fake-engine registration needed).
5. `test_off_neutrality_no_conversion_no_replication` — the differential leg:
   setting OFF ⇒ identical DDL stays `MergeTree` and a row inserted on `node_off1`
   is **absent** on `node_off2` (a plain `MergeTree` in a Replicated DB replicates
   its definition but not its data). This is the pre/post evidence pair.
6. `test_log_engine_backstop_when_only_replicated_allowed` — for the record (§5.3):
   with `database_replicated_allow_only_replicated_engine=1`, `ENGINE = Log` in a
   Replicated DB is **rejected** (upstream-owned behavior, documents the combined
   posture).
7. `test_attach_not_converted` — the §5.1 `!attach` guard. A `MergeTree` table is
   created while the setting is OFF (stored as `MergeTree` on disk) and
   `DETACH … PERMANENTLY`-ed; the setting is then turned ON (config drop-in +
   `restart_clickhouse`) and the table re-`ATTACH`-ed. The `ATTACH` runs as a
   `SECONDARY_QUERY` with `attach=true` against a stored `MergeTree` engine inside a
   `Replicated` database — exactly the path the guard protects — and the engine
   stays `MergeTree` (no mis-adoption; the on-disk row `7` survives). This honestly
   exercises the guard: without it the stored `MergeTree` would be converted on
   ATTACH.

Why this distinguishes the Aiven behavior (AGENTS §7): the ON/OFF differential
(cases 1 vs 5) proves the SERVER setting — not some unrelated mechanism — is what
drives the conversion + replication, and case 7 proves the `!attach` guard via a
real user `ATTACH` DDL on the protected path.

### 8.1 Coverage note / judgment call (§5.4 missing-twin)

The proposal's §5.5/§5.4 missing-twin case ideally registers a fake `*MergeTree`
engine with no `Replicated*` twin. All real registered `*MergeTree` engines in core
have `Replicated*` twins, and a test cannot register a fake engine from SQL.
**Resolution (not a reduction):** case 4 uses `ENGINE = FooMergeTree`, which still
*enters* the rewrite (`endsWith "MergeTree"`, not `startsWith "Replicated"`) but
whose candidate `ReplicatedFooMergeTree` is unregistered — so the
`storages.contains` guard leaves the name unchanged and resolution fails with the
normal `UNKNOWN_STORAGE` for the **original** name. This is a faithful, sufficient
test of the missing-twin branch; no coverage was dropped.

## 9. Rollback considerations

- Revert safety: safe. No schema migration, no on-disk format change. The rewrite
  only changes the engine *name* chosen at `CREATE` time for new tables.
- Surviving state: tables already created as `Replicated*` while the setting was on
  stay `Replicated*` (their stored DDL is `Replicated*`); disabling the setting only
  stops *future* conversions. This is the intended, safe behavior.
- Disable without rebuild: set `aiven_replace_mergetree_with_replicated` to false
  (its default). Server-level setting; fully controlled by config.

## 10. Per-uplift notes

### ≤25.8-aiven (historical)

Original carry `226ed6cc31` (Tilman Moeller): an **unconditional**
`engine_name.insert(0, "Replicated")` with no gate, no `!attach` guard, and no
twin-existence check. Re-applied ≈12 times back to ~25.3; never upstreamed in this
*convert* form.

### 26.3-aiven (this uplift)

- Landed as ONE commit `patch-port(004)`.
- **Gated** (parent policy call, clause v): the whole rewrite is behind the new
  default-off `aiven_replace_mergetree_with_replicated`. Gate OFF = byte-identical
  to stock 26.3.
- **Four gap fixes** over the source: gate, exclude `ATTACH`, missing-twin safety,
  stored-DDL consistency (the last verified already satisfied → carried as a test
  assertion, not a code move).
- Build: warm incremental, ~11 TUs recompiled (`StorageFactory.cpp`,
  `ServerSettings.cpp`, `InterpreterCreateQuery.cpp`, `registerStorages.cpp`, and
  dependents); no `#deps 0` staleness after the `StorageFactory.h` method
  declaration (consumed only by `StorageFactory.cpp`). Green, no warnings.
- Tests added: `tests/integration/test_aiven_replace_mergetree_with_replicated/`
  (8/8 PASS).
- Judgment call: the missing-twin case is covered via `ENGINE = FooMergeTree` SQL
  (§8.1) rather than registering a fake engine (impossible from a test) — a
  faithful test of the branch, no coverage dropped. The ATTACH case uses an
  OFF→ON config flip + restart to obtain a stored `MergeTree` engine that an
  explicit user `ATTACH` DDL then exercises against the live guard.

# Proposal: port patch 004 (`MergeTree` → `ReplicatedMergeTree` auto-substitution) behind a default-off `aiven_` server setting, with gap fixes and proper testing

> **Document type:** Proposal / design sketch (awaiting maintainer ratification).
> **Date:** 2026-06-17.
> **Status:** DRAFT — proposed. Decision needed before patch 004 is dispatched.
> **Audience:** the maintainer who owns the Aiven fork's Replicated-database behavior + managed-config surface; the engineer who would port 004.
> **One-sentence framing:** Carry 004's transparent "write `ENGINE=MergeTree` in a `Replicated` database, get `ReplicatedMergeTree`" behavior, but (a) wrap it in a single default-**off** server setting `aiven_replace_mergetree_with_replicated` so a stock build is byte-for-behavior identical to upstream, (b) close the coverage/correctness gaps the 25.8 source left open (`ATTACH`, stored-DDL consistency, non-`MergeTree` disk engines, missing-twin engines), and (c) test it with a real cross-node replication evidence pair.

## 1. Goal & motivation

In a `DatabaseReplicated`, a table created with a non-replicated engine (`ENGINE = MergeTree`) is **not** replicated: an `INSERT` lands on one replica and never propagates, so the database silently diverges. This is also the exact hazard that makes the **REPL-1** production flip (`internal_replication=true`, the dropped-002 behavior) unsafe — `internal_replication=true` is only sound when *every* table behind the auto-cluster is `ReplicatedMergeTree` (see [`major-upstream-changes.md`](../uplifts/26.3/major-upstream-changes.md) REPL-1, dossier 002 §5).

Patch 004 makes that invariant hold **transparently**: it rewrites `*MergeTree` engine names to their `Replicated*` twin at table-creation time inside a Replicated database, so callers need not know about replication.

**Why this is a clause-(v) patch.** 004 changes default `CREATE TABLE` behavior for every Replicated database — a broad blast radius. Per `AGENTS.md` §7 / clause (v) it must not ship on by default; the human decides whether to gate it behind a new default-disabled server setting. Production runs it on, so the resolution is **default-off-but-opt-in**, not drop.

**Decision requested:** approve the gated design (§4), the gap fixes (§5), and the test plan (§8), then dispatch the 004 port.

## 2. Background — what 004 adds, and the 26.3 landscape

### 2.1 What 004 does (source `226ed6cc31`, 2 files / +30)
`StorageFactory::get` calls a new `rewriteUnreplicatedMergeTreeEngines(database_name, context, engine_name)` *before* engine extraction. The method prepends `"Replicated"` to `engine_name` iff:
- `endsWith(engine_name, "MergeTree")` and **not** `startsWith(engine_name, "Replicated")`, and
- `query_kind == SECONDARY_QUERY` **and** the database engine is `Replicated`.

So `MergeTree`→`ReplicatedMergeTree`, `SummingMergeTree`→`ReplicatedSummingMergeTree`, etc., only on the replica-execution path of a Replicated DB. It has lived on the Aiven line across many LTS carries (≈12 re-applications, back to ~25.3); it has never been upstreamed in this *convert* form.

### 2.2 Upstream 26.3 has the same intent, different mechanism
`database_replicated_allow_only_replicated_engine` (`Bool`, default `false`, **Cloud default `1`**; `Settings.cpp:5632`, enforced `InterpreterCreateQuery.cpp:2080`): when on, a non-`Replicated` disk-storing table in a `Replicated` DB is **rejected** with an error — it does **not** convert.

| | 004 (Aiven) | upstream `allow_only_replicated_engine=1` |
|---|---|---|
| Behavior | silently **converts** `*MergeTree` → `Replicated*` | **rejects** any non-`Replicated` disk engine |
| Caller impact | `ENGINE=MergeTree` "just works" | caller must write `ReplicatedMergeTree` or error |
| Coverage | `*MergeTree` family only | **all** disk-storing non-replicated engines |
| Cost | Aiven code (this patch) | config only (setting exists) |

**Conclusion (from the 004 deep screen):** 004 is **not** obsoleted — its transparent-convert UX is genuinely absent upstream. But the two are complementary: 004 converts the `*MergeTree` family; the upstream reject-setting backstops the *non*-`MergeTree` disk engines 004 cannot convert (gap §5.3). The recommended production posture (§7) uses both.

## 3. Why a server-level, default-off guard

- **Right layer.** "Auto-replicate tables in Replicated DBs" is a fleet/deployment decision, not a per-query one. A `ServerSetting` that cannot be overridden in a session (the posture of `enforce_https_for_url_storage`, `ServerSettings.cpp`) is correct — a tenant must not toggle it from a `SETTINGS` clause.
- **Clause (v) compliance.** Default-off ⇒ a stock build behaves identically to upstream (no rewrite), so there is no broad-blast-radius default change and the stateless suite is undisturbed. The gated behavior is tested with the setting explicitly on (§8).
- **`aiven_` convention (AGENTS §8).** The setting is brand-new (no upstream equivalent), so it takes the `aiven_` prefix.

## 4. Proposed design — `aiven_replace_mergetree_with_replicated`

### 4.1 The setting
`src/Core/ServerSettings.cpp` — one `DECLARE`, modelled on the other `aiven_` server guards:

```cpp
DECLARE(Bool, aiven_replace_mergetree_with_replicated, false, R"(
Aiven: when enabled, a table created in a `Replicated` database with a
non-replicated `*MergeTree` engine (e.g. `MergeTree`, `SummingMergeTree`) is
automatically rewritten to its `Replicated*` equivalent, so every table in a
Replicated database is self-replicating even if the caller wrote a plain
`MergeTree` engine. Applies only on the replica-execution (`SECONDARY_QUERY`)
path and never to `ATTACH`. Disabled by default; behaves exactly like upstream
when off. This is a server-level setting and cannot be overridden in a session.)", 0) \
```

### 4.2 The gate point
Guard the rewrite inside `rewriteUnreplicatedMergeTreeEngines` on
`local_context->getServerSettings()[ServerSetting::aiven_replace_mergetree_with_replicated]`
— off ⇒ early-return, the engine name is untouched, behavior is stock 26.3. One switch, one place to reason about "is the feature live."

## 5. Gap fixes (carried as part of the port, not the bare 25.8 source)

The 25.8 patch is a minimal string-prepend. The port closes four gaps; each is a small, bounded addition with its own test (§8).

### 5.1 Exclude `ATTACH` (correctness)
The 25.8 code has **no `!attach` guard** (upstream's reject-check does: `!create.attach`). Converting on `ATTACH` could re-engine a pre-existing on-disk `MergeTree` table as `Replicated*` and mis-adopt its local data. **Fix:** skip the rewrite when the query is an `ATTACH` (thread the attach flag, or detect via the create AST). Invariant: *the rewrite only ever applies to a fresh `CREATE`, never to adopting existing data.*

### 5.2 Stored-DDL consistency (no engine/metadata skew) — verified already satisfied
The rewrite mutates the in-memory AST during `StorageFactory::get`. **Source check (2026-06-17):** at `InterpreterCreateQuery.cpp:2061` the rewrite runs inside `StorageFactory::instance().get(create, …)`, which mutates `create.storage->engine->name` on the **same** `ASTCreateQuery` that line 2109 then passes to `database->createTable(…, query_ptr)` — so the rewritten `Replicated*` engine is what gets **persisted** to the local `.sql`, and because the rewrite is deterministic every replica re-derives the same engine from the (still-`MergeTree`) replicated DDL-log entry and converges to identical stored metadata (so the `DatabaseReplicated` digest matches across replicas). Therefore the gate point can **stay** in `rewriteUnreplicatedMergeTreeEngines` (no need to move it into the create interpreter). Downgraded from a fix to a **test assertion**: the integration test asserts `SHOW CREATE TABLE` reports `Replicated*` on **both** nodes (§8.1). Invariant (now proven, not just asserted by construction): *all replicas converge to the same stored engine text; `SHOW CREATE` never disagrees with the actual storage.*

### 5.3 Non-`MergeTree` disk engines (coverage)
004 only touches `*MergeTree`; a `Log`/`StripeLog`/`TinyLog`/other disk engine stays non-replicated — the very divergence REPL-1 fears. **Fix (config, complementary, not code):** production additionally sets the upstream `database_replicated_allow_only_replicated_engine = 1` (its Cloud default), which **rejects** any non-replicated disk engine 004 didn't convert. Net posture: 004 *converts* the `*MergeTree` family transparently; the upstream guard *rejects* the rest. Document this pairing in the ledger (§7). (If a future requirement wants conversion of non-`MergeTree` engines too, that is a separate, larger change.)

### 5.4 Missing-twin safety (robustness)
`engine_name.insert(0, "Replicated")` assumes every `*MergeTree` has a registered `Replicated*` twin. **Fix:** after constructing the candidate name, check it exists in the `StorageFactory` registry (`storages.find`); if not, leave the name unchanged and let normal resolution proceed (or throw a clear, specific error) rather than fabricating a non-existent engine. Invariant: *the rewrite never produces an unregistered engine name.*

## 6. Invariants the design must protect

1. **No default-behavior change.** Guard off ⇒ no rewrite, stock 26.3 behavior (clause (v)).
2. **Single server-only switch.** Not session-overridable; one audit point.
3. **`CREATE`-only.** Never rewrites `ATTACH` (§5.1) — existing data is never silently re-engined.
4. **Cross-replica convergence.** Stored DDL + metadata digest reflect the rewritten engine; all replicas agree (§5.2).
5. **No fabricated engines.** Only rewrites to a registered `Replicated*` twin (§5.4).
6. **Replicated-DB-scoped.** Only fires for `SECONDARY_QUERY` inside a `Replicated` database; standalone tables and non-Replicated DBs are untouched.

## 7. Backward-compat & integration

- **Production opt-in (required config, not code):**
  ```xml
  <aiven_replace_mergetree_with_replicated>true</aiven_replace_mergetree_with_replicated>
  ```
  and, to backstop the non-`MergeTree` gap (§5.3), keep the upstream
  `database_replicated_allow_only_replicated_engine = 1` (Cloud default).
- **REPL-1 coupling (load-bearing sequencing).** This setting is what makes the
  REPL-1 `internal_replication=true` flip safe (all auto-cluster tables become
  `ReplicatedMergeTree`). Enable `aiven_replace_mergetree_with_replicated=1`
  (and the reject backstop) **before/with** flipping `internal_replication=true`,
  never after. Conversely, if 004 is *not* enabled, do not flip REPL-1.
- **Ledger.** Record as a new `REPL-6` entry in
  [`major-upstream-changes.md`](../uplifts/26.3/major-upstream-changes.md),
  tagged `⚠ operational`, cross-linking REPL-1.
- **Open assumption (verify).** Whether Aiven's control-plane / customers rely on
  the transparent-convert UX (vs being able to write `ReplicatedMergeTree`
  directly). This proposal assumes *yes* (hence port rather than drop-to-reject);
  if *no*, the simpler disposition is to drop 004 and rely on
  `allow_only_replicated_engine=1` alone (the 002/058 pattern).

## 8. Verification plan (proper testing)

Default-off ⇒ no stateless-suite impact. The evidence-of-causation test runs the **gated** behavior with the setting explicitly enabled (clause (v)). Because the property is genuinely cluster-level (cross-replica data flow), this is an **integration** test: `tests/integration/test_aiven_replace_mergetree_with_replicated/`.

1. **On — conversion + REAL replication (the causation pair).** 2-node `Replicated` DB, setting on. `CREATE TABLE t (...) ENGINE = MergeTree ORDER BY ...`; assert `engine` is `Replicated*` in `system.tables` on **both** nodes, `SHOW CREATE TABLE` agrees (§5.2), and an `INSERT` on node A is visible on node B (proves real replication, not a cosmetic rename). The differential leg (setting **off**, same DDL) yields a plain `MergeTree` and the row is **absent** on node B.
2. **Variant coverage.** Parametrize `SummingMergeTree` / `ReplacingMergeTree` → `Replicated*` twins convert and replicate.
3. **Off — neutrality.** Setting off ⇒ `ENGINE=MergeTree` stays `MergeTree` (identical to stock 26.3); and inside a non-Replicated DB the rewrite never fires even when on.
4. **`ATTACH` not converted (§5.1).** `ATTACH` of an existing `MergeTree` table keeps its `MergeTree` engine (no silent adoption).
5. **Missing-twin (§5.4).** A `*MergeTree`-named engine without a registered `Replicated*` twin is not rewritten to a bogus name (clear behavior, no crash).
6. **Non-`MergeTree` backstop (§5.3).** With `database_replicated_allow_only_replicated_engine=1`, `ENGINE=Log` in a Replicated DB is rejected (documents the combined posture; upstream-owned behavior, asserted for the record).

## 9. Decision checklist

- [ ] Approve the **default-off server guard** `aiven_replace_mergetree_with_replicated` (§4) as the clause-(v) resolution (vs faithful default-on port, vs drop-to-reject).
- [ ] Confirm the setting **name** (alts: `aiven_auto_replicate_mergetree`, `aiven_replicated_db_force_replicated_engine`).
- [ ] Approve the **gap fixes** §5.1–§5.4 as part of the port (esp. the `!attach` guard and the stored-DDL-consistency requirement).
- [ ] Confirm the **production posture** (§7): enable 004's setting **and** keep `database_replicated_allow_only_replicated_engine=1` as the non-`MergeTree` backstop, sequenced with the REPL-1 flip.
- [ ] Resolve the **open assumption** (§7): is transparent-convert relied upon? (If not → drop 004, use reject-config only.)
- [ ] Approve the **integration test** plan (§8), incl. the off/on causation pair and the gap-guard cases.

Once decided, 004 is dispatched against this gated design: new server-setting decl + the guarded `rewriteUnreplicatedMergeTreeEngines` with the four gap fixes; an integration test with the setting on; and a `REPL-6` ledger entry. The `aiven_` setting follows the AGENTS §8 convention.

# Patch 058 — disallow-replication-parameter-customization

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `9a2883592c` | Tilman Moeller (author), co-authored by Dmitry Potepalov, 2026-01-13 | (the version we are porting FROM) |
| 26.3-aiven | `patch-drop(058)` | parent agent, 2026-06-17 | `obsoleted-by-upstream` — drop; replacement is **config**, see §2/§5 **(SUPERSEDED — see next row)** |
| 26.3-aiven (`v26.3.15.4`) | re-port, this session | agent, 2026-06-30 | **`ported-gated`** — the drop is reversed: 058's original code is reinstated **verbatim**, wrapped in the default-off server setting `aiven_enforce_default_replication_path`. See §2 "Reversal" and §3. |

> **Status: REVERSED → ported-gated.** The 2026-06-17 drop (rows above and the
> §2 findings) was superseded by an explicit human decision to bring 058 back
> rather than rely on config (`database_replicated_allow_replicated_engine_arguments`
> pinned `readonly`). The drop rationale is **kept verbatim below for the
> historical record** — read §2 "Reversal" first; everything before it is the
> as-of-2026-06-17 analysis that no longer reflects the shipped state.

The shipped 26.3 state carries 058's original lambda guard (the
`expand_special_macros`-based comparison), unchanged except for being **gated**
behind a default-off `aiven_` server setting so that gate-OFF is byte-identical
to upstream.

## 1. Purpose

Per the source commit body:

> Disallow replication parameters customization
>
> Enforce that ReplicatedMergeTree tables must use default ZooKeeper path
> and replica name from server settings. Disallows any custom values to
> ensure consistent replication configuration.

The change (`9a2883592c`, 1 file `src/Storages/MergeTree/registerStorageMergeTree.cpp`,
+29) added, inside `extractZooKeeperPathAndReplicaNameFromEngineArgs`'s
`expand_macro` lambda, a guard that throws `BAD_ARGUMENTS` when the
user-provided `zookeeper_path` / `replica_name` differ from the
**macro-expanded** server defaults (`default_replica_path` /
`default_replica_name`, expanded via a new `expand_special_macros` helper with
`expand_special_macros_only = true` and `table_id.uuid = Nil`). The guard is
**unconditional** — no setting can switch it off.

The motivation in Aiven's managed fleet: every `ReplicatedMergeTree` table's ZK
coordination path and replica name must be derived from the server-templated
defaults, so customers cannot pin a divergent path that breaks the managed
replication topology / backup / restore conventions.

Source SHA on `v25.8.x-lts-aiven`: `9a2883592c`. Original author:
`tilman.moeller@aiven.io`.

## 2. Upstream-drift / validity findings

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`.

### Commands run

```bash
git show 9a2883592c                       # the original 058 diff
git grep -n 'database_replicated_allow_replicated_engine_arguments' \
    src/Storages/MergeTree/registerStorageMergeTree.cpp src/Core/Settings.cpp
#  registerStorageMergeTree.cpp:263-294 — the upstream enforcement branch
#  Settings.cpp:5637 — DECLARE(UInt64, ..., 0, ...) default 0
```

### Findings

- **26.3 already enforces 058's intent by default.** Upstream
  `registerStorageMergeTree.cpp:263-294` keys on the session setting
  `database_replicated_allow_replicated_engine_arguments` (`UInt64`, **default
  `0`**, `Settings.cpp:5637`):
  - `0` → throws `BAD_ARGUMENTS` ("It's not allowed to specify explicit
    `zookeeper_path` and `replica_name` for `ReplicatedMergeTree` arguments in
    Replicated database…") when the explicit args differ from the defaults.
  - `1` → allow + `LOG_WARNING`; `2` → allow but replace with defaults; `3` →
    allow silently.
  So a stock 26.3 server already rejects custom ZK path / replica name for
  `ReplicatedMergeTree` in a `Replicated` database — the same outcome 058
  produced.

- A verbatim cherry-pick would be **redundant** (a second, parallel guard for the
  same rejection) and carries two semantic mismatches against the upstream design
  (see the table), so the disposition is `obsoleted-by-upstream`.

| Aspect | Patch 058 (Aiven, from 25.8) | Upstream 26.3 |
|---|---|---|
| Mechanism | Unconditional throw in `expand_macro` lambda | Throw gated on `database_replicated_allow_replicated_engine_arguments == 0` |
| Default posture | Always on (cannot be disabled) | On by default (setting default `0`), but **a user can `SET …=1/2/3` to bypass** |
| Scope | All `ReplicatedMergeTree` creations the lambda sees (incl. **standalone**, non-Replicated-DB tables) | Only when `is_replicated_database` (`SECONDARY_QUERY` + engine `Replicated`) |
| Comparison | Against **macro-expanded** defaults (`expand_special_macros`) | Against the **raw** setting string (so `CREATE TABLE t1 AS t2` copying raw-template args still passes) |

### Consequences of dropping 058 — three deltas (the question that drove this screen)

1. **Bypassability (the material one).** Upstream's guard is a **session setting**;
   a user can `SET database_replicated_allow_replicated_engine_arguments=1` and
   then pass a custom path, bypassing the protection. 058 could **not** be
   bypassed. → If Aiven needs a *hard* guarantee, the upstream default alone is
   insufficient and the setting must be made **non-overridable** (see §5).
2. **Scope gap.** Upstream's check requires `is_replicated_database`, so it does
   **not** fire for a standalone `ReplicatedMergeTree` created outside a Replicated
   database; 058's lambda-level check did. → Only relevant if Aiven creates
   replicated tables outside Replicated databases. **Open assumption (unverified at
   screen time):** Aiven's topology is believed to be exclusively Replicated
   databases (same premise underlying the 002/004 decisions), in which case this
   gap is moot. **To verify before relying on the drop in production.**
3. **Comparison semantics.** Upstream's **raw**-string comparison is deliberate —
   it lets `CREATE TABLE t1 AS t2` (which copies t2's stored raw-template engine
   args, e.g. `/clickhouse/tables/{uuid}/{shard}`) pass; 058's **expanded**
   comparison could *reject* that `CREATE … AS` case. Here upstream is arguably
   **better**, so porting 058 unconditionally would risk a `CREATE AS` regression.

- Conclusion (as of 2026-06-17): **`obsoleted-by-upstream`** — drop. *(Superseded
  — see "Reversal" below.)*

### Reversal (2026-06-30, `v26.3.15.4`) — re-port gated behind `aiven_enforce_default_replication_path`

Human decision: do **not** rely on the config-only path. Two of the three deltas
above made the drop materially weaker than 058, and downstream tests
(`test_replicated_merge_tree_does_not_accept_custom_replication_params`, ×3)
encode 058's behavior, not upstream's:

1. **Bypassability** — upstream's guard is a *session* setting; a tenant can
   `SET database_replicated_allow_replicated_engine_arguments=1` and pin a foreign
   path. The `readonly` constraint in §5 closes this only if applied to **every**
   profile; 058's code path cannot be bypassed at all.
2. **Scope** — upstream only fires for `is_replicated_database`; 058's lambda
   guard fires for every `ReplicatedMergeTree` creation, including standalone
   (non-Replicated-DB) tables, which is what the downstream tests exercise.

Decision: **stick to the original patch** (do not redesign the comparison or the
scope), and only add a gate so the fork default and upstream test suite are
undisturbed. Concretely:

- Reinstated 058's `expand_special_macros` helper and its two `BAD_ARGUMENTS`
  throws inside the `expand_macro` lambda, **verbatim**.
- Wrapped both throws in `if (server_settings[ServerSetting::aiven_enforce_default_replication_path])`.
  The setting is a `Bool` server setting, **default `false`** → gate-OFF behaves
  exactly like upstream (only the `database_replicated_allow_replicated_engine_arguments`
  guard runs). Aiven's managed config sets it `true`.
- **Carried caveat (accepted, not fixed):** the verbatim expanded-comparison can
  reject `CREATE TABLE t1 AS t2` when `t2`'s stored engine args carry the
  raw-template path — this is delta 3 above. We keep it because it shipped this
  way in 25.8 production without issue (parity argument, same call made for 009),
  and because "fixing" it (raw-OR-expanded compare) would diverge from the
  original patch. Documented here so the next uplift does not mistake it for a
  regression.
- **Inherent assumption (load-bearing), corrected:** the guard lives in the lambda
  that the *default-args* path also calls (passing the raw `default_replica_path`),
  and it compares that against `expand_special_macros(default_replica_path)`. Per
  `Common/Macros.cpp`, `expand_special_macros_only` expands **only** `{database}`
  and `{table}` — `{uuid}`, `{shard}`, `{replica}`, and config macros are left
  intact (`{uuid}` is explicitly gated behind `!expand_special_macros_only`). So
  the raw default equals its expansion — i.e. default creation does **not**
  throw — as long as `default_replica_path` / `default_replica_name` do **not**
  embed `{database}`/`{table}`. The standard/upstream default
  `/clickhouse/tables/{uuid}/{shard}` + `{replica}` is therefore **safe** (an
  earlier draft of this note wrongly claimed `{uuid}` broke it). The integration
  test can use the stock default.

## 3. C++ review

Re-ported code (1 file, `src/Storages/MergeTree/registerStorageMergeTree.cpp`,
plus the `DECLARE` in `src/Core/ServerSettings.cpp`):

- **Helper** `expand_special_macros` — verbatim from `9a2883592c`. Expands only
  the identity-bound special macros (`{uuid}`/`{database}`/`{table}`) with a Nil
  uuid, leaving server macros (`{shard}`/`{replica}`) intact, so the managed
  default template can be compared independently of per-table identity.
- **Guard** inside `expand_macro`: throws `BAD_ARGUMENTS` if the supplied
  `zookeeper_path` / `replica_name` differ from the expanded managed defaults.
  Gated on `aiven_enforce_default_replication_path` (default `false`).
- **Invariant protected:** in the managed fleet every `ReplicatedMergeTree`
  ZooKeeper coordination path/replica name is derived from the server template; a
  tenant cannot pin a divergent path that could collide with, read, or corrupt
  another tenant's replicated metadata.
- **Placement note:** `const auto & server_settings = local_context->getServerSettings();`
  was moved above the lambda (it was previously declared after it) so the `[&]`
  capture can see it — same mechanical move the original patch made.
- **Exception safety:** the throw happens before any ZK node is created
  (`TableZnodeInfo::resolve` runs after the guard), so a rejected `CREATE` leaves
  no partial state. Gate-OFF = no new code path executes.

## 4. Test design

Integration test `tests/integration/test_aiven_enforce_default_replication_path/`
(**5 cases, all green** — `5 passed`). One node turns the gate **on** via
`<aiven_enforce_default_replication_path>1`; a companion node leaves it absent
(default off). Tables are **standalone** `ReplicatedMergeTree` (not in a
Replicated DB) — precisely the scope upstream's
`database_replicated_allow_replicated_engine_arguments` guard does **not** cover,
so the ON/OFF difference isolates the Aiven patch.

> **Test-construction lesson (cost a first red run):** a *standalone* table cannot
> resolve the stock `/clickhouse/tables/{uuid}/{shard}` default — `TableZnodeInfo::resolve`
> throws the unrelated upstream *"Macro `uuid` … only supported … when using the
> Replicated database engine"* (`Macros.cpp:115`), which fires **after** our guard
> and masks it. The guard itself passed (it correctly let the default template
> through). Fix: the gate-ON node overrides `default_replica_path` to a
> `{shard}`-only template (`/clickhouse/tables/{shard}/s058`) — still non-special
> (no self-reject) and `{uuid}`-free (valid for standalone tables). In production
> the stock `{uuid}` default is fine because Aiven tables live in Replicated DBs,
> where `{uuid}` resolves.

Cases (gate ON):

- **Default creation passes** — `CREATE TABLE … ReplicatedMergeTree ORDER BY …`
  with no explicit args succeeds (proves the default-args path does not
  self-reject; the load-bearing assumption from §2).
- **Explicit default-template path passes** — passing the exact
  `'/clickhouse/tables/{shard}/s058'`, `'{replica}'` succeeds (equals the
  expanded default).
- **Foreign explicit path rejected** — `ReplicatedMergeTree('/foreign/path/{shard}','{replica}')`
  throws `BAD_ARGUMENTS` "Setting ZooKeeper path … is not allowed".
- **Foreign replica name rejected** — `ReplicatedMergeTree('/clickhouse/tables/{shard}/s058','other_replica')`
  throws `BAD_ARGUMENTS` "Setting replica name … is not allowed".

Gate OFF (companion node), same DDL:

- **Neutrality** — the foreign-path `CREATE` is **accepted** (upstream does not
  guard standalone replicated tables), proving gate-OFF = upstream.

Fails-before/passes-after (AGENTS §7(a)): before the re-port the setting does not
exist, so even gate-ON accepts the foreign path on a standalone table → the
rejection assertions fail; after, they pass.

## 5. Rollback / production handover considerations

- **Critical handover — the behavior moves from code → config.** With 058 dropped,
  a stock 26.3 binary already rejects custom ZK path / replica name *by default*
  (setting default `0`), but the rejection is **bypassable per-session**. To
  restore 058's un-bypassable guarantee, pin the setting **non-overridable** in
  Aiven's managed config (it is already `0` by default, so a constraint suffices):

  ```xml
  <profiles>
      <default>
          <constraints>
              <database_replicated_allow_replicated_engine_arguments>
                  <readonly/>
              </database_replicated_allow_replicated_engine_arguments>
          </constraints>
      </default>
  </profiles>
  ```

  (Equivalently, a `min`/`max` constraint pinning it to `0`.) This gives 058's
  hard enforcement with zero patch surface and avoids the `CREATE … AS`
  regression risk of the expanded-comparison variant. **This drop is only safe
  paired with that config constraint** (and applied to every profile a customer
  query can run under, not only `default`).
- **Open assumption to verify (scope gap, §2 delta 2).** Upstream's guard only
  covers `ReplicatedMergeTree` **inside Replicated databases**. If Aiven ever
  allows standalone replicated tables, the config constraint above does **not**
  reproduce 058's broader coverage — in that case re-open the disposition and
  consider porting only the non-Replicated-DB scope behind an `aiven_` setting.
- Revert safety: `n/a` — no code carried by the drop.
- Future-uplift watch: if a later upstream removes or changes the default of
  `database_replicated_allow_replicated_engine_arguments`, the next uplift's drift
  check (the `Settings.cpp` grep in §2) must re-validate this analysis.

## 6. Per-uplift notes

### 25.8-aiven (historical)

Original carry: `9a2883592c` (author Tilman Moeller, co-authored by Dmitry
Potepalov, 2026-01-13). On the 25.8 base the enforcement was added as an
unconditional throw because the base lacked (or Aiven wanted a stronger form of)
the upstream `database_replicated_allow_replicated_engine_arguments` gate.

### 26.3-aiven (this uplift)

- Decision (human, 2026-06-17): drop the code; restore the un-bypassable
  guarantee in production via a `readonly` constraint on
  `database_replicated_allow_replicated_engine_arguments`. **SUPERSEDED.**
- Decision (human, 2026-06-30, `v26.3.15.4`): **reverse the drop — re-port 058.**
  Rationale: the config-only path is weaker (bypassable unless every profile is
  pinned; narrower scope — Replicated-DB only), and downstream tests assert 058's
  behavior on standalone replicated tables. Instruction was to "stick to the
  original patch" and only "gate [it behind] the new `aiven_` setting".
- Code: 058's helper + lambda guard reinstated **verbatim**, wrapped in the
  default-off server setting `aiven_enforce_default_replication_path`
  (`ServerSettings.cpp`). Gate-OFF = upstream-identical.
- Test added: `tests/integration/test_aiven_enforce_default_replication_path/`
  (see §4).
- Surprising bit: the original guard sits in the lambda shared by the
  default-args path, so it only avoids self-rejecting default creation when
  `default_replica_path` uses **non-special** macros — true in Aiven prod, false
  for the upstream `{uuid}` default. The gate being default-off is what keeps the
  upstream suite (which uses the `{uuid}` default) green.

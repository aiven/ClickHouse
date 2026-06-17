# Patch 058 — disallow-replication-parameter-customization

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `9a2883592c` | Tilman Moeller (author), co-authored by Dmitry Potepalov, 2026-01-13 | (the version we are porting FROM) |
| 26.3-aiven | `patch-drop(058)` | parent agent, 2026-06-17 | `obsoleted-by-upstream` — drop; replacement is **config**, see §2/§5 |

No code is carried by the drop; the reason is in §2 and the production handover in §5.

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

- Conclusion: **`obsoleted-by-upstream`** — drop. The only meaningful gap
  (bypassability) is closed by **configuration**, not code (§5). The upstream
  `DECLARE` default stays `0`, so there is no fork delta and the upstream test
  suite is undisturbed.

## 3. C++ review

`n/a — no code carried by the drop.` The relevant observation is the equivalence
in §2: upstream's `database_replicated_allow_replicated_engine_arguments == 0`
branch produces the same rejection 058's lambda guard did, for the
Replicated-database case that matters to Aiven.

## 4. Test design

(b)/(c) **No new test — the patch is DROPPED, not ported.**

- **Existing upstream coverage:** the `database_replicated_allow_replicated_engine_arguments`
  enforcement is upstream code exercised by the existing `Replicated`-database
  DDL suites.
- **Why no Aiven test is warranted:** there is no Aiven *code* change to test, so
  a fails-before/passes-after evidence pair (AGENTS §7(a)) is impossible by
  construction.
- **What a repo test cannot cover (deliberately):** the production hard-guarantee
  now lives in *config* (a `readonly` constraint pinning the setting at `0`), in
  Aiven's managed deployment layer outside this checkout.

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

- Cherry-pick: NOT performed. Parent stopped at Step 1 (drift/validity) with
  conclusion `obsoleted-by-upstream`.
- Decision (human, 2026-06-17): drop the code; restore the un-bypassable
  guarantee in the production environment via a `readonly` constraint on
  `database_replicated_allow_replicated_engine_arguments` (default already `0`).
  Recorded in `major-upstream-changes.md` (REPL-5). The non-Replicated-DB scope
  gap is flagged as an **open assumption to verify** (topology believed
  Replicated-DB-only).
- Test added: `n/a — no source change; see §4.`
- Surprising bit: the supersession is a default-value + bypassability question,
  not a like-for-like port — upstream provides the *same rejection by default*
  but as a soft (overridable) session setting, so the Aiven-specific value is
  precisely the *un-bypassability*, which is a config constraint rather than code.

# Patch 002 — enable-internal-replication-for-databasereplicated-clusters

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `fdc262dc9d` | Tilman Moeller (author) / Aliaksei Khatskevich (committer), co-authored by Kevin Michel, 2025-12-04 | (the version we are porting FROM) |
| 26.3-aiven | `patch-drop(002)` | parent agent, 2026-06-15 | `obsoleted-by-upstream` — drop; replacement is **config**, see §2/§5 |

The drop was committed as `patch-drop(002)` (no code carried; reason in §2).
Find it with `git log --grep '^patch-drop(002)'`.

## 1. Purpose

Per the source commit body, the intent was to make `Distributed` tables built
over a `DatabaseReplicated` cluster use **internal replication** — write to one
replica per shard and let `ReplicatedMergeTree` replicate asynchronously, rather
than writing to all replicas directly:

> Enable internal_replication=true for clusters created programmatically by
> DatabaseReplicated to improve performance and consistency of Distributed
> tables over Replicated databases.
> [...]
> This ensures that Distributed tables over Replicated databases write to one
> replica per shard, allowing ReplicatedMergeTree to handle replication
> asynchronously, instead of writing to all replicas directly.
> Benefits: Reduces network traffic (1× instead of N× writes per shard);
> Improves performance; Better consistency (uses ReplicatedMergeTree replication).

The change (`fdc262dc9d`, 3 files, +27) added an `internal_replication` field to
`ClusterConnectionParameters` (default `false`), set it `true` in
`DatabaseReplicated::getClusterImpl`, and threaded it through both `Cluster`
constructors into `addShard` (which builds the per-shard `insert_path` for
internal replication).

Source SHA on `v25.8.18.1-lts-aiven`: `fdc262dc9d`. Original author:
`tilman.moeller@aiven.io`.

## 2. Upstream-drift / validity findings

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`.

### Commands run

```bash
# Is the whole mechanism (field + getClusterImpl wiring + addShard) already in 26.3?
git grep -n 'internal_replication' src/Databases/DatabaseReplicated.cpp
#  → 101: extern const DatabaseReplicatedSettingsBool internal_replication;
#  → 512: return std::make_shared<Cluster>(..., db_settings[DatabaseReplicatedSetting::internal_replication]);
git grep -n 'internal_replication|_all_replicas|prefer_localhost_replica' src/Interpreters/Cluster.cpp
#  → 602/634: Cluster ctors take `bool internal_replication`
#  → 620/697: passed to addShard
#  → 645: insert_paths.compact = fmt::format("shard{}_all_replicas", current_shard_num);
#  → 671-676: if (internal_replication) concatInsertPath(prefer/no_prefer_localhost_replica, dir_name)

# Is internal_replication a per-DB setting upstream, and with what default?
rg -n 'DECLARE\(Bool, internal_replication' src/Databases/DatabaseReplicatedSettings.cpp
#  → default false ("...send data to one of replicas (internal replication...) or to all replicas...")

# How is the per-DB setting's value resolved (precedence)?
sed -n '2670,2673p' src/Databases/DatabaseReplicated.cpp
#  → initial = context->getDatabaseReplicatedSettings();  (server-config <database_replicated> defaults)
#  → DatabaseReplicatedSettings db_settings{initial};      (start from config defaults)
#  → if (engine_define->settings) db_settings.loadFromQuery(*engine_define);  (CREATE SETTINGS override)
rg -n 'loadFromConfig\("database_replicated"' src/Interpreters/Context.cpp
#  → 6392: db_replicated_settings.loadFromConfig("database_replicated", config);
```

Full logs under `tmp/patch-002/` (screening session).

### Findings

- **The entire C++ mechanism patch 002 introduced is already in upstream 26.3**,
  via a cleaner design. All three of 002's sites exist at HEAD:
  - `addShard` insert-path plumbing is **byte-for-byte present** (`Cluster.cpp:645`
    `shard{}_all_replicas` marker; `:671-676` the `concatInsertPath(...)` block).
  - both `Cluster` constructors already take a `bool internal_replication`
    parameter (`Cluster.h:87,314`) and thread it to `addShard` (`:620,:697`).
  - `getClusterImpl` already passes it (`DatabaseReplicated.cpp:512`).
- **Upstream made it a per-database setting, default `false`.**
  `DatabaseReplicatedSettings::internal_replication` (`DatabaseReplicatedSettings.cpp`)
  is the source of the value, threaded as an **explicit `Cluster` constructor
  argument** — not, as 002 did, a field hard-coded `true` on the
  `ClusterConnectionParameters` struct. So a verbatim cherry-pick would both
  **conflict** (duplicate `addShard` logic) and be **architecturally
  incompatible** (struct-field vs explicit-arg).
- **The value is settable via server config — durably, fleet-wide, with no
  code.** Precedence (`DatabaseReplicated.cpp:2670-2673`): compiled `DECLARE`
  default (`false`) → overridden by the server-config `<database_replicated>`
  section (loaded once via `Context::getDatabaseReplicatedSettings` →
  `loadFromConfig`, `Context.cpp:6392`) → overridden by the per-DB
  `CREATE … SETTINGS internal_replication=…`. Because `loadFromQuery` only
  applies settings explicitly present in the stored CREATE, a DB created on
  25.8-aiven (where the setting did not exist, so it is absent from the stored
  DDL) resolves `internal_replication` from the config default on attach — so a
  config default of `true` restores the prior behavior for **existing and new**
  DBs across **all** creation paths.

| Aspect | Patch 002 (Aiven, from 25.8) | Upstream 26.3 |
|---|---|---|
| Mechanism | `ClusterConnectionParameters.internal_replication` field, hard-coded `true` in `getClusterImpl` | Per-DB `DatabaseReplicatedSettings::internal_replication`, threaded as explicit `Cluster` ctor arg |
| Default | `true` (unconditional) | `false` |
| Configurable | No (hard-coded) | Yes — `CREATE … SETTINGS`, server-config `<database_replicated>`, or `ALTER DATABASE MODIFY SETTING` (in-memory only, see note) |
| `addShard` insert-path logic | Added by the patch | Already present (byte-identical) |

- Conclusion: **`obsoleted-by-upstream`** — drop. The replacement is **not** more
  code; it is **configuration**: set
  `<database_replicated><internal_replication>true</internal_replication></database_replicated>`
  in Aiven's managed server config (see §5). The upstream `DECLARE` default stays
  `false`, so there is **no fork delta** in the repo and the upstream test suite is
  undisturbed.

### The only reasons to NOT drop (considered, rejected)

- **Behavior parity with 25.8.** 25.8-aiven forced `internal_replication=true`;
  26.3's default is `false`, so doing nothing is a silent regression (N× writes,
  lost performance/consistency benefit). This is a real concern but it is solved
  by the **config** default, not by carrying code — so it argues for the config
  handover (§5), not for porting.
- **Non-disableable behavior.** The hard-code could not be turned off; the setting
  can. For a managed fleet this is not a loss (Aiven sets the config and does not
  expose it), and it is strictly better than 002 — same default behavior **plus**
  a per-DB override the hard-code never allowed.

## 3. C++ review

`n/a — no code carried by the drop.` The relevant observation is the equivalence
in §2: upstream's per-DB `internal_replication` setting, fed through the same
`addShard` insert-path machinery 002 added, produces the identical write-to-one
behavior — gated by a setting (with a server-config default) rather than
hard-coded.

## 4. Test design

(b)/(c) **No new test — the patch is DROPPED, not ported.**

- **Existing upstream coverage:** the `internal_replication` plumbing
  (`Cluster::addShard` insert-path construction, `ShardInfo::hasInternalReplication`,
  `insertPathForInternalReplication`) is upstream code exercised by the existing
  `Distributed`/`Replicated`-database suites.
- **Why no Aiven test is warranted:** there is no Aiven *code* change to test — we
  carry nothing, so a fails-before/passes-after evidence pair (AGENTS §7(a)) is
  impossible by construction.
- **What a repo test cannot cover (deliberately):** the production behavior now
  lives in *config* (`<database_replicated><internal_replication>true</…>`), in
  Aiven's managed deployment layer outside this checkout. That guarantee belongs
  to the deployment layer and the §5 handover, not to a test here.

## 5. Rollback considerations

- **Critical handover — the behavior moves from code → config.** With the patch
  dropped, the binary defaults `internal_replication=false` for
  `DatabaseReplicated` clusters. To retain the 25.8 behavior, Aiven's managed
  server config MUST set
  `<database_replicated><internal_replication>true</internal_replication></database_replicated>`.
  This applies durably to existing and new Replicated DBs (the config default wins
  whenever the stored CREATE did not pin the setting). **This drop is only safe
  paired with that config change.**
- **Safety coupling to patch 004 (load-bearing).** `internal_replication=true` is
  safe **only** when every table behind the Replicated-DB auto-cluster is
  `ReplicatedMergeTree`. A plain `MergeTree` (or other non-self-replicating engine)
  would receive a `Distributed` INSERT on one replica that is then **never**
  replicated → silent cross-replica divergence / effective data loss. That is
  exactly why upstream chose `false` as the general-safe default. The invariant
  that makes `true` safe is patch **004** (`Replace MergeTree with
  ReplicatedMergeTree in Replicated databases`, currently held for a clause-(v)
  decision). The config flip should be sequenced **with/after 004**, or gated on a
  positive confirmation that no Replicated DB in production can hold a
  non-Replicated table.
- **Do not rely on `ALTER DATABASE MODIFY SETTING` for this.** Patch 003's
  `applySettingsChanges` mutates only the **in-memory** `db_settings` of the local
  replica and does not persist to ZooKeeper or propagate — so an
  `ALTER DATABASE … MODIFY SETTING internal_replication=1` is non-durable and
  per-replica. Use the server-config default (or per-DB `CREATE … SETTINGS`)
  instead.
- Revert safety: `n/a` — no code carried by the drop.
- Future-uplift watch: if a later upstream ever removes
  `DatabaseReplicatedSettings::internal_replication` or changes its default, the
  next uplift's drift check (the `DatabaseReplicatedSettings.cpp` grep in §2) must
  re-validate this analysis.

## 6. Per-uplift notes

### 25.8-aiven (historical)

Original carry: `fdc262dc9d` (author Tilman Moeller, committer Aliaksei
Khatskevich, co-authored by Kevin Michel, 2025-12-04). On the 25.8 base there was
no per-DB `internal_replication` setting, so the patch hard-coded `true` on the
cluster built by `DatabaseReplicated::getClusterImpl`.

### 26.3-aiven (this uplift)

- Cherry-pick: NOT performed. Parent stopped at Step 1 (drift/validity) with
  conclusion `obsoleted-by-upstream`.
- Decision (human, 2026-06-15): drop the code; restore the behavior in the
  production environment via the `<database_replicated>` server-config default
  (`internal_replication=true`). Recorded in `major-upstream-changes.md` (REPL-1).
- Test added: `n/a — no source change; see §4.`
- Surprising bit: the supersession is not a like-for-like setting rename but a
  cleaner re-architecture (struct-field hard-code → per-DB setting fed as an
  explicit ctor arg, with a server-config default layer). The patch's intent
  survives entirely as a default-value choice; only the **invariant** that makes
  that default safe (patch 004, all-ReplicatedMergeTree) is the thing to track.
```

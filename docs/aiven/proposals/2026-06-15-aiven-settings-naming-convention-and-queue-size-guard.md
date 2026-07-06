# Proposal: `aiven_` setting-naming convention + a default-off server guard for the replication-queue-size limiter (patch 008)

> **Document type:** Proposal / design sketch (awaiting maintainer ratification).
> **Date:** 2026-06-15.
> **Status:** DRAFT — proposed. Two coupled decisions: (1) a fork-wide setting-naming convention (a new bootstrap rule), and (2) the concrete re-architecture of patch 008's enablement that is the convention's first application. Decision needed before patch 008 is dispatched.
> **Audience:** The maintainer who owns the Aiven fork's settings contract and the managed-service config surface; the engineer who would port patch 008.
> **One-sentence framing:** Introduce the rule that *newly* introduced Aiven settings carry an `aiven_` prefix, and apply it by carrying patch 008 **verbatim** (including its `queue_size_monitor` query setting) but wrapping the whole replication-queue-size monitor + insert-gating feature in a single default-**off** server setting `aiven_enable_replication_queue_size_limit` — so a stock build is byte-for-behavior identical to upstream, 008's own switches are untouched, and production opts in explicitly.

## 1. Goal & motivation

Two things land together here because the second is the motivating first instance of the first:

1. **A naming convention (bootstrap rule).** Aiven settings introduced on the fork are today indistinguishable from upstream settings by name (`enforce_https_for_url_storage`, `user_with_indirect_database_creation`, the Kafka knobs, the 25.8 queue thresholds all look upstream). Provenance matters: it drives which settings we must preserve across an LTS uplift, which appear in `system.settings`/`system.server_settings` as fork surface, and which a reviewer must scrutinise. The proposal: **new** Aiven-introduced settings carry an `aiven_` prefix. This is **forward-only** — settings we already shipped are grandfathered (renaming a shipped setting breaks stored DDL / the external contract; see the *settings backward-compatibility* theme in [`major-upstream-changes.md`](../uplifts/26.3/major-upstream-changes.md)).

2. **Patch 008, guarded (clause (v)).** Patch 008 ("Fix unbounded replication queue growth") ships a per-table background monitor thread plus insert delay/throw gating keyed on `ReplicatedMergeTree` queue size. In its 25.8 form the master enable is `queue_size_monitor` — a **query setting defaulting to `true`** — so a *faithful-as-is* port would change default behavior for **every** `ReplicatedMergeTree` table in the fleet (a new background thread per table + a new insert-rejection path). That is precisely the "default-behavior change with broad blast radius" that `AGENTS.md` §7 / clause (v) says must **not** ship on by default: the human decides whether to gate it behind a **new default-disabled server setting**. Resolution: carry patch 008 **verbatim** (its settings and switches untouched, "to keep behavior") and add a single default-**off** server setting as an **outer master guard** over the whole feature. Production runs the feature on, so we want default-off-but-opt-in, not drop.

**Decision requested:** approve the `aiven_` convention (§4) and the patch-008 guard design (§5), then dispatch the 008 port against the gated design.

## 2. Background — what patch 008 adds (per the 25.8-aiven source / preflight screening)

> File-level internals below are from the patch-008 preflight screening of the 25.8-aiven source; line anchors are to be confirmed during the port (008 is not yet in this tree — verified: no `queue_size_monitor` symbol in `src/`).

| Aspect | Detail |
|---|---|
| Master enable (25.8) | `queue_size_monitor` — **query** `Setting` (`Bool`), **default `true`** (`Core/Settings.cpp`) |
| Threshold settings (25.8) | `queue_size_to_delay_insert`, `queue_size_to_throw_insert`, `queues_total_size_to_delay_insert`, `queues_total_size_to_throw_insert` — declared **twice**: as query `Setting`s (`Core/Settings.cpp`) and as per-table `MergeTreeSetting`s (`Storages/MergeTree/MergeTreeSettings.cpp`) |
| Monitor thread | `ReplicatedMergeTreeQueueSizeThread` — new per-table `BackgroundSchedulePool` task that polls ZooKeeper for each replica's `queue/` size and publishes a max into the table |
| Gating sink | reuses `MergeTreeData::delayInsertOrThrowIfNeeded` — adds a branch comparing the published queue size against the thresholds and delaying / throwing the insert |
| Drift vs 26.3 | `BackgroundSchedulePool::createTask` gained a `StorageID` first arg (2-arg → 3-arg); logger idiom `Poco::Logger *` → `LoggerPtr`; expected conflicts in `MergeTreeData.cpp` |

**Why the feature exists.** An unbounded replication queue (replica falling behind, or fetch storms) grows the in-ZooKeeper `queue/` znode set without bound, which can OOM Keeper and destabilise the whole shard. The limiter back-pressures inserts (delay, then throw) before the queue runs away. It is a real safety mechanism Aiven runs in production — hence opt-in default-off, not drop.

## 3. Why a server-level, default-off guard (not the query setting)

- **Right layer.** Whether the fleet-wide back-pressure safety mechanism is active is a **deployment** decision, not a per-query one. A `ServerSetting` that "can only be set in the server configuration and cannot be overridden in a session" (the exact posture of `enforce_https_for_url_storage`, `ServerSettings.cpp:1522`) matches the intent: a tenant cannot turn the fleet's queue protection off (or on) from a `SETTINGS` clause.
- **Clause (v) compliance.** Default-off means a stock Aiven build behaves identically to upstream — no new thread, no new insert-rejection path for any existing table — so there is no broad-blast-radius default change and no need to "prove safety by running the whole suite." The gated behavior is tested with the setting explicitly on (§8).
- **Single outer switch.** One server setting gates both the thread start and the insert-gating branch — exactly one place to reason about "is this feature live at all" — while 008's own `queue_size_monitor` sub-switch is carried untouched underneath it.
- **Production keeps the feature.** Production keeps `queue_size_monitor = 1` exactly as today and *adds* `aiven_enable_replication_queue_size_limit = 1` in managed server config (§7) — additive, no migration of the existing setting.

## 4. The naming convention (proposed bootstrap rule)

**Rule (forward-only).** A setting that Aiven introduces and that does **not** exist upstream is named with the **`aiven_`** prefix.

- **Firm scope (as requested):** new **server** settings (`ServerSettings.cpp`). Example: `aiven_enable_replication_queue_size_limit` (§5).
- **Recommended extension (decision §10):** apply the same prefix to *any* new Aiven-introduced `Setting` / `MergeTreeSetting` too, for uniform provenance and a single `git grep '\baiven_'` audit. (The user's instruction named "server settings"; this extends it for consistency. If rejected, the firm rule still stands for server settings.)
- **Grandfathering (the "only newly introduced" clause):** settings Aiven already shipped under a non-prefixed name are **never renamed**. A shipped setting name is part of the external contract — a 25.x table stores its `SETTINGS` verbatim and re-parses them on `ATTACH`; an unknown name throws `UNKNOWN_SETTING` and the table fails to load. Grandfathered examples: `enforce_https_for_url_storage`, `user_with_indirect_database_creation`, the Kafka settings (029/031/033), and patch 008's four queue thresholds (introduced in 25.8 → keep their names). Only settings introduced **fresh on 26.3-aiven or later** take the prefix.
- **Why a prefix and not, say, a registry doc:** the name is the only provenance signal that survives into `system.settings`, `SHOW SETTINGS`, stored DDL, and a reviewer's `git grep`. A separate registry drifts; the prefix is self-documenting at every site.

**Rationale for "forward-only".** The prefix is a *labelling* convention, and a label that breaks stored DDL is worse than no label. Backward-compat (don't rename shipped settings) strictly dominates uniformity-of-prefix. Hence the convention can only bind settings that have no existing contract yet — i.e. brand-new ones. The 008 guard qualifies (it never existed); the 008 thresholds do not (they shipped in 25.8).

**Proposed home for the rule** (the actual edits are in §9, gated on ratification):
- `docs/aiven/AGENTS.md` — a new short invariant subsection (workers obey it when introducing a setting).
- `docs/aiven/runbooks/commit-hygiene.md` §3 — a one-line note in the bootstrap checklist so the convention is reaffirmed each LTS transition (and grandfathered names are explicitly *not* retro-prefixed at carry-forward).

## 5. Proposed design — `aiven_enable_replication_queue_size_limit`

### 5.1 The setting

`src/Core/ServerSettings.cpp` — one `DECLARE`, modelled on `enforce_https_for_url_storage`:

```cpp
DECLARE(Bool, aiven_enable_replication_queue_size_limit, false, R"(
Aiven: master switch for the replication-queue-size limiter. When enabled, each
`ReplicatedMergeTree` table runs a background thread that monitors replica
replication-queue sizes and delays or throws inserts once the configured
`queue_size_to_delay_insert` / `queue_size_to_throw_insert` /
`queues_total_size_to_delay_insert` / `queues_total_size_to_throw_insert`
thresholds are exceeded, to bound queue growth and protect ZooKeeper/Keeper.
Disabled by default; behaves exactly like upstream when off. This is a
server-level setting and cannot be overridden in a session.)", 0) \
```

### 5.2 The two gate points (both required), as an *outer* guard

The server setting is the **outer** gate; 008's own logic (including `queue_size_monitor`) runs **inside** it, unchanged.

1. **Thread start** — in `StorageReplicatedMergeTree::startupImpl`, wrap 008's existing `queue_size_monitor`-conditioned thread start in `if (getContext()->getServerSettings()[ServerSetting::aiven_enable_replication_queue_size_limit])`. Off ⇒ the thread never starts, no ZooKeeper polling, no per-table task — zero overhead, regardless of `queue_size_monitor`.

2. **Insert-gating branch** — in `MergeTreeData::delayInsertOrThrowIfNeeded`, short-circuit the new queue-size delay/throw block on the same server setting.

   **Why gate the branch too, not rely on inert inputs (the 0-footgun).** When the thread is off, the published queue size stays `0`. The comparisons are unsigned (`size_t queue_size >= threshold`). If a threshold ever resolves to `0` (its default, or an operator setting it to `0` *intending* "disabled"), then `0 >= 0` is **true** — the gate would throw on *every* insert with the feature nominally off. Gating the branch on the server setting makes "off" mean off unconditionally and removes the footgun. (Independently, document that `0` does not mean "disabled" for these thresholds; a sentinel like "0 = no limit" should be handled explicitly if desired, but that is a separate hardening.)

### 5.3 `queue_size_monitor` — carried verbatim, NOT retired

Carry `queue_size_monitor` exactly as patch 008 ships it: a live query `Setting` (`Bool`, default `true`), with its 008 semantics intact (it conditions the monitor thread *inside* the outer server guard). This keeps the port faithful and avoids any divergence:

- **No `MAKE_OBSOLETE`, no rename, no semantic change** — the smallest possible delta from the 008 source.
- **No stored-DDL / profile risk** — because the name remains a live, known setting, 25.8 profiles and any DDL that set `queue_size_monitor` load natively (no `UNKNOWN_SETTING`, no obsolete shim needed).
- **No config migration of the existing setting** — production keeps `queue_size_monitor = 1` and merely *adds* the new server flag.

The default-`true` of `queue_size_monitor` is harmless on a stock build because the **outer** server guard defaults `false`: the feature is off until an operator sets `aiven_enable_replication_queue_size_limit = true`, at which point 008 behaves exactly as it did in 25.8.

### 5.4 The four thresholds — grandfathered, carried as-is

`queue_size_to_delay_insert`, `queue_size_to_throw_insert`, `queues_total_size_to_delay_insert`, `queues_total_size_to_throw_insert` keep their 25.8 names (both the query `Setting` and the per-table `MergeTreeSetting` forms). They are the tuning knobs *under* the master switch; the switch decides whether they are consulted at all. They predate the convention ⇒ no `aiven_` prefix (grandfathered, §4).

### 5.5 Layering (the truth table)

| `aiven_enable_replication_queue_size_limit` (server, **outer**) | `queue_size_monitor` (query, 008) | Result |
|---|---|---|
| `false` (default) | `true` (008 default) or `false` | **upstream behavior**: no monitor thread, no queue-size gating — no change vs stock, regardless of `queue_size_monitor` |
| `true` (Aiven production) | `true` (production) | monitor thread runs; inserts delayed/thrown per thresholds (= the 25.8 feature, unchanged) |
| `true` | `false` | feature *enabled* at the fleet level but the per-008 monitor sub-switch off — i.e. exactly 008's `queue_size_monitor=false` behavior |

The default-`false` **outer** guard means a stock Aiven build is behavior-identical to upstream; the feature only bites when the operator sets the server setting on, at which point 008's own switches (`queue_size_monitor` + thresholds) govern exactly as in 25.8.

## 6. Invariants the design must protect

1. **No default-behavior change.** Outer guard off ⇒ a normal build runs no new thread and rejects no insert it didn't already, regardless of `queue_size_monitor` (clause (v) satisfied).
2. **Single outer switch.** One server setting gates both the thread and the gating branch — one place to audit "is the feature live"; 008's logic is untouched beneath it.
3. **Server-only.** Not session-overridable (like `enforce_https_for_url_storage`); a tenant cannot toggle fleet queue protection.
4. **Minimal-divergence / stored-DDL safety.** Patch 008 is carried verbatim — `queue_size_monitor` stays a live setting and the four thresholds keep their names — so legacy profiles/DDL load natively (no `UNKNOWN_SETTING`, no obsolete shim).
5. **Off means off (no 0-footgun).** The gating branch is guarded by the outer setting, so a `0` threshold cannot resurrect the feature when the guard is off.
6. **Exception, not crash.** The throw path surfaces as the existing insert-rejection exception (an exception, not a server crash, in the release build).

## 7. Backward-compat & integration

- **Production opt-in (required config change, not code):** keep the existing `queue_size_monitor = 1` profile entry and *add* the new server flag:
  ```xml
  <aiven_enable_replication_queue_size_limit>true</aiven_enable_replication_queue_size_limit>
  ```
  The four thresholds and `queue_size_monitor` are unchanged; this is purely additive.
- **Ledger.** This behavior change is recorded in [`major-upstream-changes.md`](../uplifts/26.3/major-upstream-changes.md) (entry REPL-2) tagged `⚠ operational`.

## 8. Verification plan

Default-off ⇒ no stateless-suite impact (the feature is invisible unless the server setting is on). The evidence-of-causation test runs the **gated** behavior with the setting explicitly enabled (clause (v): "Test the gated behavior with the setting explicitly enabled"):

1. **Off (default):** with `aiven_enable_replication_queue_size_limit = 0` (and `queue_size_monitor` at its 008 default `1`, to prove the outer guard dominates), no `ReplicatedMergeTreeQueueSizeThread` is started (assert via logs / `system.metrics` background-task count) and inserts at any queue size are accepted — i.e. unchanged from upstream.
2. **On:** with the server setting `= 1` and a small `queue_size_to_throw_insert`, construct a replica whose queue exceeds the threshold and assert the insert is rejected with the limiter's specific error (error_code **and** a message substring specific to the queue-size check, per `AGENTS.md` §7 — not a bare shared code), and that with the throw threshold high but delay threshold low the insert is *delayed* rather than thrown.
3. **Sub-switch still works:** with the server setting `= 1` but `queue_size_monitor = 0`, the thread does not start — confirming 008's own switch is preserved beneath the outer guard.

Because the monitor is genuinely cluster-level (per-replica ZooKeeper queue state), this is an **integration** test, not stateless — the one case where `AGENTS.md` §7(a)'s "integration only when genuinely cluster-level" applies.

## 9. Proposed rule text (apply on ratification)

**(a) `docs/aiven/AGENTS.md` — new invariant subsection** (insert after §7, renumber Navigation):

```md
## 8. Naming Aiven-introduced settings

When a port or net-new patch introduces a setting that does **not** exist
upstream, name it with the `aiven_` prefix (firm for `ServerSetting`s;
recommended for `Setting` / `MergeTreeSetting` too). This marks provenance and
keeps the fork's surface greppable (`git grep '\baiven_'`).

The rule is **forward-only**. Never rename a setting Aiven already shipped under
a non-prefixed name — a shipped name is stored-DDL/external contract (see the
settings backward-compatibility theme). Grandfathered (do NOT prefix):
`enforce_https_for_url_storage`, `user_with_indirect_database_creation`, the
Kafka settings, the 25.8 replication-queue thresholds, etc. Only settings
introduced fresh on this LTS-aiven line or later take the prefix.
```

**(b) `docs/aiven/runbooks/commit-hygiene.md` §3 — one line in the bootstrap checklist** (so it is reaffirmed each transition and carry-forward does not retro-prefix grandfathered names):

```md
- New Aiven-introduced settings follow the `aiven_` prefix convention
  (`AGENTS.md` §8). At carry-forward, do NOT retro-prefix already-shipped
  settings — the convention is forward-only.
```

## 10. Decision checklist

- [ ] Approve the `aiven_` **naming convention** as a bootstrap rule (§4), forward-only / grandfathered.
- [ ] Decide convention **scope**: server settings only (as requested) **or** extend to all new Aiven `Setting`/`MergeTreeSetting` (recommended, §4).
- [ ] Approve patch 008's **default-off server guard** `aiven_enable_replication_queue_size_limit` (§5) as the clause-(v) resolution (vs faithful default-on port, vs drop).
- [ ] Confirm the **two gate points** (thread start + gating branch) and the 0-footgun guard (§5.2).
- [x] **Decided (2026-06-15):** carry `queue_size_monitor` **verbatim** as an inner sub-switch — do NOT retire it (§5.3). The new flag is the sole *outer* guard.
- [ ] Confirm the guard **name** `aiven_enable_replication_queue_size_limit` (alts: `aiven_replication_queue_size_limit`, `aiven_enable_replicated_queue_monitor`).
- [ ] Approve applying the §9 rule text to `AGENTS.md` / `commit-hygiene.md` (or keep proposal-only until the first port lands).

Once decided, patch 008 is dispatched against this gated design (new server-setting decl + two outer gate points; 008 carried verbatim — `queue_size_monitor` and the four thresholds untouched; integration test with the setting on), and the §9 edits land as a bootstrap (category A) commit.

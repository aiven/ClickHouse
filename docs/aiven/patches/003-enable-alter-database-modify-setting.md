# Patch 003 — enable-alter-database-modify-setting

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | n/a (lineage not recorded in this dossier) | n/a | unknown |
| 25.8-aiven | `1055b0defeb848571ff1705e565cf94f9179a18f` | Tilman Moeller (author) / alex.khatskevich (committer) | original carry on `v25.8.18.1-lts-aiven` |
| 26.3-aiven | (staged) | T3.19 worker | conflict-resolved (one-anchor re-target in `DatabaseReplicated.cpp`) |

The current uplift's row stays "(staged)" until the human commits.

## 1. Purpose

Enable `ALTER DATABASE <db> MODIFY SETTING <name> = <value>` for the `Replicated`
database engine. Upstream's base `IDatabase::applySettingsChanges` throws
`NOT_IMPLEMENTED`, so `Replicated` databases could not have their settings
altered at runtime — the only way to change a setting was to drop and recreate
the entire database. The durable motivation is operational: rotating
`cluster_secret` (and adjusting tunables like `max_broken_tables_ratio`) without
a destructive recreate of the database. Changing `cluster_secret` also
invalidates the cached cluster so the new authentication credentials take effect.

Source SHA on `v25.8.18.1-lts-aiven`: `1055b0defeb848571ff1705e565cf94f9179a18f`.
Original author: `Tilman Moeller <tilman.moeller@aiven.io>` (author date 2025-12-04;
committed by `alex.khatskevich@aiven.io`).
Original purpose (verbatim from `git log --format=%B`):

```
Enable ALTER DATABASE MODIFY SETTING for Replicated databases

Add support for altering DatabaseReplicated settings at runtime without
requiring database recreation. This is particularly important for
rotating cluster_secret, which previously required dropping and
recreating the entire database.

Changes:
- Override applySettingsChanges() in DatabaseReplicated to handle
  MODIFY SETTING commands
- Add applyChange() and has() methods to DatabaseReplicatedSettings
  wrapper to expose BaseSettings functionality
- Invalidate cached cluster when cluster_secret is changed to ensure
  new authentication credentials are used

Implementation details:
- Thread-safe: protected by DatabaseReplicated::mutex
- Validates setting existence before applying changes
- Automatically resets cluster cache when cluster_secret changes
- Supports all DatabaseReplicatedSettings (max_broken_tables_ratio,
  max_replication_lag_to_enqueue, collection_name, etc.)

Usage:
    ALTER DATABASE  MODIFY SETTING cluster_secret='new_secret'

Co-authored-by: Kevin Michel <kevin.michel@aiven.io>
```

> Note: the T3.19 dispatch prompt asserted the source commit has a one-line
> message with no body. That is inaccurate — the source commit carries the body
> quoted above plus a `Co-authored-by: Kevin Michel <kevin.michel@aiven.io>`
> trailer. Surfaced to the human in the halt-and-escalate report so the
> committed message can be enriched if desired.

## 2. Upstream-drift findings

### Commands run

```bash
git log --oneline v25.8.18.1-lts..v26.3.10.62-lts -- \
  src/Databases/DatabaseReplicated.cpp src/Databases/DatabaseReplicated.h \
  src/Databases/DatabaseReplicatedSettings.cpp src/Databases/DatabaseReplicatedSettings.h
# → 141 commits (see tmp/patch-003/drift-upstream-touched.log)
git log --oneline v25.8.18.1-lts..v26.3.10.62-lts --grep 'applySettingsChanges'
# → only a9147cbb44b "Clamp settings constraints in DDL worker ..." (DDL worker, not a Replicated override)
rg 'applySettingsChanges' src/Databases/DatabaseReplicated.{cpp,h}   # → 0 hits at HEAD
```

### Findings (re-verified independently — parent findings 1–5)

- Upstream changes to touched files between prior and current LTS:
  - `DatabaseReplicated.cpp`: heavy churn (141 commits across the 4 files), incl.
    the addition of `getStatus` + `Coordination::setCurrentComponent` calls and
    "Hold `metadata_mutex` around both ZK ops" — this is exactly what moved the
    2-arg `getFullReplicaName` insertion anchor.
  - `DatabaseReplicated.h` / `DatabaseReplicatedSettings.{cpp,h}`: pure
    index-hash drift; the surrounding declarations the patch appends to are intact.
- Upstream changes that touched the patch's behavior (symbols, error codes, callers):
  - `DatabaseReplicated::applySettingsChanges`: **absent at HEAD** (Finding 1, `rg`
    → 0 hits). Upstream did NOT add an equivalent override → not obsoleted.
  - base `IDatabase::applySettingsChanges(const SettingsChanges &, ContextPtr)`:
    declared `IDatabase.h:441`, base impl throws `NOT_IMPLEMENTED` at
    `IDatabase.cpp:197` (Finding 3) — the differential observable still holds.
  - members present at HEAD (Finding 3): `DatabaseReplicatedSettings db_settings`
    (`DatabaseReplicated.h:254`, non-const), `mutable ClusterPtr cluster`
    (`:281`), inherited `mutable std::mutex mutex` (`IDatabase.h:466`),
    `ErrorCodes::BAD_ARGUMENTS` available (`DatabaseReplicated.cpp:108`).
  - `DatabaseReplicatedSettings::impl` is a `BaseSettings` exposing
    `applyChange`/`has` — the wrapper the patch adds delegates to them.
- Anchor re-target (Finding 2): 2-arg
  `getFullReplicaName(const String &, const String &)` moved 207 → 240 on HEAD;
  the constructor now occupies the old `:207` region; the no-arg overload is at
  `:297`. `git cherry-pick` conflicted at exactly this one anchor (Outcome B);
  the other 3 files auto-merged.
- Style (Finding 5): source already uses Allman braces and angle-bracket include
  (`<Common/SettingsChanges.h>`); no restyle performed.
- Conclusion: **`still-needed-but-rewrite`** — semantics byte-identical to the
  source (decomposition diff empty, see §6), but the cherry-pick could not apply
  cleanly due to the one-anchor `getFullReplicaName` move; manual conflict
  resolution placed `applySettingsChanges` immediately after the 2-arg
  `getFullReplicaName` definition (byte-identical to its source position).

## 3. C++ review

Apply `docs/aiven/skills/cpp-review-checklist.md`. One bullet per section.

- 1 Lifetime + ownership: `✓` — no raw pointers introduced; `cluster.reset()`
  acts on the existing `mutable ClusterPtr cluster` member (a `shared_ptr`),
  matching surrounding conventions. `ContextPtr query_context` is unused (matches
  the source; the base virtual takes it but `Replicated` does not need it).
- 2 Exception safety: `✓` — `db_settings.has(change.name)` is checked **before**
  `applyChange`, so an unknown setting throws `BAD_ARGUMENTS` without mutating
  state. The loop applies changes one at a time; a throw mid-loop leaves the
  already-applied earlier changes in place. This is acceptable and matches the
  in-memory, per-replica semantics noted below (no transactional rollback is
  promised by `ALTER ... MODIFY SETTING`).
- 3 Thread-safety + concurrency: `✓` — the whole body runs under
  `std::lock_guard lock{mutex}` (the inherited `DatabaseWithOwnTablesBase` mutex,
  `IDatabase.h:466`), which is the same mutex guarding `db_settings`/`cluster`
  access elsewhere. No new lock taken while holding a `Context` mutex. No sleeps.
- 4 Performance + memory: `n/a` — this is a control/DDL path (`ALTER DATABASE
  MODIFY SETTING`), not a per-row hot path; performance is irrelevant.
- 5 Settings as public API: `✓` — the patch does not add a new server/query
  setting; it operates on `DatabaseReplicatedSettings` (the engine's own settings
  registry). Validation goes through `BaseSettings::has`, so any valid
  `DatabaseReplicatedSettings` name (e.g. `max_broken_tables_ratio`,
  `cluster_secret`, `collection_name`) is accepted and unknown names rejected.
- 6 Error handling: `✓` — `ErrorCodes::BAD_ARGUMENTS` is real
  (`DatabaseReplicated.cpp:108`). The message `"Database engine {} does not
  support setting `{}`"` (with `getEngineName()` → "Replicated") is distinctive
  enough to distinguish the Aiven gate from the base `NOT_IMPLEMENTED` throw
  (different code AND different message), satisfying AGENTS.md §7.
- 7 Upstream / vendored code: `✓` — touches only `src/Databases/*`; no
  `contrib/**`, `.claude/**`, `.github/workflows/**`, or root `AGENTS.md`.
- 8 Behavior under settings: `n/a` — the change is not gated behind a feature
  setting; it unconditionally enables `MODIFY SETTING` for the `Replicated`
  engine. With no `ALTER ... MODIFY SETTING` issued, behavior is unchanged
  (the override is only reached on that DDL).

**Scope boundary (policy call 2 — record, do NOT "fix").**
`applySettingsChanges` mutates the **in-memory** `db_settings` of the **local
replica only**; it does NOT re-write the setting into ZooKeeper. Whether the
`ALTER DATABASE MODIFY SETTING` query itself propagates to other replicas is
governed by `shouldReplicateQuery` / DDL-log routing, which is out of scope for
this patch. Do not assume "every replica applies it". This is a known scope
boundary, not a defect.

## 4. Test design

(a) **New test that fails on the parent commit and passes after the patch.**

- Test path: `tests/queries/0_stateless/9003_enable_alter_database_modify_setting.sql`
  (+ `.reference`), per the Aiven `9<NNN>_<slug>` convention
  (`docs/aiven/runbooks/testing-suites.md` §4.1). Pattern reference:
  `9010_default_logs_to_keep.sql` (creates a `Replicated` DB, `no-parallel`,
  unique ZK path via `currentDatabase()`).
- Pre-patch run output (the FAIL — `tmp/patch-003/test-prepatch.log`):

  ```text
  9003_enable_alter_database_modify_setting:                              [ FAIL ] 0.13 sec.
  Reason: return code:  48
  Received exception from server (version 26.3.10):
  Code: 48. DB::Exception: Received from 127.0.0.1:9000. DB::Exception: Database engine Replicated
  either does not support settings, or does not support altering settings. (NOT_IMPLEMENTED)
  (query: ... ALTER DATABASE replicated_003_modify_setting MODIFY SETTING max_broken_tables_ratio = 0.5;)
  Having 1 errors! 0 tests passed.
  ```

- Post-patch run output (the PASS — `tmp/patch-003/test-postpatch.log`):

  ```text
  9003_enable_alter_database_modify_setting:                              [ OK ] 0.13 sec.
  1 tests passed. 0 tests skipped.
  ```

- Why this test distinguishes the Aiven gate from upstream behavior (AGENTS.md §7):
  the load-bearing observable is line-1 SUCCESS vs `NOT_IMPLEMENTED`. Pre-patch
  the base `IDatabase::applySettingsChanges` throws `NOT_IMPLEMENTED` (an
  un-annotated error → the test FAILS); post-patch the override applies the known
  setting (prints `known-setting-applied`) and rejects the unknown setting with
  `BAD_ARGUMENTS` (a different code AND message than the base throw). The test
  thus pins to the Aiven override, not merely to a shared error code.

## 5. Rollback considerations

- Reverting in production is safe: no schema migration, no on-disk format change.
  Reverting only re-disables `ALTER DATABASE MODIFY SETTING` for `Replicated`
  (it reverts to `NOT_IMPLEMENTED`). Any setting already changed at runtime via
  this path lives in the in-memory `db_settings` of each replica that applied it.
- State surviving a restart: none introduced by this patch. The change is
  in-memory only and is NOT persisted to ZooKeeper, so a server restart drops a
  runtime-applied setting unless it was also persisted by another mechanism
  (e.g. recreating the database with the new setting in its DDL). See the §3
  scope boundary.
- Disable without rebuilding: there is no setting to toggle; the behavior is
  unconditional on the `Replicated` engine. To disable, revert the patch.

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — lineage prior to 25.8 not recorded in this dossier (dossier born at the
26.3 dispatch).

### 25.8-aiven (historical, may be empty)

Original carry: `1055b0defeb848571ff1705e565cf94f9179a18f` on
`v25.8.18.1-lts-aiven` (author Tilman Moeller, committer alex.khatskevich,
2025-12-04). Co-authored-by Kevin Michel.

### 26.3-aiven (this uplift)

- Cherry-pick was: **conflict-resolved**. `git cherry-pick --no-commit -x` hit a
  single content conflict in `DatabaseReplicated.cpp` at the `getFullReplicaName`
  anchor (the 2-arg overload moved 207 → 240 and a new `getStatus` now sits
  between it and the no-arg overload). Resolved by placing `applySettingsChanges`
  immediately after the 2-arg `getFullReplicaName` definition (byte-identical to
  its source position), keeping `getStatus` intact. The other 3 files auto-merged.
- Upstream-drift conclusion: `still-needed-but-rewrite` (from §2).
- Patch-id: source `3a92a27fd1a0c1f09138844ec38f826dcebbed82`, staged
  `45f8242588be2703377f21cfa327c7d43df69825` — differ, so `byte_equivalent:
  false`. Tier-2 decomposition (`tmp/patch-003/decomposition.log`) is **empty**:
  the added/removed lines are byte-identical to the source; only context/anchor
  lines shifted. No semantic conflict.
- Test added at: `tests/queries/0_stateless/9003_enable_alter_database_modify_setting.{sql,reference}`.
  Evidence pair: `tmp/patch-003/test-postpatch.log` (PASS),
  `tmp/patch-003/test-prepatch.log` (FAIL, line-1 `NOT_IMPLEMENTED`).
- Time-to-port: warm-cache build dir (existing `clickhouse` binary, hot sccache).
  Three `ninja -C build clickhouse` invocations (post-patch full incremental +
  two worktree-flip rebuilds): 19 / ~19 / ~19 steps, ~51s / ~38s / ~30s each.
- Anything surprising: (1) the test needs Keeper — ran against a scratch
  Keeper-enabled server (`keeper_port.xml` + `zookeeper.xml` + absolute-path
  `config.d` drop-in + granted `default` user), NOT `--no-zookeeper`; the
  pre-patch run required restarting the server onto the rebuilt pre-patch binary.
  (2) The source commit message has a real body + `Co-authored-by` trailer,
  contrary to the dispatch prompt's "one-line message" claim (see §1 note).

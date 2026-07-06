# Patch 010 — default-logs-to-keep

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | (unknown — to be researched at 25.X dossier merge) | — | — |
| 25.8-aiven | `199db087991c02d215aafa6c6274200d507e31a9` | Tilman Moeller (author), Aliaksei Khatskevich (committer) | the version we are porting FROM |
| 26.3-aiven | `patch-port(010)` | T3.7 worker | conflict-resolved (context-line shift); `byte_equivalent: false` per Tier 2 decomposition (only `+`/`-` lines preserved, surrounding context drifted) |

## 1. Purpose

Reduce the default value of the `logs_to_keep` setting from `1000` to `300` for
`DatabaseReplicated` databases. The "why" is a ZooKeeper-resource-consumption
reduction: the queue-history retention is the dominant ZK memory footprint for
each Replicated database in multi-tenant managed environments. Cutting the
default to `300` reduces ZK memory use by ~70% per database while still keeping
a 6× safety margin over `max_replication_lag_to_enqueue` (which defaults to
`50` in the same settings struct). Customers cannot override this on a managed
fleet because they do not have `ALTER_DATABASE_SETTINGS` privilege; setting a
better default is therefore the right knob.

Source SHA on `v25.8.18.1-lts-aiven`: `199db087991c02d215aafa6c6274200d507e31a9`
(from `docs/aiven/uplifts/26.3/inventory.md` row 010).
Original author: Tilman Moeller <tilman.moeller@aiven.io>, 2025-12-08.
Original purpose (verbatim from `git log --format=%B`):

```
Change default logs_to_keep from 1000 to 300 for DatabaseReplicated

Reduce the default value of  setting from 1000 to 300 for
DatabaseReplicated databases. This reduces ZooKeeper resource consumption
by ~70% while maintaining a 6x safety margin over max_replication_lag_to_enqueue (50).

Context:
Previously, we implemented a server-level setting (replicated_database_logs_to_keep)
to centralize control of this value. However, after analysis, we determined that:
1. Customers do not have ALTER_DATABASE_SETTINGS permission, so they cannot
   modify database settings via ALTER DATABASE MODIFY SETTING
2. The simpler approach of changing the database-level default is sufficient
3. No additional readonly checks are needed since access control already
   prevents customer modifications

This change affects only newly created databases. Existing databases retain
their current logs_to_keep value stored in ZooKeeper.

The default value of 300 provides adequate recovery buffer while significantly
reducing ZooKeeper memory usage in multi-database managed provider environments.

Co-authored-by: Khatskevich <khatskevich@aiven.io>
```

## 2. Upstream-drift findings

### Commands run

```bash
# Identifier inventory (6 identifiers; all PRESENT on HEAD).
for id in logs_to_keep NonZeroUInt64 DatabaseReplicatedSettings \
          LIST_OF_DATABASE_REPLICATED_SETTINGS DECLARE_SETTINGS_TRAITS \
          max_replication_lag_to_enqueue; do
  git grep -c -- "$id" -- 'src/Databases/' 'src/Storages/' 'src/Core/'
done

# File-level history between LTSes.
git log v25.8.18.1-lts..v26.3.10.62-lts -- src/Databases/DatabaseReplicatedSettings.cpp --oneline

# Targeted history search for the setting name.
git log v25.8.18.1-lts..v26.3.10.62-lts --grep 'logs_to_keep' --oneline

# Hunk-line verification.
sed -n '20,30p' src/Databases/DatabaseReplicatedSettings.cpp
```

### Findings

- All 6 identifiers (`logs_to_keep`, `NonZeroUInt64`, `DatabaseReplicatedSettings`,
  `LIST_OF_DATABASE_REPLICATED_SETTINGS`, `DECLARE_SETTINGS_TRAITS`,
  `max_replication_lag_to_enqueue`) are present on HEAD with non-zero counts;
  evidence in `tmp/patch-010/drift-identifiers.log`.
- 5 commits touched `src/Databases/DatabaseReplicatedSettings.cpp` between
  `v25.8.18.1-lts` and `v26.3.10.62-lts` (`tmp/patch-010/drift-file-history.log`):
  `51f1c81cab0` (doc fix), `4eeadb9c352` (add `internal_replication`),
  `f4aa968b13a` (intrusive-ptr AST refactor), `956465b4691` (add
  `default_replica_path` / `default_replica_shard_name` / `default_replica_name`),
  `6f9c0740002` (`UInt32` → `NonZeroUInt64` type change for `logs_to_keep`).
  None of them changed the **default value** `1000`. Spot-checked
  `6f9c0740002` to confirm the diff was a type token change with the literal
  `1000` preserved.
- `git log --grep 'logs_to_keep'` (`tmp/patch-010/drift-grep-history.log`)
  yields only `57012e420f9` (merge of the type-change PR) and `6f9c0740002`
  (the type change itself). No upstream commit moved the default.
- Hunk-context check (`tmp/patch-010/drift-hunk-context.log`): the pre-image
  line `DECLARE(NonZeroUInt64, logs_to_keep, 1000, "Default number of logs to keep in ZooKeeper for Replicated database.", 0) \`
  is present verbatim on HEAD at line 25.

- **Conclusion: `still-needed-and-applies`** (semantically). However, the
  cherry-pick produced a textual conflict at Tier 1 because two of the
  upstream commits (`956465b4691` adding the three `default_replica_*`
  settings, and `4eeadb9c352` adding `internal_replication`) inserted four
  unrelated `DECLARE` lines BETWEEN the patched `logs_to_keep` line and the
  source diff's lower context line (`DECLARE_SETTINGS_TRAITS(...)`). The
  source diff has only two context lines below the change (a blank and the
  `DECLARE_SETTINGS_TRAITS` line); on HEAD those two lines are pushed four
  lines further down, so git's 3-way merge could not match the pre-image
  block verbatim and emitted conflict markers. This is the canonical
  context-line drift case described in
  `docs/aiven/schema/halt-and-escalate.md` "Patch-id decomposition runbook".

- **Resolution**: keep all of HEAD's structure intact (the four upstream-added
  `DECLARE` lines below `logs_to_keep` stay verbatim) and change only the
  literal `1000` → `300` token on the `logs_to_keep` line. This is the only
  change the source patch makes anywhere in the tree.
- **Verification**: ran the schema's Tier-2 decomposition runbook — diff'ed
  the `+`/`-` lines (excluding `+++`/`---` headers) of the source patch
  against the staged diff. Output empty
  (`tmp/patch-010/patch-id-decomposition.log`). The added/removed lines are
  byte-identical between source and staged; only context lines differ. Per
  the runbook this is **Tier 2 GREEN with `byte_equivalent: false`**.
- Patch-ids: source `6f11b897bbe68fbf6375935e847ec1100396fe22`, staged
  `3b6a95beb3f6f312548a05b0d2f5f3cf8e305eb7` (different, as expected — context
  lines are part of the patch-id input).

## 3. C++ review

- 1 Lifetime + ownership: `n/a — patch changes a literal default value in an X-macro DECLARE; no lifetimes involved.`
- 2 Exception safety: `n/a — single-token change in a constant table; no new throw paths.`
- 3 Thread-safety + concurrency: `n/a — default is read at database-creation time on whichever thread issues CREATE DATABASE.`
- 4 Performance + memory: `✓ — the entire patch is a memory-reduction optimization (300 vs 1000 = ~70% less ZK memory per Replicated DB queue history). The C++ side is cheaper too: smaller default → fewer cleanup-thread iterations on average.`
- 5 Settings as public API: `note — the setting itself is unchanged in name/type/visibility (NonZeroUInt64, named "logs_to_keep"); only its default value changes. Per the source commit body this is intentional: customers without ALTER_DATABASE_SETTINGS still cannot override it, and customers with the privilege still can via ALTER DATABASE MODIFY SETTING.`
- 6 Error handling: `n/a — patch does not introduce error paths.`
- 7 Upstream / vendored code: `✓ — file is upstream-owned; patch is an Aiven default-policy tuning, documented in the commit body. No contrib/ touch.`
- 8 Behavior under settings: `note — the change affects only newly-created Replicated databases. Existing databases retain their ZK-persisted value indefinitely (the source commit body calls this out explicitly). Documented as a known characteristic; the test asserts only the new-DB path.`
- 9 Parent preflight discipline: `✓ — parent's prior was "still-needed-and-applies" with cherry_pick_clean=no (correctly flagged by the classifier). Worker verified the semantic conclusion holds; the textual conflict was a benign context-line shift (4 upstream-added DECLARE lines below logs_to_keep), resolvable mechanically and validated by the Tier-2 decomposition runbook (empty diff between source-patch +/- lines and staged +/- lines). Parent's identifier inventory and history-search prior were both confirmed.`

## 4. Test design

(a) **New stateless test that fails on the parent commit and passes after the patch.**

- Test path: `tests/queries/0_stateless/9010_default_logs_to_keep.sql` plus
  `tests/queries/0_stateless/9010_default_logs_to_keep.reference` per the
  Aiven `9<NNN>_<slug>` convention (`docs/aiven/runbooks/testing-suites.md` §4.1).
  `add-test` was deliberately NOT used.
- Test body creates a Replicated database without an explicit `logs_to_keep`
  setting and reads the persisted value back from ZooKeeper:

  ```sql
  CREATE DATABASE replicated_010_logs_to_keep
    ENGINE = Replicated('/test/aiven_010/' || currentDatabase() || '/replicated_010_logs_to_keep', 'shard_1', 'replica_1');
  SELECT value FROM system.zookeeper
  WHERE path = '/test/aiven_010/' || currentDatabase() || '/replicated_010_logs_to_keep'
    AND name = 'logs_to_keep';
  ```

  Reference is exactly `300\n` (verified by `od -c`). Tagged `no-parallel`
  (Replicated DB creation interacts with cluster state; same reason as the
  reference test `02710_default_replicated_parameters.sql`).

- Pre-patch run output (the FAIL — `tmp/patch-010/test-prepatch.log`):

  ```text
  9010_default_logs_to_keep:                                              [ FAIL ] 0.13 sec.
  Reason: result differs with reference:
  --- 9010_default_logs_to_keep.reference   2026-05-26 17:14:18.934911584 +0200
  +++ 9010_default_logs_to_keep.stdout      2026-05-26 17:17:31.456994509 +0200
  @@ -1 +1 @@
  -300
  +1000

  Database: test_3ujixnm7
  Having 1 errors! 0 tests passed.
  ```

- Post-patch run output (the PASS — `tmp/patch-010/test-postpatch.log`):

  ```text
  Connected to server 26.3.10.1 @ f03db74f48ee9568b59a930189e297d5007c88f2 v26.3.10.62-lts-aiven-dev
  9010_default_logs_to_keep:                                              [ OK ] 0.13 sec.
  1 tests passed. 0 tests skipped. 0.17 s elapsed (MainProcess).
  ```

- Why this test distinguishes the Aiven gate from upstream behavior
  (per `docs/aiven/AGENTS.md` §7): upstream's `logs_to_keep` default has been
  `1000` since the setting was introduced and continues to be `1000` on
  vanilla 26.3 (verified by the pre-patch run on the ported 26.3 base). The
  test asserts the EXACT value `300` (not just "not 1000"), which is uniquely
  produced by THIS patch's `+300` line. Reading from `system.zookeeper`
  inspects the persisted node written at CREATE time, so client-side setting
  overrides cannot accidentally produce `300` — the value comes from the
  C++ macro default.

## 5. Rollback considerations

- **Revert is safe.** The change is a single literal token in an X-macro
  DECLARE; no schema migration, no on-disk format change. Reverting `300` →
  `1000` is mechanically equivalent to the inverse of this patch.
- **Persisted state.** Newly-created databases use whatever default is
  current at the CREATE time and persist that value to ZooKeeper at
  `<zookeeper_path>/logs_to_keep` (see `src/Databases/DatabaseReplicated.cpp`
  near the line `zkutil::makeCreateRequest(zookeeper_path + "/logs_to_keep", ...)`).
  Existing databases retain their ZK-persisted value indefinitely, regardless
  of source-tree default. So flipping the default in either direction does
  not retroactively touch any database.
- **Per-database override without rebuild.** An operator with the
  `ALTER_DATABASE_SETTINGS` privilege can run
  `ALTER DATABASE <db> MODIFY SETTING logs_to_keep = <new_value>` at runtime.

## 6. Per-uplift notes

### 25.3-aiven (historical)

n/a — this patch was authored on 25.8-aiven; no 25.3 carry exists.

### 25.8-aiven (historical)

This is the LTS that introduced the patch (commit
`199db087991c02d215aafa6c6274200d507e31a9` on `v25.8.18.1-lts-aiven`).
Author: Tilman Moeller. Committer on the source branch: Aliaksei Khatskevich.
Co-author trailer: `Co-authored-by: Khatskevich <khatskevich@aiven.io>`.

### 26.3-aiven (this uplift)

- Cherry-pick was: **conflict-resolved (context-line shift only)**. The
  `git cherry-pick --no-commit -x 199db087991c...` command exited with a
  3-way-merge conflict because four upstream-added DECLARE lines sit between
  the patched `logs_to_keep` line and the source diff's lower context line
  (`DECLARE_SETTINGS_TRAITS`). The resolution was mechanical: keep all of
  HEAD's structure, change only the literal `1000` → `300` token on the
  `logs_to_keep` line. Validated via the schema's Tier-2 decomposition runbook
  (`tmp/patch-010/patch-id-decomposition.log`, empty output).
- Upstream-drift conclusion: `still-needed-and-applies` (semantically clean;
  textual conflict was pure context drift, not a semantic drift).
- Test added at: `tests/queries/0_stateless/9010_default_logs_to_keep.sql` +
  `.reference` (Aiven `9<NNN>_<slug>` convention, no `add-test`).
- Time-to-port (subagent wall-clock): ~10 minutes (warm-cache build dir from
  prior dispatches). Two incremental builds (~47s + ~34s + ~34s for the
  restore = ~2 minutes total compile time); cherry-pick + drift analysis +
  tests are essentially instant.
- Anything surprising: T3.7 was the FIRST T3.X dispatch where a "trivial"
  source change (1 file, 1 line) produced a Tier-1 textual conflict. The
  parent's preflight had concluded "Hunk should apply cleanly" based on
  "no upstream commit changes the default value" — but ignored that two
  upstream commits inserted four unrelated DECLARE lines BELOW the patched
  line, and the source diff's context anchor (`DECLARE_SETTINGS_TRAITS`)
  was therefore four lines further away on HEAD than on the source. This
  is a useful preflight refinement to feed back: "no semantic change to
  the patched line itself" is necessary but not sufficient for cherry-pick
  cleanliness; "no inserted/deleted lines in the source diff's context
  window" must also be checked. The `cherry_pick_clean=no` field on the
  inventory row was correct; the parent's prose interpretation of it
  ("Hunk should apply cleanly") was the slip.

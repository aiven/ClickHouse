# Patch 053 — check-mergetree-settings-constraints-before-ddl

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | n/a | n/a | not in scope for this uplift's records (no earlier carry identified) |
| 25.8-aiven | `bb048483602c9e180fc2446d29e4099cf3b24fca` | Tilman Moeller (author) / Joe Lynch (committer) | original carry |
| 26.3-aiven | `patch-port(053)` (staged) | T3.19 worker | conflict-resolved (include-context drift); patch-id NOT byte-equal, +/- content identical |

The current uplift's row stays the stable handle `patch-port(053)` until the human commits.

## 1. Purpose

Constraints on MergeTree settings (defined in a settings profile, e.g.
`merge_tree_max_suspicious_broken_parts MAX 5`) were previously only enforced
when the `DDLWorker` executed a queued DDL entry — and at that point the worker
runs under the **system** profile, which carries no per-user constraints. A
non-admin user (e.g. the default profile) could therefore submit a CREATE/ALTER
to a `DatabaseReplicated` and set a MergeTree setting their own profile forbids:
the user-context enqueue did not check the constraint, and the system-context
execution had nothing to check against. This is a privilege bypass.

The patch closes the hole by checking `checkMergeTreeSettingsConstraints` **early
— at enqueue, while the user's query context (and thus their profile
constraints) is still attached** — in `checkTableEngine` (CREATE) and
`checkQueryValid` (ALTER … MODIFY SETTING). The check runs before the engine is
rewritten to a `Replicated*MergeTree`, so it deliberately matches both plain
`MergeTree` and `Replicated*MergeTree` engine names (`endsWith(name, "MergeTree")`).
The "why" — enforce user-profile constraints at the only point where the user's
identity is known — is durable; the exact lines may move every uplift.

Source SHA on `v25.8.18.1-lts-aiven`: `bb048483602c9e180fc2446d29e4099cf3b24fca`.
Original author: `tilman.moeller@aiven.io` (committed by `joelynch112@gmail.com`).
Original purpose (quoted, not paraphrased):

```
Check MergeTree settings constraints before enqueuing DDL queries

The constraints were previously only checked when executing the query from
the replicated DDL queue. At that point the DDLWorker was using the system
profile for settings constraints. This meant that a non-admin user, using
the default profile, could use a MergeTree settings that was not allowed
by its profile.

Fix that by checking the MergeTree settings constraints before the query
is enqueued, when we still have the query context attached to the user
running the query. We check both Replicated*MergeTree and apparently
non-replicated MergeTree because this check happens before we rewrite the
query to enforce Replicated*MergeTree engine types.

This commit fixes a security vulnerability where users could bypass
profile-based settings restrictions by submitting DDL queries to
DatabaseReplicated. ...

Co-authored-by: Kevin Michel <kevin.michel@aiven.io>
Co-authored-by: Aliaksei Khatskevich <alex.khatskevich@aiven.io>
```

## 2. Upstream-drift findings

### Commands run

```bash
git log --oneline v25.8.18.1-lts..v26.3.10.62-lts -- src/Databases/DatabaseReplicated.cpp
git log --oneline -S 'checkMergeTreeSettingsConstraints' v25.8.18.1-lts..v26.3.10.62-lts -- src/Databases/DatabaseReplicated.cpp
# identifier inventory on HEAD (src/):
for id in checkMergeTreeSettingsConstraints getReplicatedMergeTreeSettings \
          checkTableEngine checkQueryValid ASTSetQuery settings_changes endsWith; do
  printf '%s: ' "$id"; git grep -c "$id" -- src/ | awk -F: '{s+=$NF} END{print s+0}'; done
```

### Findings

- Upstream changes to touched files between prior and current LTS:
  - `src/Databases/DatabaseReplicated.cpp`: churn (file grew; the two patched
    blocks shifted from ~1074/1277 in 25.8 to ~1155/1355 on HEAD). No change to
    the *semantics* of `checkTableEngine` (the `replicated_table = …(startsWith …
    "Shared")` line + `if (!replicated_table || …) return;`) or of the
    `checkQueryValid` ALTER loop (the `isSupportedAlterTypeForOnClusterDDLQuery`
    throw). The `#include` block was reordered upstream — `ASTUpdateQuery.h` moved
    to after `ASTFunction.h` — which is the sole cherry-pick conflict (see §6).
- Upstream changes that touched the patch's behavior (symbols, error codes, callers):
  - `Context::checkMergeTreeSettingsConstraints` and
    `Context::getReplicatedMergeTreeSettings` both still exist on HEAD (the
    `-S` history search on the file is empty → upstream did not add this enqueue
    check) → not obsoleted, no dependency.
  - Identifier inventory all PRESENT on HEAD: `checkMergeTreeSettingsConstraints=8`,
    `getReplicatedMergeTreeSettings=9`, `checkTableEngine=4`, `checkQueryValid=3`,
    `ASTSetQuery=192`, `settings_changes=317`, `endsWith=132`.
- Conclusion: **still-needed-and-applies** (semantics intact). The cherry-pick
  conflicted at ONE site — the `#include` insertion — purely because upstream
  reordered neighbouring includes (context drift, no competing semantic change;
  the HEAD side of the conflict was empty). Resolved by hand, inserting
  `#include <Parsers/ASTSetQuery.h>` in HEAD's alphabetical order. The two code
  hunks applied cleanly.

## 3. C++ review

- 1 Lifetime + ownership: ✓ — both new blocks only read `storage.settings->changes`
  / `alter_command.settings_changes->…->changes` (borrowed from the parsed AST,
  owned by the query) and pass them by const-ref to a Context method; no
  allocation, no ownership transfer, no new lifetimes.
- 2 Exception safety: ✓ — the new call throws `SETTING_CONSTRAINT_VIOLATION` on
  violation; this happens at enqueue time before any state mutation (the query is
  not yet written to the DDL log), so it is a clean early-abort with no rollback needed.
- 3 Thread-safety + concurrency: ✓ — `checkMergeTreeSettingsConstraints` takes the
  Context `SharedLockGuard` internally; the AST data read is request-local. No new
  shared/global state.
- 4 Performance + memory: ✓ — one extra constraint check per `DatabaseReplicated`
  CREATE/ALTER-SETTING DDL (a map lookup per changed setting). Negligible; on the
  DDL path, not a hot loop. Guarded by `storage.settings != nullptr` /
  `settings_changes != nullptr` so non-SETTINGS DDL pays nothing.
- 5 Settings as public API: n/a — no new setting. Behavior change only affects
  users who actually have a constraint on a MergeTree setting; default behavior is
  unchanged. Not a broad-blast-radius default change → no clause-(v) policy gate.
- 6 Error handling: ✓ — reuses the existing `SETTING_CONSTRAINT_VIOLATION`
  (code 452) raised by `SettingsConstraints::Checker::check`; messages such as
  "Setting … shouldn't be greater than N" / "should not be changed". No new code.
- 7 Upstream / vendored code: ✓ — `src/Databases/DatabaseReplicated.cpp` only;
  nothing under `contrib/`.
- 8 Behavior under settings: ✓ — the check consults the *user's* profile
  constraints via the query context; with no constraint configured it is a no-op,
  preserving upstream behavior.

## 4. Test design

(a) **New test that fails on the parent commit and passes after the patch.**

- Test path: `tests/queries/0_stateless/9053_check_mergetree_settings_constraints_before_ddl.sh`
  (+ `.reference`), per the Aiven `9<NNN>_<slug>` convention
  (`docs/aiven/runbooks/testing-suites.md` §4.1). Format is `.sh` because the test
  must submit DDL **as a non-admin user** (identity switch via
  `--user/--password`), which pure `.sql` cannot do. The database is
  `ENGINE = Replicated(...)` — **required**, because the new check lives in the
  `DatabaseReplicated` DDL path; an `Atomic` database would not reach it.
- Setup: a settings profile `… SETTINGS merge_tree_max_suspicious_broken_parts MAX 5`
  (MergeTree-setting constraints use the `merge_tree_` prefix, see
  `src/Access/resolveSetting.h`); a non-admin user bound to it, granted
  `CREATE TABLE, ALTER, DROP TABLE` on the Replicated DB. The user then submits a
  violating `CREATE TABLE … SETTINGS max_suspicious_broken_parts = 999` and a
  violating `ALTER TABLE … MODIFY SETTING max_suspicious_broken_parts = 999`
  (exercises BOTH patched hunks). Each is classified asserting **both** the error
  name `SETTING_CONSTRAINT_VIOLATION` AND the message substring
  `shouldn't be greater than`.

- Pre-patch run output (the FAIL):

  ```text
  9053_check_mergetree_settings_constraints_before_ddl:                   [ FAIL ] 0.89 sec.
  Reason: result differs with reference:
  @@ -1,2 +1,2 @@
  -create_rejected_by_constraint
  -alter_rejected_by_constraint
  +create_unexpectedly_allowed
  +alter_unexpectedly_allowed
  ```

- Post-patch run output (the PASS):

  ```text
  9053_check_mergetree_settings_constraints_before_ddl:                   [ OK ] 0.88 sec.
  1 tests passed. 0 tests skipped.
  ```

- Why this test distinguishes the patch's contribution (per AGENTS.md §7): the
  asserted error code `SETTING_CONSTRAINT_VIOLATION` is shared, so the test does
  not rely on it alone — pre-patch the violating DDL **succeeds** (the bypass:
  enqueued in the user context with no check, then executed by the DDLWorker under
  the system profile, which has no constraint → table created / setting modified),
  yielding `*_unexpectedly_allowed`; post-patch it is rejected at enqueue,
  yielding `*_rejected_by_constraint`. The differential (allowed → rejected) is
  the causation, and the `shouldn't be greater than` substring ties the rejection
  to the MergeTree-settings-constraint path specifically. Reachability (iv-a) was
  confirmed empirically: the pre-patch binary lets the violating DDL through.

## 5. Rollback considerations

- Reverting is safe: the change adds a read-only validation at enqueue; no schema
  migration, no on-disk format change, no new persisted state (ZK nodes, files,
  caches). A revert (drop the two blocks + the include) restores the old behavior
  without a rebuild-blocking migration.
- No setting toggles the new behavior; it is always active. Its effect is scoped
  to users who have a MergeTree-setting constraint configured — for everyone else
  it is a no-op. To "disable" it for a given user, remove the constraint from
  their profile.

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — no earlier carry identified within this uplift's records.

### 25.8-aiven (historical, may be empty)

Original carry: `bb048483602c9e180fc2446d29e4099cf3b24fca` (author Tilman Moeller,
committer Joe Lynch), co-authored by Kevin Michel and Aliaksei Khatskevich. One
`#include` plus two small guard blocks in `DatabaseReplicated.cpp`
(`checkTableEngine` for CREATE, `checkQueryValid` for ALTER … MODIFY SETTING).

### 26.3-aiven (this uplift)

- Cherry-pick was: **conflict-resolved** — `git cherry-pick --no-commit -x`
  reported one content conflict in `DatabaseReplicated.cpp`, confined to the
  `#include` block. Upstream had relocated `#include <Parsers/ASTUpdateQuery.h>`
  (now after `ASTFunction.h`), so git could not reconcile the patch's trailing
  context; the HEAD side of the conflict was **empty** (no competing change). Per
  `docs/aiven/schema/halt-and-escalate.md` this context-drift conflict is not a
  `textual_conflict` — resolved by inserting `#include <Parsers/ASTSetQuery.h>` in
  HEAD's alphabetical include order. Both code hunks applied cleanly.
- Upstream-drift conclusion: still-needed-and-applies (semantics intact).
- Tier 2 (patch-id): `git patch-id --stable` of the staged diff does NOT match the
  source (`4f4d3652… ≠ 0dc5650a…`) because the resolved `#include` hunk has
  different surrounding context lines than the source (upstream's include
  reorder). The decomposition runbook check — `diff` of only the `+`/`-` content
  lines (excluding `---`/`+++` headers) between source and staged — is **empty**,
  proving the added/removed lines are byte-identical and only context shifted.
  Therefore Tier 2 is GREEN with `byte_equivalent: false`.
- Test added at: `tests/queries/0_stateless/9053_check_mergetree_settings_constraints_before_ddl.{sh,reference}`.
- Time-to-port: warm-cache build directory (sccache hot). `.cpp`-only change →
  incremental builds: full post-patch ~30s, pre-patch incremental ~33s,
  post-patch restore incremental ~13s.
- Anything surprising: (1) the parent preflight predicted a clean cherry-pick with
  a MATCHing patch-id; in reality the upstream include reorder forced a one-site
  context-drift conflict and a non-matching (but decomposition-clean) patch-id.
  (2) The compliant `CREATE TABLE` setup statement on a Replicated DB emits a
  per-replica status row (`shard_1 replica_1 OK 0 0`) on stdout; the test silences
  all setup/cleanup DDL so only the two assertion tokens reach the diff.
  (3) `StrReplace`/`Write` on the ~2700-line `DatabaseReplicated.cpp` tripped the
  documented `spawn E2BIG` hook failure (`integration-tests.md §7.4.b`); the
  include conflict was resolved via a Python-through-Shell edit instead.

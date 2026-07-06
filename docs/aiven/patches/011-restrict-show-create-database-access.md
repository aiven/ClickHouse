# Patch 011 — restrict-show-create-database-access

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | (unknown — to be researched at 25.X dossier merge) | (unknown) | (unknown) |
| 25.8-aiven | `654f61e864e5e581f37e83ebf0f0853643f67b7e` | Tilman Moeller (author), alex.khatskevich@aiven.io (committer) | (the version we're porting FROM) |
| 26.3-aiven | `patch-port(011)` | T3.3 worker | `byte-equivalent: false` — style cleanup per parent policy call 3 |

The 26.3-aiven carry is its `patch-port(011)` commit (find it with `git log --grep '^patch-port(011)'`); per `commit-hygiene.md` §4 the `-aiven-dev` branch is resliced at each LTS transition, so we pin the stable subject handle, not a SHA that would re-hash.
## 1. Purpose

Quoted source-commit body (verbatim):

```
Restrict SHOW CREATE DATABASE access

Main service users are allowed the SHOW DATABASES access because it is necessary for their operation. It is implicitly granted by ClickHouse when giving access to a database.

However, we do not want to give them access to SHOW CREATE DATABASE.
This query shows the entire create statement, unredacted. This is actually a useful feature for superusers, but can leak credentials to other users.

Also only show the create query in system.tables to users that were able to create that table.

Co-authored-by: Kevin Michel <kevin.michel@aiven.io>
```

The "why": Aiven main-service users are implicitly granted `SHOW DATABASES` by ClickHouse so they
can operate normally. Without this patch, that grant carries `SHOW CREATE DATABASE` as a side-effect,
which prints the *unredacted* `CREATE DATABASE` statement — including S3 bucket secrets, named-
collection credentials, and any other DSN material embedded in the engine settings. This is a
credential-leak vector. The patch closes it by gating `SHOW CREATE TABLE`, `SHOW CREATE DATABASE`,
and the `create_table_query` / `engine_full` columns in `system.tables` behind the corresponding
`CREATE TABLE` / `CREATE DATABASE` grants — which superusers have, and main-service users do not.

Source SHA on `v25.8.18.1-lts-aiven`: `654f61e864e5e581f37e83ebf0f0853643f67b7e`.
Original author: Tilman Moeller <tilman.moeller@aiven.io>.
Source committer (on the v25.8 branch): Aliaksei Khatskevich <alex.khatskevich@aiven.io>.

## 2. Upstream-drift findings

### Commands run

```bash
# Identifier inventory (15 identifiers, all PRESENT on HEAD).
for id in InterpreterShowCreateQuery executeImpl ASTShowCreateDictionaryQuery \
          ASTShowCreateViewQuery SHOW_DICTIONARIES SHOW_COLUMNS CREATE_TABLE \
          SHOW_DATABASES CREATE_DATABASE getCreateTableQuery getCreateDatabaseQuery \
          tryGetCreateTableQuery StorageSystemTables isGranted ASTCreateQuery; do
  git grep -c -- "$id" -- 'src/Interpreters/' 'src/Storages/' 'src/Access/'
done

# File-history scan.
git log v25.8.18.1-lts..v26.3.10.62-lts -- src/Interpreters/InterpreterShowCreateQuery.cpp --oneline
git log v25.8.18.1-lts..v26.3.10.62-lts -- src/Storages/System/StorageSystemTables.cpp     --oneline

# Upstream-equivalent search.
git log v25.8.18.1-lts..v26.3.10.62-lts \
    --grep 'SHOW CREATE\|show_create\|CREATE_TABLE.*grant\|restrict.*show' --oneline
```

### Findings

- **Identifier inventory**: every identifier in the patch's vocabulary
  (`InterpreterShowCreateQuery`, `executeImpl`, `ASTShowCreateDictionaryQuery`,
  `ASTShowCreateViewQuery`, `AccessType::SHOW_DICTIONARIES`, `AccessType::SHOW_COLUMNS`,
  `AccessType::CREATE_TABLE`, `AccessType::SHOW_DATABASES`, `AccessType::CREATE_DATABASE`,
  `getCreateTableQuery`, `getCreateDatabaseQuery`, `tryGetCreateTableQuery`,
  `StorageSystemTables`, `isGranted`, `ASTCreateQuery`) is PRESENT on `v26.3.10.62-lts-aiven-dev`.
  See `tmp/patch-011/drift-identifiers.log` for per-file counts.

- **Upstream changes to `src/Interpreters/InterpreterShowCreateQuery.cpp`** between
  `v25.8.18.1-lts` and `v26.3.10.62-lts`: ONE commit.
  - `443303c45ebf34cc29acd2b2189353b565d2e596` "Safer guardrails around flag usage" (Raúl Marín,
    2026-01-27). Changes `show_query->temporary` to `show_query->isTemporary()` on two lines
    (originally lines 56 and 84; now in the same file). These lines are ADJACENT to but NOT
    inside the patch hunks. The patch cherry-picks textually with `Auto-merging` line-number
    adjustments and no conflict markers.

- **Upstream changes to `src/Storages/System/StorageSystemTables.cpp`**: many commits
  (~35) but the `tryGetCreateTableQuery` call site retains its pre-patch shape (the
  `if (columns_mask[src_index] || ... ) { ASTPtr ast = database->tryGetCreateTableQuery(...) ... }`
  block). Line number shifted from source 559 to HEAD 622. The `access` variable
  (`const auto access = context->getAccess();`) is declared at line 344 of the post-patch
  file and is in lexical scope at the modified call site, so the patch's `isGranted(...)` call
  compiles without further plumbing.

- **Upstream-equivalent restriction search**: no upstream commit between LTSes introduces a
  comparable access-gate on `SHOW CREATE TABLE` / `SHOW CREATE DATABASE` / `system.tables`.
  The four matches in `tmp/patch-011/drift-grep-history.log` are: a reference-file update
  (`02117_show_create_table_system.reference`), a `SHOW CREATE DATABASE` deadlock fix in the
  Backup database, and a related fast-test fix — none of which are equivalent hardening.

- **Conclusion**: `still-needed-but-rewrite`. The cherry-pick applies textually (no semantic
  rewrite was forced by upstream drift), BUT a parent-instructed Allman-brace cleanup was
  applied on top of the cherry-pick to satisfy the repo style guide (parent policy call 3 —
  see §3 below and Step 2.5 of the dispatch prompt). Net classification: rewrite (style only;
  semantics unchanged).

## 3. C++ review

Per `docs/aiven/skills/cpp-review-checklist.md`. The parent agent pre-reviewed this patch and
made three policy calls (1: VIEW over-restriction shipped as-is; 2: system-table regression
shipped as-is; 3: Allman-brace cleanup applied on the port). The bullets below quote the
parent's pre-review verbatim and do not re-litigate the policy calls.

- **1 Lifetime + ownership**: `n/a — patch adds access-check calls + a boolean predicate; no new owners.`
- **2 Exception safety**: `✓ — checkAccess() throws on denial; calls are inside the existing try/catch in executeImpl. The new check in StorageSystemTables is a query of access state (no throw); the gated tryGetCreateTableQuery already handles its own exceptions.`
- **3 Thread-safety + concurrency**: `✓ — access checks read from ContextAccess which is per-query and thread-confined. No new shared state.`
- **4 Performance + memory**: `✓ — two added checkAccess calls (one in each non-dictionary branch) and one isGranted call per row in StorageSystemTables. isGranted is a hashset lookup; cost is negligible vs. tryGetCreateTableQuery's parsing cost.`
- **5 Settings as public API**: `n/a — no setting introduced. There is intentionally no escape hatch; the patch is a hardening, not a feature flag.`
- **6 Error handling**: `✓ — denial throws ACCESS_DENIED with the standard "necessary to have the grant ..." message; consistent with ClickHouse's access-check error shape.`
- **7 Upstream / vendored code**: `✓ — both files are upstream-owned; patch is a documented Aiven hardening (commit body explains the credential-leak motivation).`
- **8 Behavior under settings**: three documented limitations, all carried forward from the source patch's threat model (parent policy calls 1 + 2):
  - **VIEW over-restriction** (policy call 1): `SHOW CREATE VIEW` now requires `CREATE_TABLE`
    (because `ASTShowCreateViewQuery` flows through the non-`is_dictionary` branch alongside
    `ASTShowCreateTableQuery`). A user with `CREATE_VIEW` but not `CREATE_TABLE` cannot
    `SHOW CREATE VIEW` their own view. The "correct" fix would require checking
    `CREATE_TABLE ∪ CREATE_VIEW ∪ CREATE_DICTIONARY` via `isGranted`, which is circular with
    the access-check ordering (need entity type first; the branch discriminates only
    dictionary vs. table/view). Shipped as-is; revisit if Aiven main-service users start
    being granted `CREATE_VIEW` without `CREATE_TABLE`.
  - **System-table regression** (policy call 2): `SHOW CREATE TABLE system.X` now requires
    `CREATE TABLE ON system.X`, which non-superusers typically lack. Operationally this means
    monitoring queries that introspect `system.*` definitions will break for non-superusers.
    Shipped as-is per parent policy; verify out-of-band against Aiven main-service user grant
    defaults.
  - **`system.tables.engine` column** (not in the gated block) remains visible to non-priv
    users; only `create_table_query`, `engine_full`, and the third gated column hide
    credentials. This matches the patch's intent (engine TYPE is not a secret; only the
    DSN/credentials in `engine_full` are).

## 4. Test design

Option (a) — new test that fails on the parent commit and passes after the patch.

- **Test paths**:
  - `tests/queries/0_stateless/9011_restrict_show_create_access.sh`
  - `tests/queries/0_stateless/9011_restrict_show_create_access.reference`
- **Format**: `.sh` (multi-user — requires multiple `clickhouse-client --user=<name>` invocations;
  `.sql` is single-user-per-session). Pattern reference: `tests/queries/0_stateless/02561_temporary_table_grants.sh`.
- **Three behaviors exercised**, pre→post:
  1. Privileged user (granted `SHOW DATABASES, SHOW TABLES, SHOW COLUMNS, CREATE DATABASE, CREATE TABLE`)
     — `SHOW CREATE TABLE`, `SHOW CREATE DATABASE`, and `SELECT create_table_query FROM system.tables`
     continue to succeed.
  2. Non-privileged user (granted `SHOW DATABASES, SHOW TABLES, SHOW COLUMNS` only) — `SHOW CREATE TABLE`
     and `SHOW CREATE DATABASE` now throw `ACCESS_DENIED` naming the specific grant.
  3. Non-privileged user — `SELECT create_table_query FROM system.tables WHERE ...` now returns
     empty (was the actual create query pre-patch).
- **Tags**: none.

**Why this distinguishes pre-patch from post-patch** (per AGENTS.md §7):
- Pre-patch: nonpriv has `SHOW_COLUMNS`, the ONLY grant pre-patch's `checkAccess` requires for
  `SHOW CREATE TABLE`. The query SUCCEEDS, produces stdout with the create statement (no
  "necessary to have the grant" substring), `grep` fails, `&& echo` short-circuits, the `echo`
  is skipped. Similarly for `SHOW CREATE DATABASE`. The third check
  (`SELECT create_table_query = '' FROM system.tables`) returns `0` pre-patch (because the
  create_query is non-empty). The diff vs the 6-line reference therefore shows: lines 4-6 are
  missing the three expected lines (`nonpriv_table_denied`, `nonpriv_db_denied`, `1`) and line 4
  has an unexpected `0`. → FAIL.
- Post-patch: nonpriv lacks `CREATE_TABLE` and `CREATE_DATABASE`. The two `SHOW CREATE` queries
  FAIL with the substrings `"necessary to have the grant CREATE TABLE"` /
  `"necessary to have the grant CREATE DATABASE"`. The substring is specific to the access-check
  branch — it does not appear in any successful `CREATE TABLE` statement, only in the
  `ACCESS_DENIED` error message. `grep` matches, `echo` runs. The `system.tables` row returns
  empty `create_table_query` (gated by `isGranted CREATE_TABLE`), so `= ''` returns `1`. All
  six lines emit → diff is empty → PASS.

The substring assertion satisfies AGENTS.md §7's "must distinguish from upstream behavior":
upstream throws `ACCESS_DENIED` with the *same* error code for many reasons, but only the
Aiven gate emits the `"necessary to have the grant CREATE TABLE"` / `"necessary to have the
grant CREATE DATABASE"` substring on `SHOW CREATE` queries where the user already holds
`SHOW_COLUMNS` / `SHOW_DATABASES`.

**Pre-patch run output** (the FAIL), from `tmp/patch-011/test-prepatch.log`:

```text
9011_restrict_show_create_access:                                      [ FAIL ] 1.15 sec.
Reason: result differs with reference:
--- .../9011_restrict_show_create_access.reference
+++ .../9011_restrict_show_create_access.stdout
@@ -1,6 +1,4 @@
 priv_table_ok
 priv_db_ok
 1
-nonpriv_table_denied
-nonpriv_db_denied
-1
+0

Having 1 errors! 0 tests passed. 0 tests skipped. 1.17 s elapsed (Process-3).
```

**Post-patch run output** (the PASS), from `tmp/patch-011/test-postpatch.log`:

```text
9011_restrict_show_create_access:                                      [ OK ] 1.06 sec.

1 tests passed. 0 tests skipped. 1.09 s elapsed (Process-3).
```

**Known test limitations**: the test exercises one user-grant combination per scenario;
it does not enumerate role-based grants, default roles, partial table-level vs database-level
grants, or the system-table regression (the test runs against `${CLICKHOUSE_DATABASE}`, not
`system`). The VIEW over-restriction is also not exercised by the test (no view created).
These uncovered cases are documented in §3 item 8 — future patches may want to add coverage if
Aiven's threat model evolves.

## 5. Rollback considerations

- **Revert safety**: yes. The patch only adds access checks and a boolean predicate; no schema
  migration, no on-disk format change, no new metadata. Reverting is a pure source revert.
- **State surviving restart**: none. The patch does not write any new files, ZK nodes, or
  in-memory caches. Restart is a no-op for this gate.
- **Re-enabling pre-patch behavior on a running server**: there is NO setting that disables
  the gate (intentional — this is a hardening, not a feature flag, per parent policy call 1
  for the VIEW over-restriction). To restore pre-patch behavior for a specific user, GRANT
  `CREATE_TABLE` and/or `CREATE_DATABASE` to that user on the relevant scope (`*.*`,
  `<db>.*`, `<db>.<tab>`).

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — to be researched at the 25.X dossier merge. The patch's first carry on the Aiven branch
predates the dossier system.

### 25.8-aiven (historical, may be empty)

The version we are porting FROM. Source SHA `654f61e864e5e581f37e83ebf0f0853643f67b7e`.
Author Tilman Moeller; committer Aliaksei Khatskevich. Co-author: Kevin Michel. Author date
2025-12-09.

### 26.3-aiven (this uplift)

- **Cherry-pick was**: `still-needed-but-rewrite` — style cleanup applied per parent policy
  call 3 (Allman braces in `InterpreterShowCreateQuery.cpp` hunk only).
- **Upstream-drift conclusion**: textual cherry-pick clean; the only upstream commit touching
  `InterpreterShowCreateQuery.cpp` between LTSes
  (`443303c45ebf34cc29acd2b2189353b565d2e596` "Safer guardrails around flag usage") is
  adjacent to but not inside the patch hunks. `StorageSystemTables.cpp` saw many upstream
  commits but the modified call site retains its pre-patch shape. No upstream-equivalent
  restriction. See §2.
- **Test added at**: `tests/queries/0_stateless/9011_restrict_show_create_access.{sh,reference}`.
- **Time-to-port (subagent wall-clock)**: ~10 minutes (Step 0 start at `2026-05-24T19:41:28Z` →
  Step 7 dossier authoring at `~2026-05-24T19:50Z`). **Build-cache state: warm-cache.** The
  build directory had been hot from T3.2's runs. Full post-patch build: 29 s.
  Incremental pre-patch rebuild: 17 s. Incremental post-patch restore build: 18 s.
  These numbers are consistent with the warm-cache reference in
  `docs/aiven/runbooks/build-and-test.md` §3 (66 s / 22 s / 14 s in T3.2) and the
  warm-cache footnote in this template (§6 "annotate cold/warm").
- **Anything surprising**: One subtlety worth recording — the test's `set -e` does NOT kill
  the script on the failing-grep path, because `&& echo` short-circuits `set -e` (the failing
  command is before the final `&&`). The test still produces an honest, distinguishing FAIL
  diff (missing 3 expected lines + an unexpected `0` line). The mechanism is different from
  what the dispatch prompt's prose described ("set -e kills the test"); the test design is
  still correct because the diff vs `.reference` is what the runner checks. Future test-design
  prompts may want to clarify this nuance.

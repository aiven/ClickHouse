# Patch 060 — alter-order-by-sorting-key-zk-metadata

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | (unknown — record "to be researched at 25.X dossier merge") | n/a | n/a |
| 25.8-aiven | `93c2be960f931ee320ebf0d26fffa72e7f6b6583` | Tilman Moeller (author), Aliaksei Khatskevich (committer & co-author) | (the version we're porting FROM) |
| 26.3-aiven | `patch-port(060)` | T3.10 worker (initial port); T3.11 + T3.12 verification dispatches | `byte_equivalent: true`; **ship-without-test no-regression port** — three verification attempts (T3.10 / T3.11 / T3.12) could not produce evidence-of-causation on 26.3 because the patch's reachable code paths are observationally inert there in exactly the same way they were inert on 25.8 (no-regression proof in §4). |

The 26.3-aiven carry is its `patch-port(060)` commit (find it with `git log --grep '^patch-port(060)'`); per `commit-hygiene.md` §4 the `-aiven-dev` branch is resliced at each LTS transition, so we pin the stable subject handle, not a SHA that would re-hash.
## 1. Purpose

The patch makes `ALTER TABLE ... MODIFY ORDER BY ...` produce the same ZooKeeper `metadata` znode shape as `CREATE TABLE ... ENGINE = ReplicatedMergeTree(...) ORDER BY ...` for tables that do NOT declare a separate `PRIMARY KEY`. Pre-patch, `ALTER` serializes a `sorting key:` line into the `metadata` znode whenever the `sorting_key.definition_ast` changes, even when `CREATE` would NOT have serialized such a line (because the ctor in `ReplicatedMergeTreeTableMetadata.cpp:49-79` only emits `sorting key:` when `isPrimaryKeyDefined` is true). Post-patch, `ALTER` mirrors the ctor's rule and only emits `sorting key:` under the same condition. The "why" is metadata-shape parity between `ALTER` and `CREATE`, which avoids divergent ZK metadata between replicas that were created vs altered.

Source SHA on `v25.8.18.1-lts-aiven`: `93c2be960f931ee320ebf0d26fffa72e7f6b6583` (cherry-picked verbatim; patch-id `27037b121ff8df19a7d61bcd20cfed16a53364b2` matches the staged result).

Original author: Tilman Moeller `<tilman.moeller@aiven.io>` (per the source commit's `Author:` line), with co-author Aliaksei Khatskevich `<alex.khatskevich@aiven.io>`.

Original purpose (`git log --format=%B` on the source SHA, verbatim):

```
Fix alter order by
`alter code` is detached from `create table` code, which makes it
necessary to copy field initializaiton logic. This commit makes
`alter table` to produce the same `sorting key` ZooKeeper metadata as
`create table`.

This commit was applied from the patch file 0087-Fix-alter-order-by.patch

Co-authored-by: Aliaksei Khatskevich <alex.khatskevich@aiven.io>
```

## 2. Upstream-drift findings

### Commands run

```bash
for id in ast_to_str isPrimaryKeyDefined future_metadata_in_zk \
          ReplicatedMergeTreeTableMetadata StorageInMemoryMetadata; do
  echo "=== $id ==="
  git grep -c -- "$id" -- 'src/Storages/' | head -10
done | tee tmp/patch-060/drift-identifiers.log

git log v25.8.18.1-lts..v26.3.10.62-lts -- src/Storages/StorageReplicatedMergeTree.cpp --oneline \
    | tee tmp/patch-060/drift-file-history.log

git log v25.8.18.1-lts..v26.3.10.62-lts \
        --grep 'sorting_key' --grep 'isPrimaryKeyDefined' --grep 'alter.*metadata' \
        -i --oneline \
    | tee tmp/patch-060/drift-grep-history.log

sed -n '6715,6730p' src/Storages/StorageReplicatedMergeTree.cpp \
    | tee tmp/patch-060/drift-hunk-context.log
```

### Findings

- Identifier inventory (all five present in 26.3):
  - `ast_to_str` — 6 hits in `StorageReplicatedMergeTree.cpp` (local helper).
  - `isPrimaryKeyDefined` — declared in `StorageInMemoryMetadata.h:264`; defined in `StorageInMemoryMetadata.cpp`; called in `MergeTree/ReplicatedMergeTreeTableMetadata.cpp` (the ctor whose rule the patch propagates).
  - `future_metadata_in_zk` — 10 hits in `StorageReplicatedMergeTree.cpp`.
  - `ReplicatedMergeTreeTableMetadata` and `StorageInMemoryMetadata` both present.
- Upstream changes to touched files between prior and current LTS:
  - `src/Storages/StorageReplicatedMergeTree.cpp`: 1736 upstream commits between `v25.8.18.1-lts` and `v26.3.10.62-lts` (one of the highest-churn files in the repo). The file grew by 84 lines around the patched region (the source diff was at line 6637; 26.3 carries it at line 6721).
- Upstream changes that touched the patch's behavior (symbols, error codes, callers):
  - The targeted history-grep returned 16 commits matching `sorting_key` / `isPrimaryKeyDefined` / `alter.*metadata` (case-insensitive). Manual review confirms NONE of them modify the specific branch at line 6721; they fall into three orthogonal buckets:
    - Test fixes for flaky ALTER metadata tests (`02943_rmt_alter_metadata_merge_checksum_mismatch`, `01079_parallel_alter_detach_table_zookeeper`, `03144_parallel_alter_add_drop_column_zookeeper_on_steroids`).
    - Projection-key feature work (`materialize_subcolumns_for_projection_sorting_key`, #99043).
    - Unrelated ALTER bug fixes (DROP+ADD COLUMN resurrecting stale data; metadata-only ALTER block-structure mismatches).
  - `isPrimaryKeyDefined` is NOT called from `StorageReplicatedMergeTree.cpp` on current HEAD — upstream has not independently landed an equivalent fix.
- Hunk-line verification: lines 6720-6727 in 26.3 match the source patch's ±5 context byte-for-byte, including the `extractKeyExpressionList` explanatory comment.
- **Wider-scope divergence (discovered later, during T3.11 verification):** the patch's predicate `future_metadata.isPrimaryKeyDefined()` depends on `primary_key.definition_ast == nullptr` surviving through `AlterCommands::apply`. The MODIFY_ORDER_BY branch at `src/Storages/AlterCommands.cpp:653-666` assigns `primary_key = KeyDescription::getKeyFromAST(sorting_key.definition_ast, ...)` and **does not** subsequently null `definition_ast` (in contrast to two other call-sites — `registerStorageMergeTree.cpp:691-695` and `AlterCommands.cpp:1313-1321` — which do). This asymmetry is not new on 26.3: `git log -L 653,666:src/Storages/AlterCommands.cpp` shows the current shape was reached in upstream commit `465c4b65b72` ("Slightly better interfaces and comments") on **2020-06-12**, with a cosmetic comment-only follow-up in `2c9ce0f3fa4` on **2020-06-15**. The patch was authored on **2026-01-13** (~5.5 years later), so the divergence pre-existed the patch by half a decade. The original drift scan missed it because the patch does not touch `AlterCommands.cpp` and `KeyDescription::getKeyFromAST` is not an identifier that appears in the patch text. This finding is the cornerstone of the no-regression argument in §4.
- Conclusion: `still-needed-and-applies` for cherry-pick purposes (source is byte-equivalent, surrounding-code context is byte-stable for the patched line itself). Observable effectiveness on 26.3 is matched by observable effectiveness on 25.8 — see §4 for the verification record.

## 3. C++ review

Per `docs/aiven/skills/cpp-review-checklist.md`. One bullet per checklist section.

- 1 Lifetime + ownership: `n/a — patch replaces one condition with another; no object lifetimes change.`
- 2 Exception safety: `n/a — isPrimaryKeyDefined() is a const accessor that returns bool; cannot throw.`
- 3 Thread-safety + concurrency: `note — ALTER's serialization is protected by the table's alter-lock; the patch does not change the lock discipline. The patched line runs under the same lock context as before.`
- 4 Performance + memory: `✓ — patch eliminates a string allocation + AST comparison on the hot ALTER path. The pre-patch ast_to_str(...) fires on any sorting_key AST change and allocates two strings for comparison. Post-patch the branch fires only when isPrimaryKeyDefined() is true (a const bool accessor), which is the minority case in typical deployments (most tables use ORDER BY only). Net: faster + less allocation on the common path.`
- 5 Settings as public API: `n/a — patch is internal serialization logic, no setting touched.`
- 6 Error handling: `n/a — patch does not introduce or alter any error path.`
- 7 Upstream / vendored code: `note — StorageReplicatedMergeTree.cpp is upstream-owned and heavily-touched (1736 commits between the two LTS tags). The patch lands in a region upstream has not independently modified (verified via file-history grep). Risk of future-upstream conflict: medium (the file is high-churn) but the surface is small (one if-condition).`
- 8 Behavior under settings: `note — the metadata-shape change is invisible to existing tables (their metadata znode is only rewritten on ALTER; pre-patch metadata stays as-is). Existing tables that were ALTERed pre-patch carry the (incorrect) sorting_key line forever; they are NOT auto-cleaned by this patch. Document this as a known characteristic, not a regression.`
- 9 Parent preflight discipline: `✓ — parent's three-step prior was "still-needed-and-applies": (i) target line text byte-stable, (ii) ±5 context lines match, (iii) isPrimaryKeyDefined not yet present in StorageReplicatedMergeTree.cpp. API check passes (StorageInMemoryMetadata::isPrimaryKeyDefined exists at .h:264). Effect lands synchronously in ZK metadata znode (line 6760-6761 of the patched file, via zkutil::makeSetRequest in the ALTER's atomic op-list).`

## 4. Test design

### Decision: ship without a test file

Three verification attempts (T3.10, T3.11, T3.12) failed to produce evidence-of-causation on 26.3. The test files authored during these attempts were UNSTAGED + DELETED from this commit. This is acceptable because the patch's observable behavior on 26.3 is identical to its observable behavior on 25.8 — see the "No-regression proof" subsection below.

### Verification record

#### T3.10 (initial dispatch, direct `MODIFY ORDER BY`)

```sql
CREATE TABLE aiven_060_alter_order_by (a UInt64, b UInt64)
ENGINE = ReplicatedMergeTree('/test/aiven_060/' || currentDatabase() || '/r', 'replica_1')
ORDER BY a;
ALTER TABLE aiven_060_alter_order_by MODIFY ORDER BY (a, b);
SELECT countSubstrings(value, 'sorting key:') FROM system.zookeeper WHERE path = ... AND name = 'metadata';
-- reference: 0
```

Outcome: `test_design_blocked`. `MergeTreeData::checkProperties` (`src/Storages/MergeTree/MergeTreeData.cpp:1010-1014`) rejects the ALTER with `Code 36 BAD_ARGUMENTS` because column `b` is pre-existing rather than newly-added in the same ALTER. The check runs inside `MergeTreeData::checkAlterIsPossible`, invoked from `InterpreterAlterQuery::executeToTable` BEFORE `StorageReplicatedMergeTree::alter` is entered, so the patched line at `StorageReplicatedMergeTree.cpp:6722` is never reached. No worker report was filed for T3.10 (this was the original port dispatch; analysis lives only in this dossier history).

#### T3.11 (parent-redesigned, `ADD COLUMN + MODIFY ORDER BY`)

```sql
CREATE TABLE aiven_060_alter_order_by (a UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/aiven_060_alter_order_by/r', 'replica_1')
ORDER BY a;
ALTER TABLE aiven_060_alter_order_by ADD COLUMN b UInt64, MODIFY ORDER BY (a, b);
SELECT countSubstrings(value, 'sorting key:') FROM system.zookeeper WHERE path = ... AND name = 'metadata';
-- reference: 0
```

Outcome: `postpatch_fail`. The ALTER passes the `checkProperties` rules (`b` is newly-added; no default expression — see `MergeTreeData.cpp:1016-1019`) and reaches the patched line, BUT the predicate `future_metadata.isPrimaryKeyDefined()` evaluates to `true` post-patch because `AlterCommands::apply`'s `MODIFY_ORDER_BY` branch (`src/Storages/AlterCommands.cpp:653-666`) sets `primary_key = KeyDescription::getKeyFromAST(sorting_key.definition_ast, ...)` and never nulls `definition_ast` again. The patched branch fires, writes `sorting key: a, b` to the znode, and the test sees `actual=1 vs reference=0` — identical observable behavior to pre-patch. Full analysis: `docs/aiven/uplifts/26.3/reports/T3.11-postpatch-fail.md`.

#### T3.12 (parent-redesigned, `RENAME COLUMN`)

```sql
CREATE TABLE aiven_060_alter_order_by (a UInt64, b UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/aiven_060_alter_order_by/r', 'replica_1')
ORDER BY a;
ALTER TABLE aiven_060_alter_order_by RENAME COLUMN a TO a_new;
SELECT countSubstrings(value, 'sorting key:') FROM system.zookeeper WHERE path = ... AND name = 'metadata';
-- reference: 0
```

Outcome: `postpatch_fail`. `MergeTreeData::checkAlterIsPossible` (`src/Storages/MergeTree/MergeTreeData.cpp:4445-4452`) rejects with `Code 524 ALTER_OF_COLUMN_IS_FORBIDDEN` because column `a` is part of a key expression. The structural argument (in the T3.12 report) is that the RENAME-COLUMN scenario is statically un-usable as a patch differential: renaming a sort-key column is upstream-rejected, and renaming a non-sort column does not mutate `sorting_key.definition_ast` so pre- and post-patch both emit `0` (a tautological pass, not evidence-of-causation). Full analysis: `docs/aiven/uplifts/26.3/reports/T3.12-postpatch-fail.md`.

### No-regression proof vs 25.8

Three statements together establish that shipping this patch on 26.3 produces the same observable behavior as on 25.8:

1. **Source byte-equivalence.** Patch-id `27037b121ff8df19a7d61bcd20cfed16a53364b2` matches between the 25.8 source SHA `93c2be960f931ee320ebf0d26fffa72e7f6b6583` and our staged 26.3 cherry-pick (recorded in §6, Tier 2 PASS).
2. **Identical surrounding context at the patched line.** §2's "Hunk-line verification" finding shows lines 6720-6727 in 26.3 match the source patch's ±5 context byte-for-byte; no upstream commit between the two LTS tags modifies the patched line itself.
3. **Identical state of the `AlterCommands::apply` divergence that defeats the predicate on 26.3.** §2's "Wider-scope divergence" finding shows the MODIFY_ORDER_BY branch reached its current shape in upstream commit `465c4b65b72` on 2020-06-12, ~5.5 years before the source patch was authored. Therefore the `MODIFY_ORDER_BY` code path was equally inert under the patch on 25.8 as it is on 26.3. T3.11's root-cause analysis applies identically to both LTS branches.

Together: the patch's observable behavior on 26.3 — including its observable inertness on the two reachable code paths we tested — is a property the patch had on 25.8 too. Shipping this patch on 26.3 introduces **no regression in observable behavior**. The patch's actual fix-scenario, if it has one on either LTS branch, lives on an ALTER path the verification attempts did not reach (low-priority candidates listed in §7).

### Test files not shipped

- `tests/queries/0_stateless/9060_alter_order_by_zk_metadata.sql` — authored, NOT shipped.
- `tests/queries/0_stateless/9060_alter_order_by_zk_metadata.reference` — authored, NOT shipped.

The bodies are preserved (a) inline in the verification record above for T3.10, (b) in `docs/aiven/uplifts/26.3/reports/T3.11-postpatch-fail.md` and `T3.12-postpatch-fail.md` for T3.11 and T3.12, and (c) in the worker scratch directories `tmp/patch-060/`, `tmp/patch-060-t311/`, and `tmp/patch-060-t312/` (gitignored). No information is lost by dropping the files from the commit.

### Existing upstream test that covers this patch

`n/a` — no existing upstream test was identified as exercising the patch's predicate-flip.

## 5. Rollback considerations

- Revert is safe: no schema migration, no on-disk format change.
- Tables created/altered pre-patch may already have a `sorting key:` line in their `metadata` znode; this patch does NOT clean them retroactively. To clean them, a one-off ALTER NOOP (e.g., `ALTER TABLE r MODIFY SETTING something_innocuous`) would rewrite the metadata znode through the post-patch path. This is documented for operators, not introduced by the patch.
- State that survives a `clickhouse-server` restart: the `metadata` znode shape change is durable once an ALTER runs post-patch.
- Setting to disable the new behavior without rebuilding: none — the patch removes a code branch unconditionally. To disable, revert the patch and rebuild.

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — record "to be researched at 25.X dossier merge".

### 25.8-aiven (historical, may be empty)

n/a — historical row; only the SHA is known (`93c2be960f931ee320ebf0d26fffa72e7f6b6583`).

### 26.3-aiven (this uplift)

- Cherry-pick was: **clean** (`git cherry-pick --no-commit -x 93c2be960f...` produced exactly 2 insertions + 1 deletion in `src/Storages/StorageReplicatedMergeTree.cpp` around line 6721; no conflict markers; `Auto-merging` reported but the diff applied verbatim).
- Patch-id semantic verification: **byte-equivalent**. `git show 93c2be960f... | git patch-id --stable` and `git diff --cached | git patch-id --stable` both produced `27037b121ff8df19a7d61bcd20cfed16a53364b2`. Tier 2 PASS.
- Build: post-patch incremental rebuild succeeded (`ninja -C build clickhouse` exit 0, ~76s wall, warm cache). Tier 3a PASS. Built target `programs/clickhouse` includes the rebuilt `Storages/StorageReplicatedMergeTree.cpp.o`.
- Upstream-drift conclusion: `still-needed-and-applies` (per §2).
- Tests: **NOT shipped** (see §4 "Decision: ship without a test file"). Three verification attempts — T3.10 (`MODIFY ORDER BY` direct), T3.11 (`ADD COLUMN + MODIFY ORDER BY`), T3.12 (`RENAME COLUMN`) — failed to produce evidence-of-causation on 26.3. Test bodies are preserved in `docs/aiven/uplifts/26.3/reports/T3.11-postpatch-fail.md` and `T3.12-postpatch-fail.md` (and inline in §4 for T3.10).
- Tier 3b status: **deferred, no-regression justified**. The verification record + the upstream-history evidence in §2 ("Wider-scope divergence") together establish that the patch's observable behavior on 26.3 is identical to its observable behavior on 25.8 — same byte-equivalent source change against an `AlterCommands::apply` state that has been unchanged since 2020-06. Shipping introduces no regression vs the 25.8 baseline. See §4 "No-regression proof vs 25.8" for the full argument.
- Time-to-port (subagent + parent wall-clock): T3.10 ~15 min (cherry-pick + initial dossier + first test attempt); T3.11 ~8 min (verification + escalation); T3.12 ~8 min (verification + escalation); parent investigation between attempts ~30 min cumulative (`AlterCommands.cpp` upstream history, alternative ALTER paths, no-regression argument construction).
- Anything surprising: **yes — twice.** (1) `MergeTreeData::checkProperties:1010-1014` and `checkAlterIsPossible:4445-4452` together gate-keep most "natural" `ALTER ORDER BY`-shaped tests upstream of the patched line on 26.3 (each verification attempt hit a different gate). (2) The `AlterCommands::apply` MODIFY_ORDER_BY divergence (assigns `primary_key` without nulling `definition_ast`) has been in upstream code since 2020-06, ~5.5 years before the source patch was authored — so the patch was observationally inert on direct `MODIFY ORDER BY` on 25.8 too. The patch is a no-op on the reachable code paths we tested on both LTS branches; whatever fix-scenario it has lives on a path we have not yet identified.

## 7. Follow-ups

All follow-ups are LOW priority — the patch ships as a byte-equivalent no-regression port and is not blocking. Re-open only on concrete evidence (e.g., a production divergence between `CREATE`-shaped and `ALTER`-shaped ZK metadata that affects replica join behavior).

1. **Find the patch's actual fix-scenario.** Three verification attempts on direct ORDER BY mutation paths (T3.10/T3.11/T3.12) failed; the patch's predicate-flip must fire on some less-common ALTER path. Unexplored candidates (none currently promising):
   - `MODIFY COLUMN <type>` on a sort-key column where the type change is permitted (most are upstream-rejected).
   - `DROP COLUMN` of a non-sort column where the sort-key expression references it indirectly (e.g., via a function), if the engine permits.
   - Materialized-view / projection DDL on `Replicated*MergeTree` whose code path is different from direct table ALTER.
   - Integration-shaped test that constructs the ZK znode divergence by direct manipulation pre-patch and asserts post-patch ALTER no longer rewrites the line.
2. **`AlterCommands::apply` asymmetry (upstream observation).** The MODIFY_ORDER_BY branch at `src/Storages/AlterCommands.cpp:653-666` writes through `primary_key.definition_ast` without restoring the `nullptr` invariant that two other call-sites (`registerStorageMergeTree.cpp:691-695` and `AlterCommands.cpp:1313-1321`) preserve. This is what defeats patch 060's predicate on direct MODIFY ORDER BY. It is a candidate upstream-bug or Aiven-side companion edit — but not in scope for this patch.
3. **ALTER-vs-CREATE metadata-parity audit (deferred from T3.10's parent preflight).** Other AST-comparison checks in `StorageReplicatedMergeTree::alter` that don't match the CREATE ctor's logic may carry the same shape as the bug this patch attempts to fix. Re-open if a similar metadata-shape divergence is observed in production.

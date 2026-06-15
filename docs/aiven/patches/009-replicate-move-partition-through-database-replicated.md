# Patch 009 — replicate-move-partition-through-database-replicated

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | n/a | n/a | (not carried on 25.3) |
| 25.8-aiven | `110900c986af8128279f1b7332a4cd265bed44b9` | Tilman Moeller (committed by Aliaksei Khatskevich) | original carry |
| 26.3-aiven | (staged — source + dossier; test design ESCALATED) | T3.18 worker | conflict-free cherry-pick + Allman restyle; `test_design_blocked` (see §4) |

The current uplift's row stays "(staged)" until the human commits.

## 1. Purpose

In a `DatabaseReplicated` cluster, `ALTER TABLE ... MOVE PARTITION ... TO TABLE ...`
was not entered into the database DDL log, so it executed only on the replica
that received the client query. The patch extends
`DatabaseReplicated::shouldReplicateQuery` to return `true` for
`PartitionCommand::MOVE_PARTITION`, routing the move through the DDL log so it is
replayed as a `SECONDARY_QUERY` on every replica. The stated motivation is that
`MOVE PARTITION TO TABLE` needs database-level coordination to keep source and
destination tables consistent across replicas, whereas `DROP`/`ATTACH PARTITION`
are claimed to ride `ReplicatedMergeTree`'s own replication.

Source SHA on `v25.8.18.1-lts-aiven`: `110900c986af8128279f1b7332a4cd265bed44b9`
(inventory.md row 009).
Original author: `tilman.moeller@aiven.io` (committed by
`alex.khatskevich@aiven.io`), 2025-12-07.
Original purpose (verbatim from the commit body):

> Previously, MOVE PARTITION commands were not replicated through the
> DatabaseReplicated DDL log, causing data inconsistency when moving
> partitions between tables across replicas in a DatabaseReplicated cluster.
> The issue occurred because shouldReplicateQuery() only checked for
> AlterCommand (metadata changes) but not for PartitionCommand::MOVE_PARTITION.
> Other partition commands like DROP PARTITION and ATTACH PARTITION are
> handled by ReplicatedMergeTree's own replication mechanism, but MOVE
> PARTITION TO TABLE requires database-level coordination to ensure both
> source and destination tables are updated consistently across all replicas.

## 2. Upstream-drift findings

### Commands run

```bash
git grep -c <identifiers> -- src/                              # identifier-grep.log
git log --oneline v25.8.18.1-lts..v26.3.10.62-lts -- src/Databases/DatabaseReplicated.cpp   # file-history.log
git log --oneline -S MOVE_PARTITION v25.8.18.1-lts..v26.3.10.62-lts -- src/Databases/DatabaseReplicated.cpp  # upstream-equivalent.log
sed -n '40,60p' src/Databases/DatabaseReplicated.cpp          # head-includes.log
grep -n 'Metadata alter should go through database' src/Databases/DatabaseReplicated.cpp
```

### Findings

- Upstream changes to touched files between prior and current LTS:
  - `src/Databases/DatabaseReplicated.cpp`: many commits touched the file, but
    NONE rewrote the `shouldReplicateQuery` MOVE-PARTITION region. The pre-image
    is byte-identical (the `for`-loop over `alter->command_list->children`
    calling `AlterCommand::parse` then `return false`).
- Upstream changes that touched the patch's behavior (symbols, error codes, callers):
  - `MOVE_PARTITION` in `DatabaseReplicated.cpp`: `upstream-equivalent.log` is
    EMPTY — upstream did NOT add MOVE-PARTITION handling to
    `shouldReplicateQuery`. NOT obsoleted-by-upstream.
  - All referenced identifiers present on HEAD: `shouldReplicateQuery` (13),
    `ASTAlterCommand` (508), `AlterCommand::parse` (6), `PartitionCommand::parse`
    (2), `MOVE_PARTITION` (23). `PartitionCommand::parse` returns
    `std::optional<PartitionCommand>` (`PartitionCommands.h:76`); `MOVE_PARTITION`
    enumerator exists (`PartitionCommands.h:26`).
- Conclusion: **`still-needed-but-rewrite`** — semantics unchanged; "rewrite" is
  only the mandated K&R→Allman brace restyle + quoted→angle include change +
  redundant-`else` removal (parent policy calls 1 & 2), NOT semantic drift.
  (Caveat: §4 shows the patch's data-consistency *premise* does not reproduce on
  this base — that is a TEST-design problem, not an upstream-drift conclusion.
  The source change itself is structurally sound and applies cleanly.)

## 3. C++ review

Per `docs/aiven/skills/cpp-review-checklist.md`. One bullet per section.

- 1 Lifetime + ownership: `n/a` — `child_command` is a transient
  `ASTAlterCommand *` borrowed from `alter->command_list->children` (owned by
  the AST); `partition_command` is a stack `std::optional<PartitionCommand>`.
  No new ownership introduced.
- 2 Exception safety: `✓` — the new `PartitionCommand::parse` call runs inside
  the SAME existing `try { ... } catch (...) { tryLogCurrentException(log); }`
  whose fall-through is `return true` (conservative: replicate-if-unsure). A
  throw from `parse` is caught exactly as before; no invariant change.
- 3 Thread-safety + concurrency: `n/a` — pure read of the parsed AST on the
  query thread; no shared mutable state, no locks, no `sleep`.
- 4 Performance + memory: `✓` — control/parse path, not a per-row hot path. One
  extra `PartitionCommand::parse` per `ALTER` child only when
  `AlterCommand::parse` returned false. Negligible.
- 5 Settings as public API: `n/a` — no setting read or added. (Clause (v) does
  NOT fire: this changes routing for `MOVE PARTITION` only, not a fleet-wide
  default with broad blast radius.)
- 6 Error handling: `n/a` — no new error code; matches on the existing
  `PartitionCommand::MOVE_PARTITION` enumerator (`PartitionCommands.h:26`).
- 7 Upstream / vendored code: `✓` — only `src/Databases/DatabaseReplicated.cpp`
  changes; no `contrib/**`, no `.github/**`, no never-touch path. No
  `.gitmodules` change (clause (vi) does NOT fire).
- 8 Behavior under settings: `n/a` — not gated by a setting.

## 4. Test design

(a) New integration test authored; **the evidence-of-causation pair could NOT
be produced on this LTS** → escalated `test_design_blocked`.

- Test path (staged, design-blocked):
  `tests/integration/test_aiven_replicate_move_partition_through_database_replicated/test.py`
  + `configs/config.xml`.
- Transport: INTEGRATION (parent policy call 4) — the observable ("the move
  reached the replica that did NOT receive the client query") is genuinely
  cluster-level and unreachable from a single-server stateless test.
- Topology: two replicas of one shard (`main_node` shard1/replica1, `dummy_node`
  shard1/replica2) + Keeper, modeled on `test_replicated_database`.

**Scenario.** `CREATE DATABASE moved ENGINE = Replicated(...)` on both nodes;
`src`/`dst` tables with `PARTITION BY p`; `INSERT` 2 rows into partition 1 and
1 row into partition 2 on `main_node`; `ALTER TABLE moved.src MOVE PARTITION 1
TO TABLE moved.dst` on `main_node`; assert on `dummy_node` that
`count(dst)==2` and `count(src)==1`.

**Why the pair could not be produced (clause-(iv) confounder).**

- A `Replicated` database does NOT auto-convert plain `MergeTree` to
  `ReplicatedMergeTree` (confirmed by `test_replicated_database::test_simple_alter_table`,
  whose `SHOW CREATE` for `engine=MergeTree` shows plain `MergeTree`). With plain
  `MergeTree` tables the INSERTed data never reaches `dummy_node.src`, so a
  replayed `MOVE` has nothing to move there — `dst` stays 0 BOTH pre- and
  post-patch (observed: first post-patch run FAILED with `'2' != '0'`,
  `tmp/patch-009/test-postpatch.log` first iteration). No distinction.
- Switching `src`/`dst` to `ReplicatedMergeTree` (so the data IS present on both
  replicas via table-level replication) makes the **post-patch test PASS**
  (`tmp/patch-009/test-postpatch.log`, `1 passed`) — but **also makes the
  pre-patch test PASS** (`tmp/patch-009/test-prepatch.log`, `1 passed`, genuine
  pre-patch binary: build step `[13/18] Building ... DatabaseReplicated.cpp.o`,
  worktree `MOVE_PARTITION` absent). On 26.3, **table-level `ReplicatedMergeTree`
  replication propagates the cross-table `MOVE PARTITION TO TABLE`** to the other
  replica regardless of the DDL-log routing. This directly contradicts the
  patch's premise that `MOVE PARTITION TO TABLE` is not handled by RMT's own
  replication.
- Pre-patch run output (the expected FAIL — but it PASSED):

  ```text
  test_move_partition_replicated_through_database PASSED
  ======================== 1 passed, 3 warnings in 12.90s ========================
  ```

- Post-patch run output (the PASS):

  ```text
  test_move_partition_replicated_through_database PASSED
  ======================== 1 passed, 3 warnings in 12.54s ========================
  ```

- Why this test fails to distinguish the Aiven behavior from upstream: the data
  observable (rows on the non-receiving replica) converges via table-level RMT
  replication on this base whether or not the move is routed through the DDL
  log. A distinguishing test would need an observable that table-level
  replication does NOT also satisfy (e.g. inspecting the database DDL log /
  `system.distributed_ddl_queue` for the `MOVE` entry, or a topology where
  table-level replication is absent yet data is present on the target replica).
  Designing that is a parent/human adjudication → `test_design_blocked` (decoupled
  fallback, `integration-tests.md §7.3`).

## 5. Rollback considerations

- Revert safety: SAFE. The change only flips a routing predicate; no schema
  migration, no on-disk format change.
- Persistent state: the only new effect is that a `MOVE PARTITION` would be
  written into the database DDL log (a ZK entry). Reverting stops new such
  entries; already-replayed moves are ordinary data and survive.
- Disable without rebuild: no setting gates the behavior (none added).

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — not carried on 25.3.

### 25.8-aiven (historical, may be empty)

Original carry as `110900c986af8128279f1b7332a4cd265bed44b9` (K&R braces, quoted
include). No dossier from that uplift.

### 26.3-aiven (this uplift)

- Cherry-pick was: clean (auto-merge, hunks at offset +8 / +~235; no conflict),
  then restyled per parent policy calls 1 & 2 (K&R→Allman, quoted→angle include
  regrouped into the `Storages/` block, redundant `else` removed,
  `auto const`→`const auto`). `byte_equivalent: false` (intended).
- Upstream-drift conclusion: `still-needed-but-rewrite` (source applies; NOT
  obsoleted by upstream).
- Test added at: `tests/integration/test_aiven_replicate_move_partition_through_database_replicated/`
  — staged but **design-blocked**: no evidence-of-causation pair on 26.3 (both
  pre- and post-patch PASS; see §4). ESCALATED `test_design_blocked`.
- Build: warm-cache, `ninja -C build clickhouse` (post-patch ~55s, pre-patch
  flip ~32s, restore ~25s), all exit 0; single-file code-only patch, no new TU.
- Time-to-port: subagent wall-clock ~30 min (warm-cache).
- Anything surprising: the patch's data-consistency premise — that
  `MOVE PARTITION TO TABLE` is not propagated by `ReplicatedMergeTree`'s own
  replication — does not reproduce on 26.3 for same-cluster RMT tables; the move
  converges across replicas via table-level replication regardless of the
  DDL-log routing. Whether the patch is still needed (e.g. for a non-RMT or
  cross-cluster case) is a parent/human call.

# Patch 009 — replicate-move-partition-through-database-replicated

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | n/a | n/a | (not carried on 25.3) |
| 25.8-aiven | `110900c986af8128279f1b7332a4cd265bed44b9` | Tilman Moeller (committed by Aliaksei Khatskevich) | original carry |
| 26.3-aiven | **DROPPED — `not-justified` (net-negative on single-shard RMT). No commit.** | parent agent + human, 2026-06-15 | cherry-pick was clean, but analysis showed: leader-only execution ⇒ no data benefit over table replication (data-loss justification RETRACTED, §1b CORRECTION), while routing through the DDL log amplifies a benign `MOVE` failure into a DB-DDL-queue stall + forced recovery (§6). Residual risk: original incident not located — see §6 handover. |

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

## 1b. Finding (2026-06-15): the real justification, after deeper analysis

> **CORRECTION (2026-06-15, after the routing test ran) — parts (b) and (c)
> below, and the KEEP conclusion, are RETRACTED.** They assumed that routing the
> move through the DB DDL log makes **every** replica run `movePartitionToTable`
> locally. That is **false** for a single-shard `ReplicatedMergeTree`. Verified
> from source and runtime:
>
> - `DDLWorker::taskShouldBeExecutedOnLeader` (`src/Interpreters/DDLWorker.cpp:807-818`)
>   excludes only `isMovePartitionToDiskOrVolumeAlter` (MOVE TO **DISK/VOLUME**),
>   **not** `MOVE ... TO TABLE`. `isMovePartitionToDiskOrVolumeAlter` matches only
>   `move_destination_type ∈ {DISK, VOLUME}` (`ASTAlterQuery.cpp:658-670`), so a
>   TO-TABLE move falls through to `return storage->supportsReplication()` →
>   `true` → `task.execute_on_single_replica = true` (`DDLWorker.cpp:730`).
> - At runtime the follower logs `Task query-… has already been executed by
>   replica (shard1|replica1) of the same shard` (`tryExecuteQueryOnSingleReplica`,
>   `DDLWorker.cpp:850-855`) and **skips local SQL execution**.
>
> Consequence: with OR without the patch, exactly **one** replica per shard runs
> `movePartitionToTable`; every peer converges via table-level
> `REPLACE_RANGE`/`DROP_RANGE` replication. Therefore the patch does **not**
> change the peer convergence path, does **not** make peers clone from their
> local `src`, does **not** fix the `obtain_part` ordering hazard (which is
> identical both ways), and introduces **no** cross-replica
> `alter_partition_version` contention (only the single leader bumps it, as
> before). Part (a) (archaeology) still stands. The verified residual effect of
> the patch on single-shard `ReplicatedMergeTree` is narrow: the move is
> **serialized in the DB DDL log** (globally ordered vs. other DDL; subject to
> DDL-log consensus/retry) instead of executing immediately in the client
> session on the receiving replica. **Outcome: DROPPED — `not-justified`**
> (net-negative: marginal ordering benefit vs. a real DDL-queue failure-
> amplification regression). See §4c and §6.

The original commit body's premise — *"MOVE PARTITION commands were not
replicated … causing data inconsistency"* — is **imprecise on 26.3**. The first
test pass (§4) showed the move *does* converge across replicas via table-level
replication, which made the patch look redundant. A parent investigation
established a more accurate picture that supersedes the "premise does not
reproduce / maybe not needed" reading:

**(a) History — `MOVE PARTITION` routing is governed by a 2023 upstream
optimization, not a deliberate exclusion.**

- Before upstream `353b57f13ff` ("an optimiation for alters and replicated db",
  Alexander Tokmakov, 2023-06-15), `DatabaseReplicated::shouldReplicateQuery`
  returned `true` (route through the DDL log) for *every* `ALTER` except the
  explicit `isAttachAlter()/isFetchAlter()/isDropPartitionAlter()/keepermap`
  set. So `MOVE PARTITION` was DDL-logged for **every** Replicated DB,
  single-shard included.
- `353b57f13ff` added the single-shard carve-out present today: the
  `has_many_shards() || !is_replicated_table()` gate plus the `for`-loop that
  routes only *metadata* alters through the DB and returns `false` for
  *"ALTER PARTITION or mutation … doesn't involve database"*. Its premise: on a
  single-shard replicated table, partition/mutation ops are handled by the
  table's own replication, so skip the DDL log.
- Therefore `MOVE PARTITION` is **not** "deliberately kept out" of the
  exclusion list — it was DDL-logged by default pre-2023, and the 2023
  optimization stripped DDL-logging from *all* single-shard partition ops via
  the broad gate. There is **no `MOVE`-specific upstream intent** to cite either
  way (verified by `git log -S` on `isDropPartitionAlter` / `has_many_shards`;
  both lines land in upstream Tokmakov commits, no Aiven authorship).

**(b) Mechanism — the 2023 optimization's premise has a hole for cross-table
moves.**

- The premise holds cleanly for `DROP`/`ATTACH PARTITION`: each is a single,
  self-contained, idempotent table-log entry.
- `MOVE PARTITION TO TABLE` is **two** entries across **two** tables — a
  `REPLACE_RANGE` on `dst`'s log and a `DROP_RANGE` on `src`'s log
  (`StorageReplicatedMergeTree::movePartitionToTable`, ~lines 9313 / 9326) —
  living in independent table queues, pulled independently on followers.
- When a follower materializes the `dst` `REPLACE_RANGE`, `obtain_part`
  (`executeReplaceRange`, ~line 3219) clones the moved part from its **local
  `src`** if present, else fetches the new `dst` part from a remote replica,
  else **throws** `"Not found part … neither source table neither remote
  replicas"` (~line 3179). If a follower applies `src`'s `DROP_RANGE` *before*
  `dst`'s `REPLACE_RANGE` and the `dst` part is no longer fetchable (merged or
  cleaned on the initiator), the move **fails / the partition is lost on that
  follower**. Upstream itself flags this path as fragile
  (`"Yes, MOVE PARTITION is trash"`, ~line 9388).
- Routing the move through the DDL log makes **every replica run
  `movePartitionToTable` locally**, cloning from its own `src` parts at
  execution time — sidestepping the cross-replica `REPLACE_RANGE`-sourcing
  ordering hazard. **This is the patch's real justification:** it exempts
  `MOVE PARTITION TO TABLE` from the 2023 single-shard optimization because the
  optimization's premise (table replication suffices) is leaky for cross-table
  moves. The data-loss window is narrow (needs the bad ordering *and* the `dst`
  part gone from the initiator) but the consequence is severe (silent
  inconsistency or a stalled replica queue).

**(c) Cost — bounded contention, recorded as a known trade-off.**

- With the patch each replica re-executes the move and they contend on the
  source `alter_partition_version` node (`movePartitionToTable`, ~line 9240),
  whose check **throws `CANNOT_ASSIGN_ALTER`** (no retry at that checkpoint)
  when a concurrent execution bumped the version. The DDL worker retries the
  entry; on retry the source partition is already gone → the re-execution is a
  no-op → success. Redundant `dst` parts dedup by content-hash `block_id`
  ("already attached", ~line 9277) → **no data duplication**.
- Tail risk: a replica accumulating
  `subsequent_errors_count >= max_retries_before_automatic_recovery`
  (`DatabaseReplicatedWorker.cpp:100-110`) resets its digest and triggers full
  recovery. A single move converges well inside that bound; only a sustained
  storm of concurrent moves on the same partition could approach it (not an
  expected workload).

**Conclusion / decision (2026-06-15): KEEP.** A real (if narrow) data-loss /
stuck-queue path is closed at a low, bounded contention cost on Aiven's
single-shard HA topology. The earlier "make single-shard consistent with
multi-shard" framing is dropped in favor of the premise-gap framing above
(consistency is a side effect, not the reason). Confidence is "well-argued from
source," not "reproduced": a deterministic reproduction of the data-loss race
would require a `FailPoint` forcing `DROP_RANGE`-before-`REPLACE_RANGE` (a
stretch goal, not required to land). The shipped test therefore proves the
patch's *mechanism* (DDL-log routing) deterministically — see §4.

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
  (The commit body's "consistency" *premise* does not reproduce as a data
  observable on this base — see §4a — but §1b re-establishes the real
  justification from source: closing the cross-table `MOVE PARTITION` data-loss
  race left open by upstream's 2023 single-shard optimization. The source change
  is structurally sound and applies cleanly.)

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

Transport: INTEGRATION (parent policy call 4) — the observable concerns what
happens on the replica that did NOT receive the client query, which is
genuinely cluster-level and unreachable from a single-server stateless test.
Topology: two replicas of one shard (`main_node` shard1/replica1, `dummy_node`
shard1/replica2) + Keeper, modeled on `test_replicated_database`. Path:
`tests/integration/test_aiven_replicate_move_partition_through_database_replicated/`.

### 4a. First attempt (data-convergence observable) — BLIND, abandoned

The first test asserted, on `dummy_node`, that after
`ALTER TABLE moved.src MOVE PARTITION 1 TO TABLE moved.dst` on `main_node`,
`count(dst)==2` and `count(src)==1` (with `src`/`dst` as `ReplicatedMergeTree`).
It **passed both pre- and post-patch** (`tmp/patch-009/test-{pre,post}patch.log`;
genuine pre-patch binary confirmed by the `[13/18] … DatabaseReplicated.cpp.o`
rebuild with `MOVE_PARTITION` absent from the worktree):

```text
# pre-patch (expected FAIL — but PASSED)
test_move_partition_replicated_through_database PASSED
======================== 1 passed, 3 warnings in 12.90s ========================
# post-patch (PASS)
test_move_partition_replicated_through_database PASSED
======================== 1 passed, 3 warnings in 12.54s ========================
```

Reason (see §1b(b)): on the happy path, table-level `ReplicatedMergeTree`
replication converges the cross-table move regardless of DDL-log routing, so the
data observable is **blind** to the routing change the patch makes. (A plain
`MergeTree` variant fails both ways instead: a `Replicated` database does NOT
auto-convert `MergeTree`→`ReplicatedMergeTree` — confirmed by
`test_replicated_database::test_simple_alter_table` — so the data never reaches
`dummy_node.src` and the move has nothing to move; observed first post-patch run
`'2' != '0'`.) This attempt is retained only as the lesson: **do not assert an
outcome that two independent mechanisms can both produce.**

### 4b. Chosen design (DDL-routing observable) — deterministic

Assert the patch's *direct mechanical effect*: the move is replayed on the
non-receiving replica as a secondary query iff it was routed through the DB DDL
log. With `src`/`dst` as `ReplicatedMergeTree`:

- Issue `ALTER TABLE moved.src MOVE PARTITION 1 TO TABLE moved.dst` on
  `main_node`.
- `SYSTEM FLUSH LOGS` on `dummy_node`; query its `system.query_log` for a
  `MOVE PARTITION` `ALTER` with `is_initial_query = 0` (a `SECONDARY_QUERY`
  replayed from the DB DDL log).
  - **Post-patch:** present (the move was enqueued in the DDL log and replayed
    on `dummy_node`).
  - **Pre-patch:** absent (`dummy_node` only applies table-replication log
    entries — `DROP_RANGE`/`REPLACE_RANGE` — never the SQL query).
- Deterministic FAIL-pre / PASS-post. This proves the routing change (the
  patch's mechanism); §1b carries the data-loss causation that motivates the
  routing. If `query_log` proves flaky, the equivalent observable is the `MOVE`
  entry in `system.distributed_ddl_queue` (or the DB's ZK `log` path).

Stretch (not required to land): a `FailPoint` forcing `DROP_RANGE`-before-
`REPLACE_RANGE` on a follower would let the data-loss race itself be the
observable (pre-patch loses the partition, post-patch does not). Deferred unless
a suitable failpoint already exists around `executeReplaceRange`.

### 4c. Evidence pair — BLOCKED AGAIN (and why it matters)

The routing observable (§4b) **also failed to produce a pair** — post-patch the
count was `0`, not `>= 1` (`tmp/patch-009/test-postpatch.log`). Root cause is the
same fact that retracts §1b(b)/(c): on a single-shard `ReplicatedMergeTree`,
`MOVE ... TO TABLE` is a **leader-only** DDL task
(`taskShouldBeExecutedOnLeader` → `execute_on_single_replica`), so the follower
**processes** the DB DDL entry but **skips the SQL** (`… has already been
executed by replica (shard1|replica1) …`) and converges via table replication.
The replayed SQL therefore never lands in the follower's `system.query_log`.

Both observables tried so far — data convergence (§4a) and `query_log` SQL
replay (§4b) — are **blind for the same reason**: on single-shard, the patch
does not add a second SQL execution, only DB-DDL-log serialization of the one
execution. A distinguishing test must observe the *serialization itself*, not a
per-replica data/query effect. Candidate observables (not yet attempted):

- The `MOVE` entry in the DB's DDL log / `system.distributed_ddl_queue`
  (presence post-patch, absence pre-patch) — observes routing directly.
- A **two-shard** topology, where a leader-only DDL DOES execute on a
  non-initiator node, restoring a `query_log` signal.
- The narrow consistency win (DDL-vs-MOVE global ordering) via an interleaving
  with a concurrent DB-DDL `ALTER` — harder to make deterministic.

Pre-patch run was not performed (escalation triggered on the post-patch
failure). Source untouched (`git diff src/Databases/DatabaseReplicated.cpp`
empty); the routing `test.py` attempt is in the worktree **unstaged** (a
non-working test was deliberately not staged).

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
- Test at: `tests/integration/test_aiven_replicate_move_partition_through_database_replicated/`
  — first observable (data convergence) was **blind** (passed both pre/post);
  re-scoped to the **DDL-routing observable** (§4b), a deterministic
  FAIL-pre/PASS-post pair. Authoring of the re-scoped test is a separate
  decoupled dispatch.
- Build: warm-cache, `ninja -C build clickhouse` (post-patch ~55s, pre-patch
  flip ~32s, restore ~25s), all exit 0; single-file code-only patch, no new TU.
- Time-to-port: subagent wall-clock ~30 min (warm-cache).
- Decision: **DROP — `not-justified` (net-negative on the target topology)**
  (2026-06-15, human call). On single-shard `ReplicatedMergeTree` — the *only*
  case the patch affects, and Aiven's actual topology — the patch is net-
  negative:
  - **No correctness benefit.** `MOVE … TO TABLE` is a leader-only DDL task
    (`taskShouldBeExecutedOnLeader`), so exactly one replica executes it and
    peers converge via table-level replication **with or without** the patch.
    The data-loss-race justification was retracted (§1b CORRECTION). The only
    residual effect is DB-DDL-log serialization of the move (a narrow ordering
    property, not the commit body's claimed data-consistency fix).
  - **New regression (the decisive factor).** Routing the move through the DDL
    log changes its failure semantics. Without the patch a failed
    `MOVE … TO TABLE` (a known-fragile op — `alter_partition_version`
    contention, source partition changed, `obtain_part` "Not found",
    destination conflict) is an isolated **client-visible error** with no side
    effects. With the patch that same failure is a DDL-log entry that **throws
    on every retry**, **head-of-line blocks the entire database DDL queue**, and
    once `subsequent_errors_count >= max_retries_before_automatic_recovery`
    forces the replica to **reset its digest and re-sync from ZooKeeper**
    (`DatabaseReplicatedWorker.cpp:100-117`: *"Database got stuck at processing
    task … Will reset digest … and trigger recovery"*). A benign retryable error
    is amplified into a queue-stalling, recovery-triggering, DB-wide event.

  **Residual risk of dropping + git-history sweep (2026-06-15).** The patch
  ships in the running 25.8-aiven stack, so dropping changes today's behavior. A
  git sweep for the originating incident found:
  - **No tracker / PR / incident reference anywhere in the lineage** (searched
    `AIV-` / jira / github / `/pull/` / `/issues/` / `#NNNN` / incident / RCA
    across `110900c986…` and neighbours — none).
  - The change traces to the patch file **`0075-Replicate-move-partition.patch`**
    first committed as `d8f0e8ffefd` ("Replicate more ALTER TABLE MOVE PARTITION
    queries.", Aris Tritas, 2024-12-03, co-authored by Joe Lynch) and
    **repeatedly re-applied across lineages** (multiple identical-subject SHAs);
    `110900c986…` (Tilman Moeller, 2025-12-07) is its re-application to 25.8.
  - The written rationale across the whole lineage is the **falsified leader-only
    premise** itself: *"We require these queries to be replicated to all nodes of
    the database"* and *"all replicas execute the operation atomically"*. As §1b
    establishes, routing to the DDL log puts the entry on all nodes but only the
    **leader executes** — so the stated guarantee was never actually delivered by
    this patch, with or without it.

  Conclusion: git provides **no evidence of a concrete incident** and the
  documented intent rests on the premise we disproved. The only place a real
  incident could still live is Aiven's **internal tracker / Slack** (not
  in-repo). **Recommended de-risk before release:** a quick Jira/Slack check for
  a `MOVE PARTITION` data-inconsistency report; if none exists, the drop is
  unconditionally safe. Handover item, not a blocker.

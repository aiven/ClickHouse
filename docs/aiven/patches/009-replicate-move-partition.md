---
description: 'Routes ALTER TABLE ... MOVE PARTITION through the Replicated database queue, so that moves to another disk or volume run on every replica'
sidebarTitle: '009: replicate MOVE PARTITION'
slug: '/aiven/patches/009-replicate-move-partition'
title: 'Patch 009: replicate MOVE PARTITION through the Replicated database'
doc_type: 'reference'
aiven_identity: '009'
aiven_absorbs: []
aiven_changelog_category: 'Improvement'
aiven_settings: []
---

# Patch 009 — replicate `MOVE PARTITION` through the `Replicated` database {#patch-009-replicate-move-partition}

## Summary {#summary}

In a `Replicated` database with one shard, `ALTER TABLE ... MOVE PARTITION` goes through the database queue
instead of running only in the session of the server that received it. A move `TO DISK` or `TO VOLUME`
then runs on every replica, so `ALTER TABLE t MOVE PARTITION p TO VOLUME 'remote'` frees local disk on all
replicas, not on one. Other moves (`TO TABLE`, `TO SHARD`) are queued too and still run on one replica.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 23.8, 24.3 | package patch set | original, written for 23.8 |
| 25.3 | `d8f0e8ffefda` | applied from the package patch |
| 25.8 | `110900c986af` | re-applied |
| 26.3 | `bb89b308c8ab` | dropped on `.10.62`, restored on `.15.4` with an integration test |
| 26.8 | `patch-port(009)` | `still-needed`, ported as on 26.3 |

The 26.3 notes said 25.3 did not carry the patch; it did.

## Background (ClickHouse) {#background}

A `Replicated` database replicates two different things:

- **The database queue** (a log in Keeper) carries DDL as SQL. Every replica replays every entry in
  order. An entry can never be skipped, because the order keeps the metadata identical on all replicas.
- **The `ReplicatedMergeTree` table log** carries data and partition operations (inserts, merges,
  `DROP_RANGE`, `REPLACE_RANGE`). A move to another disk or volume is local to one replica and is not in
  this log.

`DatabaseReplicated::shouldReplicateQuery` decides which `ALTER`s go through the database queue. Until 23.3
almost all of them did. Upstream `353b57f13ff` (23.6) added a shortcut: with one shard and a replicated
table, only metadata `ALTER`s are queued; partition commands and mutations run in the session, on the
reasoning that the table log replicates them. That holds for `DROP` or `ATTACH PARTITION`, not for a move to
another disk.

A queued entry is then run either on every replica or on one leader replica.
`DDLWorker::taskShouldBeExecutedOnLeader` decides; `ASTAlterQuery::isMovePartitionToDiskOrVolumeAlter`
(all commands are moves to a disk or volume) is one of the exceptions that run on every replica.

Upstream documents moves to a disk or volume as not replicated, because replicas can have different storage
policies (upstream issue [#46464](https://github.com/ClickHouse/ClickHouse/issues/46464) asks for it). On
the managed service every replica has the same policy.

## Component tour {#component-tour}

- **`DatabaseReplicated::shouldReplicateQuery`** (upstream). In the single-shard branch, a command that
  `PartitionCommand::parse` reads as `MOVE_PARTITION` makes the whole `ALTER` go through the database queue,
  like a metadata command.

## Problem {#problem}

Without the patch, `ALTER TABLE t MOVE PARTITION p TO VOLUME 'remote'` on a single-shard `Replicated`
database moves the parts on the replica that received the query. The other replicas keep theirs on local
disk; the user sees the move succeed and believes the partition is in object storage. Background moves
(`move_factor`, TTL) run on every replica anyway; only the explicit move diverges.

## Decisions {#decisions}

### Port the 26.3 predicate {#decision-predicate}

- **Chosen:** route every `MOVE_PARTITION`, as since 23.8.
- **Rejected:** route only moves to a disk or volume (`isMovePartitionToDiskOrVolumeAlter`). Every query
  where the two differ (`TO TABLE`, `TO SHARD`, a batch that mixes a move with other commands) is a
  leader-only task, so it runs on one replica either way and the data ends up the same. The narrow form
  would keep those moves out of the queue; it would also change behaviour that has run in production
  since 23.8.
- **Rung:** 5, routing in an upstream path.

### Keep the move synchronous {#decision-sync}

- **Chosen:** the initiator waits for all replicas (`distributed_ddl_task_timeout`), as before.
- **Rejected:** forcing `alter_move_to_space_execute_async`. The move would only be scheduled, and errors
  on any replica would no longer reach anyone.

### Keep failed replays fatal for the replica {#decision-failed-replay}

- **Chosen:** upstream's rule for database entries, unchanged (see Approach).
- **Rejected for this port:** letting a failed replay of a pure disk or volume move write an error status
  instead. Skipping a move cannot reorder metadata, so it is a sound follow-up, but it changes an upstream
  hot path and needs its own review.

## Approach {#approach}

A queued move runs on the initiator first; only if it succeeds is the entry committed for the other
replicas. The replay on another replica can still fail on that replica's own state:

- `UNKNOWN_DISK`: the parts are already on the volume (its background move got there first);
- `NO_SUCH_DATA_PART`: it has not fetched the partition yet;
- `ABORTED`: moves are stopped there (`SYSTEM STOP MOVES`);
- `PART_IS_TEMPORARILY_LOCKED`: zero-copy, another replica is moving the same parts.

A failed database entry is not skipped: that replica retries it every few seconds and blocks its database
queue, so later DDL waits. After `max_retries_before_automatic_recovery` (10) identical failures it marks
itself lost and recovers from Keeper, which skips the move; its parts stay where they were. The client gets
`TIMEOUT_EXCEEDED` after `distributed_ddl_task_timeout`, although the move succeeded on the initiator. This
has been the behaviour since 23.8.

A long move (a large partition to object storage) holds the database queue on each replica while it runs,
and the client times out after `distributed_ddl_task_timeout`; the move continues.

With zero-copy replication, the replay on the other replicas links the data the first replica uploaded
instead of uploading it again.

A batch that mixes a move to a volume with another command is leader-only, so the move to the volume runs on
one replica. Split such batches.

## Concept {#concept}

A `Replicated` database answers two questions with two functions: whether a query goes through the queue,
and who runs it. Changing the first cannot change the second, which is why this patch never made
`TO TABLE` run on every replica. A queued entry also inherits the queue's failure rules: it must succeed
on every replica, or that replica stalls and recovers.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed`

- `shouldReplicateQuery` on 26.8 is the 26.3 function plus 054's `KeeperMap` settings routing; the
  single-shard branch is unchanged. Upstream has no equivalent.
- The include list changed; the only conflict was there.
- `taskShouldBeExecutedOnLeader` and the failure handling for database entries are unchanged.

## Customer impact {#customer-impact}

- **Who:** services that move partitions by hand between local disk and object storage.
- **What they see:** the same as on 26.3. A move to a disk or volume applies to every replica. If it fails
  on a replica that is not the initiator, the client gets a timeout, DDL in that database waits for about a
  minute on that replica, and that replica's copy is not moved.
- **Existing tables:** not affected; the patch adds no state.
- **How to restore upstream behaviour:** none without rebuild.
- **Upstream tests edited:** none.

## Security review {#security-review}

No new grants or paths: the queued query is the client's query, checked with the client's grants on the
initiator and run as an internal replay elsewhere, like every database entry. The remaining risk is
availability: a move that fails on one replica delays DDL in that database on that replica until it
recovers. The delay is bounded by `max_retries_before_automatic_recovery` and is the same as on 25.8 and
26.3.

## Tests {#tests}

- **Paths:** `tests/integration/test_aiven_move_partition_to_volume_replicated/`
- **Proves** (two replicas of one shard, tiered policy with `move_factor = 0`):
  - `TO VOLUME` and `TO DISK` from node1 move the part on both replicas and add one entry to the database
    log.
  - `TO TABLE` adds one entry and runs once; both replicas end with the rows in `dst`.
  - A batch with `TO VOLUME` and `TO TABLE` is queued and moves the part on one replica.
  - With moves stopped on node2, the client gets `TIMEOUT_EXCEEDED`, node2 recovers as a lost replica, a
    later `CREATE TABLE` reaches it, and node2's part stays on the default disk.
- **Run:** `python -m ci.praktika run "integration" --test test_aiven_move_partition_to_volume_replicated`
- **Evidence (2026-11-16):**
  - RED on the parent commit `11104798e347`: all five fail. node2's part stays on `default` after
    `TO VOLUME` and `TO DISK`; the database log has no move entry; the failing move returns success.
  - GREEN with the patch: five passed.

## Rollback {#rollback}

- **Downgrade:** not supported; services only move forward.
- **Revert in a later release:** safe; nothing is stored. Moves run in the session again, on one replica.
- **Disable without rebuild:** not possible.

## Retirement {#retirement}

When upstream replicates moves to a disk or volume in `Replicated` databases, for example behind a
database setting for services whose replicas share a storage policy (#46464). That, with a failed replay
of a move that does not block the queue, is an upstream candidate.

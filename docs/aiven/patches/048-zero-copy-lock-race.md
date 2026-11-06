---
description: 'Fix an insert failing with LOGICAL_ERROR when two shards commit a zero-copy part with the same name at the same time'
sidebarTitle: '048: Zero-copy lock race'
slug: '/aiven/patches/048-zero-copy-lock-race'
title: 'Patch 048: Zero-copy lock race on part commit'
doc_type: 'reference'
aiven_identity: '048'
aiven_absorbs: []
aiven_changelog_category: 'Bug Fix'
aiven_settings: []
---

# Patch 048 — Zero-copy lock race on part commit {#patch-048-zero-copy-lock-race}

## Summary {#summary}

With zero-copy replication, the shards of a table in a `Replicated` database keep their
part locks under one Keeper node per part name. When two shards commit a part with the same name
at the same time, typically the first part of a partition, the second commit fails to create that
node and the insert aborts with a `LOGICAL_ERROR` exception that claims no race is possible. The
patch creates the node idempotently before the commit, so both inserts succeed. Nothing changes
for tables without zero-copy replication.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.8 | `c6d9323ba2` | two changes: this commit fix, and a `dropAllData` change for tables stuck on drop |
| 26.3 | `patch-port(048)` `481f436ced91` | commit fix only; the `dropAllData` change was dropped because upstream `25b0406c35c` fixed the stuck drop while keeping shared data |
| 26.8 | `patch-port(048)` | `still-needed`: unchanged |

## Background (ClickHouse) {#background}

With `allow_remote_fs_zero_copy_replication`, replicas share the objects of a part in object
storage, and each replica records that it uses them with a lock node in Keeper:
`<remote_fs_zero_copy_zookeeper_path>/zero_copy_<disk>/<table shared id>/<part name>/<blob id>/<replica>`.
The object storage is only cleaned when the last lock under a blob id is gone.

`ReplicatedMergeTreeSink::commitPart` adds the creation of this lock to the Keeper transaction
that commits a new part. `ZooKeeper::checkExistsAndGetCreateAncestorsOps` first reads which
ancestors exist, then adds a plain create for each missing one to the transaction. If any lock
request fails, the sink throws `LOGICAL_ERROR`: "Creating shared lock for part ... It's a bug. No
race is possible since it is a new part."

## Component tour {#component-tour}

- **`ReplicatedMergeTreeSink::commitPart`** (upstream). After the part gets its name, and when
  zero-copy applies to the part, creates the `<part name>` node for every zero-copy path with
  `createAncestors` and `createIfNotExists`.

## Problem {#problem}

The table shared id is the table's UUID, which is the same on every shard of a table in a
`Replicated` database. Two shards committing a part with the same name, for example `all_0_0_0`
during `SYSTEM RESTORE REPLICA` or the first insert after creating the table, both see
`<part name>` missing and both add a create for it; the second transaction fails with
`ZNODEEXISTS`. The `<blob id>` below differs per shard, because the shards store different data,
so `<part name>` is the only node they share.

## Decisions {#decisions}

### Port unchanged {#decision-port}

- **Chosen:** the 26.3 change as is.
- **Rejected:** creating the node inside the transaction with Keeper's create-if-not-exists
  request. It needs a Keeper feature flag, does not work with ZooKeeper, and would change
  `checkExistsAndGetCreateAncestorsOps` for every caller.
- **Rejected:** retrying on a failed lock request. That error is meant to catch real bugs.
- **Rung:** 5, a change in upstream's commit path.

## Approach {#approach}

The node is created before the transaction's requests are built, so the existence check finds it
and the transaction only creates the per-shard nodes below it, which no other shard creates.
Creating it outside the transaction is safe: an empty `<part name>` node holds no lock and
protects no object.

## Concept {#concept}

"Check, then create" between independent writers is a race. Create shared nodes idempotently and
keep the strict "must not exist" creates for nodes that only one writer owns.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed`

- `checkExistsAndGetCreateAncestorsOps`, the lock path layout and the sink's error are unchanged;
  the anchor after `part->setName` in `commitPart` is unchanged.
- Upstream `25b0406c35c` is still in place, so the 25.8 `dropAllData` change stays dropped.

## Customer impact {#customer-impact}

- **Who:** services with zero-copy replication (on by default in the managed configuration) and
  tables sharded in a `Replicated` database.
- **What they saw before:** an insert, or the inserts of `SYSTEM RESTORE REPLICA`, failing with

```text
Code: 49. DB::Exception: Creating shared lock for part all_0_0_0 has failed with error: Node exists.
It's a bug. No race is possible since it is a new part. (LOGICAL_ERROR)
```

- **Now:** both inserts succeed.
- **Remaining:** if another shard removes the last lock of an older part with the same name
  between the early create and the commit, the commit can still fail the same way. That needs an
  old and a new part of the same name on two shards at the same moment.
- **How to restore upstream behaviour:** none needed; no setting.
- **Upstream tests edited:** none.

## Security review {#security-review}

No new input or privilege. The node is created under the table's own zero-copy path, with the
same Keeper session and permissions as the transaction that follows.

## Tests {#tests}

- **Paths:** `tests/integration/test_aiven_zero_copy_lock_race/`
- **Proves:** two shards of a `Replicated` database on zero-copy S3 insert their first part at
  the same time, with the `rmt_delay_commit_part` failpoint holding both between the existence
  check and the commit; both inserts succeed, each shard has `all_0_0_0`, and the shared part node
  holds both shards' locks.
- **Run:** `python -m ci.praktika run "integration" --test test_aiven_zero_copy_lock_race`, or
  `pytest` from `tests/integration` with `CLICKHOUSE_TESTS_SERVER_BIN_PATH` set.
- **Evidence (2026-11-06):**
  - RED on the parent commit `5914cefcc99a`: the test fails. The second shard's insert hits the
    `LOGICAL_ERROR` `Creating shared lock for part all_0_0_0 has failed with error: Node exists. It's a bug.`
    The local build aborts on logical errors; a release build throws an exception and the insert fails.
  - GREEN with the patch: 1 passed, three runs in a row.

## Rollback {#rollback}

- **Downgrade:** not supported; services only move forward.
- **Revert in a later release:** no format of its own; the race comes back.
- **Disable without rebuild:** not possible; it only acts with zero-copy replication.

## Retirement {#retirement}

Upstream creating the shared zero-copy part node idempotently, or scoping zero-copy locks per
shard. Re-check upstream on every uplift; this is a good upstream contribution candidate.

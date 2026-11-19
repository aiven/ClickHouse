---
description: 'Dropped: ZooKeeper sorting-key metadata after ALTER ... MODIFY ORDER BY, a workaround for a problem upstream fixed in 24.9'
sidebarTitle: '060: ALTER MODIFY ORDER BY metadata (dropped)'
slug: '/aiven/patches/060-alter-order-by-sorting-key-zk-metadata'
title: 'Patch 060: Sorting-key metadata after ALTER MODIFY ORDER BY (dropped)'
doc_type: 'reference'
aiven_identity: '060'
aiven_absorbs: []
aiven_changelog_category: 'Bug Fix'
aiven_settings: []
---

# Patch 060 — Sorting-key metadata after `ALTER ... MODIFY ORDER BY` (dropped) {#patch-060-alter-order-by-sorting-key-zk-metadata}

## Summary {#summary}

Before 24.9, `ALTER TABLE ... MODIFY ORDER BY` with an unchanged key left a `ReplicatedMergeTree` table whose
definition and ZooKeeper metadata disagreed, so a replica created later from that definition, for example
by a restore, failed the metadata check. The patch made `ALTER` write the sorting key to ZooKeeper whenever
the table's primary key is explicit. Upstream [#67436](https://github.com/ClickHouse/ClickHouse/pull/67436)
fixed the cause in 24.9, and on 26.8 the patch changes nothing in any of the scenarios it was written for.
It is not carried on 26.8.

## Lineage {#lineage}

The two changed lines are the same in every version below.

| LTS | Commits | Outcome |
|---|---|---|
| 23.x | package patch set (2024-06) | original; its test covered an unchanged `MODIFY ORDER BY (key)` and `ADD COLUMN value, MODIFY ORDER BY (key, value)`, each with and without an explicit `PRIMARY KEY`, followed by a backup and restore |
| 24.3 | package patch set (2024-10), then `a0f453ef15d` | rebased, then moved into the fork unchanged |
| 24.8 | `3859263f613` | unchanged; 24.8 lacks the upstream fix, so still needed |
| 25.3 | `333361dd519` | unchanged; the upstream fix is in the base from here on |
| 25.8 | `93c2be960f9` | unchanged |
| 26.3 | `192665ce12b` | unchanged, without a test. The same commit also carries the code of patch `042` (dropped on 26.8) |
| 26.8 | `patch-drop(060)` | dropped, `irrelevant-by-upstream` |

## Background (ClickHouse) {#background}

A `ReplicatedMergeTree` table keeps its structure in the `metadata` znode. When a table has only `ORDER BY`,
the znode stores it in the `primary key:` line. When the table has an explicit `PRIMARY KEY`, the znode
stores the primary key there and the `ORDER BY` in a separate `sorting key:` line
(`ReplicatedMergeTreeTableMetadata`). A replica that attaches compares its local definition with the znode and
refuses to start on a mismatch.

`AlterCommands::apply` for `MODIFY ORDER BY` turns an implicit primary key into an explicit one: the old
`ORDER BY` becomes the `PRIMARY KEY`, so that the new `ORDER BY` may extend it. `StorageReplicatedMergeTree::alter`
writes the `sorting key:` line only when the sorting-key text changes.

## Problem {#problem}

Before 24.9 an unchanged `MODIFY ORDER BY (key)` was applied like any other: the table's definition gained an
explicit primary key, while `alter` wrote no `sorting key:` line because the text had not changed. The table's
definition and its znode then described the table differently, and a replica created from the definition the
first replica reported failed to attach. The patch wrote the `sorting key:` line whenever the primary key is
explicit, which kept the two in step.

Upstream #67436 (`16d7ff11be7`, merged 2024-09-02) fixes the cause: `AlterCommands::prepare` ignores a
`MODIFY ORDER BY` whose key equals the current one, so it no longer makes the primary key explicit.

## Decisions {#decisions}

### Drop the patch {#decision-drop}

- **Chosen:** do not carry it.
- **Rejected:** porting it as on 26.3. On 26.8 it is a no-op on every path measured (see Tests), and it would
  keep a fork change in `StorageReplicatedMergeTree::alter`.

## Concept {#concept}

A patch whose effect cannot be observed is either redundant or tested in the wrong scenario. Reproduce the
failure it was written for before deciding which: the 26.3 review analysed the predicate without the
original scenario, misread the purpose, and concluded that the patch's case was unknown.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `irrelevant-by-upstream`

- `16d7ff11be7` is an ancestor of every base since 25.3, including `v26.8.34.13-lts`.
- `AlterCommands::apply` still makes the primary key explicit for a `MODIFY ORDER BY` that is applied, and
  `alter` still writes `sorting key:` only when the text changes.

## Customer impact {#customer-impact}

- **Who:** nobody.
- **What changes against 26.3:** nothing observable. In each of the four original scenarios, the table's
  definition, its ZooKeeper metadata, a replica created from that definition and the first replica after
  re-attaching are the same with and without the patch.
- **Upstream tests edited:** none.

## Tests {#tests}

- **Evidence (2026-11-17):** a throwaway stateless probe of the four original scenarios, run on the parent
  commit and with the patch applied: identical output. In every case the second replica attached and the
  first replica was not read-only after `DETACH`/`ATTACH`. No test is added for a dropped patch.

## Security review {#security-review}

n/a — dropping the patch changes no access path.

## Rollback {#rollback}

- **Downgrade:** not supported; services only move forward.
- **Revert in a later release:** nothing to migrate. The patch wrote only the metadata line upstream also writes when the key changes.

## Retirement {#retirement}

Retired on 26.8.

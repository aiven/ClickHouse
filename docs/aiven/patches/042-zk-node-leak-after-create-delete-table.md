---
description: 'Dropped: removal of the empty parent znode after DROP TABLE, a minimal backport of upstream PR 69574 that upstream has shipped since 24.10'
sidebarTitle: '042: Empty parent znode after DROP TABLE (dropped)'
slug: '/aiven/patches/042-zk-node-leak-after-create-delete-table'
title: 'Patch 042: Remove the empty parent znode after DROP TABLE (dropped)'
doc_type: 'reference'
aiven_identity: '042'
aiven_absorbs: []
aiven_changelog_category: 'Bug Fix'
aiven_settings: []
---

# Patch 042 — Remove the empty parent znode after `DROP TABLE` (dropped) {#patch-042-zk-node-leak-after-create-delete-table}

## Summary {#summary}

When the last replica of a `ReplicatedMergeTree` table was dropped, ClickHouse before 24.10 removed the
table's znode but left its parent, `/clickhouse/tables/<uuid>` for the default path, as an empty znode in
ZooKeeper. The patch removed that parent too. It was written in 2024 as a minimal backport of upstream
[#69574](https://github.com/ClickHouse/ClickHouse/pull/69574), which fixes the same leak and is part of
every base since 25.3. It is not carried on 26.8.

## Lineage {#lineage}

The function body is the same in every version below; only the brace style changed on 26.3.

| LTS | Commits | Outcome |
|---|---|---|
| 24.3 | package patch set (2024-10) | original, "Minimal backport of #69574"; its test checked that `/clickhouse/tables/<uuid>` is gone after dropping a table with the default path. 24.3 lacks the upstream fix, so the patch was needed |
| 24.3 | `6bbf9839deb` | moved from the package patch set into the fork, unchanged |
| 24.8 | `ed708afc27e` | unchanged; 24.8 lacks the upstream fix, so still needed |
| 25.3 | `d895e89acfe` | unchanged; the upstream fix is in the base from here on, so the patch runs right after upstream's own cleanup and is redundant |
| 25.8 | `0d6eb5bece77` | unchanged code; the commit message was rewritten, dropping the reference to #69574 and describing the patch as complementing upstream's cleanup |
| 26.3 | `f56743cd153`, then `4e086aa3681` | braces only, plus a stateless test with a path without `{uuid}`. In the final cherry-pick the `.cpp` hunks (the function body and its call) landed in the commit of patch `060` (`192665ce12b`); the `042` commit holds only the declaration and the test |
| 26.8 | `patch-drop(042)` | dropped, `irrelevant-by-upstream` |

## Background (ClickHouse) {#background}

`StorageReplicatedMergeTree::removeTableNodesFromZooKeeper` runs when the last replica of a table is
dropped. It removes the table's znode and everything below it.

Upstream #69574 (`9a3adc70bde`, 24.10) adds `TableZnodeInfo::path_prefix_for_drop`. When the table's path
was built from the `{uuid}` macro, as the default `/clickhouse/tables/{uuid}/{shard}` is, then after a
complete removal `TableZnodeInfo::dropAncestorZnodesIfNeeded` also removes the ancestors that became empty,
up to and including the `{uuid}` component. It deliberately removes nothing above that component, because
those znodes may be shared with other tables. For a path without `{uuid}` it removes nothing.

The patch removed the immediate parent of the table path, whatever the path, after every call. For the
default path that is the `{uuid}` znode: the same effect as upstream.

## Problem {#problem}

On 24.3 and 24.8, every dropped table left an empty `/clickhouse/tables/<uuid>` znode, so ZooKeeper
accumulated one empty znode per dropped table.

## Decisions {#decisions}

### Drop the patch {#decision-drop}

- **Chosen:** do not carry it. Its purpose, the default `{uuid}` path, is covered by upstream on 26.8.
  Verified on the parent commit: after `DROP TABLE ... SYNC` of a table with a `{uuid}` path, the parent znode is gone.
- **Rejected:** porting it as on 26.3. For `{uuid}` paths it is a second `tryRemove` of a znode upstream
  already removed, with an INFO log line on every drop. For other paths it removes a parent that may be
  shared, which upstream avoids on purpose: `CREATE` creates the ancestors once before its retry loop, so a
  `CREATE` of a sibling table that races with the drop fails. The call is also outside any `try`, and for a
  path directly under the root it passes an empty path to `tryRemove`.
- **Rejected:** a narrow rewrite for paths without `{uuid}`. The managed configuration enforces the default
  path for new tables (patch `058`), so only older tables with their own path benefit, by one empty znode
  each, and the race above remains.

### The `060` commit on 26.3 {#decision-060}

On 26.3 the commit of patch `060` (`192665ce12b`) carries this patch's `.cpp` hunks. The 26.8 port of
`060` must contain only its own change.

## Concept {#concept}

A "minimal backport" of an upstream PR is needed only until the base contains that PR. Keep the PR
reference in the commit message, and on each uplift check whether the base contains the upstream commit
(`git merge-base --is-ancestor <commit> <base tag>`). Here the reference was dropped on 25.8, and the
26.3 review compared only the changes between 25.8 and 26.3, so the patch outlived its reason by three
versions.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `irrelevant-by-upstream`

- `9a3adc70bde` is an ancestor of every base since 25.3, including `v26.8.34.13-lts`.
- `TableZnodeInfo::dropAncestorZnodesIfNeeded` and its call in `removeTableNodesFromZooKeeper` are unchanged.
- Probe on the parent commit: a table with a `{uuid}` path leaves no parent znode; a table with a literal
  path leaves its empty parent.

## Customer impact {#customer-impact}

- **Who:** nobody, for tables with the default path, which every new table on a managed service has.
- **What changes against 26.3:**
  - Dropping the last replica of an older table whose path has no `{uuid}` leaves one empty parent znode,
    as upstream does.
  - `SYSTEM DROP REPLICA ... FROM ZKPATH` that removes the last replica leaves `/clickhouse/tables/<uuid>`,
    as upstream does.
  - The INFO log lines `Removed ancestor table znode` and `Did not remove ancestor table znode` are gone.
- **Data:** no change. An empty znode holds no data, and ZooKeeper never removes a znode that has children.
- **Upstream tests edited:** none.

## Security review {#security-review}

n/a — dropping the patch removes a ZooKeeper delete of an empty znode and adds nothing.

## Rollback {#rollback}

- **Downgrade:** not supported; services only move forward.
- **Revert in a later release:** nothing to migrate. The patch stored nothing.

## Retirement {#retirement}

Retired on 26.8. Empty znodes left by `SYSTEM DROP REPLICA ... FROM ZKPATH` are a candidate for an upstream
change: give the bare `TableZnodeInfo` it builds a `path_prefix_for_drop` when the path has a UUID component.

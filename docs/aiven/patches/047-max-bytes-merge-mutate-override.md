---
description: 'Server-wide caps on the size of merges and mutations, changeable at runtime and not stored in table definitions'
sidebarTitle: '047: Max bytes to merge/mutate override'
slug: '/aiven/patches/047-max-bytes-merge-mutate-override'
title: 'Patch 047: Per-server max bytes to merge and mutate'
doc_type: 'reference'
aiven_identity: '047'
aiven_absorbs: []
aiven_changelog_category: 'Improvement'
aiven_settings:
  - 'aiven_max_bytes_to_merge_override'
  - 'aiven_max_bytes_to_mutate_override'
---

# Patch 047 — Per-server max bytes to merge and mutate {#patch-047-max-bytes-merge-mutate-override}

## Summary {#summary}

Two server settings cap, on one server, the total source size of a merge
(`aiven_max_bytes_to_merge_override`) and the size of a part to mutate
(`aiven_max_bytes_to_mutate_override`). The cap applies both to merges and mutations the server
assigns and to those it executes from the replication queue; larger ones are postponed. Both
default to `0` (no cap) and can be changed with a config reload. Aiven uses them during
maintenance upgrades, so that new nodes keep doing the small merges that hold the part count
down while skipping the large ones that would delay fetching the data from the old nodes.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.x | `4a05c78da70` | original, settings `max_bytes_to_merge_override` and `max_bytes_to_mutate_override` |
| 26.3 | `patch-port(047)` `325c299be991`, `patch-fix(047)` `69170b8549c` | hand-applied after upstream renamed the size functions; the fix only removed duplicate declarations |
| 26.8 | `patch-port(047)` | `still-needed`: renamed to the `aiven_` prefix, runtime state made lock-free, reported as changeable without restart |

## Background (ClickHouse) {#background}

`CompactionStatistics::getMaxSourcePartsBytesForMerge` computes the largest merge a table may
start now, from the free slots in the background pool and the table's
`max_bytes_to_merge_at_min_space_in_pool` / `max_bytes_to_merge_at_max_space_in_pool`.
`getMaxSourcePartBytesForMutation` does the same for mutations from the free disk space. They
are used when a server selects merges and mutations (`StorageMergeTree`, and
`StorageReplicatedMergeTree` when it assigns them to the replication log) and when a replica
decides whether to execute a queue entry (`ReplicatedMergeTreeQueue::shouldExecuteLogEntry`),
which postpones an entry whose source parts are larger.

During a maintenance upgrade, new nodes are synced with `SYSTEM SYNC REPLICA ... LIGHTWEIGHT`,
which waits for fetches but not for merges. A large merge still blocks the fetches of parts it
covers.

## Component tour {#component-tour}

- **`CompactionStatistics`** (upstream). Both functions take the minimum with the cap when it is
  not zero.
- **`Context`** (upstream). Holds the two caps as atomics, read without a lock.
- **Server startup and config reload** (upstream, `Context` and `Server.cpp`). Load the caps.
- **`system.server_settings`** (upstream). Reports both as changeable without restart.
- **Settings.** `aiven_max_bytes_to_merge_override`, `aiven_max_bytes_to_mutate_override` in
  `ServerSettings.cpp`.

## Problem {#problem}

The merge-size limits upstream offers are MergeTree settings. Set with `ALTER TABLE`, they
change customer table definitions and replicate to every replica; set in the server's
`<merge_tree>` section, they are read once at startup, copied into each table, overridden by a
table's own `SETTINGS`, and do not cover mutations. None of them can limit one node for the
duration of an upgrade.

## Decisions {#decisions}

### The patch stays {#decision-the-patch-stays}

- **Chosen:** carry both server settings.
- **Rejected:** upstream settings; no server-level cap on merge or mutation size exists on 26.8
  (the server-wide controls are bandwidth and memory), and the MergeTree settings cannot serve,
  see Problem.
- **Rung:** 5, a cap inside upstream's merge and mutation sizing.

### Rename to the `aiven_` prefix without an alias {#decision-rename}

- **Chosen:** `aiven_max_bytes_to_merge_override`, `aiven_max_bytes_to_mutate_override`
  (user, 2026-11-06).
- **Why:** every fork-local setting carries the prefix. Server settings cannot keep an old name
  as an alias in the config file: the unknown-element check and the loader only know canonical
  names.
- **Consequence:** a config with the old names stops a 26.8 server from starting
  (`UNKNOWN_ELEMENT_IN_CONFIG`), and a config reload that adds them is rejected. The managed
  configuration must write the new names for 26.8 before 26.8 is rolled out.

### Smaller differences from 26.3 {#decision-small}

- The caps are atomics instead of fields under the global `Context` mutex: they are read for
  every queued merge and mutation on every scheduling round.
- They are loaded at startup next to the other mirrored limits, not only by the first config
  reload.
- `system.server_settings` reports them as changeable without restart, which they are.
- The mutation function keeps upstream's early return; 26.3 restructured it and re-derived its
  log comment from the result.

## Approach {#approach}

Capping the shared functions covers every caller at once: a capped server neither assigns a
larger merge nor executes one assigned by another replica. The replication queue skips the size
check only when the computed maximum equals `max_bytes_to_merge_at_max_space_in_pool` (a merge
ordered while the pool was idle); a cap below that value keeps the check. `OPTIMIZE ... PARTITION`
and `OPTIMIZE ... FINAL` select parts without these limits, so on `MergeTree` they are not capped;
on `ReplicatedMergeTree` the merge they put in the log still waits on a capped replica.

## Concept {#concept}

A temporary, node-local operating limit belongs in server configuration, not in replicated
table metadata: it must not outlive the operation or spread to other replicas.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed`

- Both `CompactionStatistics` functions, the reload block and the `Context` fields are unchanged
  in shape.
- `SYSTEM SYNC REPLICA ... LIGHTWEIGHT`, which the procedure depends on, is unchanged.

## Customer impact {#customer-impact}

- **Who:** only nodes whose configuration sets a cap, which the managed configuration does
  during maintenance upgrades. Otherwise nothing changes.
- **What they see** on a capped node: large merges and mutations wait, visible in
  `system.replication_queue`:

```sql
SELECT type, new_part_name, postpone_reason
FROM system.replication_queue
WHERE postpone_reason LIKE '%greater than the current maximum%';
-- MERGE_PARTS  all_0_5_1  Not executing log entry queue-0000000006 of type MERGE_PARTS for part all_0_5_1
--   because source parts size (12.03 MiB) is greater than the current maximum (5.00 MiB).

SELECT name, value, changeable_without_restart
FROM system.server_settings WHERE name LIKE 'aiven_max_bytes_to_%';
-- aiven_max_bytes_to_merge_override   5242880  Yes
-- aiven_max_bytes_to_mutate_override  2097152  Yes
```

- **How to restore upstream behaviour:** set both to `0` (or remove them) and reload the config.
- **New versus 26.3:** the settings are renamed; the old names are rejected.
- **Upstream tests edited:** none.

## Security review {#security-review}

No new input from queries: the caps are server configuration, readable in
`system.server_settings` like other server settings. A cap only postpones work; it cannot drop
data. Leaving a cap in place indefinitely keeps parts from merging, which shows up as a growing
part count; the managed configuration clears it after the upgrade.

## Tests {#tests}

- **Paths:** `tests/integration/test_aiven_max_bytes_merge_mutate_override/`
- **Proves:** the caps show in `system.server_settings` as changeable without restart; a capped
  `MergeTree` table never merges six ~2 MiB parts into one while an uncapped one does; a
  mutation of an ~8 MiB part stays pending under a 2 MiB cap; on `ReplicatedMergeTree`, a merge
  assigned by an uncapped replica is postponed on the capped one with the size reason,
  `SYSTEM SYNC REPLICA ... LIGHTWEIGHT` still returns, and removing the cap with a config reload
  lets the merge run.
- **Run:** `python -m ci.praktika run "integration" --test test_aiven_max_bytes_merge_mutate_override`,
  or `pytest` from `tests/integration` with `CLICKHOUSE_TESTS_SERVER_BIN_PATH` set.
- **Evidence:** on the parent commit every test errors at setup, because the server rejects
  both unknown elements (`UNKNOWN_ELEMENT_IN_CONFIG`); with the patch all 4 pass, three runs in
  a row (2026-11-06).

## Rollback {#rollback}

- **Downgrade:** not supported; services only move forward.
- **Revert in a later release:** no on-disk or Keeper format of its own.
- **Disable without rebuild:** set both caps to `0` and reload the config.

## Retirement {#retirement}

An upstream server-level, runtime-changeable cap on merge and mutation size. Re-check upstream
on every uplift. The release that drops the patch keeps both settings declared with
`SettingsTierType::OBSOLETE`; an unknown element stops the server from starting.

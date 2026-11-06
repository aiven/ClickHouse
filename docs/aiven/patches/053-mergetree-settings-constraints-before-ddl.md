---
description: 'Check the user''s MergeTree settings constraints before a DDL query is enqueued in a Replicated database'
sidebarTitle: '053: MergeTree settings constraints before DDL'
slug: '/aiven/patches/053-mergetree-settings-constraints-before-ddl'
title: 'Patch 053: MergeTree settings constraints before Replicated DDL'
doc_type: 'reference'
aiven_identity: '053'
aiven_absorbs: []
aiven_changelog_category: 'Bug Fix'
aiven_settings: []
---

# Patch 053 — MergeTree settings constraints before `Replicated` DDL {#patch-053-mergetree-settings-constraints-before-ddl}

## Summary {#summary}

A settings profile can constrain MergeTree table settings, for example
`merge_tree_max_suspicious_broken_parts MAX 5`. In a `Replicated` database these constraints were not
applied to the user: `CREATE TABLE ... SETTINGS` and `ALTER TABLE ... MODIFY SETTING` / `RESET SETTING`
accepted any value. The patch checks them against the user's profile before the query is enqueued.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.x | `bb048483602c` | original: checks `CREATE` settings and `ALTER ... MODIFY SETTING` |
| 26.3 | `patch-port(053)` `60d77206faa9` | unchanged; include-block conflict only |
| 26.8 | `patch-port(053)` | `still-needed`: `ALTER` checked against the table's settings, `RESET SETTING` added |

## Background (ClickHouse) {#background}

A `Replicated` database does not execute DDL in the session that sent it. `tryEnqueueReplicatedDDL`
validates the query, writes it to the database queue in Keeper, and every replica, the initiator included,
executes it from there through the DDL worker. `DatabaseReplicatedTask::makeQueryContext` builds that
context from the worker's own context, and the entry carries no user (`initiator_user` is only written by
`ON CLUSTER` DDL), so the checks inside the storage (`registerStorageMergeTree`,
`MergeTreeData::checkAlterIsPossible`) apply the server's default constraints, not the user's.

`SettingsConstraints` skips a change whose value equals the current one, so what a check compares
against matters as much as the change.

## Component tour {#component-tour}

- **`DatabaseReplicated::checkTableEngine`** (upstream). For an engine ending in `MergeTree` with a
  `SETTINGS` clause, checks the clause against the user's constraints, with the server's
  `ReplicatedMergeTree` defaults as the current values. Called for the table and for every inner table of
  a materialized view.
- **`DatabaseReplicated::checkQueryValid`** (upstream). Collects `MODIFY SETTING` changes and
  `RESET SETTING` names from all `ALTER` commands. If the table is a `MergeTreeData`, turns each reset into
  a change to the server default of its kind and checks all of them against the table's current settings.

Both run only on the initiator, in the user's context. Replicas executing the entry from the queue do not
check again.

## Problem {#problem}

Profile constraints on MergeTree settings are the way to keep users away from table settings that are
unsafe on a managed service. Through a `Replicated` database they did not apply.

## Decisions {#decisions}

### Check before enqueue {#decision-before-enqueue}

- **Chosen:** check in `checkTableEngine` and `checkQueryValid`, the last point where the user's context
  exists.
- **Rejected:** writing `initiator_user` into the entry and running the worker as the user. It changes
  how every DDL in a `Replicated` database is executed and authorised on every replica, including replicas
  that may not have the user yet. Too large for a patch; a candidate upstream change.
- **Rung:** 4, a check added to upstream's DDL validation.

### `ALTER` against the table's current settings {#decision-alter-current}

- **Chosen:** look up the table and use its settings as the current values.
- **Rejected:** the 26.3 form, server defaults as the current values. A change back to the default is
  then "unchanged" and skipped: a table at 4 could be set to 100 above a cap of 5. It also cast every
  name as a MergeTree setting, even for tables of other engines.

### `RESET SETTING` included {#decision-reset}

- **Chosen:** a reset is a change to the server default, as upstream's `checkAlterIsPossible` treats it.
  The test shows an `Atomic` database rejecting the same reset.
- **Rejected:** leaving resets out, as 26.3 did. Same bypass, one keyword away.

## Approach {#approach}

`CREATE`: plain `MergeTree` is checked too, because the engine can still be rewritten to
`ReplicatedMergeTree` when the table is created, after this check. The current values are the server's
`ReplicatedMergeTree` defaults; a value equal to the default is not a change and is not checked, as
upstream.

`ALTER`: the check is skipped when the table does not exist or is not a `MergeTreeData`; the query then
fails or proceeds exactly as upstream.

## Concept {#concept}

Authorisation must happen where the identity is. When work is handed to a queue that runs under a service
identity, every check that depends on who asked has to run before the hand-off.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed`

- `checkTableEngine` and `checkQueryValid` unchanged at their anchors; only caller is
  `tryEnqueueReplicatedDDL`.
- Upstream added running distributed DDL as the initial user (`distributed_ddl_use_initial_user_and_roles`)
  for `ON CLUSTER` only, and clamping of the entry's query settings (#99317); neither covers table settings
  in a `Replicated` database.
- Upstream reworked the storage-level checks (`is_fresh_definition`, effective settings after `ALTER`);
  they still run in the worker's context.
- The 26.3 test reported a bypass for the wrong reason on 26.8: the `CREATE` failed with `ACCESS_DENIED`
  because the test configuration requires `TABLE ENGINE` grants, and every error was counted as
  "allowed". Rewritten.

## Customer impact {#customer-impact}

- **Who:** users with a settings profile that constrains MergeTree settings, on `Replicated` databases.
- **What they saw before:** DDL with a value outside the constraint succeeded.
- **Now:** the query fails with `SETTING_CONSTRAINT_VIOLATION` before it is enqueued, as it does in an
  `Atomic` database.
- **Existing tables:** unchanged; values set before the upgrade stay. Only new DDL is checked.
- **How to restore upstream behaviour:** none; no setting.
- **Upstream tests edited:** none.

## Security review {#security-review}

The patch closes a gap where profile constraints on MergeTree settings were not enforced for DDL in
`Replicated` databases. It only adds checks, in the user's own context, before anything is written to
Keeper; it does not change grants, the queue format, or what replicas execute. A rejected query leaves
nothing in the queue.

## Tests {#tests}

- **Paths:** `tests/queries/0_stateless/aiven_053_mergetree_settings_constraints_before_ddl.sh`
- **Proves:** a user with `merge_tree_max_suspicious_broken_parts MAX 5` in a `Replicated` database is
  rejected for `CREATE` with `MergeTree` and `ReplicatedMergeTree`, `MODIFY SETTING` above the cap,
  `MODIFY SETTING` back to the default of 100, and `RESET SETTING` to that default. Values within the cap
  and a reset of an unconstrained setting are allowed. The same reset in an `Atomic` database is rejected
  by upstream. Any other error is printed, so it cannot pass as "allowed"; the table settings are listed.
- **Run:** `python -m ci.praktika run "Stateless tests (amd_debug, parallel)" --test aiven_053_mergetree_settings_constraints_before_ddl`
- **Evidence (2026-11-06):**
  - RED on the parent commit `69073d0b8d14`: all five over-the-cap queries are allowed; the tables have
    `max_suspicious_broken_parts` 999 and 100. Within-cap queries allowed; `Atomic` reset rejected.
  - GREEN with the patch: OK, three runs in a row.

## Rollback {#rollback}

- **Downgrade:** not supported; services only move forward.
- **Revert in a later release:** no format of its own; the constraints stop applying again.
- **Disable without rebuild:** not possible.

## Retirement {#retirement}

Upstream running `Replicated` database DDL as the initiating user, or checking MergeTree settings
constraints before enqueue. A good upstream contribution candidate.

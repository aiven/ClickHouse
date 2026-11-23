---
description: 'Only the default settings profile and its descendants can be assigned by a principal whose default profile disables allow_non_default_profile'
sidebarTitle: '014: Default profile escape'
slug: '/aiven/patches/014-default-profile-escape'
title: 'Patch 014: Default profile escape'
doc_type: 'reference'
aiven_identity: '014'
aiven_absorbs: []
aiven_changelog_category: 'Improvement'
aiven_settings:
  - 'allow_non_default_profile'
---

# Patch 014 — Default profile escape {#patch-014-default-profile-escape}

## Summary {#summary}

Settings constraints limit what a user may set, but a user who manages access could
previously give a new user, role or settings profile a parent profile that the
constraints do not cover, and then log in as that user. The query-level setting
`allow_non_default_profile` closes this. When it is disabled, every settings profile
assigned to a user, role or settings profile must be the default profile or inherit from
it, and a settings profile created without a parent inherits from the default profile.
The managed configuration disables the setting in the default profile and locks it, and
enables it in the administrative profiles. With the default value, `1`, nothing changes,
except that an `ALTER SETTINGS PROFILE` forming an inheritance cycle is rejected.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.8 | `e65f68836b` | **origin.** The setting, the parent check in `SettingsConstraints::check`, the cycle check and the injected default parent |
| 26.3 | `patch-port(014)` at `b3a4ac4134b` on `v26.3.60.2-lts-aiven` | applied clean |
| 26.8.2.7 | — | not ported |
| 26.8.34.13 | `patch-port(014):` | `still-needed-and-applies-with-conflicts`: one include conflict next to the patch 022 code; the walk rewritten, descent required along every chain of parents, `RESTORE` checks extended |

## Background (ClickHouse) {#background}

A settings profile is a set of setting values and constraints. Users, roles and other
profiles reference profiles as parents (`SETTINGS PROFILE p`, `INHERIT p`), by UUID. The
server merges the parents in order, and the profile named by `default_profile` in the
server configuration is applied first for everyone. Constraints (`MIN`, `MAX`, `CONST`,
`CHANGEABLE_IN_READONLY`) are checked when a user changes a setting in a session, and when
an access entity is created or altered with settings: `SettingsConstraints::check` then
compares every setting element against the constraints of the user running the statement.

That check covers setting values and constraint changes, but not parent profiles. A parent
profile carries its own values and constraints, and nothing compared them with the
constraints of the user who assigned it.

## Component tour {#component-tour}

| Component | Role in this patch |
|---|---|
| `Settings` (`allow_non_default_profile`) | The query-level switch; read from the settings of the user running the statement |
| `SettingsConstraints::check` | Rejects a parent profile which is not the default profile or a descendant of it, for users, roles and profiles alike |
| `SettingsProfilesCache` | Knows the default profile's UUID; walks parent links to decide descent along every chain, and reachability for the cycle check |
| `AccessControl` | Forwards the descent queries to the cache |
| `InterpreterCreateSettingsProfileQuery` | Rejects inheritance cycles on `ALTER`; injects the default profile as parent of a profile left without one |
| `AccessRestorerFromBackup`, `RestorerFromBackup` | Apply the same rule, and the settings constraints, to the users, roles and profiles which `RESTORE` writes |
| `SettingsChangesHistory` | Records the new setting for `compatibility` |

## Problem {#problem}

Constraints in the default profile only bind users whose settings come from the default
profile. A principal holding `CREATE USER`, `CREATE ROLE` or `CREATE SETTINGS PROFILE`
could create an entity whose parent is a profile outside the default profile's tree, such
as an administrative profile defined in `users.xml`, and so hand out settings that its own
constraints forbid.

## Decisions {#decisions}

### Keep the name `allow_non_default_profile` {#decision-name}

- **Chosen:** keep the name used on 25.8 and 26.3.
- **Why:** the managed configuration sets the setting by this name in its profiles. A
  rename would have to ship in the managed configuration before this binary, and an unknown
  setting in a `users.xml` profile is an error.
- **Exception to the doctrine:** fork-local settings are otherwise named `aiven_*`
  (`porting-doctrine.md`). This one is part of the managed configuration's contract.

### One check for every entity type {#decision-single-check}

- **Chosen:** the parent check lives in `SettingsConstraints::check`, which the user, role
  and profile interpreters already call with their `SettingSource`. It does not depend on
  the source.
- **Rejected:** a check per interpreter. Three copies, and a fourth way of assigning a
  profile would miss it.

### Every chain of parents must reach the default profile {#decision-every-chain}

- **Chosen:** `SettingsProfilesCache::isDefaultProfileOrDescendant` accepts a profile only
  if it is the default profile, or has parents and each of them passes the same test. A
  profile with no parent, or with a parent that cannot be read, does not descend.
- **Why:** a profile merges the settings of all its parents, and later parents override
  earlier ones. One parent outside the default profile's tree is enough to carry settings
  that the default profile's constraints would refuse, even when another parent is the
  default profile.
- **Rejected:** any chain reaching the default profile. That accepts a profile inheriting
  from both the default profile and an administrative one.

### The walks read the storages and visit each profile once {#decision-walk}

- **Chosen:** both walks, descent along every chain and reachability for the cycle check,
  keep a visited set and read each profile through `AccessControl::tryRead`. The cache
  mutex is held only to read the default profile's UUID.
- **Why a visited set:** `users.xml` can define profiles that inherit from each other, and
  upstream's `substituteProfiles` tolerates such cycles. A profile on a cycle is not a
  descendant: it is marked so while its parents are visited.
- **Why not `all_profiles`:** a profile written to a storage directly, by
  `CREATE ... IN <storage>` or by `RESTORE`, reaches the cache only with the next batch of
  change notifications. Walking the cache would reject a statement that references a
  profile created a moment earlier.

### `RESTORE` follows the same rule {#decision-restore}

- **Chosen:** `RESTORE` writes access entities into the storage past the interpreters, so
  it checks them itself when `allow_non_default_profile` is disabled for the user running
  it. Every settings profile it inserts must inherit from the default profile, and every
  parent of a user or role it inserts must be the default profile or inherit from it. A
  parent restored from the same backup counts when it passes the same rule. The setting
  values and constraints of what it inserts are checked against the constraints of the
  user running it, as `CREATE` checks them.
- **Only what is inserted:** an entity in the backup that the restore keeps as it exists,
  because one of the same name exists and the creation mode does not replace it, is not
  checked; a reference to it resolves to the existing entity, which is checked instead.
- **Checked twice:** once before anything is restored, and again when the restore resolves
  names against the storages and decides what it inserts. Between the two, a concurrent
  `DROP` can turn a kept entity into an inserted one, so the second check decides on what
  is actually written. A restore that fails the second check has already created its
  databases and tables.
- **Why profiles need a parent:** the interpreter injects the default profile into a
  profile created without a parent, so every profile created under the setting has one.
  A restored profile without a parent would be the only exception.
- **Dependencies:** a parent that exists already is resolved by name, the way the restore
  resolves it. A parent ID that the backup does not list as a dependency is kept verbatim
  by the restore, so it is checked as an ID, like the `TO` sets in patch 022.

### The default parent is injected into the profile as written {#decision-injection}

- **Chosen:** with the setting disabled, a profile which `CREATE` or `ALTER ... SETTINGS`
  leaves without a parent gets the default profile as its first parent. The decision is
  made on the profile after the statement is applied, so an `ALTER` that adds settings to
  a profile with parents keeps them, and `DROP ALL PROFILES` leaves the default profile.
  The default profile itself never inherits from itself.
- **Why first:** later parents override earlier ones, so the default profile must not
  override a stricter parent added later.

### The cycle check is unconditional {#decision-cycle}

- **Chosen:** as on 25.8 and 26.3, an `ALTER SETTINGS PROFILE` whose new parent already
  inherits from the altered profile is rejected whatever the setting. A cycle has no use.
- **Limit:** the check reads the storages before the `ALTER` writes, so two concurrent
  `ALTER`s can still form a cycle together, and `users.xml` can define one. The walks and
  upstream's `substituteProfiles` terminate on cycles, and a profile on a cycle does not
  descend from the default profile, so such a cycle grants nothing.

## Approach {#approach}

`SettingsConstraints::check` asks `AccessControl::isDefaultProfileOrDescendant` about each
element with a parent profile when the setting is disabled.
`InterpreterCreateSettingsProfileQuery` runs the cycle check, the constraints check, and,
with the setting disabled, prepends the default profile as parent to a profile left
without one. `RestorerFromBackup::checkAccessEntitiesInheritFromDefaultProfile` calls
`AccessRestorerFromBackup::checkProfilesInheritFromDefault` and checks
`AccessRestorerFromBackup::getSettingsOfRestoredEntities` against the constraints of the
user executing the restore. It runs next to the patch 022 check, and again after
`generateRandomIDsAndResolveDependencies`.

## Concept {#concept}

The invariant: with `allow_non_default_profile` disabled for the user writing it, every
settings profile written to SQL storage inherits from the default profile along every
chain of parents, and every profile assigned to a user or role is the default profile or
inherits from it the same way. Constraints in the default profile therefore reach every
entity such a user creates, and the setting itself cannot be lifted, because the default
profile locks it.

The invariant holds when an entity is written. A later `DROP SETTINGS PROFILE` of a parent
removes the reference, which can leave a profile without a parent. Such a profile no longer
counts as a descendant, so it cannot be assigned again: the failure is closed.

## Drift on this uplift {#drift-on-this-uplift}

- `InterpreterCreateSettingsProfileQuery` gained the patch 022 protected-target checks and
  upstream's `DROP SETTINGS PROFILE` requirement for `CREATE ... OR REPLACE`. Only the
  include block conflicted; the 014 block sits after the access check.
- This line records fork-local settings in `SettingsChangesHistory`; 26.3 had no entry.
- Stateless tests on this line are named `aiven_<NNN>_<slug>`.
- `allow_non_default_profile` is listed in
  `ci/jobs/scripts/check_style/experimental_settings_ignore.txt`: it is not an
  experimental-feature switch.

## Downstream contract {#downstream-contract}

- The managed configuration sets `allow_non_default_profile` to `0` in the default profile
  with a readonly constraint, and to `1` in the administrative and system profiles.
- The error for a rejected profile is `SETTING_CONSTRAINT_VIOLATION` with
  `All profiles must be the default profile or inherit from it.`; for a cycle,
  `SETTING_CONSTRAINT_VIOLATION` with `Inherited profiles may not have circular dependencies`.

## Customer impact {#customer-impact}

- A customer creating users, roles or profiles can assign the default profile and the
  profiles inheriting from it. `INHERIT default` is added to a new profile that names no
  parent.
- A profile with several parents, one of them outside the default profile's tree, can no
  longer be assigned by such a customer. Existing assignments keep working. To find these
  profiles:
  `SELECT profile_name, groupArray(inherit_profile) FROM system.settings_profile_elements WHERE profile_name != '' AND inherit_profile != '' GROUP BY profile_name HAVING length(groupArray(inherit_profile)) > 1`.
- `RESTORE` of access entities created under these rules is unaffected, unless their
  setting values or constraints exceed the constraints of the user running the restore.
- An inheritance cycle in `ALTER SETTINGS PROFILE` is an error on every server.

## Known residual {#known-residual}

- The rule holds at write time only; see [Concept](#concept) for a dropped parent.
- Concurrent `ALTER`s can form an inheritance cycle; see [the cycle decision](#decision-cycle).

## Security review {#security-review}

- **Bypass of the setting:** the setting is read from the user running the statement. A
  value in a session, in the query's settings, or in the settings of a new user or profile,
  and a `WRITABLE` or `CHANGEABLE_IN_READONLY` constraint on it, are all rejected by the
  `CONST` constraint of the default profile. The tests cover each.
- **Switching profiles in a session:** `SET profile` applies the profile's settings as a
  change by the session user, so they meet the user's constraints. A profile that enables
  `allow_non_default_profile` is refused by the `CONST` constraint.
- **Running as another user:** `EXECUTE AS` requires `IMPERSONATE` on the target user,
  which is granted separately from access management.
- **Writers of access entities:** the interpreters for users, roles and profiles, and
  `RESTORE`, apply the rule. `users.xml` is read-only to SQL.
- **Availability:** the walk terminates on cyclic graphs and holds no lock while reading
  the storages, so a malformed configuration cannot stall or crash the server through it.
- **Failure mode:** a parent that cannot be read is not a descendant, so the check rejects.

## Tests {#tests}

| Test | What it proves |
|---|---|
| `aiven_014_default_profile_escape.sh` | `CREATE`/`ALTER` of users, roles and profiles, and `CREATE SETTINGS PROFILE OR REPLACE`, reject a parent outside the default profile's tree; the setting cannot be lifted by `SET`, query settings, entity settings or constraint changes; the default profile and its descendants stay assignable; the injected parent; the cycle check; without the setting anything goes |
| `aiven_014_default_profile_escape_restore.sh` | `RESTORE` rejects a user on a profile outside the tree and a profile without a parent; it restores a user on a descendant and a profile chain restored together with its user; without the setting anything is restored |
| `aiven_014_default_profile_escape_checks.sh` | A profile with one parent outside the default profile's tree is rejected as a parent; diamonds within the tree are allowed; `ALTER ... ADD SETTINGS` keeps the parents a profile has; `DROP ALL PROFILES` leaves the default profile |
| `aiven_014_default_profile_escape_restore_checks.sh` | `RESTORE` rejects settings and constraints beyond the restoring user's constraints and accepts those within them; rejects a profile with a parent outside the tree; keeps an existing profile of the same name and binds the restored user to it |
| `test_aiven_default_profile_escape` | The managed shape: the setting locked by a `users.xml` constraint, an administrative profile outside the tree, a `users.xml` profile inheriting from both, and two `users.xml` profiles inheriting from each other. The walk terminates on the cycle; `SET profile` to the administrative profile is refused |

Causation: on the parent binary every case fails, because the setting does not exist. On a
build without the `RESTORE` check and without the visited set, the restore test fails its
two rejection cases and the cycle test hangs until the client times out.

Evidence, 2026-11-27: on the build before the changes to every-chain descent, restore
constraints and injection, the two `_checks` tests fail on each rejection case and on the
kept parents; with them, the stateless suites `aiven_`, `settings_profile` and
`profile_settings` pass except `01294_create_settings_profile` and
`aiven_054_keeper_map_read_only_setting`, which fail the same way on the earlier build, and
`test_aiven_default_profile_escape` passes 7 of 7.

## Rollback {#rollback}

Reverting removes the setting. A `users.xml` profile that sets it then fails to load, so
the managed configuration has to drop it first. Downgrade: not supported.

## Retirement {#retirement}

Retire if upstream checks parent profiles against the constraints of the user assigning
them, for SQL and `RESTORE`. Search upstream `SettingsConstraints::check` for a
`parent_profile` check.

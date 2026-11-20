---
description: 'Mark service-managed users and roles PROTECTED so that only a holder of PROTECTED_ACCESS_MANAGEMENT can change them'
sidebarTitle: '022: Protected users and roles'
slug: '/aiven/patches/022-protected-users-and-roles'
title: 'Patch 022: Protected users and roles'
doc_type: 'reference'
aiven_identity: '022'
aiven_absorbs: ['079']
aiven_changelog_category: 'New Feature'
aiven_settings: []
---

# Patch 022 — Protected users and roles {#patch-022-protected-users-and-roles}

## Summary {#summary}

A user or role created with `PROTECTED` can only be created, altered, renamed, replaced,
dropped, moved, re-granted or un-protected by a principal holding the new privilege
`PROTECTED_ACCESS_MANAGEMENT` (SQL: `PROTECTED`). The same privilege is needed to aim a row
policy, quota, settings profile or default role at a protected entity, to change one whose
stored targets already reach a protected entity, to drop something a protected entity
depends on, and to restore a protected entity from a backup. Nobody may alter, replace or
revoke from themselves, even holding the privilege. Aiven carries it so that the main
service user, the monitoring user and the two managed roles survive a tenant that legitimately
holds access management. Nothing changes until an entity is marked `PROTECTED`, which the
control plane does on every service powerup.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.8 | `ce74bc008d` (users), `f4552b14c77` (`ON CLUSTER` fix), `827d8ebb573d` (roles) | carried as three commits |
| 26.3 | `patch-port(022)` `785f00febc45`, `patch-port(079)` `c2a4fc417327`, `patch-fix(022,079)` `d158b35e863e` | ported as three commits; rewritten against the drifted Access subsystem |
| 26.8 | `patch-port(022)` | `still-needed-but-rewrite`, one commit absorbing `079` |

On 26.8 the patch carries identity `022` and absorbs `079` (protected roles): the role half
reuses every mechanism of the user half, and a commit with only one of them fails its own
tests. Five pieces of prior work land together: the protected-user base, the `ON CLUSTER`
hoist, protected roles, the `optional<bool>` round-trip fix and the `checkProtectedTargets`
type-erasure fix. Three pieces are new on 26.8: the
[dependency-cascade guard](#the-cascade-a-door-that-is-new-on-268), the
[stored-target check](#stored-targets) and the [`RESTORE` check](#restore).

## Background (ClickHouse) {#background}

ClickHouse keeps users, roles, quotas, row policies and settings profiles as access entities
behind a common interface, `IAccessStorage`. A managed deployment stacks three storages under a
`MultipleAccessStorage`: the read-only one backed by `users.xml`, a `DiskAccessStorage` for
entities created over SQL, and a `ReplicatedAccessStorage` that keeps them in ZooKeeper so every
replica agrees. All writes funnel into `insertImpl`, `removeImpl` and `updateImpl`.

Above that sit the interpreters in `src/Interpreters/Access/`, one per statement family, which
translate SQL into those calls after checking privileges. `RESTORE` is the one writer that does
not go through them: `restoreAccessEntitiesFromBackup` inserts entities directly, after
`RestorerFromBackup` has checked the privileges listed by `AccessRestorerFromBackup`.

The detail that shapes this whole patch is how `ON CLUSTER` works. The interpreter calls
`executeDDLQueryOnCluster` and returns immediately; the statement is queued in ZooKeeper and
replayed on every replica by `DDLWorker`, under a different identity than the one that typed it.
A privilege check placed after that call never runs for the original user, so every check of
this patch sits above the dispatch.

Privileges form a tree in `src/Access/Common/AccessType.h` (`ALL` → `ACCESS MANAGEMENT` →
`CREATE USER`, ...); granting or revoking a node acts on its whole subtree.

## Component tour {#component-tour}

- **Grammar.** `CommonParsers.h` gains the `PROTECTED` keyword. `ParserCreateUserQuery` and
  `ParserCreateRoleQuery` accept `PROTECTED` / `NOT PROTECTED` in their option loop, so it may
  appear before or after an `IDENTIFIED` clause. `ASTCreateUserQuery` and `ASTCreateRoleQuery`
  carry it as `std::optional<bool> protected_flag` and re-emit it when formatting.
- **Entity.** `IAccessEntity` gains a virtual `isProtected` returning `false`, overridden by
  `User` and `Role`, which also compare the flag in `equal`.
  `InterpreterShowCreateAccessEntityQuery` renders the keyword back out, which is also how the
  flag travels through a backup.
- **Privilege.** `PROTECTED_ACCESS_MANAGEMENT`, spelled `PROTECTED` in SQL and rendered as
  `PROTECTED ACCESS MANAGEMENT` in error text, is a direct child of `ALL`, not of
  `ACCESS MANAGEMENT`.
- **Interpreters.** `InterpreterCreateUserQuery`, `InterpreterCreateRoleQuery`,
  `InterpreterDropAccessEntityQuery`, `InterpreterGrantQuery` and
  `InterpreterMoveAccessEntityQuery` check protection and self-protection above the
  `ON CLUSTER` dispatch.
- **Targets.** `checkProtectedTargets` (new, `src/Interpreters/Access/`) has two overloads: one
  for the `TO` clause of `CREATE`/`ALTER ROW POLICY`, `QUOTA`, `SETTINGS PROFILE` and
  `SET DEFAULT ROLE`, and one for the target set already stored on an existing policy, quota
  or profile.
- **Storage.** `IAccessStorage` threads a `CheckFunc` through `insertImpl` and `removeImpl` into
  `MemoryAccessStorage`, `DiskAccessStorage` and `ReplicatedAccessStorage`/`ZooKeeperReplicator`,
  so the existing entity is re-checked under the storage lock just before it is overwritten or
  deleted.
- **Backups.** `AccessRestorerFromBackup::touchesProtectedEntities` (new) tells
  `RestorerFromBackup::checkAccessForObjectsFoundInBackup` to require the privilege.

## Problem {#problem}

The control plane needs the main service user (`avnadmin`) to be a real SQL user with scoped
privileges rather than a `users.xml` entry, and needs two managed roles (`aiven_admin_role`,
`aiven_readonly_role`) and a monitoring user to exist as ordinary access entities. But
`avnadmin` holds the admin role, and that role carries `CREATE`/`ALTER`/`DROP USER`,
`DROP ROLE`, `ALTER ROLE`, `ROLE ADMIN` and the rest of access management.

Without this patch the tenant can therefore delete or rewrite the entities the service depends
on with ordinary SQL: drop or rename the managed roles, replace `avnadmin`, or drop the
monitoring user. The service loses the ability to manage the cluster and monitoring goes blind.
None of it needs an exploit; it is the documented behaviour of privileges the tenant
legitimately holds.

## Decisions {#decisions}

### Protection is a flag on the entity, checked on every write path {#decision-flag}

- **Chosen:** a per-entity flag plus one privilege, enforced at every path that can change a
  protected entity or what applies to it (see [Approach](#approach)).
- **Rejected:** keeping the managed principals in `users.xml`. They could not be granted or
  revoked per service, rotated over SQL, or hold fewer privileges than the XML format allows.
- **Rejected:** a list of protected statements. Every upstream release adds statement forms
  (`OR REPLACE` checks, new authentication clauses, `EXECUTE AS`); a check on the entity
  catches them without a change to this patch.

### The privilege sits under `ALL`, not under `ACCESS MANAGEMENT` {#decision-privilege-placement}

- **Chosen:** a direct child of `ALL`, so a grant or revoke of `ACCESS MANAGEMENT` never
  includes it.
- **Consequence:** a `users.xml` user without a `<grants>` section starts from
  `GRANT ALL` and keeps the privilege even with `access_management = 0`, because that flag
  revokes only the `ACCESS MANAGEMENT` subtree. Without access management the privilege
  allows nothing on its own: it is a lock, not a capability.

### Self-protection is unconditional {#decision-self-protection}

- **Chosen:** nobody may create, replace, alter or revoke from themselves, even holding the
  privilege. This covers one's own password and settings, not only irrecoverable operations;
  the control plane owns the credentials, and the principal holding the privilege must not be
  able to lock itself out.
- The `GRANT` path compares grantees by UUID and by name, because the resolved grantee id is
  not always the session's `getUserID`.
- `GRANT ... WITH REPLACE OPTION` and `GRANT <role> ... WITH REPLACE OPTION` revoke every
  privilege or every granted role before granting, so they count as a revoke here (2026-11-19).

### `TO ALL` is refused, not narrowed {#decision-to-all}

- **Chosen:** `TO ALL` resolves to a set containing the protected entities, so an unprivileged
  principal is refused and writes `TO ALL EXCEPT <protected entities>`.
- **Rejected:** silently adding the protected entities to the `EXCEPT` list. It would freeze the
  exclusion at DDL time, so an entity protected later would not be covered, and the stored
  policy would depend on who created it.
- **Rejected:** refusing a target set because it contains the caller, as 26.3 did. `ALL` always
  contains the caller, so that refused every `TO ALL` on every server.

### `RESTORE` is checked through its required access {#decision-restore}

- **Chosen:** `RESTORE` requires the privilege when it would create a protected entity or, with
  `create_access = 'replace'`, overwrite one (2026-11-19).
- **Extended (2026-11-19):** the same applies to a restored row policy, quota or settings
  profile whose `TO` set reaches a [protected target](#protected-targets), and, with
  `create_access = 'replace'`, to one replacing an entity whose stored set does.
- **Rejected:** relying on a build-time gate that compiles `RESTORE` out. No such gate is on
  the 26.8 line; on 26.3 it is patch `N06`, which defaults to `RESTORE` being enabled. This
  patch therefore assumes `RESTORE` is available.
- **Rejected:** a `CheckFunc` on the restore's insert as well. It would close the window
  between the check and the write. Only a principal holding the privilege can make an entity
  protected in that window, so the window gives an unprivileged caller nothing, and the hunk
  would sit in a file upstream changes often.

### `users.xml` users are protected targets {#decision-users-xml-targets}

- **Chosen (2026-11-19):** a user or role of a read-only storage (`users.xml`, LDAP, a
  read-only disk storage) counts as a protected target in the `TO` clause of a row policy,
  quota or settings profile, through `AccessControl::isProtectedTarget`. SQL cannot alter such
  a user, and it must not change what applies to it through another entity's `TO` clause
  either. The service's internal admin user lives in `users.xml`.
- **Rejected:** a flag on `users.xml` users. The storage is read-only, and the property that
  matters is exactly that SQL does not manage these users.
- **Consequence:** the unprivileged form of a fleet-wide policy is
  `TO ALL EXCEPT <protected entities>, <users.xml users>`.

## Approach {#approach}

Enforcement sits at six points:

1. **Above the `ON CLUSTER` dispatch**, in every interpreter that can reach a protected entity.
   This is the check that protects the entity; the rest is depth. On `CREATE ... OR REPLACE`
   it runs after upstream's own check that requires `DROP` as well as `CREATE`, so each missing
   privilege is reported on its own.
2. **At the storage write**, through the `CheckFunc`, re-reading the existing entity under the
   storage lock before it is replaced or removed. This closes the time-of-check-to-time-of-use
   window of the replicated storage, where two nodes race on the same ZooKeeper node.
3. **Self-protection**, per [the decision above](#decision-self-protection).
4. **The dependency cascade**, see [below](#the-cascade-a-door-that-is-new-on-268).
5. **Targets of policies, quotas, profiles and default roles**, see
   [below](#to-all-and-the-boundary-of-self-protection) and [stored targets](#stored-targets).
6. **`RESTORE`**, see [below](#restore).

### The cascade: a door that is new on 26.8 {#the-cascade-a-door-that-is-new-on-268}

26.8 added `IAccessStorage::removeReferencesToRemovedIDs`. After a top-level `remove`, it calls
`update` on every entity that referenced the removed id, so no dangling references remain. It
writes through `updateImpl`, which carries no `CheckFunc`, as a side effect with no user
identity in scope. What it can strip from a protected entity is `default_roles`,
`granted_roles`, `grantees` and `settings`, not its direct privileges.

The guard goes in the interpreter: the triggering `DROP` is refused on the initiator, above the
`ON CLUSTER` dispatch, when a protected entity depends on the target and the caller lacks the
privilege. A `CheckFunc` on `updateImpl` would have no identity to check, and refusing the
update would leave protected entities holding exactly the dangling references the cascade
exists to remove. The dependency scan is the walk the cascade already performs.

Two other statements remove an entity and so trigger the same cascade, and the same guard,
`checkNoProtectedDependents`, refuses them (2026-11-19). `CREATE ROLE ... OR REPLACE` and
`CREATE USER ... OR REPLACE` give the entity a new ID, so the old ID disappears from every
entity that held it. `MOVE` removes the entity from its source storage. For `MOVE` the scan
covers every storage, not only the source one: it refuses slightly more than the cascade would
reach, and it is the same scan.

In the current managed configuration the roles granted to protected users are themselves
protected, so the cascade is not reachable there; that is how the control plane happens to
assign grants, not a server-side invariant, so it is guarded anyway.

### `TO ALL` and the boundary of self-protection {#to-all-and-the-boundary-of-self-protection}

The `TO` clause of a row policy, quota or settings profile disables an account without
touching it: a restrictive row policy, a quota of one query, a profile with a tiny memory limit.
`checkProtectedTargets` refuses a set that reaches a protected target. It reads each target
through `IAccessEntity`, because a role is a legitimate target and reading it as a `User` would
raise `LOGICAL_ERROR`, and treats it as a [protected target](#protected-targets) or not.

It does not refuse a set because the caller is in it. These statements store the set on the
policy, as `to_roles`; the listed users are never rewritten, so naming yourself is not
self-modification. `SET DEFAULT ROLE ... TO` does rewrite its targets, so a protected target
needs the privilege there too, and it has no self-check for the same `ALL` reason.

The check is server-global: one protected entity anywhere makes every unprivileged `TO ALL`
fail. That is why the stateless tests here are tagged `no-parallel`.

### Protected targets {#protected-targets}

A protected target is what `AccessControl::isProtectedTarget` accepts: an entity with the flag,
or any entity of a read-only storage such as `users.xml` (see
[the decision](#decision-users-xml-targets)). The rule is shared by `checkProtectedTargets` and
the [`RESTORE` check](#restore), so the two cannot drift apart.

### Stored targets {#stored-targets}

`ALTER ROW POLICY`, `ALTER QUOTA` and `ALTER SETTINGS PROFILE` without a `TO` clause keep the
stored set, and `CREATE ... OR REPLACE` throws it away; both change what applies to the
entities in it. Above the dispatch, the interpreter reads the existing entity of each name and
runs the same check on its stored `to_roles`. For a row policy without a database, the
current database is used, as the statement itself does.

`DROP ROW POLICY`, `DROP QUOTA` and `DROP SETTINGS PROFILE` run the same check on the stored set
of each entity they drop (2026-11-19). Dropping a quota or profile only lifts a restriction, but
dropping a permissive row policy under `throw_on_unmatched_row_policies` makes the table
unreadable for the users it named, and a principal that may not attach a policy to a protected
target should not be able to detach one either.

### `RESTORE` {#restore}

`AccessRestorerFromBackup::touchesProtectedEntities` returns true when a user or role to be
restored carries the flag, or when `create_access = 'replace'` and an existing entity of the
same name is protected. `RestorerFromBackup` then adds `PROTECTED_ACCESS_MANAGEMENT` to the
access it checks for the caller before anything is written. `kCreate` fails on an existing
entity anyway, and `kCreateIfNotExists` keeps it.

For a row policy, quota or settings profile it also returns true when the restored `TO` set is
`ALL` or names an entity that is, or resolves by name to, a [protected target](#protected-targets).
The IDs in a backup are the backup's own, so each target is resolved by name the way the
restore resolves its dependencies. An ID the backup does not declare as a dependency is kept
verbatim by the restore, so it is checked as an ID of this server. With
`create_access = 'replace'`, the stored set of the entity being replaced is checked as well. Over-approximating `ALL`, without evaluating its
`EXCEPT` list against the server, keeps the check independent of the ID remapping.

`RESTORE ... ON CLUSTER` runs this check on the initiator: `BackupsWorker` builds a check-only
restorer for every host before it sends the query on.

## Concept {#concept}

An access check is only as good as the funnel it sits in, and the funnel moves between
releases. This feature has been defeated three times by a right check in the wrong place: the
25.8 base checked after `executeDDLQueryOnCluster`, interpreter checks alone left a race on
replicated storage, and 26.8 added a cascade that writes past both. When you add an
authorization check, list the write paths to the thing you protect, not the statements a user
can type, and write the list down; the next release will add one.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-but-rewrite`.

The feature is absent upstream; no `PROTECTED`, `isProtected` or `protected_flag` exists in
`src/Access` on 26.8.

- **Free.** `IAccessEntity.h`, `User.h`, `User.cpp`, `Role.h` and `Role.cpp` are unchanged
  between 26.3 and 26.8 upstream. `Role::equal` keeps upstream's `fetched_from_remote_at_ms` term
  next to the flag.
- **Expensive.** The storage layer: `DiskAccessStorage::insertImpl`, `removeImpl` and
  `updateImpl` were rewritten upstream, and `IAccessStorage.h` changed at exactly the
  `insertOrReplace` overloads and the `insertImpl`/`removeImpl` virtuals. The `CheckFunc`
  threading was re-derived against the new bodies.
- **New in 26.8.** `IAccessStorage::removeReferencesToRemovedIDs`, guarded
  [above](#the-cascade-a-door-that-is-new-on-268).
- **26.8.2.7 → 26.8.34.13.** Upstream now requires `DROP` for `CREATE ... OR REPLACE` of every
  access entity type ([#117960](https://github.com/ClickHouse/ClickHouse/pull/117960)); its check
  runs first and this patch's checks follow, which conflicted only in the include lists.
  A readonly session may be refused `SET ROLE`
  ([#118021](https://github.com/ClickHouse/ClickHouse/pull/118021)), in a branch of
  `InterpreterSetRoleQuery` this patch does not touch. A nested query under `EXECUTE AS` is now
  checked as the impersonated user
  ([#119848](https://github.com/ClickHouse/ClickHouse/pull/119848)), which is what makes
  self-protection and the privilege apply to the impersonated user rather than the caller.
  `Disk(...)` backup locations require a source grant
  ([#117406](https://github.com/ClickHouse/ClickHouse/pull/117406)); that narrows `RESTORE`
  but does not replace the [`RESTORE` check](#restore).
- **Not carried.** The 26.3 change in `InterpreterDropQuery.cpp` belongs to patch `019`, which
  writes its check with `PROTECTED_ACCESS_MANAGEMENT` itself, so `019` lands after this patch.

## Downstream contract {#downstream-contract}

The control plane is the only consumer, so its behaviour is the specification. It runs, as its
internal admin user defined in `users.xml`:

```sql
CREATE USER IF NOT EXISTS <name> IDENTIFIED WITH sha256_hash BY <hash> SALT <salt> PROTECTED
CREATE ROLE IF NOT EXISTS aiven_admin_role PROTECTED
CREATE ROLE IF NOT EXISTS aiven_readonly_role PROTECTED
```

The user form runs on every service powerup for the main service user and the monitoring user;
the role forms run from a keyed privileges action, followed by the roles' grants and
`GRANT aiven_admin_role TO avnadmin WITH ADMIN OPTION` with `SET DEFAULT ROLE`.

- **`IF NOT EXISTS` must be idempotent and concurrency-safe.** More than one node may run these
  statements at once; that is acceptable only because the flag is always set, so the service
  owns the entity either way. A tenant must therefore never be able to create a protected
  entity, or the retry would adopt it.
- **The keyword is accepted before and after `IDENTIFIED ... SALT ...`,** once. A repeated or
  contradictory `PROTECTED` / `NOT PROTECTED` in one statement is a syntax error (2026-11-19).
- **Protection must survive unrelated `ALTER` and `GRANT`.** A statement that does not mention
  `PROTECTED` leaves the flag alone (`std::optional<bool>`); a regression silently unprotects
  `avnadmin`.
- **The admin role may be granted onward.** Granting `aiven_admin_role` to, or revoking it from,
  a tenant's own users stays allowed, because those grantees are not protected; changing the
  role's own grants is refused.

The actor in the control plane's own tests is `avnadmin` itself: a protected user holding the
admin role and lacking only `PROTECTED_ACCESS_MANAGEMENT`. Self-protection and entity protection
therefore interact in almost every case, and neither may mask a regression in the other. The
[test matrix](#tests) restates those cases as SQL.

## Customer impact {#customer-impact}

- **Who:** every principal without `PROTECTED_ACCESS_MANAGEMENT`, which in a managed service is
  every customer-facing user, `avnadmin` included.
- **What they see:** `ACCESS_DENIED` naming `PROTECTED ACCESS MANAGEMENT` when dropping,
  renaming, replacing, altering, un-protecting, moving, granting to or revoking from a protected
  user or role, with or without `ON CLUSTER`; when a `TO` clause or the stored targets of a
  policy, quota or profile reach a [protected target](#protected-targets), which includes the
  users of `users.xml`; and when `RESTORE` would create or replace a protected entity, or
  restore a policy, quota or profile aimed at a protected target. A self-modification is
  refused with `User ... cannot modify themselves, even with PROTECTED_ACCESS_MANAGEMENT
  permission`. `SHOW CREATE USER` / `SHOW CREATE ROLE` shows the keyword.
- **`REVOKE ... FROM ALL`** is always refused with `cannot revoke rights from themselves`,
  because `ALL` contains the caller. Name the grantees, or use `FROM ALL EXCEPT <caller>, ...`.
  The same holds for `GRANT ... TO <caller> WITH REPLACE OPTION`.
- **Entities a protected entity references:** `DROP`, `CREATE ... OR REPLACE` and `MOVE` of a
  role, user or settings profile that a protected entity holds (a granted or default role, a
  grantee, a profile) need the privilege.
- **Policies, quotas and profiles aimed at a protected target:** creating, altering, replacing
  and dropping them all need the privilege, so a policy `TO ALL` can only be dropped by a holder
  of it.
- **Entities created before the upgrade:** a row policy, quota or profile created on 26.3 as
  `TO ALL EXCEPT <protected entities>` also reaches the `users.xml` users, so after the upgrade
  its owner can no longer alter, replace or drop it. A holder of the privilege has to rewrite or
  drop it; list them before the upgrade.

```sql
-- As avnadmin:
DROP ROLE aiven_readonly_role;
-- Code: 497. DB::Exception: avnadmin: Not enough privileges. To execute this query, it's
-- necessary to have the grant PROTECTED ACCESS MANAGEMENT ON *.*. (ACCESS_DENIED)

-- Accepted only when the EXCEPT list also names every user of `users.xml`
-- (`SELECT name FROM system.users WHERE storage = 'users_xml'`).
CREATE ROW POLICY p ON db.t USING 1 TO ALL EXCEPT avnadmin, aiven_monitoring, <users.xml users>;
```

- **New versus 26.3:** dropping, replacing or moving a role, user or profile a protected entity
  references is refused instead of silently rewriting the protected entity;
  `TO ALL EXCEPT <protected entities>` works again, where 26.3 refused every `TO ALL`; `ALTER`,
  `OR REPLACE` or `DROP` of a policy, quota or profile whose stored targets reach a protected
  entity is refused; a `TO` clause reaching a `users.xml` user needs the privilege; `RESTORE` of
  a protected entity, or of a policy, quota or profile aimed at a protected target, needs the
  privilege; `GRANT ... TO <caller> WITH REPLACE OPTION` is refused; a repeated or contradictory
  `PROTECTED` is a syntax error.
- **How to restore upstream behaviour:** grant `PROTECTED`, or clear the flag with
  `ALTER USER ... NOT PROTECTED` / `ALTER ROLE ... NOT PROTECTED` as a holder of it.
- **Upstream tests edited:** `03274_precise_alter_user_grants`, see
  [below](#upstream-tests-this-patch-diverges-from).

## Known residual {#known-residual}

- **`EXECUTE AS`.** Impersonation runs a statement as another user, so a principal allowed to
  impersonate a holder of the privilege acts with it. The managed admin role does not include
  `IMPERSONATE`; that grant list is the only safeguard.
- **`BACKUP` of `system.users` and `system.roles`** writes every entity of the backed-up
  storages, protected ones included, with their authentication data. It needs only `BACKUP` on
  that table; the managed admin role does not hold `BACKUP`, which is the only safeguard.
- **Masking policies** have a `TO` clause like row policies, and 022 does not check it. This
  build refuses masking-policy statements outright; enabling them needs the target check.

## Security review {#security-review}

The write paths to a protected entity, and what guards each:

| Path | Guard |
|---|---|
| `CREATE`/`ALTER`/`DROP`/`RENAME`/`MOVE` of a user or role, `OR REPLACE`, `GRANT`/`REVOKE`, with or without `ON CLUSTER` | interpreter check above the dispatch, then the storage `CheckFunc` |
| A statement executed under `EXECUTE AS` | the same checks, as the impersonated user |
| `TO` clause of row policies, quotas, settings profiles, `SET DEFAULT ROLE` | `checkProtectedTargets` on the clause |
| `GRANT ... WITH REPLACE OPTION` to oneself | refused as a revoke from oneself |
| `ALTER` / `OR REPLACE` / `DROP` of a row policy, quota or profile | `checkProtectedTargets` on the stored set |
| Cascade of a `DROP`, `CREATE ... OR REPLACE` or `MOVE` into referencing entities | the statement is refused on the initiator |
| `RESTORE` of users and roles | required access of the restore |
| `RESTORE` of row policies, quotas, settings profiles | required access of the restore, on the restored and the replaced `TO` sets |
| `users.xml` users themselves | read-only to SQL |
| `users.xml` users as the `TO` target of another entity | `checkProtectedTargets` and the `RESTORE` check, as [protected targets](#protected-targets) |
| ZooKeeper and the access directory on disk | outside SQL; the service's own credentials |

Creating a protected entity, or setting or clearing the flag, needs the privilege too, so a
tenant cannot pre-create an entity the control plane will later adopt with `IF NOT EXISTS`.
The [known residuals](#known-residual) are the paths not covered.

## Tests {#tests}

- **Paths:**
  - `tests/queries/0_stateless/aiven_022_protected_user_management.sh`: the user matrix against
    a principal with access management but not `PROTECTED`, one with `PROTECTED`, and self;
    `IF NOT EXISTS` idempotency, the keyword in both positions, and an unrelated `ALTER`/`GRANT`
    leaving the flag intact.
  - `aiven_022_protected_roles.sh`: the same for roles, plus `ALTER ROLE ... SETTINGS` and
    `REVOKE ... FROM <protected role>`.
  - `aiven_022_protected_entity_targets.sh`: the `TO` clause, including a plain role as a
    target, `TO ALL EXCEPT <protected entities>` (allowed) and plain `TO ALL` (refused).
  - `aiven_022_protected_stored_targets.sh`: `ALTER` and `OR REPLACE` without a `TO` clause on
    entities whose stored set reaches a protected user, a protected role or `ALL`.
  - `aiven_022_protected_auth_replace_execute_as.sh`: `ADD IDENTIFIED`,
    `RESET AUTHENTICATION METHODS` and `VALID UNTIL`; `OR REPLACE` refused for `DROP` first and
    for `PROTECTED` second; `EXECUTE AS` checked as the impersonated user.
  - `aiven_022_protected_restore.sh`: `RESTORE` replacing an existing protected user and
    creating one from a backup that carries the flag, refused without the privilege and
    allowed with it; an unprotected restore stays allowed.
  - `aiven_022_protected_dependency_cascade.sh`: dropping a role granted to a protected user is
    refused, and the user's `granted_roles` is unchanged.
  - `aiven_022_protected_xml_and_restore_targets.sh`: a `TO` clause naming a `users.xml` user is
    refused without the privilege; `RESTORE` of a quota, settings profile or row policy aimed at
    a protected entity, a `users.xml` user or `ALL` is refused, as is replacing a quota whose
    stored set reaches a protected entity, and restoring a backup whose target ID is not
    declared as a dependency.
  - `aiven_022_protected_replace_move_drop.sh`: `GRANT ... TO <self> WITH REPLACE OPTION` for
    privileges and roles, also with the privilege; `CREATE ROLE/USER ... OR REPLACE` and `MOVE`
    of a role or user a protected user references; `DROP` of a quota, row policy or settings
    profile aimed at a protected user, a protected role, a `users.xml` user or `ALL`, also with
    `ON CLUSTER` and `IF EXISTS`; a repeated or contradictory `PROTECTED`. Each group has an
    allowed control for an unrelated entity or a holder of the privilege.
  - `tests/integration/test_aiven_protected_users/`, `tests/integration/test_aiven_protected_roles/`:
    two nodes with a replicated access storage; `ON CLUSTER` refused on the initiator, the flag
    replicates, and the state is asserted on every node, not only on the initiator.
- **Proves:** every denial asserts `ACCESS_DENIED` naming the expected privilege and that the
  entity is unchanged afterwards.
- **Run:** `tests/clickhouse-test aiven_022`, and
  `python -m ci.praktika run "integration" --test test_aiven_protected_users test_aiven_protected_roles`.
- **Evidence (2026-11-19):** the nine `aiven_022_*` stateless tests and the related upstream access tests
  (`05078`, `05213`, `03274`, `01271`, `03231`, `04027`) pass. With the stored-target and `RESTORE` checks disabled,
  18 stored-target denials and both restore denials are not raised. With the read-only-storage rule and the
  `RESTORE` target check disabled, all ten denials of `aiven_022_protected_xml_and_restore_targets` are not raised.
  With the `WITH REPLACE OPTION`, `OR REPLACE`, `MOVE`, `DROP`-target and parser checks disabled,
  `aiven_022_protected_replace_move_drop` fails in every group: the role replace on oneself, both
  `OR REPLACE` forms, `MOVE` and every `DROP` succeed and strip the protected user's reference or
  drop the entity, and the repeated keyword parses. In the upstream access suite, every failure is
  an environment gap of the local server (missing system log tables, S3, clusters, executable UDFs); none involves
  `PROTECTED ACCESS MANAGEMENT` or the self-protection messages. The integration suites `test_aiven_protected_users`
  and `test_aiven_protected_roles` pass (9 of 9) on a two-node replicated access storage cluster.

### Upstream tests this patch diverges from {#upstream-tests-this-patch-diverges-from}

`03274_precise_alter_user_grants` used a user altering itself as the positive arm for
`GRANT ALTER USER ON <user>` scoping. Self-protection forbids that, so those arms use a third
party as the actor; the grant-scoping assertions are unchanged. Run the upstream access tests by
hand when touching this subsystem: the Aiven stateless lane selects only `aiven_*`, and that
blind spot is how the 26.3 `TO ALL` refusal reached a release.

## Rollback {#rollback}

- **Downgrade:** not supported; services only move forward.
- **Revert in a later release:** the flag is stored in the entity definition on disk and in
  ZooKeeper, so a binary without the keyword cannot parse a protected entity. Clear the flag
  with `NOT PROTECTED` first.
- **Disable without rebuild:** grant nobody `PROTECTED` and mark nothing `PROTECTED`; every check
  is then inert. The control plane marks four entities on every powerup.

## Retirement {#retirement}

An upstream mechanism that protects individual access entities from holders of access
management. Re-check upstream on every uplift. Until then the patch stays, and the release that
drops it must first clear the flag on every entity, because the keyword is part of the stored
definition.

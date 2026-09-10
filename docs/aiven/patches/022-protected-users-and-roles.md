---
description: 'Dossier for the Aiven patch that marks service-managed users and roles PROTECTED so only a holder of PROTECTED_ACCESS_MANAGEMENT can change them'
sidebarTitle: '022: Protected users and roles'
slug: '/aiven/patches/022-protected-users-and-roles'
title: 'Patch 022: Protected users and roles'
doc_type: 'reference'
---

# Patch 022 — Protected users and roles {#patch-022-protected-users-and-roles}

## Lineage {#lineage}

| LTS | How to find | Outcome |
|---|---|---|
| 25.8 | `ce74bc008d` (users, base) + `f4552b14c77` (`ON CLUSTER` fix) + `827d8ebb573d` (roles) on `v25.8.24.21-lts-aiven` | carried as three commits |
| 26.3 | `patch-port(022)` at `785f00febc45`, `patch-port(079)` at `c2a4fc417327`, `patch-fix(022,079)` at `d158b35e863e` | ported as three commits; `022` needed a rewrite against the drifted Access subsystem |
| 26.8 | `patch-port(022):` | `still-needed-but-rewrite`, **one commit** |

On 26.8 this patch carries identity `022` and absorbs `079`, per
[consolidate at port time](../uplifts/26.8/execution-plan.md#consolidate). Five
pieces of prior work land together: the protected-user base, the `ON CLUSTER`
hoist, protected roles, the `optional<bool>` round-trip fix, and the
`checkProtectedTargets` type-erasure fix. A sixth piece is new on this line —
the [dependency-cascade guard](#the-cascade-a-door-that-is-new-on-268).

The three older lines are only useful as a source of *intent*. The 26.3
implementation is not a template for 26.8: see [drift](#drift-on-this-uplift).

## Background (ClickHouse) {#background}

ClickHouse keeps users, roles, quotas, row policies and settings profiles as
**access entities** behind a common interface, `IAccessStorage`. In a managed
deployment three storages are stacked under a `MultipleAccessStorage`: the
read-only one backed by `users.xml`, a `DiskAccessStorage` holding entities
created over SQL, and a `ReplicatedAccessStorage` that keeps them in ZooKeeper
so every replica agrees. All writes funnel into three virtual methods —
`insertImpl`, `removeImpl` and `updateImpl`.

Above that sit the interpreters in `src/Interpreters/Access/`, one per statement
family, which translate SQL into those calls after checking privileges.

The detail that shapes this whole patch is how `ON CLUSTER` works. When a
statement carries `ON CLUSTER`, the interpreter calls `executeDDLQueryOnCluster`
and **returns immediately** — the statement is written to a ZooKeeper queue and
later replayed on every replica by `DDLWorker`, under a different identity than
the one that typed it. So any check placed *after* that call never runs on the
initiator, and cannot be re-created later: by the time `DDLWorker` executes the
statement, the original user is gone from the picture. A privilege check is only
worth anything if it sits **above** the dispatch.

Privileges themselves are an enum in `src/Access/Common/AccessType.h`, checked
with `checkAccess` (throws `ACCESS_DENIED`) or `isGranted` (returns a bool).

## Component tour {#component-tour}

The patch is wide but shallow — one thin change in each of five layers.

**Grammar.** `CommonParsers.h` gains the `PROTECTED` keyword.
`ParserCreateUserQuery` and `ParserCreateRoleQuery` accept it as an option in
the statement's option loop, which is why it may appear either before or after
an `IDENTIFIED` clause. `ASTCreateUserQuery` and `ASTCreateRoleQuery` carry it
as `std::optional<bool> protected_flag`, and their `formatImpl` must re-emit
it — the AST round-trips through text in more places than is obvious, which is
what the `optional<bool>` fix is about.

**Entity.** `IAccessEntity` gains a virtual `isProtected()` returning `false`,
overridden by `User` and `Role`, which also add the flag to `equal` so a
protection change counts as a real change. This is the only layer that is free
on 26.8 — see [drift](#drift-on-this-uplift).

**Privilege.** `AccessType.h` gains `PROTECTED_ACCESS_MANAGEMENT`, spelled
`PROTECTED` in SQL and rendered as `PROTECTED ACCESS MANAGEMENT` in error text.
It sits under `ALL`, not under `ACCESS MANAGEMENT` — granting a tenant access
management therefore does not grant it.

**Interpreters.** `InterpreterCreateUserQuery`, `InterpreterCreateRoleQuery`,
`InterpreterDropAccessEntityQuery`, `InterpreterGrantQuery` and
`InterpreterMoveAccessEntityQuery` each resolve their targets and check
protection **above** the `ON CLUSTER` dispatch. A shared helper,
`checkProtectedTargets`, does the same for the `TO <grantee>` clause of
`CREATE ROW POLICY` / `QUOTA` / `SETTINGS PROFILE` and `SET DEFAULT ROLE`.
`InterpreterShowCreateAccessEntityQuery` renders the keyword back out.

**Storage.** `IAccessStorage` threads a `CheckFunc` — a closure the interpreter
builds — through `insertImpl` and `removeImpl` down into `MemoryAccessStorage`,
`DiskAccessStorage` and `ReplicatedAccessStorage`/`ZooKeeperReplicator`, so the
entity is re-checked under the storage lock immediately before it is
overwritten or deleted. This is the layer that costs the most to port.

## Problem {#problem}

Aiven's control plane needs `avnadmin` to be a real SQL user with scoped
privileges rather than a hardcoded `users.xml` entry, and needs two
service-managed roles (`aiven_admin_role`, `aiven_readonly_role`) plus a
monitoring user to exist as ordinary access entities. But `avnadmin` holds the
admin role, and that role carries `DROP ROLE`, `ALTER ROLE`, `ROLE ADMIN` and
access management.

Without this patch the customer-facing principal can therefore delete or rewrite
the entities the service depends on, over the plain SQL interface:
`DROP ROLE aiven_admin_role`, `ALTER ROLE aiven_readonly_role RENAME TO …`,
`CREATE USER OR REPLACE avnadmin IDENTIFIED WITH no_password`,
`DROP USER <monitoring_user>`. The service loses the ability to manage the
cluster, and monitoring goes blind. None of it requires an exploit — it is the
documented behaviour of privileges the tenant legitimately holds.

The fallback without the feature is to keep these principals in `users.xml`,
which means they cannot be granted or revoked per service, cannot be rotated
over SQL, and cannot hold fewer privileges than the XML format allows.

## Approach {#approach}

Add a per-entity `Protected` flag and a global privilege that gates it: a
protected user or role may only be created, altered, renamed, replaced, dropped,
moved between storages, or have its grants changed by a principal holding
`PROTECTED_ACCESS_MANAGEMENT`. Aiven's internal admin holds that privilege;
`avnadmin` does not.

Enforcement sits at four points, and all four are load-bearing:

1. **Above the `ON CLUSTER` dispatch**, in every interpreter that can reach a
   protected entity. This is the check that actually protects the entity; the
   rest is depth.
2. **At the storage write**, through the `CheckFunc`, re-reading the existing
   entity under the storage lock before it is replaced or removed. This closes
   the time-of-check-to-time-of-use window that the replicated storage would
   otherwise have, where two nodes race on the same ZooKeeper node.
3. **Self-protection**, which is unconditional: a principal may not create,
   replace, alter, rename, drop or revoke from *itself*, *even holding the
   privilege*. Note how wide that is — it covers changing your own password and
   your own settings, not merely the irrecoverable operations. That is broader
   than the phrase "drop or revoke from themselves" suggests, and it is the rule
   the code has implemented on both prior LTS lines; the narrower wording in
   earlier dossiers was simply inaccurate. It matches production `avnadmin`
   behaviour, where the control plane owns the credential and a user rotating it
   out of band would desynchronise it. This is a ratified decision — do not relax
   it. The `GRANT` path compares grantees by both UUID and name, because the
   resolved grantee id does not always equal `getUserID`. For the boundary of the
   rule — why it deliberately does *not* extend to the `TO` clause of a policy —
   see [below](#to-all-and-the-boundary-of-self-protection).
4. **The dependency cascade**, new on this line — see below.

### The cascade: a door that is new on 26.8 {#the-cascade-a-door-that-is-new-on-268}

26.8 added `IAccessStorage::removeReferencesToRemovedIDs`. After any top-level
`remove()`, it walks every access entity type and calls `update()` on anything
that referenced the deleted id, so no dangling references are left behind. That
is a good change, and it opens a door behind both of the checks above: it writes
through `updateImpl`, which carries no `CheckFunc`, and it runs as a side effect
with no user identity in scope.

What it can strip from a protected entity is `default_roles`, `granted_roles`,
`grantees` and `settings` — role grants and settings-profile references. It does
**not** touch the entity's direct privileges (`access`), so this is narrower than
a `REVOKE`. The exposure is any *non-protected* entity that a protected one
references: drop that, and a principal without `PROTECTED_ACCESS_MANAGEMENT` has
caused a protected entity's definition to be rewritten.

In the current control-plane configuration this is not reachable, because the roles
granted to the protected users are themselves protected and so cannot be
dropped. That is a property of how the control plane happens to assign grants,
not a server-side invariant, and `avnadmin` can freely create and drop ordinary
roles and settings profiles. We guard it rather than depend on the coincidence.

**The guard goes in the interpreter, not the storage.** Deny the triggering
`DROP` on the initiator, above the `ON CLUSTER` dispatch, when a protected
entity depends on the target and the actor lacks the privilege. Putting a
`CheckFunc` on `updateImpl` instead would be wrong twice over: the cascade has
no user identity to check against down there, and refusing the update would
leave protected entities holding exactly the dangling references upstream added
the cascade to clean up. The dependency scan is the same `O(N)` walk the cascade
already performs, so the check is not a new cost.

### `TO ALL` and the boundary of self-protection {#to-all-and-the-boundary-of-self-protection}

The `TO` clause of `CREATE`/`ALTER ROW POLICY`, `QUOTA` and `SETTINGS PROFILE` is
a second way to disable an account without touching it: leave the protected user
alone and attach a quota of one query per hour, or a profile carrying
`max_memory_usage = 1`. `checkProtectedTargets` covers that, and it has to.

It must **not**, however, refuse a target set merely because the caller is inside
it. Those three statements store the set on the policy entity itself, as
`to_roles`; the listed users are never rewritten, so naming yourself there is not
self-modification. The 26.3 port applied self-blocking anyway, on the stated
premise that the clause "rewrites the listed users and roles". That premise is
false, and the consequence is severe: `RolesOrUsersSet::getMatchingIDs` expands
`ALL` to every user and role on the server, so the caller is *always* in the set
and **every `TO ALL` statement is refused on any server**, whether or not
anything is protected. It fails 42 upstream stateless tests, and it is live on
the 26.3 line today, where tenants cannot use `TO ALL` at all and are told they
need a privilege that would not help them. That is tracked separately from this
port.

`SET DEFAULT ROLE ... TO` is the one member of the group that really does rewrite
its targets, writing `default_roles` on each, so a protected target there needs
the privilege for a genuine reason. It still gets no self-blocking, and that is a
deliberate narrow divergence from 26.3. Reinstating the self-check there would
re-break `SET DEFAULT ROLE ... TO ALL` for exactly the reason above; and blocking
self only when it is *named*, while `ALL` still reaches it, would be incoherent,
since the `ALL` spelling accomplishes the same self-modification. A protected
target is refused either way, so all that actually changes is that a
non-protected user may set its own default roles — which is upstream behaviour.

**`ALL` is not narrowed.** On a server with protected entities, `TO ALL` resolves
to a set containing them, so an unprivileged principal is refused and has to
write `TO ALL EXCEPT <protected entities>` instead. Silently adding the protected
entities to the `EXCEPT` list on the caller's behalf was considered and rejected:
it would freeze the exclusion at DDL time, so an entity protected later would not
be covered, and it would make the stored policy depend on who created it.
Refusing is honest, and the workaround is one clause.

The cost is that the check is server-global. A single protected entity anywhere
makes every unprivileged `TO ALL` fail, including in statements with no relation
to it. That is why the stateless tests here are tagged `no-parallel`: a protected
user they create would otherwise deny `TO ALL` in whatever unrelated test
happened to be running beside them.

## Concept {#concept}

**An access check is only as good as the funnel it sits in, and the funnel moves
between releases.**

Three times now this feature has been defeated not by a wrong check but by a
right check in the wrong place. The 25.8 base checked protection *after*
`executeDDLQueryOnCluster`, so `ON CLUSTER` walked past it. The interpreter
checks alone leave a race on replicated storage, so the `CheckFunc` re-checks
under the storage lock. And now 26.8 has added a write path that reaches
entities without passing either.

The rule of thumb: when you add an authorization check, enumerate the *write
paths* to the thing you are protecting rather than the *statements* a user can
type, and write the enumeration down — because the next release will add one.
A statement-shaped check answers "can this user run this SQL"; a path-shaped
check answers "can this entity be modified", which is the actual invariant.

The corollary for reviews: a diff that adds a check is not reviewable on its own.
You have to ask what else writes here.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-but-rewrite`.

The feature is entirely absent upstream — no `PROTECTED`, `isProtected` or
`protected_flag` anywhere in `src/Access` on 26.8 — so the premise holds
unchanged. Every source file the 26.3 commits touched still exists. Of the 41
touched, 25 moved upstream between the lines, by +902/−160. The split is sharp:

- **Free.** `IAccessEntity.h`, `User.h`, `User.cpp`, `Role.h` and `Role.cpp` are
  byte-identical between 26.3 and 26.8 upstream. The entity-layer work applies
  as written. `Role.h` and `Role.cpp::equal` will hit the same additive conflict
  with upstream's `fetched_from_remote_at_ms` that 26.3 hit — keep both members,
  and both terms in the equality expression.
- **Expensive.** The storage layer, which was already the hardest part at 26.3.
  `DiskAccessStorage::insertImpl` grew from 30 to 82 lines, `removeImpl` and
  `updateImpl` were both rewritten, and `IAccessStorage.cpp` gained 123 lines.
  `IAccessStorage.h` changed at exactly the two places the patch edits: the
  `insertOrReplace` overloads and the protected `insertImpl`/`removeImpl`
  virtuals. The structural model survives — `DiskAccessStorage` still delegates
  to an internal `MemoryAccessStorage` whose `*NoLock` methods still carry
  `bool notify = true` — so the 26.3 shape is still right in outline, but the
  `CheckFunc` threading must be re-derived against rewritten bodies rather than
  re-applied.
- **New.** `IAccessStorage::removeReferencesToRemovedIDs`, covered
  [above](#the-cascade-a-door-that-is-new-on-268).

One 26.3 hunk is **not** carried. `022` there upgraded a privilege check inside
`InterpreterDropQuery.cpp` from `ACCESS_MANAGEMENT` to
`PROTECTED_ACCESS_MANAGEMENT`, but that block is Aiven code introduced by `019`,
not upstream, and it does not exist on 26.8. Rather than reordering the two,
`019` writes the check with the right privilege when it lands. See the
[ordering constraints](../uplifts/26.8/execution-plan.md#ordering-constraints).

## Downstream contract {#downstream-contract}

The control plane is the only consumer of this feature, so its behaviour is the
specification. Three statement forms are issued against a live service, and the
port must keep all of them working:

```sql
CREATE USER IF NOT EXISTS <name> IDENTIFIED WITH sha256_hash BY <hash> SALT <salt> PROTECTED
CREATE ROLE IF NOT EXISTS aiven_admin_role PROTECTED
CREATE ROLE IF NOT EXISTS aiven_readonly_role PROTECTED
```

The control plane runs the user form twice on **every service powerup** — once
for the main service user (`avnadmin`), once for the monitoring user — and runs
the role forms from a keyed privileges action. Three properties follow, and none
of them were covered by the 26.3 tests:

- **`IF NOT EXISTS` must be idempotent and concurrency-safe.** The powerup
  action is deliberately non-exclusive, so more than one node may run these
  statements simultaneously, and that is only acceptable because the flag is
  always set — the service knows it owns the entity either way.
- **The keyword is accepted in two positions.** Production emits `PROTECTED`
  *after* the `IDENTIFIED … SALT …` clause; the control plane's own tests emit
  it *before* (`CREATE USER bob PROTECTED IDENTIFIED WITH sha256_password BY …`).
  Both must parse.
- **Protection must survive unrelated `ALTER` and `GRANT` statements.** The
  powerup path re-runs privilege updates against these users every time. This is
  what the `optional<bool>` round-trip fix exists for: a statement that does not
  mention `PROTECTED` must leave the flag alone rather than clearing it. A
  regression here silently unprotects `avnadmin` in production.

The threat model is also sharper than a synthetic test suggests. The actor in
the control plane's tests is `avnadmin` itself: a **protected** user holding the admin
role with `DROP ROLE`, `ALTER ROLE`, `ROLE ADMIN` and access management, lacking
only `PROTECTED_ACCESS_MANAGEMENT`. Self-protection and entity-protection
therefore interact in almost every case — `DROP USER avnadmin` is at once a
self-drop and a protected-entity drop, and the port must not let either check
mask a regression in the other.

The control plane's ACL suite and its multi-node cluster suite hold roughly
forty denial cases between them. They are the source for the
[test matrix](#tests) below. Those suites are not public, so the cases are
restated here as SQL rather than cited by path — this section and the
[test matrix](#tests) are the fork's copy of the contract.

## Customer impact {#customer-impact}

Mostly unchanged from 26.3 — the same statements are refused with the same
error — but two behaviours differ, one more restrictive and one less.

- **Who.** Any principal without `PROTECTED_ACCESS_MANAGEMENT`, which in a
  managed service means every customer-facing user, `avnadmin` included.
- **What they see.** `ACCESS_DENIED` naming `PROTECTED ACCESS MANAGEMENT`, when
  dropping, renaming, replacing, altering, un-protecting, moving or granting
  to/revoking from a protected user or role, with or without `ON CLUSTER`.
  `SHOW CREATE USER` / `SHOW CREATE ROLE` renders the `PROTECTED` keyword, so
  protection is visible rather than a hidden property.
- **More restrictive than 26.3.** Dropping a role or settings profile that a
  protected user or role references is now refused for a principal without the
  privilege, instead of silently rewriting the protected entity's definition.
  This closes the [cascade door](#the-cascade-a-door-that-is-new-on-268).
- **Less restrictive than 26.3.** `TO ALL` in `CREATE`/`ALTER ROW POLICY`,
  `QUOTA` and `SETTINGS PROFILE` becomes usable again for the part of the fleet
  a tenant is entitled to touch. On 26.3 every such statement is refused
  outright, for the [reason described above](#to-all-and-the-boundary-of-self-protection);
  here `TO ALL EXCEPT <protected entities>` succeeds. Plain `TO ALL` is still
  refused, but now for the right reason and with an error naming a privilege
  that genuinely governs it.
- **How to restore.** Grant `PROTECTED ACCESS MANAGEMENT`, or clear the flag
  with `ALTER USER … NOT PROTECTED` / `ALTER ROLE … NOT PROTECTED` from a
  principal that holds it. No rebuild, no restart.

## Tests {#tests}

Four vehicles. The stateless tests carry the matrix; the integration tests are
the only ones that can exercise the `ON CLUSTER` hoist and the ZooKeeper
replication path, because both need more than one node.

- `tests/queries/0_stateless/aiven_022_protected_user_management.sh` — the core
  user matrix against three actors: a principal with access management but not
  `PROTECTED`, a principal with `PROTECTED`, and self. Drop, rename, replace,
  password change, settings, settings profile, `REVOKE ALL`, self-drop and
  self-revoke. Extends the 26.3 version with the
  [downstream contract](#downstream-contract) cases: `IF NOT EXISTS` idempotency,
  `PROTECTED` both before and after an `IDENTIFIED … SALT …` clause, and an
  unrelated `ALTER`/`GRANT` leaving the flag intact.
- `tests/queries/0_stateless/aiven_022_protected_roles.sh` — the same matrix for
  roles, plus `ALTER ROLE … SETTINGS`, `REVOKE … FROM <protected role>` and
  `CREATE ROLE IF NOT EXISTS … PROTECTED`.
- `tests/queries/0_stateless/aiven_022_protected_entity_targets.sh` — the
  `checkProtectedTargets` surface: the `TO <grantee>` clause of `CREATE ROW
  POLICY` / `QUOTA` / `SETTINGS PROFILE` and `SET DEFAULT ROLE`. Must include a
  **plain role** in a `TO` set, which is the case that regressed at 26.3 into a
  `LOGICAL_ERROR` because the helper cast every target to `User`. Also pins the
  [`TO ALL` boundary](#to-all-and-the-boundary-of-self-protection):
  `TO ALL EXCEPT <protected entities>` has to succeed for an unprivileged
  principal even though `ALL` contains the caller, and plain `TO ALL` has to be
  refused for reaching the protected entities rather than for reaching the
  caller.
- `tests/queries/0_stateless/aiven_022_protected_dependency_cascade.sh` — new on
  this line, and the one test with no prior-LTS precedent. Grant an ordinary
  role to a protected user, drop the role as a principal without the privilege,
  and assert the drop is refused and the protected user's `granted_roles` is
  unchanged. This is the FAIL-on-parent case for the cascade guard.
- `tests/integration/test_aiven_protected_users/` and
  `tests/integration/test_aiven_protected_roles/` — two nodes plus Keeper, with
  `user_directories` pointing at a `<replicated>` ZooKeeper path. Two properties:
  an `ON CLUSTER` statement is refused on the initiator, and the protected flag
  replicates so enforcement holds on the second node.

  **Assert per replica, not just on the initiator.** After the `ACCESS_DENIED`,
  query *every* node as admin and assert the state did not move — the user still
  exists, the renamed-to name does not, the role's grants are still there. An
  initiator-only assertion cannot distinguish "refused" from "refused after
  queueing", and the queued form is the failure that matters. This is the
  control plane's own pattern in its cluster suite, and it is stronger than what
  26.3 had.

Evidence: pending — FAIL on parent / PASS with patch to be recorded at dispatch.

### Upstream tests this patch diverges from {#upstream-tests-this-patch-diverges-from}

`03274_precise_alter_user_grants` used a user altering *itself* as the positive
arm for precise `GRANT ALTER USER ON <user>` scoping. Self-protection forbids
that, so those arms were rewritten to use a third party as the actor; every
assertion the test makes about grant scoping is unchanged, and only the choice of
actor moved.

It is recorded here for a reason. A diverged upstream test that nobody wrote down
is indistinguishable from a regression the next porter introduced, and the Aiven
stateless lane selects only `aiven_*` — so nothing in our own CI exercises the
several hundred upstream access tests. That blind spot is how the inherited
[`TO ALL` self-check](#to-all-and-the-boundary-of-self-protection) reached a
shipped LTS while breaking 42 of them. Run the upstream access suite by hand when
touching this subsystem; that defect was found no other way.

## Rollback {#rollback}

Reverting the commit is safe: the feature is additive — one privilege, one bool
per entity, one keyword, and the checks. A binary without the feature
deserializes a protected entity as an ordinary one and ignores the flag.

Persistent state does outlive a revert. The flag is written into the entity
definition on disk and in ZooKeeper, so on a downgrade the older parser meets a
keyword it does not know. Clear protection with `ALTER USER … NOT PROTECTED` and
`ALTER ROLE … NOT PROTECTED` before downgrading if a strict round-trip is
required.

To disable without rebuilding, grant nobody `PROTECTED_ACCESS_MANAGEMENT` and
mark nothing `PROTECTED`; every check is then inert. Note that the control plane
marks four entities protected on every service powerup, so this is a fork-level
statement, not something to try on a managed service.

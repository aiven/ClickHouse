# Patch 022 — protected-users

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `ce74bc008d` (base) + `f4552b14c77` (ON CLUSTER fix) | Tilman Moeller (author) `tilman.moeller@aiven.io` (base), Joe Lynch `joelynch112@gmail.com` (fix), committed by `joelynch112@gmail.com` | (the version we are porting FROM) |
| 26.3-aiven | `patch-port(022)` (single combined commit) | parent agent, 2026-06-09 | `still-needed-but-rewrite` — Access subsystem drifted; storage write paths re-threaded against 26.3 signatures; 3 interpreters reconstructed in their final post-fix form |

`byte_equivalent: false` — the Access subsystem changed substantially between
25.8 and 26.3, so several hunks were adapted to current signatures rather than
applied verbatim, and two blocks of debug-only logging shipped by the base were
dropped (see §3). The combined feature is landed as ONE commit.

## 1. Purpose

Add a `Protected` flag to each user. Protected users can only be created,
altered, or removed by a principal holding a new global privilege
`PROTECTED_ACCESS_MANAGEMENT` (`PROTECTED`). The privilege is meant for Aiven's
internal admin (`avnadmin`); the `Protected` flag is given to the main service
user so it can exist as a real SQL user (with fewer privileges than a hardcoded
XML user) yet remain non-removable / non-alterable by ordinary cluster users.

The feature also enforces **fleet-wide self-protection**: a user can never drop
or revoke rights from themselves, even with `PROTECTED_ACCESS_MANAGEMENT`. This
matches production `avnadmin` behavior and is a parent-ratified decision (do not
change).

Source SHAs on `v25.8.18.1-lts-aiven`:
- BASE `ce74bc008d` — "Added support for protected users" (author
  `tilman.moeller@aiven.io`, co-authored by Kevin Michel).
- FIX `f4552b14c77` — "Enforce protected-user checks before ON CLUSTER dispatch"
  (author `joelynch112@gmail.com`).

### Why the fix is mandatory (the ON CLUSTER hole)

In the base, the self-protection + `isProtected`→`PROTECTED_ACCESS_MANAGEMENT`
checks for `DROP`, `CREATE`/`ALTER USER`, and `GRANT`/`REVOKE` were evaluated
only on the LOCAL execution path (inside storage callbacks or after the cluster
dispatch). For `ON CLUSTER` statements the interpreters return early via
`executeDDLQueryOnCluster` BEFORE those checks ran — the initiator skipped the
check and the query was forwarded through `DDLWorker`, where it executes under a
different identity and the original-user check can no longer be enforced. The
fix HOISTS the checks ABOVE the `if (!query.cluster.empty()) return
executeDDLQueryOnCluster(...)` dispatch in the three interpreters, and hardens
the `GRANT` self-revoke check to compare grantees by BOTH UUID and name.

## 2. Upstream-drift findings

> Verify the patch is still SEMANTICALLY needed against `v26.3.10.62-lts`.

### Findings

- `PROTECTED_ACCESS_MANAGEMENT` / a `Protected` flag / `PROTECTED` keyword: **no
  upstream equivalent** on 26.3. The feature's premise holds → **still needed**.
- The Access subsystem drifted in ways that touch the patch's mechanism:
  - `IAccessStorage` write API: `insertImpl` / `removeImpl` signatures are the
    integration point for the `CheckFunc` — unchanged in shape, so the `CheckFunc`
    overloads thread cleanly.
  - `DiskAccessStorage` was refactored to **delegate to an internal
    `MemoryAccessStorage memory_storage`** (no own `entries_by_id`/`Entry`); the
    25.8 hunks that manipulated `Entry` directly no longer apply.
  - `MemoryAccessStorage::insertNoLock`/`removeNoLock`/`updateNoLock` gained a
    trailing `bool notify = true` parameter on 26.3.
  - `ZooKeeperReplicator` gained `Coordination::setCurrentComponent(...)` guards
    and bumped retry attempts from 10 → 1000.
  - `InterpreterDropAccessEntityQuery` gained a `MASKING_POLICY` branch and a
    `do_drop(names, storage_name)` shape.
  - `InterpreterCreateUserQuery::updateUserFromQueryImpl` gained an extra
    `override_roles` parameter.
  - `InterpreterDropQuery` already carries a prior Aiven block (`cluster_database`
    gating) that checked `ACCESS_MANAGEMENT`.
- Conclusion: **`still-needed-but-rewrite`** — semantics required; several hunks
  adapted to 26.3 signatures (§3).

## 3. Conflict resolution, per file

38 files; ~28 applied cleanly via `git apply --3way`. The 9 hotspots below were
resolved manually. The combined (base + fix) final state is what is staged.

### Storage layer (CheckFunc threading)

- **`src/Access/MemoryAccessStorage.{h,cpp}`** — MERGE of two independent
  parameter additions: keep 26.3's `bool notify = true` AND add the patch's
  `const CheckFunc & check_func`. Final `insertNoLock`/`removeNoLock` carry both;
  `check_func(new_entity)` is invoked at the top of `insertNoLock`, and
  `check_func(it->second.entity)` before a `removeNoLock`. Internal
  collision-removal calls pass `CheckFunc{}` (the new entity was already checked).
- **`src/Access/DiskAccessStorage.cpp`** — 26.3 delegates to `memory_storage`, so
  the 25.8 `Entry`-based hunks were discarded and the INTENT re-expressed against
  the current shape:
  - `insertNoLock`: on a name collision (the `CREATE USER OR REPLACE` path), read
    the colliding entity via `memory_storage.read(*collision_id, ...)` and call
    `check_func(existing_entity)` BEFORE `deleteAccessEntityOnDisk`, then forward
    `check_func` into `memory_storage.insert(...)`.
  - `removeNoLock`: call `check_func(entity)` after reading and before any
    mutation.
  - The stale 25.8 `setAllInMemory` / `removeAllExceptInMemory` functions (absent
    in 26.3, which uses `memory_storage.setAll`) were dropped entirely.
- **`src/Access/ZooKeeperReplicator.{h,cpp}`** — MERGE: keep 26.3's
  `setCurrentComponent` guard + `retryOnZooKeeperUserError(1000, ...)` AND add the
  patch's `CheckFunc`-carrying `insertEntity` / `insertZooKeeper` overloads. The
  existing-entity permission check (`check_existing_func(existing_entity)`) is
  invoked before the ZK `Remove`/`Set` ops in BOTH collision branches (UUID
  collision and name collision). The base's verbose `LOG_INFO`/emoji tracing was
  stripped, keeping only the functional check + the pre-existing `LOG_DEBUG`.
  `ReplicatedAccessStorage::removeImpl` (applied cleanly) reads the entity and
  calls `check_func` before `replicator.removeEntity`. **This is the ZK-replicated
  TOCTOU-safe write path.**

### Interpreters (rewritten by the fix — reconstructed in final post-fix form)

- **`src/Interpreters/Access/InterpreterCreateUserQuery.cpp`** — reconstructed
  from the pristine 26.3 `execute()` plus the fix's final logic: hoist storage
  resolution and the self-modify/self-replace + `isProtected` (via
  `tryRead<User>`) checks ABOVE the cluster dispatch (a single
  `check_protected_change(existing_is_protected)` helper requires
  `PROTECTED_ACCESS_MANAGEMENT` when `existing_is_protected || query.protected_flag`);
  set `protected_flag` on new/updated users; the `CREATE OR REPLACE` path passes a
  simplified `CheckFunc` to `insertOrReplace`. The static `updateUserFromQuery`
  helper sets `protected_flag` so the flag survives (de)serialization. The base's
  password-hash-deferral refactor was NOT carried (see deviation note below).
- **`src/Interpreters/Access/InterpreterDropAccessEntityQuery.cpp`** —
  reconstructed: resolve target IDs, `tryRead` each entity and apply the
  self-drop + `isProtected` `check_func` BEFORE `executeDDLQueryOnCluster`;
  `check_func` is still passed to `remove()` on the local path as defense in
  depth. The 26.3 `MASKING_POLICY` branch is preserved.
- **`src/Interpreters/Access/InterpreterGrantQuery.cpp`** — kept 26.3's TABLE
  ENGINE validation + grantee resolution, then inserted the fix's pre-dispatch
  block: compare each grantee to the current user by BOTH UUID and name (so the
  self-revoke check fires even when the resolved grantee UUID differs from
  `getContext()->getUserID`), and aggregate `isProtected()` to require
  `PROTECTED_ACCESS_MANAGEMENT` before the cluster dispatch. The base's
  defense-in-depth `isProtected` check inside `update_func` is kept. The base's
  noisy includes (`escapeString.h`, `IDatabase.h`, `base/sleep.h`,
  `logger_useful.h`) were dropped; `ACCESS_DENIED` added to `ErrorCodes`.

### Other

- **`src/Interpreters/InterpreterDropQuery.cpp`** — the prior Aiven
  `cluster_database`-gated block already existed on 26.3 checking
  `ACCESS_MANAGEMENT`; patch 022 upgrades that to `PROTECTED_ACCESS_MANAGEMENT`.
  Resolved by keeping the 26.3 Allman-formatted block and only swapping the
  privilege.
- **`src/Parsers/Access/ASTCreateUserQuery.h`** — clean apply (`protected_flag`
  field added at the listed line).
- **`src/Interpreters/Context.cpp`** — the base's only change here was
  debug-only `LOG_INFO`/try-catch tracing in `checkSettingsConstraints`; it is
  not part of the feature and was reverted to the pristine 26.3 file.

### Deviation: password-hash deferral not carried

The base refactored `InterpreterCreateUserQuery` to defer
`AuthenticationData::fromAST` hashing until after permission checks (to avoid the
side effect of hashing for a rejected query). The fix already hoists the
protected/self checks above all storage writes, and that deferral added
significant complexity without affecting any of the stated security invariants.
The final form computes auth methods as upstream 26.3 does and relies on the
hoisted checks for enforcement; a rejected protected-user statement at worst
wastes a hash computation (no security or correctness impact). Documented rather
than silently dropped.

### Follow-up fix — `checkProtectedTargets` bad-cast on role targets (`patch-port(022)` #2)

The 26.3 rewrite consolidated the base patch's inline "TO `<user>`" enforcement
(for `CREATE/ALTER ROW POLICY`, `QUOTA`, `SETTINGS PROFILE`, and
`SET DEFAULT ROLE`) into a new shared helper
`src/Interpreters/Access/checkProtectedTargets.cpp`. That reconstruction assumed
the resolved target set is user-only and inspected every id with
`access_control.tryRead<User>(id)`.

`tryRead<User>` is **not** type-safe: it forwards to
`read<User>(id, /*throw_if_not_exists=*/false)`, which only suppresses the
*not-found* case. A wrong-**type** id (a role) still reaches `throwBadCast`, so
the `... TO` set is resolved by `getMatchingIDs` and any role in it raised
`Code: 49 ... role <x> expected to be of type USER. (LOGICAL_ERROR)`. The
`TO` clause for `ROW POLICY` / `QUOTA` / `SETTINGS PROFILE` is parsed with
`allowRoles().allowUsers()`, so a role there is a legitimate target, not an
error. (`SET DEFAULT ROLE`'s `TO` clause sets `allow_roles = false`, so a role
can never be a target there — that call site only ever sees users and never
triggered the bad cast.)

Fix: inspect each id type-erased and rely on the virtual `isProtected`:

```cpp
auto entity = access_control.tryRead<IAccessEntity>(id);
if (entity && entity->isProtected())
    require_protected_priv = true;
```

`read<IAccessEntity>` returns the entity without any `typeid_cast` (the
`if constexpr (std::is_same_v<EntityClassT, IAccessEntity>)` branch in
`IAccessStorage::read`), so it never calls `throwBadCast`; a missing id still
yields `nullptr`. Because patch 079 gave `Role` a real `isProtected`, the single
virtual call now covers protected users **and** protected roles — closing both
the crash and the 079 coverage gap where a `PROTECTED` role in a `... TO` set
would have escaped `PROTECTED_ACCESS_MANAGEMENT`.

**Invariant protected:** protected-entity enforcement must inspect targets by
their real entity type; a heterogeneous `RolesOrUsersSet` must never be cast to a
single concrete type. A type mismatch is a normal validation outcome, not a
`LOGICAL_ERROR`.

This slipped past the original tests because `09079` created a role but only ever
used it as the *subject* of `SET DEFAULT ROLE <role> TO <user>` — never as a
member of a `... TO` set, so the helper only ever saw user-only sets. See the
26.3 fork-handover regression set, `tmp/26-3-ci-logs/clickhouse_26_3_fork_handover.md`
§B. The fix lands as a second `patch-port(022)` commit (squashed into the single
022 patch at finalization).

## 4. Security invariant

1. **Checks run before ON CLUSTER dispatch.** In all three interpreters
   (`Create`/`Drop`/`Grant`) the self-protection + `isProtected` →
   `PROTECTED_ACCESS_MANAGEMENT` checks execute on the initiator BEFORE
   `executeDDLQueryOnCluster`. An `ON CLUSTER` statement therefore cannot bypass
   them via `DDLWorker`. (Verified by integration test, §5c.)
2. **CheckFunc on every storage write path.** `insertImpl`/`removeImpl` thread a
   `CheckFunc` through `MultipleAccessStorage` → `Disk`/`Memory`/`Replicated`
   storage. For replace-by-name the EXISTING entity is checked before deletion in
   the disk path (`DiskAccessStorage::insertNoLock`) and the ZK path
   (`ZooKeeperReplicator::insertZooKeeper`), and on remove
   (`DiskAccessStorage::removeNoLock`, `ReplicatedAccessStorage::removeImpl`).
   This is the TOCTOU guarantee for the replicated case.
3. **Self-drop / self-revoke block.** A user can never drop themselves
   (`InterpreterDropAccessEntityQuery` `check_func`), modify/replace themselves
   (`InterpreterCreateUserQuery` pre-dispatch loop), or revoke from themselves
   (`InterpreterGrantQuery` pre-dispatch, UUID-or-name), even with
   `PROTECTED_ACCESS_MANAGEMENT`.

## 5. Test design

### (a) `09079_protected_user_extra_statements` (stateless, ported)

Ported + renumbered from the base's shipped `04004_protected_user_extra_statements`
(the `04004` prefix already collides with several 26.3 tests; `add-test` assigns
the next free number). Covers the `checkProtectedTargets` "TO `<user>`" matrix for
`CREATE ROW POLICY` / `QUOTA` / `SETTINGS PROFILE` / `SET DEFAULT ROLE`
(noperm→protected denied, admin-with-`PROTECTED`→protected ok, admin→self denied).

Extended by the follow-up fix (§3) with **role** targets: for `ROW POLICY` /
`QUOTA` / `SETTINGS PROFILE` (whose `TO` clause accepts roles), a plain role in
the set is allowed for the unprivileged `noperm` user (locking the bad-cast
regression — pre-fix this raised `LOGICAL_ERROR`), and a `PROTECTED` role
(`CREATE ROLE <r> PROTECTED`) requires `PROTECTED_ACCESS_MANAGEMENT`
(noperm→denied, admin→ok). `SET DEFAULT ROLE` is intentionally not given a
role-target case because its `TO` clause does not accept roles.

### (b) `09080_protected_user_management` (stateless, new)

The core protected-user matrix, untested by 09079. Three actors: a user with
`ACCESS MANAGEMENT` but NOT `PROTECTED_ACCESS_MANAGEMENT` (`noperm`), a user with
`PROTECTED_ACCESS_MANAGEMENT` (`admin`), and self. Asserts: `DROP USER` protected
(denied w/o priv; self-drop denied WITH priv), `CREATE USER ... PROTECTED`,
`ALTER ... PROTECTED` / `... NOT PROTECTED`, `CREATE USER OR REPLACE`, `RENAME`,
password change (`IDENTIFIED WITH ...`), `SETTINGS`, `SETTINGS PROFILE`,
`REVOKE ALL`, and self-revoke — all denied without the privilege; positives where
`admin` performs them on protected users (except self-drop/self-revoke).

### (c) `tests/integration/test_aiven_protected_users/` (integration, new)

The ONLY vehicle that exercises the FIX and the ZK replication path. 2 ClickHouse
nodes + Keeper, `user_directories` configured with a `<replicated>` ZooKeeper
path (`ReplicatedAccessStorage`). A self-contained bootstrap user `clickadmin`
holding `GRANT ALL` is defined in `configs/users.xml` and used to create the
protected service user (note: `access_management` alone does NOT grant
`PROTECTED_ACCESS_MANAGEMENT`, which lives under `ALL`; and combining `<grants>`
with `access_management` on the framework's `default` user is rejected by the
server, so a separate user is used rather than redefining `default`). Asserts:
- `test_on_cluster_bypass_denied_on_initiator`: `DROP USER <prot> ON CLUSTER`,
  `ALTER USER <prot> ON CLUSTER NOT PROTECTED`, `CREATE USER OR REPLACE <prot> ON
  CLUSTER`, `REVOKE ON CLUSTER ALL FROM <prot>` each raise `ACCESS_DENIED` for the
  restricted cluster user, and the protected user is left intact.
- `test_protected_flag_replicates_via_zookeeper`: a protected user created on
  node1 is visible and shown as `PROTECTED` on node2, and enforcement (DROP /
  `NOT PROTECTED`) is denied on node2 — proving the flag replicates via ZooKeeper.

## 6. Rollback considerations

- Revert safety: the feature is additive (a new privilege + a per-user bool +
  parser keyword + interpreter checks). Reverting the commit removes enforcement;
  any persisted `Protected` flag in disk/ZK entity definitions is simply ignored
  by a binary without the feature (it deserializes as a non-protected user).
- Persistent state: the `Protected` flag is serialized into the access entity
  definition (disk file / ZK znode). On downgrade the extra token would be unknown
  to the old parser — operators should clear protection (`ALTER USER ... NOT
  PROTECTED`) before downgrading if strict round-trip is required.
- Disable without rebuilding: do not grant `PROTECTED_ACCESS_MANAGEMENT` and do
  not mark any user `PROTECTED`; the checks are then inert for all users.

## 7. Per-uplift notes

### 25.8-aiven (historical)

Base `ce74bc008d` (Tilman Moeller, co-authored Kevin Michel) + follow-up
`f4552b14c77` (Joe Lynch) closing the ON CLUSTER bypass. Base shipped one
stateless test (`04004_protected_user_extra_statements`).

### 26.3-aiven (this uplift)

- Cherry-pick was: **adapted** (apply-then-fix; 9 conflicts manually resolved
  against the drifted Access subsystem; 3 interpreters reconstructed in their
  final post-fix form). Base + fix landed as ONE commit.
- Upstream-drift conclusion: `still-needed-but-rewrite` (§2).
- Tests added: `09079_protected_user_extra_statements` (ported/renumbered),
  `09080_protected_user_management` (new), `test_aiven_protected_users` (new
  integration).
- Anything surprising: `DiskAccessStorage` now delegating to an internal
  `MemoryAccessStorage` meant the 25.8 storage hunks could not be force-applied —
  the `CheckFunc` had to be re-threaded through the delegation boundary.

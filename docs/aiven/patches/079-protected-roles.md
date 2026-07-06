# Patch 079 — protected-roles

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven (`v25.8.24.21-lts-aiven`) | `827d8ebb573da08964232e87781ac5939769f956` (author Joe Lynch `joe.lynch@aiven.io`, 2026-06-09; merge `dce60488c99`, PR #29 2026-06-17) | Joe Lynch | original carry (post-fork-point of our 26.3 base) |
| 26.3-aiven | `patch-port(079)` (staged) | T3 worker, 2026-06-19 | `still-needed-applies-cleanly` for 5/8 files; `conflict-resolved` for 3 files (`byte_equivalent: false`) |

The current uplift's row stays "(staged)" until the human commits.

## 1. Purpose

Extend the existing protected-entity mechanism — the per-entity `Protected`
flag plus the `PROTECTED_ACCESS_MANAGEMENT` global privilege and the SQL keyword
`PROTECTED`, all landed by `patch-port(022)` (protected-users) — to ROLE
entities. A role marked `PROTECTED` can only be created, altered, renamed,
replaced, dropped, moved between storages, or have its grants changed by a
principal holding `PROTECTED_ACCESS_MANAGEMENT`. This lets service-managed roles
exist as real SQL roles that ordinary cluster users (who legitimately hold
`CREATE`/`ALTER`/`DROP ROLE` and `ROLE ADMIN`) cannot remove or tamper with.

This is the **post-fork-point sibling of patch 022**: 079 landed on the Aiven
source line (`v25.8.24.21-lts-aiven`) AFTER our 26.3 fork point `f4552b14c77`,
and it reuses 022's privilege + storage `CheckFunc` plumbing wholesale. It is the
root cause of the harness `aiven_admin_role` failure on 26.3: stock 26.3 has no
protected-role grammar, so `CREATE ROLE aiven_admin_role ... PROTECTED` fails at
the parser with `SYNTAX_ERROR` on 26.3 while succeeding on 25.x.

Source SHA on `v25.8.24.21-lts-aiven`: `827d8ebb573da08964232e87781ac5939769f956`.
Original author: Joe Lynch `joe.lynch@aiven.io` (per `git show --format=%ae`).
Original purpose (quoted from the commit body):

> Extends the existing protected-entity mechanism (the per-entity `Protected`
> flag plus the `PROTECTED_ACCESS_MANAGEMENT` global privilege, SQL keyword
> `PROTECTED`) to ROLE entities. [...] This lets service-managed roles exist as
> real SQL roles that ordinary cluster users cannot remove or tamper with.

## 2. Upstream-drift findings

> Verify the patch is still SEMANTICALLY needed against `v26.3.10.62-lts`.

### Commands run

```bash
# identifier inventory (all consumed identifiers present on HEAD)
for id in PROTECTED_ACCESS_MANAGEMENT 'Keyword::PROTECTED' 'tryRead<Role>' \
          'IAccessStorage::CheckFunc' getAccess isProtected; do
  git grep -c "$id" -- src/ ; done
git grep -c 'protected_flag' -- src/Access/Role.h src/Parsers/Access/ASTCreateRoleQuery.h  # 0 (net-new)
git grep -n 'insertOrReplace' -- src/Access/IAccessStorage.h     # 2-arg CheckFunc overload present
git grep -c PROTECTED -- src/Parsers/Access/ParserCreateRoleQuery.cpp  # 0 (gap real, not obsoleted)
```

### Findings

- `PROTECTED_ACCESS_MANAGEMENT` (18 hits), `Keyword::PROTECTED` (3), `tryRead<Role>`
  (2), `IAccessStorage::CheckFunc` (7), `Context::getAccess` (202), `isProtected`
  (11) all present on HEAD — the privilege/keyword/storage plumbing from 022 is
  the premise and is in place. The load-bearing 2-arg
  `insertOrReplace(entities, check_func)` overload is present
  (`src/Access/IAccessStorage.h:185-186`).
- `protected_flag` is absent from `Role.h` / `ASTCreateRoleQuery.h` on HEAD (net-new
  members this patch adds — expected 0 hits). `Role` does NOT yet override the
  `IAccessEntity::isProtected` base (`return false`); this patch fills that gap.
- No upstream equivalent: `git grep -c PROTECTED -- src/Parsers/Access/ParserCreateRoleQuery.cpp`
  = 0 on HEAD. `PROTECTED` is an Aiven concept; stock 26.3 has no protected-role
  grammar. The gap is real (root cause of the `aiven_admin_role` failure).
- **Drift the parent preflight missed (found at cherry-pick time):** HEAD's `Role`
  carries an upstream/Aiven member `std::optional<UInt64> fetched_from_remote_at_ms;`
  that does NOT exist in the patch's source pre-image. This collided additively
  with the new `protected_flag` in `Role.h` (same anchor) and `Role.cpp::equal`
  (same return expression).
- **Drift from the 022 port (found at cherry-pick time):** on 26.3, `patch-port(022)`
  already HOISTED the protected-USER `MOVE` check ABOVE the `ON CLUSTER` dispatch in
  `InterpreterMoveAccessEntityQuery.cpp` (with a self-move guard); on the source
  branch the USER `MOVE` check is still post-dispatch. The source 079 appended its
  ROLE `MOVE` check post-dispatch, next to the source's post-dispatch USER block.
- Conclusion: **`still-needed-and-applies`** (semantics required). 5/8 files apply
  cleanly; 3 require conflict resolution (§3). `byte_equivalent: false`.

## 3. Conflict resolution, per file

8 files touched; **5 applied cleanly** via `git cherry-pick --no-commit -x`
(`InterpreterCreateRoleQuery.cpp`, `InterpreterShowCreateAccessEntityQuery.cpp`,
`ASTCreateRoleQuery.cpp`, `ASTCreateRoleQuery.h`, `ParserCreateRoleQuery.cpp` —
each matched the source diff exactly). **3 conflicted** and were resolved per the
human-ratified `tmp/patch-079/proposed-resolution.md`:

- **`src/Access/Role.h`** — additive merge: keep BOTH HEAD's
  `std::optional<UInt64> fetched_from_remote_at_ms;` AND the patch's
  `bool protected_flag = false;`. The `isProtected()` override auto-merged cleanly.
- **`src/Access/Role.cpp`** (`Role::equal`) — additive merge: AND both comparisons
  (`fetched_from_remote_at_ms == ...` from HEAD AND `protected_flag == ...` from the
  patch) into the equality expression.
- **`src/Interpreters/Access/InterpreterMoveAccessEntityQuery.cpp`** —
  **security-positive divergence from the source patch (human-ratified):** the new
  protected-ROLE `MOVE` check is HOISTED ABOVE the
  `if (!query.cluster.empty()) return executeDDLQueryOnCluster(...)` dispatch,
  immediately after HEAD's hoisted USER block (mirroring the 022 USER hoist). The
  cherry-pick's post-dispatch duplicate USER block and post-dispatch ROLE block
  were DISCARDED (conflict region resolved to HEAD). See §6 for the rationale and
  why this is a strict improvement over the source form.

### Patch-id

Source patch-id `01a13cf4…` vs staged `9a4b682f…` → `byte_equivalent: false`.
Decomposition (`tmp/patch-079/decomposition.log`, `step3-conclusion.txt`) confirms
the ONLY add/remove-line deltas are (a) the additive `fetched_from_remote_at_ms`
merge in `Role.cpp::equal` and (b) the MOVE-check comment text + brace style; the
ROLE-check logic lines are byte-identical to source. No other semantic delta.

## 4. C++ review

Per `docs/aiven/skills/cpp-review-checklist.md`:

- **1 Lifetime + ownership:** ✓ — checks use `tryRead<Role>` (returns `RolePtr`,
  a `shared_ptr<const Role>`) and `getContext()->getAccess()` (a `ContextAccess`
  shared_ptr); no new raw owners. Matches surrounding code.
- **2 Exception safety:** ✓ — `checkAccess` throws `ACCESS_DENIED` before any
  storage mutation; the pre-dispatch checks fail closed with no partial state. The
  `insertOrReplace` `CheckFunc` re-check is atomic inside the storage op.
- **3 Thread-safety + concurrency:** ✓ — read-only access through `AccessControl`
  / `ContextAccess` thread-safe accessors; no new locks; no sleeps.
- **4 Performance + memory:** n/a — DDL/parse/control path (`CREATE/ALTER/DROP/MOVE
  ROLE`), not a per-row hot path.
- **5 Settings as public API:** n/a — carried UNCONDITIONAL, no new server setting
  (parent policy call; clause (v) does not trip — additive keyword + privilege gate).
- **6 Error handling:** ✓ — uses the existing `PROTECTED_ACCESS_MANAGEMENT`
  privilege; the "Not enough privileges" message names `PROTECTED ACCESS MANAGEMENT`
  specifically, distinguishing the Aiven gate from a generic denial (the limited
  user DOES hold `DROP/ALTER/CREATE ROLE`, so only the protected gate fires). Tested
  by stateless assertion (H) and integration MOVE-denial.
- **7 Upstream / vendored code:** ✓ — touches only `src/Access` + `src/Interpreters/Access`
  + `src/Parsers/Access`; no `contrib/**`, `.claude/**`, `.github/workflows/**`, root
  `AGENTS.md`.
- **8 Behavior under settings:** ✓ — a role that is not `PROTECTED` behaves exactly
  as on stock 26.3 (no observable change, no extra allocations); the gate fires only
  when the statement sets `PROTECTED` or the target role is already protected.

## 5. Test design

### (a) Required — stateless `9079_protected_roles` (new, evidence-of-causation pair)

- Test path: `tests/queries/0_stateless/9079_protected_roles.{sh,reference}` (Aiven
  `9<NNN>_` convention; `9079_` 4-digit does not collide with 022's `09079`/`09080`).
- Covers the local + `ON CLUSTER` denial matrix via `InterpreterCreateRoleQuery`:
  `DROP` / `ALTER ... RENAME` / `CREATE OR REPLACE` / `ALTER ... SETTINGS` /
  `ALTER ... NOT PROTECTED` / `CREATE ... PROTECTED`, each also `ON CLUSTER
  test_shard_localhost`. Asserts deterministic end-state (role survival, flag
  retention, no rename, no new role) plus one gate-specificity message check (H)
  and a control (B: the limited user CAN drop a non-protected role).
- Pre-patch run (the FAIL) — `tmp/patch-079/test-prepatch.log`:

  ```text
  9079_protected_roles:                                                   [ FAIL ] 1.28 sec.
  Reason: having stderror:
  Code: 62. DB::Exception: Syntax error: failed at position 37 (PROTECTED): PROTECTED;.
    ... (SYNTAX_ERROR)
  ... There is no role `prot_role_...` in `user directories`. (UNKNOWN_ROLE)
  stdout: 0 0 0 0 0 0 0
  ```

- Post-patch run (the PASS) — `tmp/patch-079/test-postpatch.log`:

  ```text
  9079_protected_roles:                                                   [ OK ] 1.24 sec.
  1 tests passed. 0 tests skipped.
  ```

- Why it distinguishes the Aiven gate: pre-patch, `PROTECTED` is unknown to the
  `CREATE ROLE` parser on stock 26.3 (`SYNTAX_ERROR`) — the exact root-cause gap;
  post-patch the flag round-trips and the protected role survives all ten mutation
  attempts by a role-management user, with the `ON CLUSTER` denial naming
  `PROTECTED ACCESS MANAGEMENT` on the initiator.

### (b) Required (scope addition) — integration `test_aiven_protected_roles/` (new)

- Test path: `tests/integration/test_aiven_protected_roles/` (`test.py`,
  `__init__.py`, `configs/{config,users,zookeeper}.xml`). 2 nodes + Keeper +
  `ReplicatedAccessStorage`, mirroring `test_aiven_protected_users` (022). Adds a
  second SQL storage (`memory`) alongside `replicated` so the `MOVE` check is
  reachable (the single-storage stateless harness cannot reach it).
- Asserts (all PASS post-patch — `tmp/patch-079/integration-run-2.log`):
  - `test_protected_flag_replicates_via_zookeeper`: `CREATE ROLE ... PROTECTED IN
    replicated` on node1 → on node2 `SHOW CREATE ROLE` emits `PROTECTED`, and the
    limited cluster user (holds `CREATE/ALTER/DROP ROLE` + `ROLE ADMIN`, NOT
    `PROTECTED_ACCESS_MANAGEMENT`) is denied `DROP ROLE` and `ALTER ... NOT
    PROTECTED` on node2.
  - `test_move_protected_role_denied_on_initiator[move_local]` and `[move_on_cluster]`:
    the limited user is denied `MOVE ROLE <prot> TO memory` AND `MOVE ROLE <prot> TO
    memory ON CLUSTER default`, the error names `PROTECTED ACCESS MANAGEMENT`, and the
    role stays in `replicated` (the hoist makes the `ON CLUSTER` denial fire on the
    initiator before dispatch).
  - `test_move_nonprotected_role_allowed` (control): the SAME user CAN `MOVE` a
    non-protected role `replicated → memory`, proving the move capability is genuinely
    held and only the protected gate blocks the protected role.

  Result: `4 passed in 14.83s` (praktika exit 0).

## 6. Rollback considerations

- Revert safety: additive (a new per-role bool + parser keyword + interpreter
  checks). Reverting removes enforcement; any persisted `Protected` flag on a role
  is ignored by a binary without the feature (deserializes as non-protected).
- Persistent state: the `Protected` flag is serialized into the role's access-entity
  definition (disk file / ZK znode) and round-trips via `SHOW CREATE ROLE`. On
  downgrade the `PROTECTED` token is unknown to the old parser — clear protection
  (`ALTER ROLE ... NOT PROTECTED`) before downgrading if strict round-trip is needed.
- Disable without rebuilding: do not grant `PROTECTED_ACCESS_MANAGEMENT` and do not
  mark any role `PROTECTED`; the checks are then inert for all roles.

### MOVE-check hoist (human-ratified divergence from source)

The source 079 places its protected-ROLE `MOVE` check AFTER the `ON CLUSTER`
dispatch, because on the `v25.8.24.21-lts-aiven` source branch the
`InterpreterMoveAccessEntityQuery` USER check is also post-dispatch (un-hoisted).
On our 26.3 HEAD, `patch-port(022)` already HOISTED the USER `MOVE` check ABOVE
`if (!query.cluster.empty()) return executeDDLQueryOnCluster(...)` so an `ON
CLUSTER` move cannot be laundered through `DDLWorker` under a different identity.
Taking the source's post-dispatch ROLE placement verbatim on 26.3 would have
re-opened that exact `ON CLUSTER` bypass for `MOVE ROLE`. The worker escalated
this as a security-relevant semantic conflict; the human ratified hoisting the
ROLE check into the pre-dispatch block alongside the USER check. This is a strict
improvement over the source form and is directly covered by the integration
`move_on_cluster` assertion. (Note for the source-line maintainers: the upstream
Aiven 079 likely ships a latent `MOVE ROLE ... ON CLUSTER` bypass on the
un-hoisted source branch — worth reporting back.)

## 7. Per-uplift notes

### 25.8-aiven (`v25.8.24.21-lts-aiven`, post-fork-point)

Single commit `827d8ebb573` (Joe Lynch, co-authored by Claude Opus 4.8 — the
co-author trailer is dropped on carry per source-author-preservation policy).
Shipped no test.

### 26.3-aiven (this uplift)

- Cherry-pick was: **conflict-resolved** (5/8 clean; 3 resolved — 2 additive
  `fetched_from_remote_at_ms` merges + 1 human-ratified MOVE hoist). `byte_equivalent:
  false`.
- Upstream-drift conclusion: `still-needed-and-applies` (§2).
- Carried UNCONDITIONAL (no new server setting), per parent policy call 1 (clause
  (v) does not trip — additive keyword + privilege check; 022/065 precedent).
- Tests added: `9079_protected_roles` (stateless, evidence-of-causation pair) +
  `test_aiven_protected_roles` (integration, 4 tests — replication + the hoisted
  MOVE check, the latter unreachable from the single-storage stateless harness).
- Time-to-port: ~ (warm-cache build dir; one full + two incremental builds +
  stateless pair + one integration run + escalation round-trip).
- Anything surprising: the parent preflight forecast a clean, byte-equivalent apply,
  but the post-fork-point source had drifted vs HEAD in two ways (the
  `fetched_from_remote_at_ms` member and the 022 MOVE-hoist), forcing a 3-file
  conflict resolution and a human-ratified security-positive divergence on the MOVE
  path. First dispatch where a previously-ported sibling (022) changed the *correct
  resolution* of a later sibling (079).

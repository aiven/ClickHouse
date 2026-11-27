---
description: 'Allow an EXCEPT clause in GRANT that carves privileges out of the same statement'
sidebarTitle: 'N11: GRANT ... EXCEPT'
slug: '/aiven/patches/N11-grant-except'
title: 'Patch N11: GRANT ... EXCEPT'
doc_type: 'reference'
aiven_identity: 'N11'
aiven_absorbs: []
aiven_changelog_category: 'New Feature'
aiven_settings: []
---

# Patch N11 — `GRANT ... EXCEPT` {#patch-n11-grant-except}

## Summary {#summary}

`GRANT SELECT ON db.* EXCEPT SELECT ON db.secrets TO john` grants the privileges and revokes
the excepted part in one statement. The result is the same as a `GRANT` followed by a
`REVOKE`: `SHOW GRANTS` prints both lines, and the stored entity holds ordinary grants and
partial revokes. One statement means one replicated access change, no window in which the
grantee holds the wider access, and one statement for the control plane to issue.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 26.3 | `ea931bd375af` (cherry-pick of `042b106a6cd`), 2026-07-13, on `v26.3.60.2-lts-aiven` | **origin.** Parser, AST and interpreter support, integration test |
| 26.8.34.13 | `patch-port(N11):` | `still-needed-but-rewrite`: re-applied on top of `019` and `022`, stateless test, user documentation |

The 26.3 line uses the identity `N07` for another patch; this patch is `N11` on both lines.

## Background (ClickHouse) {#background}

- **Partial revokes.** Revoking a table from a database-wide grant stores a partial revoke,
  printed by `SHOW GRANTS` as a `REVOKE` line.
- **`InterpreterGrantQuery`.** Collects the elements to grant and to revoke, checks that the
  current user holds them with grant option, and applies them to every grantee, locally or
  through `ON CLUSTER`.

## Component tour {#component-tour}

- **`ParserGrantQuery`** (upstream). After the privileges of a `GRANT`, an optional
  `EXCEPT` followed by privileges without options. A syntax error after a role list or after
  `DEFAULT REPLICATED DATABASE PRIVILEGES` (patch `019`).
- **`ASTGrantQuery`** (upstream). New member `access_rights_elements_except`, formatted as
  ` EXCEPT ...` before ` TO `, so the statement round-trips through `ON CLUSTER` and the DDL
  log.
- **`InterpreterGrantQuery`** (upstream). A third list, applied after the grant in
  `updateGrantedAccessRightsAndRoles` and `grantCurrentGrants`. The new
  `checkExceptIsInsideGrantedRights` throws `BAD_ARGUMENTS` when an excepted element is not
  part of what the statement grants. `execute` throws `LOGICAL_ERROR` if an excepted element
  carries options, so the equivalence with a `REVOKE` is checked, not only assumed, and
  `BAD_ARGUMENTS` if it carries a filter. `TABLE ENGINE` names are validated in both lists.
- **`UsersConfigParser`** (upstream). A `GRANT ... EXCEPT` in the `<grants>` of a `users.xml`
  user or role applies the excepted part after the grant, as SQL does.

## Problem {#problem}

Carving a part out of a wide grant took two statements. Between them the grantee holds the
wider access, and if the second statement fails the wider access stays.

## Decisions {#decisions}

### The excepted part lies inside the grant {#decision-inside}

- **Chosen:** every excepted element must be covered by the privileges the same statement
  grants, else `BAD_ARGUMENTS`, checked before anything is applied or sent to other hosts.
- **Why:** `EXCEPT` narrows this grant. Revoking unrelated privileges that the grantee
  already held belongs in a `REVOKE`.
- **Consequence:** the permission checks of the grant, and the access required to run it
  `ON CLUSTER`, already cover the excepted part, so they are unchanged.

### `CURRENT GRANTS` checks the excepted part as `REVOKE` does {#decision-current-grants}

- **Chosen:** with `GRANT CURRENT GRANTS(...) EXCEPT ...`, the excepted part goes through the
  check of `REVOKE`: the current user needs the grant option on the part the grantees hold.
- **Why:** `CURRENT GRANTS` grants only the part of the listed privileges the current user may
  grant, and needs no grant option for the rest. The excepted part is revoked from the grantee
  in full, so it needs what a `REVOKE` of it needs. Rights the same statement adds are
  grantable by the current user, so they need no further check.
- **Rejected:** the grant option on every excepted element. An administrator never holds the
  grant option on everything, so `GRANT CURRENT GRANTS ON *.* EXCEPT ALL ON system.*` would
  fail although the equivalent `REVOKE` succeeds.

### No filters in `EXCEPT` {#decision-filters}

- **Chosen:** an excepted element with a source filter, such as `READ ON S3('s3://b/x/.*')`,
  is rejected with `BAD_ARGUMENTS`.
- **Why:** a revoke matches the source of a filtered element but not its filter, so the
  shorthand would revoke the whole source. Narrow such a grant with a filter in the grant itself.

### Exactly a `GRANT` followed by a `REVOKE` {#decision-equivalence}

- **Chosen:** the statement equals `GRANT ...` followed by `REVOKE <except> FROM ...`, with
  or without `WITH GRANT OPTION` and `WITH REPLACE OPTION`.
- **Why:** users and the control plane can reason about it, and read it back, as the two
  statements they already know.

### Not with `DEFAULT REPLICATED DATABASE PRIVILEGES` {#decision-019}

- **Chosen:** syntax error.
- **Why:** that clause expands to a set fixed by the managed configuration; carving into it
  has no use case and widens the surface of patch `019`.

### `EXCEPT` counts as a revoke {#decision-022}

- **Chosen:** for the protection of patch `022` that stops a user from revoking their own
  rights, a `GRANT` with `EXCEPT` counts as a revoke.

## Approach {#approach}

The interpreter replaces empty databases in the excepted list as in the granted list, runs
the scope check, then hands both lists to the same update function. `WITH REPLACE OPTION`
revokes everything, grants, then revokes the excepted part. `GRANT CURRENT GRANTS(...)`
applies the excepted part after computing the union of current grants.

## Concept {#concept}

A shorthand must be exactly its expansion. Define it by the statements it replaces, test it
against them, and reject inputs whose expansion would do something the shorthand's name does
not promise.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-but-rewrite`

- Upstream 26.8 has no `EXCEPT` in `GRANT`.
- Patch `019` added `DEFAULT REPLICATED DATABASE PRIVILEGES` to the same parser branch, and
  patch `022` added initiator checks to `InterpreterGrantQuery::execute`. The port sits
  after both.
- The 26.3 integration test is replaced by a stateless test.

## Downstream contract {#downstream-contract}

- **The control plane** may issue `GRANT ... EXCEPT ... TO ...`, including `ON CLUSTER` and
  `WITH GRANT OPTION`, and reads the result through `SHOW GRANTS`.

## Customer impact {#customer-impact}

- **Who:** every user allowed to grant privileges.
- **What they see**, from the stateless test:

```sql
GRANT SELECT ON db.* EXCEPT SELECT ON db.t2 TO u1;
SHOW GRANTS FOR u1;
-- GRANT SELECT ON db.* TO u1
-- REVOKE SELECT ON db.t2 FROM u1

GRANT SELECT ON db.t1 EXCEPT SELECT ON db.t3 TO u1;
-- Code: 36. DB::Exception: EXCEPT SELECT ON db.t3 is not part of the access granted
-- by the same statement. (BAD_ARGUMENTS)
```

- **Unchanged:** `GRANT` and `REVOKE` without `EXCEPT`, including `GRANT r1 EXCEPT r2`
  for role lists.
- **How to restore upstream behaviour:** issue a `GRANT` and a `REVOKE`.

## Known residual {#known-residual}

- A role list keeps its own `EXCEPT` (`GRANT r1, r2 EXCEPT r2 TO u`), so privileges after a
  role list fail with a generic syntax error instead of the dedicated message.

## Security review {#security-review}

The statement can only reduce what the same statement grants: the scope check rejects any
excepted element outside the grant, before anything is applied locally or sent to other
hosts. The current user needs grant option on everything granted, which covers the
excepted part; with `CURRENT GRANTS`, which needs no grant option for what it does not grant,
the excepted part is checked as a `REVOKE` of it would be. The resulting entity is ordinary grants and partial revokes, so no new stored
format or code path evaluates access. Patch `022` treats the statement as a revoke, and the
protected-target checks on grantees apply unchanged.

## Tests {#tests}

- **Path:** `tests/queries/0_stateless/aiven_N11_grant_except.sh`.
- **Proves:** formatting round-trips, including `ON CLUSTER`, `WITH GRANT OPTION` and
  `CURRENT GRANTS`; `EXCEPT` after roles or `DEFAULT REPLICATED DATABASE PRIVILEGES` is a
  syntax error while upstream role-list `EXCEPT` still parses. Table and database exclusion
  for users and roles; the excluded table is unreadable and not re-grantable under
  `WITH GRANT OPTION`; the result equals `GRANT` plus `REVOKE`; an excepted element outside
  the grant, by object or by privilege, is rejected and changes nothing; `WITH REPLACE OPTION`
  and `CURRENT GRANTS` work; unqualified names resolve to the current database, locally and
  through a real `ON CLUSTER` run; `EXCEPT` on the current user is rejected by patch `022`.
- **Path:** `tests/queries/0_stateless/aiven_N11_grant_except_checks.sh`.
- **Proves:** `CURRENT GRANTS ... EXCEPT` is refused without any privilege and with the grant
  option on only part of what it revokes, and leaves the grantee unchanged; it succeeds when the
  grantee holds nothing of the excepted part, as `REVOKE` does. Column-level and wildcard
  exceptions store the expected partial revokes, and a table-wide exception from a column grant
  is rejected. An excepted element with a filter and an unknown `TABLE ENGINE` are rejected; a
  `users.xml` grant with `EXCEPT` stores the partial revoke.
- **Run:** `python -m ci.praktika run "Stateless tests (arm_binary, parallel)" --test aiven_N11_grant_except`.
- **Evidence (2026-11-23):** passes with the patch, three runs in a row; fails on the parent
  commit, where `EXCEPT` does not parse. Without the `EXCEPT` list in
  `ASTGrantQuery::replaceEmptyDatabase`, the `ON CLUSTER` case fails: the executing host
  resolves the excepted table in its own default database, and the scope check rejects it.
- **Evidence (2026-11-27):** `aiven_N11_grant_except_checks` passes with the checks and fails
  without them in every refused case: the refused statements change the grantee, the filter and
  the unknown engine are accepted, and `users.xml` drops the `EXCEPT` part. The column, wildcard
  and `EXCEPT ALL ON system.*` cases pass on both, as they pin unchanged behaviour. Rejecting a filter in
  `users.xml` must not format the element: that locks the global context, which is held while
  the users config loads.

## Rollback {#rollback}

- **Downgrade:** not supported; services only move forward.
- **Revert in a later release:** no on-disk or Keeper format of its own; stored entities
  hold ordinary grants. `ON CLUSTER` statements with `EXCEPT` left in the DDL log would fail
  to parse on a binary without the patch.
- **Disable without rebuild:** not possible; the syntax is always available.

## Retirement {#retirement}

Upstream support for `EXCEPT` in `GRANT` with the same semantics. Re-check upstream on every
uplift.

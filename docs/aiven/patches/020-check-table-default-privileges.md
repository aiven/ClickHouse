# Patch 020 — check-table-default-privileges

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `c5b03b2c0e` | Tilman Moeller (author) / Aliaksei Khatskevich (committer), 2025-12-13 | (the version we are porting FROM) |
| 26.3-aiven | `patch-port(020)` (`(staged)`) | parent agent, 2026-06-03 | `still-needed` — ported as a direct one-line edit; see §2 |

One-line follow-up to patch 019 (the `019→020` chain). It cannot be cherry-picked
until 019 lands because the privilege block it edits does not exist on 26.3 HEAD
otherwise. `byte_equivalent: false` (see §2 for why a literal cherry-pick was not used).

## 1. Purpose

Adds `CHECK` to the curated privilege set granted by
`GRANT DEFAULT REPLICATED DATABASE PRIVILEGES` (introduced by 019). Per the source
commit body: *"`CHECK TABLE` requires an explicit grant since `e96e0ae`"* — so
without this, a user holding the default set cannot run `CHECK TABLE` on tables in
their replicated database. The change is a single inserted line in the privilege
string built by `InterpreterGrantQuery::execute`:

```cpp
            "SELECT, "
            "SHOW, "
            "CHECK, "                 // <-- patch 020
            "SYSTEM SYNC REPLICA, "
            "TRUNCATE "
```

## 2. Upstream-drift / validity findings

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`.

- **`CHECK` is still a distinct, separately-grantable privilege on 26.3.**
  `src/Access/Common/AccessType.h:162` — `M(CHECK, "", TABLE, ALL) /* allows to
  execute CHECK TABLE; */`. It is not implied by `SELECT`/`SHOW`, so a user with
  the curated set genuinely cannot `CHECK TABLE` without it. The patch's premise
  holds → **still needed**.
- **Why not a literal cherry-pick.** The source diff's surrounding context line is
  `… TO " + escapeString(grantee) + …`, but the 026.3 port of 019 hardened that
  to `backQuote(grantee)` (019 finding A). The added `"CHECK, "` line sits in an
  unaffected location (after `"SHOW, "`), so the change was applied as a direct
  one-line edit rather than fighting the cherry-pick context. The resulting line
  is identical to the source's intent.

## 3. C++ / security review

Trivial and safe: it widens a fixed, curated allow-list by exactly one table-level
privilege (`CHECK TABLE`). No new elevation, no parser/AST change, no privilege
outside the existing `db.*` scope. `CHECK TABLE` is a read-only integrity check; it
cannot mutate data or schema. The grant is still emitted `WITH GRANT OPTION`,
consistent with the rest of the set.

## 4. Test design

No new test file — the behavior is asserted by extending 019's case C
(`test_aiven_indirect_database_creation::test_c_grant_default_privileges`), per the
"keep C in sync" note in the 019 dossier:

- `CHECK` added to the expected privilege set and to the representative
  must-be-present subset asserted against `SHOW GRANTS`.
- Validated together with the full 019 module (case C is the gate for this change).

## 5. Rollback considerations

- Revert safety: dropping the one line returns the curated set to its 019 form;
  no data/migration impact.
- Chain: this is the tail of `019→020`; it must sit on top of `patch-port(019)`.
- User-facing footprint: users granted `DEFAULT REPLICATED DATABASE PRIVILEGES`
  (incl. `avnadmin` on its auto-created databases) now also receive `CHECK TABLE`.

## 6. Per-uplift notes

### 25.8-aiven (historical)

Source `c5b03b2c0e` (author Tilman Moeller, committer Aliaksei Khatskevich,
2025-12-13; co-authored by Aliaksei Khatskevich). Single-line addition of
`"CHECK, "` to the default privilege list.

### 26.3-aiven (this uplift)

- Applied as a direct one-line edit (not a cherry-pick; see §2).
- Test: case C extended to assert `CHECK`; full 019 module re-validated.
- Depends on `patch-port(019)` (`2a8529c5935`).

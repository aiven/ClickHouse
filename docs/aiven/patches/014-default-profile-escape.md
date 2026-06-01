# Patch 014 — default-profile-escape

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `e65f68836b` | tilman.moeller@aiven.io (author) / kevin.michel@aiven.io (co-author) | (the version we are porting FROM) |
| 26.3-aiven | `patch-port(014)` | parent agent, 2026-06-01 | ported **clean** (`git apply` with zero conflicts; all anchors present on 26.3) — faithful port + style cleanup + stateless test. See §2–§4. |

## 1. Purpose

Close a privilege-escalation hole in settings-profile management. A user who is
constrained by a restrictive `default` profile but who *also* holds
`CREATE USER` / `CREATE SETTINGS PROFILE` can today escape its constraints:
they create a new user (or profile) that points directly at a *less* restrictive
profile (e.g. an `admin` profile), then connect as that user. The
profile-switch verification that normally blocks moving to a less-restrictive
profile is **skipped** when a profile is assigned directly at create time, so
the constraint is bypassed.

The fix enforces that every profile assigned via SQL must be the configured
`default` profile or one of its descendants — **unless** the acting user has the
new `allow_non_default_profile` setting enabled. The pre-created `admin` profile
(defined in `users.xml`, not via SQL) can carry `allow_non_default_profile =
true`, while the `default` profile carries it `false` (locked), so trusted
admins keep full freedom and constrained users cannot escape.

### Mechanism (where it is enforced)

There are three cooperating pieces:

1. **Setting `allow_non_default_profile`** (`Bool`, default **`true`** → guard
   off) in `src/Core/Settings.cpp`. The protection is opt-in: a deployment turns
   it on by setting it `false` (with a `CONST`/locked constraint) in the
   `default` profile.
2. **Enforcement gate in `SettingsConstraints::check`**
   (`src/Access/SettingsConstraints.cpp`): for every profile element that
   carries a `parent_profile`, if the acting user's
   `current_settings[allow_non_default_profile]` is `false` and the parent is
   **not** `isDefaultProfileOrDescendant`, throw
   `SETTING_CONSTRAINT_VIOLATION` "All profiles must be the default profile or
   inherit from it." Because `check` is the **shared** sink called by
   `InterpreterCreateSettingsProfileQuery` (`SettingSource::PROFILE`),
   `InterpreterCreateUserQuery` (`SettingSource::USER`), and
   `InterpreterCreateRoleQuery` (`SettingSource::ROLE`), and the new branch is
   **not** keyed on `source`, the same rule covers `CREATE/ALTER PROFILE`,
   `CREATE/ALTER USER ... SETTINGS PROFILE x`, and `CREATE/ALTER ROLE ...
   SETTINGS PROFILE x` in one place — this is what closes the *user*-creation
   vector described in the purpose, not just profile creation.
3. **Interpreter logic in `InterpreterCreateSettingsProfileQuery::execute`**:
   - **Circular-dependency rejection** (unconditional, not gated by the
     setting): on `ALTER`, for each named profile, if a proposed parent already
     has the altered profile as an ancestor, throw "Inherited profiles may not
     have circular dependencies".
   - **Auto-injection of the default parent**: when the guard is on
     (`allow_non_default_profile = false`) and the create/alter specifies no
     parent, the `default` profile id is inserted as the first parent element,
     so an operator does not have to spell out `INHERIT default` every time.
     If no default profile is configured, throws; altering `default` together
     with other profiles in one statement is rejected.

The trust check itself lives in `SettingsProfilesCache`
(`getDefaultProfileId` / `isDefaultProfileOrDescendant` /
`isExpectedProfileOrDescendant`, all taking the cache `mutex` and calling
`ensureAllProfilesRead`), surfaced through thin `AccessControl` forwarders.

## 2. Upstream-drift / validity findings

`still-needed-and-applies-clean`. The feature is wanted on 26.3, does not exist
upstream, and the 25.8 commit `e65f68836b` applied with **zero conflicts**
(`git apply --check` clean). All anchors are present and unchanged on 26.3:

- `SettingsConstraints::check(const Settings &, const SettingsProfileElements &,
  SettingSource)` — same signature.
- The three create interpreters still call `checkSettingsConstraints(...,
  SettingSource::{PROFILE,USER,ROLE})`.
- `SettingsProfilesCache` still exposes `default_profile_id`,
  `ensureAllProfilesRead`, and `access_control.tryRead<SettingsProfile>` under
  `mutex`.
- The `DECLARE(...)` settings macro list and the `readonly` anchor used by the
  new `DECLARE(Bool, allow_non_default_profile, ...)` are intact.

No layout-affecting headers change (see §5), so the `#deps 0` hazard does not
apply to this patch.

### Style cleanup applied during the port (not behavior)

Two faithful-but-non-conforming bits from the source were normalized for the CI
style check:

- a stray blank line between `if (settings_from_query)` and its `{` was removed;
- the alternative token `and` was replaced with `&&`
  (`has_default_profile && query.names.size() > 1`).

## 3. C++ review — observations and dispositions

### A. Protection is **off by default** — kept (by design)

`allow_non_default_profile` defaults to `true`, so on a vanilla deployment the
gate in `SettingsConstraints::check` and the auto-injection in the interpreter
are both no-ops. This is intentional: the protection is opt-in and Aiven enables
it by locking the setting `false` in the `default` profile. Consequence to be
aware of: **upgrading to this build changes nothing** until the operator sets
`allow_non_default_profile = false`; the dossier and changelog must say so or
operators will assume they are protected.

### B. Circular-dependency check is **unconditional** — kept, noted

The cycle rejection in `InterpreterCreateSettingsProfileQuery` runs regardless of
`allow_non_default_profile`. So even default deployments gain a new behavior:
`ALTER SETTINGS PROFILE` that would form an inheritance cycle now throws
`SETTING_CONSTRAINT_VIOLATION` instead of being accepted. This is a strict
improvement (cyclic inheritance has no legitimate use), but it is a *global*
behavior change, not gated by the new setting — flagged so it is not mistaken
for an opt-in-only change. (Note: the cycle loop only executes for `ALTER`,
since `name_ids` is populated only when `query.alter`; a brand-new `CREATE`
cannot be in a cycle yet.)

### C. Unbounded recursion in `isExpectedProfileOrDescendantLocked` — **threat, recommend hardening**

`isExpectedProfileOrDescendantLocked` walks the parent chain by **plain
recursion with no visited-set / depth bound**. The patch prevents *new* cycles
from being created via SQL, but a cyclic profile graph can still exist:

- profiles defined in `users.xml` (that path does not go through the interpreter
  cycle check),
- access entities replicated from a node running a build **without** this patch
  (mixed-version cluster / rolling upgrade),
- entities written directly to the access storage.

The existing, sibling traversal `SettingsProfilesCache::substituteProfiles` is
deliberately **cycle-safe** — it carries a `substituted_profiles_set` visited
set — which is direct evidence that the codebase already treats this graph as
possibly cyclic. The new helper does not, so on a pre-existing cycle it recurses
unbounded → **stack overflow while holding `SettingsProfilesCache::mutex`**
(a fatal crash, and the worst place to crash because the cache lock is held).
This fires from `isDefaultProfileOrDescendant` (the per-DDL gate, guard on) or
from the interpreter's cycle check, whenever the traversed subgraph contains a
cycle that does not include the search target.

Disposition: shipped **faithful** in this commit (matches 25.8; the realistic
trigger window is narrow because SQL-created cycles are now blocked). Recommended
follow-up hardening: give `isExpectedProfileOrDescendantLocked` a visited-set or
a depth cap mirroring `substituteProfiles`, so a malformed graph degrades to
"not a descendant" instead of crashing. Tracked here rather than fixed inline to
keep this commit a clean, reviewable port; promote to a fix commit if we want the
robustness now.

### D. Repeated guard-on `ALTER` re-injects `default` as a parent — minor, noted

`has_parent_profile` is computed only from the *alter's* `add_settings`, not from
the profile's existing inheritance. So under the guard, an `ALTER SETTINGS
PROFILE x SETTINGS some=1` (without an explicit `INHERIT`) auto-injects `INHERIT
default` again on every such alter, which can accumulate duplicate parent
elements in the serialized form. `substituteProfiles` de-duplicates at apply
time, so it is cosmetic (shows up in `SHOW CREATE`), not a correctness/security
issue. Specifying `INHERIT default` explicitly avoids it.

### E. Lock ordering of the new reads — safe (consistent with existing code)

`isExpectedProfileOrDescendantLocked` calls `access_control.tryRead<SettingsProfile>`
while holding `SettingsProfilesCache::mutex`. This is the **same** order already
used by the pre-existing `ensureAllProfilesRead` (which does
`access_control.findAll` / `tryRead` under the same `mutex`), so the patch
introduces no new lock-inversion.

## 4. Test design

`tests/queries/0_stateless/09078_allow_non_default_profile_escape.sql` (stateless,
deterministic, parallel-safe — uniquely-suffixed profile names, no `TO ALL`, no
user login).

The guard is toggled in-session with `SET allow_non_default_profile = {0,1}`.
This drives the **exact same** enforcement branches as locking it via a `CONST`
constraint in a profile — `SettingsConstraints::check` reads
`current_settings[allow_non_default_profile]` and the interpreter reads
`getSettingsRef()[allow_non_default_profile]`, and neither cares *how* the value
got there. So a one-file `.sql` covers the logic without standing up multiple
users.

| Case | Setup | Asserts |
|---|---|---|
| escape blocked | guard **on**, `CREATE PROFILE ... INHERIT <non-default>` | `SETTING_CONSTRAINT_VIOLATION` → the core fix |
| default inheritance allowed | guard **on**, `CREATE PROFILE ... INHERIT default` | succeeds; `SHOW CREATE` shows `INHERIT default` → the gate is not over-broad |
| auto-injection | guard **on**, `CREATE PROFILE ...` with no parent | succeeds; `SHOW CREATE` shows `INHERIT default` was prepended |
| cycle detection | guard **off**, `ALTER` forming `a→b→a` | `SETTING_CONSTRAINT_VIOLATION` → unconditional cycle check (§3.B) |
| opt-out | guard **off**, `CREATE PROFILE ... INHERIT <non-default>` | succeeds → confirms the protection is exactly what `allow_non_default_profile` gates |

The escape-blocked + opt-out pair is the evidence-of-causation: the *same*
statement is rejected with the guard on and accepted with it off, so the
assertion is non-vacuous and pinned to `allow_non_default_profile` specifically.

Note on a pre/post-patch differential run: it is degenerate here because the
setting `allow_non_default_profile` does **not exist** on the pre-patch binary
(`SET allow_non_default_profile = 0` errors with "Unknown setting"), so the whole
test exercises new-only surface. The post-patch `[ OK ]` plus the in-test
guard-on-vs-off contrast is the meaningful proof.

Run locally:

```bash
export PATH="$PWD/build/programs:$PATH"
CLICKHOUSE_PORT_TCP=9000 CLICKHOUSE_PORT_HTTP=8123 \
  ./tests/clickhouse-test --no-random-settings --no-random-merge-tree-settings \
  09078_allow_non_default_profile_escape
```

## 5. Build note (this uplift)

Pure additive change: the two touched headers (`AccessControl.h`,
`SettingsProfilesCache.h`) add only **method declarations** — no data members,
no signature changes to existing functions — so no struct layout shifts and the
`#deps 0` false-green hazard (see `docs/aiven/runbooks/build-and-test.md` §7)
does **not** apply. The build was nonetheless verified to recompile all six
touched TUs and relink `programs/clickhouse` (warm incremental, ~61s).

## 6. Rollback considerations

Reverting the commit removes the setting, the gate, the cycle check, and the
auto-injection together. Any deployment that had set `allow_non_default_profile =
false` would silently lose the protection (and the setting would become unknown,
breaking configs that reference it) — so a revert must be paired with removing
that setting from `users.xml`/profiles. The port + style cleanup + test ship as
one commit, so the code revert itself is atomic.

## 7. Per-uplift notes

### 25.8-aiven (historical, source)

Source commit `e65f68836b` (Tilman Moeller, co-author Kevin Michel). Same logic;
carried the stray blank line and the `and` token cleaned up here.

### 26.3-aiven (this uplift)

Applied clean. Shipped faithful, with the unbounded-recursion robustness gap
(§3.C) documented as a recommended follow-up rather than fixed inline, plus a
stateless `.sql` test.

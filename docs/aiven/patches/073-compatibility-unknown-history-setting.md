---
description: 'Dossier for the Aiven patch that keeps the compatibility setting working when the settings history names a setting the build does not have'
sidebarTitle: '073: Absent settings-history entries'
slug: '/aiven/patches/073-compatibility-unknown-history-setting'
title: 'Patch 073: Tolerate settings-history entries for absent settings'
doc_type: 'reference'
aiven_identity: '073'
aiven_absorbs: []
aiven_changelog_category: 'Bug Fix'
aiven_settings: []
---

# Patch 073 — Tolerate settings-history entries for absent settings {#patch-073-compatibility-unknown-history-setting}

## Summary {#summary}

When the settings history names a setting that this build does not have, the
`compatibility` setting now skips that entry and logs a warning naming it,
instead of failing every `SET compatibility` and every first access to a
`MergeTree` table. Aiven carries it as insurance: selective backports can ship
exactly that inconsistency, which is how the 25.3 line broke `compatibility` for
every user. On a correctly built release nothing changes; on a broken one,
operators get one warning per missing setting instead of an outage.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.3 | predates the `patch-port(<NNN>)` subject convention | ported (session door only) |
| 26.3 | `patch-port(073): Fix compatibility setting crash on removed setting` at `af5d57389935efda7bf2f373b97c2c289762ac68` | ported (session door only) |
| 26.8 | `patch-port(073): Tolerate settings-history entries for absent settings` | `still-needed-but-rewrite`, one commit; ported first on 26.8.2.7 and re-implemented on 26.8.34.13 |

The 26.3 commit's own trailers cite `aec2378` and `406526f` from the 25.3 era.
The subject was deliberately not preserved verbatim on 26.8: the original says
"crash" for what is an exception, and this port is a rewrite rather than a
straight cherry-pick.

## Background {#background}

`compatibility` is a session setting that asks the server to behave like an
older release. It is implemented by replay: `src/Core/SettingsChangesHistory.cpp`
holds an ordered map from version to a list of `{name, previous_value,
new_value}` records, and setting `compatibility` walks that map in descending
version order, writing `previous_value` back for every record newer than the
requested version. The history is therefore a *second, implicit schema* for the
settings list — a name recorded there is assumed to exist as a setting.

There are two independent histories and two independent replay functions: one
for session settings and one for `MergeTree` table settings. Both histories are
exposed read-only through `system.settings_changes`, keyed by a `type` column
(`Session` / `MergeTree`).

## Component tour {#component-tour}

`src/Core/Settings.cpp` — the session-settings layer. `SettingsImpl::apply
CompatibilitySetting` is private and reached only from `SettingsImpl::set` when
the assigned name is `compatibility`. Foreground, runs per `SET` and per
settings-profile application. The patch adds a `has` check on the resolved name
ahead of the loop's first lookup, and a file-local helper,
`warnHistoryEntryForAbsentSetting`, that reports each skipped name.

`src/Core/BaseSettings.h` / `.cpp` — the generic settings container underneath
both layers, including name lookup. Two helpers matter here:
`BaseSettingsHelpers::throwSettingNotFound` (fatal, `UNKNOWN_SETTING`) and
`BaseSettingsHelpers::warningSettingNotFound` (skip and log a throttled warning,
accumulating names in a `thread_local` so one bad peer does not flood the log).
The latter already backs the settings *deserialization* path, where settings
arriving from a peer running another version are skipped rather than rejected.

`src/Storages/MergeTree/MergeTreeSettings.cpp` — the storage layer.
`MergeTreeSettings::applyCompatibilitySetting` is public but called from exactly
two places, `Context::getMergeTreeSettings` and
`Context::getReplicatedMergeTreeSettings`. Both lazily build a process-wide
cached `MergeTreeSettings` under `shared->mutex` from the *default profile's*
`compatibility` value. Server-internal, not per query — and not drivable from a
session-level `SET`. The patch replaces the loop's `LOGICAL_ERROR` throw with a
skip and its own copy of the reporting helper.

`tests/queries/0_stateless/` — upstream ships `03999_stateless_settings_history.sh`,
which fails when either history names a setting that does not exist. The Aiven
Buildkite lane runs `aiven_*.{sql,sh}` only (`.buildkite/run_aiven_stateless.sh`),
so that test does not run for us.

## Problem {#problem}

Two halves, one data and one code.

**Data.** The fork can ship a build whose history names a setting the build does
not have. It happens when a port cherry-picks a commit that touches
`SettingsChangesHistory.cpp` without the commit that declares the setting. That
is how the 25.3 build acquired entries from 25.5/25.6 naming
`parallel_replicas_connect_timeout_ms`.

**Code.** Both replay loops treat such an entry as fatal. On the session side the
loop's first use of the resolved name is `getTier`, which bottoms out in
`throwSettingNotFound`, so *every* `SET compatibility = <older version>` throws
`UNKNOWN_SETTING` for every user — the 25.3 symptom. On the `MergeTree` side the
loop throws `LOGICAL_ERROR "Unknown setting in history"` explicitly, and because
`Context::getMergeTreeSettings` caches into an optional that stays unset when the
call throws, the exception recurs on every first touch of a `MergeTree` table, in
every session, regardless of who set `compatibility`. The 26.3 port closed only
the session door; the wider one was left open.

## Decisions {#decisions}

### Both replay doors are guarded, not only the session one {#decision-both-doors}

- **Chosen:** skip a dangling entry in both `SettingsImpl::applyCompatibilitySetting`
  and `MergeTreeSettings::applyCompatibilitySetting`.
- **Rejected:** carrying the 26.3 shape, which guards the session loop only. It
  leaves the wider door open: the `MergeTree` loop runs for the default
  profile's `compatibility` and its failure is not cached, so it repeats for
  every session.
- **Rung:** 4, runtime guard. A managed-config rung cannot fix a binary whose
  own data disagrees with its code, and the upstream test that would catch the
  data (`03999_stateless_settings_history`) is not in our lane.

### Report a skipped entry, do not skip silently {#decision-report-not-silent}

- **Chosen:** log a warning naming the setting under the `Settings` or
  `MergeTreeSettings` logger.
- **Rejected:** a bare `continue`, as on 26.3. An inconsistent history is a
  packaging bug; the guard buys availability and must not also buy silence.
- **Rejected:** `BaseSettingsHelpers::warningSettingNotFound`. It carries an
  accumulate-then-flush contract: it appends to a `thread_local` vector that
  only `flushWarnings` clears, and across `src/**` it is called solely from
  `BaseSettings<T>::read`, which flushes at the end of its loop. Called from a
  replay site with no matching flush, it would grow that vector on pooled query
  threads forever, and its latched `logged` flag would go silent after the first
  warning.
- **Rung:** 4.

### One warning per missing name, not one per process {#decision-warn-per-name}

- **Chosen:** each file keeps a process-wide set of reported names behind a
  mutex, so every distinct missing setting is named exactly once.
- **Rejected:** a single function-local `std::once_flag`, as in the first 26.8
  port. It logs only the first missing name ever seen: a history with two
  dangling entries tells the operator about one, they fix it, redeploy, and only
  then learn about the second.
- **Rung:** 4.
- **Supersedes:** the `std::once_flag` reporting of the first 26.8 port
  (26.8.2.7). The set cannot grow without bound, because it can only ever hold
  names written in the compiled-in history.

### No setting to switch the guard off {#decision-no-switch}

- **Chosen:** the guard is unconditional.
- **Rejected:** an `aiven_*` setting to restore the fatal behaviour. On a
  correctly built release neither branch is reachable, so a switch would only
  add surface.
- **Rung:** 4.

## Approach {#approach}

Skip a dangling entry in both replay loops rather than failing, and report it.
On the session side, the `has(final_name)` check sits ahead of the first lookup,
so an alias resolving to a missing setting is covered too. On the `MergeTree`
side it replaces the `LOGICAL_ERROR` throw, and the now-unused `LOGICAL_ERROR`
error-code declaration goes with it. The reported name is the one written in
the history (`change.name`), because that is the string an operator greps for in
`SettingsChangesHistory.cpp`.

Then guard the data rather than only the code: `aiven_073_*` asserts that neither
history contains a name absent from the corresponding settings list, and replays
both histories from the oldest version, so a bad port fails our lane instead of
reaching a customer.

Non-goals: no new setting, no attempt to reconstruct the skipped entry's value,
and no guard for an entry whose previous value no longer fits its setting's type
(a renamed enum value, a string recorded for a number). That entry still throws:
catching it would mean wrapping every write-back in a `try`/`catch`, a fallback
that hides an error with no single name to report. The full-replay tests catch
it instead. A skipped entry means `compatibility` quietly fails to restore that one
setting, which is a genuine if narrow semantic loss — the test is what keeps the
path dead.

Two divergences a reviewer might misread as bugs: the guard is deliberately not
silent, and an upstream `LOGICAL_ERROR` becomes a warning. Both branches are dead
code on a consistent build.

Accepted cost: `MergeTreeSettings.cpp` is high-churn, so replacing an upstream
throw there adds recurring conflict surface for the `CH Inc sync` job in exchange
for a branch that never executes on a correct build. Worth it, because the whole
point of porting `073` is insurance for the case where the tripwire is bypassed
(a hand-built hotfix, or a port that never ran our lane), and in that case the
`MergeTree` door is the worse of the two failures. The reporting helper is
duplicated in the two files rather than shared through
`SettingsChangesHistory.h`, which upstream edits on every release.

## Concept {#concept}

`SettingsChangesHistory` is a second, implicit schema for the settings list, and
`compatibility` is an interpreter that replays it backwards. A build where the two
disagree carries a latent runtime failure that reading either file alone will
never reveal, and only selective backporting creates the disagreement. That is
why the durable fix belongs in CI and the runtime guard is merely a blast-radius
limiter — the trap is believing a code guard fixed a data problem.

Smaller and more reusable: a "warn once" guard is only as useful as what it
names. Throttle per distinct key, not per call site, when the operator's next
step depends on the full list.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-but-rewrite`

- Retarget from 26.8.2.7 to 26.8.34.13: neither replay loop changed upstream
  between the two patch releases, so the anchors are identical. This line was
  re-implemented by hand from the 26.8.2.7 port rather than cherry-picked, and
  changed one decision (one warning per missing name).
- On the session side, `getTier(final_name)` is still the loop's first lookup
  and the first thing to throw.
- Trigger absent on a clean 26.8.34.13 tree: both tripwire arms return empty.
  The port is forward insurance.
- New evidence of the trigger class: upstream's own 26.8 backports needed two
  follow-up fixes to `SettingsChangesHistory.cpp` between 26.8.2.7 and
  26.8.34.13 — leaked `26.9`/`26.10` blocks, and a setting recorded under the
  `26.10` label. Those were wrong labels rather than missing names, but they show
  the file is edited by hand on every backport.
- The 26.3 test (`9073_…`: `SET compatibility = '25.2'; SELECT 1`) passes on the
  *unpatched* 26.8 tree and asserts nothing Aiven-specific. It is replaced, not
  renamed.
- No sibling or follow-up patch exists on any Aiven line; `073` stands alone.
  The one to watch is `046-add-early-fetch-pool`, the only fork patch that
  modifies `src/Core/SettingsChangesHistory.cpp` — exactly the class of change
  that manufactures this trigger. Order `073` before it so the tripwire is armed
  first.

## Customer impact {#customer-impact}

`none — no user-visible change on a consistent build.` Both guarded branches are
unreachable while the shipped history matches the shipped settings, which is the
state 26.8.34.13 is in.

Should a future port ship a dangling entry:

- **Who:** every user who sets `compatibility`, and every session that first
  touches a `MergeTree` table when the default profile sets `compatibility`.
- **What they see:** the statement and the table access succeed. The server logs,
  once per process for each missing name, under the `Settings` or
  `MergeTreeSettings` logger:

  ```text
  Setting '<name>' is recorded in SettingsChangesHistory but does not exist in this build; the 'compatibility' setting cannot restore it
  ```

  The affected setting is not restored to its old value, so a tenant relying on
  `compatibility` for that one setting silently gets current semantics — the
  cost of staying available.
- **Versus 26.3:** the first `MergeTree` access keeps working instead of raising
  a repeated `LOGICAL_ERROR` exception in every session (26.3: still broken), and
  the skip is logged (26.3: silent).
- **Versus 26.8.2.7:** every missing name is logged, not only the first.
- **Restore the previous behaviour:** not possible without a rebuild, by design.

## Security review {#security-review}

`n/a — the patch changes how the server replays its own compiled-in settings history; no user input, access check or secret reaches the changed code.`

## Tests {#tests}

- **Paths:**
  - `tests/queries/0_stateless/aiven_073_compatibility_unknown_history_setting.sql`
  - `tests/queries/0_stateless/aiven_073_compatibility_mergetree_full_replay.sh`
- **Proves:**
  - (a) Neither history contains a name absent from `system.settings` /
    `system.merge_tree_settings`, the exact condition that broke 25.3.
  - (b) The session history replays from `compatibility = '1.0'`, which writes
    back every entry, so an entry whose old value no longer fits its type fails
    here. A managed service accepts any version up to the current one, so the
    oldest is a value customers can choose; the earlier `25.8` / `26.3` arms
    never reached the older blocks. The reset to `''` also completes.
  - (c) The `MergeTree` history replays from `1.0` through the `MergeTree` door
    itself. That door runs once per process, from the first
    `Context::getMergeTreeSettings` call, so a running server cannot be driven
    from a session-level `SET`. The `.sh` test uses a fresh `clickhouse-local`
    whose first `MergeTree` access happens under `--compatibility 1.0`, and
    asserts that the guard's warning appears zero times.
- **Run:** `python3 -m ci.praktika run "Stateless tests (amd_debug, parallel)" --test aiven_073_compatibility_unknown_history_setting aiven_073_compatibility_mergetree_full_replay`
- **Evidence:** `tests.added: no_trigger_on_current_lts` for the shipped tests:
  both histories are consistent on 26.8.34.13, so they pass on the unpatched tree
  too. They are tripwires on the data, not regression tests. Causation was proven
  separately by manufacturing the trigger in the working tree only: two invented
  names in the `26.8` block of the session history and one in the `MergeTree`
  history.
  - Unpatched: `SET compatibility = '25.8'` raised `UNKNOWN_SETTING` naming the
    first invented setting, and both tripwire arms named exactly the invented
    settings.
  - Session side guarded only: the `SET` succeeded and both invented session
    names were logged; the first `MergeTree` access with a default
    `compatibility = 25.8` (`clickhouse local --compatibility 25.8`) aborted the
    debug build, which is how a debug build reports `LOGICAL_ERROR`.
  - Both sides guarded: the same statements succeeded, `SELECT count()` on the
    new `MergeTree` table returned `1`, and the server logged all three invented
    names, one warning each.
  - Type mismatch, both sides guarded: a non-numeric old value for
    `max_block_size` in the session `24.1` block and for `index_granularity` in
    the `MergeTree` `24.12` block. `SET compatibility = '25.8'` still succeeded,
    so the old arms were blind to it; `SET compatibility = '1.0'` failed with
    `CANNOT_PARSE_INPUT_ASSERTION_FAILED`, and so did the first `MergeTree`
    access under `clickhouse-local --compatibility 24.2`, a version that skips
    the bad session entry. Both shipped tests failed. Under `--compatibility 25.8`
    the `MergeTree` door logged the invented `MergeTree` name, which is the
    warning the `.sh` test counts.

  The scratch edits were reverted before staging;
  `src/Core/SettingsChangesHistory.cpp` is untouched.
- **Over-blocking:** upstream `03999_stateless_settings_history` and
  `04652_system_documentation_settings_history` pass with the patch.

## Rollback {#rollback}

- **Revert:** safe on a running fleet. Reverting restores fatal behaviour on both
  doors, which only matters on an inconsistent build.
- **Persistent state:** none — no on-disk or Keeper state, no access entities,
  no format change.
- **Disable without rebuild:** none, by design; the guarded path should never be
  reached on a correctly built release.

## Retirement {#retirement}

Retire when upstream tolerates an absent history entry in both replay loops, or
when the Aiven stateless lane runs upstream's `03999_stateless_settings_history`
and every port is required to pass it — then the data can no longer ship
inconsistent and the guard is dead weight. Re-check both on the next uplift.

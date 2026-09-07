---
description: 'Dossier for the Aiven patch that keeps the compatibility setting working when the settings history names a setting the build does not have'
sidebarTitle: '073: Absent settings-history entries'
slug: '/aiven/patches/073-compatibility-unknown-history-setting'
title: 'Patch 073: Tolerate settings-history entries for absent settings'
doc_type: 'reference'
---

# Patch 073 — Tolerate settings-history entries for absent settings {#patch-073-compatibility-unknown-history-setting}

## Lineage {#lineage}

| LTS | How to find | Outcome |
|---|---|---|
| 25.3 | predates the `patch-port(<NNN>)` subject convention | ported (session door only) |
| 26.3 | `patch-port(073): Fix compatibility setting crash on removed setting` at `af5d57389935efda7bf2f373b97c2c289762ac68` | ported (session door only) |
| 26.8 | `patch-port(073): Tolerate settings-history entries for absent settings` | `still-needed-but-rewrite` |

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
settings-profile application.

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
session-level `SET`.

`tests/queries/0_stateless/` — upstream ships `03999_stateless_settings_history.sh`,
which fails when either history names a setting that does not exist. The Aiven
Buildkite lane globs `aiven_*.{sql,sh}` only (`.buildkite/run_aiven_stateless.sh`),
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

## Approach {#approach}

Skip a dangling entry in both replay loops rather than failing, but **report it**
instead of a bare `continue`. An inconsistent history is a packaging bug; the
guard buys availability and must not also buy silence. On the `MergeTree` side
this replaces the `LOGICAL_ERROR` throw.

The report is a function-local `static std::once_flag` plus `LOG_WARNING`,
matching the precedent in `src/Core/SettingsQuirks.cpp`, and deliberately **not**
`BaseSettingsHelpers::warningSettingNotFound`, which looks like the obvious fit
but is not. That helper carries an accumulate-then-flush contract: it
unconditionally appends the name to a `thread_local` vector that only
`flushWarnings` clears, and across `src/**` it is called solely from
`BaseSettings<T>::read`, which flushes at the end of its loop. Calling it from a
replay site with no matching flush would grow that vector on pooled query threads
forever and, because its `logged` flag latches, would go silent after the first
warning. `std::call_once` has no shared mutable state and cannot flood.

Then guard the data rather than only the code: `aiven_073_*` asserts that neither
history contains a name absent from the corresponding settings list, so a bad
port fails our lane instead of reaching a customer.

Non-goals: no new setting, and no attempt to reconstruct the skipped entry's
value. A skipped entry means `compatibility` quietly fails to restore that one
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
`MergeTree` door is the worse of the two failures.

## Concept {#concept}

`SettingsChangesHistory` is a second, implicit schema for the settings list, and
`compatibility` is an interpreter that replays it backwards. A build where the two
disagree carries a latent runtime failure that reading either file alone will
never reveal, and only selective backporting creates the disagreement. That is
why the durable fix belongs in CI and the runtime guard is merely a blast-radius
limiter — the trap is believing a code guard fixed a data problem.

Smaller and more reusable: ClickHouse already has a house policy for "a setting
name I do not recognize" — skip it and log a throttled warning, which is what the
wire-deserialization path does for settings from a peer on another version. Adopt
that policy before inventing a fallback; a bare `continue` would have been the
un-idiomatic choice here, not the conservative one. Adopting the *policy* is not
the same as reusing the *helper*, though: `warningSettingNotFound` only throttles
correctly inside a caller that flushes, so borrowing it outside
`BaseSettings<T>::read` would have inherited a contract this site cannot honour.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-but-rewrite`

- Anchor intact, throwing call moved: 26.3 reached the exception at
  `get(final_name)`; on 26.8 `getTier(final_name)` is hit first
  (`src/Core/Settings.cpp:9529`). Same error code, one line earlier.
- Trigger absent on a clean 26.8 peel (see Tests). The port is forward insurance
  plus the two additions above.
- New since 26.3: upstream's `03999_stateless_settings_history.sh` now fails on a
  dangling entry in either history — a detection mechanism 25.3 lacked. It is not
  in our lane, which is why we ship our own tripwire.
- The 26.3 test (`9073_…`: `SET compatibility = '25.2'; SELECT 1`) passes on the
  *unpatched* 26.8 tree and asserts nothing Aiven-specific. It is replaced, not
  renamed.
- No sibling or follow-up patch exists on any Aiven line; `073` stands alone.
  Its real dependency is on the porting process, not on another patch. The one
  to watch is `046-add-early-fetch-pool`, the only fork patch that modifies
  `src/Core/SettingsChangesHistory.cpp` — exactly the class of change that
  manufactures this trigger. Order `073` before it so the tripwire is armed
  first.

## Customer impact {#customer-impact}

`none — no user-visible change on a consistent build.` Both guarded branches are
unreachable while the shipped history matches the shipped settings, which is the
state 26.8 is in.

Should a future port ship a dangling entry, the delta versus 26.3 is: the first
`MergeTree` access keeps working instead of raising a repeated logical-error
exception in every session (26.3: still broken), and the server logs, once per
process under the `Settings` or `MergeTreeSettings` logger, `Setting '<name>' is
recorded in SettingsChangesHistory but does not exist in this build; the
'compatibility' setting cannot restore it` (26.3: silent). Operators get a line
to grep instead of an outage. The affected setting is still
not restored to its old value, so a tenant relying on `compatibility` for that one
setting silently gets current semantics — which is the cost of staying available.

## Tests {#tests}

- Path: `tests/queries/0_stateless/aiven_073_compatibility_unknown_history_setting.sql`
- What it proves (causation): (a) neither history contains a name absent from
  `system.settings` / `system.merge_tree_settings` — the exact condition that
  broke 25.3, and the only reachable coverage for the `MergeTree` door, since
  that door reads the default profile's `compatibility` and cannot be driven from
  a session-level `SET`; (b) the replay completes for the LTS versions we ship
  from, `25.8` and `26.3`, and for the reset to `''`.
- Evidence: `tests.added: no_trigger_on_current_lts` for the two skip branches —
  `SET compatibility = '25.2'` already returns `1` on the unpatched tree and the
  dangling-entry query returns zero rows, so no FAIL/PASS pair exists for them.
  Captures under `tmp/uplift-26.8/patch-073/`. The test earns its place as a
  tripwire on the data, which is where the failure originates.
- Causation was nevertheless proven by manufacturing the trigger in the working
  tree only: one entry in the 26.8 block of `SettingsChangesHistory.cpp` was
  renamed to a name no setting has. Unpatched, `SET compatibility = '25.8'` then
  raised `UNKNOWN_SETTING` and the session tripwire arm named the bogus setting
  (`build/test_patch_073_red.log`); with the guard, the same statement succeeded
  and the warning was logged (`build/test_patch_073_green.log`). The scratch edit
  was reverted before staging; `src/Core/SettingsChangesHistory.cpp` is untouched.
- Because the `MergeTree` change touches a path upstream also validates, the
  final run included `03999_stateless_settings_history` and
  `04652_system_documentation_settings_history`; both pass.

## Rollback {#rollback}

Safe and complete: no on-disk or ZooKeeper state, no setting, no format change.
Reverting restores fatal behavior on both doors. There is deliberately no runtime
switch — the guarded path should never be reached on a correctly built release.

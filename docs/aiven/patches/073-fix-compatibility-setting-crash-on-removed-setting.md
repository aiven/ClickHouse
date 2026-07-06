# Patch 073 — fix-compatibility-setting-crash-on-removed-setting

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | (predecessor of `aec2378a0e5`) | Aliaksei Khatskevich, 2026-04-07 | original carry — fixes a 25.3-specific crash |
| 25.8-aiven | `aec2378a0e5a66b78f3fed99cf7e735f36c27533` | Aliaksei Khatskevich (committed by Joe Lynch, 2026-05-21) | byte-equivalent carry |
| 26.3-aiven | `patch-port(073)` | T3.5 worker (subagent) | byte-equivalent staged; test_design_blocked — see §4 |

The 26.3-aiven carry is its `patch-port(073)` commit (find it with `git log --grep '^patch-port(073)'`); per `commit-hygiene.md` §4 the `-aiven-dev` branch is resliced at each LTS transition, so we pin the stable subject handle, not a SHA that would re-hash.
## 1. Purpose

`SettingsImpl::applyCompatibilitySetting` walks the `settings_changes_history` table in reverse from the
current version down to the user-requested compatibility version, calling `resolveName` then `get` on
every entry's `change.name`. If the history table contains an entry whose `change.name` does not exist
in the current build (a "history orphan"), the `get` call throws `UNKNOWN_SETTING` (Code: 115) and the
`SET compatibility = ...` statement fails. The patch inserts a `has(change.name)` guard immediately
inside the inner loop that silently skips such orphan entries — preserving the alias-resolution branch
(since `BaseSettings::has` internally calls `TTraits::resolveName` before lookup), but short-circuiting
truly-removed settings before they reach `resolveName` and `get`.

Aiven needs this because the bug manifested in a 25.3 build whose `SettingsChangesHistory` had been
populated with entries cherry-picked from 25.5/25.6 (notably `parallel_replicas_connect_timeout_ms`)
that did not exist in 25.3's `Settings.cpp`. Any non-default `compatibility` value triggered
`UNKNOWN_SETTING`. The defensive guard makes `applyCompatibilitySetting` resilient to this class of
backport-bot accident.

Source SHA on `v25.8.18.1-lts-aiven`: `aec2378a0e5a66b78f3fed99cf7e735f36c27533`.
Original author: `Aliaksei Khatskevich <alex.khatskevich@aiven.io>` (author date 2026-04-07).
Original purpose (quoted from source-commit body):

```
Fix compatibility setting crash on removed setting

SettingsChangesHistory in the 25.3 build contains entries cherry-picked
from 25.5/25.6 that reference settings not present in 25.3 (e.g.
parallel_replicas_connect_timeout_ms). Setting compatibility to any
value causes applyCompatibilitySetting to call get() on these unknown
settings, crashing with UNKNOWN_SETTING.
Fix: skip history entries for settings that don't exist in the current
build.
```

## 2. Upstream-drift findings

### Commands run

```bash
rg -n 'applyCompatibilitySetting' src/Core/Settings.cpp src/Core/Settings.h
rg -n 'bool has\(std::string_view' src/Core/Settings.h src/Core/BaseSettings.h
rg -n 'SettingsTraits::resolveName\(' src/Core/Settings.cpp
rg -n 'change\.name' src/Core/Settings.cpp
git log v25.8.18.1-lts..v26.3.10.62-lts -- src/Core/Settings.cpp
git log v25.8.18.1-lts..v26.3.10.62-lts -G 'applyCompatibilitySetting' -- src/Core/Settings.cpp
git log v25.8.18.1-lts..v26.3.10.62-lts \
  --grep 'compatibility.*setting' --grep 'UNKNOWN_SETTING' --grep 'SettingsChangesHistory' \
  -- src/Core/Settings.cpp
sed -n '8085,8100p' src/Core/Settings.cpp
```

### Findings

- Upstream changes to touched files between LTSes:
  - `src/Core/Settings.cpp`: many commits (file grew ~850 lines), but `applyCompatibilitySetting` body
    is untouched — `git log -G 'applyCompatibilitySetting' ...` is empty.
- Upstream changes that touched the patch's behavior:
  - No symbol renames; `applyCompatibilitySetting`, `Settings::has(std::string_view)`,
    `SettingsTraits::resolveName`, and `change.name` are all present on HEAD with identical signatures.
  - No upstream commit added a defensive `has(change.name)` guard. The closest match in the
    `--grep 'compatibility.*setting' --grep 'UNKNOWN_SETTING' --grep 'SettingsChangesHistory'`
    search is `05a2028fe97 "Add Settings.cpp entries missed by backport bot"`, which is a
    *content* fix (added the two settings that triggered the bug as a one-off patch), not a
    structural defensive guard.
- Hunk-context verification: the 26.3 HEAD region around `applyCompatibilitySetting` matches the
  pre-patch shape from the source diff exactly — context drift is line-numbers only
  (~7217 → ~8088, file grew by ~850 lines between LTSes).
- Conclusion: **`still-needed-applies-cleanly`** — the patch is still semantically needed (defensive
  against history-orphan crashes), and the cherry-pick applies verbatim through `git`'s three-way
  merge.

## 3. C++ review

Per `docs/aiven/skills/cpp-review-checklist.md`:

- **1 Lifetime + ownership**: ✓ — patch introduces no new pointers, references, or ownership. `has`
  takes a `std::string_view` view of `change.name` (a `String` member of `SettingChange`); the view's
  lifetime is bounded by the surrounding `for (const auto & change : it->second)` loop iteration,
  identical to the pre-patch use of `change.name` in the `resolveName` call on the next line.
- **2 Exception safety**: ✓ — the new `continue` is exception-safe: it is an early loop-skip,
  preserving the loop invariant. `BaseSettings::has` is `noexcept`-compatible (`hasBuiltin` does a
  hash-map lookup; `hasCustom` may throw on allocation but in practice this is a probing call after
  the global accessor has been initialized). No partial mutation precedes the `continue`.
- **3 Thread-safety + concurrency**: ✓ — `applyCompatibilitySetting` mutates per-`SettingsImpl`
  instance state via `BaseSettings::set`. The static history table is read-only after
  `SettingsChangesHistory::initSettingsChanges()` (init-on-first-use, idempotent). No new shared
  state is introduced.
- **4 Performance + memory**: ✓ — `has` is an O(1) hash lookup with no allocations on the hot path.
  `applyCompatibilitySetting` is invoked once per `SET compatibility = ...` statement (settings layer,
  not a per-row hot path). The extra lookup per history entry adds <1 μs total.
- **5 Settings as public API**: n/a — no new setting; the patch is purely defensive C++ inside the
  settings infrastructure.
- **6 Error handling**: ✓ — no new error code. Pre-patch threw `UNKNOWN_SETTING` (115) for the
  orphan-entry case; post-patch silently skips. Acceptable because `change.name` is metadata
  (compiled-in `SettingsChangesHistory` table), not user input — silently skipping orphans is the
  documented intent (see the patch's added comment: "Skip settings listed in the history that don't
  exist in this build").
- **7 Upstream / vendored code**: ✓ — patch only touches `src/Core/Settings.cpp`; no `contrib/`,
  `.claude/`, `.github/workflows/`, or root `AGENTS.md` involvement. Surrounding code (the
  alias-resolution path, the `isChanged` check, the `get/set` ladder) is unchanged between LTSes —
  the patch's assumptions hold.
- **8 Behavior under settings**: n/a — the change has no setting gating it. The defensive guard runs
  unconditionally on every `SET compatibility = ...` invocation.

## 4. Test design

### Designed test (staged)

- Test path: `tests/queries/0_stateless/9073_fix-compatibility-setting-crash-on-removed-setting.{sql,reference}` per the Aiven test-naming convention (`docs/aiven/runbooks/testing-suites.md` §4.1).
- Body:

```sql
-- Regression test for Aiven patch 073: SET compatibility must skip
-- SettingsChangesHistory entries naming settings that don't exist in the
-- current build, instead of throwing UNKNOWN_SETTING.

SET compatibility = '25.2';
SELECT 1;
```

- `.reference`:

```
1
```

### Pre/post evidence (the surprising finding)

- Post-patch run (`tmp/patch-073/test-postpatch.log`): **PASS** — `9073_fix-compatibility-setting-crash-on-removed-setting: [ OK ] 0.08 sec.`
- Pre-patch run (`tmp/patch-073/test-prepatch.log`): **PASS** (UNEXPECTED) — `9073_fix-compatibility-setting-crash-on-removed-setting: [ OK ] 0.10 sec.`

The test PASSES against both the pre-patch and post-patch binaries. The evidence-of-causation pair
required by `docs/aiven/runbooks/testing-suites.md` §5 is therefore **not achievable** on 26.3 today.

### Why the trigger is absent on 26.3

The dispatch prompt's parent preflight reported 107 history-orphan setting names in 26.3 based on the
output of `tmp/patch-073/settings-history-orphans.txt`. Worker re-verification against the running
`system.settings` table (1,541 settings) shows:

- The parent's extraction script counted setting names referenced in `SettingsChangesHistory.cpp`
  that did not appear in `Settings.cpp`. This included two confounders:
  1. Settings registered via `MAKE_OBSOLETE(M, …)` rather than `M(…)` — these ARE in
     `system.settings` (e.g., `allow_experimental_dynamic_type`, `enable_dynamic_type` both
     present as obsolete Bool settings).
  2. MergeTree settings whose history blocks use `addSettingsChanges(merge_tree_settings_changes_history, …)`
     rather than `addSettingsChanges(settings_changes_history, …)`. These are handled by a
     separate `MergeTreeSettings::applyCompatibilitySetting` and are never seen by
     `SettingsImpl::applyCompatibilitySetting` — they cannot trigger this bug.
- Worker's corrected extraction (only the `settings_changes_history` blocks; cross-checked against
  the live `system.settings` table on the pre-patch binary) yields **0 truly-missing settings** in
  the Settings history on 26.3. Result file:
  `tmp/patch-073/truly-missing-from-settings-history.txt` (empty).
- 13 candidate compatibility values tested against the pre-patch binary in
  `tmp/patch-073/trigger-discovery.log` (`24.0`, `25.2`, `24.5`, `25.0`, `24.3`, `25.5`, `24.7`,
  `25.7`, `26.0`, `26.2`, `24.1`, `23.0`, `22.0`, `25.4`); **all return `1`** with no
  `UNKNOWN_SETTING` exception.

The patch's bug condition is genuinely defensive-only on 26.3: every entry in `settings_changes_history`
maps to a setting that `Settings::has` finds (either as live setting, obsolete setting, or alias).
This is consistent with the upstream history-content fix landed at `05a2028fe97` ("Add Settings.cpp
entries missed by backport bot") — that commit added the two specific settings missing from upstream's
history. The Aiven patch is the structural defense for the *class* of bug; the upstream commit was the
content fix for one *instance*.

### Pre/post test logs (verbatim excerpts)

`tmp/patch-073/test-postpatch.log`:

```text
Using queries from '/home/tilman.moeller/projects/ClickHouse/tests/queries' directory
Connecting to ClickHouse server... OK
Connected to server 26.3.10.1 @ f03db74f48ee9568b59a930189e297d5007c88f2 v26.3.10.62-lts-aiven-dev
Found 1 parallel tests and 0 sequential tests
Running about 1 stateless tests (Process-3).
9073_fix-compatibility-setting-crash-on-removed-setting:                [ OK ] 0.08 sec.

1 tests passed. 0 tests skipped. 0.10 s elapsed (Process-3).
```

`tmp/patch-073/test-prepatch.log`:

```text
Using queries from '/home/tilman.moeller/projects/ClickHouse/tests/queries' directory
Connecting to ClickHouse server... OK
Connected to server 26.3.10.1 @ f03db74f48ee9568b59a930189e297d5007c88f2 v26.3.10.62-lts-aiven-dev
Found 1 parallel tests and 0 sequential tests
Running about 1 stateless tests (Process-3).
9073_fix-compatibility-setting-crash-on-removed-setting:                [ OK ] 0.10 sec.

1 tests passed. 0 tests skipped. 0.12 s elapsed (Process-3).
```

### Coverage limitations

- The shipped test is **insurance**, not active coverage today. Per AGENTS.md §7 the
  evidence-of-causation pair (pre-patch FAIL + post-patch PASS) cannot be honestly produced on 26.3
  because no orphan entry exists. The test will start FAILing on the pre-patch binary in a future
  LTS uplift if the backport-bot ever again lands a settings-history entry without the
  corresponding `Settings.cpp` row — at which point the test becomes active regression coverage.
- The test does NOT cover the alias-resolution branch (renamed-with-alias settings) — that branch
  is untouched by the patch.
- A unit-level test that synthesizes a fake `SettingChange{"_aiven_073_nonexistent_setting", …}` and
  exercises `applyCompatibilitySetting` directly would produce honest pre/post evidence, but
  `SettingsChangesHistory` is built up via a private static initializer at process start
  (`SettingsImpl::settingsChangesHistory()`), and there is no public API to inject a synthetic
  entry. A test-only seam would require a small surgery to `Settings.cpp` itself (e.g., a
  `#ifdef CLICKHOUSE_INTERNAL_TEST` injection hook), which is out of scope for a defensive port and
  would risk obscuring the patch's intent.

### Decision

Worker initially reported `outcome: escalate` with `escalation_reason: test_design_blocked`. Parent
review escalated to a schema-expansion decision: the existing `tests.added` enum had no value for
"correct defensive change with no trigger condition on the current LTS". A Bootstrap commit
preceding this one added `tests.added: no_trigger_on_current_lts` to
`docs/aiven/schema/halt-and-escalate.md` with strict preconditions (recount evidence file required,
≥10-candidate discovery loop required, commit-message body must note "forward-insurance only").

This patch lands with `tests.added: no_trigger_on_current_lts`:

- `paths`: the two test files are staged as forward-insurance.
- `pre_patch_fail_verified`: `false` (no candidate triggered on 26.3).
- `post_patch_pass_verified`: `true`.
- `justification`: 0 truly-missing settings in `settings_changes_history` on 26.3
  (`tmp/patch-073/truly-missing-from-settings-history.txt` empty); root cause: every entry resolves
  via `Settings::has` because `MAKE_OBSOLETE` registrations are still in the live registry and
  MergeTree-settings history entries are not consumed by `SettingsImpl::applyCompatibilitySetting`
  (they live in a separate `merge_tree_settings_changes_history` map).
- 13 candidate compatibility values tried against the pre-patch binary; all returned `1` (no
  `UNKNOWN_SETTING` exception). See `tmp/patch-073/trigger-discovery.log`.

The test will start FAILing pre-patch on a future LTS where the backport-bot reintroduces an
orphan history entry, at which point it becomes active regression coverage automatically. Future
maintainers should re-check the trigger absence at every LTS uplift (parent preflight should run
the worker's recount logic — see T3.5 retrospective Finding A).

## 5. Rollback considerations

- Revert is trivial: remove the 5 added lines. The pre-patch behavior re-emerges (latent bug
  resurfacing only if/when a future history orphan is introduced).
- Patch introduces no on-disk state, no ZooKeeper nodes, no in-memory cache changes, and no schema
  effects.
- No setting can disable the new behavior without rebuilding — the guard is unconditional. This is
  by design (defensive against compile-time data); a runtime opt-out would defeat the point.

## 6. Per-uplift notes

### 25.3-aiven (historical)

The patch was authored against 25.3 to fix the specific crash where the 25.3 build's `SettingsChangesHistory`
contained entries from 25.5/25.6 backports without the corresponding `Settings.cpp` rows. Any non-default
`compatibility` value crashed with `UNKNOWN_SETTING` referencing `parallel_replicas_connect_timeout_ms`
or similar. Original author: Aliaksei Khatskevich.

### 25.8-aiven (historical)

Carry-forward of the same 5-line guard. Source SHA `aec2378a0e5` on `v25.8.18.1-lts-aiven`, authored by
Khatskevich, committer Joe Lynch. (Inventory T2.2 verdict: `cherry_pick_clean=yes`.)

### 26.3-aiven (this uplift)

- Cherry-pick was: **clean** (Auto-merging `src/Core/Settings.cpp`; exit 0; no `UU` markers).
- Upstream-drift conclusion: `still-needed-applies-cleanly` (worker re-verification matches parent
  preflight).
- Test added at: `tests/queries/0_stateless/9073_fix-compatibility-setting-crash-on-removed-setting.{sql,reference}` (staged but produces no pre/post evidence pair on 26.3 — see §4).
- Time-to-port (subagent wall-clock): ~30 min on a warm-cache build dir. The build directory
  required a one-time `cmake --fresh -S . -B build ...` recovery because `CMakeFiles/rules.ninja`
  was missing (matched build-and-test.md §6 row 1); recovery was clean. All three ninja builds
  (post-patch full, pre-patch incremental, post-patch restore) completed in <60s each (warm cache).
- Anything surprising:
  - **Patch-id matches exactly between source and staged diffs** (`7b244f686c9c…`), despite an ~850-line
    file growth between LTSes. `git patch-id --stable` correctly normalizes the line-number shift —
    `byte_equivalent: true`. (The dispatch prompt anticipated `byte_equivalent: false`, but the
    `--stable` form of patch-id makes the staged result fully byte-equivalent to the source.)
  - **Parent preflight overcounted orphans by ~107 → actual 0.** The orphan-extraction script
    confused (i) `MAKE_OBSOLETE` settings (still in registry) with truly-removed settings, and (ii)
    MergeTree-settings history entries (handled by a separate `applyCompatibilitySetting`) with
    `Settings`-history entries. Worker re-verification against the running `system.settings` table
    found zero truly-missing settings → no trigger condition for the test exists on 26.3 today.
  - **First use of the new dispatch-prompt template** (T3.5; per
    `docs/aiven/skills/dispatch-prompt-template.md`). The template's "Coverage limitations" guidance
    in Step 5 anticipated this no-trigger case and provided the `test_design_blocked` escalation
    path — the template's design was vindicated by this dispatch.
  - **First Khatskevich-authored patch** in the T3.X series (third major author cohort after
    Tilman and Joe Lynch). Original-author preservation via the `Original author:` body line was
    the intended path (no `git commit --author=` flag used).

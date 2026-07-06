# Patch 075 — register-arrowflight-flags

## 0. Lineage

| LTS uplift | First-carry handle on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `9650c1a6f5bf849748f7b623d58f3eac997003d6` | Aliaksei Khatskevich (author) / Joe Lynch (committer), 2026-04-23 | original carry (the version we port FROM) |
| 26.3-aiven | `patch-port(075)` (staged) | T3 patch worker (T3.26), 2026-06-12 | conflict-free cherry-pick + parent `option()` block — see §2 and §6 |

The current uplift's row uses the stable handle `patch-port(075)` (NOT a SHA);
it stays "(staged)" until the human commits. Find it later with
`git log --grep '^patch-port(075)'`.

## 1. Purpose

Introduce the Aiven `REGISTER_ARROWFLIGHT_TABLE_ENGINE` and
`REGISTER_ARROWFLIGHT_FUNCTION` compile-time flags and upgrade the three
ArrowFlight registration guards to the two-level form
`USE_ARROWFLIGHT && REGISTER_ARROWFLIGHT_TABLE_ENGINE`. The `USE_ARROWFLIGHT`
term is the upstream compile-library gate (is the Arrow Flight client linked at
all); the `REGISTER_ARROWFLIGHT_TABLE_ENGINE` term is Aiven's per-surface
registration gate (should this build expose the engine/function even though the
library is present). This lets Aiven ship a build that links Arrow Flight
support yet keeps the engine/function unregistered until they are "completely
sure it is safe and fully integrated" — the durable motivation shared with the
rest of the `REGISTER_*` family.

This is the **final, sixth link of the `REGISTER_*` engine/function family**
(045 → 051 → 052 → 070 → 071 → **075**), and the ArrowFlight analogue of 071
(YTsaurus). Unlike 071 there is **no 045-style overlap** — every ArrowFlight
guard on HEAD is exactly `#if USE_ARROWFLIGHT` (the source's pre-image), so the
cherry-pick applied cleanly.

Source SHA on `v25.8.18.1-lts-aiven`: `9650c1a6f5bf849748f7b623d58f3eac997003d6`
(from `docs/aiven/uplifts/26.3/inventory.md` row 075).
Original author: `alex.khatskevich@aiven.io` (committer `joelynch112@gmail.com`).
Original purpose (verbatim from the source commit subject):

> Add register arrowflight flags

## 2. Upstream-drift findings

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`, not just textually applicable.

### Commands run

```bash
# ArrowFlight flags absent on HEAD (075 adds them):
grep -nE "REGISTER_ARROWFLIGHT_(TABLE_ENGINE|FUNCTION)" src/Common/config.h.in
#   -> ABSENT (good)                                       (tmp/patch-075/source.diff context)

# All 3 guards are the source pre-image '#if USE_ARROWFLIGHT' on HEAD:
grep -nB1 -A1 "registerStorageArrowFlight"       src/Storages/registerStorages.cpp
#   -> 32:#if USE_ARROWFLIGHT (forward-decl), 151:#if USE_ARROWFLIGHT (call)  (tmp/patch-075/head-storage.log)
grep -nB1 -A1 "registerTableFunctionArrowFlight" src/TableFunctions/registerTableFunctions.cpp
#   -> 41:#if USE_ARROWFLIGHT                               (tmp/patch-075/head-tf.log)

# Insertion context present (AZURE_BLOB + YTSAURUS lines, the latter from committed 071):
grep -nE "REGISTER_(AZURE_BLOB|YTSAURUS)_(TABLE_ENGINE|FUNCTION)" src/Common/config.h.in
#   -> 108 AZURE_BLOB_TABLE_ENGINE / 109 YTSAURUS_TABLE_ENGINE /
#      128 AZURE_BLOB_FUNCTION / 129 YTSAURUS_FUNCTION       (tmp/patch-075/head-config-anchors.log)

# No ArrowFlight option yet; engines/functions block ends at REGISTER_YTSAURUS_FUNCTION:
grep -nE "option\(REGISTER_(ARROWFLIGHT|YTSAURUS_FUNCTION)" src/configure_config.cmake
#   -> 297:option(REGISTER_YTSAURUS_FUNCTION "..." ON)       (tmp/patch-075/head-cmake.log)
```

### Findings

- `src/Common/config.h.in`: both ArrowFlight flags ABSENT on HEAD; the source's
  insertion anchors (`…AZURE_BLOB_TABLE_ENGINE` / `…AZURE_BLOB_FUNCTION`, with
  `…YTSAURUS_*` trailing context from committed `patch-port(071)`) match the
  source pre-image, so the `config.h.in` hunks applied with no conflict (the new
  flags landed at line 109 after `…AZURE_BLOB_TABLE_ENGINE` and after
  `…AZURE_BLOB_FUNCTION`).
- `src/Storages/registerStorages.cpp` (forward-decl + call) and
  `src/TableFunctions/registerTableFunctions.cpp`: HEAD pre-image is
  `#if USE_ARROWFLIGHT` on all three, matching the source pre-image, so every
  hunk applied CLEAN → `#if USE_ARROWFLIGHT && REGISTER_ARROWFLIGHT_TABLE_ENGINE`.
  No 045-style overlap (contrast 071's dictionary-source divergence).
- No upstream symbol rename affects the patch: `registerStorageArrowFlight` and
  `registerTableFunctionArrowFlight` are both present on HEAD.
- Conclusion: **`still-needed-but-rewrite`** — the cherry-pick itself is clean,
  but the port is not byte-equivalent because it adds the parent-authored
  `option(... ON)` block the 25.8 source omits (PC-1, §6).

## 3. C++ review

Apply `docs/aiven/skills/cpp-review-checklist.md`. The change is preprocessor
guards + build-system options only; no runtime data path is altered when the
flags + `USE_ARROWFLIGHT` are ON (the default).

- 1 Lifetime + ownership: `n/a` — no object lifetime touched; guards only elide
  the `registerStorageArrowFlight` / `registerTableFunctionArrowFlight` calls
  (and the storage forward-decl) when the flag is 0.
- 2 Exception safety: `n/a` — no new throwing code; eliding a `registerX` call
  cannot break exception safety of the surrounding scope.
- 3 Thread-safety + concurrency: `n/a` — registration runs once at startup; no
  concurrency semantics changed.
- 4 Performance + memory: ✓ — strictly reductive when the flag is 0 (surface not
  registered); default build (flags + `USE_ARROWFLIGHT` ON) is behavior-identical
  to upstream.
- 5 Settings as public API: ✓ — no server *setting* added; `USE_ARROWFLIGHT` and
  the `REGISTER_ARROWFLIGHT_*` flags are compile-time CMake options (default ON).
  Default build ⇒ no public-API surface change.
- 6 Error handling: ✓ — when a surface is compiled out, using it fails via the
  existing upstream "unknown table engine/function" path; no new error code.
- 7 Upstream / vendored code: ✓ — all edited files are ClickHouse-owned
  (`src/Common/config.h.in`, `src/Storages/registerStorages.cpp`,
  `src/TableFunctions/registerTableFunctions.cpp`, `src/configure_config.cmake`);
  none under `contrib/**`.
- 8 Behavior under settings: ✓ — with the flags + `USE_ARROWFLIGHT` ON (default)
  the engine and function are still registered (verified: `system.table_engines`
  smoke = 1 for `ArrowFlight`, `system.table_functions` smoke = 1 for
  `arrowFlight`; generated `config.h` shows both REGISTER flags = 1 and
  `USE_ARROWFLIGHT = 1`). With a flag set to 0 the corresponding surface is
  absent and the build links.

## 4. Test design

(b) **Documented justification — no new test, with reference to existing
upstream tests (`tests.added: no_justified`), mirroring committed siblings
045/051/052/070/071.**

This patch is **build-system-only** (two `#cmakedefine01` flags, two `option()`
lines, three preprocessor-guard upgrades) and adds **no runtime behavior on a
default build**: `USE_ARROWFLIGHT` defaults ON and both `REGISTER_ARROWFLIGHT_*`
options default ON, so each two-level guard
`USE_ARROWFLIGHT && REGISTER_ARROWFLIGHT_TABLE_ENGINE` is **true on a default
build → zero default-behavior change**. The behavior the patch *introduces* —
eliding the ArrowFlight surface — is a **compile-time** property of the produced
binary, gated by CMake flags, not a runtime setting. A stateless `.sql`/`.sh`
test cannot observe it on the default build (the surfaces are present), and
producing a "disabled" binary requires a second CMake configure
(`-DREGISTER_ARROWFLIGHT_TABLE_ENGINE=0`) + rebuild that the stateless runner
cannot express. A worktree-flip evidence pair is also unreachable: pre- and
post-patch both register ArrowFlight on a default build (pass-pass), so there is
no differential observable. Per AGENTS §7(b) the correct posture is
`no_justified` with build-system verification + existing upstream tests that
exercise the gated surfaces.

### Build-system verification (the correctness evidence)

- **Generated `config.h` shows both new flags = 1** plus `USE_ARROWFLIGHT = 1`
  (`tmp/patch-075/configh-generated.log`): `REGISTER_ARROWFLIGHT_TABLE_ENGINE = 1`,
  `REGISTER_ARROWFLIGHT_FUNCTION = 1`, `USE_ARROWFLIGHT = 1` in
  `build/includes/configs/config.h` — the flags resolve ON on a default build, so
  the new guards are true and nothing is disabled by default.
- **Flag↔option coverage diff EMPTY** (`tmp/patch-075/flag-option-coverage.log`):
  the set of newly-added `config.h.in` flags equals the set of newly-added
  `option()` names, both exactly `{REGISTER_ARROWFLIGHT_TABLE_ENGINE,
  REGISTER_ARROWFLIGHT_FUNCTION}` — no flag is left without a matching ON option
  (which would silently resolve to 0).
- **Reconfigure + broad rebuild green** (`tmp/patch-075/build-postpatch.log`):
  editing `config.h.in`/`configure_config.cmake` forced a CMake reconfigure
  (`Re-running CMake…`, `Configuring done`, `Generating done`) and a 3280-step
  rebuild (config.h is widely included), ninja exit 0.

### Existing upstream tests exercising the gated surfaces (run post-patch, PASS)

- `tests/queries/0_stateless/01645_system_table_engines.sql` — selects from
  `system.table_engines`, the registration catalog that
  `registerStorageArrowFlight(factory)` feeds.
- `tests/queries/0_stateless/02414_all_new_table_functions_must_be_documented.sql`
  (and the broader `system.table_functions` catalog) — the registration catalog
  that `registerTableFunctionArrowFlight(factory)` feeds.
- Default-build smoke (`tmp/patch-075/smoke-engine.log`,
  `tmp/patch-075/smoke-tf.log`): on the post-patch server,
  `SELECT count() FROM system.table_engines WHERE name='ArrowFlight'` = **1** and
  `SELECT count() FROM system.table_functions WHERE name='arrowFlight'` = **1**
  (the registered names are `ArrowFlight` for the engine and `arrowFlight` for
  the function; a lowercase `arrowflight` alias also exists). Because
  `USE_ARROWFLIGHT = 1` on this build, engine=1/TF=1 is the expected result and
  confirms both surfaces still register on a default build.
- Why this distinguishes the contract: with the flags + `USE_ARROWFLIGHT` ON
  (default) the surfaces are still registered and the catalog queries behave
  exactly as upstream — the default-build invariant this patch must preserve.

## 5. Rollback considerations

- Revert safety: ✓ — pure compile-time guards + build-system options. Reverting
  removes the `#cmakedefine01`/`option()` lines and the `&& REGISTER_ARROWFLIGHT_TABLE_ENGINE`
  suffixes; no schema migration, no on-disk format change, no ZK state.
- State surviving restart: none.
- Disabling the new behavior without rebuilding: `n/a` — the toggles are
  compile-time CMake options. To unregister the surface, rebuild with
  `-DREGISTER_ARROWFLIGHT_TABLE_ENGINE=0` (or `-DUSE_ARROWFLIGHT=0` to drop the
  whole library gate).

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — patch first appears on the 25.8-aiven line.

### 25.8-aiven (historical)

Original carry. Author: Aliaksei Khatskevich; committer: Joe Lynch
(`9650c1a6f5`). The 25.8 commit touched three files: `config.h.in` (+2 flags),
`registerStorages.cpp` (forward-decl + call, `#if USE_ARROWFLIGHT` →
`#if USE_ARROWFLIGHT && REGISTER_ARROWFLIGHT_TABLE_ENGINE`), and
`registerTableFunctions.cpp` (same upgrade). It did NOT touch
`configure_config.cmake`. No 25.8 stateless test (build-system change).

### 26.3-aiven (this uplift)

- Cherry-pick was: **clean** (conflict-free). All three guards on HEAD were the
  source pre-image `#if USE_ARROWFLIGHT` and the `config.h.in` insertion context
  (`…AZURE_BLOB_*` with the `…YTSAURUS_*` trailing lines from committed 071)
  matched, so `git cherry-pick --no-commit -x` auto-merged the 3 source files
  with no conflict. No 045-style overlap (contrast 071).
- 26.3 adaptation (PC-1): added the two `option(REGISTER_ARROWFLIGHT_* … ON)`
  lines to `src/configure_config.cmake` (absent from the 25.8 source) at the END
  of the "table engines and table functions" block, immediately after
  `option(REGISTER_YTSAURUS_FUNCTION … ON)` (placed BY CATEGORY — these are a
  table engine + a table function, not a dictionary source). Without a matching
  `option(REGISTER_<NAME> … ON)` an undefined `#cmakedefine01` resolves to 0 and
  silently disables the surface. `byte_equivalent: false` (this is the only
  divergence from the source; decomposition shows exactly these 2 added lines).
- **FAITHFUL-PORT QUIRK — `REGISTER_ARROWFLIGHT_FUNCTION` is defined but
  referenced by NO guard.** The 25.8 source adds
  `#cmakedefine01 REGISTER_ARROWFLIGHT_FUNCTION` to `config.h.in`, but the
  ArrowFlight **table-function** guard it writes uses
  `REGISTER_ARROWFLIGHT_TABLE_ENGINE`, NOT `REGISTER_ARROWFLIGHT_FUNCTION`. So
  `REGISTER_ARROWFLIGHT_FUNCTION` ships **defined-but-referenced-by-no-guard**.
  This was **ported verbatim — NOT "fixed"** by rewiring the TF guard to
  `_FUNCTION`. Rationale (parent decision, ratified): faithfulness to the 25.8
  source + the Aiven RPM spec contract — the spec passes whatever flags the
  source defined (`-DREGISTER_ARROWFLIGHT_TABLE_ENGINE` /
  `-DREGISTER_ARROWFLIGHT_FUNCTION`), and silently diverging the guard wiring
  could break that contract. The flag still gets its `option(… ON)` (PC-1) so the
  coverage check stays symmetric and the symbol is defined. The TF guard
  therefore tracks `REGISTER_ARROWFLIGHT_TABLE_ENGINE` — toggling
  `REGISTER_ARROWFLIGHT_FUNCTION` alone has no compile-time effect on this base
  (intentional, matches 25.8).
- Spec consumer-contract: the Aiven RPM build passes
  `-DREGISTER_ARROWFLIGHT_TABLE_ENGINE` / `-DREGISTER_ARROWFLIGHT_FUNCTION`, which
  now have matching `option()` declarations so the `-D` cache value wins over the
  default ON. Cross-ref: 071 (the YTsaurus analogue; same PC-1 `option()` pattern,
  same family).
- Upstream-drift conclusion: `still-needed-but-rewrite` (§2).
- Test: no new stateless test — `no_justified`, build-system-only, default
  `USE_ARROWFLIGHT=1` + both flags ON ⇒ no default-behavior change. Build-system
  verification: generated `config.h` both flags = 1 + `USE_ARROWFLIGHT = 1`
  (`tmp/patch-075/configh-generated.log`), coverage diff EMPTY
  (`tmp/patch-075/flag-option-coverage.log`), reconfigure + rebuild green
  (`tmp/patch-075/build-postpatch.log`, ninja exit 0), smoke = 1/1
  (`tmp/patch-075/smoke-engine.log`, `tmp/patch-075/smoke-tf.log`).
- Time-to-port: build was a **broad rebuild** (~4 min, 3280 steps) because
  `config.h.in` is widely included and forced a CMake reconfigure. Build dir was
  warm-cache (sccache hot).
- Anything surprising: the source's `REGISTER_ARROWFLIGHT_FUNCTION` is a
  defined-but-unused flag (the TF guard uses `_TABLE_ENGINE`); ported verbatim
  per parent policy (see the quirk note above). Completing the family
  (045/051/052/070/071/075).

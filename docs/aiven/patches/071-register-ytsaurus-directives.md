# Patch 071 — register-ytsaurus-directives

## 0. Lineage

| LTS uplift | First-carry handle on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `1b350e6b6522ba1f92ed938b0339ce1d5071994c` | Aliaksei Khatskevich (author) / Joe Lynch (committer), 2026-03-18 | original carry (the version we port FROM) |
| 26.3-aiven | `patch-port(071)` (staged) | T3 patch worker (T3.25), 2026-06-12 | conflict-resolved (dict-source flag dedup against 045; dict guard upgraded to two-level form) — see §2 and §6 |

The current uplift's row uses the stable handle `patch-port(071)` (NOT a SHA);
it stays "(staged)" until the human commits. Find it later with
`git log --grep '^patch-port(071)'`.

## 1. Purpose

Introduce the Aiven `REGISTER_YTSAURUS_TABLE_ENGINE` and `REGISTER_YTSAURUS_FUNCTION`
compile-time flags and upgrade the three YTsaurus registration guards to the
two-level form `USE_YTSAURUS && REGISTER_YTSAURUS_*`. The `USE_YTSAURUS` term is
the upstream compile-library gate (is the YTsaurus client linked at all); the
`REGISTER_YTSAURUS_*` term is Aiven's per-surface registration gate (should this
build expose the engine/function even though the library is present). This lets
Aiven ship a build that links YTsaurus support yet keeps the engine/function/
dictionary-source unregistered until they are "completely sure it is safe and
fully integrated" — the durable motivation shared with `patch-port(070)`.

This is the **fourth link of the `REGISTER_*` engine/function family**
(045 → 051 → 052 → 070 → **071** → 075). 045 introduced the dictionary-source
flag (`REGISTER_DICTIONARY_SOURCE_YTSAURUS`) and its option; 070 wrapped the
storage call in `#if USE_YTSAURUS` for build-correctness. 071 adds the two
remaining REGISTER flags and prepends `USE_YTSAURUS && ` to all three YTsaurus
guards (engine, function, dictionary-source).

Source SHA on `v25.8.18.1-lts-aiven`: `1b350e6b6522ba1f92ed938b0339ce1d5071994c`
(from `docs/aiven/uplifts/26.3/inventory.md` row 071).
Original author: `alex.khatskevich@aiven.io` (committer `joelynch112@gmail.com`).
Original purpose (verbatim from the source commit subject):

> Add REGISTER_YTSAURUS directives

## 2. Upstream-drift findings

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`, not just textually applicable.

### Commands run

```bash
# Divergence A — dict-source flag already present on HEAD (added by 045):
grep -n "REGISTER_DICTIONARY_SOURCE_YTSAURUS" src/Common/config.h.in
#   -> 105:#cmakedefine01 REGISTER_DICTIONARY_SOURCE_YTSAURUS   (tmp/patch-071/head-dictflag.log)

# Divergence B — dict guard pre-image on HEAD is the 045 (REGISTER-only) form:
grep -nB1 -A1 "registerDictionarySourceYTsaurus(source_factory)" \
  src/Dictionaries/registerDictionaries.cpp
#   -> 89:#if REGISTER_DICTIONARY_SOURCE_YTSAURUS                (tmp/patch-071/head-dictguard.log)

# Clean hunks — pre-image is #if USE_YTSAURUS (storage from 070, tf from upstream):
grep -nB1 -A1 "registerStorageYTsaurus(factory)"      src/Storages/registerStorages.cpp
#   -> 197:#if USE_YTSAURUS                                     (tmp/patch-071/head-storage.log)
grep -nB1 -A1 "registerTableFunctionYTsaurus(factory)" src/TableFunctions/registerTableFunctions.cpp
#   -> 95:#if USE_YTSAURUS                                      (tmp/patch-071/head-tf.log)

# dict-source option already present (045); engine/func options absent:
grep -nE "option\(REGISTER_(DICTIONARY_SOURCE_YTSAURUS|YTSAURUS)" src/configure_config.cmake
#   -> 255:option(REGISTER_DICTIONARY_SOURCE_YTSAURUS "..." ON)  (tmp/patch-071/head-cmake.log)
```

### Findings

- `src/Common/config.h.in`: the dictionary-source flag
  `#cmakedefine01 REGISTER_DICTIONARY_SOURCE_YTSAURUS` is ALREADY on HEAD (line 105,
  committed `patch-port(045)`). The source commit re-adds it; this is **Divergence A**
  (both-added). Git's 3-way merge recognized the line as common and merged
  `config.h.in` WITHOUT a conflict, keeping a single copy and inserting the two
  genuinely-new flags (`REGISTER_YTSAURUS_TABLE_ENGINE` at line 109 after
  `…AZURE_BLOB_TABLE_ENGINE`; `REGISTER_YTSAURUS_FUNCTION` at line 129 after
  `…AZURE_BLOB_FUNCTION`).
- `src/Dictionaries/registerDictionaries.cpp`: HEAD's guard is the 045 form
  `#if REGISTER_DICTIONARY_SOURCE_YTSAURUS` (line 89). The source rewrites
  `#if USE_YTSAURUS` → `#if USE_YTSAURUS && REGISTER_DICTIONARY_SOURCE_YTSAURUS`, so
  the pre-images differ — this is **Divergence B** (the cherry-pick conflicted).
  Resolved to the source's end-state `#if USE_YTSAURUS && REGISTER_DICTIONARY_SOURCE_YTSAURUS`
  (the incoming side of the conflict was already exactly that). 045's REGISTER
  term is preserved.
- `src/Storages/registerStorages.cpp` and `src/TableFunctions/registerTableFunctions.cpp`:
  HEAD pre-image is `#if USE_YTSAURUS` on both (storage from 070, function from
  upstream), matching the source pre-image, so both hunks applied CLEAN:
  `#if USE_YTSAURUS && REGISTER_YTSAURUS_TABLE_ENGINE` and
  `#if USE_YTSAURUS && REGISTER_YTSAURUS_FUNCTION`.
- Conclusion: **`still-needed-but-rewrite`**. Two hand-resolutions vs committed
  045/070 (dict-source flag dedup + dict guard upgrade); net-new = 2 flags + 2
  options + 3 guard upgrades. No upstream symbol rename affects the patch
  (`registerStorageYTsaurus` / `registerTableFunctionYTsaurus` /
  `registerDictionarySourceYTsaurus` all present on HEAD).

### Required deviation from the literal 25.8 patch (intentional)

1. **Drop the duplicate `config.h.in` dict-source line (Divergence A).** Keep the
   single line shipped by `patch-port(045)`; do not duplicate it.
2. **Resolve the dict guard to the two-level form (Divergence B).** Prepend
   `USE_YTSAURUS && ` to HEAD's 045 guard rather than replacing the source's
   `#if USE_YTSAURUS` pre-image; the `REGISTER_DICTIONARY_SOURCE_YTSAURUS` term
   from 045 is kept.
3. **Add the `option(... ON)` block to `src/configure_config.cmake` (PC-1).** The
   25.8 source omits `configure_config.cmake`; without a matching
   `option(REGISTER_<NAME> … ON)` an undefined `#cmakedefine01` resolves to 0 and
   silently disables the surface. Two options added
   (`REGISTER_YTSAURUS_TABLE_ENGINE`, `REGISTER_YTSAURUS_FUNCTION`); the
   dict-source option is NOT re-added (already from 045). This is the reason
   `byte_equivalent: false`, mirroring `patch-port(052)`/`(051)`.

## 3. C++ review

Apply `docs/aiven/skills/cpp-review-checklist.md`. The change is preprocessor
guards + build-system options only; no runtime data path is altered when the
flags + `USE_YTSAURUS` are ON (the default).

- 1 Lifetime + ownership: `n/a` — no object lifetime touched; guards only elide
  `registerStorageYTsaurus` / `registerTableFunctionYTsaurus` /
  `registerDictionarySourceYTsaurus` calls when a flag is 0.
- 2 Exception safety: `n/a` — no new throwing code; eliding a `registerX` call
  cannot break exception safety of the surrounding scope.
- 3 Thread-safety + concurrency: `n/a` — registration runs once at startup; no
  concurrency semantics changed.
- 4 Performance + memory: ✓ — strictly reductive when a flag is 0 (surface not
  registered); default build (all flags + `USE_YTSAURUS` ON) is behavior-identical
  to upstream.
- 5 Settings as public API: ✓ — no server *setting* added; `USE_YTSAURUS` and the
  `REGISTER_YTSAURUS_*` flags are compile-time CMake options (default ON). Default
  build ⇒ no public-API surface change.
- 6 Error handling: ✓ — when a surface is compiled out, using it fails via the
  existing upstream "unknown table engine/function/dictionary source" path; no
  new error code needed.
- 7 Upstream / vendored code: ✓ — all edited files are ClickHouse-owned
  (`src/Common/config.h.in`, `src/Dictionaries/registerDictionaries.cpp`,
  `src/Storages/registerStorages.cpp`, `src/TableFunctions/registerTableFunctions.cpp`,
  `src/configure_config.cmake`); none under `contrib/**`.
- 8 Behavior under settings: ✓ — with all flags + `USE_YTSAURUS` ON (default) the
  engine and function are still registered (verified: `system.table_engines` and
  `system.table_functions` smoke = 1 each; generated `config.h` shows all three
  REGISTER flags = 1). With a flag set to 0 the corresponding surface is absent
  and the build links.

## 4. Test design

(b) **Documented justification — no new test, with reference to an existing
upstream test (`tests.added: no_justified`), mirroring committed siblings
045/051/052/070.**

This patch is **build-system-only** (two `#cmakedefine01` flags, two `option()`
lines, three preprocessor-guard upgrades) and adds **no runtime behavior on a
default build**: `USE_YTSAURUS` defaults ON and all `REGISTER_YTSAURUS_*` options
default ON, so each two-level guard `USE_YTSAURUS && REGISTER_YTSAURUS_*` is **true
on a default build → zero default-behavior change**. The behavior the patch
*introduces* — eliding a YTsaurus surface — is a **compile-time** property of the
produced binary, gated by CMake flags, not a runtime setting. A stateless
`.sql`/`.sh` test cannot observe it on the default build (the surfaces are
present), and producing a "disabled" binary requires a second CMake configure
(`-DREGISTER_YTSAURUS_TABLE_ENGINE=0` etc.) + rebuild that the stateless runner
cannot express. A worktree-flip evidence pair is also unreachable: pre- and
post-patch both register YTsaurus on a default build (pass-pass), so there is no
differential observable. Per AGENTS §7(b) the correct posture is `no_justified`
with build-system verification + an existing upstream test that exercises the
gated surfaces.

### Build-system verification (the correctness evidence)

- **Generated `config.h` shows all three flags = 1**
  (`tmp/patch-071/configh-generated.log`): `REGISTER_YTSAURUS_TABLE_ENGINE = 1`,
  `REGISTER_YTSAURUS_FUNCTION = 1`, `REGISTER_DICTIONARY_SOURCE_YTSAURUS = 1`
  (and `USE_YTSAURUS = 1`) in `build/includes/configs/config.h` — the flags
  resolve ON on a default build, so the new guards are true and nothing is
  disabled by default.
- **Flag↔option coverage diff EMPTY** (`tmp/patch-071/flag-option-coverage.log`):
  the set of newly-added `config.h.in` flags equals the set of newly-added
  `option()` names, both exactly `{REGISTER_YTSAURUS_TABLE_ENGINE,
  REGISTER_YTSAURUS_FUNCTION}` — no flag is left without a matching ON option
  (which would silently resolve to 0).
- **Reconfigure + broad rebuild green** (`tmp/patch-071/build-postpatch.log`):
  editing `config.h.in`/`configure_config.cmake` forced a CMake reconfigure
  (`Re-running CMake…`, `Configuring done`) and a 3281-step rebuild (config.h is
  widely included), ninja exit 0.

### Existing upstream tests exercising the gated surfaces (run post-patch, PASS)

- `tests/queries/0_stateless/01645_system_table_engines.sql` — selects from
  `system.table_engines`, the registration catalog that
  `registerStorageYTsaurus(factory)` feeds.
- `tests/queries/0_stateless/02414_all_new_table_functions_must_be_documented.sql`
  (and the broader `system.table_functions` catalog) — the registration catalog
  that `registerTableFunctionYTsaurus(factory)` feeds.
- Default-build smoke (`tmp/patch-071/smoke-engine.log`,
  `tmp/patch-071/smoke-tf.log`): on the post-patch server,
  `SELECT count() FROM system.table_engines WHERE name='YTsaurus'` = **1** and
  `SELECT count() FROM system.table_functions WHERE name='ytsaurus'` = **1** —
  confirming both surfaces still register on the default build (the registered
  names are `YTsaurus` for the engine and `ytsaurus` for the function).
- Why this distinguishes the contract: with all flags + `USE_YTSAURUS` ON
  (default) the surfaces are still registered and the catalog queries behave
  exactly as upstream — the default-build invariant this patch must preserve.

## 5. Rollback considerations

- Revert safety: ✓ — pure compile-time guards + build-system options. Reverting
  removes the `#cmakedefine01`/`option()` lines and the `USE_YTSAURUS && ` prefixes;
  no schema migration, no on-disk format change, no ZK state.
- State surviving restart: none.
- Disabling the new behavior without rebuilding: `n/a` — the toggles are
  compile-time CMake options. To unregister a surface, rebuild with
  `-DREGISTER_YTSAURUS_TABLE_ENGINE=0` / `-DREGISTER_YTSAURUS_FUNCTION=0` /
  `-DREGISTER_DICTIONARY_SOURCE_YTSAURUS=0` (or `-DUSE_YTSAURUS=0` to drop all).

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — patch first appears on the 25.8-aiven line.

### 25.8-aiven (historical)

Original carry. Author: Aliaksei Khatskevich; committer: Joe Lynch
(`1b350e6b65`). The 25.8 commit touched four files: `config.h.in` (+3 flags),
`registerDictionaries.cpp` / `registerStorages.cpp` /
`registerTableFunctions.cpp` (each `#if USE_YTSAURUS` → `#if USE_YTSAURUS &&
REGISTER_YTSAURUS_*` / `… && REGISTER_DICTIONARY_SOURCE_YTSAURUS`). It did NOT
touch `configure_config.cmake`. No 25.8 stateless test (build-system change).

### 26.3-aiven (this uplift)

- Cherry-pick was: **conflict-resolved**.
  - `src/Common/config.h.in` (**Divergence A**): the source's dict-source flag
    was already on HEAD (045, line 105). Git's 3-way merge merged the file
    WITHOUT conflict — single dict-source line kept, the two new flags added
    (lines 109, 129). No duplicate.
  - `src/Dictionaries/registerDictionaries.cpp` (**Divergence B**): CONFLICTED.
    HEAD had `#if REGISTER_DICTIONARY_SOURCE_YTSAURUS` (045); the incoming side
    was `#if USE_YTSAURUS && REGISTER_DICTIONARY_SOURCE_YTSAURUS` (the desired
    end-state). Resolved by keeping the incoming line; the
    `REGISTER_DICTIONARY_SOURCE_YTSAURUS` term is preserved (045 not regressed).
  - `src/Storages/registerStorages.cpp` + `src/TableFunctions/registerTableFunctions.cpp`:
    applied CLEAN (`#if USE_YTSAURUS` → `#if USE_YTSAURUS && REGISTER_YTSAURUS_TABLE_ENGINE`
    / `… && REGISTER_YTSAURUS_FUNCTION`).
- 26.3 adaptation: added the two `option(REGISTER_YTSAURUS_* … ON)` lines to
  `src/configure_config.cmake` (PC-1; absent from the 25.8 source) after the
  existing `option(REGISTER_DICTIONARY_SOURCE_YTSAURUS … ON)`. The dict-source
  flag/option already shipped in `patch-port(045)`, so the duplicate `config.h.in`
  line is dropped and the dict guard is resolved to
  `USE_YTSAURUS && REGISTER_DICTIONARY_SOURCE_YTSAURUS`. `byte_equivalent: false`.
  Cross-ref: 045 (dict-source flag/option/guard), 070 (`USE_YTSAURUS` storage
  guard). Spec consumer-contract: the Aiven RPM build passes
  `-DREGISTER_YTSAURUS_TABLE_ENGINE` / `-DREGISTER_YTSAURUS_FUNCTION` (and the
  pre-existing `-DREGISTER_DICTIONARY_SOURCE_YTSAURUS`), which now have matching
  `option()` declarations so the `-D` cache value wins over the default ON.
- Upstream-drift conclusion: `still-needed-but-rewrite` (§2).
- Test: no new stateless test — `no_justified`, build-system-only, default
  `USE_YTSAURUS=1` + all flags ON ⇒ no default-behavior change. Build-system
  verification: generated `config.h` three flags = 1
  (`tmp/patch-071/configh-generated.log`), coverage diff EMPTY
  (`tmp/patch-071/flag-option-coverage.log`), reconfigure + rebuild green
  (`tmp/patch-071/build-postpatch.log`, ninja exit 0), smoke = 1/1
  (`tmp/patch-071/smoke-engine.log`, `tmp/patch-071/smoke-tf.log`).
- Time-to-port: build was a **broad rebuild** (~5 min, 3281 steps) because
  `config.h.in` is widely included and forced a CMake reconfigure — heavier than
  070's 20-step incremental, as expected. Build dir was warm-cache (sccache hot).
- Anything surprising: Divergence A did NOT surface as a textual conflict —
  git's 3-way merge recognized the 045 dict-source line as common context and
  merged `config.h.in` cleanly, so only `registerDictionaries.cpp` (Divergence B)
  needed hand-resolution. The flag↔option coverage diff and the generated
  `config.h` 3×1 check still independently confirm no duplication and no
  silently-disabled surface.

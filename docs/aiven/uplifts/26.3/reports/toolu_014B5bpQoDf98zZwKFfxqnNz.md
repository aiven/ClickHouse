<!-- subagentStop archive | source=transcript | status=completed | type=general-purpose | ts=2026-06-12T09:40:26Z -->

Everything is verified and staged. Here is the Step 8 exit report.

---
outcome: success
patch_slug: register-arrowflight-flags
source_sha: 9650c1a6f5bf849748f7b623d58f3eac997003d6
proposed_commit:
  staged_files:
    - src/Common/config.h.in
    - src/Storages/registerStorages.cpp
    - src/TableFunctions/registerTableFunctions.cpp
    - src/configure_config.cmake
    - docs/aiven/patches/075-register-arrowflight-flags.md
  commit_message: |
    patch-port(075): Add register arrowflight flags

    Original author: Aliaksei Khatskevich <alex.khatskevich@aiven.io>, 2026-04-23.

    (cherry picked from commit 9650c1a6f5bf849748f7b623d58f3eac997003d6)

    26.3 adaptation: added the matching option(REGISTER_ARROWFLIGHT_* ... ON) lines
    to src/configure_config.cmake (absent from the 25.8 source). Without them
    #cmakedefine01 resolves the undefined symbols to 0 and silently disables the
    engine. All flags default ON, mirroring patch-port(045)/(051)/(052)/(071) - no
    default-behavior change. Note: REGISTER_ARROWFLIGHT_FUNCTION is defined to match
    the source but is referenced by no guard (the table-function guard uses
    REGISTER_ARROWFLIGHT_TABLE_ENGINE); kept verbatim for source/spec fidelity.
    byte_equivalent: false.
  byte_equivalent: false
tests:
  added: no_justified
  kind: upstream-existing
  paths: []
  upstream_reference:
    - tests/queries/0_stateless/01645_system_table_engines.sql
    - tests/queries/0_stateless/02414_all_new_table_functions_must_be_documented.sql
  pre_patch_fail_verified: false
  post_patch_pass_verified: true
  justification: |
    Build-system-only change (two #cmakedefine01 flags, two option() lines, three
    preprocessor-guard upgrades). USE_ARROWFLIGHT defaults ON and both
    REGISTER_ARROWFLIGHT_* options default ON, so each two-level guard
    `USE_ARROWFLIGHT && REGISTER_ARROWFLIGHT_TABLE_ENGINE` is true on a default
    build → zero default-behavior change. The introduced behavior (eliding the
    ArrowFlight surface) is a compile-time property requiring a second CMake
    configure + rebuild, which the stateless runner cannot express; a worktree-flip
    pair is pass-pass (no differential). Per AGENTS §7(b) the posture is
    no_justified with build-system verification + existing upstream tests that
    exercise the gated catalogs (system.table_engines / system.table_functions),
    run post-patch and observed to pass (smoke = 1/1). Mirrors committed
    045/051/052/070/071.
escalation_reason: none
---

## Tier results

- Tier 1 (textual cherry-pick): **pass** — `git cherry-pick --no-commit -x 9650c1a6f5` auto-merged all 3 source files with no conflict; all guards matched the `#if USE_ARROWFLIGHT` pre-image.
- Tier 2 (semantic patch-id): **pass** (with `byte_equivalent: false`) — decomposition shows the ONLY divergence from the source is the 2 intentional `configure_config.cmake` option lines (PC-1); no semantic C++ divergence.
- Tier 3 (build + test): **pass** — reconfigure + 3280-step rebuild, ninja exit 0; generated `config.h` 2×1 + `USE_ARROWFLIGHT=1`; smoke 1/1.

## Evidence

**Cherry-pick result (`tmp/patch-075/cherrypick.log`) — CLEAN, 3 files staged:**
```
Auto-merging src/Common/config.h.in
Auto-merging src/Storages/registerStorages.cpp
Auto-merging src/TableFunctions/registerTableFunctions.cpp
--- exit: 0 ---
Changes to be committed:
	modified:   src/Common/config.h.in
	modified:   src/Storages/registerStorages.cpp
	modified:   src/TableFunctions/registerTableFunctions.cpp
```

**Staged C++ diff (the 2 flags + 3 guard upgrades, incl. the quirk):**
```
+#cmakedefine01 REGISTER_ARROWFLIGHT_TABLE_ENGINE   (after …AZURE_BLOB_TABLE_ENGINE)
+#cmakedefine01 REGISTER_ARROWFLIGHT_FUNCTION        (after …AZURE_BLOB_FUNCTION)
-#if USE_ARROWFLIGHT
+#if USE_ARROWFLIGHT && REGISTER_ARROWFLIGHT_TABLE_ENGINE   (storage forward-decl)
-#if USE_ARROWFLIGHT
+#if USE_ARROWFLIGHT && REGISTER_ARROWFLIGHT_TABLE_ENGINE   (storage call)
-#if USE_ARROWFLIGHT
+#if USE_ARROWFLIGHT && REGISTER_ARROWFLIGHT_TABLE_ENGINE   (table-function guard — uses _TABLE_ENGINE, NOT _FUNCTION = quirk, verbatim)
```

**2 added option lines + placement confirmation** (`src/configure_config.cmake`, appended after `option(REGISTER_YTSAURUS_FUNCTION … ON)` at the END of the "table engines and table functions" block — BY CATEGORY, not near a dictionary source):
```
option(REGISTER_YTSAURUS_TABLE_ENGINE "Register YTsaurus table engine" ON)
option(REGISTER_YTSAURUS_FUNCTION "Register ytsaurus table function" ON)
+option(REGISTER_ARROWFLIGHT_TABLE_ENGINE "Register ArrowFlight table engine" ON)
+option(REGISTER_ARROWFLIGHT_FUNCTION "Register arrowFlight table function" ON)

set(SOURCE_DIR ${PROJECT_SOURCE_DIR})
```

**Flag↔option coverage diff (`tmp/patch-075/flag-option-coverage.log`) — EMPTY:**
```
--- diff exit: 0 (0 = EMPTY/identical) ---
added config.h.in flags : REGISTER_ARROWFLIGHT_FUNCTION, REGISTER_ARROWFLIGHT_TABLE_ENGINE
added options           : REGISTER_ARROWFLIGHT_FUNCTION, REGISTER_ARROWFLIGHT_TABLE_ENGINE
```

**Decomposition (`tmp/patch-075/decomposition.log`) — only the 2 option lines diverge from source:**
```
8a9,10
> +option(REGISTER_ARROWFLIGHT_TABLE_ENGINE "Register ArrowFlight table engine" ON)
> +option(REGISTER_ARROWFLIGHT_FUNCTION "Register arrowFlight table function" ON)
```

**Build (`tmp/patch-075/build-postpatch.log`) — reconfigure ran + ninja exit 0:**
```
[0/2] Re-checking globbed directories...
[1/2] Re-running CMake...
-- Configuring done (8.9s)
-- Generating done (3.2s)
[3277/3280] Linking CXX executable programs/clickhouse
ninja exit: 0
```

**Generated config.h (`tmp/patch-075/configh-generated.log`) — both flags = 1, USE_ARROWFLIGHT = 1** (`build/includes/configs/config.h`):
```
REGISTER_ARROWFLIGHT_TABLE_ENGINE = 1
REGISTER_ARROWFLIGHT_FUNCTION = 1
USE_ARROWFLIGHT = 1
```

**ArrowFlight smoke (USE_ARROWFLIGHT = 1 ⇒ expect 1/1):**
```
smoke-engine.log: SELECT count() FROM system.table_engines   WHERE name='ArrowFlight'  -> 1
smoke-tf.log    : SELECT count() FROM system.table_functions WHERE name='arrowFlight'  -> 1
(registered names confirmed: engine 'ArrowFlight', function 'arrowFlight' (+ lowercase 'arrowflight' alias))
```
Interpretation: this build has `USE_ARROWFLIGHT = 1`, so the expected smoke is engine=1 / TF=1 — **observed 1/1**, confirming both surfaces still register on a default build (both REGISTER flags default ON). 5a (config.h 2×1) is the option-pairing proof; the 1/1 smoke is the corroborating runtime evidence.

**Final staged state (`tmp/patch-075/final-status.log`) — exactly 5 files, no `tests/queries/`:**
```
docs/aiven/patches/075-register-arrowflight-flags.md
src/Common/config.h.in
src/Storages/registerStorages.cpp
src/TableFunctions/registerTableFunctions.cpp
src/configure_config.cmake
 5 files changed, 263 insertions(+), 3 deletions(-)
```

## What I did

- **Preflight:** HEAD = `v26.3.10.62-lts-aiven-dev`, tip = `533aa0fa40c patch-port(071)`, nothing pre-staged (all other working-tree changes are pre-existing carry-overs), `9650c1a6f5` is a commit, author = Aliaksei Khatskevich, 2026-04-23.
- **Drift (Step 1):** ArrowFlight flags ABSENT on HEAD; all 3 guards exactly `#if USE_ARROWFLIGHT` (source pre-image); 071 YTsaurus anchors present in config.h.in; no ArrowFlight option yet → conclusion `still-needed-but-rewrite`.
- **Cherry-pick (Step 2):** `--no-commit -x` applied CLEAN; staged C++ matches source verbatim (incl. the `_TABLE_ENGINE` quirk on the TF guard).
- **Option block (Step 2.5, PC-1):** appended the 2 ArrowFlight options at the END of the engines/functions block (after `REGISTER_YTSAURUS_FUNCTION`), by category; coverage diff EMPTY and == {`REGISTER_ARROWFLIGHT_TABLE_ENGINE`, `REGISTER_ARROWFLIGHT_FUNCTION`}; staged.
- **Build (Step 4):** reconfigure + 3280-step broad rebuild (config.h widely included), ninja exit 0.
- **Verify (Step 5):** generated `config.h` shows both `REGISTER_ARROWFLIGHT_*` = 1 and `USE_ARROWFLIGHT = 1`; started the smoke server (granted scratch `users.xml`), smoke = 1/1, stopped the server cleanly.
- **Dossier (Step 6):** wrote `docs/aiven/patches/075-register-arrowflight-flags.md` per template; §6 documents the `REGISTER_ARROWFLIGHT_FUNCTION` defined-but-unused quirk (ported verbatim, TF guard kept on `_TABLE_ENGINE`, spec-contract rationale) and cross-references 071.
- One transient hook block on a `git add` ("returned no output") — retried once per the environment note, succeeded immediately.

**Quirk confirmation:** `REGISTER_ARROWFLIGHT_FUNCTION` was ported VERBATIM — defined in `config.h.in` and given its `option(… ON)`, but the table-function guard remains `#if USE_ARROWFLIGHT && REGISTER_ARROWFLIGHT_TABLE_ENGINE` (NOT rewired to `_FUNCTION`). The flag ships defined-but-referenced-by-no-guard, exactly as the 25.8 source has it. Documented in dossier §6.

## Proposed next step

Ready for human commit. Suggested:
```
git commit -F tmp/patch-075/commit-message.txt
```
(using the `proposed_commit.commit_message` above — the body includes the `Original author:` line per the source-author-preservation policy; do NOT use `git commit --author=` or `-c CHERRY_PICK_HEAD`). The cherry-pick left `CHERRY_PICK_HEAD` set, which is expected for the human's commit; the staged set is exactly the 5 files listed.

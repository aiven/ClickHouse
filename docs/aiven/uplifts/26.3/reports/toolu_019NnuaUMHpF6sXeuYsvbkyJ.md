<!-- subagentStop archive | source=transcript | status=completed | type=general-purpose | ts=2026-06-11T19:04:31Z -->

All steps complete. Here is the halt-and-escalate report.

---
outcome: success
patch_slug: register-flags-newer-engines
source_sha: f987c06d9e928469d9dd4bcd282bc22339cd76c8
proposed_commit:
  staged_files:
    - src/Common/config.h.in
    - src/Storages/registerStorages.cpp
    - src/TableFunctions/registerTableFunctions.cpp
    - src/configure_config.cmake
    - docs/aiven/patches/052-register-flags-newer-engines.md
  commit_message: |
    patch-port(052): additional compiler flags for newer engines/function

    Original author: Tilman Moeller <tilman.moeller@aiven.io>, 2026-01-08.

    (cherry picked from commit f987c06d9e928469d9dd4bcd282bc22339cd76c8)

    26.3 adaptation: added the matching option(REGISTER_<NAME> ... ON) lines to
    src/configure_config.cmake (absent from the 25.8 source). Without them
    #cmakedefine01 resolves the undefined symbols to 0 and silently disables the
    engines/functions. All flags default ON, mirroring patch-port(051)/(045) — no
    default-behavior change. byte_equivalent: false.
  byte_equivalent: false
tests:
  added: no_justified
  kind: upstream-existing
  paths: []
  upstream_reference:
    - tests/queries/0_stateless/03222_create_timeseries_table.sql
  pre_patch_fail_verified: false
  post_patch_pass_verified: true
  justification: |
    Build-system-only patch; all 5 flags default ON via option(... ON), so the
    default build's registration set is identical to upstream — there is no
    runtime-observable differential on a default build (pre/post both register
    everything → no evidence-of-causation pair), mirroring committed siblings
    051/045. Correctness is proven by build-system verification: flag↔option
    coverage diff empty (5 flags == 5 options); generated config.h defines all 5
    new flags = 1 (no silent disable); runtime smoke shows every gated surface
    resolves (TimeSeries engine = 1; functions timeSeriesData/s3/iceberg = 3).
    The existing upstream test 03222_create_timeseries_table.sql exercises the
    REGISTER_TIMESERIES_TABLE_ENGINE-gated `ENGINE = TimeSeries` surface and was
    run against the staged binary: [ OK ].
escalation_reason: none
---

## Tier results

- Tier 1 (textual cherry-pick): pass — 3 source files; `registerTableFunctions.cpp` clean, two trivial context conflicts hand-resolved (`config.h.in` stale `SOURCE_DIR` trailer; new adjacent `registerStorageAlias` in `registerStorages.cpp`).
- Tier 2 (semantic patch-id): pass — patch-ids differ (expected, PC-3), decomposition shows the ONLY +/- divergence is the 5 added `option(... ON)` lines; no semantic C++ difference.
- Tier 3 (build + test): pass — reconfigure confirmed, ninja exit 0 (3281 steps), generated `config.h` all 5 = 1, smoke + upstream test pass.

## Evidence

Cherry-pick (`cherrypick.log`) — conflicts arose, both trivial-shift:
```
Auto-merging src/Common/config.h.in
CONFLICT (content): Merge conflict in src/Common/config.h.in
Auto-merging src/Storages/registerStorages.cpp
CONFLICT (content): Merge conflict in src/Storages/registerStorages.cpp
Auto-merging src/TableFunctions/registerTableFunctions.cpp   (clean, staged)
```
Resolutions: `config.h.in` keeps the two new function flags, drops the upstream-removed `#cmakedefine SOURCE_DIR` trailer; `registerStorages.cpp` wraps only `registerStorageTimeSeries` in `#if REGISTER_TIMESERIES_TABLE_ENGINE … #endif` and keeps `registerStorageAlias(factory)` (a new upstream-26.3 call) **unconditional**.

Flag↔option coverage (`flag-option-coverage.log`) — empty diff (symmetric):
```
=== flag<->option coverage diff (expect EMPTY) ===   exit: 0
newly-added flags == newly-added options:
  REGISTER_DATALAKE_FUNCTION / REGISTER_OBJECT_STORAGE_FUNCTION /
  REGISTER_OBJECT_STORAGE_TABLE_ENGINE / REGISTER_TIMESERIES_FUNCTION /
  REGISTER_TIMESERIES_TABLE_ENGINE
```

Decomposition (`decomposition.log`, canonical schema form) — only the cmake block diverges:
```
15a16,20
> +option(REGISTER_TIMESERIES_TABLE_ENGINE "Register TimeSeries table engine" ON)
> +option(REGISTER_OBJECT_STORAGE_TABLE_ENGINE "Register generic ObjectStorage table engine" ON)
> +option(REGISTER_TIMESERIES_FUNCTION "Register timeSeries table function" ON)
> +option(REGISTER_OBJECT_STORAGE_FUNCTION "Register object-storage table functions (s3/azureBlobStorage/hdfs/...)" ON)
> +option(REGISTER_DATALAKE_FUNCTION "Register data-lake table functions (iceberg/deltaLake/hudi)" ON)
```

Build (`build-postpatch.log` tail) — reconfigure ran, ninja exit 0:
```
[0/2] Re-checking globbed directories...
[1/2] Re-running CMake...
-- Configuring done (7.4s)
[3278/3281] Linking CXX executable programs/clickhouse
ninja exit: 0
```
Wall: 18:54:33Z → 18:59:02Z (~4.5 min, warm-cache). 0 `FAILED:`/`error:`.

Generated config.h (`configh-generated.log`) — all 5 = 1 (no silent disable):
```
REGISTER_TIMESERIES_TABLE_ENGINE = 1
REGISTER_OBJECT_STORAGE_TABLE_ENGINE = 1
REGISTER_TIMESERIES_FUNCTION = 1
REGISTER_OBJECT_STORAGE_FUNCTION = 1
REGISTER_DATALAKE_FUNCTION = 1
```

Smoke (`smoke-engines.log` / `smoke-functions.log`, `smoke-note.txt`):
```
engines  count(name IN ('TimeSeries'))                       = 1
functions count(name IN ('timeSeriesData','s3','iceberg'))   = 3
```
Per-block presence: TimeSeries → `timeSeriesData`,`timeSeriesSelector`,`prometheusQuery`; ObjectStorage → `s3`,`azureBlobStorage`,`hdfs`; DataLake → `iceberg`,`deltaLake`,`hudi`. **Correction noted:** the dispatch's draft smoke used `timeSeries`, which is not a registered TF name (the engine name `TimeSeries` is correct); `registerTableFunctionTimeSeries` registers `timeSeriesData/timeSeriesTags/timeSeriesMetrics/timeSeriesSelector/prometheusQuery/prometheusQueryRange`, so I used `timeSeriesData` as the representative.

Upstream test (`upstream-test.log`) — run against staged binary:
```
03222_create_timeseries_table:   [ OK ] 0.13 sec.
1 tests passed. 0 tests skipped.
```

Final staged set (`final-status.log`) — exactly 5, no `tests/queries/`:
```
docs/aiven/patches/052-register-flags-newer-engines.md
src/Common/config.h.in
src/Storages/registerStorages.cpp
src/TableFunctions/registerTableFunctions.cpp
src/configure_config.cmake
 5 files changed, 315 insertions(+)   (source: 20 insertions, dossier: 295)
```

## What I did

- Preflight: confirmed HEAD `v26.3.10.62-lts-aiven-dev`, `f987c06d9e` is a commit, author Tilman Moeller 2026-01-08. Working tree carried more parent bookkeeping than the prompt's three named files (extra `docs/aiven/*` edits, `contrib/*` submodule pointer drift, untracked reports/dossiers/`access/`) — none of it overlaps my 5 target files and nothing was pre-staged, so I proceeded and left it untouched.
- Drift (Step 1): 5 flags ABSENT from HEAD `config.h.in`; base tag has 0 (`base-has-flags.log`); all 7 wrapped call-sites present. Conclusion `still-needed-but-rewrite`.
- Cherry-pick `--no-commit -x`; hand-resolved the two trivial conflicts; staged the 3 source files.
- Step 2.5 (PC-1): added the 5 `option(REGISTER_<NAME> "..." ON)` lines to `src/configure_config.cmake` after `option(REGISTER_URL_FUNCTION ...)`; coverage diff empty; staged.
- Patch-id/decomposition: confirmed only-cmake divergence (byte_equivalent false).
- Build with clang-21 (reconfigure forced by the cmake edit; ninja exit 0); verified config.h all-=1; ran server smoke + the TimeSeries upstream test; stopped server cleanly.
- Wrote dossier `docs/aiven/patches/052-register-flags-newer-engines.md` (all sections filled) and staged it; wrote the proposed commit message to `tmp/patch-052/commit-message.txt`.
- Did NOT commit, branch, push, or touch any never-touch path.

## Proposed next step

Ready for human commit. Suggested:
```
git commit -F tmp/patch-052/commit-message.txt
```
The message body includes the required `Original author: Tilman Moeller <tilman.moeller@aiven.io>, 2026-01-08.` line; do not use `git commit --author=` or `-c CHERRY_PICK_HEAD` (the local human is the author of record). After committing, update the dossier §0 row from "(staged)" to the new commit handle and append the patch 052 entry to the uplift log.

One reviewer note for sign-off: the highest-risk trap (a `#cmakedefine01` without a matching `option(... ON)`) is closed and double-checked (coverage diff empty + config.h all-=1). The only intentional source divergence from the 25.8 commit is the 5-line cmake block.

Today you learned: a `#cmakedefine01 FOO` of an *undefined* CMake symbol expands to `#define FOO 0` — so an Aiven `REGISTER_*` guard added without the companion `option(FOO ... ON)` silently compiles the engine/function out of a default build, with no error.
Rule of thumb: every `#cmakedefine01 REGISTER_<NAME>` MUST be paired with an `option(REGISTER_<NAME> ... ON)`; assert it mechanically (coverage diff) and confirm in the generated `config.h`, never by eye.
Next rabbit hole: the next family links (070/071 YTsaurus, 075 ArrowFlight) — the RPM spec can't faithfully reproduce 25.8's selection until the whole `REGISTER_*` family lands, so a CI assertion that no `REGISTER_*` name appears in CMake's "Manually-specified variables were not used" warning would catch a half-ported family.

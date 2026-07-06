# Patch 052 — register-flags-newer-engines

## 0. Lineage

| LTS uplift | First-carry handle on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `f987c06d9e928469d9dd4bcd282bc22339cd76c8` | Tilman Moeller (author) / Joe Lynch (committer), 2026-01-08 | original carry (the version we port FROM) |
| 26.3-aiven | `patch-port(052)` (staged) | T3 patch worker (T3.23), 2026-06-11 | conflict-resolved + hardened-rewrite — see §2 and §6 |

The current uplift's row uses the stable handle `patch-port(052)` (NOT a SHA);
it stays "(staged)" until the human commits. Find it later with
`git log --grep '^patch-port(052)'`.

## 1. Purpose

Extend the Aiven `REGISTER_*` compile-time engine/function family (foundation
laid by patch 051, sibling 045) with five additional toggles for the newer
TimeSeries / generic ObjectStorage / DataLake surfaces:
`REGISTER_TIMESERIES_TABLE_ENGINE`, `REGISTER_OBJECT_STORAGE_TABLE_ENGINE`,
`REGISTER_TIMESERIES_FUNCTION`, `REGISTER_OBJECT_STORAGE_FUNCTION`,
`REGISTER_DATALAKE_FUNCTION`. Each adds a `#cmakedefine01` flag plus an `#if`
guard around the corresponding registration call, so an Aiven managed build can
exclude these engines/functions at compile time via `-DREGISTER_<NAME>=0`. The
motivation is durable and identical to 051/045: Aiven's managed fleet wants to
disable risky/unneeded engines/functions for security/compliance and to produce
smaller binaries with a reduced attack surface. Defaults are **ON**, so a no-flag
(dev/CI/OSS) build registers exactly what upstream does — the toggles are
opt-*out*.

Source SHA on `v25.8.18.1-lts-aiven`: `f987c06d9e928469d9dd4bcd282bc22339cd76c8`
(from `docs/aiven/uplifts/26.3/inventory.md` row 052).
Original author: `tilman.moeller@aiven.io` (committer `joelynch112@gmail.com`).
Original purpose (verbatim from the source commit body):

> additional compiler flags for newer engines/function
>
> REGISTER_TIMESERIES_TABLE_ENGINE, REGISTER_TIMESERIES_FUNCTION, REGISTER_OBJECT_STORAGE_TABLE_ENGINE, REGISTER_OBJECT_STORAGE_FUNCTION, and REGISTER_DATALAKE_FUNCTION

### Ground truth — where the defaults live (RPM build harness)

As with 051/045, the production default selection is **not** in the ClickHouse
repo. Aiven's RPM build harness passes an explicit `-DREGISTER_<NAME>=0/1` for
each flag. The in-tree `option(... ON)` default (added here, §2/§6) only governs
builds (dev/CI) that pass no flags; it is the safety net that keeps a no-flag
build identical to upstream.

## 2. Upstream-drift findings

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`, not just textually applicable.

### Commands run

```bash
# The REGISTER_* mechanism is Aiven's own; the new flags must NOT exist on HEAD.
grep -nE "REGISTER_(TIMESERIES|OBJECT_STORAGE)_(TABLE_ENGINE|FUNCTION)|REGISTER_DATALAKE_FUNCTION" \
  src/Common/config.h.in            # -> ABSENT (good)

# The whole mechanism must be absent from the base (upstream) tag.
git show v26.3.10.62-lts:src/Common/config.h.in \
  | grep -cE "REGISTER_(TIMESERIES|OBJECT_STORAGE)"   # -> 0

# All wrapped registration entry points still present on HEAD?
for id in registerStorageTimeSeries registerStorageObjectStorage \
          registerTableFunctionTimeSeries registerTableFunctionObjectStorage \
          registerTableFunctionObjectStorageCluster registerDataLakeTableFunctions \
          registerDataLakeClusterTableFunctions; do
  printf '%s: ' "$id"
  git grep -c "$id" -- src/Storages/registerStorages.cpp \
                        src/TableFunctions/registerTableFunctions.cpp \
    | awk -F: '{s+=$NF} END{print s+0}'
done    # -> every id >= 1 (call-site present, currently unguarded on HEAD)
```

### Findings

- 26.3 HEAD has **no** `REGISTER_(TIMESERIES|OBJECT_STORAGE)_*` /
  `REGISTER_DATALAKE_FUNCTION` flag (`tmp/patch-052/source.diff` confirms the
  source adds them; HEAD grep returns ABSENT). The base-flag check returns `0`
  (`tmp/patch-052/base-has-flags.log`). The `REGISTER_*` mechanism is **Aiven's
  own**, chaining onto the committed siblings 045 (`0d6ef43e590`) and 051
  (`46880f95584`); it is absent from `v26.3.10.62-lts`. The patch is **still
  needed**; it is NOT obsoleted by upstream.
- All seven wrapped registration entry points still exist on HEAD
  (`tmp/patch-052/identifier-grep.log`). No upstream rename/removal of the gated
  call-sites.
- `src/Common/config.h.in` drifted at the trailer: the 25.8 source carried the
  `.incbin`/`#cmakedefine SOURCE_DIR "@SOURCE_DIR@"` block as trailing context,
  but upstream 26.3 **removed** that block (it now lives as `set(SOURCE_DIR ...)`
  in `configure_config.cmake`). The cherry-pick conflicted only on this trailer;
  resolved by hand — the two new function flags
  (`REGISTER_OBJECT_STORAGE_FUNCTION`, `REGISTER_DATALAKE_FUNCTION`) are kept and
  the stale `SOURCE_DIR` context is **not** reintroduced (same resolution 051
  applied).
- `src/Storages/registerStorages.cpp` drifted: upstream 26.3 added a new
  unconditional `registerStorageAlias(factory);` call **immediately after** the
  guarded `registerStorageTimeSeries(factory);`. The cherry-pick conflicted on
  where `#endif` lands; resolved by hand so that only
  `registerStorageTimeSeries` is wrapped in `#if REGISTER_TIMESERIES_TABLE_ENGINE
  … #endif` and `registerStorageAlias` stays **unconditional** (it is not part of
  this patch's gating). The `registerStorageObjectStorage` hunk applied cleanly.
- `src/TableFunctions/registerTableFunctions.cpp` applied cleanly (all three
  `#if` blocks: `REGISTER_TIMESERIES_FUNCTION`, `REGISTER_OBJECT_STORAGE_FUNCTION`,
  `REGISTER_DATALAKE_FUNCTION`).
- Conclusion: **`still-needed-but-rewrite`** — semantics intact, but the port
  intentionally diverges from the source by ADDING the matching
  `option(REGISTER_<NAME> "..." ON)` block to `src/configure_config.cmake` (the
  source omitted it), and the two trivial context conflicts above were
  hand-resolved.

### Required deviations from the literal 25.8 patch (intentional hardening)

1. **`option(REGISTER_<NAME> "..." ON)` defaults in `src/configure_config.cmake`.**
   The 25.8 source commit added the five `#cmakedefine01 REGISTER_<NAME>` lines
   and the `#if` guards but **did not** add matching `option()` lines — on 25.8
   the private RPM build always passed explicit `-D` flags. On 26.3, following
   the committed 045/051 pattern: `#cmakedefine01` of an **undefined** CMake
   symbol resolves to `#define VAR 0`, so without an `option(... ON)` line every
   guarded engine/function would be **silently disabled** in any no-flag
   (dev/CI/OSS) build — a release-breaking regression. We add the full five-line
   `option(... ON)` block. `option()` (not `set(... 1)`) is parity-safe: a
   `-DREGISTER_<NAME>=0/1` already in the CMake cache **wins** over the option
   default, so the RPM harness's explicit `-D` flags are unaffected, while a
   no-flag build keeps everything ON (= upstream behavior). This is the
   load-bearing divergence and the reason `byte_equivalent: false`.

## 3. C++ review

Apply `docs/aiven/skills/cpp-review-checklist.md`. The change is build-system +
preprocessor-guard only; no runtime data path is altered when defaults are ON.

- 1 Lifetime + ownership: `n/a` — no object lifetime touched; guards only elide
  `registerStorageX` / `registerTableFunctionX` calls at compile time.
- 2 Exception safety: `n/a` — no new throwing code; eliding a `registerX` call
  cannot break exception safety of the surrounding scope.
- 3 Thread-safety + concurrency: `n/a` — registration runs once at startup; no
  concurrency semantics changed.
- 4 Performance + memory: ✓ — strictly reductive (a disabled engine/function is
  not registered → smaller catalog/binary). Default build is behavior-identical
  to upstream.
- 5 Settings as public API: ✓ — no server *setting* added; these are
  compile-time CMake options, not user-visible runtime settings. Default-ON ⇒
  no public-API surface change (de-escalated clause (v): not a default-behavior
  change when ported with `option(... ON)`).
- 6 Error handling: ✓ — when an engine/function is compiled out, using it fails
  via the existing upstream "unknown table engine" / "unknown function" path;
  no new error code needed.
- 7 Upstream / vendored code: ✓ — all edited files are ClickHouse-owned
  (`src/Common/config.h.in`, `src/configure_config.cmake`,
  `src/Storages/registerStorages.cpp`,
  `src/TableFunctions/registerTableFunctions.cpp`); none under `contrib/**`. The
  `#cmakedefine01` → 0 trap is closed by the `option(... ON)` block (§2
  deviation 1; verified by the generated `config.h` all-=1 check, §4).
- 8 Behavior under settings: ✓ — with all toggles default-ON the registration
  set is unchanged (verified: generated `config.h` all = 1; engine smoke 1/1,
  function smoke 3/3); with a toggle `=0` the corresponding engine/function is
  absent (exercised only by the Aiven managed build, out of repo scope). No new
  wrap beyond the source.

## 4. Test design

(b) **Documented justification — no new test, with reference to an existing
upstream test (`tests.added: no_justified`), mirroring committed siblings
051/045.**

This patch is **build-system-only** and adds **no runtime behavior on a default
build**: all five flags default ON via `option(... ON)`, so the registration set
is identical to upstream. The behavior the patch *introduces* — eliding an
engine/function's registration — is a **compile-time** property of the produced
binary, gated by a CMake option, not a runtime setting. A stateless `.sql`/`.sh`
test cannot observe it on the default build (everything is present), and
producing a "disabled" binary requires a second CMake configure + rebuild that
the stateless runner cannot express. A worktree-flip evidence pair is also
unreachable: pre- and post-patch both register everything on a default build
(pass-pass), so there is no differential observable. Per AGENTS §7(b) the
correct posture is `no_justified` with build-system verification + an existing
upstream test that exercises a gated surface.

### Build-system verification (the correctness evidence)

- **Generated `config.h` all = 1** (`tmp/patch-052/configh-generated.log`):
  the post-patch `build/includes/configs/config.h` defines **all 5** new flags to
  `1` on a no-flag build — proving the `option(... ON)` block works and no
  engine/function is silently disabled (the failure mode this dispatch guards
  against). Flag↔option coverage diff (`tmp/patch-052/flag-option-coverage.log`)
  is symmetric: 5 added flags == 5 added options.
- **Runtime smoke** (`tmp/patch-052/smoke-engines.log`,
  `tmp/patch-052/smoke-functions.log`): on the post-patch server,
  `SELECT count() FROM system.table_engines WHERE name IN ('TimeSeries')` = **1**
  and `SELECT count() FROM system.table_functions WHERE name IN
  ('timeSeriesData','s3','iceberg')` = **3** — one representative resolving per
  gated `#if` block.
  - Note: the dispatch's draft smoke used the literal name `timeSeries`, which is
    **not** a registered table function. `registerTableFunctionTimeSeries`
    registers `timeSeriesData`, `timeSeriesTags`, `timeSeriesMetrics`,
    `timeSeriesSelector`, `prometheusQuery`, `prometheusQueryRange`. Corrected to
    `timeSeriesData` as the `REGISTER_TIMESERIES_FUNCTION` representative (see
    `tmp/patch-052/smoke-note.txt`). All three gated function blocks resolve:
    TimeSeries (`timeSeriesData`/`timeSeriesSelector`/`prometheusQuery`),
    ObjectStorage (`s3`/`azureBlobStorage`/`hdfs`), DataLake
    (`iceberg`/`deltaLake`/`hudi`).

### Existing upstream test exercising a gated surface (run post-patch, PASS)

Run against the staged/post-patch binary (`tmp/patch-052/upstream-test.log`):

- `tests/queries/0_stateless/03222_create_timeseries_table.sql` — creates tables
  with `ENGINE = TimeSeries` (gated by `REGISTER_TIMESERIES_TABLE_ENGINE`).
  Result: `[ OK ] 0.13 sec`.

- Why this distinguishes the Aiven change from upstream: upstream 26.3 has **no**
  `REGISTER_*` macro at all (§2), so the `#if REGISTER_<NAME>` guards and their
  observable effect (registration present at `=1`) exist *only* because of this
  patch family. With defaults ON the existing suite's behavior is unchanged,
  which is exactly the contract this patch must preserve.

## 5. Rollback considerations

- Revert safety: ✓ — pure compile-time change. Reverting removes the toggles,
  guards, and the `option()` block; no schema migration, no on-disk format
  change, no ZK state.
- State surviving restart: none.
- Disabling the new behavior without rebuilding: `n/a` — the toggles are
  compile-time. To re-enable an engine/function that was compiled out, rebuild
  with the toggle `=1` (or no flag, since the in-tree default is ON).

## 6. Per-uplift notes

### 25.8-aiven (historical)

Original carry. Author: Tilman Moeller; committer: Joe Lynch
(`f987c06d9e`). No 25.8 stateless test (build-system change; production selection
driven entirely by the RPM harness `-D` flags). The 25.8 commit did **not** add
`option(... ON)` defaults to `configure_config.cmake` — it relied on the private
build always passing explicit `-D` flags. That omission is corrected in the 26.3
carry (§2 deviation 1), consistent with siblings 051/045.

### 26.3-aiven (this uplift)

- Cherry-pick was: **conflict-resolved + hardened-rewrite**.
  - `src/TableFunctions/registerTableFunctions.cpp` applied cleanly (all three
    `#if` blocks).
  - `src/Common/config.h.in`: first hunk auto-merged (TIMESERIES/OBJECT_STORAGE
    table-engine flags + TIMESERIES function flag). The trailing hunk conflicted
    because upstream removed the `.incbin`/`SOURCE_DIR` trailer that the 25.8 diff
    carried as context; hand-resolved — the two new function flags
    (`REGISTER_OBJECT_STORAGE_FUNCTION`, `REGISTER_DATALAKE_FUNCTION`) are kept,
    `SOURCE_DIR` is **not** reintroduced.
  - `src/Storages/registerStorages.cpp`: `registerStorageObjectStorage` hunk
    applied cleanly; the `registerStorageTimeSeries` hunk conflicted because
    upstream inserted a new unconditional `registerStorageAlias(factory);`
    adjacent to it. Hand-resolved so only `registerStorageTimeSeries` is wrapped
    and `registerStorageAlias` stays unconditional.
- 26.3 adaptation (§2 deviation 1): added the matching five-line
  `option(REGISTER_<NAME> "..." ON)` block to `src/configure_config.cmake`
  (absent from the 25.8 source). `byte_equivalent: false`. Tier-2 patch-id
  decomposition (`tmp/patch-052/decomposition.log`) shows the **only** +/-
  divergence is that `configure_config.cmake` block — no semantic C++ difference
  (the `#if`/`#endif` lines added to the two `.cpp` files and the five
  `#cmakedefine01` lines are byte-identical to the source's added lines). Flag↔
  option coverage diff (`tmp/patch-052/flag-option-coverage.log`) is symmetric:
  5 added flags == 5 added options. Cross-reference siblings 051
  (`46880f95584`) and 045 (`0d6ef43e590`).
- Upstream-drift conclusion: `still-needed-but-rewrite` (§2).
- Test: no new stateless test — `no_justified`, build-system-only. Build-system
  verification (generated `config.h` all = 1; engine smoke 1/1; function smoke
  3/3) + one existing upstream test run post-patch and PASS (§4):
  `03222_create_timeseries_table`.
- Time-to-port: build was **warm-cache** — editing `configure_config.cmake`
  forced a CMake reconfigure (confirmed `Re-running CMake...`), then a 3281-step
  incremental rebuild (sccache-accelerated). Wall: ~4.5 min
  (`tmp/patch-052/build-start.txt` 18:54:33Z → `build-end.txt` 18:59:02Z, ninja
  exit 0).
- Anything surprising: (1) the source commit's omission of the `option()` block
  is the single highest-risk trap — without it, `#cmakedefine01` of the five
  undefined symbols would silently disable TimeSeries / ObjectStorage / DataLake
  engines+functions in any default build. Step 2.5's coverage diff and Step 5a's
  `config.h` all-=1 check make that impossible to miss. (2) The dispatch's draft
  smoke name `timeSeries` is not a real table-function name; corrected to
  `timeSeriesData` (the engine name `TimeSeries` IS correct).

### Consumer contract (for the `REGISTER_*` family)

The Aiven RPM spec's `%build` passes `-DREGISTER_<NAME>=0/1` for these flags
(`-DREGISTER_TIMESERIES_TABLE_ENGINE`, `-DREGISTER_OBJECT_STORAGE_TABLE_ENGINE`,
`-DREGISTER_TIMESERIES_FUNCTION`, `-DREGISTER_OBJECT_STORAGE_FUNCTION`,
`-DREGISTER_DATALAKE_FUNCTION`). The `option(... ON)` form (not `set(... 1)`) is
load-bearing: a `-D` cache entry is set before `configure_config.cmake` runs, and
`option()` never overwrites an existing cache entry, so the spec's value wins.
**Family caveat (from 051):** a `-DREGISTER_<NAME>=…` whose flag is not yet
defined (because 070/071/075 are not yet ported) is silently ignored by CMake
(only a `Manually-specified variables were not used by the project` warning) — so
the 26.3 spec can only faithfully reproduce 25.8's selection once the whole
family lands. Recommendation: the spec/CI should assert that no `REGISTER_*` name
appears in that CMake warning.

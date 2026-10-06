# Plan — backport upstream PromQL + `TimeSeries` support onto 26.3-aiven

| Field | Value |
|---|---|
| Status | **Port implemented: compiles, links, PromQL evaluates correctly.** See the port record below. Integration tests and the compliance gate remain. |
| Category | (E) upstream feature port — see `docs/aiven/runbooks/commit-hygiene.md` §1(E) |
| Dossier | `docs/aiven/patches/E-promql-timeseries-backport.md` |
| **Pinned upstream commit** | **`290adb52c4ee238c9f3f0b557d6f2ca9bb3b29ff`** (`upstream/master`, 2026-09-17 08:23:47 +0000) |
| Branch point from `master` | `aa5df024249641558bd2e553166246ab017c1a43` (2026-03-19 15:14:37 +0000) |
| Our base | `v26.3.32.14-lts-aiven-dev` @ `a5cc61775e7`, `VERSION_STRING 26.3.32.1` |
| Ledger | `2026-09-17-promql-timeseries-ledger.md` (reconstructed; **184** PRs to port) |

Supporting evidence, all generated 2026-09-17 and all in this directory:

| Document | What it settles |
|---|---|
| `…-ledger.md` | The 212-row PR table, group classification, incidental / net-cancelled analysis |
| `…-snapshot-manifest.md` | Exact ADDED / COMMON / REMOVED file sets, include-closure, build glue |
| `…-migration-safety.md` | Schema versioning, the six renames, the production-migration hazard |
| `…-verification-harness.md` | Test inventory, the compliance harness, the gate design |
| `…-spike-error-surface.md` | The Phase 0 compile-error surface, classified |
| **`…-port-record.md`** | **What was actually done: every 26.3 adaptation, and the verification results** |
| `…-stateless-failures.md` | Classification of the remaining stateless-test failures |
| `evidence-2026-09-17/` | PromQL capability (8 → 41 expressions) and end-to-end correctness evidence |

---

## 1. Goal

Bring the `TimeSeries` engine and the PromQL / Prometheus-HTTP layer up to upstream
`master` parity. Upstream closed its PromQL umbrella issue
([#57545](https://github.com/ClickHouse/ClickHouse/issues/57545)) on 2026-09-15. Our fork
branched on 2026-03-19, *before* aggregation and binary operators landed.

### The gap, measured on our own binary

Probed against `build/programs/clickhouse` (26.3.32.1) via
`prometheusQuery(currentDatabase(), 'ts', <expr>, now())`. Full log:
`evidence-2026-09-17/promql-capability.txt`. `Code: 48` is `NOT_IMPLEMENTED`.

**Works (8):** instant and range selectors with label matchers, `rate`, `irate`, `delta`,
`idelta`, `last_over_time`, unary minus.

**Throws `NOT_IMPLEMENTED` (28):** every aggregation (`sum` `avg` `count` `min` `max`
`stddev` `topk`, with and without `by`), every binary operator (`+` `/`), every comparison
(`>` `==`), every set operator (`and` `or` `unless`), `histogram_quantile`, all five plain
`*_over_time` aggregators, `increase`, `deriv`, `changes`, `resets`, `absent`, `clamp_min`,
`round`, `label_replace`, `label_join`.

This is the difference between a demo and a product surface: no Grafana dashboard is
serviceable without aggregation and binary operators. The port takes the implemented
PromQL function count from **40 to 59** — but the function count understates it, because
our `Converter.cpp` dispatches only 8 AST node types and has *no* aggregation-operator and
*no* binary-operator support at all; upstream adds ~30 files for those.

---

## 2. Established facts

Everything below was verified in-repo on 2026-09-17 against the pinned commit. **Several
of the originating brief's figures were wrong; the corrections are marked.** Re-verify
anything you intend to act on — `upstream/master` moves daily, and re-pinning invalidates
the file-set numbers.

### Scope of the delta

| Metric | Brief claimed | **Measured** | |
|---|---|---|---|
| PR merges touching the subsystem | 198 | **204** with real net change (212 candidates) | corrected |
| — incidental (repo-wide refactor grazing ≤2 files) | −18 | **−8** | corrected |
| — net-cancelled (landed then reverted) | −9 | **−12** (6 exact-inverse pairs) | corrected |
| **PRs to port** | **171** | **184** | corrected |
| Subsystem files / line delta | 201 files, +24,023 / −5,820 | **229 files, +32,505 / −5,956** | corrected |
| Upstream subsystem files | 205 | **261** (ours: 165) | corrected |
| — that do not exist here | 84 | **104** (84 source + 20 integration-test) | clarified |
| **Aiven-local divergence in the path set** | 3 files, 57 lines | **3 files, +70 / −6** | corrected |
| Shared headers we lack | 9 | **9 headers, but 17 files** (`.cpp` siblings + closure) | corrected |

`171` is not reachable by any of the three counting methods tried. The ledger documents
all three and explains why the brief's literal `git log --merges -- <paths>` under-counts:
default history simplification hides most upstream `Merge pull request` commits, surfacing
only 145.

Two counting subtleties worth carrying forward:

- **`#117693` survives at PIN and must be ported.** The trio `#117693 → #118118 → #118119`
  is land → revert → re-revert, so only *two* of the three net-cancel. Treating the whole
  trio as noise would silently drop a live change.
- There is a class the brief does not mention: **8 zero-net PRs** whose net diff against
  master contains no in-subsystem file (rebase / long-lived-branch artifacts). One of them,
  `#117168`, has a commit whose diff literally deletes the entire subsystem.

### Confirmed unchanged from the brief

- `/api/v1/labels`, `/api/v1/label/<name>/values` and `/api/v1/series` **do** already exist
  here (`src/Server/PrometheusRequestHandler.cpp:422-448`). Endpoints are not the gap.
- `BinaryOperator` and `AggregationOperator` parse but fall through `Converter.cpp`'s
  `default:` arm to `NOT_IMPLEMENTED` (`Converter.cpp:89`).
- Range functions are limited to the 5 in `applyFunctionOverRange.cpp`'s `impl_map`.
- **`#120336` (2026-09-16) reverted `#112842`** — merged 2026-09-15 23:15, reverted
  2026-09-16 08:35, under 10 hours later. `present_over_time`, `absent_over_time`,
  `quantile_over_time` and `predict_linear` are **unimplemented even at PIN**. Do not
  promise them. (Bare `absent` *is* implemented at PIN; `absent_over_time` is not.)
- The contrib grammar delta is **exactly** as claimed: 10 files, **+948 / −748**. Upstream
  checks the regenerated parser in, so no ANTLR run is needed.

### Things that do not exist in our tree at all

Worth stating explicitly, because the brief's path set implies otherwise:

- `src/Processors/QueryPlan/ReadFromTimeSeries.{cpp,h}` — absent.
- `src/Parsers/getTimeSeriesSettingVersion.{cpp,h}` — absent.
- `src/Storages/TimeSeries/TimeSeriesVersion.h` — absent. **We have no schema-versioning
  layer whatsoever**, and `TimeSeriesSettings.h` contains no `version` token. Our tree
  *is* upstream's "prealpha".

We *do* have `src/AggregateFunctions/TimeSeries/**` (12 files, 3,837 lines).

### Aiven-local divergence — the whole of it

Exactly **two** commits touch the subsystem, established by diffing against the
pure-upstream mirror `origin/v26.3.33.24-lts-upstream`:

| Commit | Subject | In-subsystem footprint |
|---|---|---|
| `776a227e3cc` | `patch-new(N03)`: Register TimeSeries external target tables as referential dependencies | test files only (+58) |
| `fc54a00f752` | `patch-new(N04)`: Do not dereference a missing TimeSeries metrics target on ATTACH | `StorageTimeSeries.cpp` +12/−6 |

Two consequences that change the Phase 5 plan:

- **N03's functional change lives in `src/Databases/DDLDependencyVisitor.cpp`, which is
  *outside* the snapshot path set** — the snapshot cannot overwrite it. And it is **fully
  subsumed** by upstream `#108388` (+`#115944`, which adds a `RecentSamples` kind we do not
  have). PIN's `DDLDependencyVisitor.cpp:133-149` is byte-identical in mechanism. **Drop
  N03**; do not resolve it forward. It would conflict textually and would not compile
  (`Kind::Data` / `Kind::Metrics` are gone at PIN). Drop its test too — PIN carries the
  same test as `04409_time_series_referential_dependencies`, which is a **number collision
  hazard**: ours is `04410`, so a naive port duplicates it.
- **N04 is inside the snapshot and will be overwritten.** It must be re-applied, and PIN
  restructured the area it patches (`getTargetTableID` / `tryGetTargetTableID` at
  `:339/:344` vs our single `getTargetTableId` at `:234`), so it needs re-derivation rather
  than a cherry-pick. There is also a separate Aiven integration test,
  `tests/integration/test_aiven_time_series_recovery/`, outside the brief's path set.

Neither N03 nor N04 has a dossier under `docs/aiven/patches/` (only `N01` and `N02` do).
That is a pre-existing documentation gap, noted here but not in scope.

---

## 3. Strategy — snapshot port

Confirmed correct, and Phase 0 strengthened the case: take upstream's subsystem wholesale
at the pinned commit, port the shared prerequisites, re-apply the one genuine Aiven patch.
The ledger is the *manifest of what the snapshot contains*, not a pick list. Rationale and
conditions are codified in `commit-hygiene.md` §1(E).

The decisive evidence is §5's compile spike: overlaying 220 files produced **zero missing
headers and zero missing error codes**. The include graph closes. We are not missing
files; we are missing symbols inside files that already exist — which is a bounded,
compiler-enumerable problem.

---

## 4. BLOCKING — two new decisions

The originating brief's two blocking decisions were resolved on 2026-09-17 (contrib:
scoped exception, granted in `AGENTS.md` §3; commit category: (E) with one dossier, added
to `commit-hygiene.md` §1(E)). Phase 0 surfaced two more. **Both need a human call before
Phase 2/3 code is written.**

### 4.1 🔴 A straight port silently splits every existing Aiven series

This is the most important finding of Phase 0 and it was **not** in the brief.

Upstream `#114300` changed how a series identifier is computed, and **there is no
backward-compatibility path for it**, while `MIN_WRITABLE = 0` means the server keeps
writing to old tables regardless. Three facts combine:

1. `normalizeTimeSeriesDefinitionImpl.cpp:1168-1187` **overwrites** the stored `id` DEFAULT
   expression on ATTACH. Upstream's own comment concedes it: *"Function getDefault has
   changed since the prealpha version, so it can generate different identifiers now."*
2. PIN computes `reinterpretAsUUID(sipHash128(tags))`
   (`TimeSeriesIDGenerator.cpp:80-89`); we compute
   `reinterpretAsUUID(sipHash128(metric_name, all_tags))`
   (`TimeSeriesDefinitionNormalizer.cpp:294-336`).
3. Pinning the old expression does not rescue it: at OURS `all_tags` **excludes**
   `__name__` (`PrometheusRemoteWriteProtocol.cpp:342-352`), while at PIN `all_tags` is an
   alias of `tags`, which **includes** `__name__` (`TimeSeriesSink.cpp:305-323`, `:506-510`).
   **No in-tree setting reproduces the old identifiers.**

Effect: after upgrade, a live series gets a second tags row under a new id; its history
stays under the old id. A PromQL query spanning the boundary sees two distinct series with
an identical labelset. Upstream's own migration test cannot catch this — it writes series
`foo` before the upgrade and a *different* series `bar` after.

Note also that the brief's ordering constraint is *incomplete*: **`#114300` and `#115441`
merged before `#111204`** (schema versioning), so upstream never wrote version-guarded
compatibility for them. `#115441`'s retrofit is complete; `#114300`'s is not.

**Options:** (a) write an Aiven-local version-0 compatibility shim that reproduces the old
`id` expression (needs a `tags`-without-`__name__` accessor that does not currently exist
at PIN — real work, and it diverges from upstream); (b) accept a one-time series split and
handle it operationally (dashboards spanning the upgrade window break); (c) offline
re-keying migration of existing data; (d) raise it upstream and wait for a version-guarded
fix. This is a product/operations decision, not an engineering one.

### 4.2 `ViewTarget::Kind` rename needs a compatibility decision

Upstream renamed `Kind::Data` → `Samples` and `Kind::Metrics` → `MetricFamilies` and added
`RecentSamples`. This touches **14 of the 49** failing spike TUs and, unlike the other
blockers, is *not* mechanical: 26.3 already ships `DATA` and `METRICS` DDL clauses, so the
rename carries serialization and back-compat consequences in `ASTViewTargets`,
`ASTCreateQuery` printing, `toString` / `parseFromString` and the `Keyword` table.

PIN's own answer is version-gated (`MIN_WITH_METRIC_FAMILIES_TARGET_NAME = 4`, with the
parser accepting both keywords), so the machinery exists — but adopting it means adopting
`TimeSeriesVersion` wholesale, which is the right call anyway and is why `#111204` must
land early. **Decision needed:** confirm we take upstream's version-gated dual-keyword
behavior verbatim rather than inventing an Aiven variant.

### 4.3 Still outstanding from the brief

**A real production `SHOW CREATE TABLE` is required** for the Layer 3 acceptance test
(§7). We can derive a plausible version-0 DDL from the code and existing tests, and have
(see `…-migration-safety.md`), but "plausible" is not good enough for the gate that decides
whether the port ships. Please supply the output of `SHOW CREATE TABLE` for a real
26.3.32-aiven production `TimeSeries` table.

---

## 5. Phase 0 results — the compile spike

Reproduce per `2026-09-17-promql-timeseries-port-record.md` §5.

**What was applied:** 220 files, **+25,015 / −5,832** staged — the 261-file subsystem
taken from PIN, the 17-file shared-prerequisite closure, and `git rm` of the 8 files that
exist here but not at PIN.

**Result:** `cmake -B build` reconfigured cleanly (the source glob picked up the new
files). `ninja -k 0 clickhouse` rebuilt **482 steps** and failed **49 targets**.

### The three empty categories — the strongest signal

| Category | Distinct errors |
|---|---|
| `missing-header` (`'X' file not found`) | **0** |
| `error-code-missing` (undeclared `ErrorCodes::X`) | **0** |
| Missing `Setting::` / `ServerSetting::` vocabulary | **0** |

Not one missing include in the whole log. The 9 headers the brief identified are all
genuinely needed and all genuinely sufficient — with the closure correction: they pull in
8 more files (`.cpp` siblings plus `src/DataTypes/IDataType_fwd.h`), for 17 total. The
closure is stable at depth 2, and **no new contrib submodule is needed**.

### What actually blocks the build

Ranked by blast radius; full classification in `…-spike-error-surface.md`.

| # | Blocker | TUs | Fix |
|---|---|---|---|
| 1 | `VectorWithMemoryTracking` alias ODR clash | 23 (17 sole) | ~4 lines: `IDataType_fwd.h` + `TimeSeriesTagsFunctionHelpers.h` → `std::vector`; make `IDataType.h` include the `_fwd` header |
| 2 | `ViewTarget::Kind` rename | 14 | **§4.2 decision**, then port `ASTViewTargets` |
| 3 | `IStorage::alter` 3→4 params; `checkTableSizeBelowDropLimit` | 12 | adapt 2 call sites to 26.3's 3-arg `alter` |
| 4 | `getInMemoryMetadataPtr(ContextPtr, bool)` — **`#102505`** | 5 | adapt 7 call sites `(ctx,false)` → `()` |
| 5 | `IAggregateFunction` `mergeImpl` NVI hook drift | 4 | one header fix clears ~19 cascades |
| 6 | `makeASTLambda` absent | 4 (3 sole) | port a ~10-line helper |
| 7 | `IColumn::convertToFullIfWrapped` absent | 3 (2 sole) | port into `IColumn.h` |
| 8 | virtuals-into-metadata — **`#102644`** | 5 | hard-required by exactly **one** file |
| 9 | `ColumnConstPtr` undefined | 2 | one-line alias |
| 10 | `TimeSeriesSettings.cpp` settings boilerplate | 1 | fork's `IMPLEMENT_SETTINGS_TRAITS` is the 2-arg form; ~15 lines |

### The brief's six predicted breakage classes: 3 confirmed, 3 absent

| Prediction | Verdict |
|---|---|
| `getInMemoryMetadataPtr` context-awareness (`#102505`) | **CONFIRMED** — but 5/49 TUs, all mechanical |
| virtuals into in-memory metadata (`#102644`) | **CONFIRMED** — 5 TUs, hard-required by one file |
| Aggregate-function interface | **CONFIRMED, and worse than expected** — not docs, the virtual dispatch contract: overlay implements `mergeImpl ... override` against a pure-virtual `merge`, giving "abstract class marked `final`" and 19 cascades |
| `FunctionDocumentation` / embedded-docs fields | **NOT PRESENT** — our `FunctionDocumentation.h` is already current |
| `ASTPtr` `shared_ptr`→`intrusive_ptr`, `make_shared`→`make_intrusive` | **NOT PRESENT** — `IAST_fwd.h:14` already `boost::intrusive_ptr` |
| `IDatabase::alterTable` arity | **NOT PRESENT** (a *different* arity drift does exist: `IStorage::alter` 3→4, `executeDropQuery` 7→8) |

### Two findings that revise the plan

- **Do not front-load `#102505` and `#102644`.** The brief's Phase 1 puts these two
  highest-risk repo-wide PRs first. They account for ~10 of 49 failing TUs and ~11% of
  distinct errors. `#102505` can be avoided outright with 7 call-site edits; `#102644` is
  hard-required by exactly one file. Front-load the ODR fix and the `ViewTarget` decision;
  schedule `#102644` last, when its blast radius is the only thing left to absorb.
- **`src/Core/ConstantValue.h` is a pre-existing fork defect** — a dead header that has
  never been compiled here. The overlay is the first thing to include it.

### Corrections to the runbooks

- `build-and-test.md` §1 pins the toolchain at `/opt/llvm-21`. On this VM the gate
  (`cmake/tools.cmake`: `CLANG_MINIMUM_VERSION 21`) is satisfied by Fedora 43's packaged
  `clang 21.1.8` at `/usr/bin/clang`. `/opt/llvm-21` is not required — only clang ≥ 21 is.
- `build-and-test.md` §7 documents this build dir as having **no** header-dependency
  records (`#deps 0`), making incrementals untrustworthy. **Not true of a freshly
  configured `build/` here:** `ninja -t deps` reports `#deps 2186` for
  `StorageTimeSeries.cpp.o`. The 482-step closure is therefore honest, and the
  `find … | xargs -0 touch` workaround was not needed. The `#deps 0` state is a property
  of a *corrupted* build dir, not of the configure.

---

## 6. Revised work plan

Phase gating is unchanged in spirit: each phase builds and tests green before the next.
The ordering constraint from §2 (**`#111204` schema versioning before `#119225` /
`#119227` / `#119613`**) holds — upstream's own merge order satisfies it, with `#111204`
(09-05) preceding them by 8–10 days.

| Phase | Content | Revision vs the brief |
|---|---|---|
| **0** | Compile spike | **DONE.** Pin recorded above. |
| **1** | Shared prerequisites: the **17**-file closure, smallest first. Then the blocker fixes 1, 3, 5, 6, 7, 9, 10 from §5 — all small and mechanical. | **Reordered.** `#102505` and `#102644` move to the *end* of Phase 1, and `#102505` may be replaced by 7 call-site edits. `#102304` is unevidenced — do not port it speculatively. |
| **2** | Engine + storage snapshot. Land `#111204` first. | Unchanged, but gated on §4.1 and §4.2. |
| **3** | PromQL layer snapshot + the contrib grammar (exception granted). | Unchanged. Aggregation and binary operators arrive here. |
| **4** | HTTP layer snapshot. | Unchanged. |
| **5** | Re-apply Aiven patches: **N04 only** (re-derived, not cherry-picked). | **N03 is dropped as subsumed** — see §2. |
| **6** | Tests and verification. First task: convert the compliance test into a real gate. | Unchanged; see §7. |
| **7** | Performance check. | Unchanged. |

Run the full stateless suite after any change that touches `#102644`'s surface — it reaches
every storage engine, so green PromQL tests prove nothing about it.

### Build-glue edits the snapshot does *not* bring (4 files, ~10 lines)

`CMakeLists.txt`, `registerStorages.cpp`, `registerTableFunctions.cpp` and
`registerFunctions.cpp` need **no** change — every hook already exists here, and
`Dialect::promql` is already present. **Do not take PIN's versions of
`registerStorages.cpp` / `registerTableFunctions.cpp`:** their diff is almost entirely
upstream removing Aiven's `REGISTER_*_TABLE_ENGINE` / `REGISTER_*_FUNCTION`
binary-trimming guards (patches 051/052), and adopting it would silently strip them.

Required by hand:

1. `registerAggregateFunctions.cpp` — 2 lines for `registerAggregateFunctionTimeSeriesTopKMasks`.
2. `src/Core/Settings.cpp` — 3 declarations (`enable_time_series_table` aliased to
   `allow_experimental_time_series_table`, `enable_time_series_aggregate_functions`,
   `time_series_prefer_recent_samples_table`).
3. `src/Common/ErrorCodes.cpp` — not in the brief: `M(772, INCOMPATIBLE_SCHEMA)`,
   `M(779, UNSUPPORTED_MEDIA_TYPE)`, `M(1017, ASYNC_INSERT_FLUSH_TIMEOUT)`. All three
   numbers are unused here; insert verbatim.
4. `src/Core/SettingsChangesHistory.cpp` — 2 alias entries.

Also not in the brief: **10 integration points outside the declared path set** gain new
subsystem includes and need hand-merging (`BackupUtils.cpp`, `DatabaseAtomic.cpp`,
`DatabaseOnDisk.cpp`, `InterpreterCreateQuery.cpp`, `ASTCreateQuery.cpp`,
`ASTViewTargets.cpp`, `CreateQueryUUIDs.cpp`, `optimizeTree.cpp`). Several are shared files
with 700–1,500 lines of unrelated churn. Note the layering smell: at PIN,
`clickhouse_parsers` files include `Storages/TimeSeries/*` headers with no new link edge.

### The 8 files to delete, and why

All 8 are renames or rewrites, and all references to them at OURS are inside the
subsystem (hence overwritten). `applyMathSimpleFunction.{cpp,h}` are true renames
(`git -M` R042/R032) to `applyOneArgumentMathFunction.*`. The other four —
`TimeSeriesColumnsValidator`, `TimeSeriesDefinitionNormalizer`,
`TimeSeriesInnerTablesCreator` — were **rewritten**, not renamed, into
`normalizeTimeSeriesDefinition{,Impl}.*` and `createTimeSeriesInnerTable.*` (106 KB new vs
35 KB old). Do not expect a mechanical mapping.

---

## 7. Verification strategy

Port upstream's harness; do not invent one. Corrections to the brief's account:

- **Our test dir has 13 files; PIN has 33, not 34** — 20 PIN-only files (13 test modules,
  `generate_compliance_data.py`, `update_compliance_baseline.py`, 4 configs, and the
  `backups/time_series_prealpha.zip` fixture, 7,705 bytes).
- **Stateless coverage: ours 4 distinct tests, PIN 39** — 36 PIN-only. Unit: `gtest_PromQLParser`
  exists at both (PIN's is ~300 lines longer), plus 2 PIN-only gtests.
- **The 539-query compliance corpus is neither checked in nor downloaded.** It is
  hand-vendored as Python source inside `test_compliance.py`: 118 templates plus a
  reimplementation of upstream's `expand.go` variant sets, expanding to exactly 539
  distinct queries (5 with `should_fail=True`). That is a drift liability — the corpus can
  silently diverge from prometheus/compliance with no signal.
- **`test_promql_compliance` has no assertion — CONFIRMED, and it is worse than the brief
  says.** Zero `assert` statements; 0% still exits green. The per-query `(query, reason)`
  pairs are printed and then **discarded**, so nothing durable records *which* queries
  failed. The baseline is S3-only
  (`clickhouse-builds.s3.amazonaws.com/REFs/master/<sha>/promql_compliance/…`), the diff is
  a bare `new_pct - base_pct`, the job is `allow_failure=True` behind a `comp-promql`
  label, and `should_publish_master_baseline` hard-codes `ClickHouse/ClickHouse` + `master`
  — **a fork can never publish a baseline.** It is advisory at every single layer.
- **Our fixture does not suffice as-is.** We *do* have `with_prometheus_receiver` and
  `compose/docker_compose_prometheus.yml` pinning the same `prom/prometheus:v3.5.0` as PIN.
  But upstream refactored the accessors from scalar `prometheus_receiver_ip/_port` to dicts
  `prometheus_ip["receiver"]`, and `test_compliance.py` uses the dict API → `AttributeError`
  at ingestion. A ~10-line dict shim over our scalars fixes it without touching our 5
  working tests. The compose split into three per-role files is *not* a blocker.

### The gate to build (Phase 6, first task)

Baseline at `tests/integration/test_prometheus_protocols/compliance_baseline.json` — inside
the integration job's cache digest, and reviewable in-diff. Format: `totals`, plus
`queries: {query → pass|fail|unsupported}`, plus a `corpus_sha256` drift guard (essential,
since the corpus is Python source).

**Assert on the per-query set, never the aggregate `pct`** — an aggregate floor is defeated
by breaking N queries while fixing N others. Use ordered severity `fail < unsupported <
pass` so an `unsupported → fail` transition also counts as a regression (silently wrong is
worse than an honest 501). Fail on *improvements* too, or the baseline rots.
`update_compliance_baseline.py` at PIN already provides the refresh driver; it needs
per-query capture, an in-tree output default, and a `COMPLIANCE_BASELINE_REFRESH` flag so
the refresh run can skip its own assertions.

Reference scores: 63.1% on 26.4.1.1, 74.4% after the July series. **Do not baseline before
the port** — the corpus is dominated by `{{.simpleAggrOp}}` / `{{.binOp}}` /
`{{.compBinOp}}` expansions, so most of the 539 queries currently fail on node-type
dispatch rather than on any individual function.

### Layer 3 — the migration test we must write (acceptance gate)

Upstream's `test_upgrade_from_prealpha.py` hand-forges old metadata (`write_metadata()` +
`SYSTEM CLEAR DISK METADATA CACHE` + `ATTACH`; a third case uses `RESTORE … FROM Disk`).
**Our tree *is* upstream's prealpha** — 8 of 16 compared aspects match exactly, including
the inner kind strings `data`/`tags`/`metrics`, the `INNER UUID` keywords, `type`/`unit` as
plain `String`, the `id` DEFAULT text, both engine defaults, and the absence of both
`INNER COLUMNS` and `version`.

Four edits plus a regenerated fixture: (1) `all_tags` must exclude `__name__` — §4.1's bug
in fixture form; (2) drop `allow_dimensions_outside_sorting_key = 1`, which does not exist
here; (3) add `min_time` / `max_time` to `PREALPHA_COLUMNS`; (4) regenerate the zip from an
Aiven build.

Then add the assertion upstream lacks: **re-insert the *same* series after upgrade and
assert `uniq(id) = 1`.** Against a straight port this **will fail** — that is §4.1, and it
is the acceptance gate. If it fails, the port does not ship regardless of compliance score.

### What none of this proves — state plainly in any status report

- Compliance tops out well below 100% even upstream; the four functions reverted in
  `#120336` stay unimplemented.
- Aiven-specific config paths (our `prometheus.xml` shape, replicated-database
  interactions, N04) are not covered by upstream tests at all.
- A 25k-line snapshot has **no per-commit bisect trail**. Per-phase commits mitigate this;
  they do not eliminate it.
- None of it substitutes for a staging rollout against real metrics traffic before customer
  exposure.

---

## 8. Environment

Provisioned and verified on this VM, 2026-09-17.

| | Recommended by brief | **This VM** |
|---|---|---|
| vCPU | 32 | 32 ✅ |
| RAM | 96–128 GB | **62 GB** ⚠️ |
| Disk | 750 GB – 1 TB | **138 GB** ⚠️ |

x86_64, matching every `.buildkite/pipeline.yml` step's `expensive-x86_64` queue — the
right call given how many float-precision edge cases this port carries against
`FLOAT_MARGIN = 1e-4`.

**The RAM shortfall is the brief's own documented OOM shape** (32 vCPU × 62 GB, with ninja
choosing ~34 jobs and the rule against passing `-j`). Mitigated with a **16 GB swapfile**
(23 GB total swap). btrfs needs the `chattr +C` dance:

```bash
sudo truncate -s 0 /swapfile2 && sudo chattr +C /swapfile2
sudo fallocate -l 16G /swapfile2 && sudo chmod 600 /swapfile2
sudo mkswap /swapfile2 && sudo swapon /swapfile2
```

Peak observed usage was ~7 GB resident with 44 GB in page cache; swap was never touched,
including through the final link. **The cold build completed clean: 15,981/15,982 steps,
zero failures, ~55 min** (with subagents competing for CPU), producing a 3.96 GB binary.
Both health probes pass: `SELECT version()` → `26.3.32.1`, and
`.claude/tools/cppexpr.sh -i Core/Block.h 'OUT(sizeof(DB::Block))'` → `96`.

**Disk is the real constraint.** 138 GB total, ~74 GB free after `.git` (20 GB),
submodules, a RelWithDebInfo build and a 20 GB-capped ccache. The brief budgets 50–80 GB
for Docker images alone, so **integration tests do not fit alongside this build** — which
is why Docker is installed but unused, and why Phase 6 needs either a bigger volume or a
dedicated host. The `testing-suites.md` §6 worktree-flip technique also wants ~50 GB for a
second worktree; it will not fit either.

### Setup deltas from `build-and-test.md`

The runbook's §1 is written for a pre-provisioned host. From a bare Fedora 43 cloud image
the actual sequence is:

```bash
sudo dnf install -y clang lld llvm ninja-build cmake ccache git python3 python3-requests \
                    moby-engine nasm yasm perl-IPC-Cmd
ccache -M 20G                                  # cmake/ccache.cmake:48 FATALs without a cache
curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain none
rustup toolchain install nightly-2025-07-07 --profile minimal   # pinned by corrosion-cmake
git submodule update --init --recursive --jobs 16

export CC=/usr/bin/clang CXX=/usr/bin/clang++
cmake --fresh -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_C_COMPILER="$CC" -DCMAKE_CXX_COMPILER="$CXX"
ninja -C build clickhouse > build/ninja-baseline.log 2>&1   # no -j
```

Three gates the runbook does not mention, each a hard `configure` failure:
`cmake/ccache.cmake:48` requires ccache **or** sccache; `contrib/corrosion-cmake` requires
Rust **`nightly-2025-07-07`** exactly; and the `contrib/NuRaft` recursive submodule fetch
emits `upload-pack: not our ref a0eda005b0f…` — **harmless**, the pinned
`5a7f10a6960` checks out fine and the build is unaffected.

---

## 9. First actions for whoever picks this up

1. **Read §4.** Two decisions block Phase 2/3 code, and §4.1 is a production-data hazard,
   not a preference. Get a real `SHOW CREATE TABLE` for §4.3.
2. Re-verify §2 against current `upstream/master`, or accept the pin. Re-pinning
   invalidates every file-set number here.
3. Phase 1 is ready to start now and is **not** gated on §4 — the 17-file prerequisite
   closure plus blockers 1, 3, 5, 6, 7, 9, 10 are all mechanical.
4. Before scheduling from §5's table, note the analyst's caveat: the spike **never reached
   linking**, so link-time gaps are unmeasured.

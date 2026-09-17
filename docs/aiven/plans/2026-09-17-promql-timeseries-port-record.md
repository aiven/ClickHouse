# Port record — PromQL / `TimeSeries` snapshot port onto 26.3-aiven

Companion to `2026-09-17-promql-timeseries-backport.md` (the plan). This file records **what was
actually done**, every 26.3 adaptation and why, and the verification results.

| Field | Value |
|---|---|
| Pinned upstream commit | `290adb52c4ee238c9f3f0b557d6f2ca9bb3b29ff` (2026-09-17 08:23:47 +0000) |
| Base | `v26.3.32.14-lts-aiven-dev` @ `a5cc61775e7` (`VERSION_STRING 26.3.32.1`) |
| Status | **Compiles, links, and evaluates PromQL correctly.** 70/86 stateless tests pass, 49/49 subsystem unit tests, 15,625 unit tests overall. One open decision (§5). Staged, not committed. |

**Product decision (2026-09-17):** `TimeSeries` is used only in test ClickHouse, so backward
compatibility is explicitly **not** preserved. Upstream's schema and DDL are taken verbatim: no
version-gated compatibility layer, no Aiven migration test, no production DDL needed. This retires
the `#114300` series-splitting hazard (plan §4.1) as a blocker, and settles the `ViewTarget::Kind`
question (plan §4.2) in favour of upstream's shape.

---

## 1. Compile burn-down

| Iteration | State | Failing TUs |
|---|---|---|
| — | baseline `HEAD`, no overlay | 0 (15,981/15,982 steps, 3.96 GB binary, health probes green) |
| P1a | 17 prerequisite files only | 3 |
| P1b | + 2 mechanical fixes | 1 |
| P1c | + `StorageWithCommonVirtualColumns` adapted | **0 — Phase 1 green, `#102644` avoided** |
| S1 | + full subsystem snapshot (220 files) | 46 |
| S2 | + alias/settings/metadata/alter fixes | 29 |
| S3 | + aggregate-function interface, `ViewTarget` rename, small helpers | 9 |
| S4 | + virtuals, `finishExecutedQuery`, `resetChild`, arity fixes | 2 |
| S5 | + Prometheus HTTP layer | **0 compile errors — reached linking** |
| S6 | + error codes, settings, `topKMasks` registration | **LINKS. 3.99 GB binary.** |

Reaching the link stage was the single largest unmeasured risk in the plan: neither Phase 0 spike
had got there. Link surfaced exactly six undefined symbols — three error codes and three settings —
all of which the snapshot manifest had predicted.

---

## 2. The two high-risk prerequisites were both avoided

The plan's Phase 1 called for porting `#102505` (`getInMemoryMetadataPtr` context-awareness) and
`#102644` (virtuals into in-memory metadata, 138 files, every storage engine). **Neither was
ported.**

- **`#102505`** reduced to 13 mechanical call sites across 8 files. Upstream takes
  `(ContextPtr, bool)`; 26.3 takes `(bool bypass_metadata_cache = false)`. Every call site passed
  `false`, so dropping the context argument preserves semantics exactly. *Lost:* MergeTree metadata
  caching keyed on query context.
- **`#102644`** was wanted by exactly two places, both adapted:
  - `StorageWithCommonVirtualColumns.cpp` — replaced
    `VirtualColumnUtils::filterVirtualColumns` (which keys off the `is_common` flag and
    `VirtualsMaterializationPlace`, both introduced by `#102644`) with a local
    `filterCommonEphemeralVirtuals` that drops the two common ephemeral virtuals this class
    materializes itself and keeps upstream's "at least one physical column survives" guarantee via
    `ExpressionActions::getSmallestColumn`.
  - The three TimeSeries storages — `storage_metadata.setVirtuals` → `IStorage::setVirtuals`, and
    the 4-argument `addEphemeral` → 26.3's 3-argument form.

---

## 3. Every 26.3 adaptation, and what it costs

Each is marked in the source with a `/// NOTE(aiven):` comment naming what upstream does and why
26.3 differs.

### Ported into shared code (upstream behaviour reproduced)

| What | Where | Note |
|---|---|---|
| `finishExecutedQuery` + `QueryFinishCallback` | `Interpreters/executeQuery.{h,cpp}` | Clean port, not an adaptation — both dependencies (`BlockIO::releaseQuerySlot`, `onFinish(finish_time)`) already exist at 26.3. 5 call sites. |
| `ReadFromTimeSeriesStep` optimizer branch | `Processors/QueryPlan/Optimizations/optimizeTree.cpp` | **Found by testing, not by the compiler.** Without it every plain `SELECT … FROM <TimeSeries table>` throws `Code: 49`. Adapted only in `tryMergeExpressions`'s third argument. |
| `ASTColumnDeclaration::resetChild` / `resetDefaultExpression` | `Parsers/ASTColumnDeclaration.{h,cpp}` | 26.3 already had the packed-index children design, only the reset half was missing. |
| `makeASTLambda` | `Parsers/ASTFunction.{h,cpp}` | |
| `IAST::{set,replace,setOrReplace,reset}(ASTPtr &, ASTPtr)` | `Parsers/IAST.h` | 26.3 had only the `T * &` forms. No overload ambiguity. |
| `equalsCaseInsensitive(string_view, string_view)`, `toLowerASCII`, `toUpperASCII` | `Common/StringUtils.h` | |
| `UTF8::isSurrogateCodePoint` | `Common/UTF8Helpers.h` | |
| `SettingsChanges::{tryGetChange,setSetting(SettingChange),setSettings,removeSettings}` | `Common/SettingsChanges.{h,cpp}` | |
| `MULTITARGET_FUNCTION_X86_V4` | `Common/TargetSpecific.h` | Purely additive: upstream *renamed* 26.3's `_V4_V3` to `_V4` and reintroduced `_V4_V3` with our exact old body. The undefined macro name was being parsed as a declarator, wrecking a whole class body — 13 diagnostics from one missing macro. |
| `PrometheusMetricsWriter` constant labels + `getReservedLabelNames` | `Server/PrometheusMetricsWriter.{h,cpp}` | Also adopts upstream's `writeDoubleQuotedString` escaping of histogram/dimensional label values. |
| `WriteBufferFromHTTPServerResponse` `buf_size` | `Server/HTTP/WriteBufferFromHTTPServerResponse.{h,cpp}` | 26.3's `HTTPWriteBuffer` already accepted it; 2 lines. |
| Two-alias support for settings | `Core/BaseSettings.h` | `DECLARE_SETTINGS_WITH_ALIAS_TRAITS_` made variadic over the alias list. It was the only fixed-arity macro consuming `ALIAS`; everything else receiving those entries was already variadic. Needed because upstream's own tests use all three of `enable_time_series_aggregate_functions`, `allow_experimental_time_series_aggregate_functions` and `allow_experimental_ts_to_grid_aggregate_function`. |
| `IColumn::convertToFullIfWrapped` | `Columns/IColumn.h` (+ `ColumnDynamic.h`, `ColumnVariant.h`) | **Added alongside** `convertToFullIfNeeded`, not renamed: upstream's rename also dropped LowCardinality from the chain, which would change behaviour at ~30 unrelated 26.3 call sites. |

### Adapted to 26.3 (upstream feature deliberately not taken)

| Upstream feature | Why not ported | What is lost |
|---|---|---|
| `#102644` virtuals into in-memory metadata | 138 files, every storage engine, for 2 call sites | nothing — behaviour reproduced locally |
| `#102505` `getInMemoryMetadataPtr(ContextPtr, bool)` | 13 mechanical sites vs a repo-wide signature change | MergeTree metadata caching keyed on query context |
| `merge`/`mergeImpl` non-virtual-interface split | **88 declaration sites across 70 files**, plus upstream's new `chassert(place != rhs)` going live for every aggregate function in the tree | upstream's self-aliasing assertion. The 4 overlaid `mergeImpl` overrides were renamed back to `merge`. |
| AST-JSON subsystem | does not exist at 26.3 (`src/Parsers/ASTJSON*.h`, `IAST::writeJSON`/`readJSON`/`createFromJSON` all absent); ~185 of the 300 added lines in `ASTViewTargets.cpp` | `ASTViewTargets`/`ASTCreateQuery` JSON round-trip; `parseQueryToJSON`. `formatTarget` therefore takes `time_series_version` explicitly. |
| `Core/ConstantValue.h` + `evaluateConstantExpressionAsColumn` | 2 call sites, both just reading a String literal argument; porting needed a restructure of `evaluateConstantExpression` **and** de-duplication of `DB::ConstantValue`, which still exists at 26.3 as `Analyzer/ConstantValue.h` — 6 files of Analyzer surgery | a `Field` materialization avoided. Dropping this also let the `ColumnConstPtr` plumbing be reverted (`COW.h` access widening, `IColumn_fwd.h` aliases, `ColumnConst.cpp` overloads), shrinking the port. |
| `FieldVisitorToCastedLiteral` / `shouldPrintParametersWithTypes` | new `Field` visitor + a `DataTypeAggregateFunction::getNameImpl` rewrite | printed aggregate-function parameter type names lose their original decimal scale. Verified safe: the untyped form still round-trips through `parseTimeSeriesTimestamp` / `parseTimeSeriesDuration`. |
| `IAggregateFunction::addBatchWithNonNullPlaces` | no such virtual and no caller at 26.3 | nothing |
| Embedded storage documentation | 26.3's `StorageFactory::registerStorage` takes only (name, creator_fn, features) | the `TimeSeries` engine's ~550-line docs blob is not in the binary |
| `default_session_user` | a whole feature: a `ServerSettings` entry, a composable-protocols per-endpoint override, and consumers in `GRPCServer` and `ArrowFlight/AuthMiddleware`, none of which exist at 26.3 | a Prometheus endpoint cannot override which user an unauthenticated request runs as. **Mitigation:** the per-endpoint `<user>` element still works (it becomes `connection_config.credentials`). |
| `allow_dimensions_outside_sorting_key` | the MergeTree setting does not exist at 26.3, and emitting it makes `CREATE TABLE … ENGINE = TimeSeries` fail outright with `UNKNOWN_SETTING` | nothing: 26.3 has no such rejection either, so the layout is accepted as before. The off-key columns are functionally dependent on `id`, which is in the sorting key. |
| `DimensionalMetrics` metric type | `DimensionalMetrics::MetricFamily` has no type at 26.3 | dimensional metrics are always exposed as `gauge` |
| Key-value asynchronous metrics | `AsynchronousMetricValue` is a plain scalar at 26.3 — no `isMap`, `key_label`, `key_values` | asynchronous metrics never carry a label, so `cpu`/`device`/`disk`/… stay usable as constant labels |
| `SettingFieldBase` removal | 26.3's settings framework still requires the polymorphic base | nothing — the two new field types were adapted to derive from it, with member-wise copy members so the copy does not route through the base's implicit copy constructor (`-Wdeprecated-copy-with-dtor` is `-Werror` here) |
| `IStorage::alter`'s 4th `DDLGuardPtr &`, `executeDropQuery`'s 8th `propagate_metadata_transaction` | no such concept at 26.3 | DDL-transaction propagation on inner-table truncate |
| `checkTableSizeBelowDropLimit` | it is upstream's **rename** of 26.3's `checkTableCanBeDropped`, not a missing feature | nothing |

### Build glue the snapshot did not bring

`CMakeLists.txt`, `registerStorages.cpp`, `registerTableFunctions.cpp` and `registerFunctions.cpp`
needed **no** change. **Do not take PIN's `registerStorages.cpp` / `registerTableFunctions.cpp`:**
their diff is almost entirely upstream removing Aiven's `REGISTER_*_TABLE_ENGINE` /
`REGISTER_*_FUNCTION` binary-trimming guards (patches 051/052).

Required by hand, all confirmed by the linker:

1. `Common/ErrorCodes.cpp` — `M(772, INCOMPATIBLE_SCHEMA)`, `M(779, UNSUPPORTED_MEDIA_TYPE)`,
   `M(1017, ASYNC_INSERT_FLUSH_TIMEOUT)` at upstream's numbers (all unused at 26.3), placed in
   numeric order, and **`END` bumped 1004 → 1017** because the values array is indexed by code.
2. `Core/Settings.cpp` — `enable_time_series_table` (alias `allow_experimental_time_series_table`),
   `enable_time_series_aggregate_functions` (both old aliases), and the new
   `time_series_prefer_recent_samples_table`. Upstream's `PRIVATE_PREVIEW` tier does not exist at
   26.3, so `EXPERIMENTAL` is kept.
3. `Interpreters/executeQuery.cpp` — the PromQL-dialect gate now reads `enable_time_series_table`.
4. `Core/SettingsChangesHistory.cpp` — three entries recording the renames and the new setting.
5. `AggregateFunctions/registerAggregateFunctions.cpp` —
   `registerAggregateFunctionTimeSeriesTopKMasks`. **Found by testing, not by the compiler:** without
   it `topk` / `bottomk` fail at runtime with `Code: 46 UNKNOWN_FUNCTION`.

### Aiven-local patches

- **`patch-new(N03)` dropped as fully subsumed** by upstream `#108388` (+`#115944`). PIN's
  `DDLDependencyVisitor.cpp` has the byte-identical mechanism and additionally covers
  `RecentSamples`. Its duplicate test `04410_time_series_referential_dependencies` was removed —
  PIN carries the same test as `04409_…`, so keeping both would duplicate it.
- **`patch-new(N04)`** is inside the snapshot and was overwritten. **It still needs re-deriving**
  against PIN's restructured `StorageTimeSeries` (`getTargetTableID`/`tryGetTargetTableID` at
  `:339/:344` vs our old single `getTargetTableId`). Its integration test at
  `tests/integration/test_aiven_time_series_recovery/` is untouched and still present.

---

## 4. Verification results

### Unit tests — 49/49 green

```
./build/src/unit_tests_dbms --gtest_filter='PromQL*:*TimeSeries*:*Timeseries*:*Prometheus*:*prometheus*'
  PromQLParser                        26 tests  PASSED   (includes ComplianceQueries)
  PrometheusMetricsWriter              2 tests  PASSED
  NormalizeTimeSeriesDefinitionTest   21 tests  PASSED   (includes ConvertsDefinitionsOfOlderVersions)
```

Full suite: **15,625 tests pass.** The failures are all outside the port's footprint:
`AIClientFactory` / `AITestFixture` need a live API key; `WBS3/SyncAsync` and
`DiskObjectStorageTest.CopyEmptyFileToPlainRewritable` need object storage (the latter aborts on a
libc++ hardening assertion); and `TemporaryReplaceTableName.{FromString,ToString}ValidInput` is
**demonstrably pre-existing** — the test passes `_tmp_replace_` while
`Interpreters/TemporaryReplaceTableName.cpp` produces `.tmp_replace_`, and none of those three files
was touched by this port.

> Not proven: the `DiskObjectStorageTest` abort was not checked against a rebuilt baseline binary, so
> "pre-existing" is inference from it being outside the change footprint, not measurement.

### Stateless tests — 70 passed, 16 failed, 3 skipped

All 182 `*time_series*` / `*timeseries*` / `*promql*` / `*prometheus*` stateless test files were
replaced with PIN's versions, because the implementation is now upstream's. That took the suite from
19 tests to 91; ours carried 26.3-era references for functions upstream has since changed.

Three real defects were found this way — **two are fixed, one is escalated**:

| Defect | Status |
|---|---|
| Missing `ReadFromTimeSeriesStep` optimizer branch — every plain `SELECT … FROM <TimeSeries table>` threw `Code: 49` | **FIXED** (see §3) |
| `HashTable::reserve(0)` doubled the buffer, giving **×2 memory per thread** in `timeSeries*ToGridMerge` — ~1.5 TiB at `max_threads=32` for a 3-row input | **FIXED** — upstream `af15bc01cd5`, one line in `Common/HashTable/HashTable.h`. `resize(0)` fell through to the grow-one-step primitive, and with N threads and few rows N−1 empty per-thread states each called `reserve(0)` on the destination map. Verified: 64 MiB now suffices at every thread count. **A generic bug, not TimeSeries-specific** — it hit any `reserve(size() + 0)` site. |
| `timeSeriesGroupArray` missing from `supported_functions` in `DataTypes/DataTypeCustomSimpleAggregateFunction.cpp` | **FIXED** — one line |
| **`CREATE TABLE d AS <ts> SETTINGS …` fails** with `Unknown setting … for storage MergeTree` | **OPEN — escalated, see §5** |

The 16 remaining failures are fully accounted for and none indicates a defect in the ported
subsystem:

| Cause | Tests | Note |
|---|---|---|
| Upstream's **EXPLAIN pretty formatter** is absent (`Processors/QueryPlan/QueryPlanFormat.{cpp,h}`, ~861 lines, plus `explain_query_plan_default = PRETTY`, default since 26.7) | 6 — `04816`, `04817`, `04836`, `04891`, `05059_promql_empty_by_group`, `05141_time_series_select_join_settings` | **Only plan-*text* assertions fail; every functional assertion in all six passes.** Recommend not porting: it is a general 26.7 EXPLAIN overhaul, unrelated to this subsystem. |
| `allow_dimensions_outside_sorting_key` in hand-written test fixtures | 4 — `04202`, `04337`, `04549`, `04897` | The tests hand-roll a tags table with that setting. Fixture-side, so the port is not implicated. |
| Test server started from `programs/server/config.xml`, not `tests/config/install.sh` | 2 — `04815` (needs `listen.xml` for `remote('127.0.0.2')`), `03254_timeseries_instant_value_aggregate_functions` (needs `process_query_plan_packet`) | Environment. The latter then needs one `::Decimal64` reference edit, the only place the dropped `shouldPrintParametersWithTypes` shows up in 182 files. |
| ZooKeeper absent | 1 — `04341` (`ENGINE = Replicated`) | Untagged `zookeeper`, so `--no-zookeeper` cannot skip it; it consumed 600 s of the 635 s run. |
| AST-JSON not ported (`parseQueryToJSON`) | 1 — `05213` | Diff is exactly 3 reference lines. |
| `CREATE … AS … SETTINGS` gap | 1 — `05059_time_series_create_as_keeps_settings` | The open defect above. |
| Upstream's 26.7 numeric-`DateTime64` input semantics | 1 — `05142` | Unrelated to the subsystem. |

### Capability and correctness evidence

`evidence-2026-09-17/promql-capability.txt` — 41 PromQL expressions probed. **Before: 8 evaluated,
28 returned `NOT_IMPLEMENTED`. After: all 41 evaluate.**

`evidence-2026-09-17/promql-end-to-end.txt` — 14 queries over a 3-series fixture, all numerically
verified by hand: `sum` = 112, `sum by (job)` = {api: 105, web: 7}, `avg` = 37.3333,
`avg_over_time` = {a: 40, b: 20, c: 4}, `sum by (job) (rate(…))` = {api: 0.35, web: 0.0233}. Prometheus
label semantics are correct too — `rate` drops `__name__`, comparison operators keep it.

The new upstream schema is live: `SHOW CREATE TABLE` emits `SAMPLES` / `TAGS` / `METRIC FAMILIES`
targets with `INNER COLUMNS` clauses, and `id` is `Tuple(UInt64, LowCardinality(UUID))`.

### Not yet done

- **Integration tests** — need Docker, which does not fit on this VM alongside the build (plan §8).
  This includes the whole `test_prometheus_protocols` suite and therefore the entire
  Prometheus-compliance measurement. **This is the largest remaining gap in verification.**
- **The compliance gate** — `test_promql_compliance` still has no assertion (plan §7).
- **`patch-new(N04)`** re-derivation against PIN's restructured `StorageTimeSeries`.
- **Performance check** (plan Phase 7).

---

## 5. Open decision — `CREATE TABLE d AS <ts> SETTINGS …`

Precisely scoped: `CREATE TABLE d AS <ts>` **works** and inherits the `TimeSeries` engine; only the
form with an explicit `SETTINGS` clause fails, with
`Code: 115 Unknown setting 'store_min_time_and_max_time': for storage MergeTree`.

Cause: in our `Interpreters/InterpreterCreateQuery.cpp` `setEngine`, the
`if (create.storage) { if (!create.storage->engine) setDefaultTableEngine(...); return; }` block runs
*before* the `AS`-clause extraction, so a partial storage clause (SETTINGS only) short-circuits to
`default_table_engine`. Upstream restructured `setEngine` so the `AS` extraction runs first and then
merges, adding both an `else if (as_create.is_time_series_table)` branch and a general
"`AS y` + partial storage clause inherits the engine and merges settings" path.

**Not fixed unilaterally, and here is why.** `setEngine` governs **every** `CREATE TABLE … AS …` in
the server. Reordering it changes engine resolution for all table engines, not just `TimeSeries` —
which is the "default behavior change with broad blast radius" that `docs/aiven/AGENTS.md` §7 says to
escalate as `policy_call` rather than ship. It is also the same judgement applied throughout this
port: `#102644`, the `merge`/`mergeImpl` split and the EXPLAIN formatter were all declined for the
same reason.

Three options for the human:

1. **Port upstream's `setEngine` restructuring** and run the full stateless suite (~10,187 tests) to
   clear the blast radius. Correct and upstream-aligned; the most work.
2. **Add a narrow `TimeSeries`-only branch** before the early return: when `create.storage` has no
   engine and the `AS` source is a `TimeSeries` table, inherit that engine and let
   `normalizeTimeSeriesDefinition` merge the settings. Contained, but diverges from upstream and will
   conflict at the next rebase.
3. **Accept the gap.** `CREATE … AS <ts>` works; only the `SETTINGS` variant does not. Given
   `TimeSeries` is test-only here, this may simply be acceptable — in which case
   `05059_time_series_create_as_keeps_settings` should be marked as a known divergence.

---

## 6. Reproduction

```bash
PIN=290adb52c4ee238c9f3f0b557d6f2ca9bb3b29ff

# Subsystem snapshot (see the plan §2 for the exact path set), then:
git checkout $PIN -- <subsystem paths>
git checkout $PIN -- <the 16 shared prerequisite files>   # Core/ConstantValue.h is NOT among them
git rm --ignore-unmatch <the 8 renamed/rewritten predecessors>
git checkout $PIN -- <the 182 stateless test files>

export CC=/usr/bin/clang CXX=/usr/bin/clang++
cmake -B build            # plain, never --fresh: refreshes the source glob
ninja -C build clickhouse
```

The 26.3 adaptations are the unstaged half of the working tree; the verbatim upstream content is the
staged half, which makes review straightforward: `git diff --cached` is upstream, `git diff` is ours.

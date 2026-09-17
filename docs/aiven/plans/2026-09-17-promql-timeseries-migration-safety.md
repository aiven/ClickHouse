# TimeSeries migration-safety analysis

- **PIN** = `290adb52c4ee238c9f3f0b557d6f2ca9bb3b29ff` (`upstream/master`)
- **OURS** = `HEAD` (`a5cc61775e7`)
- **branch point** (`git merge-base`) = `aa5df024249641558bd2e553166246ab017c1a43`

---

## 0. CRITICAL FINDING (read this first)

**#114300 (`tags` column change) has NO backward-compatibility path for time-series
identifier generation. A version-0 table (which is exactly what every Aiven
production `TimeSeries` table is) will, after the port, compute *different* `id`
values for the *same* time series than the ones already stored in its tags/samples
tables. `MIN_WRITABLE = 0`, so the server happily keeps writing into such a table —
this is silent series-splitting, not a hard error.**

Three independent code facts combine to produce it:

1. **The stored `id` DEFAULT expression is overwritten on ATTACH.**
   `src/Storages/TimeSeries/normalizeTimeSeriesDefinitionImpl.cpp:1168-1187` (PIN),
   inside `convertPrealphaDefinition()`:

   ```cpp
   case ViewTarget::Tags:
   {
       /// Column "id".
       add_column(TimeSeriesColumnNames::ID, dataTypeToAST(id_type));
       {
           auto & new_decl = new_list->children.back()->as<ASTColumnDeclaration &>();
           new_decl.ephemeral_default = false;
           if (!time_series_settings[TimeSeriesSetting::id_generator].value)
           {
               /// Function getDefault has changed since the prealpha version,
               /// so it can generate different identifiers now.
               new_decl.default_specifier = ColumnDefaultSpecifier::Default;
               new_decl.setDefaultExpression(TimeSeriesIDGenerator::getDefault(id_type, table_id));
           }
   ```

   `add_column` first *clones* the pre-existing declaration (with the old
   `DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags))`), then the block
   above **replaces** that DEFAULT. Upstream's own comment admits the consequence.

2. **The new canonical generator hashes a different thing.**
   `src/Storages/TimeSeries/TimeSeriesIDGenerator.cpp:80-89` (PIN):

   ```cpp
   ASTPtr TimeSeriesIDGenerator::tryGetDefault(const DataTypePtr & id_type)
   {
       /// The `tags` column contains all the tags, including the `__name__` tag with the metric name
       /// and the tags stored in the columns specified in the `tags_to_columns` setting,
       /// so hashing just `tags` is enough to identify a time series.
       ASTs arguments_for_hash_function{make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags)};
   ```

   New: `reinterpretAsUUID(sipHash128(tags))`.
   OURS (`src/Storages/TimeSeries/TimeSeriesDefinitionNormalizer.cpp:294-336`):
   `reinterpretAsUUID(sipHash128(metric_name, all_tags))`.

3. **Even preserving the old expression does not preserve the old value**, because the
   *content* of the input columns changed. At OURS
   (`src/Storages/TimeSeries/PrometheusRemoteWriteProtocol.cpp:342-352`) `all_tags`
   is filled with every label **except** `__name__`:

   ```cpp
   if (tag_name == TimeSeriesTagNames::MetricName)
   {
       metric_name_column.insertData(tag_value.data(), tag_value.length());
   }
   else
   {
       if (time_series_settings[TimeSeriesSetting::use_all_tags_column_to_generate_id])
       {
           all_tags_names->insertData(tag_name.data(), tag_name.length());
           all_tags_values->insertData(tag_value.data(), tag_value.length());
       }
   ```

   (Verified identical at the branch point `aa5df02` — i.e. this is genuinely how our
   production data was written.)

   At PIN, `all_tags` is just an alias of `tags`
   (`src/Storages/TimeSeries/TimeSeriesSink.cpp:506-510`):

   ```cpp
   if (id_generator_uses_all_tags)
   {
       /// The `all_tags` column always contains the same data as the `tags` column.
       tags_header_before_id.insert(ColumnWithTypeAndName{tags_map_type, TimeSeriesColumnNames::AllTags});
   }
   ```

   and `tags` now includes `__name__` and the `tags_to_columns` tags
   (`src/Storages/TimeSeries/TimeSeriesSink.cpp:305-323`):

   ```cpp
   /// The "tags" column gets all the tags, including the metric name and the tags
   /// which are also stored in dedicated columns.
   out_tags_names.insertData(tag_name.data(), tag_name.size());
   out_tags_values.insertData(tag_value.data(), tag_value.size());
   ```

   So `sipHash128(metric_name, all_tags)` evaluated by PIN code ≠ the value evaluated
   by our current code, even for a byte-identical expression. **There is no in-tree
   setting that reproduces the old identifiers.**

**Consequence.** After the upgrade, the tags table accumulates a *second* row per live
series (old id + new id) with an identical resolved label set once
`timeSeriesTagsToMap` merges `metric_name` back in. Historical samples stay under the
old id, new samples land under the new id. A PromQL instant/range query spanning the
upgrade boundary can therefore see two series with the same labelset — which PromQL
semantics reject ("vector cannot contain metrics with the same labelset") or, at best,
returns as a discontinuity. Upstream's own migration test does **not** cover this: it
inserts `foo` before the upgrade and a *different* series `bar` after, so the
same-series-across-the-boundary case is never exercised (see §5).

Grep evidence that no version guard exists in the write path: the only uses of
`getVersion()` in `TimeSeriesSink.cpp` and `PrometheusRemoteWriteProtocol.cpp` at PIN
are `TimeSeriesColumnNames::getOuterSamples(...)` (lines 437, 519, 596 and 328
respectively). Nothing branches on version for the `id`/`tags` computation.

**Required decision before the port can be accepted:** either (a) add a version-0
branch that restores the old `all_tags` semantics (all labels except `__name__` as a
separate column) and keeps the stored `id` DEFAULT untouched, or (b) treat this as a
one-way data migration (`INSERT ... SELECT` into a freshly created table) and make
version-0 tables read-only in the Aiven build by raising `MIN_WRITABLE` to 1.

---

## 1. Schema versioning (#111204)

`src/Storages/TimeSeries/TimeSeriesVersion.h` at PIN. There is also a
`TimeSeriesVersion.cpp` with the three check functions.

### Version constants — verbatim

```cpp
    /// The latest version, new tables get it unless the CREATE query specifies another supported version.
    /// Bump it each time the schema of the target tables or the semantics of the stored data changes;
    /// every version in [MIN_SUPPORTED, LATEST] must stay supported, so either make the schema generation
    /// version-aware or bump MIN_SUPPORTED too.
    constexpr UInt64 LATEST = 4;

    /// The first version recording the `id_type` setting (see the version history above).
    /// A table of an earlier version must not have the setting: an older server wouldn't understand it.
    constexpr UInt64 MIN_WITH_ID_TYPE_SETTING = 2;

    /// The first version naming the outer column with samples `samples` instead of `time_series` (see the version history above).
    constexpr UInt64 MIN_WITH_SAMPLES_OUTER_COLUMN = 3;

    /// The minimum version which can be read with SELECT and whose creation can be replayed on another node.
    /// A table with an older version can still be attached, inspected with SHOW CREATE TABLE and dropped.
    constexpr UInt64 MIN_SUPPORTED = 0;

    /// The minimum version which can be written into (INSERT, Prometheus remote-write).
    /// Older supported tables are read-only, so the data can be copied out of them with INSERT-SELECT.
    constexpr UInt64 MIN_WRITABLE = 0;

    /// The minimum version supported by the PromQL execution layer (the `prometheusQuery`, `prometheusQueryRange`
    /// and `timeSeriesSelector` table functions, the `promql` dialect, and the Prometheus HTTP query API).
    /// The PromQL layer may support fewer versions than the table engine itself.
    constexpr UInt64 MIN_SUPPORTED_BY_PROMQL = 0;

    /// The first version whose "metric families" target is named "metricfamilies" in the names of inner tables
    /// and in backups, and is written with the keyword `METRIC FAMILIES` in the definition.
    /// The earlier versions name it "metrics" and write it with the keyword `METRICS`, so an older server can read them.
    constexpr UInt64 MIN_WITH_METRIC_FAMILIES_TARGET_NAME = 4;
```

Full list: `LATEST = 4`, `MIN_SUPPORTED = 0`, `MIN_WRITABLE = 0`,
`MIN_SUPPORTED_BY_PROMQL = 0`, `MIN_WITH_ID_TYPE_SETTING = 2`,
`MIN_WITH_SAMPLES_OUTER_COLUMN = 3`, `MIN_WITH_METRIC_FAMILIES_TARGET_NAME = 4`.

Note `MIN_WRITABLE = 0` and `MIN_SUPPORTED_BY_PROMQL = 0`: version-0 tables are fully
readable **and writable**. This is what turns the §0 finding from "blocked at the
door" into "silent corruption".

### Version history (from the file's header comment)

```
///   0 - Tables created before the `version` setting was introduced (including "prealpha" tables
///       and tables without the recent samples table).
///   1 - The `version` setting was introduced.
///   2 - The `id_type` setting was introduced: ...
///   3 - The outer column `time_series` was renamed to `samples`. ...
///   4 - The "metrics" target table was renamed to "metric families": ...
```

### Control flow for a definition with NO explicit `version`

`src/Storages/TimeSeries/normalizeTimeSeriesDefinitionImpl.cpp:1881-1917`:

```cpp
void normalizeTimeSeriesDefinitionImpl(ASTCreateQuery & create_query, const NormalizeTimeSeriesDefinitionParams & params)
{
    chassert(create_query.is_time_series_table);

    /// Whether we're creating a new table.
    bool is_new_table = params.isNewTable();

    /// Whether the create query may come from an older version, so it can be converted to the current form.
    /// The initial CREATE query is excluded: it must be written in the current form already.
    bool can_convert = (params.mode != LoadingStrictnessLevel::CREATE) || params.is_restore_from_backup;

    /// Convert the create_query if it was created before the `version` setting was introduced.
    if (can_convert && !hasExplicitTimeSeriesSettingVersion(create_query))
    {
        convertDefinitionWithoutExplicitVersion(create_query);
        chassert(hasExplicitTimeSeriesSettingVersion(create_query));
    }

    /// The older forms of the definition below were written only by servers which didn't support versioning yet,
    /// so they can be found only in tables of version 0.
    if (can_convert && (getTimeSeriesSettingVersion(create_query) == 0))
    {
        /// Convert the create_query if it was created by the old versions.
        /// (A new query written in the prealpha form must be rejected, see readTypesFromOuterColumns.)
        if (isPrealpha(create_query))
        {
            convertPrealphaDefinition(create_query);
            chassert(!isPrealpha(create_query));
        }

        /// Convert the create_query if it was created before the recent samples table existed.
        if (isVersionWithNoRecentSamplesTTL(create_query))
        {
            convertDefinitionWithoutRecentSamplesTTL(create_query);
            chassert(!isVersionWithNoRecentSamplesTTL(create_query));
        }
    }
```

`normalizeTimeSeriesDefinitionImpl.cpp:1062-1065`:

```cpp
    /// Converts a create query written by a server which didn't support versioning yet:
    /// such tables belong to version 0 (see TimeSeriesVersion.h).
    void convertDefinitionWithoutExplicitVersion(ASTCreateQuery & create_query)
    {
        setTimeSeriesSettingVersion(create_query, 0);
    }
```

`src/Storages/TimeSeries/TimeSeriesSettings.cpp:220-233` (`setTimeSeriesSettingVersion`)
writes `version = 0` into the SETTINGS clause of the stored AST.

Prealpha detection, `normalizeTimeSeriesDefinitionImpl.cpp:1069-1082`:

```cpp
    /// Detects prealpha version by outer columns: prealpha had outer columns `id`, `timestamp`, `value`,
    /// and now we don't have them.
    bool isPrealpha(const ASTCreateQuery & create_query)
    {
        ...
            if (decl.name == TimeSeriesColumnNames::Timestamp
                || decl.name == TimeSeriesColumnNames::Value
                || decl.name == TimeSeriesColumnNames::ID)
                return true;
```

Fresh CREATE takes the other branch — `normalizeTimeSeriesDefinitionImpl.cpp:1990-1996`:

```cpp
        /// Pin `version`, so that the table keeps its version if a future server bumps the latest one.
        /// Converted queries have a version at this point (see above), so a missing version here means a fresh CREATE.
        if (!settings[TimeSeriesSetting::version].isChanged() && create_query.storage)
        {
            setEngineSettings(*create_query.storage, "version",
                Field(settings[TimeSeriesSetting::version].value));
        }
```

and the setting's default is `LATEST`
(`src/Storages/TimeSeries/TimeSeriesSettings.cpp:38`):

```cpp
    DECLARE(UInt64, version, TimeSeriesVersion::LATEST, "The version of the TimeSeries table: ... Tables created before this setting was introduced are considered as version 0", 0) \
```

The AST-level reader defaults the *other* way, to `LATEST`
(`src/Parsers/getTimeSeriesSettingVersion.cpp:13-26`) — that is safe only because
`normalizeTimeSeriesDefinitionImpl` has already pinned `version = 0` for anything
loaded from disk.

### Verdict on the claim

> "a definition with no `version` setting is upgraded to version 0 on ATTACH and keeps
> the old column and target names"

**CONFIRMED, with one important qualification.**

- "upgraded to version 0 on ATTACH" — confirmed. The gate is
  `can_convert = (params.mode != LoadingStrictnessLevel::CREATE) || params.is_restore_from_backup`,
  i.e. every path except a genuinely new `CREATE` (so ATTACH, server startup load,
  FORCE_ATTACH/FORCE_RESTORE, and RESTORE-from-backup all convert).
- "keeps the old column name" — confirmed, `time_series` not `samples`, via
  `TimeSeriesColumnNames::getOuterSamples(version)`
  (`src/Storages/TimeSeries/TimeSeriesColumnNames.h:64-68`).
- "keeps the old target name" — confirmed, `METRICS` / `.inner_id.metrics.<uuid>` /
  `metrics` backup folder, via `MIN_WITH_METRIC_FAMILIES_TARGET_NAME`.
- **Qualification:** "keeps the old *definition*" is false in one crucial respect —
  the `id` column's DEFAULT expression is *not* kept (see §0). Version 0 preserves
  *names*, not *identifier semantics*.

---

## 2. The six breaking changes × backward-compat path

| # | Change | Version-0 table still works? | Backward-compat mechanism / fallback |
|---|---|---|---|
| #119225 | `time_series` → `samples` outer column | **YES** | `getOuterSamples(version)` returns `"time_series"` for `version < 3`; used on all 11 read/write/PromQL call sites |
| #119227 | `METRICS` → `METRIC FAMILIES` target | **YES** | 4 version branches (keyword, JSON kind, inner UUID name, backup folder) + parser accepts both keywords unconditionally |
| #118124 | settings renamed to `enable_*` | **YES** (n/a to DDL) | `DECLARE_WITH_ALIAS`; these are *query-level* settings, not table settings — no stored-metadata impact |
| #115441 | new `recent_samples` inner table | **YES** | `convertDefinitionWithoutRecentSamplesTTL()` pins `recent_samples_ttl_seconds = 0`; `convertPrealphaDefinition()` explicitly skips the target |
| #114300 | `tags` column change | **READS yes / WRITES NO** | reads: `timeSeriesTagsToMap` merge (version-agnostic). **writes: no compat path at all — see §0** |
| #119613 | `id_type` setting | **YES** | `settings[version] >= MIN_WITH_ID_TYPE_SETTING` guards both writing and copying the setting; `checkTimeSeriesSettings` rejects it below v2 |

### #119225 — `time_series` → `samples`

`src/Storages/TimeSeries/TimeSeriesColumnNames.h:38-41, 64-68`:

```cpp
    /// The outer column with (timestamp, value) pairs of a time series, also returned by `prometheusQuery` and `prometheusQueryRange`.
    /// It's named `time_series` in tables of versions before 3, see `getOuterSamples`.
    static constexpr const char * Samples = "samples";
    static constexpr const char * TimeSeries = "time_series";
    ...
    /// Returns the name of the outer column with samples for a TimeSeries table of the specified version.
    static constexpr const char * getOuterSamples(UInt64 version)
    {
        return (version >= TimeSeriesVersion::MIN_WITH_SAMPLES_OUTER_COLUMN) ? Samples : TimeSeries;
    }
```

Column generation, `normalizeTimeSeriesDefinitionImpl.cpp:1852-1867`:

```cpp
    /// The name of the column with samples depends on the version of the table (see TimeSeriesVersion.h).
    ColumnsDescription generateOuterColumns(const DataTypePtr & timestamp_type, const DataTypePtr & scalar_type, UInt64 version)
    ...
        add_column(TimeSeriesColumnNames::getOuterSamples(version),
```

Call sites all threaded through `getVersion()`:
`StoragePrometheusQuery.cpp:113`, `StorageTimeSeriesSelector.cpp:139`,
`PrometheusHTTPProtocolAPI.cpp:203,465,526`,
`PrometheusQueryToSQL/finalizeSQL.cpp:305`, `PrometheusQueryToSQL/getResultColumns.cpp:59`,
`PrometheusRemoteWriteProtocol.cpp:328`, `TimeSeriesSink.cpp:437,519,596`,
`makeASTSelectFromTimeSeries.cpp:700`, `normalizeTimeSeriesDefinitionImpl.cpp:2074`.
Fallback name: **`time_series`**. Regression test at PIN:
`tests/queries/0_stateless/05212_timeseries_old_version_time_series_outer_column.sql`.

### #119227 — `METRICS` → `METRIC FAMILIES`

Four separate version branches, all keyed on `MIN_WITH_METRIC_FAMILIES_TARGET_NAME`:

1. Definition keyword — `src/Parsers/ASTViewTargets.cpp:28-31`:
   ```cpp
       Keyword getKeyword(ViewTarget::Kind kind, std::optional<UInt64> time_series_version = {})
       {
           if ((kind == ViewTarget::MetricFamilies) && time_series_version && (*time_series_version < TimeSeriesVersion::MIN_WITH_METRIC_FAMILIES_TARGET_NAME))
               return Keyword::METRICS;
   ```
2. JSON AST kind — `src/Parsers/ASTViewTargets.cpp:483-488`:
   ```cpp
               /// The "metric families" target keeps its old name "Metrics" in the older versions (see readJSON).
               String kind_name = toString(target.kind);
               if ((target.kind == ViewTarget::MetricFamilies) && time_series_version
                   && (*time_series_version < TimeSeriesVersion::MIN_WITH_METRIC_FAMILIES_TARGET_NAME))
                   kind_name = "Metrics";
   ```
3. Inner-UUID map key — `src/Parsers/CreateQueryUUIDs.cpp:162-167`:
   ```cpp
           if ((kind == ViewTarget::MetricFamilies) && time_series_version && (*time_series_version < TimeSeriesVersion::MIN_WITH_METRIC_FAMILIES_TARGET_NAME))
               add_name_and_uuid_to_string("Metrics", inner_uuid);
   ```
4. Inner table name — `src/Storages/TimeSeries/createTimeSeriesInnerTable.cpp:68-76`:
   ```cpp
   String getTimeSeriesInnerTableName(ViewTarget::Kind inner_table_kind, const StorageID & time_series_storage_id, UInt64 version)
   {
       if ((inner_table_kind == ViewTarget::MetricFamilies) && (version < TimeSeriesVersion::MIN_WITH_METRIC_FAMILIES_TARGET_NAME))
           return getTimeSeriesInnerTableName("metrics", time_series_storage_id);
   ```
5. Backup folder — `src/Storages/StorageTimeSeries.cpp:680-683`:
   ```cpp
               /// A table of an older version keeps the folder name "metrics", so an older server can restore the backup.
               if (target_kind == ViewTarget::MetricFamilies && getVersion() < TimeSeriesVersion::MIN_WITH_METRIC_FAMILIES_TARGET_NAME)
                   kind_str = "metrics";
   ```
   plus an unconditional *restore* fallback, `StorageTimeSeries.cpp:707-709`:
   ```cpp
               /// Support backups where the metric families folder was named "metrics" instead of "metricfamilies".
               if (target_kind == ViewTarget::MetricFamilies && !restorer.getBackup()->hasFiles(target_data_path))
                   target_data_path = fs::path{data_path_in_backup} / "metrics";
   ```

Parser accepts both, unconditionally — `src/Parsers/ParserViewTargets.cpp:136-141`:

```cpp
                case ViewTarget::MetricFamilies:
                {
                    parsed |= tryParseViewTarget(kind, Keyword::METRIC_FAMILIES, pos, expected, res);
                    parsed |= tryParseViewTarget(kind, Keyword::METRICS, pos, expected, res);
                    break;
                }
```

Fallbacks: keyword `METRICS`, inner table `.inner_id.metrics.<uuid>`, backup folder
`metrics`, JSON kind `"Metrics"`. Also note the same file, lines 117-122, keeps
`DATA` as an alias for `SAMPLES` — which matters for us (see §4/§5).

### #118124 — settings renamed to `enable_*`

This is **not** a table-schema change. It renames *query-level* settings and moves them
from the EXPERIMENTAL to the PRIVATE_PREVIEW tier. Diff
(`d85afdf4949..af30b32ef31`, `src/Core/Settings.cpp`):

```diff
-    DECLARE(Bool, allow_experimental_time_series_table, false, R"(
+    DECLARE_WITH_ALIAS(Bool, enable_time_series_table, false, R"(
 ...
-)", EXPERIMENTAL) \
+)", PRIVATE_PREVIEW, allow_experimental_time_series_table) \
```

```diff
-    DECLARE_WITH_ALIAS(Bool, allow_experimental_time_series_aggregate_functions, false, R"(
-...
-)", EXPERIMENTAL, allow_experimental_ts_to_grid_aggregate_function) \
+    DECLARE_WITH_ALIAS(Bool, enable_time_series_aggregate_functions, false, R"(
+...
+)", PRIVATE_PREVIEW, allow_experimental_time_series_aggregate_functions, allow_experimental_ts_to_grid_aggregate_function) \
```

The PR also extends `DECLARE_SETTINGS_WITH_ALIAS_TRAITS_` in `src/Core/BaseSettings.h`
to support *two* aliases, so the doubly-renamed aggregate-function setting keeps both
historical names. `src/Core/SettingsChangesHistory.cpp` records both under 26.9.
`promql_database`, `promql_table`, `promql_evaluation_time` and
`time_series_prefer_recent_samples_table` are re-tiered only.

**Version-0 impact: none** — no stored table metadata mentions these. The only Aiven
concern is user profiles / `users.d` XML referencing
`allow_experimental_time_series_table`, and the alias covers that.

### #115441 — new `recent_samples` inner table

Two mechanisms, both keyed on version 0:

`normalizeTimeSeriesDefinitionImpl.cpp:1268-1290`:

```cpp
    /// Whether the create query was made by a version before the recent samples table existed,
    /// i.e. it doesn't record the `recent_samples_ttl_seconds` setting in its SETTINGS clause.
    bool isVersionWithNoRecentSamplesTTL(const ASTCreateQuery & create_query)
    {
        return create_query.storage && !hasExplicitTimeSeriesSettingRecentSamplesTTL(create_query);
    }

    /// Converts a create query made by a version before the `recent_samples_ttl_seconds` setting existed:
    /// records the setting explicitly in the query's SETTINGS clause, so that its value always matches the table.
    void convertDefinitionWithoutRecentSamplesTTL(ASTCreateQuery & create_query)
    {
        /// Normally the setting is pinned to zero: the table was initially created without the recent
        /// samples table, while the absent setting would read as its non-zero default. ...
        UInt64 ttl_to_pin = authored_with_recent_samples ? static_cast<UInt64>(TimeSeriesSettings{}[TimeSeriesSetting::recent_samples_ttl_seconds]) : 0;
        setEngineSettings(*create_query.storage, "recent_samples_ttl_seconds", Field(ttl_to_pin));
    }
```

and `normalizeTimeSeriesDefinitionImpl.cpp:1128-1131`:

```cpp
            /// Prealpha tables predate the recent samples table, so there is nothing to convert for it,
            /// and no RECENT SAMPLES target should be added to an old table's definition.
            if (inner_table_kind == ViewTarget::RecentSamples)
                continue;
```

Fallback behaviour: **the recent-samples table is simply absent** for a version-0
table (`recent_samples_ttl_seconds` pinned to `0`), and the query planner's
`time_series_prefer_recent_samples_table` optimization is skipped. Reads/writes go
straight to the main samples table, as today.

Note: the default TTL is nonzero (`345600` = 4 days,
`src/Storages/TimeSeries/TimeSeriesSettings.cpp:34`), so this pin-to-zero conversion
is load-bearing — without it an attached old table would grow a recent-samples target
it has no inner table for.

### #114300 — `tags` column change

**Reads: compatible, version-agnostically.** `makeASTSelectFromTimeSeries.cpp:263-277`:

```cpp
        else
        {
            /// The full Map: combines the inner `tags` Map, the metric name (as the `__name__` tag), and the tags
            /// that have their own columns via the `tags_to_columns` setting into one Map(String, String),
            /// sorted by tag name with duplicates and empty values removed.
            ASTs args;
            args.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags));
            /// `columns_by_tags` already includes `__name__` -> `metric_name`.
            for (const auto & [tag_name, column_name] : columns_by_tags)
            {
                args.push_back(make_intrusive<ASTLiteral>(tag_name));
                args.push_back(make_intrusive<ASTIdentifier>(column_name));
            }
            tags = makeASTFunction("timeSeriesTagsToMap", std::move(args));
        }
```

`timeSeriesTagsToMap` is idempotent with respect to duplicates, so it yields the same
label set whether `tags` already contains `__name__` / the dedicated tags (new rows) or
not (old rows). Documented at `TimeSeriesColumnNames.h:22-26` and
`StorageTimeSeries.cpp:1210-1214`.

Version-0-specific definition cleanup exists too —
`normalizeTimeSeriesDefinitionImpl.cpp:555-558`:

```cpp
        /// The ephemeral column "all_tags" was used in version 0 for calculating identifiers: it contained all the tags,
        /// while the "tags" column contained only the tags without dedicated columns.
        if (old_settings[TimeSeriesSetting::version] == 0)
            remove_column(TimeSeriesColumnNames::AllTags);
```

and `normalizeTimeSeriesDefinitionImpl.cpp:663-665`:

```cpp
                    /// Version 0 had other canonical expressions, hashing the ephemeral "all_tags" column.
                    return is_version_0;
```

**Writes: NO compat path.** See §0. `use_all_tags_column_to_generate_id` is reduced to
a no-op (`TimeSeriesSettings.cpp:29`: `"Obsolete setting, does nothing."`, default
flipped `true` → `false`), and the `id` DEFAULT is rewritten on ATTACH.

**This is the critical finding.**

### #119613 — `id_type`

New setting, gated both ways so an old server can still read a v0/v1 table:

`src/Storages/TimeSeries/TimeSeriesSettings.cpp:119-123`:

```cpp
    /// A table of an earlier version must be readable by a server which doesn't know the `id_type` setting.
    if ((version < TimeSeriesVersion::MIN_WITH_ID_TYPE_SETTING) && settings[TimeSeriesSetting::id_type].value)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "Setting `id_type` requires `version` to be at least {}, but the table has version {}",
            TimeSeriesVersion::MIN_WITH_ID_TYPE_SETTING, version);
```

`normalizeTimeSeriesDefinitionImpl.cpp:2054-2063`:

```cpp
        /// Record `id_type` and `id_generator` in the SETTINGS clause if they aren't kept in the definition otherwise.
        /// A table pinned to an earlier version is written the way that version did it, without the settings.
        if (create_query.storage && (settings[TimeSeriesSetting::version] >= TimeSeriesVersion::MIN_WITH_ID_TYPE_SETTING))
        {
            ...
            recordIdTypeAndIdGenerator(...);
        }
```

`normalizeTimeSeriesDefinitionImpl.cpp:513-514` (the `AS <other_table>` path):

```cpp
        /// The `id_type` setting exists from version 2 (see TimeSeriesVersion.h), so it isn't copied into a table pinned to an earlier version.
        if (const auto * value = get_new_value("version"); value && (SettingFieldUInt64{*value}.value < TimeSeriesVersion::MIN_WITH_ID_TYPE_SETTING))
```

Fallback for a version-0 table: **the setting is simply never written**, and the `id`
type is resolved from the inner/external tags table columns
(`resolveTimeSeriesTypes`, with the default `UUID` if nothing is declared). Regression
test at PIN: `tests/queries/0_stateless/05182_time_series_id_type_setting.sql`.

---

## 3. Ordering constraint

`git log --merges --format='%h %ci %s' aa5df02..290adb52` filtered for the seven PR
numbers. All seven are real master merge commits inside `branch-point..PIN`; none is an
ancestor of the branch point; none is absent; and there are no backport/cherry-pick
commits mentioning these numbers anywhere in the repo (`git log --all` returns the same
7 lines).

| # | Order | Commit | Date (`%ci`) | Subject |
|---|---|---|---|---|
| 1 | #114300 | `58bb4372e07` | 2026-08-13 16:30:54 +0000 | `Merge pull request #114300 from vitlibar/timeseries-tags-column-all-tags` |
| 2 | #115441 | `06af43bc23b` | 2026-08-22 16:01:04 +0000 | `Merge pull request #115441 from ClickHouse/timeseries-recent-samples-mv` |
| 3 | #111204 | `385506603cb` | 2026-09-05 14:30:39 +0000 | `Merge pull request #111204 from ClickHouse/timeseries-schema-versioning` |
| 4 | #118124 | `af30b32ef31` | 2026-09-08 13:43:54 +0000 | `Merge pull request #118124 from ClickHouse/timeseries-private-preview-tier` |
| 5 | #119613 | `e70aef81069` | 2026-09-13 19:37:11 +0000 | `Merge pull request #119613 from vitlibar/timeseries-id-type-setting-and-normalize-split` |
| 6 | #119225 | `0013337fc84` | 2026-09-14 13:23:28 +0000 | `Merge pull request #119225 from vitlibar/timeseries-rename-samples-column` |
| 7 | #119227 | `b41b780feec` | 2026-09-15 17:18:26 +0000 | `Merge pull request #119227 from vitlibar/timeseries-rename-metrics-to-metric-families` |

The PR *numbers* are not monotonic with merge time: #111204 has the lowest number but
merged third, and #119613 (highest number) merged before #119225/#119227.

Confirmed three independent ways: committer dates, `--first-parent` position on master
(reverse-chronological line positions 2258 → 1708 → 831 → 647 → 200 → 154 → 78), and a
full pairwise `git merge-base --is-ancestor` chain:

```
58bb4372e07 IS ancestor of 06af43bc23b
06af43bc23b IS ancestor of 385506603cb
385506603cb IS ancestor of af30b32ef31
af30b32ef31 IS ancestor of e70aef81069
e70aef81069 IS ancestor of 0013337fc84
0013337fc84 IS ancestor of b41b780feec
```

### Does upstream's order satisfy the asserted constraint?

> "#111204 must land before #119225/#119227/#119613, this is a hard ordering constraint"

**YES — the constraint holds, with 8-10 days of slack in each case.**

- #111204 before #119225? **YES** — 2026-09-05 vs 2026-09-14 (9 days).
- #111204 before #119227? **YES** — 2026-09-05 vs 2026-09-15 (10 days).
- #111204 before #119613? **YES** — 2026-09-05 vs 2026-09-13 (8 days).

And it is a *real* constraint, not just a convention: all three of those PRs express
their compatibility in terms of `TimeSeriesVersion` constants
(`MIN_WITH_SAMPLES_OUTER_COLUMN`, `MIN_WITH_METRIC_FAMILIES_TARGET_NAME`,
`MIN_WITH_ID_TYPE_SETTING`), which do not exist before #111204. Porting any of them
first would mean writing a version-free variant and then rewriting it.

**However, the brief's ordering statement is incomplete in a way that matters.**
#114300 and #115441 landed *before* #111204, i.e. **upstream never wrote
version-guarded compat code for them at all** — their backward compatibility is
retroactive, expressed as "version 0 means: prealpha, or no recent-samples table"
(see the version-history comment in `TimeSeriesVersion.h`). For #115441 that
retrofit is complete (`convertDefinitionWithoutRecentSamplesTTL`). For #114300 it is
**not** (§0). So the true ordering requirement for the Aiven port is stronger:
#114300 must be ported *together with* an Aiven-specific version-0 write path, or the
port must land after a decision to make version-0 tables read-only.

---

## 4. Our current schema (at OURS)

Our tree still carries the **pre-#111204, pre-#114300, pre-#115441 "prealpha"
architecture**. The relevant files are the *old* set —
`TimeSeriesDefinitionNormalizer.{h,cpp}`, `TimeSeriesInnerTablesCreator.{h,cpp}`,
`TimeSeriesColumnsValidator.{h,cpp}` — none of which exist at PIN, where they were
replaced by `normalizeTimeSeriesDefinition*.cpp`, `createTimeSeriesInnerTable.cpp`,
`TimeSeriesVersion.*`, `TimeSeriesSink.*`, `TimeSeriesIDGenerator.*`.

### Inner-table target names

`src/Parsers/ASTViewTargets.h:33-41`:

```cpp
        /// The "data" table for a TimeSeries table, contains time series.
        Data,

        /// The "tags" table for a TimeSeries table, contains identifiers for each combination of a metric name and tags (labels).
        Tags,

        /// The "metrics" table for a TimeSeries table, contains general information (metadata) about metrics.
        Metrics,
```

Supported: **`DATA` / `TAGS` / `METRICS`** only. No `SAMPLES`, no `RECENT SAMPLES`, no
`METRIC FAMILIES`. Registered in the parser at
`src/Parsers/ParserCreateQuery.cpp:870`:

```cpp
            ParserViewTargets({ViewTarget::Data, ViewTarget::Tags, ViewTarget::Metrics}).parse(pos, targets, expected);
```

Keywords per form (`src/Parsers/ASTViewTargets.cpp:268-296`): `DATA tbl` / `TAGS tbl` /
`METRICS tbl`; `DATA ENGINE = ...`; `DATA INNER UUID '...'`.

Inner table names (`src/Storages/TimeSeries/TimeSeriesInnerTablesCreator.cpp:150-152`):

```cpp
        res.table_name = fmt::format(".inner_id.{}.{}", toString(inner_table_kind), time_series_storage_id.uuid);
    ...
        res.table_name = fmt::format(".inner.{}.{}", toString(inner_table_kind), time_series_storage_id.table_name);
```

with `toString` giving `"data"` / `"tags"` / `"metrics"`
(`src/Parsers/ASTViewTargets.cpp:32-34`) — i.e. `.inner_id.data.<uuid>`,
`.inner_id.tags.<uuid>`, `.inner_id.metrics.<uuid>`.

### Outer columns

Flat, not `samples`/`time_series`. Generated by
`TimeSeriesDefinitionNormalizer::addMissingColumns`
(`src/Storages/TimeSeries/TimeSeriesDefinitionNormalizer.cpp:133-260`) in this exact
canonical order (mirrored by `reorderColumns`, lines 57-131):

| column | type | source line |
|---|---|---|
| `id` | `UUID` (+ generated DEFAULT) | 190-191 |
| `timestamp` | `DateTime64(3)` | 193-194 |
| `value` | `Float64` | 199-200 |
| `metric_name` | `LowCardinality(String)` | 203-208 |
| *(one per `tags_to_columns` entry)* | `String` | 210-217 |
| `tags` | `Map(LowCardinality(String), String)` | 219-224 |
| `all_tags` | `Map(String, String)` | 226-231 |
| `min_time` | `Nullable(DateTime64(3))` *(if `store_min_time_and_max_time`)* | 233-242 |
| `max_time` | `Nullable(DateTime64(3))` *(if `store_min_time_and_max_time`)* | 233-242 |
| `metric_family_name` | `String` | 246-247 |
| `type` | `String` | 249-250 |
| `unit` | `String` | 252-253 |
| `help` | `String` | 255-256 |

**`type` and `unit` are plain `String`**, not `LowCardinality(String)`
(`get_string_type()` at line 177 is used for both).

### `id` DEFAULT

`TimeSeriesDefinitionNormalizer.cpp:289-341` (`chooseIDAlgorithm`): arguments are
`metric_name` plus, when `use_all_tags_column_to_generate_id` (default **true**),
`all_tags`; the hash is `sipHash64` for `UInt64`, `sipHash128` for `FixedString(16)`,
`reinterpretAsUUID(sipHash128(...))` for `UUID`,
`reinterpretAsUInt128(sipHash128(...))` for `UInt128`. For the default `UUID` id type
this yields exactly:

```sql
id UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags))
```

### `INNER COLUMNS` clause

**NOT supported.** `grep -rn 'INNER COLUMNS\|InnerColumns\|inner_columns'` over
`src/Parsers/ParserViewTargets.cpp`, `src/Parsers/ASTViewTargets.h`,
`src/Parsers/ASTCreateQuery.h` returns nothing at OURS. Inner-table columns are derived
from the *outer* column list by `TimeSeriesInnerTablesCreator`. There is no
`ParserViewTargets.cpp` distinct target-column grammar at all.

### `SETTINGS` clause

**Supported**, but with only five settings
(`src/Storages/TimeSeries/TimeSeriesSettings.cpp:17-22`):

```cpp
#define LIST_OF_TIME_SERIES_SETTINGS(DECLARE, ALIAS) \
    DECLARE(Map, tags_to_columns, Map{}, "...", 0) \
    DECLARE(Bool, use_all_tags_column_to_generate_id, true, "...", 0) \
    DECLARE(Bool, store_min_time_and_max_time, true, "...", 0) \
    DECLARE(Bool, aggregate_min_time_and_max_time, true, "...", 0) \
    DECLARE(Bool, filter_by_min_time_and_max_time, true, "...", 0) \
```

### Is there any `version` setting?

**NO.** `grep -n 'version' src/Storages/TimeSeries/TimeSeriesSettings.cpp` at OURS
returns nothing. There is no `TimeSeriesVersion.h`, no
`getTimeSeriesSettingVersion.cpp`, no `checkTimeSeriesSettings`. **Every Aiven
production table is, by PIN's definition, a version-0 prealpha table.**

### Concrete CREATE statements our tree accepts

Minimal (from `tests/integration/test_prometheus_protocols/test_different_table_engines.py:137`
and `tests/integration/test_prometheus_protocols/test_write_read.py:96`):

```sql
SET allow_experimental_time_series_table = 1;
CREATE TABLE prometheus ENGINE = TimeSeries;
```

With settings (from `tests/queries/0_stateless/03222_create_timeseries_table.sql:4`
and `test_different_table_engines.py:143`):

```sql
CREATE TABLE 03222_timeseries_table2 ENGINE = TimeSeries
SETTINGS store_min_time_and_max_time = 1, aggregate_min_time_and_max_time = 1;

CREATE TABLE prometheus ENGINE = TimeSeries
SETTINGS tags_to_columns = {'job': 'job', 'instance': 'instance'};
```

Explicit inner engines (from `test_different_table_engines.py:167-172`):

```sql
CREATE TABLE prometheus ENGINE = TimeSeries
DATA ENGINE = MergeTree ORDER BY (id, timestamp)
TAGS ENGINE = AggregatingMergeTree ORDER BY (metric_name, id)
METRICS ENGINE = ReplacingMergeTree ORDER BY metric_family_name;
```

Custom `id` type / generator (from `test_different_table_engines.py:149,155`):

```sql
CREATE TABLE prometheus (id UInt64) ENGINE = TimeSeries;
CREATE TABLE prometheus (id FixedString(16) DEFAULT murmurHash3_128(metric_name, all_tags)) ENGINE = TimeSeries;
```

External targets (from `tests/queries/0_stateless/04410_time_series_referential_dependencies.sql:13-38`,
`test_different_table_engines.py:182-206`, and
`tests/integration/test_aiven_time_series_recovery/test.py:31-52`) — **the canonical
full example our tree accepts today**:

```sql
SET allow_experimental_time_series_table = 1;

CREATE TABLE mydata (id UUID, timestamp DateTime64(3), value Float64)
ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE mytags (
    id UUID,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    max_time SimpleAggregateFunction(max, Nullable(DateTime64(3))))
ENGINE = AggregatingMergeTree ORDER BY (metric_name, id);

CREATE TABLE mymetrics (metric_family_name String, type String, unit String, help String)
ENGINE = ReplacingMergeTree ORDER BY metric_family_name;

CREATE TABLE prometheus ENGINE = TimeSeries
DATA mydata TAGS mytags METRICS mymetrics;
```

Cited test files (all at OURS):
- `tests/queries/0_stateless/03222_create_timeseries_table.sql`
- `tests/queries/0_stateless/04410_time_series_referential_dependencies.sql`
- `tests/integration/test_prometheus_protocols/test_different_table_engines.py`
- `tests/integration/test_prometheus_protocols/test_write_read.py`
- `tests/integration/test_aiven_time_series_recovery/test.py`

### Default inner tables our tree generates

`TimeSeriesDefinitionNormalizer::setInnerEngineByDefault`
(`TimeSeriesDefinitionNormalizer.cpp:415-485`):

- `DATA`: `MergeTree` `ORDER BY (id, timestamp)`
- `TAGS`: `AggregatingMergeTree` (or `ReplacingMergeTree` when
  `aggregate_min_time_and_max_time = 0`), `PRIMARY KEY metric_name`,
  `ORDER BY (metric_name, id)` — plus `min_time, max_time` in the ORDER BY only when
  `store_min_time_and_max_time && !aggregate_min_time_and_max_time`
- `METRICS`: `ReplacingMergeTree` `ORDER BY metric_family_name`

`all_tags` in the inner tags table is EPHEMERAL with an ephemeral default
(`TimeSeriesInnerTablesCreator.cpp:96-100`); `min_time`/`max_time` become
`SimpleAggregateFunction(min|max, Nullable(DateTime64(3)))` when aggregation is on
(`TimeSeriesInnerTablesCreator.cpp:115-119`).

---

## 5. Upstream's migration test

`tests/integration/test_prometheus_protocols/test_upgrade_from_prealpha.py` at PIN,
240 lines. **Does not exist at OURS.**

### Fixture files

| file | exists at PIN? | path | size |
|---|---|---|---|
| `backups/time_series_prealpha.zip` | **YES** | `tests/integration/test_prometheus_protocols/backups/time_series_prealpha.zip` | **7705 bytes** (blob `9a7348163f465f77162da6aede832999ad1ff7d2`) |

**Not present at OURS** — `tests/integration/test_prometheus_protocols/backups/` does
not exist in our tree at all. The Aiven variant must regenerate this zip from an Aiven
build (it embeds a `SHOW CREATE`-style ATTACH query in its metadata, so an upstream zip
would not be a faithful Aiven fixture).

Other configs it needs (all already present at OURS under
`tests/integration/test_prometheus_protocols/configs/`, to be verified):
`configs/prometheus.xml`, `configs/backups_disk.xml`,
`configs/allow_experimental_time_series_table.xml`.
Helper imports: `helpers.database_disk.{get_database_disk_name, write_metadata}`,
`.prometheus_test_utils.{convert_time_series_to_protobuf, send_protobuf_to_remote_write}`.

### Old-schema DDL it writes

```python
# DDL for prealpha inner tables schema.
# The "samples" table was named "data".
# The `type` and `unit` columns were plain `String`, not `LowCardinality(String)`.
# The `INNER COLUMNS` clause was not used.
PREALPHA_DATA_DEF = (
    "(id UUID, timestamp DateTime64(3), value Float64)"
    " ENGINE=MergeTree ORDER BY (id, timestamp)"
)

PREALPHA_TAGS_DEF = (
    "(id UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),"
    " metric_name LowCardinality(String),"
    " tags Map(LowCardinality(String), String),"
    " all_tags Map(String, String) EPHEMERAL,"
    " min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))),"
    " max_time SimpleAggregateFunction(max, Nullable(DateTime64(3))))"
    " ENGINE=AggregatingMergeTree ORDER BY (metric_name, id)"
    " SETTINGS allow_dimensions_outside_sorting_key = 1"
)

PREALPHA_METRICS_DEF = (
    "(metric_family_name String, type String, unit String, help String)"
    " ENGINE=ReplacingMergeTree ORDER BY metric_family_name"
)
```

and the outer column list written into the metadata file:

```python
PREALPHA_COLUMNS = """\
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `timestamp` DateTime64(3),
    `value` Float64,
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `metric_family_name` String,
    `type` String,
    `unit` String,
    `help` String
)"""
```

### How it writes it

Not a backup restore (for the two upgrade tests) — it **hand-forges the metadata file**:

1. `CREATE TABLE prometheus (dummy UInt8) ENGINE=Null` as a placeholder, to harvest a
   real UUID and a real `metadata_path`.
2. Creates the three inner tables *manually by name*: `.inner_id.data.<ts_uuid>`,
   `.inner_id.tags.<ts_uuid>`, `.inner_id.metrics.<ts_uuid>` (Atomic) or
   `.inner.data.prometheus` etc. (Ordinary) — note the kind string **`data`**, not
   `samples`.
3. Inserts `foo` directly into those inner tables (see below).
4. `DETACH TABLE prometheus`, reads `metadata_path` from `system.detached_tables`.
5. `write_metadata(node, prometheus_sql_path, metadata)` — writes raw bytes,
   bypassing the server:
   ```python
   metadata = (
       f"ATTACH TABLE prometheus {uuid_clause}\n"
       f"{time_series_columns}\n"
       f"ENGINE = TimeSeries\n"
       f"{inner_uuid_clause}"
       f"{settings_clause}\n"
   )
   ```
   with `inner_uuid_clause` built from `DATA INNER UUID '...'`,
   `TAGS INNER UUID '...'`, `METRICS INNER UUID '...'` — **old keywords, no `version`
   setting**.
6. `SYSTEM CLEAR DISK METADATA CACHE <disk>` (needed because `write_metadata` bypassed
   the server and the cached file size is stale).
7. `ATTACH TABLE prometheus`.

The pre-upgrade insert (which is where the ID formula is baked in):

```python
def insert_foo_into_prealpha_time_series(data_table, tags_table, metrics_table):
    foo_id = node.query(
        "SELECT reinterpretAsUUID(sipHash128('foo', mapSort(map('__name__', 'foo', 'job', 'prometheus'))))"
    ).strip()
    node.query(f"INSERT INTO `{data_table}` VALUES ('{foo_id}', toDateTime64(1000, 3), 10.0)")
    node.query(
        f"INSERT INTO `{tags_table}` (id, metric_name, tags, all_tags)"
        f" VALUES ('{foo_id}', 'foo', {{'job': 'prometheus'}}, mapSort(map('__name__', 'foo', 'job', 'prometheus')))"
    )
    node.query(f"INSERT INTO `{metrics_table}` VALUES ('foo', 'gauge', 'bytes', 'Foo metric')")
```

### What it asserts

Inside `create_and_fill_prealpha_time_series`, immediately after ATTACH:

```python
    outer_columns = set(node.query(
        "SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'prometheus'"
    ).split())
    assert "time_series" in outer_columns
    assert "id" not in outer_columns
```

i.e. the prealpha flat outer columns were collapsed into the single `time_series`
column (v0 name, not `samples`).

Then, per test:

```python
def check_foo_and_bar():
    result = node.query(
        "SELECT t.metric_name, d.timestamp, d.value"
        " FROM timeSeriesData(prometheus) AS d"
        " JOIN timeSeriesTags(prometheus) AS t ON d.id = t.id"
        " ORDER BY t.metric_name, d.timestamp"
    )
    assert result == TSV([
        ["bar", "1970-01-01 00:33:20.000", "20"],
        ["foo", "1970-01-01 00:16:40.000", "10"],
    ])
```

Three test cases:
- `test_upgrade_from_prealpha` — forge metadata, ATTACH, remote-write `bar`, check both.
- `test_upgrade_from_prealpha_ordinary_db` — same in an `Ordinary` database (exercises
  the `.inner.<kind>.<name>` naming branch).
- `test_restore_from_prealpha` — `RESTORE TABLE default.prometheus FROM
  Disk('backups', 'time_series_prealpha.zip')`, then remote-write `bar`, check both.
  (This is what exercises the `data`/`metrics` backup-folder fallbacks at
  `StorageTimeSeries.cpp:704-709`.)

**What it does NOT assert:** it never re-inserts the *same* series `foo` after the
upgrade. `bar` is a different series. So the ID-formula drift documented in §0 is
invisible to this test. **The Aiven variant must add that case.**

### How close is upstream's prealpha DDL to OURS? — complete difference list

Remarkably close — our tree *is* essentially the prealpha schema. Every difference:

| # | Aspect | Upstream prealpha fixture | OURS (HEAD) | Impact on the Aiven test variant |
|---|---|---|---|---|
| 1 | `all_tags` content at insert time | includes `__name__`: `mapSort(map('__name__','foo','job','prometheus'))` | **excludes** `__name__` (`PrometheusRemoteWriteProtocol.cpp:342-352`); would be `{'job':'prometheus'}` | **Must change.** This is the §0 bug surfacing in the fixture. Aiven's `foo_id` must be `reinterpretAsUUID(sipHash128('foo', map('job','prometheus')))` to represent real production data. |
| 2 | tags-table `SETTINGS` | `allow_dimensions_outside_sorting_key = 1` | setting **does not exist** in our tree (`grep -rn allow_dimensions_outside_sorting_key src/Storages/` → nothing) | **Must drop** the clause, or the `CREATE` fails with UNKNOWN_SETTING. Our tree creates the same shape without it. |
| 3 | `all_tags` in outer columns | present as `Map(String, String) EPHEMERAL` | present, **not** ephemeral in the outer list; ephemeral only in the generated inner tags table (`TimeSeriesInnerTablesCreator.cpp:96-100`) | Cosmetic for a hand-forged metadata file; keep upstream's form since that *is* what our `SHOW CREATE` of the inner table emits. |
| 4 | `min_time`/`max_time` in outer columns | **absent** from `PREALPHA_COLUMNS` | **present** as `Nullable(DateTime64(3))` (`TimeSeriesDefinitionNormalizer.cpp:233-242`, default `store_min_time_and_max_time = true`) | **Must add** to the Aiven `PREALPHA_COLUMNS`, otherwise the fixture is not what our tree writes. (Upstream's omission is tolerated because `convertPrealphaDefinition` regenerates them from the settings.) |
| 5 | tags ORDER BY / PRIMARY KEY | `ORDER BY (metric_name, id)` only | `PRIMARY KEY metric_name` + `ORDER BY (metric_name, id)` (`TimeSeriesDefinitionNormalizer.cpp:447-465`) | Cosmetic — compatible; upstream's simpler form still attaches. |
| 6 | `version` setting in metadata | absent (that is the point) | absent (does not exist) | **No change** — identical, and this is exactly why our tables become version 0. |
| 7 | Inner table kind string | `data` / `tags` / `metrics` | `data` / `tags` / `metrics` (`TimeSeriesInnerTablesCreator.cpp:150-152`) | **No change** — exact match. |
| 8 | Target keywords in ATTACH | `DATA INNER UUID` / `TAGS INNER UUID` / `METRICS INNER UUID` | same (`ASTViewTargets.cpp:294-296`) | **No change** — and PIN still parses them (`ParserViewTargets.cpp:120,139`). |
| 9 | `type` / `unit` types | `String` | `String` (`TimeSeriesDefinitionNormalizer.cpp:249-253`) | **No change** — exact match. |
| 10 | `id` DEFAULT expression text | `reinterpretAsUUID(sipHash128(metric_name, all_tags))` | identical (`TimeSeriesDefinitionNormalizer.cpp:294-336`) | **No change** to the text — but see #1, the *value* differs. |
| 11 | `metric_family_name` column | `metric_family_name String` | same (`TimeSeriesColumnNames.h:32`) | **No change**. PIN renamed the outer concept to `metric_family` but kept `MetricFamilyName` for the target table (`TimeSeriesColumnNames.h:56-57`). |
| 12 | data table ORDER BY / engine | `MergeTree ORDER BY (id, timestamp)` | same (`TimeSeriesDefinitionNormalizer.cpp:421-429`) | **No change**. |
| 13 | metrics table engine | `ReplacingMergeTree ORDER BY metric_family_name` | same (`TimeSeriesDefinitionNormalizer.cpp:472-477`) | **No change**. |
| 14 | `INNER COLUMNS` clause | not used ("The `INNER COLUMNS` clause was not used") | not supported | **No change**. |
| 15 | Backup fixture | `backups/time_series_prealpha.zip` (7705 B) | **missing** | **Must generate** from an Aiven build. |
| 16 | User config file name | `configs/allow_experimental_time_series_table.xml` | present at OURS | verify contents still use the pre-#118124 setting name; the alias means either works. |

### Effort estimate for the Aiven variant

**Small-to-moderate — 4 concrete edits plus one new assertion plus one binary fixture.**
Differences 7-14 (eight of sixteen) are exact matches, which is the good news: our tree
really is upstream's "prealpha". The work is:

1. Fix `all_tags` in `insert_foo_into_prealpha_time_series` to exclude `__name__`
   (difference #1) — and correspondingly compute `foo_id` the Aiven way.
2. Drop `allow_dimensions_outside_sorting_key = 1` (#2).
3. Add `min_time`/`max_time` to `PREALPHA_COLUMNS` (#4).
4. Regenerate `backups/time_series_prealpha.zip` from an Aiven build (#15).
5. **Add the missing test case**: after the upgrade, remote-write the *same* series
   `foo` again and assert it lands under the *same* `id` (i.e. that
   `SELECT uniq(id) FROM timeSeriesTags(prometheus) WHERE metric_name = 'foo'` is 1).
   This test will **FAIL** against a straight port — that is the §0 finding, and this
   assertion is the acceptance gate.

---

## 6. Does #108388 + #115944 subsume `patch-new(N03)`?

### Verdict: **fully subsumed** (upstream is a strict superset)

### What N03 is

Commit `776a227e3cc` — *"patch-new(N03): Register TimeSeries external target tables as
referential dependencies"*. Its entire source change is **16 added lines in one file**,
plus a test:

- `src/Databases/DDLDependencyVisitor.cpp:131-146` (OURS), inside
  `DDLDependencyVisitorData::visitCreateQuery`'s `create.targets->targets` loop: a new
  `else if` branch after the `Kind::To` / `Kind::Inner` branches that, for
  `ViewTarget::Kind::Data | Tags | Metrics` with a non-empty `table_id.table_name`,
  qualifies the name with `current_database` and calls `dependencies.emplace(...)` —
  registering external TimeSeries targets in the **referential** dependency set
  returned by `getDependenciesFromCreateQuery` (`src/Databases/DDLDependencyVisitor.h:26`).
- `tests/queries/0_stateless/04410_time_series_referential_dependencies.{sql,reference}`,
  renamed from upstream's `04409` to dodge a local number collision.

It touches **nothing** in `StorageTimeSeries.cpp`, `DDLLoadingDependencyVisitor.cpp`,
`TablesLoader`, or `DatabaseCatalog::addDependencies`. The commit message itself states
it is `(cherry picked from commit f05c455539ee7b57c25ea1f46ceaf7f9d7d2a5ba)` of PR
#108388, with the sole adaptation being the enum spelling
(`Kind::Samples` → `Kind::Data` on 26.3).

### Upstream

- `34efeff34c3` (merge of **#108388**, 2026-06-25), squash commit `8d196ed8c1d`:
  identical 16-line block, identical comment text, identical test body — differing only
  in `ViewTarget::Kind::Samples` where N03 has `Kind::Data`. Same 3 files, same
  `16 +++ / 2 +++ / 56 +++` diffstat as N03.
- `f377c044bbe` (merge of **#115944**, 2026-08-23), commit `0fdb4291497`: extends the
  *same* condition by one line, `|| target.kind == ViewTarget::Kind::RecentSamples`,
  and extends test `04409` with external-recent-samples DROP/RENAME protection plus an
  inner-recent-samples negative case.
- Later, `e8f39dd6388` (part of #119227) renamed `Kind::Metrics` →
  `Kind::MetricFamilies` in that same condition.

Current PIN state, `src/Databases/DDLDependencyVisitor.cpp:133-149`:

```cpp
else if (target.kind == ViewTarget::Kind::Samples
    || target.kind == ViewTarget::Kind::RecentSamples
    || target.kind == ViewTarget::Kind::Tags
    || target.kind == ViewTarget::Kind::MetricFamilies)
{
    /// External target tables of a TimeSeries table are referential dependencies.
    ...
    dependencies.emplace(std::move(target_name));
}
```

Byte-for-byte the same mechanism and comment as N03, covering all three of N03's kinds
(`Data ≡ Samples`, `Tags`, `Metrics ≡ MetricFamilies`) plus a fourth
(`RecentSamples`) that does not exist in our enum at all
(`src/Parsers/ASTViewTargets.h:33-41` at OURS lists only `Data, Tags, Metrics`).

Nothing in N03 remains unaddressed. Both sides are also identical in what they *don't*
do: `DDLLoadingDependencyVisitor.cpp` has zero TimeSeries/target handling at both OURS
and PIN, and `StorageTimeSeries.cpp` never calls `addDependencies` at either revision —
so N03 never added a *loading*-dependency path, and no such gap is introduced by
dropping it.

### Conflict expectation when the port lands

**N03 will conflict, and must be dropped rather than resolved forward.**

- *Textual*: the Aiven hunk inserts at exactly the anchor (right after
  `mv_to_dependency->table_name = StorageMaterializedView::generateInnerTableName(...)`
  in the `Kind::Inner` branch) where PIN already carries the upstream block. Its
  context line also differs — OURS has
  `else if (target.kind == ViewTarget::Kind::Inner && !create.is_window_view)`, and PIN
  removed WINDOW VIEW in `69f749b5890`, so that guard is gone.
- *Semantic*: even force-applied, N03's `ViewTarget::Kind::Data` / `Kind::Metrics` do
  not compile against PIN's enum, and it would produce a duplicated/dead second branch.
- *Test file*: drop `tests/queries/0_stateless/04410_time_series_referential_dependencies.{sql,reference}`
  too. Upstream's `04409_time_series_referential_dependencies` at PIN contains N03's
  exact test body verbatim plus the recent-samples sections (reference `1/0/0/1/0` vs
  Aiven's `1/0`). Keeping 04410 would be a strict-subset duplicate — it *would* still
  pass, since PIN's parser keeps `Keyword::DATA` and `Keyword::METRICS` as
  compatibility aliases (`src/Parsers/ParserViewTargets.cpp:120,139`) — but it is
  redundant, and the original reason for the `04409 → 04410` rename (number collision)
  is moot because PIN owns 04409 for this very test.

### Side note

N03's message defers a `StorageTimeSeries` constructor null-deref to N04. That is a
separate patch, but for context PIN has restructured that area
(`src/Storages/StorageTimeSeries.cpp:102`, `getTargetTableID`/`tryGetTargetTableID` at
:339/:344, vs OURS' single `getTargetTableId` at :234), so **N04 needs its own
assessment against the rewritten upstream code.**

---

## Appendix: version-related tests at PIN worth porting

- `tests/queries/0_stateless/04612_timeseries_version_setting.sql`
- `tests/queries/0_stateless/04613_timeseries_future_version.sh`
- `tests/queries/0_stateless/05212_timeseries_old_version_time_series_outer_column.sql`
- `tests/queries/0_stateless/05182_time_series_id_type_setting.sql`
- `tests/queries/0_stateless/04409_time_series_referential_dependencies.sql`
  (supersedes Aiven's `04410`)
- `src/Storages/TimeSeries/tests/gtest_normalize_time_series_definition.cpp` (719 lines,
  added by #119613) — the cheapest place to assert version-0 normalization behaviour
  without an integration cluster.
- `tests/integration/test_prometheus_protocols/test_upgrade_from_prealpha.py` (§5)

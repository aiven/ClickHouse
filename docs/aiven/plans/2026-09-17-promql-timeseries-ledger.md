# Reconstructed PR ledger — PromQL / TimeSeries subsystem

| Field | Value |
|---|---|
| BRANCH_POINT | `aa5df024249641558bd2e553166246ab017c1a43` (2026-03-19, `Merge pull request #99717 ...`) |
| PIN (upstream/master) | `290adb52c4ee238c9f3f0b557d6f2ca9bb3b29ff` (2026-09-17, `Merge pull request #114536 ...`) |
| Generated | 2026-09-17 |
| Repository | /home/fedora/ClickHouse (Aiven fork of ClickHouse) |
| Range | `BRANCH_POINT..PIN` (181 days) |

## Method

1. Pathspec: the 31-entry subsystem path set (see appendix) applied verbatim to every `git` invocation.
2. Raw commit counts taken with default history simplification:
   `git log --no-merges --oneline BP..PIN -- <paths>` and the `--merges` variant.
3. **The brief's literal method under-counts.** `git log --merges -- <paths>` only surfaces merges that are
   TREESAME to no parent, so most upstream `Merge pull request #N` commits are hidden and only 145 PR
   numbers are directly visible. `--full-history --merges` overshoots wildly (14,494 merges / 4,952 PR
   numbers) because it disables simplification entirely.
   Instead the ledger was built by *ownership mapping*:
   * dump the whole range graph once (`git rev-list --parents BP..PIN` → 68,106 commits);
   * walk master's first-parent chain (`git rev-list --first-parent BP..PIN` → 8,134 commits, 8,082 merges)
     oldest-first, and attribute every side-branch commit to the first-parent merge that introduced it;
   * map each subsystem-touching non-merge commit to its owning first-parent merge — i.e. to its PR.
   Every one of the owning merges has subject `Merge pull request #N from <branch>`; the PR title is the
   first non-blank line of the merge body.
4. Per-PR statistics are `git diff --numstat <merge>^1 <merge>` (first parent is master, so this is exactly
   the PR's net contribution): `repo-wide files` = all rows, `in-subsystem files` = rows matching the pathspec.
5. Squash-merge detection (`(#NNNNN)` suffix on non-merge subjects) found **7 commits, all bearing `(#99475)`,
   which is a GitHub *issue* number, not a PR.** They belong to PRs #106611 and #112825. Upstream ClickHouse
   does not squash-merge, so this heuristic contributes nothing; #99475 is excluded as a false positive.

## 1. Raw commit counts (brief's literal commands)

```
$ git log --no-merges --oneline $BRANCH_POINT..$PIN -- <paths> | wc -l
527
$ git log --merges   --oneline $BRANCH_POINT..$PIN -- <paths> | wc -l
406
```

Of the 406 merges, only **145** have subject `Merge pull request #N from ...`; the other **261** are
`Merge remote-tracking branch 'origin/master' into <branch>` commits made *inside* PR branches.
Under `--full-history` the same range yields 540 non-merge commits and 14,494 merges. The 13 extra
non-merge commits all belong to the 8 ZERO-NET PRs listed in section 4a and are rebase/long-lived-branch
artifacts (plus one genuine add-then-revert inside #110606).

## 2. PR ledger

**212 distinct PRs** touch the subsystem in the range. Of these, **8** have a net
in-subsystem diff of zero files (see ZERO-NET), leaving **204** PRs with a real net subsystem change.

Columns: PR | merge SHA (short) | merge date | title | in-subsystem files | repo-wide files | group(s), primary first | flags

| PR | SHA | Date | Title | Sub files | Repo files | Groups | Flags |
|---|---|---|---|---:|---:|---|---|
| #100053 | 351afbdc79a | 2026-03-19 | Fix timeseries aggregate functions failing with parallel replicas | 1 | 3 | C |  |
| #98948 | 8f93f9a65b3 | 2026-03-20 | PromQL: Binary operators | 31 | 31 | B, A, C |  |
| #98145 | 19a42826c1b | 2026-03-21 | Memory tracking containers for columns | 1 | 89 | C | INCIDENTAL |
| #99724 | 98e10760bad | 2026-03-23 | Fix signed integer overflow in timeSeriesRange | 1 | 3 | C |  |
| #100427 | 4ff63155608 | 2026-03-27 | PromQL: Add comparison operators | 15 | 15 | B, C |  |
| #101083 | 8d060b77497 | 2026-04-09 | Preserve original parameter types in timeseries aggregate functions | 3 | 6 | C | NET-CANCELLED |
| #101210 | d2bf29ea682 | 2026-04-09 | PromQL: Add aggregation operators | 13 | 13 | B, C |  |
| #101794 | 94895491d44 | 2026-04-10 | feat(promql): Parse Prometheus Query API POST bodies as urlencoded form | 2 | 2 | D |  |
| #102246 | a4066423d47 | 2026-04-10 | Add PromQL compliance test analogue | 2 | 2 | E |  |
| #102358 | ee99e565086 | 2026-04-10 | Revert "Preserve original parameter types in timeseries aggregate functions" | 3 | 6 | C | NET-CANCELLED |
| #102304 | 00ecef1c87c | 2026-04-11 | Native Common Virtuals for Remaining Storage | 6 | 172 | C |  |
| #102425 | 7a838115990 | 2026-04-13 | Added more debug info for PromQL | 3 | 3 | C |  |
| #102505 | 6a6c47690c0 | 2026-04-13 | Make `getInMemoryMetadataPtr` aware of context | 6 | 129 | C |  |
| #102585 | 60b6d7e8ac4 | 2026-04-14 | Remove unused includes from heavy headers to reduce build times | 9 | 295 | C |  |
| #102644 | a07c52dbf17 | 2026-04-15 | Move `virtuals` into `in-memory` metadata | 3 | 138 | C |  |
| #100500 | 42e91651bda | 2026-04-16 | Sanitize query_id to prevent CRLF injection in HTTP response headers | 1 | 4 | D |  |
| #102963 | 7e98ab01ca8 | 2026-04-20 | PromQL: Add aggregation operators limitk, topk, bottomk | 12 | 16 | B, C, A |  |
| #104011 | 8f9408772fb | 2026-05-05 | Fix incorrect introduced_in tags for several functions | 2 | 17 | C |  |
| #103612 | 1c853063681 | 2026-05-08 | ci(promql): Add compliance report comment from integration tests | 2 | 8 | E |  |
| #104248 | 0f78a222106 | 2026-05-09 | Guard private-only includes with #if CLICKHOUSE_CLOUD (small files) | 1 | 29 | C |  |
| #104425 | 13096b36777 | 2026-05-09 | Fix aggregation operator for empty vector | 3 | 3 | B |  |
| #103428 | 9f7bd11cddf | 2026-05-11 | Resubmit: Preserve parameter types in timeseries aggregate functions | 3 | 12 | C | NET-CANCELLED |
| #103223 | 7e5377fae41 | 2026-05-12 | Fix signed overflow in `bucketCount` for timeseries aggregate functions (STID 2508-3c50 / 2508-3f3c) | 6 | 10 | C |  |
| #104045 | d464e47a68d | 2026-05-12 | Simplify initializing prometheus services in tests | 4 | 11 | E |  |
| #104672 | da9c603cd9c | 2026-05-12 | Revert "Resubmit: Preserve parameter types in timeseries aggregate functions" | 3 | 12 | C | NET-CANCELLED |
| #89334 | 7bd0fa28146 | 2026-05-13 | Enable date time input format "best effort" by default | 2 | 37 | C |  |
| #104741 | 8569221ad41 | 2026-05-13 | PromQL: Fix Prometheus query API error handling | 6 | 8 | D, C |  |
| #102269 | c5937c4fd74 | 2026-05-15 | Refactor settings: remove vtable, simplify boilerplate | 1 | 44 | C |  |
| #97494 | 59a0cc6285c | 2026-05-19 | Add tests for PromQL multi-block JSON response serialization | 1 | 1 | E |  |
| #104044 | 6bc012d3c99 | 2026-05-19 | Replace PREWHERE in timeSeriesSelector | 16 | 18 | C |  |
| #104564 | ba4ced7cfa1 | 2026-05-19 | PromQL: Add functions label_replace and label_join | 7 | 9 | B, C |  |
| #105000 | 7e8e36e699a | 2026-05-19 | PromQL: Add set binary operators | 9 | 9 | B, C |  |
| #96338 | a15d26bcbc4 | 2026-05-20 | Add missing `final` specifiers across the codebase | 16 | 651 | C |  |
| #104905 | b00721054f9 | 2026-05-20 | Fix UBSan null deref in StorageTimeSeries constructor | 1 | 3 | C |  |
| #105442 | 2ab3ce2270a | 2026-05-21 | Apply `*WithMemoryTracking` containers to Functions | 7 | 191 | C |  |
| #104812 | 4a0750d1a70 | 2026-05-22 | Preserve parameter types in timeseries aggregate functions, add type annotations | 3 | 15 | C |  |
| #99083 | d7cc235dda8 | 2026-05-26 | Improve TimeSeries table engine | 33 | 52 | C |  |
| #105853 | c6433d30e72 | 2026-05-28 | Regroup ReadSettings (2): nest local-FS, remote-FS and HTTP fields | 1 | 124 | C | INCIDENTAL |
| #100399 | c6183beed95 | 2026-05-31 | Enable clang-tidy check for uninitialized variables | 17 | 1005 | C, A, B |  |
| #100976 | ff8b4f248c8 | 2026-05-31 | More warnings | 7 | 827 | C |  |
| #106096 | ecb53cad7d3 | 2026-06-01 | Do not mark move operations that allocate through memory-tracking containers as noexcept | 1 | 46 | C |  |
| #104563 | cb1182c7062 | 2026-06-02 | Add functions arrayTopK and arrayBottomK | 2 | 6 | B |  |
| #105319 | 345eeda42bb | 2026-06-04 | Fix UBSan signed-integer overflow in `timeSeries*ToGrid` window check | 6 | 8 | C |  |
| #106577 | 0e75fa425c7 | 2026-06-06 | Fix timeSeriesLastToGrid() for out-of-window timestamps | 2 | 6 | C |  |
| #106504 | da698ac3be4 | 2026-06-07 | Fix timeSeriesLastToGrid() for timestamps before start | 1 | 1 | C |  |
| #103477 | 2e8e7f1eac3 | 2026-06-12 | Implement PromQL histogram_quantile function | 4 | 6 | B |  |
| #107364 | 84d36b6f938 | 2026-06-12 | Fix arm_tidy build: initialize out_of_range_value in applyHistogramQuantile | 1 | 1 | B |  |
| #106177 | 7b1a73012e2 | 2026-06-14 | Add embedded documentation for table engines | 1 | 70 | C | INCIDENTAL |
| #107417 | 0577669cbc1 | 2026-06-14 | Fix undefined behavior on a non-finite timestamp in prometheusQuery | 1 | 3 | A |  |
| #106230 | 742b4c3654d | 2026-06-17 | Add MemoryThreadStacks* async metrics for pthread stack accounting | 0 | 11 | - | ZERO-NET |
| #107457 | 2923c19be22 | 2026-06-18 | Use COW-safe `IColumn::mutate` for in-place column mutation on read/build paths | 1 | 10 | C |  |
| #107553 | d90302e03a1 | 2026-06-19 | PromQL: Support specifying database and table name in query parameters | 6 | 6 | D |  |
| #107583 | 8b21fb9e839 | 2026-06-19 | Support renaming a TimeSeries table and fix Digest does not match crash in a Replicated database | 1 | 5 | C |  |
| #106200 | ccd31d5ceb8 | 2026-06-20 | Find bad usage of storage in-memory metadata | 5 | 115 | C |  |
| #104975 | 2415dcec9e7 | 2026-06-22 | feat(promql): Support prefixed /prometheus http_handlers URLs on the main HTTP port. | 11 | 12 | D, C |  |
| #107922 | 7785cf9f600 | 2026-06-22 | Enable more Ruff checks (F401/F403/F405/F541/F841) and clean up tests | 7 | 528 | E |  |
| #108475 | a8b9586c912 | 2026-06-26 | Mask sensitive HTTP query-string parameters in logs | 1 | 9 | D |  |
| #103543 | 1bd7a2eebdf | 2026-06-27 | Add IAggregateFunction::merge self-aliasing chassert (follow-up to #103536) | 3 | 79 | C |  |
| #108796 | d70da4f4137 | 2026-06-30 | Fix server abort on cancelled INSERT into TimeSeries table | 1 | 3 | C |  |
| #106611 | 266a4e57ea6 | 2026-07-01 | fix(promql): Apply Prometheus Query API query_log tracking (#99475) | 6 | 8 | C, D |  |
| #108556 | 7feb1d36736 | 2026-07-01 | Make SQL examples in embedded documentation runnable | 3 | 21 | C |  |
| #107934 | 6f47ce39197 | 2026-07-02 | Fix spurious CHECKSUM_DOESNT_MATCH / Parquet errors when an S3/GCS object is overwritten during a read | 0 | 14 | - | ZERO-NET |
| #106782 | d423b464b4e | 2026-07-05 | Pre-flight DROP size check in CREATE OR REPLACE TABLE | 2 | 26 | C |  |
| #109086 | 8f6285d3d57 | 2026-07-06 | Make precise float parsing the default and honor it in input formats | 1 | 51 | C | INCIDENTAL |
| #108087 | d80f960b3be | 2026-07-07 | Reject AggregatingMergeTree dimensions outside the sorting key | 4 | 18 | C |  |
| #107773 | eb3d62024e7 | 2026-07-10 | Refactor `convertToFullIfNeeded` to avoid issues with recursive `LowCardinality` stripping | 2 | 35 | C |  |
| #108421 | 1a9657a9351 | 2026-07-10 | [DOCS] Document the quantiles* aggregate function variants | 1 | 33 | C |  |
| #109965 | 71dd09b53fd | 2026-07-15 | Fix malformed boxed output in function docs examples | 1 | 5 | C |  |
| #100752 | c3d70ac7ad1 | 2026-07-17 | Enable snappy compression in HTTP interface | 1 | 58 | D | INCIDENTAL |
| #106724 | 6c10ce5b197 | 2026-07-17 | Optimize and simplify timeSeries*ToGrid() aggregation functions | 10 | 23 | C |  |
| #110195 | 9d197f142d0 | 2026-07-18 | Autogenerate component reference documentation from in-code structured docs | 1 | 398 | C | INCIDENTAL |
| #110887 | e7287ebc644 | 2026-07-18 | Fix query result cache for PromQL queries | 3 | 5 | A, C |  |
| #110907 | c6e98584c83 | 2026-07-18 | feat(promql): Support ZSTD compression in the remote-write v1 handler. | 3 | 6 | D |  |
| #110875 | 3782a13d191 | 2026-07-20 | Use `::sort` and `HashMap` in `timeSeries*ToGrid` aggregate functions | 2 | 4 | C |  |
| #111023 | a49ca5d2aea | 2026-07-20 | Add PromQL function `increase` | 4 | 8 | C, B |  |
| #110945 | 83ced69dfbb | 2026-07-27 | Remove Field from constant-expression evaluation (table functions + column-native coercion) | 2 | 20 | C |  |
| #111847 | 65335cc7938 | 2026-07-27 | Docs: update links to split settings pages | 3 | 521 | C |  |
| #112108 | 4b700dcfaf4 | 2026-07-28 | Parallelize query execution in the Prometheus query API | 3 | 3 | C |  |
| #112110 | 2734eda2533 | 2026-07-28 | Add time-series codecs to auto-created TimeSeries samples columns | 2 | 7 | C |  |
| #111872 | b1ac9ed9adf | 2026-07-31 | Fix PromQL operations on instant vectors without tags | 4 | 4 | B |  |
| #112067 | a208bbb94ab | 2026-07-31 | Build the SQL parser standalone: cut abseil, cctz, re2, boost, locale, the settings schema, and optionally formatting and DCL | 1 | 171 | A | INCIDENTAL |
| #112352 | 13ff48c8238 | 2026-07-31 | Support PromQL functions changes() and resets() | 2 | 4 | B |  |
| #112494 | e81a84e377c | 2026-07-31 | Fix PromQL error positions | 2 | 2 | A |  |
| #112720 | 95f68d4857d | 2026-07-31 | tidy: fix `readability-container-contains` | 1 | 48 | C |  |
| #110606 | 1eae5fbda2f | 2026-08-01 | Add part_storage_type column to system.parts | 0 | 10 | - | ZERO-NET |
| #112799 | d7972dc790b | 2026-08-03 | Allow multi-component identifiers in TimeSeries tables | 11 | 15 | C |  |
| #112944 | bffd1f1bc64 | 2026-08-03 | Fix quadratic PromQL parse on inputs with many unrecognized characters | 1 | 3 | A |  |
| #113075 | 06d499d471c | 2026-08-03 | Add TimeSeries settings to set index granularity of inner tables | 5 | 6 | C |  |
| #108923 | 63c7647e93b | 2026-08-04 | Docs: use kebab-case for explicit heading anchors | 1 | 31 | C |  |
| #111871 | f098c3151b7 | 2026-08-04 | Fix PromQL edge cases: out-of-range quantile() phi and histogram_quantile() over series without le | 3 | 3 | B |  |
| #112614 | ed5bb83c34e | 2026-08-04 | Bump `google-protobuf` to v35.1 and `grpc` to v1.83.0 | 1 | 7 | D |  |
| #112954 | e75fe08549a | 2026-08-04 | Reject invalid PromQL duration unit order | 2 | 2 | A |  |
| #113168 | 3e57eeb71d8 | 2026-08-04 | Unwind the `PQT` abbreviation | 64 | 64 | B |  |
| #112360 | 16bd8e4947a | 2026-08-06 | Fix `deriv` to scale its slope by the timestamp tick resolution | 4 | 7 | C, B |  |
| #113580 | 56c386405fd | 2026-08-06 | Memoize consecutive id lookups in `ContextTimeSeriesTagsCollector::getGroupByID` | 1 | 1 | C |  |
| #113587 | d134c6da364 | 2026-08-08 | Reject invalid cross-delimiter escapes in PromQL strings | 2 | 2 | A |  |
| #113656 | bf4fa22d175 | 2026-08-08 | Evaluate PromQL topk/bottomk/limitk with a streaming O(T * k) plan | 4 | 12 | C, B |  |
| #113714 | 7515867daf3 | 2026-08-08 | Fix a test comment | 1 | 1 | E |  |
| #113746 | 117d283ca42 | 2026-08-08 | Fix PromQL modulo with infinite divisors | 2 | 2 | B |  |
| #111672 | 4d50ac19aa2 | 2026-08-09 | Support constant labels for the Prometheus metrics endpoint | 2 | 16 | D |  |
| #111869 | e9bb68ed9fd | 2026-08-09 | Support PromQL date/time functions called without arguments | 4 | 4 | B |  |
| #113203 | 6dcb54e113a | 2026-08-09 | Reject invalid PromQL Unicode surrogate escapes | 2 | 6 | A |  |
| #113971 | 5bf49d37073 | 2026-08-09 | PromQL: Support lookback_delta in Prometheus HTTP API | 4 | 4 | C, D |  |
| #108400 | 64cb4e86be8 | 2026-08-10 | Use `clang-22` | 0 | 16 | - | ZERO-NET |
| #113772 | b9d4064cb88 | 2026-08-10 | PromQL: evaluate subqueries shared by multiple plan steps once | 9 | 11 | B, C |  |
| #113768 | 024f563f011 | 2026-08-12 | PromQL/TimeSeries: use bare samples-table PK columns and timestamp-range-first conditions in selector SQL | 2 | 4 | C |  |
| #114326 | 86e654b8459 | 2026-08-12 | Revert the PromQL topk/limitk streaming plan and its shared-subquery materialization | 12 | 22 | B, C | NET-CANCELLED |
| #113681 | a55ac986a8b | 2026-08-13 | Replace the per-bucket hash map in `timeSeries*ToGrid` with a sorted-append sample array | 4 | 7 | C |  |
| #114300 | 58bb4372e07 | 2026-08-13 | TimeSeries: store all tags in the `tags` column | 13 | 15 | C |  |
| #114323 | f15daa72e90 | 2026-08-13 | Docs: require canonical internal links | 12 | 257 | C |  |
| #114381 | 17d2798b962 | 2026-08-13 | Reject literal LF in ordinary quoted PromQL strings | 3 | 3 | A |  |
| #114409 | e7cd7b6a9b9 | 2026-08-13 | Revert "Revert the PromQL topk/limitk streaming plan and its shared-subquery materialization" | 12 | 22 | B, C | NET-CANCELLED |
| #114131 | d0ae8f4ad90 | 2026-08-14 | Use a continuous primary-key range for whole-metric PromQL selectors of TimeSeries tables | 1 | 4 | C |  |
| #114244 | 72c65d913b1 | 2026-08-14 | PromQL/TimeSeries: use dense indexing for vectorized tag transformations | 1 | 4 | C |  |
| #114261 | 89de3f74c05 | 2026-08-14 | PromQL: run the generated SQL with the analyzer on the HTTP API | 2 | 2 | C |  |
| #114551 | 0b3b91fa026 | 2026-08-14 | Support quoted identifiers in PromQL selectors | 11 | 11 | A |  |
| #114666 | a3e34c2ea25 | 2026-08-15 | Promote ConstantValue to Core with Field-free value accessors | 2 | 17 | C |  |
| #112825 | dfbe4e304b4 | 2026-08-16 | fix(promql): Log Prometheus remote-write inserts in system.query_log. | 5 | 5 | C, D |  |
| #114558 | 369e1aafb1e | 2026-08-16 | Support PromQL @ start() and @ end() modifiers | 19 | 19 | A, B |  |
| #114790 | 0023385074a | 2026-08-16 | Remove Gorilla codec from TimeSeries engine defaults | 2 | 5 | C |  |
| #114815 | d8392cfb8ba | 2026-08-16 | Fix handling of negative timestamps and wide steps in timeSeriesRange | 1 | 5 | C |  |
| #114853 | 4706e4ed879 | 2026-08-16 | Reuse the previous row's result for equal consecutive rows in Set execution | 0 | 5 | - | ZERO-NET |
| #114889 | 586b35da039 | 2026-08-16 | Cheaper per-sample add path in timeSeries*ToGrid aggregate functions | 2 | 3 | C |  |
| #114958 | 84275634fce | 2026-08-16 | docs(promql): Add configuration/usage examples, unsupported functions, and a more defined split between metrics and server. | 1 | 3 | C |  |
| #114997 | 0fc2db3239a | 2026-08-16 | Fix PromQL offset inside range selectors | 1 | 3 | B |  |
| #111870 | 7604e8de0ca | 2026-08-17 | Implement PromQL functions clamp(), clamp_min(), clamp_max() and round() | 8 | 8 | B, C |  |
| #113839 | cc6bdb02d8c | 2026-08-17 | Use typed id maps in the time-series tags collector | 3 | 4 | C |  |
| #114076 | d59f2be34ed | 2026-08-17 | Fix mixed-case PromQL aggregation operators | 2 | 2 | A |  |
| #114886 | 8c7881bfe85 | 2026-08-17 | Replace normalizeParameter with parseTimeSeriesTimestamp/Duration | 2 | 5 | A, C |  |
| #115041 | 4dc67746c7d | 2026-08-17 | Bucket `timeSeries*ToGrid` samples in runs | 4 | 5 | C |  |
| #114548 | dbd70927e1d | 2026-08-18 | Support trailing commas in PromQL grouping labels | 4 | 4 | A |  |
| #115097 | f748f0221cd | 2026-08-18 | Rewrite bucketIndexForTimestamp in 64-bit arithmetic | 6 | 8 | C |  |
| #115267 | 6af29898812 | 2026-08-18 | TimeSeries: fast paths for PromQL instant queries | 3 | 3 | C |  |
| #115224 | a013c79d6ac | 2026-08-20 | Perf: use compact presence masks for PromQL set operators | 7 | 10 | B, C |  |
| #114953 | 941dd98b9d2 | 2026-08-21 | Fix an unkillable hang when dropping a low-sorting-name TimeSeries table | 1 | 4 | C |  |
| #115676 | 0c3dc497fad | 2026-08-21 | Prometheus HTTP API: implement /api/v1/series | 7 | 8 | C, D |  |
| #115682 | 24a632f1ddb | 2026-08-21 | Fix PromQL timestamp and duration overflow at Int64 boundary | 1 | 2 | A |  |
| #115333 | dc2e00698a3 | 2026-08-22 | Allow Map data type for asynchronous metrics | 1 | 19 | D |  |
| #115441 | 06af43bc23b | 2026-08-22 | TimeSeries: recent_samples table for short time ranges | 7 | 28 | C |  |
| #115679 | 30024ffcc4b | 2026-08-23 | Fix PromQL query_range step validation for equal start and end | 2 | 2 | B |  |
| #115688 | 7e40c1e51d9 | 2026-08-23 | Support asynchronous inserts in the Prometheus remote-write protocol | 2 | 3 | C |  |
| #115822 | 5f1109348c8 | 2026-08-23 | PromQL: speed up duplicate series zero scan | 1 | 4 | C |  |
| #115828 | 7e71d77e62e | 2026-08-23 | PromQL: reject vector matching with scalar operands | 1 | 3 | B |  |
| #115920 | 8ea2f14da4d | 2026-08-23 | Consistent handling of duplicate timestamps in timeSeries* aggregate functions | 12 | 17 | C |  |
| #110179 | ed7e57e3470 | 2026-08-24 | Add the `default_session_user` server setting | 2 | 41 | D |  |
| #114545 | 5e979cb043a | 2026-08-24 | Support quoted PromQL grouping labels | 11 | 13 | A |  |
| #115622 | e1fe8d8f3f0 | 2026-08-24 | Implement SELECT query from TimeSeries (part 1) | 8 | 12 | C |  |
| #113024 | 82cfa6a421b | 2026-08-26 | Run the documentation examples in CI | 12 | 390 | C |  |
| #116518 | 38b927aca90 | 2026-08-28 | Prometheus HTTP API: implement /api/v1/metadata | 7 | 8 | C, D |  |
| #116731 | f429c1855c7 | 2026-08-28 | Prometheus HTTP API: implement /api/v1/labels | 4 | 5 | C, D |  |
| #116791 | cb23ea937a2 | 2026-08-30 | Allow switching the key-value asynchronous metrics back to their old names | 2 | 30 | D |  |
| #116932 | 74fbd5d7279 | 2026-08-31 | Parallelize PromQL selector scans by budgeting read streams for decode-heavy columns | 1 | 3 | C |  |
| #117033 | 6f1a3d76a53 | 2026-08-31 | Support LowCardinality identifiers in the TimeSeries table engine | 8 | 13 | C |  |
| #112795 | c064f38bedf | 2026-09-02 | Implement PromQL functions absent and count_values | 9 | 9 | B, C |  |
| #115620 | cda48f97d97 | 2026-09-02 | Perf: reserve TimeSeries tag vectors by row | 1 | 1 | C |  |
| #115687 | 3ffba1ab63d | 2026-09-02 | Perf: fuse TimeSeries tag extraction and NULL map | 3 | 8 | C |  |
| #115922 | 82a79d45460 | 2026-09-02 | TimeSeries: choose inner engine families by default_table_engine | 6 | 19 | C |  |
| #117159 | a54f19b0b71 | 2026-09-02 | Prometheus HTTP API: implement /api/v1/format_query | 3 | 4 | D |  |
| #117160 | 4fec62637cd | 2026-09-02 | Prometheus HTTP API: implement /api/v1/label/<name>/values | 6 | 7 | C, D |  |
| #117168 | 6ff4042e54c | 2026-09-02 | Reclaim the orphaned buildkit cache volume in the docker clean up hook | 0 | 1 | - | ZERO-NET |
| #117340 | f17bffbc7f4 | 2026-09-02 | Support timeSeriesGroupArray in SimpleAggregateFunction | 2 | 5 | C |  |
| #117341 | 403faf8dcdd | 2026-09-02 | Support array of pairs argument in timeSeries*ToGrid functions | 2 | 4 | C |  |
| #117404 | 796431c0d39 | 2026-09-02 | Fix out of bound in timeSeriesGroupArray() | 3 | 5 | C |  |
| #117703 | ece12977510 | 2026-09-02 | Do not sort already sorted samples in timeSeriesGroupArray | 1 | 3 | C |  |
| #115567 | 04d027c74d7 | 2026-09-03 | Perf: format PromQL instant-vector timestamps once per block | 1 | 1 | C |  |
| #115683 | 24872a89e61 | 2026-09-03 | Perf: write sampling keys directly to output column | 3 | 6 | C |  |
| #117632 | a748a454401 | 2026-09-03 | Return retryable HTTP 503 on remote-write async insert flush timeout | 2 | 4 | C |  |
| #117816 | 6e221e5e226 | 2026-09-03 | Fix PromQL results with empty aggregation setting | 3 | 5 | C |  |
| #117827 | 0c195a8bbd5 | 2026-09-03 | Fix width of `timeSeriesPrometheusValueToString` doc example table (breaks "Docs examples" check) | 1 | 1 | C |  |
| #101793 | 7e7e269efac | 2026-09-04 | Fix ALTER TABLE MODIFY TTL with DateTime causing data loss on 32-bit overflow | 0 | 12 | - | ZERO-NET |
| #112353 | 58d6272de4b | 2026-09-04 | Implement PromQL functions sum_over_time(), avg_over_time() and count_over_time() | 7 | 14 | C, B |  |
| #115518 | dab9efce85a | 2026-09-04 | Perf: bulk copy dense values in timeSeriesFromGrid | 1 | 3 | C |  |
| #117536 | 47bc5bf05c2 | 2026-09-04 | Do not allocate from sizes declared in aggregate function states | 0 | 13 | - | ZERO-NET |
| #117693 | 1e20226f529 | 2026-09-04 | Fix ALTER of a TimeSeries table dropping the other settings | 1 | 3 | C |  |
| #117726 | 8458961c4c0 | 2026-09-04 | TimeSeries: stream the samples read in order for direct reads | 3 | 6 | C |  |
| #117839 | 66b16d001ca | 2026-09-04 | Fix `CREATE TABLE ... AS` dropping the settings of a TimeSeries table | 1 | 5 | C |  |
| #118040 | afb5ae2d43d | 2026-09-04 | Clamp `wait_for_async_insert_timeout` in the Prometheus remote-write protocol | 2 | 2 | C |  |
| #118118 | 323d4264788 | 2026-09-04 | Revert "Fix ALTER of a TimeSeries table dropping the other settings" | 1 | 3 | C | NET-CANCELLED |
| #118119 | 37cd60aa292 | 2026-09-04 | Revert "Revert "Fix ALTER of a TimeSeries table dropping the other settings"" | 1 | 3 | C | NET-CANCELLED |
| #111204 | 385506603cb | 2026-09-05 | Add schema versioning to the TimeSeries engine and a version guard to the PromQL layer | 12 | 18 | C, D |  |
| #117902 | bcaec2e0581 | 2026-09-05 | Fix logical error on TRUNCATE of a TimeSeries table in a Replicated database | 1 | 6 | C |  |
| #118189 | 3d792c5573c | 2026-09-06 | Perf: skip redundant timeSeries*ToGrid sample validation | 1 | 2 | C |  |
| #118307 | 384f59b5e2c | 2026-09-06 | PromQL: fuse identical agg(X) op agg(X) pairs into one aggregation pass | 8 | 12 | B |  |
| #118124 | af30b32ef31 | 2026-09-08 | Move TimeSeries and PromQL settings to the private preview tier and rename them to `enable_*` | 5 | 14 | C |  |
| #118517 | 3a3cf120854 | 2026-09-08 | Adjust a copied TimeSeries definition to the settings of the new table | 4 | 16 | C |  |
| #112354 | 5805a6b6cb0 | 2026-09-09 | Implement PromQL functions max_over_time() and min_over_time() | 15 | 23 | C, A, B |  |
| #118025 | 10bec7b8810 | 2026-09-09 | Do not value-initialise the merged flag buffers in batch aggregation | 3 | 11 | C |  |
| #118864 | 93e3ad2adad | 2026-09-09 | Respect join settings and support merge joins in `TimeSeries` | 5 | 8 | C |  |
| #118987 | 3e1d3c133c0 | 2026-09-09 | Update two-stacks thresholds of promql function deriv() | 1 | 4 | C |  |
| #113747 | 1a9e5b0b05d | 2026-09-10 | Fix PromQL sgn() for NaN and negative zero | 2 | 2 | B |  |
| #115841 | f2d582f8e0e | 2026-09-10 | Docs: migrate legacy admonitions to Mintlify components | 5 | 184 | C |  |
| #116629 | c0c69a7fdf1 | 2026-09-10 | Check access rights in the TimeSeries table functions | 6 | 10 | C | NET-CANCELLED |
| #119092 | 7597afabb00 | 2026-09-10 | Do not evaluate the PromQL whole-metric id range for every row | 1 | 5 | C |  |
| #119281 | c7a9fe7c9fb | 2026-09-10 | Revert "Check access rights in the TimeSeries table functions" | 6 | 10 | C | NET-CANCELLED |
| #118132 | 30c98e714f8 | 2026-09-11 | PromQL: optimize empty by() grouping | 1 | 4 | B |  |
| #114250 | c9d36f690be | 2026-09-12 | Fix PromQL stddev and stdvar for large finite values | 2 | 2 | B |  |
| #113586 | f3e044dd902 | 2026-09-13 | Parse PromQL octal numeric literals | 5 | 5 | A |  |
| #117009 | ace35a5495f | 2026-09-13 | Speed up aggregation batches with non-null states | 1 | 9 | C |  |
| #119613 | e70aef81069 | 2026-09-13 | TimeSeries: Add setting id_type, split normalization function to two functions to simplify testing, add new test | 9 | 29 | C |  |
| #119786 | 9cee51555a1 | 2026-09-13 | Forbid to create a table as a TimeSeries target table function | 1 | 3 | C |  |
| #103044 | 6b982cacb08 | 2026-09-14 | Fix race between ALTER and RENAME/EXCHANGE TABLES | 2 | 47 | C |  |
| #113744 | e05544e98b5 | 2026-09-14 | Fix PromQL changes for consecutive NaNs | 2 | 4 | C |  |
| #119225 | 0013337fc84 | 2026-09-14 | TimeSeries: Rename the outer column `time_series` to `samples` | 31 | 63 | C, B |  |
| #119859 | caf26def7b1 | 2026-09-14 | Fix FixedString tags in timeSeriesTagsToMap | 1 | 3 | C |  |
| #112842 | 3a43db4d688 | 2026-09-15 | Implement PromQL range functions present_over_time, absent_over_time, quantile_over_time and predict_linear | 24 | 39 | B, C, A | NET-CANCELLED |
| #118629 | 038ade68ab1 | 2026-09-15 | Deprecate the `enable_analyzer` setting: the analyzer can no longer be disabled | 1 | 441 | E | INCIDENTAL |
| #119211 | 4a10b4a0a58 | 2026-09-15 | Take the pointer after the move, not before | 1 | 5 | A |  |
| #119227 | b41b780feec | 2026-09-15 | TimeSeries: Rename the target METRICS to METRIC FAMILIES | 25 | 47 | C |  |
| #119858 | 19ef773ce1e | 2026-09-15 | Fix: Preserve timezone in timeSeriesRange DateTime64 results | 1 | 3 | C |  |
| #120221 | 0415c4bf179 | 2026-09-15 | docs(promql): Mark the Prometheus HTTP API and TimeSeries engine as private preview in Cloud | 1 | 2 | C |  |
| #120336 | e9ce26a1e6e | 2026-09-16 | Revert "Implement PromQL range functions present_over_time, absent_over_time, quantile_over_time and predict_linear" | 24 | 39 | B, C, A | NET-CANCELLED |
| #120339 | de55bc50653 | 2026-09-16 | Docs: remove Cloud not supported badge on TimeSeries engine | 1 | 1 | C |  |

## 3. Group classification

Rules applied per changed file, then the PR is labelled with every group it hits, ordered by number of
files in that group (primary first). A PR is **E** only if *all* its in-subsystem files are under `tests/`.

| Group | Definition | PRs (primary) | PRs (any) |
|---|---|---:|---:|
| A | PromQL grammar/parser — `src/Parsers/Prometheus/**`, `contrib/antlr4-grammars*` | 18 | 24 |
| B | PromQL→SQL conversion — `src/Storages/TimeSeries/PrometheusQueryToSQL/**` | 31 | 39 |
| C | engine/storage — `StorageTimeSeries*`, `StoragePrometheusQuery*`, `src/Storages/TimeSeries/**` outside PrometheusQueryToSQL, `Functions/TimeSeries`, `AggregateFunctions/TimeSeries`, `ReadFromTimeSeries`, table functions, `ContextTimeSeriesTagsCollector`, `getTimeSeriesSettingVersion` | 134 | 152 |
| D | HTTP layer — `src/Server/Prometheus*` | 14 | 22 |
| E | tests only — `tests/**` and nothing else | 7 | 7 |

(`primary` sums to 204 = the PRs with a real net subsystem change; `any` sums higher because PRs span
groups. For E the two columns are equal by construction — E is exclusive.)

### A — PromQL grammar/parser (primary)

- #107417 2026-06-14 — Fix undefined behavior on a non-finite timestamp in prometheusQuery (sub=1, groups=A)
- #110887 2026-07-18 — Fix query result cache for PromQL queries (sub=3, groups=A/C)
- #112067 2026-07-31 — Build the SQL parser standalone: cut abseil, cctz, re2, boost, locale, the settings schema, and optionally for (sub=1, groups=A)
- #112494 2026-07-31 — Fix PromQL error positions (sub=2, groups=A)
- #112944 2026-08-03 — Fix quadratic PromQL parse on inputs with many unrecognized characters (sub=1, groups=A)
- #112954 2026-08-04 — Reject invalid PromQL duration unit order (sub=2, groups=A)
- #113587 2026-08-08 — Reject invalid cross-delimiter escapes in PromQL strings (sub=2, groups=A)
- #113203 2026-08-09 — Reject invalid PromQL Unicode surrogate escapes (sub=2, groups=A)
- #114381 2026-08-13 — Reject literal LF in ordinary quoted PromQL strings (sub=3, groups=A)
- #114551 2026-08-14 — Support quoted identifiers in PromQL selectors (sub=11, groups=A)
- #114558 2026-08-16 — Support PromQL @ start() and @ end() modifiers (sub=19, groups=A/B)
- #114076 2026-08-17 — Fix mixed-case PromQL aggregation operators (sub=2, groups=A)
- #114886 2026-08-17 — Replace normalizeParameter with parseTimeSeriesTimestamp/Duration (sub=2, groups=A/C)
- #114548 2026-08-18 — Support trailing commas in PromQL grouping labels (sub=4, groups=A)
- #115682 2026-08-21 — Fix PromQL timestamp and duration overflow at Int64 boundary (sub=1, groups=A)
- #114545 2026-08-24 — Support quoted PromQL grouping labels (sub=11, groups=A)
- #113586 2026-09-13 — Parse PromQL octal numeric literals (sub=5, groups=A)
- #119211 2026-09-15 — Take the pointer after the move, not before (sub=1, groups=A)

### B — PromQL→SQL conversion (primary)

- #98948 2026-03-20 — PromQL: Binary operators (sub=31, groups=B/A/C)
- #100427 2026-03-27 — PromQL: Add comparison operators (sub=15, groups=B/C)
- #101210 2026-04-09 — PromQL: Add aggregation operators (sub=13, groups=B/C)
- #102963 2026-04-20 — PromQL: Add aggregation operators limitk, topk, bottomk (sub=12, groups=B/C/A)
- #104425 2026-05-09 — Fix aggregation operator for empty vector (sub=3, groups=B)
- #104564 2026-05-19 — PromQL: Add functions label_replace and label_join (sub=7, groups=B/C)
- #105000 2026-05-19 — PromQL: Add set binary operators (sub=9, groups=B/C)
- #104563 2026-06-02 — Add functions arrayTopK and arrayBottomK (sub=2, groups=B)
- #103477 2026-06-12 — Implement PromQL histogram_quantile function (sub=4, groups=B)
- #107364 2026-06-12 — Fix arm_tidy build: initialize out_of_range_value in applyHistogramQuantile (sub=1, groups=B)
- #111872 2026-07-31 — Fix PromQL operations on instant vectors without tags (sub=4, groups=B)
- #112352 2026-07-31 — Support PromQL functions changes() and resets() (sub=2, groups=B)
- #111871 2026-08-04 — Fix PromQL edge cases: out-of-range quantile() phi and histogram_quantile() over series without le (sub=3, groups=B)
- #113168 2026-08-04 — Unwind the `PQT` abbreviation (sub=64, groups=B)
- #113746 2026-08-08 — Fix PromQL modulo with infinite divisors (sub=2, groups=B)
- #111869 2026-08-09 — Support PromQL date/time functions called without arguments (sub=4, groups=B)
- #113772 2026-08-10 — PromQL: evaluate subqueries shared by multiple plan steps once (sub=9, groups=B/C)
- #114326 2026-08-12 — Revert the PromQL topk/limitk streaming plan and its shared-subquery materialization (sub=12, groups=B/C)
- #114409 2026-08-13 — Revert "Revert the PromQL topk/limitk streaming plan and its shared-subquery materialization" (sub=12, groups=B/C)
- #114997 2026-08-16 — Fix PromQL offset inside range selectors (sub=1, groups=B)
- #111870 2026-08-17 — Implement PromQL functions clamp(), clamp_min(), clamp_max() and round() (sub=8, groups=B/C)
- #115224 2026-08-20 — Perf: use compact presence masks for PromQL set operators (sub=7, groups=B/C)
- #115679 2026-08-23 — Fix PromQL query_range step validation for equal start and end (sub=2, groups=B)
- #115828 2026-08-23 — PromQL: reject vector matching with scalar operands (sub=1, groups=B)
- #112795 2026-09-02 — Implement PromQL functions absent and count_values (sub=9, groups=B/C)
- #118307 2026-09-06 — PromQL: fuse identical agg(X) op agg(X) pairs into one aggregation pass (sub=8, groups=B)
- #113747 2026-09-10 — Fix PromQL sgn() for NaN and negative zero (sub=2, groups=B)
- #118132 2026-09-11 — PromQL: optimize empty by() grouping (sub=1, groups=B)
- #114250 2026-09-12 — Fix PromQL stddev and stdvar for large finite values (sub=2, groups=B)
- #112842 2026-09-15 — Implement PromQL range functions present_over_time, absent_over_time, quantile_over_time and predict_linear (sub=24, groups=B/C/A)
- #120336 2026-09-16 — Revert "Implement PromQL range functions present_over_time, absent_over_time, quantile_over_time and predict_l (sub=24, groups=B/C/A)

### C — engine/storage (primary)

- #100053 2026-03-19 — Fix timeseries aggregate functions failing with parallel replicas (sub=1, groups=C)
- #98145 2026-03-21 — Memory tracking containers for columns (sub=1, groups=C)
- #99724 2026-03-23 — Fix signed integer overflow in timeSeriesRange (sub=1, groups=C)
- #101083 2026-04-09 — Preserve original parameter types in timeseries aggregate functions (sub=3, groups=C)
- #102358 2026-04-10 — Revert "Preserve original parameter types in timeseries aggregate functions" (sub=3, groups=C)
- #102304 2026-04-11 — Native Common Virtuals for Remaining Storage (sub=6, groups=C)
- #102425 2026-04-13 — Added more debug info for PromQL (sub=3, groups=C)
- #102505 2026-04-13 — Make `getInMemoryMetadataPtr` aware of context (sub=6, groups=C)
- #102585 2026-04-14 — Remove unused includes from heavy headers to reduce build times (sub=9, groups=C)
- #102644 2026-04-15 — Move `virtuals` into `in-memory` metadata (sub=3, groups=C)
- #104011 2026-05-05 — Fix incorrect introduced_in tags for several functions (sub=2, groups=C)
- #104248 2026-05-09 — Guard private-only includes with #if CLICKHOUSE_CLOUD (small files) (sub=1, groups=C)
- #103428 2026-05-11 — Resubmit: Preserve parameter types in timeseries aggregate functions (sub=3, groups=C)
- #103223 2026-05-12 — Fix signed overflow in `bucketCount` for timeseries aggregate functions (STID 2508-3c50 / 2508-3f3c) (sub=6, groups=C)
- #104672 2026-05-12 — Revert "Resubmit: Preserve parameter types in timeseries aggregate functions" (sub=3, groups=C)
- #89334 2026-05-13 — Enable date time input format "best effort" by default (sub=2, groups=C)
- #102269 2026-05-15 — Refactor settings: remove vtable, simplify boilerplate (sub=1, groups=C)
- #104044 2026-05-19 — Replace PREWHERE in timeSeriesSelector (sub=16, groups=C)
- #96338 2026-05-20 — Add missing `final` specifiers across the codebase (sub=16, groups=C)
- #104905 2026-05-20 — Fix UBSan null deref in StorageTimeSeries constructor (sub=1, groups=C)
- #105442 2026-05-21 — Apply `*WithMemoryTracking` containers to Functions (sub=7, groups=C)
- #104812 2026-05-22 — Preserve parameter types in timeseries aggregate functions, add type annotations (sub=3, groups=C)
- #99083 2026-05-26 — Improve TimeSeries table engine (sub=33, groups=C)
- #105853 2026-05-28 — Regroup ReadSettings (2): nest local-FS, remote-FS and HTTP fields (sub=1, groups=C)
- #100399 2026-05-31 — Enable clang-tidy check for uninitialized variables (sub=17, groups=C/A/B)
- #100976 2026-05-31 — More warnings (sub=7, groups=C)
- #106096 2026-06-01 — Do not mark move operations that allocate through memory-tracking containers as noexcept (sub=1, groups=C)
- #105319 2026-06-04 — Fix UBSan signed-integer overflow in `timeSeries*ToGrid` window check (sub=6, groups=C)
- #106577 2026-06-06 — Fix timeSeriesLastToGrid() for out-of-window timestamps (sub=2, groups=C)
- #106504 2026-06-07 — Fix timeSeriesLastToGrid() for timestamps before start (sub=1, groups=C)
- #106177 2026-06-14 — Add embedded documentation for table engines (sub=1, groups=C)
- #107457 2026-06-18 — Use COW-safe `IColumn::mutate` for in-place column mutation on read/build paths (sub=1, groups=C)
- #107583 2026-06-19 — Support renaming a TimeSeries table and fix Digest does not match crash in a Replicated database (sub=1, groups=C)
- #106200 2026-06-20 — Find bad usage of storage in-memory metadata (sub=5, groups=C)
- #103543 2026-06-27 — Add IAggregateFunction::merge self-aliasing chassert (follow-up to #103536) (sub=3, groups=C)
- #108796 2026-06-30 — Fix server abort on cancelled INSERT into TimeSeries table (sub=1, groups=C)
- #106611 2026-07-01 — fix(promql): Apply Prometheus Query API query_log tracking (#99475) (sub=6, groups=C/D)
- #108556 2026-07-01 — Make SQL examples in embedded documentation runnable (sub=3, groups=C)
- #106782 2026-07-05 — Pre-flight DROP size check in CREATE OR REPLACE TABLE (sub=2, groups=C)
- #109086 2026-07-06 — Make precise float parsing the default and honor it in input formats (sub=1, groups=C)
- #108087 2026-07-07 — Reject AggregatingMergeTree dimensions outside the sorting key (sub=4, groups=C)
- #107773 2026-07-10 — Refactor `convertToFullIfNeeded` to avoid issues with recursive `LowCardinality` stripping (sub=2, groups=C)
- #108421 2026-07-10 — [DOCS] Document the quantiles* aggregate function variants (sub=1, groups=C)
- #109965 2026-07-15 — Fix malformed boxed output in function docs examples (sub=1, groups=C)
- #106724 2026-07-17 — Optimize and simplify timeSeries*ToGrid() aggregation functions (sub=10, groups=C)
- #110195 2026-07-18 — Autogenerate component reference documentation from in-code structured docs (sub=1, groups=C)
- #110875 2026-07-20 — Use `::sort` and `HashMap` in `timeSeries*ToGrid` aggregate functions (sub=2, groups=C)
- #111023 2026-07-20 — Add PromQL function `increase` (sub=4, groups=C/B)
- #110945 2026-07-27 — Remove Field from constant-expression evaluation (table functions + column-native coercion) (sub=2, groups=C)
- #111847 2026-07-27 — Docs: update links to split settings pages (sub=3, groups=C)
- #112108 2026-07-28 — Parallelize query execution in the Prometheus query API (sub=3, groups=C)
- #112110 2026-07-28 — Add time-series codecs to auto-created TimeSeries samples columns (sub=2, groups=C)
- #112720 2026-07-31 — tidy: fix `readability-container-contains` (sub=1, groups=C)
- #112799 2026-08-03 — Allow multi-component identifiers in TimeSeries tables (sub=11, groups=C)
- #113075 2026-08-03 — Add TimeSeries settings to set index granularity of inner tables (sub=5, groups=C)
- #108923 2026-08-04 — Docs: use kebab-case for explicit heading anchors (sub=1, groups=C)
- #112360 2026-08-06 — Fix `deriv` to scale its slope by the timestamp tick resolution (sub=4, groups=C/B)
- #113580 2026-08-06 — Memoize consecutive id lookups in `ContextTimeSeriesTagsCollector::getGroupByID` (sub=1, groups=C)
- #113656 2026-08-08 — Evaluate PromQL topk/bottomk/limitk with a streaming O(T * k) plan (sub=4, groups=C/B)
- #113971 2026-08-09 — PromQL: Support lookback_delta in Prometheus HTTP API (sub=4, groups=C/D)
- #113768 2026-08-12 — PromQL/TimeSeries: use bare samples-table PK columns and timestamp-range-first conditions in selector SQL (sub=2, groups=C)
- #113681 2026-08-13 — Replace the per-bucket hash map in `timeSeries*ToGrid` with a sorted-append sample array (sub=4, groups=C)
- #114300 2026-08-13 — TimeSeries: store all tags in the `tags` column (sub=13, groups=C)
- #114323 2026-08-13 — Docs: require canonical internal links (sub=12, groups=C)
- #114131 2026-08-14 — Use a continuous primary-key range for whole-metric PromQL selectors of TimeSeries tables (sub=1, groups=C)
- #114244 2026-08-14 — PromQL/TimeSeries: use dense indexing for vectorized tag transformations (sub=1, groups=C)
- #114261 2026-08-14 — PromQL: run the generated SQL with the analyzer on the HTTP API (sub=2, groups=C)
- #114666 2026-08-15 — Promote ConstantValue to Core with Field-free value accessors (sub=2, groups=C)
- #112825 2026-08-16 — fix(promql): Log Prometheus remote-write inserts in system.query_log. (sub=5, groups=C/D)
- #114790 2026-08-16 — Remove Gorilla codec from TimeSeries engine defaults (sub=2, groups=C)
- #114815 2026-08-16 — Fix handling of negative timestamps and wide steps in timeSeriesRange (sub=1, groups=C)
- #114889 2026-08-16 — Cheaper per-sample add path in timeSeries*ToGrid aggregate functions (sub=2, groups=C)
- #114958 2026-08-16 — docs(promql): Add configuration/usage examples, unsupported functions, and a more defined split between metric (sub=1, groups=C)
- #113839 2026-08-17 — Use typed id maps in the time-series tags collector (sub=3, groups=C)
- #115041 2026-08-17 — Bucket `timeSeries*ToGrid` samples in runs (sub=4, groups=C)
- #115097 2026-08-18 — Rewrite bucketIndexForTimestamp in 64-bit arithmetic (sub=6, groups=C)
- #115267 2026-08-18 — TimeSeries: fast paths for PromQL instant queries (sub=3, groups=C)
- #114953 2026-08-21 — Fix an unkillable hang when dropping a low-sorting-name TimeSeries table (sub=1, groups=C)
- #115676 2026-08-21 — Prometheus HTTP API: implement /api/v1/series (sub=7, groups=C/D)
- #115441 2026-08-22 — TimeSeries: recent_samples table for short time ranges (sub=7, groups=C)
- #115688 2026-08-23 — Support asynchronous inserts in the Prometheus remote-write protocol (sub=2, groups=C)
- #115822 2026-08-23 — PromQL: speed up duplicate series zero scan (sub=1, groups=C)
- #115920 2026-08-23 — Consistent handling of duplicate timestamps in timeSeries* aggregate functions (sub=12, groups=C)
- #115622 2026-08-24 — Implement SELECT query from TimeSeries (part 1) (sub=8, groups=C)
- #113024 2026-08-26 — Run the documentation examples in CI (sub=12, groups=C)
- #116518 2026-08-28 — Prometheus HTTP API: implement /api/v1/metadata (sub=7, groups=C/D)
- #116731 2026-08-28 — Prometheus HTTP API: implement /api/v1/labels (sub=4, groups=C/D)
- #116932 2026-08-31 — Parallelize PromQL selector scans by budgeting read streams for decode-heavy columns (sub=1, groups=C)
- #117033 2026-08-31 — Support LowCardinality identifiers in the TimeSeries table engine (sub=8, groups=C)
- #115620 2026-09-02 — Perf: reserve TimeSeries tag vectors by row (sub=1, groups=C)
- #115687 2026-09-02 — Perf: fuse TimeSeries tag extraction and NULL map (sub=3, groups=C)
- #115922 2026-09-02 — TimeSeries: choose inner engine families by default_table_engine (sub=6, groups=C)
- #117160 2026-09-02 — Prometheus HTTP API: implement /api/v1/label/<name>/values (sub=6, groups=C/D)
- #117340 2026-09-02 — Support timeSeriesGroupArray in SimpleAggregateFunction (sub=2, groups=C)
- #117341 2026-09-02 — Support array of pairs argument in timeSeries*ToGrid functions (sub=2, groups=C)
- #117404 2026-09-02 — Fix out of bound in timeSeriesGroupArray() (sub=3, groups=C)
- #117703 2026-09-02 — Do not sort already sorted samples in timeSeriesGroupArray (sub=1, groups=C)
- #115567 2026-09-03 — Perf: format PromQL instant-vector timestamps once per block (sub=1, groups=C)
- #115683 2026-09-03 — Perf: write sampling keys directly to output column (sub=3, groups=C)
- #117632 2026-09-03 — Return retryable HTTP 503 on remote-write async insert flush timeout (sub=2, groups=C)
- #117816 2026-09-03 — Fix PromQL results with empty aggregation setting (sub=3, groups=C)
- #117827 2026-09-03 — Fix width of `timeSeriesPrometheusValueToString` doc example table (breaks "Docs examples" check) (sub=1, groups=C)
- #112353 2026-09-04 — Implement PromQL functions sum_over_time(), avg_over_time() and count_over_time() (sub=7, groups=C/B)
- #115518 2026-09-04 — Perf: bulk copy dense values in timeSeriesFromGrid (sub=1, groups=C)
- #117693 2026-09-04 — Fix ALTER of a TimeSeries table dropping the other settings (sub=1, groups=C)
- #117726 2026-09-04 — TimeSeries: stream the samples read in order for direct reads (sub=3, groups=C)
- #117839 2026-09-04 — Fix `CREATE TABLE ... AS` dropping the settings of a TimeSeries table (sub=1, groups=C)
- #118040 2026-09-04 — Clamp `wait_for_async_insert_timeout` in the Prometheus remote-write protocol (sub=2, groups=C)
- #118118 2026-09-04 — Revert "Fix ALTER of a TimeSeries table dropping the other settings" (sub=1, groups=C)
- #118119 2026-09-04 — Revert "Revert "Fix ALTER of a TimeSeries table dropping the other settings"" (sub=1, groups=C)
- #111204 2026-09-05 — Add schema versioning to the TimeSeries engine and a version guard to the PromQL layer (sub=12, groups=C/D)
- #117902 2026-09-05 — Fix logical error on TRUNCATE of a TimeSeries table in a Replicated database (sub=1, groups=C)
- #118189 2026-09-06 — Perf: skip redundant timeSeries*ToGrid sample validation (sub=1, groups=C)
- #118124 2026-09-08 — Move TimeSeries and PromQL settings to the private preview tier and rename them to `enable_*` (sub=5, groups=C)
- #118517 2026-09-08 — Adjust a copied TimeSeries definition to the settings of the new table (sub=4, groups=C)
- #112354 2026-09-09 — Implement PromQL functions max_over_time() and min_over_time() (sub=15, groups=C/A/B)
- #118025 2026-09-09 — Do not value-initialise the merged flag buffers in batch aggregation (sub=3, groups=C)
- #118864 2026-09-09 — Respect join settings and support merge joins in `TimeSeries` (sub=5, groups=C)
- #118987 2026-09-09 — Update two-stacks thresholds of promql function deriv() (sub=1, groups=C)
- #115841 2026-09-10 — Docs: migrate legacy admonitions to Mintlify components (sub=5, groups=C)
- #116629 2026-09-10 — Check access rights in the TimeSeries table functions (sub=6, groups=C)
- #119092 2026-09-10 — Do not evaluate the PromQL whole-metric id range for every row (sub=1, groups=C)
- #119281 2026-09-10 — Revert "Check access rights in the TimeSeries table functions" (sub=6, groups=C)
- #117009 2026-09-13 — Speed up aggregation batches with non-null states (sub=1, groups=C)
- #119613 2026-09-13 — TimeSeries: Add setting id_type, split normalization function to two functions to simplify testing, add new te (sub=9, groups=C)
- #119786 2026-09-13 — Forbid to create a table as a TimeSeries target table function (sub=1, groups=C)
- #103044 2026-09-14 — Fix race between ALTER and RENAME/EXCHANGE TABLES (sub=2, groups=C)
- #113744 2026-09-14 — Fix PromQL changes for consecutive NaNs (sub=2, groups=C)
- #119225 2026-09-14 — TimeSeries: Rename the outer column `time_series` to `samples` (sub=31, groups=C/B)
- #119859 2026-09-14 — Fix FixedString tags in timeSeriesTagsToMap (sub=1, groups=C)
- #119227 2026-09-15 — TimeSeries: Rename the target METRICS to METRIC FAMILIES (sub=25, groups=C)
- #119858 2026-09-15 — Fix: Preserve timezone in timeSeriesRange DateTime64 results (sub=1, groups=C)
- #120221 2026-09-15 — docs(promql): Mark the Prometheus HTTP API and TimeSeries engine as private preview in Cloud (sub=1, groups=C)
- #120339 2026-09-16 — Docs: remove Cloud not supported badge on TimeSeries engine (sub=1, groups=C)

### D — HTTP layer (primary)

- #101794 2026-04-10 — feat(promql): Parse Prometheus Query API POST bodies as urlencoded form (sub=2, groups=D)
- #100500 2026-04-16 — Sanitize query_id to prevent CRLF injection in HTTP response headers (sub=1, groups=D)
- #104741 2026-05-13 — PromQL: Fix Prometheus query API error handling (sub=6, groups=D/C)
- #107553 2026-06-19 — PromQL: Support specifying database and table name in query parameters (sub=6, groups=D)
- #104975 2026-06-22 — feat(promql): Support prefixed /prometheus http_handlers URLs on the main HTTP port. (sub=11, groups=D/C)
- #108475 2026-06-26 — Mask sensitive HTTP query-string parameters in logs (sub=1, groups=D)
- #100752 2026-07-17 — Enable snappy compression in HTTP interface (sub=1, groups=D)
- #110907 2026-07-18 — feat(promql): Support ZSTD compression in the remote-write v1 handler. (sub=3, groups=D)
- #112614 2026-08-04 — Bump `google-protobuf` to v35.1 and `grpc` to v1.83.0 (sub=1, groups=D)
- #111672 2026-08-09 — Support constant labels for the Prometheus metrics endpoint (sub=2, groups=D)
- #115333 2026-08-22 — Allow Map data type for asynchronous metrics (sub=1, groups=D)
- #110179 2026-08-24 — Add the `default_session_user` server setting (sub=2, groups=D)
- #116791 2026-08-30 — Allow switching the key-value asynchronous metrics back to their old names (sub=2, groups=D)
- #117159 2026-09-02 — Prometheus HTTP API: implement /api/v1/format_query (sub=3, groups=D)

### E — tests only (primary)

- #102246 2026-04-10 — Add PromQL compliance test analogue (sub=2, groups=E)
- #103612 2026-05-08 — ci(promql): Add compliance report comment from integration tests (sub=2, groups=E)
- #104045 2026-05-12 — Simplify initializing prometheus services in tests (sub=4, groups=E)
- #97494 2026-05-19 — Add tests for PromQL multi-block JSON response serialization (sub=1, groups=E)
- #107922 2026-06-22 — Enable more Ruff checks (F401/F403/F405/F541/F841) and clean up tests (sub=7, groups=E)
- #113714 2026-08-08 — Fix a test comment (sub=1, groups=E)
- #118629 2026-09-15 — Deprecate the `enable_analyzer` setting: the analyzer can no longer be disabled (sub=1, groups=E)

## 4a. Incidental PRs

Criterion from the brief: **in-subsystem files <= 2 AND repo-wide files >= 50**. Repo-wide counts come from
`git diff --numstat <merge>^1 <merge> | wc -l`.

| PR | SHA | Date | Repo files | Sub files | In-subsystem file(s) | Title |
|---|---|---|---:|---:|---|---|
| #98145 | 19a42826c1b | 2026-03-21 | 89 | 1 | `src/Functions/TimeSeries/TimeSeriesTagsFunctionHelpers.h` | Memory tracking containers for columns |
| #105853 | c6433d30e72 | 2026-05-28 | 124 | 1 | `src/Storages/StorageTimeSeries.cpp` | Regroup ReadSettings (2): nest local-FS, remote-FS and HTTP fields |
| #106177 | 7b1a73012e2 | 2026-06-14 | 70 | 1 | `src/Storages/StorageTimeSeries.cpp` | Add embedded documentation for table engines |
| #109086 | 8f6285d3d57 | 2026-07-06 | 51 | 1 | `src/AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.cpp` | Make precise float parsing the default and honor it in input formats |
| #100752 | c3d70ac7ad1 | 2026-07-17 | 58 | 1 | `src/Server/PrometheusRequestHandler.cpp` | Enable snappy compression in HTTP interface |
| #110195 | 9d197f142d0 | 2026-07-18 | 398 | 1 | `src/TableFunctions/TableFunctionTimeSeries.cpp` | Autogenerate component reference documentation from in-code structured docs |
| #112067 | a208bbb94ab | 2026-07-31 | 171 | 1 | `src/Parsers/Prometheus/PrometheusQueryParsingUtil.cpp` | Build the SQL parser standalone: cut abseil, cctz, re2, boost, locale, the setti |
| #118629 | 038ade68ab1 | 2026-09-15 | 441 | 1 | `tests/integration/test_prometheus_protocols/test_query_api.py` | Deprecate the `enable_analyzer` setting: the analyzer can no longer be disabled |

**Incidental count: 8** (the brief claims 18 — refuted).

### ZERO-NET PRs (a related but distinct class)

These 8 PRs own a subsystem-touching commit yet their net diff against master (`<merge>^1..<merge>`)
contains **no** in-subsystem file. They are artifacts of long-lived / rebased branches (a commit's diff
against its own parent shows subsystem churn that the merge result does not keep) plus one genuine
add-then-revert inside a single PR. None of them need porting.

| PR | SHA | Date | Repo files | Title | Why zero |
|---|---|---|---:|---|---|
| #106230 | 742b4c3654d | 2026-06-17 | 11 | Add MemoryThreadStacks* async metrics for pthread stack accounting | Long-lived branch; the 2 touched lines in `PrometheusRequestHandler.cpp` are added and removed again within the branch. |
| #107934 | 6f47ce39197 | 2026-07-02 | 14 | Fix spurious CHECKSUM_DOESNT_MATCH / Parquet errors when an S3/GCS obj | Contains a single-parent commit titled `Merge branch 'master' into fix-s3-read-etag-consistency` whose own diff shows `TimeSeriesSink.cpp` churn; net effect on master is nil. |
| #110606 | 1eae5fbda2f | 2026-08-01 | 10 | Add part_storage_type column to system.parts | Contains `Add timeSeriesIrateToGrid/timeSeriesIdeltaToGrid aliases` (668c16762fe) and `Revert "Add timeSeriesIrateToGrid/timeSeriesIdeltaToGrid aliases"` (d7abb29ae12) — a genuine add-then-revert inside the PR. |
| #108400 | 64cb4e86be8 | 2026-08-10 | 16 | Use `clang-22` | `clang-22` branch carried a 1-line tidy fix to `normalizeTimeSeriesDefinition.cpp` that master had already changed; nothing survives in the merge result. |
| #114853 | 4706e4ed879 | 2026-08-16 | 5 | Reuse the previous row's result for equal consecutive rows in Set exec | Rebase artifact — commit diffs touch `StorageTimeSeriesSelector.cpp` (+4/-4 across two commits) with zero net change. |
| #117168 | 6ff4042e54c | 2026-09-02 | 1 | Reclaim the orphaned buildkit cache volume in the docker clean up hook | Rebase artifact of the worst kind: commit 80151ee290a's own diff *deletes* the entire subsystem (branch parent predated it). The merge result keeps master's tree; net subsystem diff is empty. |
| #117536 | 47bc5bf05c2 | 2026-09-04 | 13 | Do not allocate from sizes declared in aggregate function states | Aggregate-function allocation work touched `AggregateFunctionTimeSeriesGroupArray.h` / `AggregateFunctionTimeseriesSamples.h` across three commits that net out to zero. |
| #101793 | 7e7e269efac | 2026-09-04 | 12 | Fix ALTER TABLE MODIFY TTL with DateTime causing data loss on 32-bit o | Very long-lived branch (`Implement a fix for DateTime32 TTL data loss`, 875f410ff58, 2026-04-19); its commit diff replays ~35 subsystem files but the merge keeps master's tree. |

## 4b. Net-cancelled PRs

Found by grepping `Revert` in both non-merge and merge subjects over the pathspec, then mapping each revert
commit to its owning PR and matching branch names of the form `revert-<PR>-<original-branch>`.

Revert chains in the range:

```
#101083 (2026-04-09, land)  --reverted-by-->  #102358 (2026-04-10)      both cancelled
#103428 (2026-05-11, resubmit) --reverted-by--> #104672 (2026-05-12) --re-reverted-by--> #104812 (2026-05-22)
        => #103428 and #104672 cancel; #104812 is the surviving superset (adds type annotations)
#113656 (2026-08-08) + #113772 (2026-08-10)  --reverted-by-->  #114326 (2026-08-12)
        --re-reverted-by--> #114409 (2026-08-13)   => #114326 and #114409 cancel; #113656/#113772 survive
#117693 (2026-09-04, land) --reverted-by--> #118118 (2026-09-04) --re-reverted-by--> #118119 (2026-09-04)
        => #118118 and #118119 cancel; #117693 SURVIVES and must be ported
#116629 (2026-09-10, land) --reverted-by--> #119281 (2026-09-10)        both cancelled
#112842 (2026-09-15, land) --reverted-by--> #120336 (2026-09-16)        both cancelled
```

Every revert above is a byte-exact inverse (insertions and deletions swap), verified with
`git diff --shortstat <merge>^1 <merge> -- <paths>`:

| Pair | original | revert |
|---|---|---|
| #101083 / #102358 | 3 files changed, 8 insertions(+), 14 deletions(-) | 3 files changed, 14 insertions(+), 8 deletions(-) |
| #103428 / #104672 | 3 files changed, 11 insertions(+), 15 deletions(-) | 3 files changed, 15 insertions(+), 11 deletions(-) |
| #114326 / #114409 | 12 files changed, 227 insertions(+), 915 deletions(-) | 12 files changed, 915 insertions(+), 227 deletions(-) |
| #117693 / #118118 | 1 file changed, 7 insertions(+), 1 deletion(-) | 1 file changed, 1 insertion(+), 7 deletions(-) |
| #118118 / #118119 | 1 file changed, 1 insertion(+), 7 deletions(-) | 1 file changed, 7 insertions(+), 1 deletion(-) |
| #116629 / #119281 | 6 files changed, 161 insertions(+), 7 deletions(-) | 6 files changed, 7 insertions(+), 161 deletions(-) |
| #112842 / #120336 | 24 files changed, 2251 insertions(+), 356 deletions(-) | 24 files changed, 356 insertions(+), 2251 deletions(-) |

| PR | SHA | Date | Title | Reason it nets to zero |
|---|---|---|---|---|
| #101083 | 8d060b77497 | 2026-04-09 | Preserve original parameter types in timeseries aggregate functions | Landed 2026-04-09, reverted in full by #102358 the next day (exact inverse diff 8+/14- vs 14+/8-). Superseded by #103428/#104812. |
| #102358 | ee99e565086 | 2026-04-10 | Revert "Preserve original parameter types in timeseries aggregate functions | Pure revert of #101083; cancels it. |
| #103428 | 9f7bd11cddf | 2026-05-11 | Resubmit: Preserve parameter types in timeseries aggregate functions | Resubmit of #101083, reverted in full by #104672 (exact inverse 11+/15- vs 15+/11-); finally superseded by #104812. |
| #104672 | da9c603cd9c | 2026-05-12 | Revert "Resubmit: Preserve parameter types in timeseries aggregate function | Pure revert of #103428; cancels it. |
| #114326 | 86e654b8459 | 2026-08-12 | Revert the PromQL topk/limitk streaming plan and its shared-subquery materi | Pure revert of #113656 + #113772 (12 files, 227+/915-); re-reverted by #114409 (exact inverse 915+/227-). |
| #114409 | e7cd7b6a9b9 | 2026-08-13 | Revert "Revert the PromQL topk/limitk streaming plan and its shared-subquer | Revert-of-revert restoring #113656/#113772; cancels #114326. |
| #118118 | 323d4264788 | 2026-09-04 | Revert "Fix ALTER of a TimeSeries table dropping the other settings" | Pure revert of #117693 (1+/7-); re-reverted by #118119 (7+/1-). |
| #118119 | 37cd60aa292 | 2026-09-04 | Revert "Revert "Fix ALTER of a TimeSeries table dropping the other settings | Revert-of-revert restoring #117693; cancels #118118. |
| #116629 | c0c69a7fdf1 | 2026-09-10 | Check access rights in the TimeSeries table functions | Landed 2026-09-10 (6 files, 161+/7-) and reverted the same day by #119281 (exact inverse 7+/161-). Not present at PIN. |
| #119281 | c7a9fe7c9fb | 2026-09-10 | Revert "Check access rights in the TimeSeries table functions" | Pure revert of #116629; cancels it. |
| #112842 | 3a43db4d688 | 2026-09-15 | Implement PromQL range functions present_over_time, absent_over_time, quant | Landed 2026-09-15 (24 files, 2251+/356-) and reverted 2026-09-16 by #120336 (exact inverse 356+/2251-). Not present at PIN. |
| #120336 | e9ce26a1e6e | 2026-09-16 | Revert "Implement PromQL range functions present_over_time, absent_over_tim | Pure revert of #112842; cancels it. |

**Net-cancelled count: 12** (6 cancelling pairs). The brief claims 9 — refuted.

### Explicit checks requested by the brief

* **#117693 / #118118 / #118119** — confirmed to exist and to form a land → revert → re-revert trio, all
  touching exactly one in-subsystem file (`src/Storages/StorageTimeSeries.cpp`, 1 file / 7 lines).
  **Only two of the three net-cancel (#118118, #118119); #117693 survives at PIN and
  must be ported.** Treating the whole trio as cancelled would be wrong.
* **#120336 reverts #112842** — CONFIRMED. `#120336` branch is `ClickHouse/revert-112842-promql/phase3-range-functions`,
  merged 2026-09-16, and its subsystem diff (24 files, 356+/2251-) is the exact inverse of #112842's
  (24 files, 2251+/356-), merged 2026-09-15. The PromQL range functions `present_over_time`,
  `absent_over_time`, `quantile_over_time` and `predict_linear` are **not** present at PIN.

## 5. Arithmetic

```
candidate PRs touching the subsystem            212
  - zero net in-subsystem diff (artifacts)      -8
  = PRs with a real net subsystem change         204
  - incidental (sub<=2 and repo>=50)            -8
  - net-cancelled (6 revert pairs)              -12
  = PRs to port                                  184
```

### Verdict on the brief's numbers

| Brief claim | Measured | Verdict |
|---|---|---|
| 198 total PRs | 212 candidates / 204 with real net change (145 by the brief's literal `git log --merges` method) | **REFUTED** |
| -18 incidental | 8 | **REFUTED** |
| -9 net-cancelled | 12 | **REFUTED** |
| **171 to port** | **184** | **REFUTED** |
| 201 files changed | 229 | **REFUTED** |
| +24,023 insertions | +32,505 | **REFUTED** |
| -5,820 deletions | -5,956 | **REFUTED** |

No claimed number reproduces. The 171 figure is not reachable from this range with any of the three
counting methods tried (145 merge-visible PRs, 204 with real net change, 212 candidates).

## 6. Aggregate subsystem diffstat

```
$ git diff --stat $BRANCH_POINT $PIN -- <paths> | tail -3
 .../test_prometheus_protocols/test_write_read.py   |   71 +-
 .../update_compliance_baseline.py                  |  205 +
 229 files changed, 32505 insertions(+), 5956 deletions(-)

$ git diff --shortstat $BRANCH_POINT $PIN -- <paths>
 229 files changed, 32505 insertions(+), 5956 deletions(-)
```

## Appendix — pathspec used verbatim

```
src/Storages/TimeSeries
src/Storages/StorageTimeSeries.cpp
src/Storages/StorageTimeSeries.h
src/Storages/StorageTimeSeriesSelector.cpp
src/Storages/StorageTimeSeriesSelector.h
src/Storages/StoragePrometheusQuery.cpp
src/Storages/StoragePrometheusQuery.h
src/Functions/TimeSeries
src/Processors/QueryPlan/ReadFromTimeSeries.cpp
src/Processors/QueryPlan/ReadFromTimeSeries.h
src/Server/PrometheusRequestHandler.cpp
src/Server/PrometheusRequestHandler.h
src/Server/PrometheusRequestHandlerFactory.cpp
src/Server/PrometheusRequestHandlerFactory.h
src/Server/PrometheusRequestHandlerConfig.h
src/TableFunctions/TableFunctionPrometheusQuery.cpp
src/TableFunctions/TableFunctionPrometheusQuery.h
src/TableFunctions/TableFunctionTimeSeries.cpp
src/TableFunctions/TableFunctionTimeSeries.h
src/TableFunctions/TableFunctionTimeSeriesSelector.cpp
src/TableFunctions/TableFunctionTimeSeriesSelector.h
src/Parsers/Prometheus
src/Parsers/getTimeSeriesSettingVersion.cpp
src/Parsers/getTimeSeriesSettingVersion.h
src/AggregateFunctions/TimeSeries
src/AggregateFunctions/AggregateFunctionQuantilePrometheusHistogram.cpp
src/Interpreters/ContextTimeSeriesTagsCollector.cpp
src/Interpreters/ContextTimeSeriesTagsCollector.h
contrib/antlr4-grammars/promql
contrib/antlr4-grammars-cmake/generated
tests/integration/test_prometheus_protocols
```

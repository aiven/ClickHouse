# Patch 006 — replicated-database-attach-with-shard-macro

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `22e03c9d9d6cf9929aec824b724e09ea5c58653f` | Tilman Moeller (author), Aliaksei Khatskevich (committer) | original carry |
| 26.3-aiven | `patch-port(006)` | T3.6 worker (port) + Tilman Moeller (integration test) | byte-equivalent; cherry-pick clean; integration test added (`test_aiven_replicated_database_attach_with_shard_macro`) with verified pre/post evidence pair |

## 1. Purpose

Fixes a `Code: 62. DB::Exception: No macro 'shard'` exception thrown on server
startup when a `DatabaseReplicated` database contains a `ReplicatedMergeTree`
table whose ZooKeeper path includes the `{shard}` macro and the running
config does not provide a global `<shard>` macro fallback. The patch extends
the existing `if (is_replicated_database)` branch in
`TableZnodeInfo::resolve` (`src/Storages/TableZnodeInfo.cpp:58`) to also fire
when `query.attach` is true, so that the loader-issued `ATTACH` operations
during startup populate `info.shard` / `info.replica` from the containing
`DatabaseReplicated` instead of relying solely on the configuration macros.

The "why" is durable: Aiven customers operate `DatabaseReplicated` setups
where the per-database shard name is the authoritative source and the global
`<shard>` is intentionally not set. Without this patch, those customers
cannot restart `clickhouse-server` cleanly.

Source SHA on `v25.8.18.1-lts-aiven`: `22e03c9d9d6cf9929aec824b724e09ea5c58653f`.
Original author: Tilman Moeller <tilman.moeller@aiven.io>, 2025-12-07.
Co-author: Kevin Michel <kevin.michel@aiven.io>.
Committer (25.8 carry): Aliaksei Khatskevich, 2026-03-12.

## 2. Upstream-drift findings

### Commands run

```bash
git log v25.8.18.1-lts..v26.3.10.62-lts --oneline -- src/Storages/TableZnodeInfo.cpp
git log --grep '{shard}' --grep 'shard macro' --grep 'restart.*replicated' \
    --regexp-ignore-case -i v25.8.18.1-lts..v26.3.10.62-lts \
    -- src/Storages/ src/Databases/ src/Interpreters/
rg -n 'TableZnodeInfo::resolve|is_replicated_database|query\.attach' \
    src/Storages/TableZnodeInfo.cpp
rg -n -B 3 -A 10 'if \(is_replicated_database\)' src/Storages/TableZnodeInfo.cpp
```

Logs: `tmp/patch-006/file-history.log`, `tmp/patch-006/upstream-equiv.log`,
`tmp/patch-006/identifier-grep.log`, `tmp/patch-006/hunk-context.log`,
`tmp/patch-006/drift-conclusion.txt`.

### Findings

- `src/Storages/TableZnodeInfo.cpp`: ZERO upstream commits in the
  `v25.8.18.1-lts..v26.3.10.62-lts` range. The file's blob on HEAD
  (`b829b25dfeb3c6f00a4fd9379f7ab0a88dec321b`) is byte-identical to the
  source commit's pre-patch blob; the source's `@@ -55,7 +55,7` hunk applies
  at the same physical lines on HEAD.
- **Deviation from parent's preflight (favorable direction):** parent's
  preflight asserted "3-line drift (source's @@ -55 vs HEAD's @@ -58)". This
  was a hunk-header read error — `@@ -55,7` means "starting at line 55, 7
  lines long"; the change line is at line 58 in BOTH source and HEAD (3
  context lines precede the `-`/`+`). Drift is actually ZERO and
  `byte_equivalent: true` is guaranteed by blob identity, not just expected.
- Upstream-equivalent grep across `src/Storages/`, `src/Databases/`,
  `src/Interpreters/` for `{shard}`, `shard macro`, `restart.*replicated`:
  ZERO matches. No superseding upstream fix.
- `query.attach` is already used at line 28 of the same file
  (`allow_uuid_macro = is_on_cluster || is_replicated_database || query.attach
  || query.has_uuid;`) — the patch literally extends this same idiom one
  line over to the if-condition at line 58. Pre-patch shape `if
  (is_replicated_database)` is intact and identical to source.
- Conclusion: **still-needed-and-applies**. Cherry-pick is clean (Tier 1
  pass), patch-id matches byte-for-byte (Tier 2 pass,
  `108c7f731670ff9ceb744dd951544cb31ada0353` on both sides).

## 3. C++ review

- 1 Lifetime + ownership: `✓` — the patch introduces no new pointer or
  ownership. `query.attach` is a `bool` already in scope from the
  `ASTCreateQuery & query` parameter; `info.shard` / `info.replica` are
  populated from `DatabaseCatalog::instance().getDatabase(...)` which returns
  a `DatabasePtr` (`shared_ptr<IDatabase>`) — that shared ownership is the
  same as the existing pre-patch branch at line 60.
- 2 Exception safety: `✓ with documented limitation` — when post-patch the
  if-block enters via `query.attach=true` on a non-`Replicated` database (an
  edge case the parent's preflight flagged), `getReplicatedDatabaseShardName`
  performs `assert_cast<const DatabaseReplicated *>(database.get())`. In
  release builds `assert_cast` is a `static_cast` (UB on type mismatch); in
  debug / sanitizer builds it calls `throwBadAssertCast`. Pre-patch the same
  scenario threw a clean `Code: 62. DB::Exception: No macro 'shard'` from
  the subsequent `Macros::expand`. The post-patch failure mode for this
  non-`Replicated`-DB edge is therefore different and (in debug builds) less
  informative. Per parent policy call #1 the patch ships as-is; the existing
  precedent at line 28 (`allow_uuid_macro = ... || query.attach || ...`)
  shows `query.attach` is used as a sufficient guard elsewhere in the same
  file without an engine-name check. Future maintenance: if a customer
  encounters this scenario, the analysis is captured here.
- 3 Thread-safety + concurrency: `n/a` — the patch only adds a disjunct to a
  branch in synchronous code; no new lock, no thread-pool work.
- 4 Performance + memory: `✓` — single boolean disjunct on a control path
  that runs at table-init / ATTACH time (cold, not per-row).
- 5 Settings as public API: `n/a` — no setting added or consumed.
- 6 Error handling: `✓ with documented limitation` — see §2 above; the
  documented limitation is the same `assert_cast` concern. The post-patch
  success path uses the existing `Macros::expand` error machinery unchanged
  (error code `NO_ELEMENTS_IN_CONFIG`/`Code: 62` is still thrown when both
  `info.shard` and the config-level fallback are absent — i.e., the patch
  shrinks the trigger surface but does not change the failure error).
- 7 Upstream / vendored code: `✓` — no `contrib/**`, no `.claude/**`, no
  `.github/workflows/**`, no root `AGENTS.md`. The surrounding ClickHouse
  code is byte-identical between 25.8 and 26.3 (file-history empty).
- 8 Behavior under settings: `n/a` — patch is not setting-gated.
- 9 Parent preflight checklist (parent-facing): §9a "ground-truth recount"
  is `n/a` — the patch makes no claim about a population/runtime-registry
  count. §9b "two-namespace confusion" is `n/a` — `is_replicated_database`
  (query-context-derived) and `query.attach` (AST-derived) are different
  categories with no parallel namespace risk.

## 4. Test design

(a) **Integration test added** at
`tests/integration/test_aiven_replicated_database_attach_with_shard_macro/`
(naming per `docs/aiven/runbooks/testing-suites.md` §4.4 — the Aiven
`test_aiven_<slug>/` convention for integration tests).

### Why integration and not stateless

The trigger is the `clickhouse-server` startup loader issuing local
`ATTACH` queries (`query.attach=true`, `INITIAL_QUERY`, no DDL log
involvement) against a `ReplicatedMergeTree` table whose ZK path uses
the `{shard}` macro, on a node whose config does NOT provide a global
`<shard>` macro. Three constraints make this unreachable from
`tests/queries/0_stateless/`:

1. The standard stateless test config always loads
   `tests/config/config.d/macros.xml`, which defines `<shard>s1</shard>`.
   That config-level fallback makes pre-patch macro expansion succeed and
   masks the bug.
2. A single-node setup cannot generate a `SECONDARY_QUERY` execution from
   a `DatabaseReplicated` DDL log (no second replica to consume the entry),
   so the table's znode ends up at the config-shard path, not the DB-shard
   path. Restart-driven ATTACH then matches the config-shard path
   pre-patch and the test passes either way.
3. `SYSTEM RESTART REPLICA` is the closest stateless trigger — it IS a
   local `INITIAL_QUERY` with `create.attach=true` — but the same
   config-shard fallback at point 1 still makes both runs pass.

Three candidate SQL triggers were investigated by the T3.6 worker and
all rejected for the reasons above. See the worker's escalation in
this dossier's pre-revision history (commit prior to the integration
test landing) for the full SQL-trigger analysis.

### The integration test

`tests/integration/test_aiven_replicated_database_attach_with_shard_macro/test.py`:
two nodes, both with `<replica>` macro only (no `<shard>` macro), Keeper
backing, a `DatabaseReplicated` engine with literal shard name `aiven_shard_a`,
a `ReplicatedMergeTree` table created via the default
`default_replica_path = /clickhouse/tables/{uuid}/{shard}` (no explicit
engine args — those are rejected inside a `Replicated` database in 26.3+
without `database_replicated_allow_replicated_engine_arguments`). CREATE
TABLE propagates via the DatabaseReplicated DDL log, so both nodes execute
it as `SECONDARY_QUERY` and the znode lands at
`/clickhouse/tables/<uuid>/aiven_shard_a/`. Insert + cross-node SELECT
verify replication. Both nodes are then restarted via
`node.restart_clickhouse(kill=True)` — the loader-issued ATTACH on
startup is the test target. Final assertion: SELECT returns the row on
both nodes.

### Pre/post evidence pair (VERIFIED 2026-05-26)

Run against the local build directory with the procedure documented in
`docs/aiven/runbooks/integration-tests.md`. Logs in
`tmp/integration-smoke/patch006-{postpatch,prepatch,postpatch-3}.log`.

| Run | Result | Wall-clock |
|---|---|---|
| Post-patch #1 | **PASS** | 41.7 s |
| Pre-patch | **FAIL** | 99.3 s |
| Post-patch #2 (sanity after restore) | **PASS** | 43.9 s |

Pre-patch failure mode (from `node1/logs/clickhouse-server.err.log`):

```
Code: 139. DB::Exception: No macro 'shard' in config while processing
substitutions in '/clickhouse/tables/{uuid}/{shard}' at '27' or macro
is not supported here: Cannot attach table `testdb`.`t` from metadata
file ... from query ATTACH TABLE testdb.t UUID '...' (`x` UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}',
'{replica}') ORDER BY x SETTINGS index_granularity = 8192.
(NO_ELEMENTS_IN_CONFIG)
```

The server's startup load job fails, the python helper's
`restart_clickhouse` then raises `Exception: Cannot start ClickHouse,
see additional info in logs`. This is the customer-reported failure
mode in the exact form they observed.

### Calibration note — error code is `Code: 139`, not `Code: 62`

The T3.6 worker's escalation predicted `Code: 62. DB::Exception: No macro
'shard'`. The empirically-observed error code is `Code: 139.
NO_ELEMENTS_IN_CONFIG` — same root cause (`Macros::expand` cannot find a
matching macro), but the error code path differs. The discrepancy is
minor (both error codes thrown from `Common/Macros.cpp` for the same
no-such-macro failure) and is recorded here so future readers don't
treat the dossier's pre-revision text as ground truth for the error
code value. The integration test's assertion is on the python-side
`Exception: Cannot start ClickHouse`, not on the specific Code value,
so the calibration does not weaken the test.

### Why we use the default `default_replica_path` instead of explicit args

The first draft of the test passed explicit
`ReplicatedMergeTree('/clickhouse/test/.../{uuid}/{shard}', '{replica}')`
arguments to mirror the customer's shape. In 26.3 this is rejected with
`Code: 36. DB::Exception: It's not allowed to specify explicit
zookeeper_path and replica_name for ReplicatedMergeTree arguments in
Replicated database. If you really want to specify them explicitly,
enable setting database_replicated_allow_replicated_engine_arguments.`
The simpler shape `ENGINE = ReplicatedMergeTree ORDER BY x` lets the DB
synthesize the path from `default_replica_path` (default value
`/clickhouse/tables/{uuid}/{shard}` — still includes the `{shard}`
macro the patch addresses) and avoids enabling a setting whose value
isn't load-bearing for the regression we're exercising. The customer's
production deployments may use either shape; both go through the same
`TableZnodeInfo::resolve` codepath on restart, so either reproduces
the bug.

## 5. Rollback considerations

- The patch is a single boolean disjunct on a control path. Reverting it
  reverts to the original `if (is_replicated_database)` shape with no
  schema or on-disk format change. Safe to revert in production if the
  documented `assert_cast` edge case (non-`Replicated` DB + ATTACH +
  `{shard}` in path + global `<shard>` config) is hit in the wild.
- The patch introduces no new state surviving a restart: no new ZK nodes,
  no new on-disk files, no new in-memory caches. It changes only which
  source (DB vs config) populates a transient `MacroExpansionInfo` struct
  during ATTACH.
- No setting can disable the new behavior without rebuilding. If such a
  setting is required, it would be a follow-up Aiven patch.

## 6. Per-uplift notes

### 26.3-aiven (this uplift)

- Cherry-pick was: clean. `git cherry-pick --no-commit -x
  22e03c9d9d6cf9929aec824b724e09ea5c58653f` exited 0 with no conflict
  markers. Staged diff is byte-identical to the source diff (same blob hash
  transition `b829b25dfeb..00b612c781a`, same hunk header `@@ -55,7 +55,7`).
- Upstream-drift conclusion: still-needed-and-applies (see §2).
- Test added at:
  `tests/integration/test_aiven_replicated_database_attach_with_shard_macro/`
  (Aiven integration-test naming convention per
  `docs/aiven/runbooks/testing-suites.md` §4.4). Pre/post evidence pair
  recorded in §4 above; this is the first integration test added during
  the 26.3 uplift and the first verified use of the new
  `test_aiven_<slug>/` convention.
- Time-to-port:
  - T3.6 worker: ~50 minutes for cherry-pick + dossier draft + the
    three-SQL-trigger investigation that produced the "test design
    blocked" escalation.
  - Integration-test follow-up (human, this session): ~45 minutes
    including bringing up the local integration-test infrastructure
    from scratch (8 user-local `pip install`s; `ci/tmp/` cleanup;
    drafting the test; one false-start with explicit ReplicatedMergeTree
    args; pre/post-patch evidence pair verification with two incremental
    rebuilds at ~32 s each).
  - The local integration-test infrastructure itself is now a durable
    runbook at `docs/aiven/runbooks/integration-tests.md` — the next
    integration-test patch will inherit that setup cost.
- Anything surprising:
  - **Pre-patch failure code is `Code: 139`, not `Code: 62`** (the worker
    and parent both predicted Code: 62 in their preflight). The
    integration-test evidence shows
    `NO_ELEMENTS_IN_CONFIG`. The root cause is identical (`Macros::expand`
    cannot find the `shard` macro); the codepath that throws differs.
    The dossier §4 records this as a calibration note for future
    preflights.
  - Parent's preflight asserted a "3-line drift" between source and HEAD
    (`source @@ -55 vs HEAD @@ -58`); empirically HEAD's blob is
    byte-identical to source's pre-patch blob (`b829b25dfeb`) and the
    change line is at line 58 in BOTH (parent appears to have read the
    hunk header `-55,7` as the change-line position rather than the
    hunk-start; the change is 3 context lines into the hunk). Drift is
    actually ZERO and `byte_equivalent: true` is guaranteed by blob
    identity. Favorable-direction discrepancy with no impact on the
    port outcome but noted for future preflight calibration.
  - The first integration-test draft tried explicit ReplicatedMergeTree
    arguments (`'/clickhouse/test/.../{uuid}/{shard}', '{replica}'`)
    and was rejected at CREATE TABLE with `Code: 36. ... It's not
    allowed to specify explicit zookeeper_path and replica_name for
    ReplicatedMergeTree arguments in Replicated database.` 26.3 added
    this guard rail by default; the test was redrafted to use the
    default `default_replica_path` (which still contains `{shard}` and
    therefore still exercises the patch). See §4 "Why we use the default
    `default_replica_path`" for the reasoning.

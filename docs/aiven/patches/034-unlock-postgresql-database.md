# Patch 034 — unlock-postgresql-database

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `b1a99ca92b` | Tilman Moeller (author), 2026-01-02; co-authored by Kevin Michel | (original carry — the version we are porting FROM) |
| 26.3-aiven | `patch-port(034)` (STAGED — HEAD unmoved at `bed2c87dc84`, commit deferred to maintainer) | T-worker, 2026-06-05 | `still-needed-but-rewrite` — original does not apply (file drifted + gained `TSA_REQUIRES` annotations); full faithful refactor + integration test against an unreachable PostgreSQL host. See §2, §3, §6. |

Source SHA on `v25.8.x-lts-aiven`: `b1a99ca92b7d65c8476a97a5e03fbdb320bcf15e`
(2 files, `src/Databases/PostgreSQL/DatabasePostgreSQL.{cpp,h}`, +222/−100).
Original author: Tilman Moeller <tilman.moeller@aiven.io>, 2026-01-02
(co-authored by Kevin Michel <kevin.michel@aiven.io>).

## 1. Purpose / intent

A database using the `PostgreSQL` engine keeps one per-database mutex (the base
`IDatabase::mutex`). Before the patch this mutex was held across *every*
PostgreSQL network call: `pool->get` (the TCP connect) plus
`fetchPostgreSQLTablesList` / `checkPostgresTable` / `fetchPostgreSQLTableStructure`.

The same mutex guards the database name and its comment, which `system.databases`
reads through `IDatabase::getDatabaseComment` and `getCreateDatabaseQuery`. So a
single failing/slow PostgreSQL integration (an unreachable host where the TCP
connect hangs until the connection timeout) makes a `SELECT … FROM system.databases`
block for the full timeout window — it cannot take the per-database mutex that is
pinned by an in-flight PostgreSQL operation. `system.databases` is a critical
monitoring path, so one wedged integration could make the whole instance look
unhealthy.

The `cached` option does not help: it caches table *structure*, not table
*existence*, so list / drop / attach / detach all still run PostgreSQL queries.

The patch reduces the lock scope so that **no PostgreSQL network call ever runs
while the mutex is held**: each method copies the few plain values it needs while
briefly holding the mutex, releases it, performs the PostgreSQL I/O, and only
re-acquires the mutex (re-validating) to mutate shared state.

## 2. Upstream-drift & still-needed evidence

> Mandatory section. Verify the patch is still SEMANTICALLY needed against `v26.3.10.62-lts`.

### Commands / inspection

```bash
git show b1a99ca92b                                   # the source commit (design reference)
git grep -n 'std::lock_guard lock' src/Databases/PostgreSQL/DatabasePostgreSQL.cpp   # HEAD
git grep -n 'TSA_REQUIRES(mutex)' src/Databases/PostgreSQL/DatabasePostgreSQL.h      # HEAD
```

### Findings

- **Problem present on HEAD**: every PostgreSQL-touching method
  (`empty`, `getTablesIterator`, `isTableExist`, `tryGetTable`, the member
  `fetchTable`, `attachTable`, `detachTable`, `dropTable`, `removeOutdatedTables`,
  `getCreateTableQueryImpl`) holds `std::lock_guard lock(mutex)` across
  `pool->get()` + the PostgreSQL query. The lock-coupling regression is real.
- **Not obsoleted-by-upstream**: there is no upstream lock-scope fix; HEAD still
  carries `TSA_REQUIRES(mutex)` on the member `fetchTable` (and on
  `getCreateDatabaseQueryImpl`).
- **Original does not apply**: the file drifted (e.g. `make_intrusive` instead of
  `std::make_shared` for AST nodes; the SSL pool-construction args from patch 021)
  and **gained clang thread-safety annotations** that did not exist in 25.8
  (`base/base/IDatabase.h` now marks `database_name TSA_GUARDED_BY(mutex)`, and the
  PostgreSQL header marks the member `fetchTable` / `getCreateDatabaseQueryImpl`
  `TSA_REQUIRES(mutex)`). A straight cherry-pick conflicts at `DatabasePostgreSQL.h:87`.
- **26.3 DROP-path drift (new finding)**: on 26.3 `DROP DATABASE` for a
  `PostgreSQL` engine no longer iterates tables, because
  `InterpreterDropQuery::executeToDatabaseImpl` guards the table-drop block with
  `if (database->shouldBeEmptyOnDetach())` and `DatabasePostgreSQL::shouldBeEmptyOnDetach`
  returns `false`. So the drop path itself does no PostgreSQL I/O. What still
  couples `DROP DATABASE` to a dead backend pre-patch is `shutdown` →
  `cleaner_task->deactivate`: the background `removeOutdatedTables`, scheduled
  immediately at load, holds the mutex across its connect attempt pre-patch, and
  `deactivate` waits for that in-flight run (~ the connection window). Post-patch
  the cleaner early-exits and never connects, so the drop is immediate. See §4.
- **Conclusion: `still-needed-but-rewrite`** — semantics unchanged; the carry is a
  full faithful refactor reconciled to the 26.3 TSA annotations.

## 3. C++ review

### The lock invariant (the whole point)

**No PostgreSQL network call may execute while `mutex` is held.** Network calls =
`pool->get`, `fetchPostgreSQLTablesList`, `fetchPostgreSQLTableStructure`, and any
`pqxx::nontransaction` / `pqxx::work` against a connection.

### Why reading `configuration` / `pool` outside the lock is safe (invariant)

`configuration`, `pool`, `cache_tables`, `log`, `persistent`, `db_uuid`,
`metadata_path` are all set once in the constructor and never mutated afterwards
(verified by inspection of the whole `.cpp`). They are *not* `TSA_GUARDED_BY`, so
the analyzer permits lock-free reads, and the immutability makes that sound. The
only mutated shared state is `cached_tables` and `detached_or_dropped` (touched
only under the lock), plus the base `database_name` / `comment` (which **are**
`TSA_GUARDED_BY(mutex)`). The refactor copies `database_name` under the lock into a
`local_database_name` everywhere it is needed off-lock — this is both faithful to
the original and required by the 26.3 TSA annotation.

### Copy-under-lock → release → I/O → re-acquire-and-re-validate, per method

| Method | Copied under lock | I/O off-lock | Re-acquire & re-validate / mutate |
|---|---|---|---|
| `empty` | `schema`, `detached_or_dropped` | `pool->get` + `fetchPostgreSQLTablesList` | none (read-only) |
| `getTablesIterator` | `schema`, `database_name`, `on_conflict`, `detached_or_dropped`, `cache_tables`, snapshot of `cached_tables` | `pool->get` + list + per-table `fetchTable` | none (builds a local `Tables`; does not write back to the shared cache — faithful to source) |
| `isTableExist` | `schema`, membership of `detached_or_dropped` | `pool->get` + `checkPostgresTable` | none |
| `tryGetTable` | `schema`, `database_name`, `on_conflict`, dropped-flag, cached entry | `checkPostgresTable` + `fetchTable` | re-lock: insert into `cached_tables` if found & absent; erase if gone |
| `attachTable` | `database`, `schema`, `database_name`, `detached_or_dropped` | `checkPostgresTable` (existence + already-attached checks) | re-lock: set cache, erase from `detached_or_dropped`, remove `.removed` marker |
| `detachTable` | `database`, `schema` | `checkPostgresTable` | re-lock: re-check `detached_or_dropped` (already dropped → throw), erase cache, add to `detached_or_dropped` |
| `dropTable` | `database`, `schema` | `checkPostgresTable` | re-lock: re-check `detached_or_dropped`, write `.removed` marker, erase cache, add to `detached_or_dropped` |
| `removeOutdatedTables` | `schema` (+ schedule next run, early-exit if nothing to reconcile) | `pool->get` + `fetchPostgreSQLTablesList` | re-lock: reconcile `cached_tables` / `detached_or_dropped` against the fetched list |
| `getCreateTableQueryImpl` | `database`, `schema`, `database_name`, `on_conflict`, dropped-flag, cached entry | `checkPostgresTable` + `fetchTable` | re-lock: cache writeback (same as `tryGetTable`) |

The four helpers `getTableNameForLogs`, `formatTableName`, `checkPostgresTable`,
`fetchTable` were converted to free functions taking the values explicitly, so
they need neither class nor lock access.

### TSA reconciliation

- The member `fetchTable(…) const TSA_REQUIRES(mutex)` is **removed** from the
  header (it no longer runs its PostgreSQL I/O under the lock; it is now a free
  function). `getTableNameForLogs`, `formatTableName`, `checkPostgresTable` member
  declarations are likewise removed.
- `getCreateDatabaseQueryImpl() const override TSA_REQUIRES(mutex)` is **left
  unchanged** — it does no PostgreSQL I/O (it reads `database_name` / `comment`
  and is called by the base `getCreateDatabaseQuery` under the lock).
- The full `clickhouse` target builds clean under `-Weverything` (which includes
  `-Wthread-safety`) with `-Werror`: zero warnings/errors for
  `DatabasePostgreSQL.cpp`. The thread-safety analysis is therefore clean for the
  touched code — **no** `TSA_NO_THREAD_SAFETY_ANALYSIS` suppression was used.

### Concurrency hazards considered & how avoided

- **TOCTOU between release and re-acquire**: a table can be dropped/attached
  concurrently while the lock is released. Handled by re-validating under the
  re-acquired lock before mutating (`tryGetTable`/`getCreateTableQueryImpl` only
  write the cache if the entry is still wanted; `detachTable`/`dropTable` re-check
  `detached_or_dropped`). This is the original commit's logic, preserved.
- **`removeOutdatedTables` cleaner**: scheduled *first*, before any early-exit or
  failure, so the periodic task never stops. The error path uses
  `scheduleAfter(reschedule_error_multiplier * cleaner_reschedule_ms, /*overwrite=*/true)`
  to override the just-scheduled normal run with the longer error backoff. The
  fetch runs off-lock (see "deviations"). `cleaner_task->scheduleAfter` is
  internally synchronized, so calling it off-lock is safe.
- **No `sleep`** is used anywhere in the C++ to coordinate; the only ordering
  primitive is the mutex itself.

### Deviations from the original commit (with 26.3-drift justification)

1. **All four helpers placed in an anonymous namespace** (the original made only
   `getTableNameForLogs` `static` and left `formatTableName` / `checkPostgresTable`
   / `fetchTable` with external linkage). `formatTableName` / `checkPostgresTable`
   / `fetchTable` are generic names; an anonymous namespace gives them internal
   linkage and avoids any ODR / link clash. Behaviorally identical.
2. **`removeOutdatedTables` runs the PostgreSQL fetch OFF the lock** (copy `schema`
   under the lock, release, `pool->get` + `fetchPostgreSQLTablesList` without the
   lock, then re-acquire to reconcile). The original commit kept the re-acquired
   `std::lock_guard lock{mutex}` *around* the fetch — i.e. it only added the
   early-exit and early-schedule, but still held the lock during that connect.
   That contradicts the ratified invariant ("no PostgreSQL call under the lock"),
   and `removeOutdatedTables` is exactly one of the methods screened as holding
   the lock during I/O on HEAD. The fetch is therefore moved out of the lock; only
   the reconciliation of `cached_tables` / `detached_or_dropped` runs under the
   re-acquired lock. This is a deliberate strengthening, not a semantic change to
   the reconciliation logic.
3. **`getTablesIterator` cleanup loop preserved verbatim** including the condition
   `if (!table_names.contains(it->first) || !local_detached_or_dropped.contains(it->first))`.
   Read literally this evicts every *non-detached* cached entry from the local
   working copy, so the second loop re-fetches them — i.e. it largely defeats the
   cache *for this call's local copy* but the produced iterator is still correct
   (detached entries are never in `cached_tables` because `detachTable`/`dropTable`
   erase them). Preserved per the "do not invent new semantics" directive; flagged
   here as the one suspicious-looking line carried as-is.
4. **`make_intrusive`** is kept for AST node construction where HEAD uses it (drift
   vs the source's `std::make_shared`); only the surrounding lock structure was
   rewritten.
5. **`detachTable` check order**: the original (and this port) check
   `checkPostgresTable` (PostgreSQL existence) *before* the
   `detached_or_dropped` re-check, whereas HEAD checked `detached_or_dropped`
   first. This is the source commit's reordering, preserved.

## 4. Test design + evidence pair

`tests/integration/test_aiven_postgres_unlock/` — a single ClickHouse node, no
real PostgreSQL. The database points at the **blackhole** address
`10.255.255.1:5432` (a non-routable RFC1918 address where the SYN is silently
dropped, so the TCP connect hangs until the attempt timeout — NOT a refused port,
which returns instantly and would not exercise the hang). Bounded, deterministic
timeouts are passed on `CREATE DATABASE`: `postgresql_connection_attempt_timeout=2`,
`postgresql_connection_pool_retries=3` (window ≈ 6 s),
`postgresql_connection_pool_wait_timeout=2000`. The latency bound is **3.0 s**.

- `test_system_databases_stays_responsive_during_pg_io` — **the deterministic
  flip**. A background `SHOW TABLES FROM <db>` (→ `getTablesIterator`) is launched
  and confirmed in-flight via `system.processes` (event-based, no fixed sleep);
  pre-patch it pins the per-database mutex for the connection window. The
  foreground `SELECT name, comment FROM system.databases` (the `comment` column is
  read under the same mutex) must complete under 3 s. Robust to window variability:
  a longer connect only pushes the pre-patch latency further above the bound, while
  the post-patch read stays in the millisecond range.
- `test_drop_database_with_unreachable_backend_is_prompt` — `DROP DATABASE <db>`
  must complete and remove the database under 3 s. Post-patch this is
  deterministic (drop does no PostgreSQL I/O, cleaner early-exits). Pre-patch it
  additionally flips via the cleaner (`deactivate` waits for the in-flight connect),
  which it did in this run — but that leg is timing-dependent (depends on the
  startup cleaner being mid-connect at drop time); the *deterministic* guarantee is
  the first test.

### Evidence-of-causation pair (worktree-flip of the 2 `src/` files)

- **Post-patch (src == patched): 2 passed** (`build/test_034_postpatch.log`,
  "2 passed in 11.60s"). Foreground `system.databases` read and `DROP DATABASE`
  each completed well under the 3 s bound.
- **Pre-patch (the 2 files restored to HEAD, rebuilt): 2 failed**
  (`build/test_034_prepatch.log`):
  - `system.databases read took 11.91s (bound 3.0s)` — the per-database mutex is
    held across PostgreSQL I/O.
  - `DROP DATABASE … took 5.99s (bound 3.0s)` — the drop is coupled to the
    cleaner's lock-held connect.
- Flip-back was unconditional; after restoring, `git diff --stat` shows exactly the
  2 intended files changed and the patched `clickhouse` was rebuilt clean.

Timeout bound used: **3.0 s**. Observed latencies — post-patch: `system.databases`
~ms, `DROP` <1 s; pre-patch: `system.databases` **11.91 s**, `DROP` **5.99 s**.

## 5. Rollback

- Revert safety: reverting restores the wider lock scope. No on-disk format change,
  no schema migration, no ZooKeeper state, no new setting. Purely an in-process
  locking change.
- Persistent state: none. The `.removed` markers and the metadata directory are
  unchanged in semantics.
- No new public surface (no setting, no error code) to remove.

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — patch first carried on 25.8.

### 25.8-aiven (historical)

Source `b1a99ca92b` (author Tilman Moeller, 2026-01-02; co-authored by Kevin
Michel). Introduced the copy-under-lock-then-release refactor against a
25.8 `DatabasePostgreSQL` that had **no** TSA annotations and a `DROP DATABASE`
path whose behavior differed (the 26.3 `shouldBeEmptyOnDetach`-gated skip is a
26.3 detail).

### 26.3-aiven (this uplift)

- `still-needed-but-rewrite`. Original does not apply (conflict at
  `DatabasePostgreSQL.h:87`; file drifted + TSA annotations added). Full faithful
  refactor of all PostgreSQL-touching methods.
- Reconciled to 26.3 TSA: removed the member helper declarations (incl.
  `fetchTable … TSA_REQUIRES(mutex)`), kept `getCreateDatabaseQueryImpl …
  TSA_REQUIRES(mutex)` (no PostgreSQL I/O). `database_name` is now
  `TSA_GUARDED_BY(mutex)` in the base, so it is copied under the lock everywhere
  (the source already did this; here it is also TSA-mandatory).
- Deviations from the source documented in §3: anonymous-namespace helpers (1);
  `removeOutdatedTables` fetch moved off-lock to honor the invariant (2);
  `getTablesIterator` cleanup loop preserved verbatim (3); `make_intrusive` drift
  kept (4); `detachTable` check order kept as in source (5).
- 26.3 DROP-path drift discovered: `DROP DATABASE` for `PostgreSQL` skips table
  iteration (`shouldBeEmptyOnDetach == false`), so the source's "drop also does
  PostgreSQL queries" rationale applies on 26.3 only via the cleaner's
  `deactivate`. Documented in §2/§4; the integration test reflects it.
- Build: `ninja -C build clickhouse` → exit 0 (incremental;
  `DatabasePostgreSQL.cpp.o` rebuilt + relink). `-Weverything`/`-Wthread-safety`
  with `-Werror`: clean for the touched TU.
- Test: `tests/integration/test_aiven_postgres_unlock` — evidence pair green
  (post-patch 2/2) / red (pre-patch 2/2 fail on the latency assertions, 11.91 s /
  5.99 s vs the 3 s bound). See §4.
- `byte_equivalent: false` — the refactor is structurally rewritten against the
  drifted 26.3 file; the documented deviations are intentional.

### Cross-cutting note (for the maintainer's ledger, not staged here)

26.3's `shouldBeEmptyOnDetach`-gated `DROP DATABASE` table-iteration skip is a
generic upstream behavior that interacts with several "external database engine"
patches in this series (PostgreSQL dictionary 036, etc.). The
`major-upstream-changes.md` ledger is intentionally left untouched; this note is
recorded here for the maintainer to fold if relevant.

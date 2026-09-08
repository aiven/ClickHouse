---
description: 'Dossier for the Aiven patch that resolves the shard macro when a replicated table is attached at server startup'
sidebarTitle: '006: Attach with shard macro'
slug: '/aiven/patches/006-replicated-database-attach-with-shard-macro'
title: 'Patch 006: Replicated database attach with shard macro'
doc_type: 'reference'
---

# Patch 006 — Replicated database attach with shard macro {#patch-006-replicated-database-attach-with-shard-macro}

## Lineage {#lineage}

| LTS | How to find | Outcome |
|---|---|---|
| 25.3 | `if (is_replicated_database \|\| query.attach)` on `v25.3.14.14-lts-aiven` | carried |
| 25.8 | `22e03c9d9d6cf9929aec824b724e09ea5c58653f` on `v25.8.18.1-lts-aiven` | ported |
| 26.3 | `patch-port(006): Replicated database attach with shard macro` at `8363bc6a7f08d275c24b7b0fdf10ca82ee449786` | ported verbatim (one line) |
| 26.8 | `patch-port(006):` | `still-needed-but-rewrite` |

The 26.3 commit is itself a cherry-pick of `d2f78fd`.

`src/Storages/TableZnodeInfo.cpp` and `src/Databases/DatabaseReplicatedHelpers.cpp`
are byte-identical between `v25.8.31.9-lts-aiven` and `v26.3.26.3-lts-aiven`, so
the three older lines share one hazard profile and one behaviour; nothing about
this patch diverged between them.

Two details that matter when reading an older tag. The DDL-context input was
`context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY`
on 25.3 and on 26.3 up to `v26.3.10.62-lts-aiven`, and is
`context->isDDLOrOnClusterInternal()` from `v26.3.26.3-lts-aiven` onward,
following the upstream switch to an explicit secondary-query flag. That input
does not affect the `assert_cast` hazard, which is conditioned on
`query.attach` alone — only the fix arm reads it. And the patch is absent from
26.3 at or below `v26.3.10.62-lts-aiven`: those builds carry neither the fix nor
the hazard, so they are exposed to the original startup failure instead.

## Background {#background}

A `ReplicatedMergeTree` table's ZooKeeper path and replica name are engine
arguments that may contain macros. They are resolved in **two passes**, and the
split is the whole reason this patch exists:

1. `{uuid}`, `{database}` and `{table}` are expanded first, and the result is
   what gets written into the table's metadata file.
2. `{shard}` and `{replica}` are expanded *afterwards* and deliberately **not**
   stored — the in-repo comment says so directly: "We do not expand them on
   previous step to make possible copying metadata files between replicas."

So the metadata on disk keeps `{shard}` symbolic on purpose. The consequence is
that every `ATTACH` — including the implicit one the server performs for each
table at startup — has to resolve `{shard}` again, from scratch.

There are two possible sources for that value: a global `<shard>` macro in the
server configuration, or the per-database shard name carried in a
`DatabaseReplicated` engine argument. For a table inside a `Replicated`
database the second is authoritative.

The obvious question is why the configuration does not simply define `<shard>`
and end the problem. Because the macro is per-*server* while the shard name is
per-*database*: one server can host several `Replicated` databases sitting on
different shards, so any single global value is wrong for all but one of them.
That is the reason the engine argument exists and is authoritative, and the
reason this is a missing-plumbing bug rather than a missing-config bug.

## Component tour {#component-tour}

`src/Storages/TableZnodeInfo.cpp` — metadata resolution, shared by `CREATE` and
`ATTACH`. Entry point `TableZnodeInfo::resolve`, reached from
`extractZooKeeperPathAndReplicaNameFromEngineArgs` in
`registerStorageMergeTree.cpp`, which every `ReplicatedMergeTree` registration
goes through. Foreground, and on the startup path it runs inside the table
loading job, so an exception here fails server startup rather than one query.

`src/Common/Macros.cpp` — the expander. `Macros::expand` consults
`MacroExpansionInfo::shard` only when it is set (`else if (info.shard && ...)`),
otherwise it falls through to the configured macros and throws if the name is
absent. Leaving `info.shard` unset is therefore not neutral: it silently shifts
resolution onto the server config.

`src/Databases/DatabaseReplicatedHelpers.cpp` — two free functions that reach
the shard and replica names. Both downcast with `assert_cast<const
DatabaseReplicated *>` and **no type check**. The same pair is called from
`StorageKafkaUtils.cpp`.

`assert_cast` is weaker than its name suggests, and weaker than the obvious
reading of "throws in debug." Its typeid check is compiled only under
`DEBUG_OR_SANITIZER_BUILD`; otherwise the body is `return static_cast<To>(from)`.
That macro is not implied by `CMAKE_BUILD_TYPE=Debug`: the local debug build
used for this patch compiles with `-DNDEBUG` and `-DDEBUG_BUILD` and **not**
`DEBUG_OR_SANITIZER_BUILD`, so `assert_cast` there is an unchecked
`static_cast`. Verified from `build/compile_commands.json`. Treat a bad
`assert_cast` as silent undefined behaviour by default, not as a caught error.

## Problem {#problem}

A `DatabaseReplicated` database contains a `ReplicatedMergeTree` table whose
path holds `{shard}` — which is the default, since `default_replica_path` ships
as `/clickhouse/tables/{uuid}/{shard}`. The server config has no global
`<shard>` macro, because the database's own engine argument is the real source.

On startup the table loader attaches the table. `TableZnodeInfo::resolve`
populates `info.shard` from the containing database only when
`is_replicated_database` holds, and that flag requires
`context->isDDLOrOnClusterInternal()` — true for DDL-queue and `ON CLUSTER`
work, false for a local startup `ATTACH`. So `info.shard` stays unset,
`Macros::expand` looks for a configured `shard` macro, finds none, and throws.
The load job fails and the server does not come up:

    Code: 139. DB::Exception: No macro 'shard' in config while processing
    substitutions in '/clickhouse/tables/{uuid}/{shard}' at '27' ... Cannot
    attach table `testdb`.`t` from metadata file ... (NO_ELEMENTS_IN_CONFIG)

Blast radius is a whole server, not a query, and it is triggered by a plain
restart of a normal configuration.

## Approach {#approach}

Populate `info.shard` and `info.replica` from the containing database whenever
the database *is* `Replicated`, on both the DDL-internal path and the attach
path, rather than only on the DDL-internal one.

The 26.3 port did this by widening the guard to
`if (is_replicated_database || query.attach)`. We do not repeat that, because
`query.attach` on its own says nothing about the database's engine, and the very
next line downcasts to `DatabaseReplicated` through `assert_cast`. Startup
attaches every table, so on that shape any `ReplicatedMergeTree` living in a
plain `Atomic` database enters the block on every boot and casts a
`DatabaseAtomic` to `DatabaseReplicated`.

That reachability was measured, not assumed — see Tests. Under the 26.3 shape
the attach-path arm demonstrably fires, and a `ReplicatedMergeTree` in an
`Atomic` database therefore executes the invalid cast on a normal restart. What
was *not* observed is any resulting failure: the test attached and queried
fine, because the out-of-bounds reads happened to yield usable values and the
build does not compile `assert_cast`'s check. So the case against the 26.3
shape is not "it breaks" — it is that a routine restart performs an invalid
downcast whose harmlessness rests on heap layout. The 26.3 dossier called this
a hypothetical edge; it is reached on every boot, and it is merely quiet.

A first attempt was to keep the helper call and add the engine name as a second
condition, `(is_on_cluster || query.attach) && getEngineName() == "Replicated"`.
That is sound, and it is the dominant idiom inside `src/Storages/`
(`registerStorageMergeTree.cpp:273`, `ObjectStorage/Utils.cpp:313`,
`StorageKafkaUtils.cpp:305`). It was still rejected: the defect being fixed is a
guard drifting away from the downcast it protects, and a second separately
widenable guard reproduces that defect one remove out. The nearest precedent
shows how it decays — `StorageReplicatedMergeTree.cpp:6758` tests the engine
string, then `dynamic_cast`s and dereferences the result with no null check, so
if string and type ever disagree it is a null dereference.

The shape taken instead makes the type test and the use the same expression, so
there is no second thing to keep in sync:

```cpp
/// `is_on_cluster` is the existing local for context->isDDLOrOnClusterInternal().
if (is_on_cluster || query.attach)
{
    /// Hold the DatabasePtr in a named local: `replicated` borrows from it.
    auto database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    if (const auto * replicated = dynamic_cast<const DatabaseReplicated *>(database.get()))
    {
        info.shard = replicated->getShardName();
        info.replica = replicated->getReplicaName();
    }
}
```

`getReplicatedDatabaseShardName` is not called at this site at all, so its
unchecked `assert_cast` is not merely unreached — it is not expressible here.
That makes `Databases/DatabaseReplicatedHelpers.h` unused in this translation
unit, so the include is swapped for `Databases/DatabaseReplicated.h`; the helper
header itself stays, since `StorageKafkaUtils` still uses it.

`is_replicated_database` itself is left exactly as it is, because it also feeds
`allow_uuid_macro` further up and that semantics must not move. Two consequences
a reviewer will spot, both deliberate. It now survives only in that one use,
where its term is provably redundant — it is defined as
`isDDLOrOnClusterInternal() && engine == "Replicated"`, and `allow_uuid_macro`
already ors in `is_on_cluster`, so `A || (A && B)` is just `A`. And the
`getEngineName()` lookup that computes it is now the only reason that database
lookup happens on the DDL path. Deleting the variable would be a correct
simplification, but it widens the diff into upstream code for no behavioural
change and buys a `CH Inc sync` conflict surface, so it is left for upstream.

Two hazards in this shape, both worth a reviewer's attention:

- **Do not collapse the `DatabasePtr` into the condition.** Temporaries in an
  `if` condition are destroyed at the end of the condition's full-expression,
  *before* the controlled statement runs, so
  `dynamic_cast<...>(DatabaseCatalog::instance().getDatabase(name).get())`
  leaves `replicated` dangling for the whole body. The named local is load
  bearing, hence the comment on it.
- **`dynamic_cast`, not `typeid_cast`.** `typeid_cast` matches the exact type
  ("cast to the ancestor will be unsuccessful", per `Common/typeid_cast.h`), so
  a future subclass of `DatabaseReplicated` would yield `nullptr` and silently
  skip shard resolution — reinstating this very startup failure as a quiet
  fallback. `DatabaseReplicated` is a leaf today, so `typeid_cast` would be
  correct now and wrong later. This runs once per table attach, never per row,
  so the hierarchy walk costs nothing worth measuring.

Taking the `nullptr` branch for a database that really is not `Replicated` is
correct rather than a fallback: such a database has no shard name to offer, and
the configured macro is the right source for it. The forbidden shape would be
skipping for a database that *is* `Replicated`, which is what the previous point
guards against.

The `is_on_cluster || query.attach` precondition is probably redundant. DDL
against a `DatabaseReplicated` flows through that database's own queue and
executes with the internal flag set, so the honest rule
is closer to "a table in a `Replicated` database takes its shard and replica
from that database, full stop." It is kept because the non-DDL paths cannot be
cheaply enumerated (restore, materialized-view inner tables, `ATTACH ... FROM`),
and narrowing the change is worth more than tidiness here. Read it as deliberate
conservatism, not as a considered requirement.

Non-goals, all of which will come up in review:

- Making `getReplicatedDatabaseShardName` fail closed. Safer for its remaining
  caller in `StorageKafkaUtils`, but it turns a one-hunk port into a change to
  shared code. The hazard stays there and is recorded as known-remaining rather
  than owned by this patch.
- Setting the DDL-internal flag on the startup loader's context. Superficially
  the smaller fix, but that flag also gates `allow_uuid_macro` and the DDL
  guards, so the blast radius runs far past this bug.
- Expanding `{shard}` into the metadata file at `CREATE` time so that `ATTACH`
  never has to re-resolve it. This is the root-cause fix and is rejected
  precisely because it destroys the replica portability the two-pass design
  exists to buy, and would change on-disk metadata — not a call a fork patch
  gets to make.

## Concept {#concept}

The transferable idea is that **a deliberately unresolved value has to stay
resolvable for as long as it can be read back**. ClickHouse keeps `{shard}` and
`{replica}` symbolic in table metadata so a metadata file can be copied between
replicas — a good decision that buys portability. The price is that resolution
happens again on every load, so any input that is only available during DDL, and
not during startup, becomes a boot-time failure rather than a DDL error. When
you see a value intentionally left as a template, ask which contexts will later
have to expand it, and whether all of them can reach the inputs.

The narrower lesson worth keeping: `assert_cast` is a debug assertion, not a
checked cast — and even that overstates it, since the check needs
`DEBUG_OR_SANITIZER_BUILD` and a plain debug build here does not define it.
Widening a condition that guards a downcast removes a type check, because in
that code the condition *was* the type check.

A corollary learned the hard way on this patch: undefined behaviour is not the
same as observable failure. The 26.3 shape executes an invalid downcast on a
normal restart and the test still passed. "I could not make it fail" is not
evidence that a cast is valid, and a test that passes on both the broken and
the fixed shape proves nothing about the difference between them.

And the repair generalises past this patch. When a defect is "the guard and the
guarded operation can drift apart," prefer a fix that makes them one expression
over a fix that adds a better guard. `if (const auto * p = dynamic_cast<T *>(x))`
is that pattern: the type test *is* the pointer you then use, so there is
nothing left to keep in sync. ClickHouse offers three casts with different
contracts — `assert_cast` (debug-only assertion, `static_cast` in release),
`typeid_cast` (exact type, `nullptr` on mismatch), `dynamic_cast` (hierarchy
aware, `nullptr` on mismatch) — and picking among them is a decision about what
should happen when the type is wrong, not a matter of style.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-but-rewrite`

- Anchor intact at `src/Storages/TableZnodeInfo.cpp:192`; the block and the
  `is_replicated_database` definition are unchanged in substance from 26.3.
- The surrounding file did move: upstream added `{database}`/`{table}`
  substitution validation, C1 control-character rejection, enum-typed error
  subjects, and an explicit secondary-`ON CLUSTER` flag. None of it touches the
  shard/replica population, so the gap survives and a verbatim cherry-pick would
  still apply — it is the *content* we are rejecting, not the context.
- No superseding upstream fix: nothing else in `src/**` sets
  `MacroExpansionInfo::shard` for this path, and `StorageKafkaUtils` still
  carries the same unwidened guard.
- The rewrite diverges from 26.3 by adding the engine check. That is a
  correctness fix to the Aiven patch itself, not drift accommodation.

## Customer impact {#customer-impact}

A server whose `Replicated` database holds a `ReplicatedMergeTree` table with
`{shard}` in its path, and whose config defines no global `<shard>` macro, now
starts instead of failing its table-load job with
`No macro 'shard' ... (NO_ELEMENTS_IN_CONFIG)`. No setting to flip, no metadata
change, and nothing to do on upgrade — a restart that previously failed simply
succeeds.

New versus 26.3: a `ReplicatedMergeTree` in a database that is *not* `Replicated`
no longer enters the shard-resolution block at all. On 26.3 such a table took an
unchecked `static_cast` to `DatabaseReplicated` on every attach. No user-visible
symptom is known or was reproduced — the reads land past the object and, in the
case tested, stayed benign — so this is a latent-defect fix rather than
something a tenant would notice either way. The 26.8 port closes it instead of
documenting it, because the alternative is shipping undefined behaviour on the
restart path.

Whether the older lines were ever exposed was checked rather than assumed. The
cast misfires only for a `Replicated*MergeTree` table in a database that is not
`Replicated`, which this finds:

```sql
SELECT database, name, engine, create_table_query
FROM system.tables
WHERE engine LIKE 'Replicated%'
  AND database NOT IN (SELECT name FROM system.databases WHERE engine = 'Replicated')
```

On 25.8 it returns 0 rows (2026-09-08); 26.3 not yet sampled. So the shipped
25.8 patch never performs the invalid cast there, and no backport is indicated.
Because the code is identical across 25.3, 25.8 and current 26.3 (see Lineage),
this audit is a question about deployed database shapes, not about versions: a
clean 25.8 result predicts nothing for 26.3 beyond "same code," and each line
still has to be sampled on its own fleet.

That result also rules out the one exposure the control plane does not cover:
system-log tables live in `system`, which is never a `Replicated` database, and
their engine is operator-configurable (`SystemLog.cpp`, default
`ENGINE = MergeTree`), so a replicated system log would have shown up here.

This reframes the 26.8 rewrite as cheap insurance rather than an incident fix.
Its value is that it stops depending on an unenforced operational invariant
whose violation would be silent — a later config change enabling replicated
system logs would reintroduce the misfire with no exception and no log line.

## Tests {#tests}

- Path: `tests/integration/test_aiven_replicated_database_attach_with_shard_macro/`
- Why integration and not stateless: the trigger needs a server configuration
  that defines `<replica>` but **not** `<shard>`, plus a real restart to exercise
  the startup load path. Neither is reachable from a `.sql` test against the
  shared stateless server.
- What it proves (causation): two nodes with Keeper, a `DatabaseReplicated` with
  a literal shard name, a `ReplicatedMergeTree` created through the default
  `default_replica_path` so the path retains `{shard}`, then
  `restart_clickhouse(kill=True)` on both nodes and a `SELECT` that only
  succeeds if the tables attached.
- The assertion is the restart itself. With `async_load_databases` false a
  failed `ATTACH` fails the startup load job, so the helper's
  `restart_clickhouse` raises `Exception: Cannot start ClickHouse` instead of
  returning a queryable server. Asserting on that, plus the `No macro 'shard'`
  text in the server log, avoids pinning an error code — the 26.3 preflights
  predicted `Code: 62` and the real code is `139`.
- A second case, `test_restart_attaches_replicated_table_in_plain_database`,
  covers the guard: a `ReplicatedMergeTree` in a non-`Replicated` database, with
  no `{shard}` in its path, still attaches across a restart. Read it as a
  regression guard on the skip path, **not** as proof that the 26.3 shape is
  broken — see the evidence below.

### Evidence (2026-09-07, local debug build, aarch64) {#evidence}

Three binaries, same tree otherwise. "Base" is upstream 26.8 with no 006 at all,
"26.3 shape" is `is_replicated_database || query.attach` with the helper calls,
"this port" is the `dynamic_cast` form.

| Case | Base | 26.3 shape | This port |
|---|---|---|---|
| 1 — Replicated database, `{shard}` in path | **FAIL** (78 s) | PASS (13 s) | PASS |
| 2 — plain `Atomic` database | PASS (11 s) | PASS (12 s) | PASS |

Case 1 on base is the real reproduction of the customer's failure on 26.8,
verbatim from `node1`'s `clickhouse-server.err.log`:

    Code: 139. DB::Exception: No macro 'shard' in config while processing
    substitutions in '/clickhouse/tables/{uuid}/{shard}' at '27' or macro is
    not supported here: Cannot attach table `testdb`.`t` from metadata file
    ... ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}',
    '{replica}') ORDER BY x ... (NO_ELEMENTS_IN_CONFIG)

followed by `Application: Caught exception while loading metadata` — the server
does not finish starting. This also confirms the 26.3 calibration note: the code
is `139 NO_ELEMENTS_IN_CONFIG`, not `62`.

Case 2 passes on all three shapes, which is a negative result worth recording
rather than hiding. Case 1 passing under the 26.3 shape proves the attach-path
arm fires, so case 2 under that same shape *does* execute
`assert_cast<const DatabaseReplicated *>` on a `DatabaseAtomic` — and nothing
observable happened. Two reasons: this build compiles `assert_cast` without its
typeid check (`-DNDEBUG`, no `DEBUG_OR_SANITIZER_BUILD`), and the out-of-bounds
reads yielded values that still attached correctly, with `replicas/r1` in the
log. So case 2 does not discriminate between the shapes here. An ASan shard,
where `DEBUG_OR_SANITIZER_BUILD` is defined, is the place that could — untested.
### No-regression argument {#no-regression-argument}

The regression surface is small enough to enumerate rather than sample. Write
`D` for `isDDLOrOnClusterInternal()`, `A` for `query.attach`, `R` for "the
database is `Replicated`". The old block fired on `D ∧ R`; the new one fires on
`(D ∨ A) ∧ R`. The difference is exactly `¬D ∧ A ∧ R` — the startup attach this
patch exists to fix. Every other combination resolves identically, including
`D ∧ ¬R`, where the old string comparison and the new `dynamic_cast` both
decline.

That equivalence needs `getEngineName() == "Replicated"` and
`dynamic_cast<const DatabaseReplicated *>` to agree. They do:
`DatabaseReplicated` is the only `IDatabase` returning that name, and nothing
derives from it.

So one behavioural risk remains, and it is not the resolution logic: the `&&` at
line 132 short-circuits, so `getDatabase` was previously not evaluated at this
site when `D` is false, and it now is whenever `A` holds. It throws
`UNKNOWN_DATABASE` for an absent database. The loader registers a database
before attaching its tables, and both restart cases above exercise the path, so
this is inert in practice.

### Regression run {#regression-run}

`tests/integration/test_replicated_database/` — the direct blast radius, since
it exercises `DatabaseReplicated` DDL, attach, recovery and metadata repair.
Run twice, same tree, only the binary differing:

| Binary | Result |
|---|---|
| Base (no 006) | 12 failed, 32 passed, 366 s |
| This port | 12 failed, 32 passed, 369 s |

The two failure sets are identical, compared by name rather than by count. The
failures are pre-existing and local to this machine: `test_table_metadata_corruption`
fails because a `clickhouse disks ... read --path-from store/...` call inside the
container errors out, which leaves the node unhealthy and cascades into the
eleven tests that follow it in the module. Not related to this patch, and not
investigated further here.

## Rollback {#rollback}

Safe and complete: one hunk, no on-disk or ZooKeeper state, no setting, no
format change. Reverting restores the startup failure for the affected
configuration. Note that reverting to the *26.3* shape rather than to upstream
would reintroduce the `assert_cast` hazard.

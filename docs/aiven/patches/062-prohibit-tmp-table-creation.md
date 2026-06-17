# Patch 062 (chain 062+063+064) — prohibit `.tmp` table creation

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | 062 `50a42b769e`, 063 `cc718390aa`, 064 `c95ef0cc6b` | 062 Tilman Moeller, 063/064 Aliaksei Khatskevich (committer `joelynch112@gmail.com`) | original carry (three commits) |
| 26.3-aiven | (staged) | `patch-port(062,063,064)` dispatch | conflict-resolved + drift-fixed + **gated** (one new default-off server setting), landed as ONE squashed commit |

The current uplift's row stays "(staged)" until the human commits.

This dossier covers the **whole chain**, because the three commits are strict-order
and the intermediate state is runtime-broken (062+063 without 064 throws on every
`CREATE OR REPLACE`). They are therefore ported as a single squashed commit
`patch-port(062,063,064)`.

## 1. Purpose

Reserve the `.tmp*` table-name namespace for engine-internal use so user tables can
never collide with the engine-generated temporaries that refreshable materialized
views and `CREATE OR REPLACE` create, and so those temporaries are skippable by name
in backup. The "why" is durable: the refresh / `CREATE OR REPLACE` machinery builds
"fake" temporary tables that deliberately do **not** carry the `is_temporary` flag
(they must replicate), so they cannot be excluded from backup or from user visibility
by the usual temporary-table mechanism — a name-prefix convention is the simple,
consistent handle.

The three source commits:

- **062 `50a42b769e`** ("Prohibit .tmp table creation", Tilman Moeller). The base:
  reject non-internal `CREATE` and `RENAME` of a table whose name starts with `.tmp`.
  Quoting the commit body:

  > This is necessary, because temporal tables for Refreshable Materialized Views
  > are not market as temporal (to allow replication), and we should not backup them.

- **063 `cc718390aa`** ("Use .tmp for all fake temporal tables", Aliaksei
  Khatskevich). Rename the `CREATE OR REPLACE` temporary table prefix from
  `_tmp_replace_` to `.tmp_replace_` so it falls inside the reserved `.tmp*`
  namespace (the MV refresh temp `.tmp.inner_id.*` already does — see
  `StorageMaterializedView::prepareTableForInsert`). Quoting the body:

  > There are 2 places with "fake temporary tables" — Temporal table for MV refresh
  > [and] Temporal table for `CREATE OR REPLACE`. [...] This commit enforces `.tmp`
  > prefix in both cases to allow [...] Avoid temporary table backup [and] Prohibit
  > creating such tables by user.

- **064 `c95ef0cc6b`** (garbled subject "asd .tmp create or replace", Aliaksei
  Khatskevich). The exemption that un-breaks the chain: in
  `doCreateOrReplaceTable`, mark the inner temp-table create **internal** so the
  062 guard does not reject the engine's own `.tmp_replace_*` create.

## 2. Policy decision — gate only 062's throw (do not re-litigate)

Per the dispatch (clause-(v) blast-radius resolution), **only 062's throw** is gated
behind a new default-`false` server setting `aiven_prohibit_tmp_table_creation`
(Bool). Everything else in the chain ships **unconditional/ungated**. Rationale: with
the gate OFF (the default) the build behaves byte-identically to stock 26.3
(test-neutral by construction); Aiven enables the gate in production to protect the
`.tmp*` namespace.

### Gated pieces (behind `aiven_prohibit_tmp_table_creation`)

- `InterpreterCreateQuery::doCreateTable` — the create-side throw (keyed on the
  existing `internal` member AND the setting).
- `InterpreterRenameQuery::executeToTables` — the rename-side throw (keyed on
  `getContext()->isInternalQuery()` — **not** the `internal` member — AND the
  setting, preserving 062's original `isInternalQuery` keying). The rename guard sits
  inside the `database->shouldReplicateQuery(...)` branch, so it only applies to
  `Replicated` databases (unchanged from 062).

Both reuse 062's exact message text: `Table name '{}' is invalid: names starting
with '.tmp' are reserved for internal use` and error code `BAD_ARGUMENTS` (36).

### Unconditional pieces (no gate)

- The new `setInternal` setter + `internal{false}` member on
  `InterpreterRenameQuery` (062 adds them; `InterpreterCreateQuery` already had its
  own at HEAD). Note the `internal` member of `InterpreterRenameQuery` is never read
  inside the interpreter (the rename guard keys on `isInternalQuery`) — it is carried
  faithfully from 062, where it is likewise dormant.
- The two internal-flips: `DatabaseReplicated::recoverLostReplica`'s `CREATE` and
  `StorageMaterializedView::exchangeTargetTable`'s `InterpreterRenameQuery` →
  marked internal. Always-correct; a behavioral no-op when the gate is off.
- **063** entirely (the `.tmp_replace_` literal + the
  `^\.tmp_replace_(\w+)_(\w+)$` regex in `TemporaryReplaceTableName.cpp`).
- **064** entirely (`doCreateOrReplaceTable` hoists the inner create into a named
  `interpreter`, calls `interpreter.setInternal(true)`, then `doCreateTable`).

The gate setting is declared in `src/Core/ServerSettings.cpp` immediately after
patch 008's `aiven_enable_replication_queue_size_limit` (same pattern; generic
loading, no per-setting plumbing) and is the second application of the AGENTS §8
`aiven_` naming convention.

## 3. Upstream-drift findings

### Findings

- **062 `InterpreterRenameQuery.cpp` include block — the only real apply conflict.**
  062 adds `#include <Common/StringUtils.h>` anchored on the context
  `AccessRightsElement.h → typeid_cast.h`, but HEAD inserted
  `#include <Common/NamedCollections/NamedCollectionsFactory.h>` between those two,
  breaking the anchor. Re-anchored: `StringUtils.h` is added after `typeid_cast.h`.
  We also add `#include <Core/ServerSettings.h>` (needed for the gate; 062 had no
  ServerSetting dependency since it was ungated).
- **062 `doCreateTable` hunk** — the
  `checkTableNameLength(create.getTable());` → `data_path = …` window is unchanged at
  HEAD; the gated throw is inserted right after `checkTableNameLength`.
- **`DatabaseReplicated.cpp`** — the bare
  `InterpreterCreateQuery(query_ast, create_query_context).execute();` anchor is
  intact; replaced with the named-interpreter + `setInternal(true)` form.
- **`StorageMaterializedView::exchangeTargetTable` — drift vs. the preflight.** The
  preflight expected a bare
  `InterpreterRenameQuery(rename_query, refresh_context).execute();`, but at HEAD the
  066+078 squash already refactored this into a named
  `auto interpreter = InterpreterRenameQuery(...); auto block_io = interpreter.execute();`.
  Adapted: insert `interpreter.setInternal(true);` between the two existing lines (the
  062 intent is preserved; the diff is one line instead of a 3-line replacement).
- **063 `TemporaryReplaceTableName.cpp`** — byte-identical region; applies clean.
- **064 `doCreateOrReplaceTable`** — `setInternal` already exists on
  `InterpreterCreateQuery`; applies clean.
- **Neutrality of 063's unconditional rename.** The only place in the test tree that
  references the old prefix is `tests/integration/test_refreshable_mv/test.py:190`
  (`if name.startswith(".") or name.startswith("_tmp_replace_")`). After 063 the
  prefix is `.tmp_replace_`, already caught by the `startswith(".")` clause; the
  `_tmp_replace_` clause becomes dead but harmless. No neutrality break.
- Conclusion: **`still-needed-but-rewrite`** — semantics carried; the chain needs one
  include re-anchor, one drift adaptation (the MV named-interpreter), and the new
  gate plumbing. `byte_equivalent: false` (the gate is an Aiven-introduced addition).

## 4. C++ review

Per `docs/aiven/skills/cpp-review-checklist.md`:

- 1 Lifetime + ownership: ✓ — the named `interpreter` locals (064 + the two
  internal-flips) live for the duration of their `execute()`/`doCreateTable` call and
  are destroyed at scope end; no reference outlives its interpreter. No new ownership.
- 2 Exception safety: ✓ — the gated throw fires before any table/metadata is created
  (it is right after `checkTableNameLength`, before `getTableDataPath`); the rename
  throw fires before the replicated-DDL enqueue. No partial state on throw.
- 3 Thread-safety + concurrency: ✓ — the setting is read once via the thread-safe
  `getServerSettings()` accessor; no new shared state, no lock, no sleep.
- 4 Performance + memory: ✓ — one `bool` server-setting read on the create/rename DDL
  path (cold, not a hot path); zero cost when the gate is off beyond that read.
  `startsWith` is only evaluated when the gate is on (short-circuit `&&`).
- 5 Settings as public API: ✓ — new `ServerSetting` `aiven_prohibit_tmp_table_creation`
  (Bool, default **false** — safe new gate), `aiven_` prefix per AGENTS §8,
  server-level (cannot be overridden per-session).
- 6 Error handling: ✓ — throws the shared `BAD_ARGUMENTS` (36) with the
  Aiven-distinctive message substring `reserved for internal use`; the test asserts
  BOTH the code and the substring, and uses a `.tmp`-prefixed name that upstream would
  accept, so it distinguishes the Aiven gate from any unrelated `BAD_ARGUMENTS`.
- 7 Upstream / vendored code: ✓ — no `contrib/**`, `.claude/**`,
  `.github/workflows/**`, or root `AGENTS.md` touched.
- 8 Behavior under settings: ✓ — gate OFF ⇒ nothing observable: both throws are
  short-circuited by the `getServerSettings()[…]` read, and the only unconditional
  behavior changes (063's temp-table rename, the `setInternal` flips) are
  engine-internal and invisible to users. Verified by the guard-OFF stateless test
  and the guard-OFF control node in the integration test.

## 5. Test design

Because `aiven_prohibit_tmp_table_creation` is a **server** setting (cannot be set
per-session), the ON-state proof is an **integration test**; a cheap stateless test
covers the OFF-state neutrality.

(a) **New integration test that exercises the gated path** —
`tests/integration/test_aiven_prohibit_tmp_table/` (`test.py` +
`configs/enable_prohibit_tmp.xml`), mirroring the patch 008 layout
(`node_on` with the guard config drop-in; `node_off` default-false). Four cases, all
**PASS** against the locally-built patched binary (`4 passed in 13.65s`):

1. `test_create_tmp_table_rejected_when_guard_on` — guard ON: `CREATE TABLE
   ".tmpfoo"` throws `BAD_ARGUMENTS` / `reserved for internal use`; guard OFF: the
   identical `CREATE` succeeds (neutrality control).
2. `test_rename_to_tmp_rejected_when_guard_on` — guard ON: `RENAME TABLE … TO
   ".tmpbar"` in a `Replicated` database throws `BAD_ARGUMENTS`; guard OFF: the
   identical rename succeeds. Each node uses its **own** Replicated database (distinct
   ZooKeeper path) so the two servers do not replicate the table to each other.
3. `test_create_or_replace_still_works_when_guard_on` — guard ON: `CREATE OR REPLACE
   TABLE` succeeds twice (proves 064's exemption of the internal `.tmp_replace_*`
   create, including the temp-then-exchange path on an existing table).
4. `test_refreshable_mv_refresh_still_works_when_guard_on` — guard ON: a refreshable
   MV refresh lands its rows (proves the internal `.tmp.inner_id.*` create is exempt).

Why this distinguishes the Aiven gate (AGENTS §7): the asserts check BOTH the error
code (`BAD_ARGUMENTS`) AND the Aiven-specific substring (`reserved for internal
use`); the guard-OFF control proves the SERVER setting — not some unrelated check —
is what gates the rejection (the same `.tmp` name is accepted with the guard off).

(b) **OFF-state stateless neutrality test** —
`tests/queries/0_stateless/09081_prohibit_tmp_table_creation_off_neutral.sql`: with
the default (gate off) server, `CREATE TABLE \`.tmp09081\`` succeeds, accepts inserts,
and is dropped. **PASS** (0.18 s).

(c) **Neutrality of the unconditional changes (gate off).** Ran the CREATE OR
REPLACE + refreshable-MV stateless suites against the patched binary; all the tests
that run in the single-node smoke environment PASS:
`01185_create_or_replace_table`, `03237`/`03238_create_or_replace_view_atomically_*`,
`03221_refreshable_matview_progress`, `03760_refreshable_mv_local`,
`04010_create_or_replace_table_clone_as`, `03918_replace_table_clone_as_verbose_result`.
Two heavy `.sh` refreshable-MV tests (`02932_refreshable_materialized_views_1`,
`03258_refreshable_mv_misc`) and `01157_replace_table` did **not** pass in this
minimal environment, but for reasons unrelated to the patch:
- `02932`/`03258` time out on `SYSTEM TEST VIEW … SET FAKE TIME` / `grant system
  views` — proven environmental by an **A/B against the baseline binary** (with the
  `src/` changes stashed): both tests time out **identically** on the unpatched
  build, so the hang is the bare smoke server, not the patch.
- `01157` fails on `CLUSTER_DOESNT_EXIST 'test_shard_localhost'` — the smoke server
  lacks the test-config cluster definition (environmental).

## 6. Rollback considerations

- Revert safety: safe. No schema migration, no on-disk format change. 063's temp
  table name (`.tmp_replace_*`) is ephemeral (created and exchanged/dropped within the
  `CREATE OR REPLACE` / refresh operation); no persisted name depends on it.
- Surviving state: none specific to this patch.
- Disable without rebuild: set `aiven_prohibit_tmp_table_creation` to false (its
  default). Server-level setting; fully controlled by config.

## 7. Per-uplift notes

### 25.8-aiven (historical)

Original carry: three commits — 062 `50a42b769e` (Tilman Moeller), 063 `cc718390aa`
and 064 `c95ef0cc6b` (Aliaksei Khatskevich), committer `joelynch112@gmail.com`. The
062 throw was **unconditional** (no server setting) on 25.8; that is why 064 was
required to exempt the engine's own `CREATE OR REPLACE` temp create.

### 26.3-aiven (this uplift)

- Landed as ONE squashed commit `patch-port(062,063,064)` (strict-order chain;
  062+063 without 064 is runtime-broken).
- **Gated** (parent policy call): only 062's two throws are behind the new default-off
  `aiven_prohibit_tmp_table_creation`; everything else ships unconditional. Gate OFF =
  byte-identical to stock 26.3.
- Drift fixes (port, not redesign): (1) re-anchor 062's `StringUtils.h` include past
  HEAD's inserted `NamedCollectionsFactory.h`; (2) add `Core/ServerSettings.h` for the
  gate; (3) adapt `StorageMaterializedView::exchangeTargetTable` to the
  066+078-refactored named-interpreter form (insert `setInternal(true)` instead of
  replacing a bare-`.execute()` line).
- Build: warm incremental, all 6 touched TUs recompiled (`InterpreterCreateQuery`,
  `InterpreterRenameQuery`, `TemporaryReplaceTableName`, `DatabaseReplicated`,
  `StorageMaterializedView`, `ServerSettings`); no `#deps 0` staleness (the
  `InterpreterRenameQuery.h` member addition is consumed only by its own `.cpp` plus
  `StorageMaterializedView.cpp`, both recompiled). Green.
- Tests added: `tests/integration/test_aiven_prohibit_tmp_table/` (4/4 PASS) and
  `tests/queries/0_stateless/09081_prohibit_tmp_table_creation_off_neutral.sql` (PASS).
- Anything surprising: the integration `RENAME` case first failed with
  `TABLE_ALREADY_EXISTS` because both nodes were replicas of one Replicated database
  (same ZK path) — fixed by giving each node its own independent Replicated database.
  The two flaky `.sh` neutrality tests were disproved as regressions via a baseline
  A/B.

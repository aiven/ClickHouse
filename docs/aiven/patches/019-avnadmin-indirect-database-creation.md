# Patch 019 — avnadmin-indirect-database-creation

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | `8ff9d3cb7ad` | Aiven | (earlier production line) |
| 25.8-aiven | `05d8148a57` | Tilman Moeller (author) / Aliaksei Khatskevich (committer), 2025-12-13 | (the version we are porting FROM) |
| 26.3-aiven | `patch-port(019)` (`2a8529c5935`) | parent agent + worker, 2026-06-02 | `still-needed-but-rewrite` — ported with fix-forwards + a 26.3-drift fix; see §2 |

Source on `v25.8.18.1-lts-aiven`: `05d8148a57` (14 files, 287 insertions). The
26.3 carry intentionally **diverges** from the source by fixing findings A, B, D
forward (§2) and by adding a `query_id` reset required by 26.3 base drift (§2.G).
`byte_equivalent: false`. Find the commit with `git log --grep '^patch-port(019)'`.

## 1. Purpose

Lets a configured admin user (Aiven's `avnadmin`) `CREATE` and `DROP` Replicated
databases via plain SQL, with the dangerous cluster parameters auto-filled and a
narrowly-scoped privilege elevation — without granting that user
`ACCESS MANAGEMENT` or making it a superuser. Mechanisms:

- **Three config-only `ServerSettings` (all default `""` → feature inert):**
  - `user_with_indirect_database_creation` — the single user name (e.g.
    `avnadmin`) for whom the indirect path fires.
  - `cluster_database` — the reference Replicated database whose pre-expansion
    shard macro and `ON CLUSTER` target are reused for new databases.
  - `reserved_replicated_database_prefixes` — comma-separated prefixes that the
    configured user may not create.
- **Indirect `CREATE` (`InterpreterCreateQuery::createReplicatedDatabaseByClient`):**
  when the configured user issues `CREATE DATABASE d`, the interpreter rewrites it
  to a fully-specified `CREATE DATABASE d ON CLUSTER <cluster_database> ENGINE =
  Replicated('/clickhouse/databases/d', '<shard_macros>', '{replica}') SETTINGS
  collection_name='cluster_secret'` and runs it on a **cloned, privilege-elevated
  context**, then grants the user `DEFAULT REPLICATED DATABASE PRIVILEGES` on the
  new database.
- **`GRANT DEFAULT REPLICATED DATABASE PRIVILEGES`** (new parser keyword + AST
  flag): expands to a fixed, curated privilege set on `db.*` (table/dictionary
  DML/DDL, `DROP DATABASE`, but never `CREATE DATABASE`, `ACCESS MANAGEMENT`, or
  `SYSTEM SHUTDOWN`), `WITH GRANT OPTION`. Patch 020 later appends `CHECK` to this
  set (the `019→020` chain).
- **Reserved-prefix guard (`checkDatabaseNameAllowed`)** and **`ON CLUSTER`
  enforcement for non-admin `DROP`/`DETACH`** (`InterpreterDropQuery`).

The elevation primitive is `Context::setGlobalContext`, which clears `user_id`
and sets `need_recalculate_access = true` so the cloned context re-resolves with
full privileges. **Invariant: this is only ever called on a `Context::createCopy`
throwaway, never on the session context** — case F (§4) is its empirical proof.

## 2. Upstream-drift / validity findings

> Mandatory section. The patch is still SEMANTICALLY needed on 26.3 (no upstream
> equivalent for the indirect-create / curated-grant feature), but it required a
> **rewrite** against heavy 26.3 drift and a security split.

### 2.A–E — Pre-existing security findings (carried/fixed per the policy split)

All five were verified **code-identical across 25.3 (`8ff9d3cb7ad`) and 25.8
(`05d8148a57`)** — they are NOT artifacts of the 26.3 port. They were
cross-checked against the consuming `aiven-core` config/tests, which establishes
the production invariants quoted below.

| ID | Finding | 26.3 disposition | Rationale |
|---|---|---|---|
| **A** | Grantee name in the superuser `GRANT` was string-escaped (`escapeString`), not identifier-quoted — a grantee containing `,` could split into multiple grantees. | **FIX-FORWARD** → `backQuote(username)`. | The grantee is an identifier; `backQuote` is correct. Production grantees are operator-controlled, so unexploited, but the fix is free and removes the injection surface. Test **H**. |
| **B** | `shard_macros` interpolated unquoted into the generated `CREATE` SQL. | **FIX-FORWARD** → `escapeString(...getShardMacros())`. | Defense-in-depth; the macro is operator-controlled in production. |
| **C** | `checkDatabaseNameAllowed` skips `internal` queries and explicit non-`Replicated` engines, so `CREATE DATABASE reserved_x ENGINE=Atomic` is unguarded. | **CARRY AS-IS** (intended), document. Test **D2**. | No reachable actor in aiven-core: `avnadmin` is rejected on non-Replicated engines before the guard; ordinary users cannot `CREATE DATABASE` at all. Replicated-only scope IS the policy. |
| **D** | Non-admin `DROP DATABASE` enforcement threw `SETTING_CONSTRAINT_VIOLATION` when `cluster_database` was empty — a **default-on behavior change** (clause (v) violation) even with the feature unconfigured. | **FIX-FORWARD** → no-op when `cluster_database` is empty. | Restores exact upstream `DROP` behavior when the feature is off. Test **R2**. |
| **E** | `executeDDLQueryOnCluster(..., skip_distributed_checks=true)` bypasses the `CLUSTER` grant and `allow_distributed_ddl` for the elevated path. | **CARRY AS-IS** (intended), document. Test **E2**. | The `DROP DATABASE` privilege itself is still required — only the distributed-DDL plumbing checks are skipped, which is the point of the managed path. E2 proves the privilege is not bypassed. |

Production-line follow-ups: A → a separate hardening ticket on 25.3/25.8 (low
priority, unexploited); D → no backport needed since `cluster_database` is always
configured (`"default"`) in production, so the buggy throw was never reached.

### 2.G — `query_id` collision (26.3 base drift; fix required)

A **sixth** issue, not in the A–E review. The elevated path runs two internal
`executeQuery(..., {.internal=true})` calls on a `Context::createCopy(context)`
throwaway. `setGlobalContext` clears only `user_id`; it leaves
`client_info.current_user` (`avnadmin`) and `client_info.current_query_id` (`Q`,
the caller's id) intact on the copy. So both inner queries register under the
same `(current_user=avnadmin, query_id=Q)` as the still-live outer `CREATE` →
`ProcessList::insert` trips the duplicate-id guard:
`Code: 216 QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING`.

**This is 26.3 base drift, not a logic bug in the patch.** On 25.3/25.8 internal
queries were never inserted into the process list at all —
`executeQuery.cpp` guarded the insert with `if (!internal && !(... ShowProcesslist ...))`.
26.3 upstream **removed the `!internal` guard** and now registers internal
queries (passing `is_internal` only to relax concurrency caps), while the
duplicate-id guard remains outside that relaxation. So the patch's elevation
pattern was correct on its native base and only collides on the drifted base.

```
# 25.8 (v25.8.18.1-lts-aiven) — internal queries NOT registered:
src/Interpreters/executeQuery.cpp:1289
    if (!internal && !(out_ast && out_ast->as<ASTShowProcesslistQuery>()))
        process_list_entry = context->getProcessList().insert(..., start_watch.getStart());   // no is_internal arg

# 26.3 HEAD — guard dropped, internal queries ARE registered:
src/Interpreters/executeQuery.cpp:1488
    if (!(out_ast && out_ast->as<ASTShowProcesslistQuery>()))
        process_list_entry = context->getProcessList().insert(..., start_watch.getStart(), internal);
```

**Fix (ratified 2026-06-02, security-neutral):** assign a fresh `query_id` to the
context used for each internal sub-query — `setCurrentQueryId("")` (empty string →
fresh random UUID, the established CH idiom). It changes only the process-list
registration identity; it does **not** touch the elevation/containment invariant
(elevation stays on the throwaway copy, gated to the configured user, never the
session) and it preserves the deliberately carried-over state (`allow_distributed_ddl=1`,
`client_info`, settings). The heavier `makeQueryContext()` reset was rejected — it
would also drop the carried-over `allow_distributed_ddl`, widening the blast radius.

**The collision has TWO sites** (the first fix alone was necessary but not
sufficient — confirmed empirically: it fixed the inner `CREATE`, then 7/11 cases
still failed with `216` at the `GRANT`):

1. **`InterpreterCreateQuery::createReplicatedDatabaseByClient`** — the inner
   `CREATE` (and `GRANT`) run on the cloned `new_context`, which inherits the
   caller's `query_id`. Fix: `new_context->setCurrentQueryId("")` after
   `setGlobalContext` / `setSetting("allow_distributed_ddl", 1)` and before the two
   `executeQuery` calls. (The `CREATE` deregisters its id before the `GRANT` reuses
   `new_context`, so one reset covers both of *these* two calls.)
2. **`InterpreterGrantQuery::execute`** (the deeper, primary site) — the
   `DEFAULT REPLICATED DATABASE PRIVILEGES` handler expands to a concrete `GRANT`
   string and runs it via a **nested** `executeQuery` on `getContext()` — i.e. the
   *outer* `GRANT`'s own still-registered context/id → self-collision. This fires
   for **any** caller of that block, including a *direct* `GRANT DEFAULT…` with no
   elevation (which is why standalone `test_c`/`test_h` failed too). Fix: run the
   nested query on a fresh-id copy —
   `auto grant_context = Context::createCopy(context); grant_context->setCurrentQueryId("");`
   then `executeQuery(default_grant_query, grant_context, {.internal = true})`. The
   copy inherits the (possibly elevated) access/settings unchanged.

General invariant: *any* internal `executeQuery` that may run while a parent query
of the same id is still registered must own a distinct `ProcessList` identity. This
patch's "query-within-a-query" design has two such sites on 26.3; both are now
covered.

**Validation (2026-06-03):** full module `test_aiven_indirect_database_creation` →
**11/11 PASS**, no regression (the 4 default-off/scope cases stay green; the 7
elevated/grant cases — incl. the hard gate `test_f_non_escalation` and
`test_h_grantee_injection` — flip to pass); `Code 216` absent from the log. Run via
the verified local-pytest path (`praktika` false-greens on a docker-in-docker pull
timeout — it reclassifies setup errors as "infrastructure", not failures).

### 2.X — Build hazard (no code defect): `#deps 0` stale link

Inserting a `Keyword` value in `src/Parsers/CommonParsers.h` shifts every
subsequent enum ordinal. ClickHouse's ninja `#deps 0` behavior does not reliably
recompile all header consumers incrementally, leaving the binary with a `Keyword`
enum ↔ string-table desync (symptom: all `GRANT`/`REVOKE` misparse; the server
even rejects the valid base-config `default_password_type=sha256_password`). The
**source is correct** — `clickhouse format` round-trips all `GRANT`/`REVOKE`
forms after a from-scratch recompile of `src/**`.

**Remedy — do NOT use `ninja -t clean`.** It deletes `ExternalProject` outputs
(e.g. the in-source-shipped `googleapis` protos extracted by `GoogleApis.cmake` at
configure time — see `file(ARCHIVE_EXTRACT)` there), which a plain `ninja` will
**not** repopulate, breaking the build at the `protoc` step
(`Could not make proto path relative: …/google/api/http.proto`). To force a
consistent recompile, either re-run CMake configure (`cmake <build_dir>`, which
re-extracts the protos and is network-free) before `ninja clickhouse`, or
`rm -rf <build_dir>` and reconfigure. A clobbered cmake cache can also masquerade
as a "consistent tree" (missing `rules.ninja` / wrong `CC`/`CXX`); a `ninja -n
clickhouse` dry-run probe catches it before a real build, and `cmake --fresh`
recovers it non-destructively (resets only config, keeps objects + protos).

## 3. C++ / security review

The review centers on the elevation, since that is the only privilege-boundary
change:

- **Elevation is clone-scoped.** `Context::createCopy(context)` →
  `setGlobalContext()` (clears `user_id`, `need_recalculate_access = true`) is
  applied to a local `new_context`, never to the session `context`. The session
  user's access is never widened. Case F is the empirical proof of non-leak.
- **Elevation is gated.** The indirect path only fires for
  `user_with_indirect_database_creation` (default `""` → no user matches → inert).
  A non-configured user (including a user literally named `avnadmin` when the
  setting is empty) takes the normal access path (case B, R).
- **The curated grant is a fixed allow-list**, never `CREATE DATABASE` /
  `ACCESS MANAGEMENT` / `SYSTEM SHUTDOWN` (asserted in case C).
- **Injection surfaces closed** for the generated SQL: database name
  `backQuote`'d, ZK path `escapeForFileName`'d, shard macro `escapeString`'d
  (B), grantee `backQuote`'d (A).
- **Process-list identity** isolated via the fresh `query_id` (2.G) so an
  internal elevated query never masquerades under the caller's registration.
- **Carried-as-intended (documented, not bugs):** reserved-prefix Replicated-only
  scope (C); distributed-DDL plumbing checks skipped on the elevated path while
  the `DROP DATABASE` privilege is still required (E).

Residual / auditability note: internal elevated queries run with
`internal = true`, so they do not appear in `system.query_log` under the invoking
user (the Q8 auditability gap). This is unchanged from the source and from
upstream internal-query behavior; flagged for the observability backlog, not a
blocker.

## 4. Test design

New integration module `tests/integration/test_aiven_indirect_database_creation/`
(Keeper-backed, since Replicated DB needs ZooKeeper/Keeper). `node_cfg` has the
feature configured (`configs/indirect_db.xml`: the three settings + a
`cluster_secret` named collection); `node_plain` is unconfigured to prove default
inertness. 11 cases (fails-before / passes-after evidence per AGENTS §7(a)):

| Case | Test | Asserts |
|---|---|---|
| A | `test_a_indirect_create_happy_path` | `avnadmin` simplified `CREATE` → Replicated DB with auto-filled ZK path, reused `{shard}`/`{replica}`; `avnadmin` receives the default grant `WITH GRANT OPTION`. |
| B | `test_b_normal_user_denied` | non-`avnadmin` user → indirect path does NOT fire → `CREATE DATABASE` refused. |
| C/G | `test_c_grant_default_privileges` | `GRANT DEFAULT REPLICATED DATABASE PRIVILEGES` parses + executes; curated set present; dangerous privileges absent. |
| D | `test_d_reserved_prefix_rejected` | reserved-prefixed names rejected for `avnadmin`; non-reserved accepted. |
| E | `test_e_on_cluster_enforced_for_non_admin_drop` | non-admin `DETACH` refused; non-admin `DROP` upgraded to `ON CLUSTER`. |
| **F** | `test_f_non_escalation` | **HARD GATE.** Indirect `CREATE` succeeds (no Code 216); inner `GRANT` landed (`db_f` in `SHOW GRANTS`); `avnadmin` session still NON-super (no `CREATE USER`, no unauthorized `SELECT`). |
| H | `test_h_grantee_injection` | finding A: comma-containing grantee lands on exactly one user; no spurious grantees. |
| D2 | `test_d2_reserved_prefix_scope` | finding C (intended): reserved-prefixed `ENGINE=Atomic` unguarded; default-engine path guarded. |
| E2 | `test_e2_drop_privilege_not_bypassed` | finding E (intended): `skip_distributed_checks` does not bypass the `DROP DATABASE` privilege. |
| R | `test_r_default_off_create` | feature unconfigured → upstream `CREATE` behavior; indirect path inert even for a user named `avnadmin`. |
| R2 | `test_r2_default_off_drop` | finding D / clause-(v): with `cluster_database` empty, non-admin `DROP` without `ON CLUSTER` behaves exactly as upstream (no `SETTING_CONSTRAINT_VIOLATION`). |

Case F's `GRANT`-landed assertion is deliberate: a second `query_id` collision
(2.G) would surface only on the inner `GRANT`, so F gates both elevated
statements. `aiven-core`'s own `test_user_create_database.py` /
`test_acl.py` independently drive this path and would also have caught the Code
216 regression — an external cross-check that the fix is necessary and
sufficient.

## 5. Rollback considerations

- Revert safety: the patch is self-contained behind the three default-`""`
  settings; reverting the commit removes all behavior. No data migration.
- **Config handover:** the feature is inert unless
  `user_with_indirect_database_creation`, `cluster_database`, and the
  `cluster_secret` named collection are provisioned (Aiven's managed config). The
  reference `cluster_database` (Replicated) and the `/clickhouse/databases/<db>`
  ZK path layout are load-bearing invariants shared with `aiven-core`.
- **Build:** any later change touching `CommonParsers.h` near this patch must be
  followed by a full clean rebuild (2.X).
- Chain: patch 020 extends case C's privilege set with `CHECK`; keep C in sync.
- Future-uplift watch: if a later upstream re-gates internal-query process-list
  registration, re-evaluate the 2.G `query_id` reset (it would become a harmless
  no-op, not a hazard).

## 6. Per-uplift notes

### 25.3-aiven / 25.8-aiven (historical)

Source `8ff9d3cb7ad` (25.3) and `05d8148a57` (25.8, author Tilman Moeller,
committer Aliaksei Khatskevich, 2025-12-13). Findings A–E present identically;
2.G could not occur because internal queries were not registered in the process
list on those bases.

### 26.3-aiven (this uplift)

- Cherry-pick: `git cherry-pick --no-commit -x 05d8148a57` against heavy drift
  (logs in `tmp/patch-019/`).
- Divergences from source (all documented above): A, B, D fixed forward; 2.G
  `setCurrentQueryId("")` added at **two** sites for 26.3 base drift
  (`createReplicatedDatabaseByClient` + the nested grant in
  `InterpreterGrantQuery::execute`). C, E carried as intended.
- Tests: integration module, 11 cases (+ case G folded into C). **11/11 PASS,
  no regression** (2026-06-03, local-pytest path); `Code 216` absent.
- Surprising bit: a faithfully-merged patch still broke purely from drift in an
  *adjacent* subsystem (`executeQuery`'s process-list insert gate, 2.G) — the
  patched lines were untouched. The tell was a Code 216 in cases that exercise
  the elevated/grant path, not a parse/compile error. And the first fix was a
  red herring's-worth incomplete: the *primary* collision was one frame deeper,
  in the nested expand-grant `executeQuery`, which also fires for a *direct*
  `GRANT DEFAULT…` with no elevation at all.

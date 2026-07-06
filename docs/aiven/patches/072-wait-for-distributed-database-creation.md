# Patch 072 — wait-for-distributed-database-creation

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | n/a (not carried — feature predates this line in current form) | n/a | n/a |
| 25.8-aiven | `92ab446d49a7f0bf492b43aae9a191674eb90a60` | Aliaksei Khatskevich (author) / Joe Lynch (committer) | original carry |
| 26.3-aiven | (staged) | T3.20 worker | conflict-resolved (body-hunk re-anchor over the `setCurrentQueryId` 26.3 fix) |

The current uplift's row stays "(staged)" until the human commits.

## 1. Purpose

`createReplicatedDatabaseByClient` (the Aiven indirect-database-creation path,
patch 019) issues an internal `CREATE DATABASE … ON CLUSTER` to materialise a
`Replicated` database on behalf of the configured `user_with_indirect_database_creation`
user. Before this patch the call was fire-and-forget: the result `BlockIO` was
discarded and its pipeline never drained, so the caller returned *success*
immediately — before creation had completed on all replicas — and any error
raised while the DDL executed on cluster hosts was silently swallowed. Aiven
needs the caller to (1) block until the database is created on every replica and
(2) receive forwarded DDL errors instead of a false success.

Source SHA on `v25.8.18.1-lts-aiven`: `92ab446d49a7f0bf492b43aae9a191674eb90a60`
(from `docs/aiven/uplifts/26.3/inventory.md` row 072).
Original author: `alex.khatskevich@aiven.io` (per `git log --format=%ae`).
Original purpose (verbatim):

```
Wait for distributed database creation

This commit changes client database creation logic:
1. wait till database created on all replicas
2. forward database creation errors to the caller
```

## 2. Upstream-drift findings

### Commands run

```bash
git cherry-pick --no-commit -x 92ab446d49a7f0bf492b43aae9a191674eb90a60
git diff --name-only --diff-filter=U          # -> only InterpreterCreateQuery.cpp
git patch-id --stable < src.patch ; git patch-id --stable < new.patch
diff <(grep -E '^[-+]' src.patch | grep -v '^[-+]\{3\}') \
     <(grep -E '^[-+]' new.patch | grep -v '^[-+]\{3\}')   # -> empty
```

(logs under `tmp/patch-072/`: `cherry-pick.log`, `patch-id-src.log`,
`patch-id-new.log`, `decomposition.log`, `drift-conclusion.txt`.)

### Findings

- Upstream changes to touched files between prior and current LTS:
  - `src/Interpreters/InterpreterCreateQuery.cpp`: the include block and the
    bare `executeQuery(create_db_query, …)` line are unchanged, BUT 26.3-aiven
    inserted a comment + `new_context->setCurrentQueryId("")` block immediately
    before the bare call (the query-id-collision fix that keeps the cloned
    context's internal queries from tripping the cross-user duplicate-id check
    in the 26.3 process list). This is the one source of textual drift.
- Upstream changes that touched the patch's behavior (symbols, error codes, callers):
  - `createReplicatedDatabaseByClient`: present at HEAD (`:2455`), single call
    site at `:2549` behind the `user_with_indirect_database_creation` gate
    (`:2535`). Unchanged in shape.
  - `executeQuery` 2-value overload still returns `std::pair<ASTPtr, BlockIO>`
    (`executeQuery.h:86`) → the structured binding compiles.
  - `QueryPipeline::getSharedHeader() const` → `SharedHeader` (`QueryPipeline.h:105`);
    `EmptySink(SharedHeader)` (`EmptySink.h:11`); `CompletedPipelineExecutor(QueryPipeline &)`
    (`CompletedPipelineExecutor.h:18`); `ErrorCodes::LOGICAL_ERROR` already used in
    this TU (`:2465`). No API rename.
- Conclusion: **`still-needed-but-rewrite`** — semantics unchanged; the cherry-pick
  body hunk could not apply cleanly because of the `setCurrentQueryId` re-anchor.
  The include hunk applied cleanly. The conflict was resolved by keeping HEAD's
  `setCurrentQueryId` block and placing the patch's drain block immediately below
  it (replacing the old bare `executeQuery` call). Decomposition (tier 2) shows
  the added/removed lines are byte-identical to source — only context shifted.

## 3. C++ review

Per `docs/aiven/skills/cpp-review-checklist.md`:

- 1 Lifetime + ownership: ✓ — `create_io` (a `BlockIO`) and the stack-local
  `CompletedPipelineExecutor executor(create_io.pipeline)` are scoped to the
  function and outlive nothing; the executor drives the locally-owned pipeline
  to completion and is destroyed at function exit.
- 2 Exception safety: ✓ — the change *adds* a throwing path on purpose
  (`executor.execute()` now propagates the forwarded DDL error, and the
  uninitialized-pipeline guard throws `LOGICAL_ERROR`). On throw the function
  returns no partial state of its own (the DDL's effect is whatever the cluster
  committed); the caller's normal `executeQuery` catch handling applies. The
  later `GRANT` simply does not run, which is correct — there is nothing to grant
  on a database that failed to create.
- 3 Thread-safety + concurrency: n/a — no shared state, no locks, no new thread;
  draining a pipeline synchronously on the calling thread. No sleep-based logic.
- 4 Performance + memory: n/a — this is a control-path (admin-issued
  `CREATE DATABASE`), not a per-row hot path. The added work is exactly the
  pipeline drain we now require for correctness.
- 5 Settings as public API: ✓ — `distributed_ddl_output_mode` (set to `"throw"`
  on the cloned internal context) is an existing core setting; no new setting is
  added. `allow_distributed_ddl` was already set on this context.
- 6 Error handling: ✓ — `ErrorCodes::LOGICAL_ERROR` is real and already used in
  this TU (`:2465`). The new guard message ("CREATE DATABASE ON CLUSTER returned
  uninitialized pipeline, distributed_ddl_task_timeout is likely set to 0") is
  distinctive. The forwarded error is the cluster's own DDL error (e.g.
  "Database collide_db already exists"), surfaced verbatim to the caller.
- 7 Upstream / vendored code: ✓ — only `src/Interpreters/InterpreterCreateQuery.cpp`
  is touched; no `contrib/**`, `.claude/**`, `.github/workflows/**`, or root
  `AGENTS.md`.
- 8 Behavior under settings: ✓ (clause-(v) "no new setting" rationale) — the
  patched lines are reachable ONLY via the existing
  `user_with_indirect_database_creation` gate, which defaults to `""` (OFF). With
  the feature off the code is inert (zero suite impact; upstream tests never set
  the gate). This is a *correctness fix on an already-opted-in path*, not a new
  default behavior, so the clause-(v) broad-blast-radius escalation does NOT
  fire: adding a second default-off gate would ship the buggy fire-and-forget
  behavior by default to exactly the deployments that enabled the feature.
  Ported as-is.

## 4. Test design

(a) **New integration test that fails on the parent commit and passes after the patch.**

- Test path: `tests/integration/test_aiven_wait_for_distributed_database_creation/`
  (`test.py` + `configs/wait_distributed_db.xml`), per the Aiven integration
  naming convention (`docs/aiven/runbooks/testing-suites.md §4.4`). Integration
  (not stateless) because the trigger needs a `Replicated` default database plus
  the `user_with_indirect_database_creation` server setting — unreachable from a
  shared default-configured stateless server (transport-follows-reachability,
  `testing-suites.md §3`).
- Single node + Keeper, configured for the indirect-create feature (`avnadmin` +
  `cluster_db`). `test_create_error_forwarded` pre-creates `collide_db` via the
  admin path, then has `avnadmin` issue `CREATE DATABASE collide_db` through the
  indirect path, which builds `CREATE DATABASE collide_db ON CLUSTER cluster_db
  ENGINE = Replicated(...)` (no `IF NOT EXISTS`). The "already exists" failure is
  raised **server-side while the ON CLUSTER DDL runs** (not by a client-side
  pre-check), so it only reaches the caller when the result pipeline is drained.
- Pre-patch run output (the FAIL — `tmp/patch-072/test-prepatch.log`):

  ```text
  FAILURES
  _________________________ test_create_error_forwarded __________________________
      err = node.query_and_get_error("CREATE DATABASE collide_db", user="avnadmin")
  ...
  E   helpers.client.QueryRuntimeException: Client expected to be failed but succeeded! stdout:
  ======================== 1 failed, 4 warnings in 11.52s ========================
  ```

  i.e. pre-patch the pipeline is never drained, `avnadmin`'s `CREATE` returns
  success, the collision error is lost, so `query_and_get_error` (which demands a
  failure) raises and the test FAILS.

- Post-patch run output (the PASS — `tmp/patch-072/test-postpatch.log`):

  ```text
  test_aiven_wait_for_distributed_database_creation/test.py::test_create_error_forwarded PASSED [100%]
  ======================== 1 passed, 3 warnings in 10.37s ========================
  ```

  i.e. post-patch `distributed_ddl_output_mode='throw'` + the pipeline drain
  forwards the "already exists" error to `avnadmin`, so `query_and_get_error`
  succeeds and the assertion (`"collide_db" in err and "already exists" in err`)
  holds.

- Why this test distinguishes the Aiven behavior from upstream (per AGENTS.md §7):
  the observable is *presence vs absence of a forwarded error*, which is exactly
  behavior-change 1. This is NOT a mere error-code assertion — pre-patch produces
  **no error at all** (the caller sees success), so the differential is the
  existence of the error, not its code.

- **Known coverage limitation** (per the dispatch): behavior-change 1's "wait for
  all replicas" sub-claim is not separately asserted — a single-node-per-instance
  harness has no multi-replica race to expose. The error-forwarding case, which
  requires the same pipeline-drain machinery, carries the evidence.

## 5. Rollback considerations

- Reverting is safe: no schema migration, no on-disk format change. The revert
  restores the fire-and-forget behavior; databases created while the patch was
  live remain valid (the patch only changes *when/whether the caller observes
  completion and errors*, not what is written).
- No new persistent state is introduced (no new ZK nodes, files, or caches beyond
  what the ON CLUSTER `CREATE DATABASE` already produces).
- To disable the new "wait + forward errors" behavior without rebuilding, turn off
  the whole feature by clearing the `user_with_indirect_database_creation` server
  setting (the gate); there is no finer-grained per-behavior setting (and none is
  warranted — see §3 item 8).

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — not carried on this line in the current form.

### 25.8-aiven (historical, may be empty)

Original carry at `92ab446d49a7f0bf492b43aae9a191674eb90a60` (author Aliaksei
Khatskevich, committer Joe Lynch, 2026-03-26).

### 26.3-aiven (this uplift)

- Cherry-pick was: **conflict-resolved**. The include hunk applied cleanly; the
  body hunk conflicted because 26.3 inserted the `setCurrentQueryId("")`
  query-id-collision fix between `allow_distributed_ddl` and the bare
  `executeQuery`. Resolution: keep HEAD's `setCurrentQueryId` block, place the
  drain block immediately below it (replacing the old bare call). The inserted
  lines are byte-identical to source (tier-2 decomposition empty).
- Upstream-drift conclusion: `still-needed-but-rewrite` (§2).
- Test added at: `tests/integration/test_aiven_wait_for_distributed_database_creation/`.
- Time-to-port: integration variant — 1 warm-cache incremental build (~90 s) plus
  2 worktree-flip incrementals (~72 s / ~57 s) and 2 Docker pytest runs (~10–12 s
  each). **warm-cache** build directory (sccache hot from prior session).
- Anything surprising: the post-patch pytest completed in ~10 s (fast container
  startup/reuse); the pre-patch FAIL surfaced as the helper's "Client expected to
  be failed but succeeded!" — the cleanest possible signature for an
  error-forwarding differential.

# Patch 005 — tolerate-zk-restart-with-exponential-backoff

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | (unknown — to be researched at 25.X dossier merge) | (unknown) | (predecessor of `6a3715017325`, if any) |
| 25.8-aiven | `6a3715017325c7953cb4959a2f6a8a652a54ea10` | Tilman Moeller (author), Aliaksei Khatskevich (committer), Kevin Michel (co-author) | the version we are porting FROM |
| 26.3-aiven | `patch-port(005)` | T3.8 + T3.9 workers | `conflict-resolved` (HUNK 1 manual; HUNKS 2 and 3 textual) / `byte_equivalent: false` (decomposition empty — only context-line shift) / `tests.added: yes` (integration test at `tests/integration/test_aiven_zk_connect_retry/`; T3.9 evidence pair) |

The 26.3-aiven carry is its `patch-port(005)` commit (find it with `git log --grep '^patch-port(005)'`); per `commit-hygiene.md` §4 the `-aiven-dev` branch is resliced at each LTS transition, so we pin the stable subject handle, not a SHA that would re-hash.
## 1. Purpose

When the ZooKeeper ensemble restarts — most commonly during a rolling
version upgrade where nodes restart sequentially — the cluster is
unavailable for ~6 seconds. Pre-patch, `ZooKeeper::connect` retries up to
~3 times in a tight loop with no inter-attempt sleep, so the total window
covered is ≈ DNS-resolution + TCP-timeout (single-digit seconds, dominated
by the OS' `connect(2)` timing). When the ZK unavailability window exceeds
this, the connect call returns failure and the DDL or replication operation
that triggered the connect propagates an error to the user. Because
ClickHouse DDL is not atomic against partial ZK writes, this can leave the
database in an inconsistent state mid-statement.

The patch widens the connect-retry window by floor-ing `num_tries` at 6
and inserting an exponential-backoff sleep between attempts
(100, 200, 400, 800, 1600, 3200 ms, capped at 10s). Total worst-case
retry-window after the patch is ≈ 6.3 seconds, which comfortably covers
the typical ZK-restart unavailability window. The patch is **defensive**;
it has no SQL-observable effect when ZK is reachable.

Source SHA on `v25.8.18.1-lts-aiven`: `6a3715017325c7953cb4959a2f6a8a652a54ea10` (from `docs/aiven/uplifts/26.3/inventory.md` row 005).
Original author: `Tilman Moeller <tilman.moeller@aiven.io>` (author date 2025-12-06).
Original purpose (quoted from source-commit body):

```
Tolerate ZooKeeper restart with increased retries and exponential backoff

When ZooKeeper restarts (especially during version upgrades), it can be
unavailable for approximately 6 seconds. ClickHouse previously failed
queries if ZooKeeper was not available within ~3 seconds, leading to
inconsistent database state because DDL operations are not fully atomic.

This change improves ZooKeeper connection resilience by:
- Increasing minimum retry attempts from 3 to 6
- Adding exponential backoff between retry attempts (100ms, 200ms, 400ms...)
- Capping maximum backoff at 10 seconds to prevent excessive delays

The total retry window now covers typical ZooKeeper restart times (~6
seconds), allowing ClickHouse to successfully reconnect after ZooKeeper
restarts without requiring manual intervention.

This is particularly important during version upgrades when ZooKeeper
nodes restart sequentially, as it prevents DDL operations from failing
mid-execution and leaving the database in an inconsistent state.

Co-authored-by: Kevin Michel <kevin.michel@aiven.io>
```

## 2. Upstream-drift findings

### Commands run

```bash
git grep -c -- '<identifier>' -- 'src/Common/ZooKeeper/'
  # for each of: min_num_tries, max_backoff_ms, milliseconds_to_wait,
  #              sleepForMilliseconds, cancelWriteBuffer, args.num_connection_retries
git log v25.8.18.1-lts..v26.3.10.62-lts \
    -- src/Common/ZooKeeper/ZooKeeperImpl.cpp --oneline
git log v25.8.18.1-lts..v26.3.10.62-lts \
    --grep 'ZooKeeper.*retry' --grep 'backoff.*ZooKeeper' \
    --grep 'connection_retr' --grep 'num_connection_retries' --oneline
sed -n '1,15p'    src/Common/ZooKeeper/ZooKeeperImpl.cpp   # HUNK 1 context
sed -n '508,524p' src/Common/ZooKeeper/ZooKeeperImpl.cpp   # HUNK 2 context
sed -n '618,628p' src/Common/ZooKeeper/ZooKeeperImpl.cpp   # HUNK 3 context
```

Logs: `tmp/patch-005/drift-identifiers.log`,
`tmp/patch-005/drift-file-history.log`,
`tmp/patch-005/drift-grep-history.log`,
`tmp/patch-005/drift-hunk{1,2,3}-context.log`.

### Findings

- Upstream changes to touched files between LTSes:
  - `src/Common/ZooKeeper/ZooKeeperImpl.cpp`: ~100 commits between
    `v25.8.18.1-lts` and `v26.3.10.62-lts`. The dominant series are
    aggregated-`zookeeper_log` instrumentation, watch deduplication,
    session-expiry plumbing, and one related-but-different connect-path
    fix (commit `14a7ce695a5c` "Fix obscure bug in ZooKeeper client on
    connect which leads to hungs and crashes", about exception handling
    inside the try block — NOT about retry/backoff).
- Upstream changes that touched the patch's behavior:
  - **None.** No upstream commit modifies `ZooKeeper::connect`'s retry
    loop, the `num_connection_retries` knob, or introduces inter-attempt
    backoff. The patch's behavioral change is unique to Aiven.
- New identifiers (`min_num_tries`, `milliseconds_to_wait`) are ABSENT on
  HEAD; `max_backoff_ms` exists only in
  `src/Common/ZooKeeper/ZooKeeperRetries.h` for the **independent**
  `ZooKeeperRetriesInfo` operation-retry layer (parent verified). The
  two retry mechanisms are not coupled.
- Hunk-context verification:
  - **HUNK 1** (source `@@ -1,3 +1,4 @@`): pre-image anchors
    (`<Common/ZooKeeper/ZooKeeperConstants.h>` followed by
    `<Compression/CompressedReadBuffer.h>`) do NOT match HEAD lines 1-4.
    HEAD's STL block (`<atomic>`/`<chrono>`/`<ranges>`) was introduced
    between LTSes; the source patch's insertion point is unreachable.
  - **HUNK 2** (source `@@ -490,8 +491,13 @@`): pre-image line
    `size_t num_tries = args.num_connection_retries + 1;` is at HEAD
    line 514, surrounded by matching context (`if (nodes.empty())
    throw …`, `bool connected = false;`, `bool dns_error = false;`).
  - **HUNK 3** (source `@@ -600,6 +606,8 @@`): pre-image line
    `cancelWriteBuffer();` is at HEAD line 622, inside the `catch (...)`
    block, with the preceding `fail_reasons << …` line matching verbatim.
- Conclusion: **`still-needed-but-rewrite`** — patch is still semantically
  required (no upstream equivalent); HUNKS 2 and 3 apply textually;
  HUNK 1 needs manual resolution due to include-block drift between
  LTSes.

## 3. C++ review

Per `docs/aiven/skills/cpp-review-checklist.md`:

- **1 Lifetime + ownership**: n/a — patch adds two function-local
  `static constexpr size_t` constants (`min_num_tries`, `max_backoff_ms`)
  and one mutable local (`milliseconds_to_wait`). No heap allocation, no
  pointer or reference introduced.
- **2 Exception safety**: ✓ — `sleepForMilliseconds` (defined at
  `src/Common/sleep.h`) wraps `nanosleep(2)` and does not throw; the
  surrounding `catch (...)` block is structurally unchanged, with the
  sleep added at the END of the catch body just before falling through
  to the next loop iteration. No new throw site is introduced; partial
  state in `fail_reasons` / `cancelWriteBuffer` is unchanged.
- **3 Thread-safety + concurrency**: ✓ — `sleepForMilliseconds` is
  per-thread (suspends only the calling thread). `connect` is called
  once per ZK session establishment; the local `milliseconds_to_wait`
  state is loop-local. **No sleep-based race-condition fix** — the sleep
  here is back-pressure between retries, not synchronization. Repo
  policy (`cpp-review-checklist.md` §3 "No sleep-based race-condition
  fixes") is honored.
- **4 Performance + memory**: note — adds up to
  `min_num_tries × max_backoff_ms / 2 ≈ 30s` of worst-case sleep
  inside a connection-retry loop. This is **intentional** (covers the
  6-second ZK-restart window with margin) and only occurs on the
  failure path. Happy-path overhead: zero (loop terminates on first
  successful iteration before the sleep executes). The connect path
  is not a per-row hot path; no `Chunk`/`IColumn` semantics involved.
- **5 Settings as public API**: note — the patch adds compile-time
  `static constexpr` constants, NOT user settings. `args.num_connection_retries`
  (the existing user-tunable knob) is preserved but now acts as a
  **lower-bound override only** via `std::max(min_num_tries, args.num_connection_retries + 1)`.
  A user who set `num_connection_retries=10` keeps 11 attempts (above
  the floor); a user who set `num_connection_retries=2` gets 6
  attempts (silently upgraded). This is a small but observable
  user-facing semantic widening — documented here in §3 because the
  setting acts as public API per the checklist's framing.
- **6 Error handling**: ✓ — no new error code. The catch block already
  swallows ALL exceptions (`catch (...)`) and routes them to
  `fail_reasons` for batched reporting; the patch only adds pacing
  between catches. If the inner code throws an exception **outside**
  the existing `try { ... } catch (...) { ... }` (e.g., `std::bad_alloc`
  from the constructor of an STL container in the loop body), behavior
  degrades to pre-patch tight-loop semantics — but this is not a
  regression because no such throw site exists in the loop body today
  (verified by source read of `ZooKeeper::connect`).
- **7 Upstream / vendored code**: ✓ — patch only touches
  `src/Common/ZooKeeper/ZooKeeperImpl.cpp`. No `contrib/`, `.claude/`,
  `.github/workflows/`, or root `AGENTS.md` involvement. The patch is
  an Aiven-only resilience-policy deviation, documented in the
  commit-message body.
- **8 Behavior under settings**: note — the patch widens existing
  `num_connection_retries` semantics by silently floor-ing the user
  value at 6 attempts. There is no gating setting that can disable
  the new behavior without rebuilding (compile-time constants). This
  is intentional per the patch's defensive framing: an operator
  cannot accidentally re-introduce the brittle behavior by setting
  `num_connection_retries=0`. Recorded here so that future
  documentation can mention the silent floor.
- **9 Parent preflight discipline** (parent-facing, recorded by worker
  per T3.5/T3.6 retrospective): ✓ — parent's pre-flight
  conclusions (HUNKS 2/3 apply, HUNK 1 needs rewrite due to
  include-block drift; new identifiers absent on HEAD; patch is
  `still-needed`) were verified independently by the T3.8 worker via the
  `(i)` identifier-grep, `(ii)` file-history scan, `(iii)`
  per-hunk-context check, and `(iv)` cherry-pick experiment. Parent's
  prior was correct in every detail except the predicted `+13/-1`
  staged stat (actual: +9/-1, because HUNK 2 is +6/-1 not +11/-1 —
  parent miscount on context-vs-added classification). The (i)/(ii)/(iii)
  discipline correctly predicted the mixed cherry-pick outcome (HUNK 1
  manual rewrite, HUNKS 2/3 textual). T3.8's subsequent `policy_call`
  escalation was NOT a parent-preflight failure: it surfaced a schema-fit
  issue (preconditions on `no_trigger_on_current_lts`) that is structurally
  separate from preflight prediction. Both findings are post-cherry-pick
  and confirm the discipline's robustness — preflight tells you what
  cherry-pick will do; schema-fit tells you what `tests.added` enum value
  is admissible; they are independent axes.

## 4. Test design

**Option (a) — New integration test that fails on the parent commit and passes after the patch.**

- Test path: `tests/integration/test_aiven_zk_connect_retry/`
- Test name: `test_clickhouse_tolerates_zk_briefly_unavailable_at_first_use`
- Convention: `test_aiven_<slug>/` per `docs/aiven/runbooks/testing-suites.md` §4.4 (second use after patch 006; this is the **second** integration test under the Aiven prefix, advancing rule-of-three GA-durability from 1/3 to 2/3).

### Test design (Shape A from the T3.8 → T3.9 policy_call discussion, with the T3.9 empirical refinement)

The connection-retry layer (`ZooKeeper::connect` in `src/Common/ZooKeeper/ZooKeeperImpl.cpp`) cannot be exercised from the stateless test runner — the runner uses a single persistent Keeper shared across all parallel tests, with no isolation primitive for per-test ZK-kill. The closest existing test (`tests/integration/test_inserts_with_keeper_retries/test_replica_inserts_with_keeper_restart`) exercises the BROADER ZK-transient-unavailability invariant but masks the connection-retry layer with `insert_keeper_max_retries=20` (operation-layer retries absorb the ZK-down window). Patch 005's specific behavior is therefore not covered by any existing test.

The new test isolates the connection-retry layer by exercising the **first** ZK use after a fresh server start. The empirical pre-patch / post-patch divergence sits on this exact path:

```
Coordination::ZooKeeper::connect
   <-- zkutil::ZooKeeper::create
   <-- DB::Context::getZooKeeper           [first-call branch, shared->zookeeper == nullptr]
   <-- DB::StorageReplicatedMergeTree::setZooKeeper
   <-- DB::StorageReplicatedMergeTree ctor
   <-- DB::InterpreterCreateQuery::doCreateTable
```

The test:
1. Pins `<zookeeper><num_connection_retries>2</num_connection_retries></zookeeper>` in the server config (pre-patch default; the patched code path silently floors this to `std::max(min_num_tries=6, 2+1=3) = 6`).
2. In a background thread, stops all 3 Keeper nodes, holds them down for 10 seconds, then restarts them.
3. Once the Keepers are observed stopped, `restart_clickhouse(kill=True)` on `node1`. The fresh server has `shared->zookeeper == nullptr`, and the readiness probe (`SELECT 20`) does not touch ZK, so the helper returns even while the Keepers are down.
4. Issues `CREATE TABLE r ... ENGINE = ReplicatedMergeTree(...)` — the first ZK-using DDL on the fresh server. This is the only path that exercises the patched `ZooKeeper::connect` retry loop.

### Test-design iterations within this dispatch (3-revision budget)

T3.9 burned all three revisions converging on the test above. Recorded here so the next worker who hits a similar shape doesn't repeat the dead ends:

- **Revision 0** — Restart `ZooKeeper::connect` while a `ReplicatedMergeTree` table exists on the server, `async_load_databases=false`. **Failed:** the load-thread ATTACH that calls `ZooKeeper::connect` gracefully puts the table in read-only mode on failure; the server still becomes ready and the helper returns success on both pre- and post-patch. The defended path is invoked but the failure is silently degraded, not propagated.
- **Revision 1** — Same shape but with a `Replicated` database engine. **Failed for the same reason:** the database load is graceful too. ZK is acquired lazily; nothing on the synchronous startup path actually throws when connect fails.
- **Revision 2** — Existing `test_inserts_with_keeper_retries` pattern (background ZK stop+restart while the server is running) but with `insert_keeper_max_retries=0` to disable operation-layer retries. **Failed in a different way:** by the time the INSERT runs (heartbeat-detected session loss is fast), the replica is already marked `is_readonly=true`, and `ReplicatedMergeTreeSink::commitPart` throws `TABLE_IS_READ_ONLY` before ever reaching `ZooKeeper::connect`. Both pre- and post-patch fail identically here; the patch does not change the read-only-mode check.
- **Revision 3 (shipped)** — Fresh server (so `shared->zookeeper == nullptr`) + first ZK use is `CREATE TABLE` of a new replicated table. Connect is the only ZK code that runs. Pre-patch: 3 tight retries × 3 nodes = 3 `Connection refused` lines in the exception message, ~100 ms total, `CREATE TABLE` fails with `Coordination::Exception: All connection tries failed`. Post-patch: backoff sleeps cover the 10 s hold; `CREATE TABLE` succeeds when the Keepers come back.

The mechanism-isolation principle is the same one called out in `docs/aiven/AGENTS.md` §7 for upstream-vs-Aiven gate distinction, generalised to "isolate the layer the patch modifies from any layer that gracefully absorbs the same failure" — a useful lens for any future patch in this neighbourhood.

### Pre/post evidence pair

**Post-patch run** (`tmp/patch-005/test-postpatch.log`):

```
test_aiven_zk_connect_retry/test.py::test_clickhouse_tolerates_zk_briefly_unavailable_at_first_use
... INFO : Stopping zookeeper node: zoo1 (12:51:38.800)
... INFO : Stopping zookeeper node: zoo2 (12:51:43.980)
... INFO : Stopping zookeeper node: zoo3 (12:51:44.157)
... INFO : Starting zookeeper node: zoo1 (12:51:54.699)  # ~10 s hold after first stop
PASSED
======================== 1 passed, 2 warnings in 29.81s ========================
pytest exit: 0
```

**Pre-patch run** (`tmp/patch-005/test-prepatch.log`):

```
test_aiven_zk_connect_retry/test.py::test_clickhouse_tolerates_zk_briefly_unavailable_at_first_use FAILED
E   Code: 999. DB::Exception: Received from 172.18.0.5:9000. Coordination::Exception. Coordination::Exception: All connection tries failed while connecting to ZooKeeper. nodes: 172.18.0.4:2181
E   Poco::Exception. Code: 1000, e.code() = 111, Connection refused (version 26.3.10.1), 172.18.0.4:2181
E   Poco::Exception. Code: 1000, e.code() = 111, Connection refused (version 26.3.10.1), 172.18.0.4:2181
E   Poco::Exception. Code: 1000, e.code() = 111, Connection refused (version 26.3.10.1), 172.18.0.4:2181  # exactly 3 attempts == num_connection_retries (2) + 1
E   ...
E    6. ./src/Common/ZooKeeper/ZooKeeperImpl.cpp:651:15: Coordination::ZooKeeper::connect(...) @ ...
E    7. ./src/Common/ZooKeeper/ZooKeeperImpl.cpp:456:9: Coordination::ZooKeeper::ZooKeeper(...) @ ...
E   11. ./src/Interpreters/Context.cpp:4928:33: DB::Context::getZooKeeper() const @ ...
E   12. ./src/Storages/StorageReplicatedMergeTree.cpp:325:37: DB::StorageReplicatedMergeTree::setZooKeeper() @ ...
E   17. ./src/Interpreters/InterpreterCreateQuery.cpp:2040:42: DB::InterpreterCreateQuery::doCreateTable(...) @ ...
E   . (KEEPER_EXCEPTION)
======================== 1 failed, 3 warnings in 28.79s ========================
pytest exit: 1
```

The pre-patch stack frame at `ZooKeeperImpl.cpp:651` is the post-loop throw site that fires when all `num_tries` attempts exhausted with no success. The three "Connection refused" entries match `num_tries = args.num_connection_retries + 1 = 3` exactly — pre-patch behaviour. Post-patch the same call site (now reached after the patched loop took its sleep budget) never throws because one of the 6+ attempts catches the Keepers coming back.

### Why this test distinguishes the Aiven gate from upstream behaviour

The patched code path (`min_num_tries = 6` floor + `sleepForMilliseconds(milliseconds_to_wait)` between attempts) is Aiven-only. Upstream's `ZooKeeper::connect` at `v26.3.10.62-lts` uses `args.num_connection_retries + 1` directly with no inter-attempt sleep. The test pins `num_connection_retries=2` and asserts SUCCESS for `CREATE TABLE` after the Keepers come back within ~10 s — pre-patch this is impossible (3 retries in ~ms give up at ~100 ms), post-patch this is precisely the patched loop's job. The test is **mechanism-isolating**: no operation-layer retries are involved, no read-only-mode short-circuit, no async load (the operation is a fresh DDL, not a load); a passing post-patch outcome can only be attributed to the patched connection-layer behaviour.

## 5. Rollback considerations

- **Revert is safe.** No schema migration, no on-disk format change, no
  ZK node introduced or removed. Reverting restores the pre-patch
  tight-loop retry behavior, which is correct under ZK-available
  conditions and degrades only under the ZK-restart-during-DDL
  scenario the patch is designed to mitigate.
- **No state survives a clickhouse-server restart.** The retry loop
  is internal to a single `ZooKeeper::connect` invocation.
- **No runtime opt-out.** The constants are compile-time. By design:
  the patch's intent is defensive resilience that cannot be
  accidentally disabled by an operator. The user-facing
  `num_connection_retries` setting acts as a lower-bound override (a
  value > 5 still wins; a value ≤ 5 is silently raised to 6).

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — predecessor (if any) not yet researched. The 25.X dossier merge
will populate this row.

### 25.8-aiven (historical, the version we ported FROM)

Carried as `6a3715017325c7953cb4959a2f6a8a652a54ea10`. Author: Tilman
Moeller; committer: Aliaksei Khatskevich; co-author: Kevin Michel.
Author date: 2025-12-06.

### 26.3-aiven (this uplift)

- **Cherry-pick was: conflict-resolved.** HUNK 1 conflicted on the
  include-block reorder between LTSes (HEAD's `<atomic>`/`<chrono>`/`<ranges>`
  STL block replaced 25.8's `<Common/ZooKeeper/ZooKeeperConstants.h>` as
  the leading include); HUNKS 2 and 3 applied textually via `git`'s
  three-way merge.
- Upstream-drift conclusion: `still-needed-but-rewrite` (from §2).
- Test added at: `tests/integration/test_aiven_zk_connect_retry/`
  (T3.9; second integration test under the Aiven prefix, rule-of-three
  progress 2/3 toward GA durability of the `test_aiven_<slug>/`
  convention). See §4 for design and pre/post evidence pair.
- Time-to-port (subagent wall-clock): ~25 minutes for T3.8 (cherry-pick
  + source build) on a half-broken build dir requiring `cmake --fresh`
  recovery (build itself took ~57s, warm-cache, one-file recompile;
  the dir matched `build-and-test.md §6` row 1, missing
  `CMakeFiles/rules.ninja`; cmake exit 0 in 12.3s configure + 2.9s
  generate). Then ~50-55 minutes for T3.9 (test authoring across 3
  design revisions + 2 worktree-flip rebuilds + 4 pytest cycles).
  Net dispatch cost: ~80 minutes wall vs ~25-30 min for a hypothetical
  single-dispatch port. The cost is amortized in two ways: (1) the
  build dir's `--fresh` recovery is one-time across the uplift;
  (2) T3.9's three rejected test designs are documented in §4 as
  "don't repeat these" guidance for future dispatches in the same
  patch-shape.
- **Anything surprising:**
  - **HUNK 1 manual resolution.** HEAD's include block was reorganized
    between LTSes — `<atomic>`/`<chrono>`/`<ranges>` were promoted to
    a leading STL-headers block, displacing 25.8's leading
    `<Common/ZooKeeper/ZooKeeperConstants.h>` to line 9. Resolved by
    inserting `<cstddef>` alphabetically between `<chrono>` and
    `<ranges>`. `size_t` resolves transitively on HEAD (HEAD's
    pre-patch code compiles without `<cstddef>` — any of the three
    STL headers brings `<cstddef>` along), so the include is
    **defensive rather than required**. Kept anyway to match the
    source patch's intent and minimize patch-id divergence (only
    context lines differ; decomposition empty per Tier 2).
  - **`byte_equivalent: false` is expected but decomposition is
    empty.** The patch-id-decomposition diff (sorted `+`/`-` lines)
    is empty; only context shifts. This is the expected
    `byte_equivalent: false` with green Tier 2.
  - **Schema-vs-dispatch escalation at T3.8, resolved at T3.9.** T3.8
    dispatched first with the parent-declared outcome
    `tests.added: no_trigger_on_current_lts` and escalated `policy_call`
    on a schema-vs-dispatch mismatch — the schema's preconditions for
    that enum value (constraint at `halt-and-escalate.md` line ~127:
    stateless test shipped + ≥10-candidate parameter sweep) are
    unsatisfiable for this patch (no parameter range; no SQL trigger).
    T3.9 followed the human's resolution path: write an integration
    test (Shape A) that isolates the connection-retry layer. The test
    produces a clean evidence-of-causation pair; the schema retains
    its current preconditions; no schema amendment was needed. Net
    throughput cost: ~80 minutes worker time across T3.8 + T3.9 (vs
    ~25-30 min for a single-dispatch port). Cost worth bearing: the
    dispatch surfaced the schema's `no_trigger_on_current_lts` value
    as gated for the patch-073-shaped case (enumerable parameter), not
    the patch-005-shaped case (external wall-clock trigger) — useful
    refinement for the schema's documented scope.
  - **Mechanism-isolation cost three test-design revisions.** §4 records
    the rejected shapes (read-only-mode short-circuit in revisions 0/1;
    `TABLE_IS_READ_ONLY` short-circuit in revision 2) and the winning
    shape (fresh server + first ZK use = new replicated DDL). The
    general lens — "isolate the layer the patch modifies from any layer
    that gracefully absorbs the same failure" — is worth promoting into
    the testing-suites runbook as a checklist item for any future
    connection-layer or retry-layer patch.
  - **First T3.x dispatch that DECOUPLES source-change cherry-pick from
    test authoring.** Patch 006 was the first integration test under
    the Aiven prefix but its cherry-pick and test were authored in the
    same dispatch. T3.8 → T3.9 is the first split: cherry-pick done at
    T3.8, test authored at T3.9 against the already-staged INDEX, with
    the worktree flipping in/out via `git restore --worktree` to
    produce the evidence pair. The flip mechanic works cleanly
    (worktree-vs-index diff was 0 lines on flip-back); it should be
    documented in the integration-tests runbook as the standard
    follow-up pattern when an earlier dispatch escalates on a
    test-design question.
  - **Parent preflight stat estimate was off.** Dispatch predicted
    `+13/-1` final stat; actual is `+9/-1`. Off by 4 lines on the
    addition side. Recount: HUNK 1 adds 1 line, HUNK 2 adds 6 and
    removes 1, HUNK 3 adds 2 — totals `+9/-1`. Documented for parent's
    preflight discipline calibration (a small but accumulating signal:
    parent's stat-prediction accuracy improves the more the worker
    surfaces miscounts).

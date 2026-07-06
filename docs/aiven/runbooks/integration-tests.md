# Runbook — running integration tests locally for the Aiven LTS uplift

**Scope.** This runbook is the durable how-to for running a single `tests/integration/<suite>/` test on the local dev machine without depending on the praktika CI wrapper, Docker-in-Docker, or any system-level changes (no `sudo`, no `iptables`, no sudoers entries).

It complements:

- `docs/aiven/runbooks/build-and-test.md` — covers stateless test running.
- `docs/aiven/runbooks/testing-suites.md` — covers _which_ test to write.
- `tests/integration/README.md` — CI-canonical description of the integration suite.

This runbook is the _local_ override: when CI integration tests work but you want to iterate on a single test against a freshly-built local binary in seconds instead of minutes, follow the steps here.

**Status convention.** Same as `testing-suites.md`. Each section is labeled VERIFIED `<ISO date>` (validated end-to-end against this repo) or PROVISIONAL (designed from first principles, awaiting first real use).

## 1. When to use this runbook

**Status: VERIFIED 2026-05-26**

Use this runbook when the patch under work satisfies any of:

- The user-observable effect requires a **server restart cycle** (loader-issued ATTACH, cache rebuild on startup, etc.).
- The trigger requires a **`DatabaseReplicated` cluster** with multiple nodes (so a DDL log entry executes as `SECONDARY_QUERY` on some replica).
- The trigger requires **external services** under controlled config: ZooKeeper / Keeper, MinIO, Kafka, MySQL, PostgreSQL, etc.

These are unreachable from `.sql` / `.sh` stateless tests by design (the stateless runner connects to a single shared default-configured server it does not own). The decision rule in `testing-suites.md` §2 routes such patches to integration. This runbook covers the local-iteration loop for those cases.

When the trigger _is_ reachable from stateless, stay with `.sql` / `.sh` — it's much cheaper to iterate on and runs in CI by default.

## 2. Prerequisites

**Status: VERIFIED 2026-05-26**

The machine must already have:

| Component | Verification command | What we observed |
|---|---|---|
| Docker daemon | `docker info` exits 0 | OK on this host; user is in the `docker` group |
| Built ClickHouse binary | `build/programs/clickhouse --version` | RelWithDebInfo (or whatever flavor is appropriate) |
| Python 3.13 | `/usr/bin/python3 --version` | 3.13.x (the integration helpers import `dict2xml`, `kazoo`, `docker`, etc., from this interpreter) |
| `/usr/bin/pytest` | `pytest --version` | 8.x; shebang `/usr/bin/python3 -P` |

The CI-canonical Python environment is the full `ci/docker/integration/runner/requirements.txt` (~109 entries). For local single-test smokes, only a small subset is needed — see §3.

## 3. One-time setup (user-local, ~80 MB, fully reversible)

**Status: VERIFIED 2026-05-26 against `test_materialized_view_restart_server` (single node, no ZK) and `test_aiven_replicated_database_attach_with_shard_macro` (two nodes + Keeper).**

```bash
/usr/bin/python3 -m pip install --user \
  docker \
  pytest-xdist pytest-timeout pytest-reportlog \
  dict2xml kazoo minio \
  pandas
```

Installs to `~/.local/lib/python3.13/site-packages/`. Removable any time with `pip uninstall <pkg>`.

**Why these eight, and not the full 109?** The conftest at `tests/integration/conftest.py` plus `helpers/cluster.py` plus `helpers/client.py` only import these eight non-stdlib third-party packages at module load time. Tests that exercise other services (Kafka, MongoDB, Iceberg, Spark, etc.) lazy-import their own dependencies on first use and will fail with a clear `ModuleNotFoundError` if you try to run them without installing the relevant package. When that happens, install the missing package and retry — the iteration loop is much faster than a full requirements install up front.

**Gotcha — namespace-package mirage.** Running `python -c "import docker"` _from the repository root_ appears to succeed even without the package installed. This is because the repository has a `docker/` directory (Dockerfile bases for ClickHouse builds), which Python 3 treats as an implicit namespace package (PEP 420). `import docker` returns a placeholder with `docker.__file__ == None` and no submodules. Verify the real package is installed by running `python -c "import docker; print(docker.__version__)"` from `tests/integration/` (where the placeholder doesn't fire). After the install above, this should print `7.x.y`.

## 4. Per-session prep

**Status: VERIFIED 2026-05-26**

Praktika and prior Docker-in-Docker runs leave root-owned residue in `ci/tmp/` and `tests/integration/.pytest_cache/`. Remove what would block a fresh run (no `sudo` required — these files live in directories we own, and `rm` requires write on the _parent_ directory, not the file):

```bash
rm -f ci/tmp/parallel.log ci/tmp/pytest_parallel.log ci/tmp/pytest_parallel.jsonl \
      ci/tmp/pytest_parallel-gw1.log ci/tmp/docker-in-docker.log
# Per-test instance dirs may also be root-owned; safe to leave (containers replace them on next start).
```

The repository's `.gitignore` excludes `ci/tmp/*`, so any logs that praktika writes there will not show up in `git status`.

## 5. Run a single integration test

**Status: VERIFIED 2026-05-26**

From `tests/integration/`:

```bash
export CLICKHOUSE_TESTS_BASE_CONFIG_DIR=$(git rev-parse --show-toplevel)/programs/server
export CLICKHOUSE_TESTS_SERVER_BIN_PATH=$(git rev-parse --show-toplevel)/build/programs/clickhouse
export CLICKHOUSE_BINARY=$(git rev-parse --show-toplevel)/build/programs/clickhouse
export CLICKHOUSE_TESTS_CLIENT_BIN_PATH=$(git rev-parse --show-toplevel)/build/programs/clickhouse
export PYTEST_CLEANUP_CONTAINERS=1

pytest <test_dir> -v --tb=short --timeout=240
```

Replace `<test_dir>` with the test you want (e.g. `test_materialized_view_restart_server`). Add `-k <expr>` to filter by test function within the suite.

The four `CLICKHOUSE_TESTS_*` env-vars and `PYTEST_CLEANUP_CONTAINERS` are the same set praktika exports at `ci/jobs/integration_test_job.py` ~ line 620. We bypass praktika entirely because:

- The praktika wrapper image (`clickhouse/integration-tests-runner:<hash>_arm`) is per-commit-tagged; locally we typically only have `:latest` cached. A `docker pull` of the per-commit tag can hang silently if the local docker can't reach the registry.
- The `sudo iptables -D DOCKER-USER 1 ||:` cleanup at the end of praktika's job script requires `sudo` (the `||:` swallows non-zero from iptables but not from sudo's TTY prompt). Single-node restart-class tests don't add the iptables rule, so the cleanup is a no-op anyway when bypassed.
- The `dmesg -T > ci/tmp/dmesg.log` step requires kernel access — irrelevant for single-test smokes.

Calling pytest directly skips all three. The trade-off is that praktika's parallel batching, retry logic, and result reporting are off — fine for single-test iteration, not fine for whole-suite runs (those should still use praktika or CI).

### Worked example, `test_materialized_view_restart_server` (single node, no ZK)

- Runtime: ~41 seconds wall-clock from `pytest` invocation to `1 passed`.
- Outcome: one ClickHouse container starts, runs a CREATE TABLE + materialized view + restart sequence, all containers torn down.
- Non-fatal warnings: `Failed to run sysctl, tests may fail with EADDRINUSE` (the cluster helper tries to widen the local port range; no root → falls back; irrelevant for single-test runs).

### Worked example, `test_aiven_replicated_database_attach_with_shard_macro` (two nodes + Keeper)

- Runtime: ~42 seconds wall-clock post-patch (PASS); ~99 seconds pre-patch (FAIL).
- Outcome: three Keeper containers + two ClickHouse containers start; CREATE DATABASE testdb ENGINE = Replicated(...); CREATE TABLE testdb.t ENGINE = ReplicatedMergeTree ORDER BY x; INSERT + cross-node SELECT verification; `node.restart_clickhouse(kill=True)` on each node; post-restart SELECT.
- Pre-patch failure mode: `Code: 139. DB::Exception: No macro 'shard' in config while processing substitutions in '/clickhouse/tables/{uuid}/{shard}' ... Cannot attach table testdb.t ... (NO_ELEMENTS_IN_CONFIG)` — server load job fails, `restart_clickhouse` raises `Exception: Cannot start ClickHouse, see additional info in logs`.

## 6. Test naming convention for Aiven-specific integration tests

**Status: VERIFIED-with-discipline 2026-05-28** (promoted from VERIFIED-with-precedent at the third independent application; see rule-of-three counter below).

Aiven-specific integration tests under `tests/integration/` MUST be placed in a directory named `test_aiven_<slug>/`. This is the integration-test analogue of the `9<NNN>_<slug>` numeric prefix used for Aiven-specific stateless tests. The full convention — motivation, hard constraints, and worked examples — lives in `docs/aiven/runbooks/testing-suites.md §4.4` (which is the canonical home for both stateless and integration test naming conventions); this section is a pointer, not a duplicate.

Short version for the impatient reader:

- New test directory: `tests/integration/test_aiven_<slug>/`
- `<slug>` matches the patch dossier slug (without the leading `NNN-`).
- Examples (all three VERIFIED in this uplift):
  - `tests/integration/test_aiven_replicated_database_attach_with_shard_macro/` ↔ `docs/aiven/patches/006-replicated-database-attach-with-shard-macro.md` (2-node DatabaseReplicated cluster; loader-issued ATTACH on server restart).
  - `tests/integration/test_aiven_zk_connect_retry/` ↔ `docs/aiven/patches/005-tolerate-zk-restart-with-exponential-backoff.md` (1-node + 3-Keeper; startup-while-ZK-down).
  - `tests/integration/test_aiven_refreshable_mv_shard_macro_expansion/` ↔ `docs/aiven/patches/049-refreshable-mv-shard-macro-expansion.md` (1-node DR; macro-expansion in `RefreshTask::RefreshTask` triggered by `CREATE MATERIALIZED VIEW ... REFRESH`).
- Rule-of-three GA-durability counter for the convention itself: **3 of 3 → VERIFIED-with-discipline**. Each of the three test directories exercises a structurally distinct topology and trigger shape (2-node + Keeper + restart, 1-node + 3-Keeper + ZK-kill, 1-node + DR + DDL-only). The convention is now load-bearing institutional knowledge — codifying the rename of any future Aiven integration test that lands under a non-`test_aiven_` directory is a one-line ask, not a debate.

## 7. Patterns for Aiven integration tests

**Scope.** This section codifies recurring shapes for writing and dispatching Aiven integration tests under the `test_aiven_<slug>/` convention (§6). Each subsection is independently labeled VERIFIED or PROVISIONAL based on how many real T3.x dispatches have used it; promote PROVISIONAL → VERIFIED on the second observed use.

### 7.1 Test-shape templates

**Status: VERIFIED 2026-05-27** against patch 006 (`test_aiven_replicated_database_attach_with_shard_macro/`, 2-node + Keeper) and patch 005 (`test_aiven_zk_connect_retry/`, 1-node + 3-Keeper).

Both Aiven integration tests so far share a load-bearing element: **`node.restart_clickhouse(kill=True)` as the trigger**. The bug being defended is reached AFTER a restart, not during steady-state operation. The reason is structural — most Aiven patches are about server-lifecycle resilience (startup loader paths, fresh-session establishment, cache rebuild on restart) which are unreachable from a `.sql` runner that doesn't own the server's lifecycle.

Two recurring sub-shapes within this trigger:

| Sub-shape | Cluster topology | Pre-restart setup | Post-restart trigger | Example |
|---|---|---|---|---|
| **A. Pre-existing-state restart** | 1-3 nodes + ZK | Create DB + tables + insert data while everything is healthy | Restart CH; the bug fires when the loader RE-attaches the existing tables | patch 006 (`test_aiven_replicated_database_attach_with_shard_macro`) |
| **B. Fresh-session restart** | 1-3 nodes + ZK | Establish baseline (`SELECT 1`); make some external service flaky (e.g., `cluster.stop_zookeeper_nodes(...)`) in a background thread that restores it later | Restart CH; the bug fires when the startup sequence tries to ESTABLISH a fresh session against the flaky service | patch 005 (`test_aiven_zk_connect_retry`) |

**Shared idiomatic pieces** (lift from these into a new `test_aiven_<slug>/test.py`):

```python
import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/<your_config>.xml"],   # custom server config — almost always needed
    with_zookeeper=True,
    stay_alive=True,                              # MANDATORY for restart_clickhouse(kill=True)
)
# Add node2, node3 if your bug is cluster-level (DDL log propagation, replica election, ...).

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_<descriptive_name>(start_cluster):
    # Baseline: prove the cluster is healthy before perturbing.
    assert node1.query("SELECT 1").strip() == "1"

    # Perturbation: either set up state (Sub-shape A) or break a service (Sub-shape B).
    # ...

    # Trigger: the restart that fires the bug.
    node1.restart_clickhouse(kill=True)

    # Assert: query the cluster post-restart. Use assert_eq_with_retry for
    # signals that have catch-up latency (replica sync, ZK reconnect, etc.).
    assert_eq_with_retry(
        node1, "SELECT <something>", "<expected>\n",
        retry_count=20, sleep_time=1,
    )
```

**Sub-shape B specific: the background-thread idiom** for "service-down-during-restart" tests, lifted from upstream `tests/integration/test_inserts_with_keeper_retries/test.py` lines 47-69 and adapted in patch 005:

```python
import time
from multiprocessing.dummy import Pool

cluster.stop_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])

p = Pool(1)
def restore_zk_after_delay():
    time.sleep(N)   # N tuned to your patch's retry window
    cluster.start_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])

job = p.apply_async(restore_zk_after_delay)
try:
    node1.restart_clickhouse(kill=True)   # blocks until CH is ready or gives up
finally:
    job.wait()
    p.close()
    p.join()
```

The `try/finally` is load-bearing: if `restart_clickhouse(kill=True)` raises (the pre-patch FAIL case), the background thread MUST still finish — otherwise it leaks across tests and breaks the module fixture's `shutdown()`.

**Why `restart_clickhouse(kill=True)` and not `restart_clickhouse()`** — the `kill=True` variant sends SIGKILL instead of SIGTERM, so the server can't flush graceful-shutdown state to disk. This matters when the bug is in the LOAD path (Sub-shape A): a graceful shutdown might persist enough state to mask the bug on the next startup; SIGKILL produces a cold-start reload.

### 7.2 Mechanism-isolation: testing the layer the patch actually modifies

**Status: PROVISIONAL** (1 instance from T3.9 dispatch for patch 005; promote to VERIFIED on the second observed use).

ClickHouse has multiple graceful-degradation layers between a user-facing surface (SQL query, server start, table read) and the low-level code a patch typically modifies. When designing a test for a patch in a "deep" layer (connection retries, session establishment, codec selection, etc.), it is easy — and the T3.9 worker observed this empirically across three rejected test shapes — to write a test whose user-facing assertion is **gracefully absorbed by a layer ABOVE the patched one**. The test then passes pre-patch AND post-patch, distinguishing nothing.

**The rule.** If three test shapes have failed to produce a pre/post divergence, you're testing the wrong layer. Find the ONLY path from a user-observable surface to the patched code that has **no fall-back**, **no soft-handler**, **no retry above the patched layer**, and assert success on a setup the unpatched code provably cannot satisfy.

**Worked example (T3.9 / patch 005, connection-retry layer in `ZooKeeper::connect`).** Three rejected test shapes:

| Rejected shape | Why it passed pre-patch (masking the patch) |
|---|---|
| **R1.** Pre-create a `ReplicatedMergeTree`; restart CH while ZK is down. | Load-thread soft-handler: when ZK is unavailable, the table goes into **read-only mode** and the LOAD completes (with a degraded table). `restart_clickhouse(kill=True)` returns normally. The test's post-restart `SELECT` succeeds against the in-memory state. The patched `ZooKeeper::connect` retry-with-backoff was never on the critical path. |
| **R2.** Create a `Replicated()` database. | The `Replicated` engine has its own startup retry layer for DDL log catch-up; it absorbs the ZK-down window. |
| **R3.** Run `INSERT INTO repl_table` with `insert_keeper_max_retries=0`. | Even with operation-layer retries off, the existing ZK session was still alive (not yet expired); the INSERT failed for a different reason (`TABLE_IS_READ_ONLY` from R1's degradation) that pre-patch and post-patch share. |

**The winning shape (Sub-shape B above).** Fresh server + first ZK use = `CREATE TABLE r ENGINE = ReplicatedMergeTree(...)` immediately after `restart_clickhouse(kill=True)`. This puts the patched code on the **only path** between the test query and a result:

```
test query → InterpreterCreateQuery::doCreateTable
           → StorageReplicatedMergeTree::setZooKeeper
           → Context::getZooKeeper
           → ZooKeeper::create
           → ZooKeeper::connect    ← patched layer; no fall-back from here.
```

The pre-patch stack trace at `tmp/patch-005/test-prepatch.log` shows exactly this chain (frames 6, 7, 10, 11, 17), with nine `Connection refused` lines matching `num_tries × num_keepers = (num_connection_retries + 1) × 3 = 3 × 3 = 9` precisely (the connect loop tries each of the 3 Keeper endpoints in turn, three rounds deep, before giving up).

**Why the rule-of-three threshold matters here.** PROVISIONAL: 1 worked example is suggestive, not conclusive — there may be patches in other layers (codec, network, storage) where the principle applies differently. Promote to VERIFIED only after a second derived test confirms the lens generalizes outside the connection-retry case.

### 7.3 Decoupled cherry-pick / test-authoring dispatch

**Status: PROVISIONAL** (1 instance from T3.8 → T3.9 follow-up; promote to VERIFIED on the second occurrence).

The default T3.x dispatch shape is "one worker, one patch port, source + test + dossier staged at the end". For most patches this is right. For integration-test patches where the parent's preflight UNDER-estimates the test-design cost — or where the worker discovers a `test_design_blocked` / `policy_call` issue mid-dispatch — a **decoupled dispatch pattern** is cheaper than re-dispatching the whole port. The pattern:

1. **T3.X (cherry-pick dispatch)**: worker cherry-picks the source change, drafts the dossier with `tests.added: <best-guess-enum>`, escalates the test-design question (`escalation_reason: policy_call` or `test_design_blocked`). Source change stays staged in the INDEX; dossier stays staged as new file; the worker does NOT touch the test (test files are NOT staged).
2. **Parent + human review the escalation**, decide on the test-design path (integration test / `no_justified` with upstream cite / amend schema / etc.).
3. **T3.X+1 (test-authoring dispatch)**: worker inherits the staged INDEX from T3.X (source + dossier still staged), authors the test, runs the evidence-of-causation pair using the **worktree-flip mechanic** below, updates the dossier §4, stages the new test files. Final staged file count grows from 2 to 4.

**The worktree-flip mechanic** (load-bearing for the pre/post evidence pair):

```bash
# Save the staged change as a reference patch — useful for debugging if flip-back fails.
git diff --cached -- <patched-file> > tmp/patch-NNN/staged-patch.diff

# Flip-out: restore the WORKTREE to HEAD. The INDEX is untouched —
# `git restore --worktree` only moves the worktree, not the staging area.
git restore --worktree -- <patched-file>

# Verify the patch is absent from the worktree:
rg '<patched-symbol>' <patched-file> || echo "VERIFIED: patch absent"

# Rebuild — incremental (~30-60 s warm-cache for a typical 10-LOC patch).
ninja -C build clickhouse

# Run the integration test — expect FAIL (this is the pre-patch evidence half).
cd tests/integration
pytest test_aiven_<slug>/ -v --tb=short --timeout=240 2>&1 | tee ../../tmp/patch-NNN/test-prepatch.log

cd $(git rev-parse --show-toplevel)

# Flip-back: re-apply the staged change to the WORKTREE. `git checkout -- <path>`
# copies the INDEX version back to the worktree.
git checkout -- <patched-file>

# Verify the patch is back in the worktree:
rg -c '<patched-symbol>' <patched-file>   # > 0

# Rebuild.
ninja -C build clickhouse

# (Run the test again with `--tee tmp/patch-NNN/test-postpatch.log` if you didn't already.)

# Final invariant: worktree must match index.
git diff <patched-file> | wc -l   # must be 0
```

**Why `git restore --worktree` and `git checkout -- <path>` and NOT `git stash`** — `git stash` moves staged content OUT of the workflow, requiring a subsequent `git stash pop` that can produce merge conflicts if the worktree drifted. The `restore --worktree` + `checkout` pair never touches the INDEX, so the staged change is the source of truth across both pre-patch and post-patch test runs. The only state that moves is the worktree's copy of the file — a single byte-for-byte swap, no merge logic, no conflict surface.

**Why decouple in the first place** — the alternative is re-dispatching the whole port, which means re-cherry-picking, re-resolving any conflicts, re-running the build, re-running drift analysis. For T3.8 → T3.9 (patch 005), the decoupled cost was ~80 min worker time across two dispatches; the re-dispatch alternative would have been ~110+ min (the cherry-pick + Tier 1/2 verification + build ate ~30 min of T3.8 that the re-dispatch would have to repeat). The pattern saves ~30 min per re-dispatch and crisply attributes evidence (the source change is from T3.X's `cherry-pick.log`; the evidence pair is from T3.X+1's `test-{pre,post}patch.log`).

**Worked example.** Patch 005: T3.8 (cherry-pick + dossier draft + `policy_call` escalation on `tests.added: no_trigger_on_current_lts` schema mismatch) → human decides Shape A integration test → T3.9 (test authoring + worktree-flip evidence pair). Commit `a591b4331d8` is the resulting single patch-port commit (4 files: source + dossier + 2 test files).

**Promotion criterion.** The pattern stays PROVISIONAL until a second T3.X → T3.X+1 decoupled dispatch lands cleanly with a 4-file staged result. On promotion, the section can be tightened by removing the worked-example narrative (it'll be in the retrospectives by then).

### 7.4 Known gotchas

**Status: PROVISIONAL** — each gotcha below is recorded at n=1 of observation. Items graduate from PROVISIONAL to VERIFIED on third independent reproduction. They are codified here despite the low counter because (a) operators will hit them again, (b) the resolution is mechanical once you know what to look for, and (c) deferring the knowledge until rule-of-three would lose it to retro-archaeology. PROVISIONAL marker means "trust the resolution, but the *frequency* of the gotcha is not yet established".

#### 7.4.a `keeper_randomize_feature_flags` flip causes `Code: 999` flakes

**Observed:** T3.14 (patch 049) integration test `test_aiven_refreshable_mv_shard_macro_expansion/`. Counter: **1 of 3**.

**Symptom.** Test run produces `Code: 999. DB::Exception: ... TIMEOUT_ERROR` or similar Keeper-RPC failures intermittently on otherwise-identical runs against the same binary. `system.zookeeper_log` shows the request was issued but received no response within the per-RPC timeout.

**Mechanism.** `tests/integration/helpers/cluster.py` flips Keeper feature flags **per-test-run** when `keeper_randomize_feature_flags=True` (the default for cluster-helper-managed Keepers). The randomization toggles experimental flags like `multi_read`, `filtered_list`, `check_not_exists`, etc. Some flags' implementations on Keeper 26.3 still have race conditions that surface only under specific topologies (e.g., a `Replicated` database doing rapid CREATE/DROP cycles).

**Resolution.** Pin the relevant feature flags explicitly via `keeper_required_feature_flags=[...]` on the `cluster.add_instance(...)` call:

```python
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/refreshable_mv.xml"],
    with_zookeeper=True,
    stay_alive=True,
    keeper_randomize_feature_flags=False,            # turn OFF the randomization
    keeper_required_feature_flags=["multi_read"],    # turn ON the flags the test actually needs
)
```

Set `keeper_randomize_feature_flags=False` AND list the specific flags the test relies on under `keeper_required_feature_flags`. The two settings together produce a deterministic Keeper config across runs.

**Why not just unconditionally pin all flags.** The test would then drift away from the customer's actual cluster shape (Aiven customers' Keeper configs do not enable every experimental flag). Pinning only the flags the test trigger depends on keeps the test's environment close to production.

**Promotion criterion.** Second independent reproduction of a `keeper_randomize_feature_flags` flake in a different Aiven integration test would lift counter to 2 of 3 and consolidate the resolution into the §7.1 test-shape template. Third reproduction would graduate to VERIFIED-with-discipline.

#### 7.4.b `StrReplace` tool fails with `spawn E2BIG` on large source files

**Observed:** T3.15 (patch 042) during the Egyptian → Allman brace reformatting in `src/Storages/StorageReplicatedMergeTree.cpp` (~12 K LOC). Counter: **1 of 3**.

**Symptom.** The agent's `StrReplace` tool returns `spawn E2BIG` when invoked on a file whose total content (or the `old_string` argument) exceeds the OS argv size limit (~128 KB on Linux). The error is opaque from the tool's perspective; the worker observes only an unexplained failure.

**Mechanism.** `StrReplace` forks a child process with the file content embedded in the argv. Linux `execve(2)` rejects the call when the argv block exceeds `MAX_ARG_STRLEN` (default 32 pages = 128 KB on a 4 K-page system) or the cumulative arg list exceeds `ARG_MAX` (typically 2 MB). Multi-thousand-LOC files like `StorageReplicatedMergeTree.cpp`, `MergeTreeData.cpp`, `Context.cpp` reproducibly trip the limit when the entire file is passed as a `replace_all` target.

**Resolution.** For large-file edits, drive the substitution via a Python script invoked through the `Shell` tool, e.g.:

```bash
python3 - <<'EOF'
from pathlib import Path
import re

p = Path('src/Storages/StorageReplicatedMergeTree.cpp')
text = p.read_text()
# Example: Egyptian → Allman braces for a specific function.
new = re.sub(
    r'(\)\s*const)\s*\{',
    r'\1\n{',
    text,
)
p.write_text(new)
EOF
```

The script reads/writes the file in-process (no argv constraint) and lets the worker use Python's mature regex engine for surgical edits. Verify the diff is what you expected with `git diff <file> | head` immediately after.

**Why not chunk the StrReplace.** Chunking introduces ordering hazards (the second `StrReplace` cannot reliably find its target if the first changed line numbers); also, the OS limit is on `old_string` length, so a chunk-based approach still fails when a single chunk is too large.

**Promotion criterion.** Second independent reproduction on a different large file (T3.X+) would lift to 2 of 3; third would graduate to VERIFIED. At that point the Python-shell pattern can be tightened into a named recipe in the runbook.

## 8. What this runbook does NOT cover

**Status: VERIFIED 2026-05-26** (each line below was observed during the integration-tests bring-up; treat as "known scope gaps", not as PROVISIONAL).

- **Full-suite parallel runs.** This runbook is for single-test iteration. For whole-suite runs, use praktika (`python -m ci.praktika run "integration" --test <selector>`) or CI.
- **Tests that simulate network partitions** (PartitionManager, `iptables -D DOCKER-USER 1` rule). These add a host-level firewall rule whose cleanup needs `sudo`; not relevant to restart-class or pure-cluster-topology tests.
- **Tests that need services not in our minimal pip set** (Kafka, MongoDB, MySQL client, Spark, Iceberg, Cassandra, ...). Expect a `ModuleNotFoundError` from the test's `helpers/<service>.py` import; install the relevant package from `ci/docker/integration/runner/requirements.txt` to proceed.
- **LLVM coverage runs** (`it-%4m.profraw` artifacts, profdata merging). The `--llvm-coverage` praktika job option is irrelevant for local single-test iteration.
- **dmesg-based OOM detection.** Praktika scrapes `dmesg` after each run; we skip this. If you suspect an OOM, run `dmesg -T` manually.

## 9. Troubleshooting

**Status: VERIFIED 2026-05-26**

### Symptom: `praktika run integration` hangs silently for 5+ minutes

Likely cause: docker pull of `clickhouse/integration-tests-runner:<hash>_<arch>` is hanging because the per-commit hash isn't cached locally and the registry pull is misbehaving.

Resolution: either (a) tag the cached image to match — `docker tag clickhouse/integration-tests-runner:latest clickhouse/integration-tests-runner:<hash>_<arch>`, (b) pass `--no-docker` to praktika, or (c) call pytest directly per §5.

### Symptom: `Permission denied: ci/tmp/parallel.log`

Cause: prior DinD run left the file root-owned.

Resolution: `rm -f ci/tmp/parallel.log` (we own the parent dir; `rm` doesn't require write on the file).

### Symptom: `ModuleNotFoundError: No module named 'docker'` from inside conftest

Cause: `docker` Python package not actually installed; the repo-root `docker/` directory was masking this as an implicit namespace package.

Resolution: run the §3 pip install. Verify with `python -c "import docker; print(docker.__version__)"` _from `tests/integration/`_ (not from the repo root).

### Symptom: `Code: 36. ... It's not allowed to specify explicit zookeeper_path and replica_name for ReplicatedMergeTree arguments in Replicated database`

Cause: A test creates a `ReplicatedMergeTree(...)` table with explicit `zookeeper_path` / `replica_name` arguments inside a `Replicated` database. In ClickHouse 26.3+ this is rejected by default.

Resolution: drop the explicit arguments. The synthesized path (default `default_replica_path` setting, `/clickhouse/tables/{uuid}/{shard}`) already includes `{shard}`, which is the macro most patches care about. If the test specifically needs to assert a custom path, enable the setting `database_replicated_allow_replicated_engine_arguments` for that query — but verify the test isn't drifting away from the customer's real shape by doing so.

### Symptom: `sysctl: permission denied on key "net.ipv4.ip_local_port_range"`

Cause: `tests/integration/conftest.py:57 tune_local_port_range` tries to widen the port range for parallel test runs and fails without root.

Resolution: non-fatal. The warning is logged once at session setup; single-test runs are unaffected. Ignore.

## 10. What was actually run when this runbook was authored

For audit purposes, the first run that promoted §3-§8 from PROVISIONAL to VERIFIED:

- Date: 2026-05-26.
- Machine: linux 6.18.4 aarch64, Fedora 42 host.
- Built binary: `build/programs/clickhouse` (3.9 GB, RelWithDebInfo, single-binary multi-target build with `clickhouse-keeper` symlinked).
- Tests run: `test_materialized_view_restart_server::test_materialized_view_with_subquery` (PASS, 40.89s), `test_aiven_replicated_database_attach_with_shard_macro::test_restart_attaches_replicated_table_with_shard_macro` (PASS post-patch in 42s, FAIL pre-patch in 99s with the documented `Code: 139` exception).
- Outcome: §3-§8 promoted to VERIFIED; integration tests are now part of the Aiven LTS uplift toolkit.

## 11. Mentor lesson (per the C++ Architect rule)

**Intuition.** A stateless `.sql` test is a unit test for the SQL surface; an integration test is a system test for the cluster. The choice is dictated by the trigger, not by preference: anything requiring a second process, server restart, or non-default config goes integration.

**Mechanism.** Stateless tests connect to a long-running default-configured server _shared with other tests in the same shard_. The runner doesn't own the server's lifecycle and cannot restart it, change its config, or add a peer. Integration tests, in contrast, are pytest sessions that start their own Docker containers (`docker compose` under the hood), one container per `cluster.add_instance(...)`, with custom configs, custom macros, and freedom to call `node.restart_clickhouse(kill=True)`. The cost is ~30-60 s startup overhead per test (container start + Keeper election + DB initialization) versus ~1 s for stateless.

**System consequence.** A patch that fixes a server-startup bug (loader-issued ATTACH, cache rebuild on startup, etc.) MUST have an integration test, because the trigger is unreachable from `.sql`. Conversely, a patch whose effect is purely a query result MUST have a `.sql` test — using integration would be ~30× slower per CI run and would camouflage real regressions in CI infrastructure noise.

**Today you learned.** That the docker namespace-package mirage (repo's `docker/` directory creating a fake `import docker` from repo root) is _the_ most insidious gotcha for first-time local integration test runners — the symptom looks like a working install until you `cd` into a subdirectory.

**Rule of thumb.** Always verify pip-installed packages from the directory where pytest will actually run them, not from the repo root.

**Next rabbit hole.** The praktika `--no-docker` path: when does it work and when does the wrapper container provide real value? (Answer outline: the wrapper buys you the runner's specific Python env + iptables tooling + dmesg scraping; we don't need any of that for single-test iteration but absolutely do for full-suite CI.)

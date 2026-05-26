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

**Status: VERIFIED 2026-05-26**

Aiven-specific integration tests under `tests/integration/` MUST be placed in a directory named `test_aiven_<slug>/`. This is the integration-test analogue of the `9<NNN>_<slug>` numeric prefix used for Aiven-specific stateless tests. The full convention — motivation, hard constraints, and worked examples — lives in `docs/aiven/runbooks/testing-suites.md §4.4` (which is the canonical home for both stateless and integration test naming conventions); this section is a pointer, not a duplicate.

Short version for the impatient reader:

- New test directory: `tests/integration/test_aiven_<slug>/`
- `<slug>` matches the patch dossier slug (without the leading `NNN-`).
- Example: `tests/integration/test_aiven_replicated_database_attach_with_shard_macro/` ↔ `docs/aiven/patches/006-replicated-database-attach-with-shard-macro.md`.

## 7. What this runbook does NOT cover

**Status: VERIFIED 2026-05-26** (each line below was observed during the integration-tests bring-up; treat as "known scope gaps", not as PROVISIONAL).

- **Full-suite parallel runs.** This runbook is for single-test iteration. For whole-suite runs, use praktika (`python -m ci.praktika run "integration" --test <selector>`) or CI.
- **Tests that simulate network partitions** (PartitionManager, `iptables -D DOCKER-USER 1` rule). These add a host-level firewall rule whose cleanup needs `sudo`; not relevant to restart-class or pure-cluster-topology tests.
- **Tests that need services not in our minimal pip set** (Kafka, MongoDB, MySQL client, Spark, Iceberg, Cassandra, ...). Expect a `ModuleNotFoundError` from the test's `helpers/<service>.py` import; install the relevant package from `ci/docker/integration/runner/requirements.txt` to proceed.
- **LLVM coverage runs** (`it-%4m.profraw` artifacts, profdata merging). The `--llvm-coverage` praktika job option is irrelevant for local single-test iteration.
- **dmesg-based OOM detection.** Praktika scrapes `dmesg` after each run; we skip this. If you suspect an OOM, run `dmesg -T` manually.

## 8. Troubleshooting

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

## 9. What was actually run when this runbook was authored

For audit purposes, the first run that promoted §3-§8 from PROVISIONAL to VERIFIED:

- Date: 2026-05-26.
- Machine: linux 6.18.4 aarch64, Fedora 42 host.
- Built binary: `build/programs/clickhouse` (3.9 GB, RelWithDebInfo, single-binary multi-target build with `clickhouse-keeper` symlinked).
- Tests run: `test_materialized_view_restart_server::test_materialized_view_with_subquery` (PASS, 40.89s), `test_aiven_replicated_database_attach_with_shard_macro::test_restart_attaches_replicated_table_with_shard_macro` (PASS post-patch in 42s, FAIL pre-patch in 99s with the documented `Code: 139` exception).
- Outcome: §3-§8 promoted to VERIFIED; integration tests are now part of the Aiven LTS uplift toolkit.

## 10. Mentor lesson (per the C++ Architect rule)

**Intuition.** A stateless `.sql` test is a unit test for the SQL surface; an integration test is a system test for the cluster. The choice is dictated by the trigger, not by preference: anything requiring a second process, server restart, or non-default config goes integration.

**Mechanism.** Stateless tests connect to a long-running default-configured server _shared with other tests in the same shard_. The runner doesn't own the server's lifecycle and cannot restart it, change its config, or add a peer. Integration tests, in contrast, are pytest sessions that start their own Docker containers (`docker compose` under the hood), one container per `cluster.add_instance(...)`, with custom configs, custom macros, and freedom to call `node.restart_clickhouse(kill=True)`. The cost is ~30-60 s startup overhead per test (container start + Keeper election + DB initialization) versus ~1 s for stateless.

**System consequence.** A patch that fixes a server-startup bug (loader-issued ATTACH, cache rebuild on startup, etc.) MUST have an integration test, because the trigger is unreachable from `.sql`. Conversely, a patch whose effect is purely a query result MUST have a `.sql` test — using integration would be ~30× slower per CI run and would camouflage real regressions in CI infrastructure noise.

**Today you learned.** That the docker namespace-package mirage (repo's `docker/` directory creating a fake `import docker` from repo root) is _the_ most insidious gotcha for first-time local integration test runners — the symptom looks like a working install until you `cd` into a subdirectory.

**Rule of thumb.** Always verify pip-installed packages from the directory where pytest will actually run them, not from the repo root.

**Next rabbit hole.** The praktika `--no-docker` path: when does it work and when does the wrapper container provide real value? (Answer outline: the wrapper buys you the runner's specific Python env + iptables tooling + dmesg scraping; we don't need any of that for single-test iteration but absolutely do for full-suite CI.)

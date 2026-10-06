# PromQL verification harness: what we inherit from the snapshot port, and what we must build

Refs:
- `PIN` = `290adb52c4ee238c9f3f0b557d6f2ca9bb3b29ff` (`upstream/master`)
- `OURS` = `HEAD` = `a5cc61775e76faa6d608dc505e1d59abbecc6d25`

All analysis read-only; no tracked file modified.

---

## 1. Test file inventory — `tests/integration/test_prometheus_protocols/`

| Ref | File count |
|---|---|
| OURS | **13** |
| PIN | **33** |

The brief's "13" for OURS is **confirmed**. The brief's "34" for PIN is **refuted**: `git ls-tree -r --name-only 290adb52c4e -- tests/integration/test_prometheus_protocols/` returns **33** paths.

There are **no OURS-only files** — our directory is a strict subset of PIN's.

### PIN-only files (20)

Test modules (13):
```
test_async_insert.py
test_compliance.py
test_format_query_api.py
test_http_port.py
test_insert_select.py
test_labels_api.py
test_label_values_api.py
test_metadata_api.py
test_prometheus_query_log.py
test_query_api.py
test_query_cache.py
test_series_api.py
test_upgrade_from_prealpha.py
```

Support files (7):
```
generate_compliance_data.py            (200 lines)
update_compliance_baseline.py          (205 lines)
configs/backups_disk.xml
configs/config.d/query_log.xml
configs/http_port.xml
configs/select_join_settings.xml
backups/time_series_prealpha.zip       (binary fixture for test_upgrade_from_prealpha)
```

Note: `test_evaluation.py`, `test_write_read.py`, `test_different_table_engines.py`, `prometheus_test_utils.py`, `configs/prometheus.xml` exist at both refs but diverge substantially (see §3).

---

## 2. The compliance test — `tests/integration/test_prometheus_protocols/test_compliance.py` at PIN

636 lines — the brief's line count is **confirmed**.

### 2.1 Data generation

Data comes from an **in-tree deterministic generator**, `generate_compliance_data.py` (PIN-only, 200 lines). The test imports `generate as generate_openmetrics` and `BASE_TIME` from it, calls it in the module fixture to write `compliance_data.om` next to the test file, then parses that OpenMetrics text back with a local regex parser (`parse_openmetrics_file`, `_METRIC_LINE_RE`, `_LABEL_RE`) into `(labels_dict, {ts: value})` tuples.

Synthetic dataset shape:
- `BASE_TIME = 1700000000`; `DATA_START = BASE_TIME - 4200` (70 min); `DATA_INTERVAL = 15s` → 281 timestamps.
- 3 instances `demo.promlabs.com:10000/10001/10002`, `job="demo"`.
- Seven metric families, all closed-form (sine/linear, **no randomness**):
  - `demo_memory_usage_bytes` gauge, `type` in free/buffers/cached
  - `demo_cpu_usage_seconds_total` counter, `mode` in idle/user/system
  - `demo_num_cpus` gauge, constant 4.0
  - `demo_disk_usage_bytes` gauge, linearly increasing
  - `demo_batch_last_success_timestamp_seconds` gauge, steps every 300s
  - `demo_api_request_duration_seconds_bucket` histogram counter, `le` in 0.001/0.01/0.1/0.5/1/5/10/+Inf × `method` in GET/POST, exponential CDF
  - `demo_intermittent_metric` gauge **with deliberate gaps** (present only in the first 60s of every 120s cycle)
- Ends with `# EOF`.

Ingestion (`_ingest_openmetrics`) sends the same batches of 50 series via Remote Write **twice** — once to the reference Prometheus receiver and once to ClickHouse `:9093/write` — using `convert_time_series_to_protobuf` / `send_protobuf_to_remote_write` from `prometheus_test_utils.py`. Labels are sorted lexicographically because Remote Write requires it.

### 2.2 Cluster setup

```python
cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_read=(9093, "/read"),
    handle_prometheus_remote_write=(9093, "/write"),
    with_prometheus_receiver=True,
)
```

`with_prometheus_receiver=True` is **confirmed**. There is **no** `with_prometheus_writer` and **no** `with_prometheus_reader` in this test — the receiver alone is the reference PromQL engine (it is started with `--enable-feature=remote-write-receiver --web.enable-remote-write-receiver --enable-feature=promql-experimental-functions`).

The fixture creates the table with a bare `CREATE TABLE prometheus ENGINE=TimeSeries`.

### 2.3 Where the 539-query corpus comes from

**Neither checked in as a data file nor downloaded at runtime.** It is **hand-vendored as Python source inside the test itself**:

- `COMPLIANCE_TEST_CASES` — a list of **118** `(query_template, variant_args, should_fail)` tuples, transcribed from `https://github.com/prometheus/compliance/blob/main/promql/promql-test-queries.yml`.
- `VARIANT_VALUES` — a dict reimplementing upstream's `promql/testcases/expand.go` placeholder sets (`range`, `offset`, `simpleAggrOp`, `simpleTimeAggrOp`, `topBottomOp`, `quantile`, `arithBinOp`, `compBinOp`, `binOp`, `simpleMathFunc`, `extrapolatedRateFunc`, `clampFunc`, `instantRateFunc`, `dateFunc`, `smoothingFactor`, `trendFactor`).
- `_expand_query` / `_expand_all_test_cases` do the cartesian expansion of `{{.name}}` placeholders.

I evaluated the expansion offline: **118 templates → 539 expanded queries, all distinct, of which 5 carry `should_fail=True`.** So the "539-query corpus" is **confirmed** and it lives entirely in `test_compliance.py` lines ~160–330. There is **no** `promql-test-queries.yml` anywhere in either tree, and no network fetch. Consequence: the corpus is a maintenance liability (it silently drifts from upstream prometheus/compliance) but it is also fully hermetic.

`smoothingFactor` / `trendFactor` are defined in `VARIANT_VALUES` but unused by any template (no `holt_winters` / `double_exponential_smoothing` case) — dead entries.

### 2.4 Scoring logic and `FLOAT_MARGIN`

```python
QUERY_START = BASE_TIME - 120
QUERY_END   = BASE_TIME
QUERY_STEP  = 60          # → 3 evaluation points per query

FLOAT_FRACTION = 0.00001
FLOAT_MARGIN   = 0.0001
```

Every query is a **range query** (`/api/v1/query_range`) with those bounds against both engines.

Tolerance (`_values_approx_equal`) mirrors upstream's `EquateApprox + EquateNaNs`:
- NaN == NaN → equal
- ±Inf equal only if same sign
- otherwise `abs(a-b) <= FLOAT_FRACTION * max(abs(a),abs(b)) + FLOAT_MARGIN`

`compare_results` requires exact `resultType` match, then:
- `scalar`: exact timestamp, approx value
- `matrix`/`vector`: series lists sorted by `_sort_key` (sorted metric label items); **exact series count**, **exact metric label set**, **exact sample count**, **exact timestamps**, approx values.

Classification per query (`ComplianceResult`):
- `ref_failed != should_fail` → `record_fail` and skip (reference disagreement, counted as a failure)
- `should_fail and test_failed` → pass; `should_fail and not test_failed` → fail
- `test_failed` and error text contains `"not implemented"` or `"501"` → **`record_unsupported`**
- other ClickHouse error → fail
- results compared → pass or fail with a diff message

`score = passed / (passed + failed + unsupported) * 100`. Note `unsupported` sits in the denominator, so unimplemented functions depress the score rather than being skipped.

The test also prints a category breakdown (`FAILURE BREAKDOWN BY CATEGORY`) keyed by regex over the failure reason: `Function X is not implemented`, `Aggregation operator 'X' is not implemented`, `Prometheus query node type X is not implemented`, plus buckets for `should_fail mismatch`, `reference mismatch`, `Number of values (0)`, `Quantile level is out of range`, `result value/count mismatch`, `other`.

### 2.5 NO-ASSERTION VERDICT — **CONFIRMED**

`test_promql_compliance` contains **zero `assert` statements** and cannot fail on a score regression. It is a reporting harness only. The final block of the function body, verbatim:

```python
    breakdown = {cat: len(entries) for cat, entries in categories.items()}
    out_path = os.environ.get("COMPLIANCE_RESULT_FILE")
    if out_path:
        record = {
            "passed": result.passed,
            "failed": result.failed,
            "unsupported": result.unsupported,
            "total": result.total,
            "pct": round(result.score, 4),
            "breakdown": breakdown,
        }
        with open(out_path, "w") as out_f:
            json.dump(record, out_f, indent=2)
            out_f.write("\n")

    print()
```

The only ways this test can fail are infrastructure faults (cluster start, `CREATE TABLE`, ingestion, or `open()` on `COMPLIANCE_RESULT_FILE`). Every PromQL divergence, every unimplemented function, and a score of 0% all still exit green. Also note: if `COMPLIANCE_RESULT_FILE` is unset the JSON is not even written, so a local run produces nothing machine-readable.

Also worth flagging for the gate design (§4): the persisted `record` carries only **aggregate counts plus per-category counts**. The per-query `(query, reason)` pairs in `result.failures` are printed to stdout and then **discarded** — nothing durable identifies *which* queries failed.

### 2.6 Baseline: S3, not in-tree

The brief's claim is **confirmed and refined**. There is no committed baseline file anywhere. The comparison lives in CI:

- `ci/jobs/promql_compliance_job.py` — the diff job.
- `ci/jobs/scripts/job_hooks/promql_compliance_s3.py` — path/bucket helpers.
- `ci/jobs/scripts/job_hooks/promql_compliance_upload_hook.py` — post-hook on every Integration tests batch.
- `ci/jobs/scripts/job_hooks/promql_compliance_comment_hook.py` — posts the GitHub comment.

**Exact bucket and paths** (`ci/defs/defs.py:13`: `S3_BUCKET_HTTP_ENDPOINT = "clickhouse-builds.s3.amazonaws.com"`):

```
master baseline:  https://clickhouse-builds.s3.amazonaws.com/REFs/master/<40-char sha>/promql_compliance/promql_compliance_result.json
PR result:        https://clickhouse-builds.s3.amazonaws.com/PRs/<pr>/<40-char sha>/promql_compliance/promql_compliance_result.json
```

Constants: `S3_KEY_DIR = "promql_compliance"`, `RESULT_NAME = "promql_compliance_result.json"`.

Flow and diff logic:
1. `ci/jobs/integration_test_job.py:1879-1881` exports `COMPLIANCE_RESULT_FILE` (default `<temp>/promql_compliance_result.json`) into the pytest env, so the batch that happens to run `test_compliance` drops the JSON.
2. `promql_compliance_upload_hook.py` runs as a post-hook after **every** integration batch. If the file exists: on upstream `master` (`should_publish_master_baseline`: `pr_number <= 0` and branch `master` and repo `ClickHouse/ClickHouse`) it uploads to `REFs/master/<sha>/…`; on a PR it uploads to `PRs/<pr>/<sha>/…`. Missing file = silent skip.
3. `promql_compliance_job.py` (job `PROMQL_COMPLIANCE`, `runs_on=ARM_TINY`, `run_after` all integration jobs, `timeout=600`, `enable_gh_auth=True`, **`allow_failure=True`**) fetches the PR JSON, then walks `master_track_commits_sha` (falling back to a `gh api` merge-base + 30-commit walk) and takes the **first master object that exists** as baseline via `fetch_baseline_from_s3`. Missing baseline → `_ZERO_BASELINE` (`pct=0, passed=0, failed=0, unsupported=0`) and `from_zero=True`.
4. Diff is a pure subtraction written to `ci/tmp/promql_compliance_comment.json`: `delta = new_pct - base_pct`, plus `cur_/base_` passed/failed/unsupported.
5. `promql_compliance_comment_hook.py` renders a markdown table with `_EPS = 1e-4` and three branches: no baseline / improved (suggests bumping) / below baseline. The regression branch says, verbatim:

   > `"\nNote: score is below the chosen baseline (informational only; this job does not enforce a hard floor).\n"`

**So the whole upstream pipeline is advisory end to end.** The job is `allow_failure=True`, the test has no assertion, and the comment hook explicitly disclaims enforcement. Additionally the job is gated on the PR carrying the `comp-promql` label (`Labels.COMP_PROMQL`, `ci/jobs/scripts/workflow_hooks/pr_labels_and_category.py:97`; skipped otherwise per `filter_job.py:327-334`), so by default nobody even sees the comment.

Aiven-relevant caveats: the S3 bucket is `ClickHouse/ClickHouse`-owned, and `should_publish_master_baseline` hard-codes `UPSTREAM_REPO = "ClickHouse/ClickHouse"` and `MASTER_BRANCH = "master"`. **A fork can never publish a baseline through this path.** Any Aiven gate must therefore be in-tree or use our own storage.

---

## 3. Our support for the harness (at OURS) — and how it diverges from PIN

### 3.1 `tests/integration/helpers/cluster.py` — `with_prometheus_receiver`

Supported, but **the brief's line 2379 is the flag-handling line, not the declaration**. Real locations at OURS:

Signature (`add_instance`), lines 1955-1957:
```python
        with_prometheus_writer=False,
        with_prometheus_reader=False,
        with_prometheus_receiver=False,
```
So `with_prometheus_writer` **and** `with_prometheus_reader` both exist at OURS too.

Handling, lines 2375-2391:
```python
        if with_prometheus_writer:
            self.prometheus_servers.append('writer')
        if with_prometheus_reader:
            self.prometheus_servers.append('reader')
        if with_prometheus_receiver:
            self.prometheus_servers.append('receiver')
        if handle_prometheus_remote_write:
            self.prometheus_remote_write_handlers.append((instance.hostname,) + handle_prometheus_remote_write)
        if handle_prometheus_remote_read:
            self.prometheus_remote_read_handlers.append((instance.hostname,) + handle_prometheus_remote_read)
        if self.prometheus_servers:
            cmds.append(
                self.setup_prometheus_cmd(
                    instance, env_variables, docker_compose_yml_dir
                )
            )
```

State, lines 872-884 (scalar per-role attributes):
```python
        # available when with_prometheus == True
        self.with_prometheus = False
        ...
        self.prometheus_writer_host = "prometheus_writer"
        self.prometheus_writer_port = 9090
        self.prometheus_writer_ip = None
        self.prometheus_reader_host = "prometheus_reader"
        self.prometheus_reader_port = 9091
        self.prometheus_reader_ip = None
        self.prometheus_receiver_host = "prometheus_receiver"
        self.prometheus_receiver_port = 9092
        self.prometheus_receiver_ip = None
        self.prometheus_servers = []
```

IP resolution at start, lines 3328-3335: `self.prometheus_receiver_ip = self.get_instance_ip(self.prometheus_receiver_host)` then `wait_for_url(.../api/v1/status/runtimeinfo)`.

### 3.2 `tests/integration/compose/docker_compose_prometheus.yml`

Exists at OURS, 119 lines, one file with three services `prometheus_writer` / `prometheus_reader` / `prometheus_receiver`. **Image pinned: `prom/prometheus:v3.5.0`** for all three (identical to PIN).

Receiver service (the one the compliance test needs), verbatim:
```yaml
  prometheus_receiver:
    image: prom/prometheus:v3.5.0
    hostname: ${PROMETHEUS_HOSTNAME:-prometheus_receiver}
    restart: always
    entrypoint: |
      /bin/sh -c '
      if [ -z ${PROMETHEUS_RECEIVER_PORT} ]; then
        exit 0
      fi
      cat << EOF > /etc/prometheus/prometheus.yml
      storage:
        tsdb:
          out_of_order_time_window: 100y
      EOF
      exec /bin/prometheus --config.file="/etc/prometheus/prometheus.yml" --storage.tsdb.path="/prometheus" --web.listen-address="0.0.0.0:${PROMETHEUS_RECEIVER_PORT}" --storage.tsdb.retention.time=100y --enable-feature=remote-write-receiver --web.enable-remote-write-receiver --enable-feature=promql-experimental-functions &> /var/log/prometheus/prometheus.log
      '
    expose:
      - ${PROMETHEUS_RECEIVER_PORT}
    healthcheck:
      test: |
        /bin/sh -c '
        if [ -z ${PROMETHEUS_RECEIVER_PORT} ]; then
          exit 0
        fi
        curl -f "https://localhost:${PROMETHEUS_RECEIVER_PORT}/api/v1/status/runtimeinfo" || exit 1
        '
      interval: 5s
      timeout: 3s
      retries: 30
    volumes:
      - type: ${PROMETHEUS_RECEIVER_LOGS_FS:-tmpfs}
        source: ${PROMETHEUS_RECEIVER_LOGS:-}
        target: /var/log/prometheus
    stop_grace_period: 5s
    cpus: 3
```

Bug worth noting even at OURS: the healthcheck uses **`https://`** against a plaintext listener, so it can never succeed. Because a healthcheck exists at all, Docker marks the container unhealthy; the cluster works only because `cluster.py` does its own `wait_for_url` over `http://`. PIN's per-service files simplified the healthcheck to a one-liner but **kept the same `https://` mistake** in writer/reader, and **fixed it to `http://` in the receiver file**.

### 3.3 Diff of the two fixtures against PIN — **upstream changed them in ways that break the compliance test**

`git diff HEAD upstream/master` summary:
```
tests/integration/compose/docker_compose_prometheus.yml          |  119 --   (deleted upstream)
tests/integration/helpers/cluster.py                             | 1319 +++++++++-----
tests/integration/test_prometheus_protocols/configs/prometheus.xml |  80 +-
tests/integration/test_prometheus_protocols/prometheus_test_utils.py | 112 +-
```

**(a) Compose file split.** PIN has no `docker_compose_prometheus.yml`. It has three files:
```
tests/integration/compose/docker_compose_prometheus_reader.yml
tests/integration/compose/docker_compose_prometheus_receiver.yml
tests/integration/compose/docker_compose_prometheus_writer.yml
```
`setup_prometheus_cmd` gained a `prometheus_server` parameter and appends one `--file docker_compose_prometheus_{role}.yml` per requested role, so only the roles a test asks for are started. Ours starts all three services from one file (the `if [ -z ${..._PORT} ]; then exit 0; fi` guard is how unused roles no-op).

**(b) Accessor API changed from scalars to dicts.** PIN, `cluster.py:951-955`:
```python
        self.prometheus_host = "prometheus"
        self.prometheus_port = {"writer": 9090, "reader": 9091, "receiver": 9092}
        self.prometheus_ip = {"writer": None, "reader": None, "receiver": None}
        self.prometheus_logs_dir = "prometheus_{}/logs"
        self.prometheus_servers = set()
```
and `cluster.py:3655-3658`:
```python
        for prometheus_server in self.prometheus_servers:
            ip = self.get_instance_ip(f"{self.prometheus_host}_{prometheus_server}")
            self.prometheus_ip[prometheus_server] = ip
            self.wait_for_url(f"http://{ip}:{self.prometheus_port[prometheus_server]}/api/v1/status/runtimeinfo")
```

`test_compliance.py` uses exactly this new API:
```python
        send_protobuf_to_remote_write(
            cluster.prometheus_ip["receiver"],
            cluster.prometheus_port["receiver"],
            ...
```

**OURS has no `prometheus_ip` / `prometheus_port` attributes at all** — only `prometheus_receiver_ip` / `prometheus_receiver_port`. So dropping PIN's `test_compliance.py` into our tree raises `AttributeError` at ingestion time. Our five existing tests use the old scalar names (`test_write_read.py:57,68`, `test_different_table_engines.py:27,71,88`, `test_evaluation.py:26,37,49,94,103`).

**(c) `prometheus_test_utils.py` diverged (+112 lines net).** PIN adds `convert_metrics_metadata_to_protobuf`, `compress_remote_write_request`, and changes signatures the compliance test does not need but sibling PIN tests do: `send_protobuf_to_remote_write(...)` and `get_response_to_remote_write(...)` became multi-line/multi-arg, `get_response_to_http_api_query` gained `params=`, `get_response_to_http_api` gained `headers=`.

**Verdict on §3: our fixture does NOT suffice as-is.** Three things are required before PIN's `test_compliance.py` can run here:
1. Provide `cluster.prometheus_ip` / `cluster.prometheus_port` dicts (either port the PIN refactor, or add dict properties as a compatibility shim over the existing scalars — the shim is ~10 lines and avoids touching our five working tests).
2. Either port the compose split, or keep our single-file compose (functionally equivalent for a receiver-only test) — **the compose file itself is not a blocker**, the image and receiver flags are already identical to PIN.
3. Port the `prometheus_test_utils.py` additions if we also take the sibling PIN tests; for `test_compliance.py` alone, our existing `convert_time_series_to_protobuf` + `send_protobuf_to_remote_write` are call-compatible (positional `host, port, path, proto`).

`configs/prometheus.xml` also diverged by 80 lines — needs a read before porting any of the API tests (`test_query_api.py` etc.), though `test_compliance.py` only needs the `9093 /read` + `9093 /write` handlers.

---

## 4. Turning `test_promql_compliance` into a real gate

### 4.1 What upstream already gives us

`update_compliance_baseline.py` (PIN, 205 lines) does **part** of the job — but read its own docstring:

> "CI compares PRs to the latest matching object on S3 …; this script is for local inspection or attaching a file to a discussion — **not for a committed baseline file**."

What it actually does:
- Resolves a `clickhouse` binary (`--binary`, else `CLICKHOUSE_TESTS_SERVER_BIN_PATH`, else `build/programs/clickhouse`).
- Sets `COMPLIANCE_RESULT_FILE=ci/tmp/compliance_baseline_refresh_result.json`, `CLICKHOUSE_TESTS_SERVER_BIN_PATH`, `CLICKHOUSE_TESTS_BASE_CONFIG_DIR=programs/server`, `PYTEST_TIMEOUT=3600`.
- Shells out to `python -m pytest test_prometheus_protocols/test_compliance.py::test_promql_compliance -vv --tb=short` from `tests/integration`, optionally teeing to `--capture-log`.
- Validates the produced JSON has `passed/failed/unsupported/total/pct`.
- Re-emits it enriched with `commit` (12-char SHA), `updated_utc`, `note`, and `breakdown`, to `ci/tmp/promql_compliance_baseline_export.json` (`--output` to override, `--dry-run` to stdout).

So we inherit the **refresh driver** for free (env plumbing, binary resolution, schema validation, provenance fields). What we must build is: an in-tree destination, per-query granularity, and the assertion.

### 4.2 Where the baseline should live

```
tests/integration/test_prometheus_protocols/compliance_baseline.json
```

Rationale: co-located with the test, so `cluster.py`'s `__file__`-relative lookup is trivial; inside `./tests/integration/` which is already in the integration job's `digest_config.include_paths`, so editing the baseline correctly invalidates the CI cache; and reviewable in the PR diff, which is the whole point of an in-tree gate. Do **not** put it under `ci/` — that mixes test data with job wiring and (per `job_configs.py`) would change the digest of unrelated jobs.

### 4.3 Format

Per-query status, not just aggregates. Sorted keys so diffs are minimal and reviewable:

```json
{
  "schema_version": 1,
  "generated": {
    "commit": "a5cc61775e76",
    "updated_utc": "2026-09-17T00:00:00Z",
    "corpus_sha256": "<sha256 of the sorted expanded query list>",
    "note": "..."
  },
  "totals": { "passed": 0, "failed": 0, "unsupported": 0, "total": 539, "pct": 0.0 },
  "queries": {
    "42": "pass",
    "abs(demo_memory_usage_bytes)": "pass",
    "quantile_over_time(0.5, demo_memory_usage_bytes[1m])": "unsupported",
    "sum by(instance) (demo_memory_usage_bytes)": "fail"
  }
}
```

Three-valued status only (`pass` / `fail` / `unsupported`) — deliberately **not** the failure *reason*. Reasons are error strings that churn on unrelated message edits and would make the baseline a maintenance tax; the category breakdown in stdout is enough for triage.

`corpus_sha256` is the load-bearing extra field: because the corpus is Python source (§2.3), a template edit silently changes the key set. Hashing the sorted expanded list lets the assertion distinguish "you regressed a query" from "you changed the corpus, re-baseline".

Keep `totals` as a redundant cross-check and for a human-readable at-a-glance diff.

### 4.4 Assertion shape

Restructure so the scoring loop returns a dict rather than only printing. Then:

```
statuses      = {query: "pass"|"fail"|"unsupported"}
baseline      = json.load(baseline_path)

# 0. corpus drift guard
assert corpus_sha256(sorted(statuses)) == baseline["generated"]["corpus_sha256"], \
    "query corpus changed; re-run update_compliance_baseline.py --write-in-tree"

# 1. per-query regressions (the real gate)
RANK = {"pass": 2, "unsupported": 1, "fail": 0}
regressions = [
    (q, baseline["queries"][q], statuses[q])
    for q in statuses
    if RANK[statuses[q]] < RANK[baseline["queries"][q]]
]

# 2. undeclared improvements (soft: fail with a "re-baseline me" message)
improvements = [... RANK[statuses[q]] > RANK[baseline["queries"][q]] ...]

assert not regressions, format_table(regressions)
assert not improvements, "score improved; refresh compliance_baseline.json"
```

Key decisions:
- **Ordered severity `fail < unsupported < pass`.** `pass -> unsupported` is a regression (a function stopped being wired up). `unsupported -> fail` is also a regression: it means we now *claim* to implement the function but produce wrong answers, which is strictly worse than an honest 501. Treating those as equivalent would hide the most dangerous class of bug.
- **Assert on the per-query set, never on aggregate `pct`.** An aggregate floor is defeated by any change that breaks N queries while fixing N others — precisely the shape of a refactor regression. Keep `pct` in the baseline for reporting, and optionally add a cheap secondary `assert totals["passed"] >= baseline_passed` as a belt-and-braces check, but the per-query diff is the gate.
- **Fail on improvements too.** Without this the baseline rots: someone implements `sum_over_time`, the gate stays green, and the newly-passing queries are never protected. Make the failure message state exactly which command to run.
- **Report, don't truncate.** Print all regressing queries with old status, new status, and the fresh diff message; an engineer must be able to act from CI stdout alone.
- **Keep the JSON dump.** Continue honouring `COMPLIANCE_RESULT_FILE` and keep the upload hook so the S3 trend line survives; the in-tree gate and the S3 report are complementary, not alternatives.

Because the assertion is inside the test, the gate rides the ordinary Integration tests job — no `comp-promql` label, no `allow_failure=True`, no separate `PROMQL_COMPLIANCE` job, and no dependence on the ClickHouse-owned S3 bucket (which a fork cannot write anyway, §2.6).

### 4.5 Changes needed to `update_compliance_baseline.py`

Small, additive:
1. Have `test_compliance.py` include the per-query `queries` map and `corpus_sha256` in the `COMPLIANCE_RESULT_FILE` payload (currently only aggregates + category counts survive; `result.failures` is thrown away).
2. Add a `--write-in-tree` / change the default `--output` to `tests/integration/test_prometheus_protocols/compliance_baseline.json`.
3. Have the script set an env flag (e.g. `COMPLIANCE_BASELINE_REFRESH=1`) that the test reads to **skip its own assertions** — otherwise the refresh run fails on the very regression you are trying to record, and you can never re-baseline.

### 4.6 Rollout ordering

The gate is only meaningful once the port lands, and it must be baselined against *our* build. Sequence: (1) fix the `cluster.py` accessor gap from §3.3; (2) land `test_compliance.py` + `generate_compliance_data.py` unmodified so we get a score; (3) generate the in-tree baseline from that run; (4) add the assertions. Landing (4) before (3) just produces a red CI with no reference point.

---

## 5. Stateless + unit coverage

### 5.1 Stateless (`tests/queries/0_stateless/`, matching `*time_series*` / `*prometheus*` / `*promql*`)

| Ref | Files (`.sql`+`.reference`+`.sh`) | Distinct tests |
|---|---|---|
| OURS | 8 | **4** |
| PIN | 78 | **39** |

OURS (4): `02267_output_format_prometheus`, `03756_time_series_wrong_tags_group`, `03779_time_series_tags_functions`, `04410_time_series_referential_dependencies`.

**PIN-only (36 tests):**
```
04042_time_series_range_overflow
04131_prometheus_query_parser
04201_time_series_selector_join_stateful
04202_prometheus_query_datetime64_nanosecond_modern_timestamp
04268_time_series_tags_to_map
04269_time_series_metric_type_to_suffixes
04337_prometheus_query_nan_timestamp
04409_time_series_referential_dependencies
04549_promql_dialect_query_cache                       (.sh)
04631_promql_count_subquery_in_range_function
04692_prometheus_query_parse_error_long_input          (.sh)
04811_promql_topk_bottomk_limitk
04811_promql_topk_bottomk_limitk_streaming
04816_promql_shared_subqueries_materialized
04817_time_series_selector_bare_pk_conditions
04836_time_series_selector_whole_metric_pk_range
04897_promql_offset_in_range_selector
04926_promql_clamp_round_argument_errors
05019_time_series_recent_samples_table
05024_time_series_extract_tag_empty_input
05025_time_series_recent_samples_dedup
05026_time_series_recent_samples_default
05028_time_series_default_table_engine
05047_promql_selector_read_parallelism
05057_time_series_alter_setting_keeps_other_settings
05057_time_series_read_aggregation_in_order
05059_prometheus_query_empty_aggregation_setting
05059_promql_empty_by_group
05059_time_series_create_as_keeps_settings
05060_time_series_truncate_in_replicated_database
05076_promql_fused_aggregation_pair
05077_promql_quantile_prefer_column_alias
05138_time_series_create_as_access                     (.sh)
05141_time_series_select_join_settings
05182_time_series_id_type_setting
05212_timeseries_old_version_time_series_outer_column
```

**Collision to resolve before porting:** OURS carries `04410_time_series_referential_dependencies` where PIN has the *same test* as `04409_time_series_referential_dependencies`. A naive port lands both, duplicating the test under two numbers. Also note PIN's `05212_timeseries_old_version_time_series_outer_column` reuses number `05212`, which the revert (§6) freed from `05212_timeseries_quantile_to_grid`.

### 5.2 Unit tests

`src/Parsers/Prometheus/` is file-for-file **identical in layout** at both refs (12 sources + 1 test); the only test file is `src/Parsers/Prometheus/tests/gtest_PromQLParser.cpp`, present at both. It grew substantially at PIN — the `quantile_over_time` case sits at PIN line 1001 vs OURS line 711, and `predict_linear` at PIN 1073 vs OURS 783, i.e. PIN's parser gtest is roughly 300+ lines longer.

`gtest_*Prom*` / `gtest_*TimeSeries*` across the whole tree:

| Ref | Files |
|---|---|
| OURS | `src/Parsers/Prometheus/tests/gtest_PromQLParser.cpp`, `src/Server/tests/gtest_prometheus_metrics_writer.cpp` |
| PIN | the same two, **plus** `src/Storages/MergeTree/Streaming/tests/gtest_cursor_promoter.cpp`, `src/Storages/TimeSeries/tests/gtest_normalize_time_series_definition.cpp` |

PIN-only unit tests: **2** (`gtest_cursor_promoter`, `gtest_normalize_time_series_definition`). Note `src/Storages/TimeSeries/tests/` does not exist at OURS at all.

---

## 6. The four reverted functions

### 6.1 The revert — **CONFIRMED**

```
commit  daa58c654bdef03d82525d1f7c7800b7d3a7ad02
author  Nikita Mikhaylov
date    Wed Sep 16 10:23:35 2026 +0200
subject Revert "Implement PromQL range functions present_over_time, absent_over_time,
        quantile_over_time and predict_linear"
```

Merge commits, both reachable from PIN:
```
3a43db4d688  Tue Sep 15 23:15:50 2026  Merge pull request #112842 from valerypetrov/promql/phase3-range-functions
e9ce26a1e6e  Wed Sep 16 08:35:52 2026  Merge pull request #120336 from ClickHouse/revert-112842-promql/phase3-range-functions
```

PR #120336 reverted #112842 **less than 10 hours after it merged**, on 2026-09-16 — matching the brief exactly. The revert deleted, among others:
```
src/AggregateFunctions/TimeSeries/AggregateFunctionTimeSeriesResultWriter.h        (-159)
src/AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesPresentToGrid.h       (-120)
src/AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesQuantileToGrid.{h,cpp} (-447)
.../PrometheusQueryToSQL/applyFunctionPredictLinear.{h,cpp}                        (-310)
.../PrometheusQueryToSQL/applyFunctionQuantileOverTime.{h,cpp}                     (-257)
.../PrometheusQueryToSQL/fixedAtModifier.{h,cpp}                                    (-87)
.../PrometheusQueryToSQL/getToGridAggregateFunctionArguments.{h,cpp}               (-127)
tests/integration/test_prometheus_protocols/test_evaluation.py                    (-137)
8 stateless tests: 05210_timeseries_present_to_grid, 05211_promql_present_over_time_absent_over_time,
                   05212_timeseries_quantile_to_grid, 05213_promql_quantile_over_time,
                   05214_timeseries_linear_regression_to_grid, 05215_promql_predict_linear
```
It also rewrote `applyFunctionAbsent.cpp` (-238/+…), `applyFunctionOverRange.cpp`, `AggregateFunctionTimeseriesBase.h`, `AggregateFunctionTimeseriesLinearRegression.h`, `AggregateFunctionTimeseriesSlidingSum.h`, `ASTFunction.{h,cpp}`, `TableFunctionTimeSeries.cpp`, and dropped 22 lines from `gtest_PromQLParser.cpp`.

### 6.2 Per-function verdict at PIN

| Function | Implemented at PIN? | Evidence |
|---|---|---|
| `present_over_time` | **NO** | `src/Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.cpp:173` — `/// present_over_time`, inside the `/// TODO:` block at lines 168-178. Not a key in `impl_map`. Zero other references anywhere in `src/Storages/TimeSeries/` or `src/Parsers/Prometheus/`. |
| `absent_over_time` | **NO** | `applyFunctionOverRange.cpp:174` — same TODO block. Not in `impl_map`. Zero other references. (Note: bare `absent` *is* implemented at PIN via `applyFunctionAbsent.cpp` / `isFunctionAbsent`; `absent_over_time` is not.) |
| `quantile_over_time` | **NO** | `applyFunctionOverRange.cpp:170` — same TODO block. Not in `impl_map`. Only other hits are **parser** tests: `src/Parsers/Prometheus/tests/gtest_PromQLParser.cpp:1001-1005` — it parses, it does not convert. |
| `predict_linear` | **NO** | `applyFunctionOverRange.cpp:169` — same TODO block. Not in `impl_map`. Only other hits are parser tests: `gtest_PromQLParser.cpp:1073-1077`. |

PIN's TODO block verbatim (`applyFunctionOverRange.cpp:168-178`):
```cpp
            /// TODO:
            /// predict_linear
            /// quantile_over_time
            /// stddev_over_time"
            /// stdvar_over_time
            /// present_over_time
            /// absent_over_time
            /// mad_over_time
            /// ts_of_last_over_time
            /// first_over_time
            /// ts_of_first_over_time
```

All four are **UNIMPLEMENTED at PIN** — the brief is **fully confirmed**. Practical consequence for §2: the compliance corpus expands `{{.simpleTimeAggrOp}}_over_time` over `["sum","avg","max","min","count","stddev","stdvar","absent","last"]` × 6 ranges, and `quantile_over_time({{.quantile}}, …)` over 9 quantiles × 6 ranges — so `stddev_over_time`, `stdvar_over_time`, `absent_over_time` (18 queries) plus all 54 `quantile_over_time` queries plus 6 `predict_linear` queries land in the `unsupported` bucket at PIN. Since `unsupported` counts in the score denominator, PIN's own score is capped well below 100%, and **the port inherits a known ~78-query hole in exactly the area #112842 was meant to fix.** Any baseline we generate must record these as `unsupported`, and re-landing #112842's work later will show up as "improvements" requiring a re-baseline (§4.4).

### 6.3 Full list of implemented PromQL functions

Derived from the `applyFunction` dispatch chain and each module's `impl_map` / predicate at each ref.

**OURS — 40 functions.** Dispatch order in `applyFunction.cpp`: `isFunctionVector`, `isFunctionScalar`, `isFunctionTime`, `isDateTimeFunction`, `isMathSimpleFunction`, `isFunctionPi`, `isFunctionOverRange`, else `NOT_IMPLEMENTED`.

- singletons (4): `vector`, `scalar`, `time`, `pi`
- date/time (8, `applyDateTimeFunction.cpp`): `day_of_month`, `day_of_week`, `day_of_year`, `days_in_month`, `hour`, `minute`, `month`, `year`
- math (23, `applyMathSimpleFunction.cpp`): `abs`, `acos`, `acosh`, `asin`, `asinh`, `atan`, `atanh`, `ceil`, `cos`, `cosh`, `deg`, `exp`, `floor`, `ln`, `log10`, `log2`, `rad`, `sgn`, `sin`, `sinh`, `sqrt`, `tan`, `tanh`
- over-range (**5**, `applyFunctionOverRange.cpp`): `delta`, `idelta`, `irate`, `last_over_time`, `rate`

**PIN — 59 functions.** Dispatch order: `isFunctionVector`, `isFunctionScalar`, `isFunctionTime`, `isFunctionAbsent`, `isDateTimeFunction`, `isOneArgumentMathFunction`, `isClampFunction`, `isRoundFunction`, `isFunctionPi`, `isLabelManipulationFunction`, `isFunctionOverRange`, `isHistogramQuantile`, else `NOT_IMPLEMENTED`.

- singletons (4): `vector`, `scalar`, `time`, `pi`
- date/time (8): identical to OURS
- math (23, `applyOneArgumentMathFunction.cpp`): identical set to OURS
- over-range (**16**): `avg_over_time`, `changes`, `count_over_time`, `delta`, `deriv`, `idelta`, `increase`, `irate`, `last_over_time`, `max_over_time`, `min_over_time`, `rate`, `resets`, `sum_over_time`, `ts_of_max_over_time`, `ts_of_min_over_time`
- clamp (3): `clamp`, `clamp_min`, `clamp_max`
- round (1): `round`
- label manipulation (2): `label_join`, `label_replace`
- absent (1): `absent`
- histogram (1): `histogram_quantile`

**Gain OURS -> PIN: +19 functions (40 -> 59).**

Newly available: `increase`, `deriv`, `changes`, `resets`, `sum_over_time`, `avg_over_time`, `min_over_time`, `max_over_time`, `count_over_time`, `ts_of_min_over_time`, `ts_of_max_over_time`, `clamp`, `clamp_min`, `clamp_max`, `round`, `label_join`, `label_replace`, `absent`, `histogram_quantile`.

Most notable: **`histogram_quantile`** (histograms are unusable without it), **`increase`** (the single most-used PromQL function in practice after `rate`), the five plain `*_over_time` aggregators, and `label_replace` / `label_join` (relabeling, heavily used in dashboards). `deriv` additionally carries a follow-up fix at PIN, `b356cac288a Fix deriv() to scale its slope by the timestamp tick resolution`.

### 6.4 The much larger gap: operators, not functions

Functions understate the delta. OURS' `Converter.cpp` node dispatch handles only `Scalar`, `StringLiteral`, `InstantSelector`, `RangeSelector`, `Subquery`, `Offset`, `Function`, `UnaryOperator` — everything else hits
`throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Prometheus query node type {} is not implemented", node->node_type)`.

**OURS therefore implements no aggregation operators and no binary operators at all.** PIN adds ~30 source files for them:
- `applyAggregationOperator.cpp` dispatching to `applyOneArgumentAggregationOperator` (`avg`, `count`, `group`, `max`, `min`, `stddev`, `stdvar`, `sum`), `applyAggregationOperatorQuantile`, `applyAggregationOperatorCountValues`, `applyLimitAggregationOperator` (`topk`, `bottomk`, `limitk`)
- `applyBinaryOperator.cpp` plus `applyBinaryOperatorAnd/Or/Unless/Set`, `applySimpleBinaryOperator`, `applyMathBinaryOperator` (incl. `atan2`), `applyComparisonOperator`, `transformGroupASTForBinaryOperator`, `transformGroupASTForAggregationOperator`
- `applyFusedAggregationBinaryOperator.cpp`, `toVectorGrid.cpp`, `checkSharedSubqueriesAreMaterialized.cpp`

Since the compliance corpus is dominated by `{{.simpleAggrOp}}` (7 values), `{{.binOp}}` (12 values) and `{{.compBinOp}}` (6 values) expansions, **most of the 539 queries fail at OURS on the node-type dispatch, not on any function.** A baseline generated against OURS today would be near-zero; the numbers only become meaningful after the port. This reinforces §4.6: baseline *after* the port, and use per-query statuses so the operator work is individually protected.

---

## Summary of what we must build ourselves

1. `cluster.prometheus_ip` / `cluster.prometheus_port` dict accessors (or a shim) — hard blocker for running PIN's `test_compliance.py`.
2. The assertion in `test_promql_compliance` — upstream has none, by design, at every layer.
3. An in-tree baseline file + per-query status capture (upstream persists only aggregates and category counts; per-query results are printed and discarded).
4. A corpus-drift hash, because the 539-query corpus is Python source, not data.
5. A refresh path that is not the ClickHouse-owned S3 bucket — `should_publish_master_baseline` hard-codes `ClickHouse/ClickHouse` + `master`, so a fork can never publish a baseline.
6. Renumber/dedup `04409` vs `04410_time_series_referential_dependencies` when porting stateless tests.

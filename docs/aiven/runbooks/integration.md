# Runbook — local integration tests (Aiven)

For a single `tests/integration/test_aiven_<slug>/` suite on a local binary,
without praktika DinD. Prefer Buildkite ASAN shards for full coverage.

## When

Use when the patch needs restart, multi-node, Keeper/MinIO/Kafka, etc. Naming:
`tests/integration/test_aiven_<slug>/` (see `testing.md`).

## Certificates — mint, never commit

**Do not hardcode PEM/key material in the pytest tree.** Self-signed fixtures
rot, leak keys into git history, and hide whether `<ca_path>` / TLS is actually
doing work.

Pattern (see existing suites):

| Example | Generator | Notes |
|---|---|---|
| `test_aiven_s3_custom_ca_path` | `minio_certs/generate_certs.sh` | Self-signed for MinIO + copy into `configs/` for `<ca_path>` |
| `test_aiven_lazy_certificates` | `certs/generate_certs.sh` | Server cert for HTTPS leg |
| `test_aiven_external_db_ssl` | `certs/gen_certs.sh` | CA + leaf certs (SAN + `keyCertSign` for OpenSSL 3) |
| `test_aiven_azure_custom_ca_path` | `certs/generate_ca.sh` | Same idea for Azurite |

Rules:

1. Ship a small `openssl`-based **`generate_*.sh` / `gen_certs.sh`** next to the
   test (comment *why* SANs / keyUsage matter).
2. Call it from the module fixture **before** `cluster.start()` / container
   configure, e.g. `subprocess.check_call(["bash", …/generate_certs.sh])`.
3. **Gitignore** minted files (`private.key`, `*.crt`, `*.pem`, `CAs/`, …);
   commit only the generator + `.gitignore`.
4. Prefer long `-days` on throwaway test CAs; the point is fresh mint each run,
   not decade-long committed secrets.
5. Negative TLS tests should use a trust store that does **not** contain the
   fixture CA — that is what proves `ca_path` (or similar) is load-bearing.

## Minimal loop

```bash
export CLICKHOUSE_TESTS_SERVER_BIN_PATH="$PWD/build_debug/programs/clickhouse"

cd tests/integration
# User-local pip once: docker pytest-xdist pytest-timeout pytest-reportlog \
#   dict2xml kazoo minio pandas
pytest test_aiven_<slug>/ -v --tb=short --timeout=240 \
  2>&1 | tee ../../tmp/uplift-26.8/test-aiven-<slug>.log
```

Pre/post evidence: same FAIL/PASS discipline as stateless (`build-and-test.md`).

## Gotchas (from 26.3)

- Prefer `keeper_randomize_feature_flags=False` and explicit
  `keeper_required_feature_flags=[…]` when flakes show `Code: 999` Keeper RPC.
- Huge `StrReplace` on large sources can hit `E2BIG` — use a small `python3`
  heredoc via Shell instead.
- Clean `ci/tmp/` / leftover containers between pre and post runs if the
  harness complains about dirty state.

Full CI path: root `AGENTS.md` + `.buildkite/run_aiven_integration.sh`.

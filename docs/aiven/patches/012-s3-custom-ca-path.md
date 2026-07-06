# Patch 012 — s3-custom-ca-path

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | (related variant present; see §2) | — | trust path identical; same connection-pool-key omission |
| 25.8-aiven | `8fc1c96ae0` | tilman.moeller@aiven.io (author) / alex.khatskevich@aiven.io (committer) | (the version we are porting FROM) |
| 26.3-aiven | `patch-port(012)` | parent agent, 2026-06-01 | ported `still-needed-but-rewrite` (throttler-struct drift), **shipped with two bug fixes (A, B) + integration test** — see §3, §4 |

## 1. Purpose

Allow an S3 disk (and S3 endpoint config) to pin a custom CA bundle through a
per-disk `<ca_path>` setting, so ClickHouse can speak TLS to an object store
whose server certificate is signed by a private / self-signed CA **without
disabling certificate verification globally**.

Without this, the only ways to reach such an endpoint are to weaken the global
`openSSL.client` config (e.g. `verificationMode=none` or an
`AcceptCertificateHandler`) — which disables verification for *all* outbound
TLS — or to install the CA into the system trust store of every node.

### Mechanism (data flow)

`<ca_path>` (disk or `<s3><endpoint>` config)
→ `S3Settings::ca_path` (`src/IO/S3Settings.cpp`)
→ `PocoHTTPClientConfiguration::ca_path` (`src/IO/S3/PocoHTTPClient.h`)
→ `PocoHTTPClient::ca_context`, a `Poco::Net::Context` built from the CA bundle
→ `makeHTTPSession(..., context)` (`src/IO/HTTPCommon.h`)
→ `HTTPConnectionPool` pool keyed per endpoint **and trust anchor**.

The context is created with
`Poco::Net::Context::VERIFY_RELAXED`, depth 9, `loadDefaultCAs = false` — i.e.
the supplied bundle becomes the **sole** trust anchor for that disk (see the
sharp edge in §3, finding C).

## 2. Upstream-drift / validity findings

`still-needed-but-rewrite`: the feature is still wanted on 26.3 and the upstream
code does not exist, but the patch did not apply cleanly.

- **`makeHTTPSession` signature widened.** The patch adds a trailing
  `Poco::AutoPtr<Poco::Net::Context> context = {}` parameter to
  `makeHTTPSession` (`src/IO/HTTPCommon.h`). Every caller is updated mechanically
  to pass `{}` (default context) — this is why the port touches ~24 files
  (`ReadWriteBufferFromHTTP`, `WriteBufferFromHTTP`, `AzureBlobStorage/PocoHTTPClient`,
  `Access/HTTPAuthClient`, `AvroRowInputFormat`, GCP OAuth, REST catalogs, …).
- **Throttler-struct drift (conflict region).** On 25.8 the patch carried
  separate `get_request_throttler` / `put_request_throttler` members. On 26.3
  these are consolidated into a single `HTTPRequestThrottler request_throttler`
  struct that *holds* `get_throttler` / `put_throttler`
  (`S3::Client::getPutRequestThrottler()` returns
  `client_configuration.request_throttler.put_throttler`). The conflict in
  `PocoHTTPClient`'s ctor was resolved to keep 26.3's single-struct member and
  insert `ca_path` / `ca_context` around it.
- **Cross-LTS note.** The 25.3-aiven line carries an equivalent change; the
  trust-establishment path is the same, and the connection-pool-key omission
  (finding A) is present there too — i.e. the bug is inherited, not introduced
  by drift.

## 3. C++ review — bugs found and dispositions

### A. Connection-pool key ignored the SSL context — **FIXED** (correctness / security)

`HTTPConnectionPool`'s `EndpointPoolKey` was
`{group, host, port, secure, proxy}` — the per-endpoint pool did **not** include
the trust anchor. `getPoolImpl` therefore returned an existing pool for an
endpoint regardless of the `context` argument ("first context wins"): a disk with
a custom CA, a disk with a *different* CA, and a disk with **no** `ca_path`
(default trust) all collided on the same pool. A connection established and
verified against one CA could be reused for a peer that should be verified
against another — a silent widening of trust.

Fix: add `const Poco::Net::Context * ssl_context` to `EndpointPoolKey`
(`operator==` + `Hasher`) and set it from the context in `getPoolImpl`. Distinct
trust anchors now get distinct pools; `nullptr` (no `ca_path`) keeps today's
default pool — fail-safe. Pointer identity is a sufficient and safe discriminator
because the pool holds an `AutoPtr` ref to the context for the pool's lifetime, so
the address cannot be freed-and-reused while a keyed entry exists.

### B. SSL context rebuilt on every request — **FIXED** (performance + prerequisite for A)

The faithful port created a fresh `Poco::Net::Context` (re-reading and parsing the
CA file from disk) **inside the per-attempt request loop** in
`PocoHTTPClient::makeRequestInternalImpl` — once per request *and* per redirect.

Fix: build it once in the constructor via `makeCAContext(ca_path)` and store it as
the `ca_context` member; the request loop reuses it. Besides removing per-request
file I/O, this is what makes fix A correct: a stable context pointer per client
means the pool key is stable, so pooling still works (a per-request context would
spawn a brand-new pool on every call).

### C. `loadDefaultCAs = false` — **kept (documented sharp edge)**

A `ca_path`-configured disk trusts *only* the supplied bundle; it can no longer
reach an endpoint whose cert chains to a public CA. This matches the patch intent
(pin a private CA) and is left unchanged to stay faithful; documented here and in
the changelog so operators are aware.

### D. `ca_path` deserialization wrapped in try/catch — **noted, out of scope**

`S3Settings` (de)serialization guards the new `ca_path` field for backward
compatibility with older serialized data. Faithful and acceptable for this commit;
flagged for a possible later cleanup.

## 4. Test design

`tests/integration/test_aiven_s3_custom_ca_path/`.

The test is the deliberate **inversion** of `test_s3_with_https`: the global
`openSSL.client` is set to `verificationMode=strict` + `RejectCertificateHandler`
and is **not** given the MinIO CA. So the per-disk `<ca_path>` is the *only*
possible trust path — nothing can pass vacuously.

| Case | Setup | Asserts |
|---|---|---|
| `test_custom_ca_path_allows_https_to_self_signed_minio` | disk **with** `ca_path` | `INSERT`/`SELECT` succeed → the feature works |
| `test_missing_ca_path_fails_certificate_verification` | disk **without** `ca_path`, **same** `minio1:9001` endpoint, run **after** the positive case | the write fails with a certificate-verification error → (1) `ca_path` is load-bearing and (2) **fix A**: the no-CA disk does not inherit the with-CA disk's trusting pooled connection |

Supporting details:
- MinIO is served over HTTPS with a **self-signed cert generated fresh at fixture
  start** (`minio_certs/generate_certs.sh`, invoked from `test.py`). It can never
  expire and is git-ignored; SAN covers `minio1`.
- `skip_access_check` on both disks so the unreachable no-CA disk does not hang
  the server's startup write-probe.
- `s3_retry_attempts=1` so the negative write fails in seconds rather than retrying
  hundreds of times with multi-second backoff.

Evidence (clean run, server log): `data_with_ca` → 0 `certificate verify failed`;
`data_no_ca` → multiple `certificate verify failed`. Same endpoint, opposite
outcomes, driven solely by `ca_path`.

Run locally:

```bash
cd tests/integration
export CLICKHOUSE_TESTS_SERVER_BIN_PATH=$(git rev-parse --show-toplevel)/build/programs/clickhouse
export CLICKHOUSE_TESTS_CLIENT_BIN_PATH=$CLICKHOUSE_TESTS_SERVER_BIN_PATH
export CLICKHOUSE_TESTS_BASE_CONFIG_DIR=$(git rev-parse --show-toplevel)/programs/server
pytest test_aiven_s3_custom_ca_path -v
```

## 5. Build note (this uplift)

This `build/` directory has no ninja header-dependency tracking
(`ninja -t deps … → #deps 0`), so editing a layout-affecting header does **not**
trigger recompilation of its includers. Patch 012 adds `ca_path` to
`PocoHTTPClientConfiguration`, shifting `request_throttler`'s offset; a stale
`ServerAsynchronousMetrics.cpp.o` then read the throttler at the old offset and the
server hit a fatal exception in `getPutRequestThrottler`. Resolved by recompiling
the full transitive include closure of the changed headers (see
`docs/aiven/runbooks/build-and-test.md`). Always verify includers rebuilt before
trusting an incremental build after a struct-layout change here.

## 6. Rollback considerations

Reverting the commit removes the `ca_path` plumbing and the `EndpointPoolKey`
trust-anchor discriminator together. Disks configured with `<ca_path>` would then
fall back to the global trust configuration. Because A + B + the port + the test
ship as a single commit, the revert is atomic.

## 7. Per-uplift notes

### 25.8-aiven (historical, source)

Source commit `8fc1c96ae0`. Carried separate get/put throttler members; built the
context per request; pool key omitted the context.

### 26.3-aiven (this uplift)

Ported with throttler-struct drift resolved, and shipped **with** fixes A and B
plus the integration test, as one commit so the whole unit cherry-picks cleanly to
other LTS lines.

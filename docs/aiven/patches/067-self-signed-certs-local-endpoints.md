# Patch 067 — self-signed-certs-local-endpoints

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `d0a99ce495` | Tilman Moeller (author) `tilman.moeller@aiven.io`, committed by `joelynch112@gmail.com`, 2026-02-11 | (the version we are porting FROM) |
| 26.3-aiven | `patch-port(067)` (`8e7b03864b2`) | parent agent, 2026-06-09 | `still-needed-but-rewrite` — HARDENED: substring host match → exact parsed-host match; ships an integration test |

`byte_equivalent: false` — this is a deliberate, parent-ratified rewrite of the
source diff (substring `find` → exact `Poco::URI(...).getHost()` comparison), so
the staged diff is NOT a verbatim cherry-pick of `d0a99ce495`. See §2 / §3.

## 1. Purpose

The DeltaLake engine reads via delta-kernel-rs, which builds its own S3 client
(the Rust `object_store` crate) over the existing FFI. ClickHouse forwards
endpoint, bucket, and credentials through `set_builder_option`, but the kernel's
HTTP client enforces default TLS verification. When integration tests (and local
development) use MinIO over HTTPS with a **self-signed** certificate, the kernel
fails with `ObjectStoreError` "error sending request" because the certificate is
not in any trust store.

The patch passes `allow_invalid_certificates=true` to the kernel builder, but
ONLY when the S3 URL is HTTPS **and** the endpoint host is on a loopback
allowlist. This lets local/test MinIO work while AWS and every other host keep
strict verification. It is a ClickHouse-only change — the `object_store` crate
already honours the option and the delta-kernel-rs FFI forwards all builder
options, so no `contrib/delta-kernel-rs` submodule patch is required.

Source SHA on `v25.8.18.1-lts-aiven`: `d0a99ce495` (from
`docs/aiven/uplifts/26.3/inventory.md` row 067).
Original author: `tilman.moeller@aiven.io` (committed by `joelynch112@gmail.com`).
Original purpose (quoted from the source commit body):

> Fix (ClickHouse-only, no submodule patch): pass `allow_invalid_certificates`
> to the kernel builder when the S3 URL is HTTPS and the endpoint is on the
> allowlist. ... Guard: use an allowlist, not a blocklist. We only set
> `allow_invalid_certificates` when the endpoint host is localhost or 127.0.0.1
> (`is_local_or_test_endpoint`).

## 2. Upstream-drift findings

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`.

### Commands run

```bash
git show d0a99ce495 -- src/Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelper.cpp
git log v25.8.18.1-lts..v26.3.10.62-lts -- src/Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelper.cpp
rg "allow_invalid_certificates" src/
```

### Findings

- Upstream changes to touched files between prior and current LTS:
  - `src/Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelper.cpp`: the
    insertion region is unchanged on HEAD. The `S3KernelHelper::createBuilder`
    body still sets `aws_bucket` and has the `if (url.uri_str.starts_with("http"))`
    block with `set_option("allow_http", ...)` + `set_option("aws_endpoint", url.endpoint)`
    exactly as in the source's pre-image (only line offsets differ: HEAD lines 92–98).
- Upstream changes that touched the patch's behavior (symbols, FFI, options):
  - `allow_invalid_certificates`: no occurrence on the base — upstream has no
    equivalent TLS-relaxation for the kernel S3 client. The patch's premise holds
    → **still needed**.
  - `ffi::set_builder_option`, `DB::S3::URI::endpoint`, `uri_str`, `bucket`: all
    present on HEAD; FFI forwarding semantics unchanged.
- The file is under `#if USE_DELTA_KERNEL_RS` (line 3), so the change only
  compiles when delta-kernel is enabled (as in the `build` dir).
- Conclusion: **`still-needed-but-rewrite`** — the behavior is still required,
  but per the parent's clause-(v) hardening decision the host check is rewritten
  from a substring match to an exact parsed-host match (see §3), so the diff is
  NOT applied verbatim.

## 3. C++ review

Apply `docs/aiven/skills/cpp-review-checklist.md`.

- 1 Lifetime + ownership: ✓ — `endpoint_host` is a local `std::string` owned by
  the stack frame; `Poco::URI(url.endpoint)` is a temporary parsed once. No
  references escape; the builder receives owned `std::string` copies via
  `set_option`.
- 2 Exception safety: ✓ — `Poco::URI`'s constructor may throw on a malformed
  endpoint, but `createBuilder` is already an exception-throwing path (it calls
  `KernelUtils::unwrapResult`), and a throw here aborts builder construction
  cleanly with no half-applied option state.
- 3 Thread-safety + concurrency: ✓ — operates only on per-call locals and the
  immutable `url` member; no shared mutable state introduced.
- 4 Performance + memory: ✓ — one extra `Poco::URI` parse + a small string per
  `createBuilder` call (once per snapshot open, not per row). Negligible.
- 5 Settings as public API: n/a — no new ClickHouse setting; the relaxation is an
  internal delta-kernel builder option, not user-configurable.
- 6 Error handling: ✓ — no new error codes; the defended failure surfaces as the
  existing `DELTA_KERNEL_ERROR` (`ObjectStoreError`).
- 7 Upstream / vendored code: ✓ — no change to `contrib/`; relies on the
  pre-existing `object_store` `allow_invalid_certificates` option and the FFI
  option-forwarding already in delta-kernel-rs.
- 8 Behavior under settings: ✓ — only active on the delta-kernel read path
  (`USE_DELTA_KERNEL_RS`); behaviour for non-HTTPS or non-local hosts is unchanged.

### Key deviation from source — HARDENING (substring → exact host)

The source matched the endpoint host with a **substring** test:

```cpp
const bool is_local_or_test_endpoint =
       (url.endpoint.find("localhost") != std::string::npos)
    || (url.endpoint.find("127.0.0.1") != std::string::npos);
```

A substring of the full endpoint URL is an over-broad allowlist: a host such as
`localhost.attacker.com` (or `https://127.0.0.1.evil.example/...`, or a bucket/key
literally containing `localhost`) would satisfy `find(...) != npos` and thus
silently **disable certificate verification** against an attacker-controlled
HTTPS endpoint. That turns a test convenience into a real MITM / SSRF-adjacent
TLS-downgrade hole.

The ported version anchors on the **exact, parsed** endpoint host:

```cpp
const bool is_https = url.uri_str.starts_with("https");
const std::string endpoint_host = Poco::URI(url.endpoint).getHost();
const bool is_local_or_test_endpoint =
    (endpoint_host == "localhost") || (endpoint_host == "127.0.0.1");
```

`Poco::URI::getHost` returns the normalized host with no scheme, port, path, or
userinfo, so the comparison is against the connect target itself, not a substring
of the URL text. `localhost.attacker.com` parses to host
`localhost.attacker.com` ≠ `localhost`, so it keeps strict TLS. For AWS or empty
endpoints `getHost` yields a non-local host (or empty string), preserving strict
verification.

### Clause-(v) security note (default-behavior change, blast radius)

- The relaxation is **loopback-only**: it fires solely for endpoint host
  `localhost` or `127.0.0.1`, AND only when the scheme is HTTPS, AND only on the
  delta-kernel S3 path.
- It is an **allowlist, not a blocklist**: anything not exactly loopback keeps
  default strict TLS verification. AWS (`*.amazonaws.com`) and any other host are
  unaffected.
- Production / on-prem HTTPS endpoints therefore retain full certificate
  verification; the change cannot weaken TLS for a remote endpoint.

### Robustness note — explicit `#include <Poco/URI.h>`

The port adds an explicit `#include <Poco/URI.h>` to the file's include block
rather than relying on the transitive include via `S3/Configuration.h` →
`S3/URI.h`. `Poco::URI` is now used directly in this translation unit, so the
include should be first-party; this keeps the file robust against an upstream
refactor that drops the transitive include.

## 4. Test design

(a) **New integration test that fails on the parent commit and passes after the
patch.**

- Test path: `tests/integration/test_aiven_delta_self_signed/` (per the
  `test_aiven_<slug>` convention, `docs/aiven/runbooks/integration-tests.md` §6).
  Committed files: `__init__.py`, `test.py`, `configs/client_ssl.xml`,
  `minio_certs/generate_certs.sh`, `minio_certs/.gitignore`, `tcp_forward.py`.
- The self-signed `minio_certs/private.key` + `minio_certs/public.crt` (and the
  mirrored `minio_certs/CAs/`) are **minted fresh on every run** by
  `generate_certs.sh` (invoked at the top of the `started_cluster` fixture, before
  `cluster.start`) and are **git-ignored** (`minio_certs/.gitignore`) — no private
  key material is committed to the repo, and the cert can never rot/expire.

Why integration (not stateless): the trigger requires a self-signed **HTTPS**
MinIO served to the delta-kernel-rs client at a **loopback** host — unreachable
from the shared single-server stateless runner.

Harness:
- MinIO is served over HTTPS with a freshly-minted self-signed certificate
  (`minio_certs/generate_certs.sh`, `minio_certs_dir=`), trusted by neither the
  container's system roots nor delta-kernel's default root set.
- A pure-stdlib loopback TCP forwarder (`tcp_forward.py`) runs inside the node
  container so the delta-kernel client connects to `https://127.0.0.1:9100`
  (host on the patch allowlist) while bytes are relayed verbatim to `minio1:9001`
  — TLS stays end-to-end, so MinIO still presents the self-signed cert and the
  SigV4 Host header the client signed is what MinIO receives (path-style keeps
  the signature valid).
- The ClickHouse-side S3 client (region detection in `S3KernelHelper`) accepts
  the self-signed cert via `configs/client_ssl.xml` (`verificationMode=none`), so
  that leg is constant pre/post — the ONLY tested variable is the delta-kernel
  TLS behaviour governed by the patch.
- The Delta table is a minimal protocol-v1 table (one parquet + one log commit)
  built at fixture time with `pyarrow` and uploaded via `cluster.minio_client`.

Cases:

| Test | Assertion |
|---|---|
| `test_delta_read_over_self_signed_https_localhost_succeeds` | `SELECT count() FROM deltaLake('https://127.0.0.1:9100/root/delta_self_signed/', 'minio', …)` returns `3`. |
| `test_delta_read_over_self_signed_https_nonlocal_host_still_strict` | the SAME self-signed table read through the non-local `minio1` host is rejected (strict TLS kept) — proves the relaxation is anchored on the exact endpoint host (the hardening). |

- Pre-patch run output (the FAIL) — `build/test_067_prepatch.log` (1 failed, 1 passed):

  ```text
  FAILED test_aiven_delta_self_signed/test.py::test_delta_read_over_self_signed_https_localhost_succeeds
  Code: 742. DB::Exception: ... Received DeltaLake kernel error ObjectStoreError:
  Error interacting with object store: Generic S3 error: Error performing GET
  https://127.0.0.1:9100/root/delta_self_signed/_delta_log/_last_checkpoint ...
  after 10 retries ... HTTP error: error sending request (in snapshot). ... (DELTA_KERNEL_ERROR)
  ```

  The non-local control PASSED pre-patch too (it is rejected with or without the patch).

- Post-patch run output (the PASS) — `build/test_067_postpatch.log` (2 passed):

  ```text
  test_delta_read_over_self_signed_https_localhost_succeeds PASSED
  test_delta_read_over_self_signed_https_nonlocal_host_still_strict PASSED
  2 passed in 13.15s
  ```

- Why this test distinguishes the Aiven gate from upstream behavior (per
  AGENTS.md §7): the divergence is the localhost read *succeeding* post-patch vs
  *throwing* `DELTA_KERNEL_ERROR` / `ObjectStoreError` "error sending request"
  pre-patch — a real causation pair, not a feature-absence artifact. The
  non-local control additionally proves the relaxation is host-anchored (so the
  hardening is exercised, not just the original substring behaviour).

## 5. Rollback considerations

- Revert safety: removing the two `const` declarations + the conditional
  `set_option("allow_invalid_certificates", "true")` restores strict TLS for the
  kernel on all endpoints. No schema, on-disk format, or ZK-state impact.
- Persistent state: none — the option is computed per `createBuilder` call; the
  server holds no new state across restart.
- Disable without rebuilding: there is no server setting to toggle the relaxation
  (it is deliberately loopback-scoped and on by default for local/test). To
  disable in production one would simply not use a `localhost`/`127.0.0.1` HTTPS
  endpoint — every real endpoint already keeps strict verification.

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — patch first carried on 25.8-aiven.

### 25.8-aiven (historical)

Source `d0a99ce495` (author Tilman Moeller `tilman.moeller@aiven.io`, committed
by `joelynch112@gmail.com`, 2026-02-11). Single-file, 7-line addition to
`KernelHelper.cpp` using a substring host match. No test shipped with the source.

### 26.3-aiven (this uplift)

- Cherry-pick was: **rewritten** (parent-ratified hardening). The substring host
  match (`url.endpoint.find("localhost"/"127.0.0.1") != npos`) is replaced by an
  exact parsed-host comparison (`Poco::URI(url.endpoint).getHost() == "localhost"
  || == "127.0.0.1"`), closing the `localhost.attacker.com` bypass. An explicit
  `#include <Poco/URI.h>` was added.
- Upstream-drift conclusion: `still-needed-but-rewrite` (§2).
- Test added at: `tests/integration/test_aiven_delta_self_signed/` (new).
- Time-to-port: warm-cache build dir; three `ninja -C build clickhouse` builds
  (post-patch incremental ~71s, pre-patch incremental ~86s, post-patch restore
  ~40s) plus two ~15s integration runs.
- Anything surprising: the delta-kernel pre-patch failure manifests already at
  schema inference (reading `_delta_log/_last_checkpoint`), so the `SELECT` never
  reaches the data files — the evidence pair is produced at the snapshot-open step.
```

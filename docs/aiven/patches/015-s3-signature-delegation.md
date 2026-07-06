# Patch 015 — s3-signature-delegation

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `d3b5e9016f` (adapted to AWS SDK 1.7.321 / `AWSAuthSignerProvider`) | tilman.moeller@aiven.io / kevin.michel@aiven.io | (the version we are porting FROM) |
| 26.3-aiven | `patch-port(015)` | parent agent, 2026-06-01 | ported `still-needed-but-rewrite` (SDK-ctor re-adaptation + `PocoHTTPClient` overlap with 012), **shipped with a net-new integration test** — see §2, §4 |

## 1. Purpose

Allow an S3 disk to delegate AWS SigV4 signature generation to an external HTTP
service through a per-disk `signature_delegation_url` setting. When configured,
ClickHouse does not compute the request signature locally; instead it POSTs the
AWS *canonical request* to the service and uses the returned signature. This lets
a proxy / external service gate, audit, or centrally control which S3 requests
get signed (security, compliance, monitoring), since the canonical request
carries everything needed to decide — method, path, host, headers, payload hash.

Protocol:

- ClickHouse → service: `POST` with JSON body `{"canonicalRequest": "PUT\n..."}`
- service → ClickHouse: `200` with JSON body `{"signature": "<hex>"}`

On any transport or parsing error the delegated signer logs and returns `""`,
which makes the SDK produce an unsigned/invalid request that the object store
rejects — i.e. it fails closed rather than silently signing with stale local
credentials.

### Mechanism (data flow)

`<signature_delegation_url>` (S3 disk / `<s3>` `auth_settings`)
→ `S3AuthSetting::signature_delegation_url` (`src/IO/S3AuthSettings.cpp`)
→ read in `getClient` (`src/Disks/DiskObjectStorage/ObjectStorages/S3/diskSettings.cpp`)
→ `ClientFactory::createClientConfiguration(..., signature_delegation_url)`
→ `PocoHTTPClientConfiguration::signature_delegation_url` (`src/IO/S3/PocoHTTPClient.h`)
→ `ClientFactory::create` → `Client::create(..., signature_delegation_url)`
→ `createSignerProvider` in `Client.cpp`: if non-empty, builds an
  `AWSAuthV4DelegatedSigner` wrapped in an `Aws::Auth::DefaultAuthSignerProvider`;
  otherwise a plain `DefaultAuthSignerProvider` (regular SigV4).
→ `Aws::S3::S3Client(signerProvider, endpointProvider, S3ClientConfiguration)`.

`AWSAuthV4DelegatedSigner` (`src/IO/S3/AWSAuthV4DelegatedSigner.{h,cpp}`) subclasses
`Aws::Client::AWSAuthV4Signer` and overrides `GenerateSignature`: it serialises the
canonical request to JSON, POSTs it via `makeHTTPSession(HTTPConnectionGroupType::DISK, ...)`,
`assertResponseIsOk`, parses `signature`, and returns it. With an empty URL it
delegates back to the base-class `GenerateSignature` (local signing), so the
signer is a safe drop-in.

## 2. Upstream-drift / validity findings

`still-needed-but-rewrite`: the feature is still wanted on 26.3, the upstream code
does not exist, and the patch does not apply cleanly (`cherry_pick_clean=no`).

### Submodule bump (prerequisite)

This patch is submodule-coupled. `contrib/aws` is redirected to Aiven's fork:

- `.gitmodules` `[submodule "contrib/aws"]`: `url = https://github.com/aiven/aws-sdk-cpp`, `branch = aiven/clickhouse-v26.3.10.62`.
- gitlink moved from upstream `22f694afbdc7e9766894998c3745e23f004f8b86`
  (ClickHouse/aws-sdk-cpp) to `c930cb8e8c51d4010dca68e01edf73ae1bb15af0`
  (aiven/aws-sdk-cpp). The aiven branch is that exact upstream commit **plus** the
  delegated-signer SDK commit (`2bdb77a6d9c "Allow delegating S3 signature ..."`)
  and an IPv6 host fix (`c930cb8e "Fix IPv6 S3 object storage host"`). The
  cherry-picks were clean, so the SDK surface the C++ expects is intact at
  `c930cb8e`: `Aws::S3::S3ClientConfiguration` and the
  `S3Client(AWSAuthSignerProvider, S3EndpointProviderBase, S3ClientConfiguration)`
  constructor (verified directly in the checked-out headers).

The fork was prepared and pushed **before** this port began (per the
`docs/aiven/AGENTS.md` clause that forbids pointing `.gitmodules` at a
not-yet-existing ref), so no `external_dependency` escalation was needed here.

### `Client` constructor SDK re-adaptation (conflict region)

25.8 (SDK 1.7.321) and 26.3's fork both expose the new signer-provider ctor, but
the 25.8 patch built the endpoint provider with
`Aws::MakeShared<Aws::S3::S3EndpointProvider>(Aws::S3::S3Client::ALLOCATION_TAG)`.
In the 26.3 fork's SDK, `ALLOCATION_TAG` is **not** a public static member of
`S3Client` (only `static const char* GetAllocationTag()` exists), and
`S3EndpointProvider` lives in `Aws::S3::Endpoint`. Rather than chase those names,
the port passes `endpointProvider = nullptr`: the signer-provider ctor defaults a
fresh `S3EndpointProvider` internally (verified in `S3Client.cpp`), which is
exactly what the previous legacy ctor did. This is the minimal, robust
re-adaptation and is behaviourally identical to 26.3's prior endpoint handling.

The old ctor parameter+member `Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy
sign_payloads` is removed; the equivalent policy
(`is_s3express_bucket ? RequestDependent : Never`) is now computed inside
`createSignerProvider` and handed to the signer. `Client::create` sets
`client_configuration.useVirtualAddressing = client_settings.use_virtual_addressing`
before constructing, because virtual-addressing is now read from the
(`S3ClientConfiguration`-derived) config instead of a separate ctor bool.

### `PocoHTTPClient` overlap with patch 012 (reconciled, not dropped)

015 reparents `struct PocoHTTPClientConfiguration` from
`Aws::Client::ClientConfiguration` to `Aws::S3::S3ClientConfiguration`, adds
`#include <aws/s3/S3ClientConfiguration.h>`, adds a `String signature_delegation_url`
member, and threads it through the private ctor. Patch 012 (already in history,
`dfff5e80905`) added `std::optional<String> ca_path` and a `Poco::AutoPtr<Poco::Net::Context> ca_context`
to the **same** struct/class and consolidated the throttlers into a single
`HTTPRequestThrottler request_throttler`. Both sets of changes were kept:
`ca_path`/`ca_context`/`request_throttler` are preserved and
`signature_delegation_url` is inserted alongside them (declared after
`request_token_path`, initialised between `request_throttler` and
`s3_use_adaptive_timeouts` to satisfy `-Wreorder`). The reparent is safe because
`S3ClientConfiguration` → `GenericClientConfiguration` → `ClientConfiguration`, so
every member the existing code touches (`checksumConfig`, `userAgent`,
`telemetryProvider`, `region`, `scheme`, `endpointOverride`, `retryStrategy`, …)
remains accessible, and `useVirtualAddressing` becomes available.

### Moved file

The patch's `src/Disks/ObjectStorages/S3/diskSettings.cpp` hunk was retargeted to
its 26.3 location `src/Disks/DiskObjectStorage/ObjectStorages/S3/diskSettings.cpp`
(adds the `signature_delegation_url` extern + passes
`auth_settings[S3AuthSetting::signature_delegation_url]` to
`createClientConfiguration`).

### Call-site signature updates

`createClientConfiguration` / `Client::create` signatures changed, so all callers
were updated: `src/Backups/BackupIO_S3.cpp`,
`src/Coordination/KeeperSnapshotManagerS3.cpp`,
`src/Databases/DataLake/GlueCatalog.cpp`, `src/IO/S3/Credentials.cpp` (3 sites),
and the gtests `src/IO/S3/tests/gtest_aws_s3_client.cpp`,
`src/IO/tests/gtest_readbuffer_s3.cpp`, `src/IO/tests/gtest_writebuffer_s3.cpp`.
Sites that did not previously pass `protocol` had to pass it explicitly
(`"https"`) before the new trailing `signature_delegation_url` argument. The
writebuffer gtest additionally dropped the now-removed `sign_payloads` argument
from its `DB::S3::Client` ctor call (this is 26.3-specific: that call site still
passed `PayloadSigningPolicy::Never`).

## 3. C++ review — dispositions

### A. Endpoint provider passed as `nullptr` — **intentional re-adaptation** (correctness)

See §2. Equivalent to the SDK's own default and to 26.3's prior behaviour; avoids
depending on SDK symbols (`ALLOCATION_TAG`, the `Endpoint` namespace) that differ
between the 25.8 and 26.3 forks.

### B. Fail-closed on delegation error — **kept (faithful, and the desired semantics)**

`GenerateSignature` returns `""` on transport/parse failure. An empty signature
yields a request the object store rejects, so a broken/again unreachable signer
service degrades to "writes fail", never to "writes succeed with a wrong/forged
signature". The error is logged at `ERROR`. Kept as-is.

### C. `signature_delegation_url` is an auth setting, not a disk-only knob — **noted**

It is declared in `AUTH_SETTINGS` (`src/IO/S3AuthSettings.cpp`), so it is settable
wherever S3 `auth_settings` are parsed (disk config, `<s3>` named collections).
The non-disk client-creation paths (backups, keeper, glue, credentials providers)
explicitly pass `""`, so they keep local signing. Faithful to the patch.

### D. `payloadSigningPolicy` carried by the signer, not the config — **noted**

`createSignerProvider` passes the policy to the signer (as the SDK signs through
the provider). The config's own `payloadSigningPolicy` field is left at its
default; this matches the original patch and the SDK's signer-driven signing path.

## 4. Test design

`tests/integration/test_aiven_s3_signature_delegation/` (net-new; the original
patch shipped no test). Modeled on `test_external_http_authenticator` (runs a
Python HTTP service **inside** the instance container via `copy_file_to_container`
+ `exec_in_container(..., detach=True)` + a `/health` poll) over the standard
`with_minio=True` MinIO on plain HTTP.

`signing_proxy.py` is a ~60-line SigV4 "signing proxy": it receives
`POST {"canonicalRequest": "..."}`, parses `x-amz-date` out of the canonical
request (the only per-request input it gets), and computes the final SigV4 step
exactly as `contrib/aws .../AWSAuthV4Signer.cpp` does —
`string-to-sign = "AWS4-HMAC-SHA256\n" + amzdate + "\n" + scope + "\n" + sha256hex(canonicalRequest)`,
scope `simpleDate/us-east-1/s3/aws4_request`, signing key the HMAC chain over the
MinIO secret — and returns `{"signature": "<hex>"}`. Region (`us-east-1`) and
service (`s3`) are configured to match the S3 disk (`<region>us-east-1</region>`).
It exposes `/sign` (correct), `/sign_wrong` (a valid-shaped but incorrect
signature), `/health`, and `/count` (number of `/sign` requests served).

| Case | Setup | Asserts |
|---|---|---|
| `test_signature_delegation_used_for_s3_disk` | disk `signature_delegation_url → /sign` | `CREATE`/`INSERT`/`SELECT` succeed **and** the proxy's `/count` advanced → delegation was actually used, not bypassed |
| `test_wrong_delegated_signature_fails` | disk `signature_delegation_url → /sign_wrong`, same MinIO | the write fails and the error references signature/authorization → the delegated signature is load-bearing and a wrong one is rejected by MinIO (non-vacuous) |

`s3_retry_attempts=1` so the negative write fails in seconds; `skip_access_check`
on both disks so the wrong-signature disk does not hang the startup write-probe.
No certs/secrets are generated or committed (plain HTTP MinIO).

Evidence (clean run, `build/test_aiven_s3_signature_delegation.log`):
`2 passed`. Positive: `SELECT count()=2`, data `[a, b]`, proxy `/count` advanced.
Negative: `INSERT`/`CREATE` failed with a signature/authorization error.

Run locally:

```bash
cd tests/integration
export CLICKHOUSE_TESTS_SERVER_BIN_PATH=$(git rev-parse --show-toplevel)/build/programs/clickhouse
export CLICKHOUSE_TESTS_CLIENT_BIN_PATH=$CLICKHOUSE_TESTS_SERVER_BIN_PATH
export CLICKHOUSE_TESTS_BASE_CONFIG_DIR=$(git rev-parse --show-toplevel)/programs/server
pytest test_aiven_s3_signature_delegation -v
```

## 5. Build note (this uplift)

This `build/` directory has no ninja header-dependency tracking
(`ninja -t deps … → #deps 0`; see `docs/aiven/runbooks/build-and-test.md` §7), so
editing a layout-affecting header does **not** trigger recompilation of its
includers. Patch 015 changes a **struct layout** — `PocoHTTPClientConfiguration`'s
base class moves from `Aws::Client::ClientConfiguration` to
`Aws::S3::S3ClientConfiguration` and a member is added, shifting every later
offset — **and** a function signature (the `Client` constructor loses
`sign_payloads`). Either alone is enough to produce a false-green incremental
build (stale `.o` reading wrong member offsets → runtime corruption far from the
edit, or a link error). Mitigation: ran `tmp/touch_includers.py` (the patch-012
helper, whose `CHANGED` set already lists `IO/S3/PocoHTTPClient.h` and
`IO/S3/Client.h`) to `touch` the full transitive include closure (130 `.cpp`
TUs), then `ninja -C build clickhouse`. The first `ninja` also rebuilt the changed
AWS SDK objects from the submodule bump. Final result: clean, with a real relink
(`[569/572] Linking CXX executable programs/clickhouse`, exit 0) — not a
false-green.

## 6. Rollback considerations

Reverting the commit removes the `signature_delegation_url` plumbing, the
`AWSAuthV4DelegatedSigner`, the signer-provider-based `Client` construction, and
the test together. **It also re-points `contrib/aws` back to upstream
`22f694afbdc7`** via the `.gitmodules` + gitlink revert, so the SDK delegated-signer
commit goes away with it — the revert is atomic across C++ and submodule. Disks
configured with `<signature_delegation_url>` would then fall back to local
signing (the setting becomes unknown and is ignored / rejected per S3 settings
parsing). Because the port + the test ship as one commit, the revert is clean.

## 7. Per-uplift notes

### 25.8-aiven (historical, source)

Source commit `d3b5e9016f`, itself an adaptation of the original delegated-signer
patch onto AWS SDK 1.7.321: it switched `S3Client` to the
`AWSAuthSignerProvider`-based constructor and bumped `contrib/aws` to
`aiven/clickhouse-v25.8.12.129`. It constructed the endpoint provider explicitly
with `Aws::S3::S3Client::ALLOCATION_TAG`. Shipped no test.

### 26.3-aiven (this uplift)

Ported with: the submodule bump to `aiven/clickhouse-v26.3.10.62` @ `c930cb8e`;
the `Client` ctor re-adapted to the 26.3 fork's SDK (endpoint provider `nullptr`,
no public `ALLOCATION_TAG`); the `PocoHTTPClient` change reconciled with patch
012's `ca_path`/`ca_context`/single-`request_throttler` shape; the moved
`diskSettings.cpp` retargeted; and a net-new positive+negative integration test.
Shipped as one commit so the whole unit (C++ + submodule + test) reverts/cherry-picks
atomically.

## 8. Draft commit message (human commits this; do NOT auto-commit)

```
patch-port(015): allow delegating S3 signature to a separate process

Port of `d3b5e9016f` from `v25.8.18.1-lts-aiven`. Adds a per-disk
`signature_delegation_url` S3 auth setting: when set, ClickHouse delegates AWS
SigV4 signature generation to an external HTTP service instead of signing
locally. It `POST`s the AWS canonical request as `{"canonicalRequest": "..."}`
and uses the `{"signature": "<hex>"}` it gets back, so a proxy can gate, audit,
or centrally control which S3 requests are signed.

Submodule-coupled: `contrib/aws` is redirected to Aiven's fork
`https://github.com/aiven/aws-sdk-cpp`, branch `aiven/clickhouse-v26.3.10.62` at
`c930cb8e8c51d4010dca68e01edf73ae1bb15af0` (upstream
`22f694afbdc7e9766894998c3745e23f004f8b86` plus the delegated-signer SDK commit
and an IPv6 host fix). That fork exposes `Aws::S3::S3ClientConfiguration` and the
`AWSAuthSignerProvider`-based `Aws::S3::S3Client` constructor this change needs.

Classified `still-needed-but-rewrite`:
  - The new `AWSAuthV4DelegatedSigner` subclasses `Aws::Client::AWSAuthV4Signer`
    and overrides `GenerateSignature` to POST the canonical request and parse the
    returned signature; on any error it returns "" (fail closed).
  - `S3::Client` switches to the `AWSAuthSignerProvider`-based `Aws::S3::S3Client`
    constructor via `createSignerProvider`, dropping the `sign_payloads` ctor
    parameter/member. The endpoint provider is passed as `nullptr` (the SDK
    defaults a fresh `S3EndpointProvider`), because the 26.3 fork's SDK does not
    expose a public `Aws::S3::S3Client::ALLOCATION_TAG`.
  - `PocoHTTPClientConfiguration` is reparented to `Aws::S3::S3ClientConfiguration`
    and gains a `signature_delegation_url` member. This overlaps patch 012
    (`dfff5e80905`), whose `ca_path`/`ca_context` and consolidated
    `request_throttler` on the same struct are preserved.
  - The patch's `diskSettings.cpp` hunk is retargeted to its 26.3 location under
    `src/Disks/DiskObjectStorage/ObjectStorages/S3/`.

Build note: this change alters a struct layout and a function signature in
widely-included headers, and this `build/` has no ninja header-dep tracking
(`#deps 0`), so the transitive include closure was force-recompiled before the
final relink to avoid a false-green incremental build (see
`docs/aiven/runbooks/build-and-test.md` §7).

Adds integration test `test_aiven_s3_signature_delegation`: a positive case (a
disk whose `signature_delegation_url` points at an in-container SigV4 signing
proxy can `CREATE`/`INSERT`/`SELECT`, and the proxy records the signing request)
and a negative case (a proxy endpoint that returns a wrong signature makes the
write fail with a signature/authorization error). MinIO runs over plain HTTP; no
secrets are committed. See docs/aiven/patches/015-s3-signature-delegation.md.

Co-authored-by: Kevin Michel <kevin.michel@aiven.io>
```

# Patch 016 — azure-signature-delegation

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `38e54d35892` (adapted from `0067-Delegated_signature_azure.patch` onto the 25.8 codebase + `aiven/azure-sdk-for-cpp` fork) | tilman.moeller@aiven.io (author) / kevin.michel@aiven.io (co-author) / alex.khatskevich@aiven.io (committer) | (the version we are porting FROM) |
| 26.3-aiven | `patch-port(016)` (staged) | parent agent + worker, 2026-06-02 | ported **`still-needed-but-rewrite`** — 013 reconciliation (Azure transport/`RequestSettings` rewrite) + submodule repoint + a dropped wiring hunk (upstream removed the `ObjectStorageConnectionInfo` family) + `delegated_signature = false` hardening, **shipped with a net-new two-case integration test** — see §2, §4 |

The current uplift's row stays "(staged)" until the human commits.

## 1. Purpose

Allow an Azure Blob Storage disk to delegate the final **SharedKey signing**
step to an external HTTP service through a per-disk `account_name` +
`signature_delegation_url` setting. When both are configured, ClickHouse does
not compute the SharedKey HMAC locally; instead it POSTs the Azure
`stringToSign` to the service and uses the returned signature. This lets a
proxy / external service gate, audit, or centrally control which Azure requests
get signed (security, compliance, cost accounting), since the `stringToSign`
carries everything needed to decide — verb, path, headers, content length.

This is the Azure twin of patch 015 (S3 SigV4 delegation). The wire protocol is
identical in shape; only the field name differs.

Protocol:

- ClickHouse → service: `POST` with JSON body `{"stringToSign": "..."}`
- service → ClickHouse: `200` with JSON body `{"signature": "<base64>"}`

Source SHA on `v25.8.18.1-lts-aiven`: `38e54d35892bb869d9aa43805c75ae26da43e445`.
Original author: Tilman Moeller <tilman.moeller@aiven.io>, 2025-12-13.
Co-authored-by: Kevin Michel <kevin.michel@aiven.io>.

### Mechanism (data flow)

`<account_name>` + `<signature_delegation_url>` (Azure disk config)
→ `RequestSettings::account_name` / `RequestSettings::signature_delegation_url`
  (read in `getRequestSettings`, `…/AzureBlobStorage/AzureBlobStorageCommon.cpp`)
→ `isDelegatedSignature(settings)` (true iff BOTH are set)
→ `getClientOptions`: when delegated, injects an `AzureDelegatedKeyPolicy` into
  `client_options.PerRetryPolicies`, built with a
  `StorageSharedKeyCredential(account_name, /* account_key= */ "ignored")` — the
  literal `"ignored"` key is never used locally because the signing HMAC is
  delegated.
→ `ConnectionParams::delegated_signature` (set at each construction site via
  `isDelegatedSignature`)
→ `ConnectionParams::createForContainer`: when `delegated_signature`, builds the
  `RawContainerClient` straight from the endpoint + `client_options` (the
  delegated policy carries auth), bypassing the local-auth `std::visit`.

`AzureDelegatedKeyPolicy`
(`…/AzureBlobStorage/AzureDelegatedKeyPolicy.{h,cpp}`) subclasses the Azure
SDK's `Azure::Storage::_internal::SharedKeyPolicy` and overrides the
fork-made-`virtual` `GetSignature(string_to_sign)`: it serialises
`{"stringToSign": "..."}`, POSTs it via
`makeHTTPSession(HTTPConnectionGroupType::DISK, …)`, `assertResponseIsOk`,
parses `signature`, and returns it. `Clone()` reuses the inherited
`m_credential` (made accessible by the fork's SDK commit). The SDK still builds
Azure's canonical `StringToSign`; only the final HMAC step is outsourced.

**Azure vs S3 (015) asymmetry, by design:** on a bad/unreachable signer the
Azure path **throws** (the `GetSignature` override propagates the Poco
exception), whereas S3's `AWSAuthV4DelegatedSigner` fails *closed* by returning
`""`. Both end at "the write fails", never "the write succeeds with a wrong
signature".

## 2. Upstream-drift / validity findings

`still-needed-but-rewrite`: the feature is still wanted on 26.3, the upstream
code does not exist, and the patch neither applies cleanly nor compiles against
upstream. `cherry_pick_clean=no`.

### Submodule bump (prerequisite)

This patch is submodule-coupled. `contrib/azure` is redirected to Aiven's fork:

- `.gitmodules` `[submodule "contrib/azure"]`:
  `url = https://github.com/aiven/azure-sdk-for-cpp`,
  `branch = aiven/clickhouse-v26.3.10.62`.
- gitlink moved from upstream `0f7a2013f7d79058047fc4bd35e94d20578c0d2b`
  (ClickHouse/azure-sdk-for-cpp) to
  `98519bd324221c1b2e3c7576317a6a09fa1825ea` (aiven/azure-sdk-for-cpp). The
  aiven branch is upstream base `0f7a2013f7` plus the Aiven delegated-signer SDK
  commit (which makes `Azure::Storage::_internal::SharedKeyPolicy::GetSignature`
  **`virtual`** and `m_credential` accessible) and an IPv6 support commit on top
  (`98519bd3 "Improve IPv6 support"`). Verified directly in the checked-out
  header `…/azure-storage-common/.../internal/shared_key_policy.hpp`:
  `virtual std::string GetSignature(const std::string&) const;` and
  `std::shared_ptr<StorageSharedKeyCredential> m_credential;` are present at
  `98519bd`.

The fork was prepared and pushed **before** this port resumed (per the
`docs/aiven/AGENTS.md` clause that forbids pointing `.gitmodules` at a
not-yet-existing ref), so no `external_dependency` escalation was needed. **The
C++ in this patch will not build against upstream `azure-sdk-for-cpp`** (the
`virtual GetSignature` does not exist there) — the submodule bump is load-bearing.

### Reconciliation with patch 013 (Azure Curl→Poco transport migration)

013 already rewrote Azure transport to the Poco client on 26.3, so the source's
hunks were retargeted at three structural spots:

1. **`AzureBlobStorageCommon.h` — `RequestSettings`.** The source added
   `account_name`/`signature_delegation_url` inside a `#if USE_AZURE_BLOB_STORAGE`
   Curl/`CurlOptions` block after `curl_ca_path`. On 26.3 that whole Curl block
   is gone; `RequestSettings` ends at 013's plain `std::optional<String> ca_path;`.
   The two new members are placed as plain `std::optional<String>` optionals
   **immediately after `ca_path`**, with **no `#if` wrapper** (matching `ca_path`'s
   `String` alias rather than the source's `std::string`).
2. **`AzureBlobStorageCommon.cpp` — `getRequestSettings`.** The source read the
   two config keys inside the same Curl `#if` block. The read is placed next to
   013's `ca_path` config read (`if (config.has(config_prefix + ".ca_path")) …`),
   with no `#if`.
3. **`getClientOptions` injection.** The source anchored its injection before
   `if (settings[Setting::azure_sdk_use_native_client])`; on 26.3 the throttling
   was restructured (`HTTPRequestThrottler request_throttler; if
   (settings[Setting::azure_max_get_rps] …)`). The injection block is placed
   right after `client_options.ClickhouseOptions = …`, preserving the 26.3
   throttler structure.

### Dropped hunk — `AzureObjectStorageConnectionInfo.cpp` (upstream feature removal)

The source patch also added `params.delegated_signature = false;` to
`AzureObjectStorageConnectionInfo::makeClient` (a `ConnectionParams{}`
aggregate-init site). **The entire `ObjectStorageConnectionInfo` family was
removed upstream between 25.8 and 26.3** — 8 files on the 25.8 source tree
(`IObjectStorageConnectionInfo`, `Azure`/`S3`/`Web` `…ObjectStorageConnectionInfo`),
**0 on `v26.3.10.62-lts`** (recount: `tmp/patch-016/drift-conclusion.txt`; only a
stray `ObjectStorageConnectionInfoPtr connection_info;` member declaration
survives in `StorageObjectStorageSource.cpp`). The cherry-pick surfaced this as a
`modify/delete` conflict.

This was **escalated** (it was not covered by the parent's preflight, which
assumed all four wiring sites exist) and **ratified** as policy call #4: drop the
hunk via `git rm`. It is a provable semantic no-op — the in-class
`bool delegated_signature = false;` default (policy call #3, §3) covers every
surviving `ConnectionParams{}` construction site, and the `makeClient` site no
longer exists on 26.3. No replacement wiring was added. The S3 twin (015) did not
touch any `ConnectionInfo` file (it wires delegation via the S3 client config), so
there is no analogous silent drop — the asymmetry is real.

### Moved files / surviving wiring

Source paths are `src/Disks/ObjectStorages/AzureBlobStorage/…`; on 26.3 the Azure
disk files live under `src/Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/…`
(the same reorg 013/015 handled). The two new files
(`AzureDelegatedKeyPolicy.{h,cpp}`) and the include were retargeted there. The
three surviving `.delegated_signature = isDelegatedSignature(...)` wiring sites —
`AzureObjectStorage.cpp` (`applyNewSettings`), `ObjectStorageFactory.cpp`
(`registerAzureObjectStorage`), and `Storages/ObjectStorage/Azure/Configuration.cpp`
(`getAzureConnectionParams`) — auto-merged clean.

### `delegated_signature = false` hardening (micro-deviation)

The source declared `bool delegated_signature;` **uninitialized**. It is shipped
here as `bool delegated_signature = false;` — a one-token hardening so any future
aggregate-init site (and, today, the `Configuration.cpp` site that assigns it
after default-construction) cannot read an indeterminate value. This is the only
behavioural micro-deviation from the source beyond placement/path.

### Brace normalization (cosmetic)

The cherry-picked delegation code carried a few Egyptian braces
(`if (delegated_signature) {`, `bool isDelegatedSignature(...) {`,
`namespace DB {`) and `) {` reconciliation blocks; these were normalized to
Allman to match the surrounding 26.3 files and the repo style rule. Whitespace
only, non-semantic (verified by the Tier-2 decomposition).

## 3. C++ review — dispositions

Per `docs/aiven/skills/cpp-review-checklist.md`:

- **1 Lifetime + ownership:** ✓ — `AzureDelegatedKeyPolicy` holds a
  `std::shared_ptr<StorageSharedKeyCredential>` (inherited `m_credential`) and a
  `std::string signature_delegation_url` by value; `Clone()` shares the credential
  by `shared_ptr`. The policy is owned by the SDK's `PerRetryPolicies` vector
  (`unique_ptr`). No raw ownership introduced.
- **2 Exception safety:** ✓ — `GetSignature` wraps transport in `try/catch`,
  rethrowing as a ClickHouse `Exception` via `CreateFromPocoTag`; it owns no state
  that needs rollback (it computes and returns a string). A throw propagates out
  through the SDK's signing path and fails the request — the intended (Azure)
  fail-by-throw semantics, vs S3's fail-closed `""`.
- **3 Thread-safety + concurrency:** ✓ — the policy is immutable after
  construction (`signature_delegation_url` and the credential are set in the ctor
  and only read); each request makes its own `makeHTTPSession`. No shared mutable
  state, no locks, no sleeps.
- **4 Performance + memory:** ✓ (acceptable) — signing already incurs network I/O
  by design; delegation adds one localhost HTTP round-trip per *signed request*
  (not per row). `withHTTPKeepAliveTimeout(0)` is deliberate (the proxy is
  same-host; avoids `NoMessageException` on early close). Not a per-row hot path.
- **5 Settings as public API:** ✓ — `account_name` / `signature_delegation_url`
  are per-disk Azure config keys (not global `Settings`), read in
  `getRequestSettings`. Off by default: absent keys ⇒ `isDelegatedSignature`
  false ⇒ no behaviour change. No new entry in `Settings.cpp` needed.
- **6 Error handling:** ✓ — uses `assertResponseIsOk` and rethrows Poco
  exceptions as `DB::Exception`; no invented error codes. The negative test
  asserts the auth/signature failure surfaces to the user.
- **7 Upstream / vendored code:** ✓ — no `.claude/**`, `.github/workflows/**`,
  or root `AGENTS.md` touched. `.gitmodules` is edited (permitted) and
  `contrib/azure` is repointed via the sanctioned submodule mechanism (gitlink
  move, not a hand-edit of a `contrib/**` file).
- **8 Behavior under settings:** ✓ — when delegation is off (the default), the
  injected policy is not added, `createForContainer` takes its normal `std::visit`
  path, and there are no extra allocations, HTTP calls, or log lines.

## 4. Test design

(a) **New test that exercises the patch's contribution** —
`tests/integration/test_aiven_azure_signature_delegation/` (net-new; the original
patch shipped no test). Modeled on `test_aiven_s3_signature_delegation` (015) for
structure and on `test_aiven_azure_custom_ca_path` (013) for the Azurite wiring.

- `__init__.py` (empty), `signing_proxy.py` (~75 lines), `test.py`.
- `signing_proxy.py` is an Azure-SharedKey signing proxy: it receives
  `POST {"stringToSign": "..."}` and returns
  `{"signature": base64(HMAC-SHA256(key=base64decode(account_key), msg=stringToSign))}`,
  exactly as the SDK's `SharedKeyPolicy::GetSignature` would. It runs **inside the
  instance container** (`copy_file_to_container` + `exec_in_container(detach=True)`
  + a `/health` poll). Endpoints: `/sign` (correct), `/sign_wrong` (valid-shaped
  but wrong), `/health`, `/count`. The account/key are the **public, well-known
  Azurite** development credentials (`devstoreaccount1` + its documented key) — a
  constant referenced inline with a comment; no secrets are generated or committed.
- Two cases over the standard HTTP Azurite (`with_azurite=True`, inline `disk(...)`
  definitions):

| Case | Setup | Asserts |
|---|---|---|
| `test_signature_delegation_used_for_azure_disk` | disk: `account_name=devstoreaccount1`, `signature_delegation_url → /sign`, `skip_access_check=0` | `CREATE`/`INSERT`/`SELECT` succeed **and** proxy `/count` advanced → delegation was actually used (not bypassed; the local key is `"ignored"`) |
| `test_wrong_delegated_signature_fails` | disk: `signature_delegation_url → /sign_wrong`, `skip_access_check=1`, `max_tries=1` | the write fails and the error references authentication/signature → the delegated signature is load-bearing and a wrong one is rejected by Azurite (non-vacuous, fails fast) |

- Post-patch run output (the PASS), `build/test_aiven_azure_signature_delegation.log`:

  ```text
  test_aiven_azure_signature_delegation/test.py::test_signature_delegation_used_for_azure_disk PASSED [ 50%]
  test_aiven_azure_signature_delegation/test.py::test_wrong_delegated_signature_fails PASSED [100%]
  ======================== 2 passed, 4 warnings in 7.89s =========================
  ```

- Why this distinguishes the Aiven feature from upstream behaviour: the positive
  case can only pass if the delegated POST happens (the proxy `/count` advances),
  which only the Aiven `AzureDelegatedKeyPolicy` path produces; without the patch
  the disk would either ignore `signature_delegation_url` and sign locally (no
  `/count` advance ⇒ assertion fails) or not compile (no `virtual GetSignature`).

### Evidence-pair note (causation model)

A stateless pre/post **worktree-flip is not a trustworthy option here**, for two
independent reasons: (1) the patch changes **struct layout** (`RequestSettings`
and `ConnectionParams` gain members) in a widely-included header, and this
`build/` has **no ninja header-dep tracking** (`#deps 0`, see
`build-and-test.md` §7), so a flipped incremental build risks a *false-green*;
(2) the feature is genuinely **cluster-level** (Azure object storage) and is
**submodule-coupled** (the pre-patch C++ cannot compile against upstream's SDK,
which lacks the `virtual GetSignature`). The causation evidence is therefore the
**two-case integration pair**: the positive case proves the delegated path is
*taken* (`/count` advanced while the local key is `"ignored"`); the negative case
proves the delegated signature is *load-bearing* (a wrong signature is rejected by
Azurite). This satisfies `docs/aiven/AGENTS.md` §7 for a submodule-coupled,
cluster-level feature — exactly as accepted for 015.

Run locally:

```bash
cd tests/integration
export CLICKHOUSE_TESTS_SERVER_BIN_PATH=$(git rev-parse --show-toplevel)/build/programs/clickhouse
export CLICKHOUSE_TESTS_CLIENT_BIN_PATH=$CLICKHOUSE_TESTS_SERVER_BIN_PATH
export CLICKHOUSE_TESTS_BASE_CONFIG_DIR=$(git rev-parse --show-toplevel)/programs/server
pytest test_aiven_azure_signature_delegation -v
```

## 5. Rollback considerations

- Reverting the commit removes the `account_name`/`signature_delegation_url`
  plumbing, `AzureDelegatedKeyPolicy`, the `getClientOptions` injection, the
  `createForContainer` delegated branch, the wiring, and the test together. **It
  also re-points `contrib/azure` back to upstream `0f7a2013f7`** via the
  `.gitmodules` + gitlink revert, so the delegated-signer SDK commit goes away with
  it — the revert is atomic across C++ and submodule.
- No on-disk format change, no schema migration, no ZK state. Disks configured
  with `<signature_delegation_url>` would, after revert, ignore the unknown key and
  fall back to local SharedKey signing (which requires a real `account_key`).
- To disable without rebuilding: remove `signature_delegation_url` (or
  `account_name`) from the disk config — `isDelegatedSignature` then returns false
  and the disk signs locally.

## 6. Build note (this uplift)

The `contrib/azure` bump (0f7a2013f7 → 98519bd) forces a heavier, partial-cold
rebuild (Azure SDK objects recompile + relink). The build was clean: a fresh
`cmake --fresh` reconfigure was needed first (the `build/` had a missing
`rules.ninja` — `build-and-test.md` §6 row 1, unrelated to the patch), then
`ninja -C build clickhouse` finished with a real relink
(`[516/517] Linking CXX executable programs/clickhouse`, exit 0, ~96 s warm-ish).

This patch *does* change struct layout in `AzureBlobStorageCommon.h`
(`RequestSettings` +2 members, `ConnectionParams` +1), the classic `#deps 0`
false-green hazard. It was sidestepped here because the full reconfigure +
SDK-bump rebuild recompiled the affected TUs from scratch (not a stale
incremental), and the post-build integration test passed against the freshly
linked binary — so the layout change is proven consistent, not false-green.

## 7. Per-uplift notes

### 25.8-aiven (historical, source)

Source commit `38e54d35892`, itself an application of
`0067-Delegated_signature_azure.patch` onto the 25.8 codebase. Bumped
`contrib/azure` to `aiven/clickhouse-v25.8.12.129` @ `3278daf75ac`; added
`AzureDelegatedKeyPolicy`, the `RequestSettings` fields (inside the Curl `#if`
block), the `ConnectionParams::delegated_signature` flag (uninitialized), and the
four wiring sites (including the now-removed `AzureObjectStorageConnectionInfo`).
Shipped no test.

### 26.3-aiven (this uplift)

Ported `still-needed-but-rewrite` with: the submodule bump to
`aiven/clickhouse-v26.3.10.62` @ `98519bd` (base `0f7a2013f7`); the 013
reconciliation (Curl→Poco transport — the `RequestSettings` members and
`getRequestSettings`/`getClientOptions` reads relocated out of the gone Curl `#if`
blocks); the `DiskObjectStorage/` path retarget for the two new files; the
`delegated_signature = false` hardening; and **policy call #4** — the
`AzureObjectStorageConnectionInfo.cpp` wiring hunk dropped because upstream removed
the entire `ObjectStorageConnectionInfo` family (escalated, ratified; provable
no-op via the in-class default). Net-new positive+negative integration test.
Shipped as one commit so the C++ + submodule + test revert/cherry-pick atomically.

- Cherry-pick was: conflict-resolved (4 conflict regions + 2 file-location moves +
  1 modify/delete drop).
- Upstream-drift conclusion: `still-needed-but-rewrite` (§2).
- Test added at: `tests/integration/test_aiven_azure_signature_delegation/`.
- Time-to-port: worker wall-clock across two dispatches (escalation + completion);
  warm-ish cache (one `cmake --fresh` + Azure-SDK-bump rebuild ≈ 96 s).
- Anything surprising: the entire `ObjectStorageConnectionInfo` family vanished
  upstream between 25.8 and 26.3, turning a presumed one-line wiring site into an
  uncovered `modify/delete` — handled via escalation + policy call #4.

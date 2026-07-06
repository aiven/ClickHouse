# Patch 013 — azure-custom-ca-path

> **Status: ported (with proportionate test), shipped in `patch-port(013)`.**
> Classified `still-needed-but-rewrite`: the original Curl-transport mechanism is
> obsolete on 26.3, so the port is a rewrite mirroring patch 012 on the Poco
> transport (§3). Ships a light Azure-config integration test (§5); end-to-end
> TLS-verification evidence is inherited from 012.

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `2f70d49490` | tilman.moeller@aiven.io (author) / kevin.michel@aiven.io (co-author) | (the version we are porting FROM) |
| 26.3-aiven | `patch-port(013)` | parent agent, 2026-06-01 | **`still-needed-but-rewrite`** — original mechanism obsoleted by upstream's Azure transport migration (§2); rewritten as a mirror of 012 (§3), shipped with a proportionate integration test (§5) |

## 1. Purpose

Allow an Azure Blob Storage disk to pin a custom CA bundle through a per-disk
`<ca_path>` setting, so ClickHouse can speak TLS to an Azure endpoint whose
server certificate is signed by a private / self-signed CA **without disabling
certificate verification globally**. This is the Azure counterpart of patch 012
(S3).

### Mechanism on 25.8 (source) — *Curl transport*

The 25.8 patch (8 loc, 2 files) was small because the Azure SDK used its own
**Curl** transport, which exposes a CA option directly:

- add `std::optional<std::string> curl_ca_path` to `RequestSettings`;
- `getRequestSettings` reads `config_prefix + ".ca_path"`;
- `getClientOptions` sets `curl_options.CAInfo = curl_ca_path` on
  `Azure::Core::Http::CurlTransportOptions`, used to build a
  `Azure::Core::Http::CurlTransport`.

The original commit message called this *"simpler than S3… no HTTP layer
modifications needed"*. **On 26.3 that is inverted** (see §2).

### Mechanism on 26.3 (as ported) — *Poco transport*

`<ca_path>` (Azure disk config)
→ `RequestSettings::ca_path` (`…/AzureBlobStorage/AzureBlobStorageCommon.{h,cpp}`)
→ `PocoAzureHTTPClientConfiguration::ca_path` (`src/IO/AzureBlobStorage/PocoHTTPClient.h`)
→ `PocoAzureHTTPClient::ca_context`, a `Poco::Net::Context` built once from the bundle
→ `makeHTTPSession(..., ca_context)` (`src/IO/HTTPCommon.h`)
→ `HTTPConnectionPool` keyed per endpoint **and trust anchor** (012's fix).

This is the *same* trust path that 012 built for S3.

## 2. Upstream-drift / validity findings — the faithful patch is dead on 26.3

The 25.8 patch will neither apply nor compile on 26.3, for three independent
reasons:

- **File moved.** `src/Disks/ObjectStorages/AzureBlobStorage/` →
  `src/Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/`. Guarantees
  `cherry_pick_clean=no` on its own.
- **Transport replaced (decisive).** 26.3 no longer uses the Azure SDK's
  `CurlTransport`. `getClientOptions` now builds ClickHouse's own
  `PocoAzureHTTPClient`:

  ```cpp
  // AzureBlobStorageCommon.cpp
  client_options.Transport.Transport = std::make_shared<PocoAzureHTTPClient>(conf);
  ```

  There is no `CurlTransportOptions`, no `IPResolve`, and no `CAInfo` anywhere —
  the patch would set a field on an object that is never constructed.
- **Anchor fields removed.** The 25.8 `RequestSettings` carried
  `using CurlOptions = …; CurlOptions::CurlOptIPResolve curl_ip_resolve;`. On
  26.3 that block is gone and `getRequestSettings` parses no `curl_*` option, so
  even force-applied the patch would not compile.

This is upstream commit-level drift: upstream migrated Azure HTTP from the SDK's
Curl transport to a ClickHouse-native Poco client. The *feature* is still wanted,
so the disposition is `still-needed-but-rewrite`.

## 3. The 26.3 rewrite — a mirror of 012 (smaller, because 012 built the infra)

`PocoAzureHTTPClient` (`src/IO/AzureBlobStorage/PocoHTTPClient.{h,cpp}`) is a
structural twin of S3's `PocoHTTPClient`. It already called the 012-extended
`makeHTTPSession`, passing an **empty** context. The port fills that slot. Four
touch points (4 files, +43/−5), implemented as:

1. `PocoAzureHTTPClientConfiguration` (`.h`): added `std::optional<String> ca_path`,
   **at the end of the struct** (see §6).
2. `PocoAzureHTTPClient` (`.h`/`.cpp`): added a cached
   `Poco::AutoPtr<Poco::Net::Context> ca_context` **at the end of the class**,
   built **once in the ctor** via `makeCAContext`, and passed at the call site
   instead of `{}`. The stale comment ("Azure doesn't use custom CA certificates")
   was replaced.
3. `getRequestSettings`: reads `config_prefix + ".ca_path"` into
   `RequestSettings::ca_path` (guarded by `config.has(...)`).
4. `getClientOptions`: sets `.ca_path = request_settings.ca_path` on the
   `PocoAzureHTTPClientConfiguration conf{…}` designated initializer (placed last,
   matching declaration order — required by C++20 designated-initializer rules).

Resolved design decision: `makeCAContext` is **duplicated** as a file-local
`static` in Azure's `PocoHTTPClient.cpp` (not shared). This is deliberate: S3's
`PocoHTTPClient.cpp` defines `DB::makeCAContext` with **external** linkage, so a
second external definition would be an ODR clash / duplicate symbol. Internal
linkage keeps the two transport clients independent and link-safe. Cost: if S3's
helper changes, Azure's must be updated in lockstep.

## 4. Relationship to 012 — shared machinery and inherited bug posture

013 rides the *identical* transport machinery 012 modified, so 012's review
findings carry over:

- **Bug A (pool-key trust isolation) — already fixed, covers Azure for free.**
  Azure's client uses the same `HTTPConnectionPool` (group `DISK`/`STORAGE`).
  012 made `EndpointPoolKey` include the SSL-context pointer, so an Azure
  custom-CA disk gets its own pool, isolated from default-trust Azure/S3
  connections. **Without 012's fix, 013 would re-expose the same trust-widening
  hole on Azure.** This is what makes the chain order `012→013` load-bearing.
- **Bug B (per-request context rebuild) — must be avoided here too.** Build the
  Azure `ca_context` once in the ctor; never per request. (A per-request context
  would also defeat pooling, since the pool key is the context pointer.)
- **Bug C (`loadDefaultCAs=false` sole-trust) — same sharp edge.** If we reuse
  `makeCAContext`, an Azure custom-CA disk trusts *only* the supplied bundle.
  Same intent as S3; document in the changelog.

## 5. Test strategy — research and decision

### 5.1 The mechanism is load-bearing (verified)

`makeHTTPSession` falls back to the global default Poco SSL context when no
context is passed:

```cpp
// src/IO/HTTPCommon.cpp
if (!context)
    context = Poco::Net::SSLManager::instance().defaultClientContext();
auto connection_pool = HTTPConnectionPools::instance().getPool(group, uri, proxy_configuration, context);
```

So the 012 **inversion** design transfers exactly: with global
`verificationMode=strict` and no global CA, an Azure disk *without* `<ca_path>`
(empty context → strict global context) fails verification against a self-signed
endpoint, while a disk *with* `<ca_path>` succeeds. The CA path is the sole trust
anchor → not vacuous.

### 5.2 Upstream-reuse survey — nothing reusable for a TLS-Azure endpoint

Surveyed every auxiliary container and TLS-capable setup in
`tests/integration/compose/`:

| Candidate | What it is | Reusable for Azure-TLS? |
|---|---|---|
| `minio_certs_dir` (`test_s3_with_https`, …, our 012 test) | MinIO with `${MINIO_CERTS_DIR}:/certs` + `--certs-dir` + `wait_minio_to_start(secure=…)` | No (S3 protocol) — but the *template* to mirror |
| `proxy1/proxy2` (`clickhouse/s3-proxy`, 80/443) | HTTPS **forward** proxies for proxy-behavior tests | No — forward proxy, not a TLS reverse-terminator |
| `docker_compose_nginx.yml` (`clickhouse/nginx-dav`) | WebDAV static server, **port 80 only** | No — no TLS |
| `docker_compose_letsencrypt_pebble.yml` | ACME test CA | No — issues certs, not an object-store front |
| `with_azurite` | Azurite, hardcoded `http://` in 3 places (compose command, `setup_azurite_cmd`, `wait_azurite_to_start`) | No — HTTP only |

Findings:

- **No upstream test does Azure-over-HTTPS / Azure-with-TLS / Azure custom CA**
  (confirmed by searching every Azure test for `cert|ssl|tls|https`).
- **There is no per-test compose convention** (`docker_compose_yml_dir` is
  hardcoded to the shared `tests/integration/compose/`; no test ships its own
  compose; no test injects one via `cluster.base_cmd.extend` — that is used only
  internally by the `setup_X_cmd` helpers). A self-contained TLS Azurite is
  *possible* via a `base_cmd.extend` injection + a test-managed readiness probe,
  but it is **novel, fragile, and has zero precedent**.
- Azurite 3.35.0 itself supports TLS (`--cert/--key`), so the container is not
  the blocker — only the harness wiring is.

### 5.3 Decision — proportionate coverage (chosen)

A full end-to-end TLS-Azure integration test would require either a shared
`cluster.py` change (`azurite_certs_dir`, mirroring `minio_certs_dir` —
permanent upstream divergence to re-carry across LTS lines) or the fragile
self-contained injection above. Neither is justified, because **013's only new
risk is the config plumbing** `Azure <ca_path> → RequestSettings → client config
→ context`; the TLS-verification-through-Poco question was already answered green
by 012's integration test (identical `makeHTTPSession`/`HTTPConnectionPool`/
`Poco::Net::Context` machinery).

### 5.4 As implemented — `tests/integration/test_aiven_azure_custom_ca_path/`

The light Azure-config test runs against the **standard HTTP Azurite** harness
(no TLS infrastructure, no `cluster.py` divergence), using inline `disk(...)`
definitions (the `test_endpoint_*` pattern from `test_merge_tree_azure_blob_storage`).

| Case | Setup | Asserts |
|---|---|---|
| `test_azure_custom_ca_path_valid_is_accepted` | inline Azure disk **with** a valid `ca_path` (a PEM minted fresh per run by `certs/generate_ca.sh`, mounted into `config.d/`) | `INSERT`/`SELECT` over HTTP Azurite succeed → a valid `<ca_path>` is parsed, threaded, and `makeCAContext` builds it without breaking normal operation |
| `test_azure_bogus_ca_path_fails` | inline Azure disk with a **nonexistent** `ca_path` | the disk fails, and the error **names the configured path** and is a file-load failure → `<ca_path>` is read from config and consumed by `makeCAContext` |

Evidence (clean run, both pass in ~6s): the negative case surfaces
`Code: 1000. DB::Exception: ... File not found:
/etc/clickhouse-server/config.d/this_azure_ca_file_does_not_exist.crt` with a
`Poco::FileNotFoundException` — i.e. `makeCAContext` loading the configured CA
file. The assertion pins on the configured path appearing in the error (a
non-vacuous, causally-specific check), not on generic keywords.

The earlier "fast-fail knobs" follow-up turned out **moot**: a bad `ca_path`
throws **synchronously** while the Azure client is constructed (`Poco::Net::Context`
file load), before any network/retry loop — so unlike S3's TLS-handshake negative
case, no `s3_retry_attempts`-style throttling is needed here. The negative disk
uses `skip_access_check = 1` so nothing probes the endpoint before the context
build fails.

What the test does **not** exercise (by design, per §5.3): the TLS handshake that
*uses* `ca_context` to verify a peer (Azurite is HTTP), pool-context isolation
(Bug A), and the sole-trust semantics — all inherited from 012's S3 test over the
identical shared machinery.

Local harness note: running the integration test under the system Python needs
`nats-py` installed (an unrelated optional dep imported before `filelock` in one
`try` block in `helpers/cluster.py`; its absence silently leaves `FileLock`
unbound). Installed into the user site via `uv`; not a repo change.

If belt-and-suspenders full E2E is wanted later, the clean route is Option A
(add `azurite_certs_dir` to the harness, ideally upstreamed to erase the
divergence) — recorded here so the option is not lost.

## 6. Build note

013 adds `ca_path` to `PocoAzureHTTPClientConfiguration` and a `ca_context` member
to `PocoAzureHTTPClient` — a `sizeof` change. The ninja `#deps 0` false-green
hazard documented for 012 (see `docs/aiven/runbooks/build-and-test.md`) is
**sidestepped here**: both new members were appended at the **end** of their
struct/class, so no existing member offset shifts, and the only `sizeof`-sensitive
construction site (`getClientOptions` → `make_shared<PocoAzureHTTPClient>`) lives
in `AzureBlobStorageCommon.cpp`, which ninja recompiles by mtime. The incremental
build was verified green: the Azure TUs recompiled and the executable relinked.

## 7. Per-uplift notes

### 25.8-aiven (historical, source)

Source commit `2f70d49490`. Configured the Azure SDK's Curl transport via
`curl_options.CAInfo`; 8 loc, 2 files.

### 26.3-aiven (this uplift)

Mechanism obsoleted by the Curl→Poco Azure transport migration. Rewritten as a
mirror of 012 on `PocoAzureHTTPClient` (§3); trust isolation (Bug A) inherited
from 012's `HTTPConnectionPool` fix; coverage is proportionate (light config test
+ inherited 012 evidence) per §5.3–5.4. Port + test ship as one commit
(`patch-port(013)`) so the unit cherry-picks cleanly to other LTS lines. Depends
on 012 being present first (the context-keyed pool).

# Patch 018 — enforce-https-url-storage

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `ae35b0cc72` | Tilman Moeller (author) / Aliaksei Khatskevich (committer), 2025-12-13 | (the version we are porting FROM) |
| 26.3-aiven | `patch-port(018)` (`f7093e7c606`) | T3 patch worker (subagent), 2026-06-02 | `still-needed-but-rewrite` — cherry-pick + rewrite (config-gated `ServerSetting` + cluster extension); see §2, §6 |

The current uplift's row is committed as `f7093e7c606`. Co-authored on
the source by Joe Lynch <joe.lynch@aiven.io>.

## 1. Purpose

Enforce HTTPS/TLS for ClickHouse features that fetch from remote URLs, so that
credentials, queries, and data are never transmitted in plaintext. Two surfaces
in the original patch: the `URL` table engine / `url` table function
(`StorageURL`) and HTTP dictionary sources (`HTTPDictionarySource`). Aiven needs
this to meet compliance requirements (PCI-DSS, HIPAA, GDPR) and to protect
against man-in-the-middle interception on managed deployments.

The durable "why": a managed fleet must be able to guarantee that no tenant can
exfiltrate or ingest data over an unencrypted channel via these URL surfaces.

Source SHA on `v25.8.18.1-lts-aiven`: `ae35b0cc725f96f12bf4d141664ee2d7da9765da`
(inventory row 018). Original author: `tilman.moeller@aiven.io`, 2025-12-13.
Original purpose (quoted from the commit body):

> This commit enforces HTTPS/TLS encryption for all HTTP/HTTPS connections in two
> ClickHouse features: 1. HTTP Dictionary Sources [...] 2. URL Storage [...]
> Previously, both features allowed unencrypted HTTP connections, which exposed
> credentials, queries, and data to potential interception. Now, any attempt to
> use HTTP (non-HTTPS) URLs is rejected with a clear error message.
> [...] Breaking change: Existing configurations using http:// URLs will fail and
> must be updated to use https:// URLs.

## 2. Upstream-drift findings

> Verify the patch is still SEMANTICALLY correct against `v26.3.10.62-lts`.

### Commands run

```bash
git show ae35b0cc725f96f12bf4d141664ee2d7da9765da > tmp/patch-018/source.diff
# Anchors re-verified on HEAD (see tmp/patch-018/drift-conclusion.txt):
rg -n 'https|require_secure|enforce_https' src/Core/ServerSettings.cpp
git cherry-pick --no-commit -x ae35b0cc725f96f12bf4d141664ee2d7da9765da
```

### Findings

- **Touched files (3 originals):**
  - `src/Storages/StorageURL.cpp`: ctor + `checkURL(Poco::URI(uri))` anchor intact;
    `BAD_ARGUMENTS` already in `ErrorCodes`; `Core/ServerSettings.h` already
    included.
  - `src/Dictionaries/HTTPDictionarySource.{h,cpp}`: `Configuration::url` still
    `std::string`; six `Poco::URI uri(configuration.url)` sites present;
    `.url = uri` in the registration lambda intact.
- **Behavior-relevant upstream drift:** `HTTPDictionarySource::loadAll`/`loadIds`/
  `loadKeys` changed return type from `QueryPipeline` to `BlockIO`, and
  `loadIds`/`loadKeys` parameter types from `std::vector<...>` to
  `VectorWithMemoryTracking<...>`. The patch's actual changed lines (removal of the
  local `Poco::URI uri(configuration.url)`) are unaffected by this; the
  `git cherry-pick --no-commit -x` auto-merged all three original files cleanly
  (no conflict markers).
- **No pre-existing upstream HTTPS-enforcement for URL storage.** `ServerSettings.cpp`
  contains only the sibling transport toggles `mysql_require_secure_transport` /
  `postgresql_require_secure_transport`; there is no `url`/`https` enforcement
  setting. `enforce_https_for_url_storage` is net-new.
- Conclusion: **`still-needed-but-rewrite`.** The cherry-pick lands the faithful
  baseline for provenance; we then rewrite the three hardcoded checks into a
  config-gated form and extend enforcement to a 4th file (`StorageURLCluster.cpp`).

## 3. C++ review

Per `docs/aiven/skills/cpp-review-checklist.md`:

- **1 Lifetime + ownership:** ✓ — no new raw pointers/ownership; the gated checks
  read through the existing `ContextPtr` (`context_` / `context`) already in
  scope at each construction site.
- **2 Exception safety:** ✓ — the new `throw` fires at storage/dictionary
  CONSTRUCTION, before any resource (socket, buffer) is acquired; no partial
  mutation to roll back. It runs before the existing `checkURL`, mirroring the
  pre-existing throw-on-bad-URL contract callers already handle.
- **3 Thread-safety + concurrency:** ✓ — `getServerSettings()` returns the
  server-global immutable `ServerSettings` (set at startup); read-only access, no
  new lock, no sleep.
- **4 Performance + memory:** ✓ — control path, not a per-row hot path. The check
  is one bool read plus, when enabled, one `Poco::URI` parse of the scheme at
  construction. When the setting is off (default) it is a single short-circuited
  bool read — no allocation.
- **5 Settings as public API:** ✓ — `enforce_https_for_url_storage` declared in
  `src/Core/ServerSettings.cpp` (`DECLARE(Bool, ..., false, ...)`). A
  `ServerSetting` (not a query `Setting`), so `SettingsChangesHistory.cpp` is
  exempt and there is no `SET` path. Default `false` preserves backward compat.
- **6 Error handling:** ✓ — error codes are real and already externed:
  `BAD_ARGUMENTS` (StorageURL / StorageURLCluster) and `UNSUPPORTED_METHOD`
  (HTTPDictionarySource). Messages are distinctive (`URL storage supports only
  HTTPS protocol`, `Only https scheme is supported for HTTPDictionarySource`) and
  the test asserts on those substrings, not just the shared code.
- **7 Upstream / vendored code:** ✓ — only `src/**` files touched; no `contrib/**`,
  `.claude/**`, `.github/workflows/**`, or root `AGENTS.md`.
- **8 Behavior under settings:** ✓ — with the setting off (default), the
  short-circuited `&&` means the scheme is never parsed and nothing observable
  changes (no log spam, no extra alloc); byte-for-byte upstream behavior.

## 4. Test design

(a) **New integration test that fails on the parent commit and passes after the
patch.**

- Test path: `tests/integration/test_aiven_enforce_https_url/` (`test.py`,
  `__init__.py`, `configs/enforce_https.xml`, `configs/remote_servers.xml`).
- Two instances on the SAME binary: `node_enforced` (overlay
  `<enforce_https_for_url_storage>true</...>` + a one-shard `test_url_cluster`
  for `urlCluster` to resolve) and `node_default` (no overlay → default off).
- Five cases:
  1. `test_url_engine_rejects_http` — `url('http://...')` on `node_enforced` →
     error contains `URL storage supports only HTTPS protocol`.
  2. `test_url_cluster_rejects_http` — `urlCluster('test_url_cluster',
     'http://...')` → same message (proves the cluster bypass is closed).
  3. `test_http_dictionary_rejects_http` — `CREATE DICTIONARY ...
     SOURCE(HTTP(url 'http://...'))` + `SYSTEM RELOAD DICTIONARY` → error
     contains `Only https scheme is supported for HTTPDictionarySource`.
  4. `test_setting_not_session_overridable` — `SET enforce_https_for_url_storage
     = 0` → `UNKNOWN_SETTING` ("neither a builtin setting nor started with the
     prefix 'custom_'"), proving there is no session/query path to weaken it.
  5. `test_default_off_allows_http` — on `node_default`, `url('http://127.0.0.1:8123/
     ?query=select 1 format CSV')` returns `1`, proving default = upstream
     behavior (no regression).

- **Evidence-of-causation pair (AGENTS §7(a))** — captured via the
  worktree-flip mechanic (integration-tests runbook §7.3): pre-patch binary
  (`git restore --source=HEAD --worktree`, rebuild) vs post-patch binary.
  - Pre-patch run (`tmp/patch-018/test-prepatch.log`): `3 failed, 2 passed`. The
    three enforcement cases FAIL at the assertion lines 67/76/97 — the `http://`
    query throws a *network* exception (no HTTPS gate exists), so the
    "...HTTPS..." substring is absent. The two non-enforcement cases pass
    vacuously (the setting never existed pre-patch, and HTTP works by default).
  - Post-patch run (`tmp/patch-018/test-postpatch.log`): `5 passed`.
- **Second, independent evidence axis (same-binary config contrast):** in the
  post-patch run, `node_enforced` rejects `http://` on all three surfaces while
  `node_default` accepts it (reads `1` over HTTP) — behavior switched ONLY by the
  server config, demonstrating the gate is the cause and the default is clean.
- **Why this distinguishes the Aiven gate from upstream (AGENTS §7):** the test
  asserts the Aiven-specific message substrings (`URL storage supports only HTTPS
  protocol`, `Only https scheme is supported for HTTPDictionarySource`), not just
  the shared `BAD_ARGUMENTS`/`UNSUPPORTED_METHOD` codes. Pre-patch the same
  `http://` URL throws a different (network) exception, so the assertion is
  provably specific to the Aiven check.

## 5. Rollback considerations

- **Revert safety:** safe. No schema migration, no on-disk format change, no ZK
  state. Reverting the code removes the gate; reverting only the config
  (`enforce_https_for_url_storage`) disables it without a rebuild.
- **State surviving restart:** none. The setting is read at startup from the
  server config; there is no persisted state.
- **Disable without rebuilding:** remove/zero `enforce_https_for_url_storage` in
  the server config (default is `false`). Because it is a `ServerSetting`, it
  cannot be toggled per-session — only via the managed config overlay.
- **Production handover:** the security guarantee is OFF by default in the
  binary. Aiven's managed config MUST set
  `<enforce_https_for_url_storage>true</enforce_https_for_url_storage>` for the
  guarantee to hold (mirrors the 017 code→config handover for
  `mysql_require_secure_transport`).

## Rewrite: hardcode → ServerSetting

The original 25.8 patch hardcoded `if (scheme != "https") throw` on every
surface — an unconditional, non-disableable, default-ON behavior change. On 26.3
that is a clause-(v) blast-radius hazard: ~2600 existing tests use `http://`
URLs, and any tenant relying on plain-HTTP URL ingestion would break out of the
box. The ratified rewrite gates every check behind a new server setting:

```cpp
if (context->getServerSettings()[ServerSetting::enforce_https_for_url_storage]
        && Poco::URI(uri).getScheme() != "https")
    throw Exception(...);
```

**Why `ServerSettings`, not `Settings` — this namespace split IS the security
boundary.** Query `Settings` are mutable per session/profile/query
(`SET ...`, a profile, a `SETTINGS` clause, or a dictionary `<settings>` block,
which injects query `Settings`). A security control that a tenant can turn off
in their own session is not a control. `ServerSettings` come ONLY from the
server config (`config.xml`); there is no `SET` path. `test_setting_not_session_
overridable` proves this empirically: `SET enforce_https_for_url_storage = 0`
is rejected with `UNKNOWN_SETTING`. Default `false` keeps clause (v) clean —
byte-for-byte upstream behavior out of the box; Aiven flips it on via the managed
config overlay.

## Cluster extension (4th file, beyond the original patch)

The original patch touched 3 files and did NOT guard `urlCluster`.
`StorageURLCluster` has its OWN constructor (`src/Storages/StorageURLCluster.cpp`)
that does NOT route through `StorageURL`'s ctor, so without an explicit guard the
`urlCluster(...)` table function would be a bypass for the exact protection the
patch adds to `url(...)`. We added the same gated check there (with
`#include <Core/ServerSettings.h>`, `extern const int BAD_ARGUMENTS;`, and the
`namespace ServerSetting` extern block). `test_url_cluster_rejects_http` proves
the bypass is closed.

## Residual gaps (noted, not necessarily fixed)

- **Only the three named surfaces are guarded.** Other URL/HTTP entry points
  (e.g. `s3`/`azureBlobStorage`/`hdfs` table functions, the `Kafka`/`NATS` engines,
  remote dictionary sources of other types, `executable`/`http` UDF sources) are
  NOT covered by this setting. Those use different schemes/transports and are out
  of scope for this patch; if a future hardening goal needs them, each has its own
  construction site to gate.
- **Scheme comparison is case-sensitive (`!= "https"`).** `Poco::URI::getScheme()`
  lowercases the scheme on parse, so `HTTPS://...` is normalized to `https` and
  accepted — no gap. Recorded for completeness.
- **Redirect-time scheme is not re-checked.** The gate validates the initial URL
  scheme at construction; an `https://` endpoint that 30x-redirects to `http://`
  is governed by the existing redirect/remote-host-filter machinery, not by this
  check. Out of scope here.

## 6. Per-uplift notes

### 25.8-aiven (historical)

Original carry: `ae35b0cc72` (author Tilman Moeller, committer Aliaksei
Khatskevich, 2025-12-13; co-authored by Joe Lynch). Hardcoded unconditional
`scheme != "https"` rejection in `StorageURL::StorageURL` and in
`registerDictionarySourceHTTP`, plus the `HTTPDictionarySource::Configuration::url`
type change `std::string` → `Poco::URI`. Used non-Allman (`if (...) {`) brace
style.

### 26.3-aiven (this uplift)

- Cherry-pick: `--no-commit -x`, auto-merged the 3 original files cleanly (the
  `BlockIO`/`VectorWithMemoryTracking` drift in `HTTPDictionarySource.cpp` did not
  conflict). Then **rewritten**: the three hardcoded checks were converted to the
  config-gated form, the new `ServerSetting` `enforce_https_for_url_storage` was
  added to `src/Core/ServerSettings.cpp`, enforcement was extended to
  `StorageURLCluster.cpp` (4th file), and all inserted code uses Allman braces
  (the original's brace style would fail the style check).
- Upstream-drift conclusion: `still-needed-but-rewrite` (§2).
- Test added at: `tests/integration/test_aiven_enforce_https_url/` (§4). Pre-patch
  `3 failed, 2 passed`; post-patch `5 passed`. Build clean (exit 0) on both
  binaries.
- `byte_equivalent: false` (intentional — added a `ServerSetting`, gated three
  checks, added a 4th file, reformatted to Allman). The patch-id decomposition
  shows every delta is exactly one of those ratified categories — no foreign
  semantic delta.
- Time-to-port: warm-cache build dir throughout (post-patch ~51s, pre-patch ~38s,
  restore-rebuild ~21s). Two integration runs ~10s each.
- Surprising bit: the cherry-pick of the original 3 files applied with NO conflict
  despite the `QueryPipeline`→`BlockIO` return-type drift, because the patch's
  changed lines were all inside function BODIES and untouched by the signature
  drift.

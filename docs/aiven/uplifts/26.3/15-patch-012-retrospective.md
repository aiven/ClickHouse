# Phase 2.1 — patch 012 (S3 custom CA path) retrospective

> **What this is:** The reflection-after-cost step applied to the first Phase-2
> patch and the first **port-with-fix** of the uplift — the first time we ported
> an Aiven patch *and* fixed latent bugs inside it *and* shipped an integration
> test, all as one cherry-pickable commit.
> **Date:** 2026-06-01.
> **Mode:** interactive parent-agent session (not a dispatched worker), human in
> the loop throughout.
> **Outcome:** `still-needed-but-rewrite`, ported with two bug fixes + integration
> test. Staged as a single commit (not yet committed at time of writing).

## Headline

Patch 012 ("Allow custom CA certificate path for S3 connections", source
`8fc1c96ae0` on `v25.8.18.1-lts-aiven`) is the first Phase-2 (object-storage)
patch. Unlike every Phase-1 patch — which was either a clean port, a
`test_design_blocked`/`no_justified` port, or a drop — 012 required us to **find
and fix bugs that were present in the original Aiven patch**, write the uplift's
**first real integration test**, and absorb a dangerous **build-infrastructure
failure** along the way.

Three firsts, each with a lesson:

1. **First port-with-fix.** The faithful port reproduced two bugs (a
   trust-isolation hole in the connection-pool key, and a per-request SSL-context
   rebuild). We shipped the port *and* the fixes *and* the test as one commit so
   the whole unit cherry-picks cleanly to 25.3/25.8 if needed.
2. **First integration test in the uplift** (`tests/integration/test_aiven_s3_custom_ca_path`).
3. **First time the build environment silently miscompiled us** — ninja has no
   header-dependency tracking in this `build/` dir, so a struct-layout change
   produced a server-side fatal exception from a stale object file.

## What worked

1. **Cross-LTS comparison established bug provenance.** Comparing the 25.3 and
   25.8 variants showed the connection-pool-key omission (Bug A) is *inherited*,
   not introduced by 26.3 drift. That turned "is this our rewrite's fault?" into
   "this is a pre-existing upstream bug we should fix while we're here."
2. **The C++ review found real bugs, not nits.** Bug A (pool key omits the SSL
   context → "first context wins" across trust anchors) is a genuine
   trust-isolation defect; Bug B (context rebuilt per request/redirect) is both a
   perf bug and the reason a naive Fix A would have destroyed pooling.
3. **The test's "inversion" design defeats vacuous passes.** Global
   `verificationMode=strict` + `RejectCertificateHandler` + *no* global MinIO CA
   makes the per-disk `<ca_path>` the only possible trust path. The negative
   case (no `ca_path`, **same** `minio1:9001` endpoint, run after the positive)
   simultaneously proves `ca_path` is load-bearing **and** regression-tests Fix A
   (no pool reuse of a trusting connection).
4. **Meaningfulness was verified from evidence, not assumed.** Server log:
   `data_with_ca` → 0 `certificate verify failed`; `data_no_ca` → multiple. Same
   endpoint, opposite outcomes, driven solely by `ca_path`.
5. **The local integration loop is fast.** After first-run image caching, a
   2-test run is ~10s — fast enough to iterate the config knobs (`skip_access_check`,
   `s3_retry_attempts`) interactively.
6. **Runtime cert generation removed an entire failure class** (see Finding C).

## What surfaced

### A. ninja `#deps 0` false-green → silent layout corruption (build hazard)

`ninja -t deps <obj>` reports `#deps 0` for objects in this `build/` dir: there
is **no recorded header-dependency information**, so editing a header does not
trigger recompilation of its includers. Patch 012 adds `ca_path` to
`PocoHTTPClientConfiguration`, shifting the offset of `request_throttler`. A
stale `ServerAsynchronousMetrics.cpp.o` (not rebuilt) then read the throttler at
the *old* offset; `S3::Client::getPutRequestThrottler` returned a garbage
`shared_ptr`, and the server hit a fatal exception in `AsynchronousMetrics::start`
— which presented first as a confusing integration-test "port 9000 never opened".

This is the same false-green family seen earlier with the `makeHTTPSession`
signature change (which manifested as a *linker* error — easier to spot). The
layout-shift variant is more dangerous because it links cleanly and corrupts at
runtime.

Workaround used: compute the **transitive include closure** of the changed
headers and `touch` every `.cpp` in it before building (`tmp/touch_includers.py`,
129 TUs). After that the build is correct and ninja records deps for those TUs.

**Decision:** codify in `docs/aiven/runbooks/build-and-test.md` — after editing a
layout-affecting header in this build dir, never trust an incremental build;
force-recompile the include closure (or do a clean build). Diagnosis aids:
`ninja -t deps <obj>` (look for `#deps 0`) and `llvm-nm -u` for stale symbols.

### B. Faithful ports can carry upstream bugs — "plumbing complete, invariant missing"

The 25.8 patch threaded the SSL context through *every* layer
(`EndpointConnectionPool` ctor, `prepareNewConnection`, `createConnectionPool`,
`getPool`/`getPoolImpl`, the stored member) **except** the one place that defines
pool identity: `EndpointPoolKey`. The plumbing looked complete, so the missing
invariant was easy to miss on a skim.

**Lesson / rule of thumb:** when a value changes how a pooled or cached resource
must be *validated*, it belongs in the cache **key**, not merely in the
constructor. Add this as a C++-review checklist item for any patch that touches a
keyed cache/pool.

### C. Committed test certificates rot; generate them at fixture start instead

The obvious model, `test_s3_with_https/minio_certs/`, ships a certificate that
**expired in July 2021**. It only "works" there because that test disables
verification (`verificationMode=none`). A test that relies on *real* verification
cannot reuse it. Committing a fresh long-dated cert just moves the rot further
out and leaves crypto blobs in the patch.

We instead generate the self-signed cert **fresh at fixture start**
(`minio_certs/generate_certs.sh`, invoked from `test.py`); the generated PEMs/key
are git-ignored. The cert can never expire, there are no committed keys, and the
patch diff is text-only (better for cherry-picking).

**Decision:** prefer runtime cert generation for new TLS integration tests; treat
committed crypto material with time-bound validity as a maintenance liability.

### D. Test-harness traps specific to per-disk `ca_path`

Captured for the next object-storage TLS test (013 especially):

- **Global trust must be strict.** Any global `verificationMode=none` or
  `AcceptCertificateHandler` makes a `ca_path` test pass vacuously.
- **An unreachable S3 disk hangs startup.** The disk access-check does an S3
  `PutObject` at startup; a disk that can't verify TLS retries (default ~501×,
  5s backoff) and the server never opens its port. Fix: `<skip_access_check>true`
  on the disks and `<s3_retry_attempts>1` globally so failures are fast.
- **Negative control must share the endpoint.** To exercise pool isolation, the
  no-`ca_path` disk must point at the *same* `host:port` as the with-`ca_path`
  disk and run *after* it (pool already populated). A different endpoint would
  pass even with Bug A present.

### E. First port-with-fix changes the commit model

Phase-1 convention was a single `patch-port(NNN)` commit (faithful), with any
fix as a follow-up. Here the human chose to fold port + fixes A/B + test +
dossier into **one** commit, because the fixes are what make the feature *safe*
to ship and the unit should cherry-pick atomically to other LTS lines. This is a
legitimate variant of the convention, not a violation: the commit body
explicitly separates "port" from "this commit also fixes two bugs".

### F. Security assessment of Bug A (recorded)

Bug A is a real trust-isolation weakness, but bounded: exploiting it requires
multiple S3 disks/endpoints to the *same* `host:port` with *different* trust
anchors (e.g. a custom-CA disk plus a default-trust or different-CA disk), where
the connection pool then serves one disk a connection validated for another's CA.
It is not a remote-attacker primitive; it is a silent widening of *configured*
trust. Severity: correctness/security-hardening, not a CVE-class remote exploit.
Fixed regardless, and regression-tested by the negative case.

## Concrete decisions for Phase 2+

| Decision | Driver |
|---|---|
| Codify the ninja `#deps 0` layout-corruption hazard + include-closure workaround in `build-and-test.md`. | Finding A |
| Add "values that affect validation of a keyed cache/pool must be in the key" to the C++-review checklist. | Finding B |
| Prefer runtime cert generation (fixture) over committed certs for new TLS integration tests. | Finding C |
| For 013 (Azure `ca_path`, next in chain): verify whether 25.8+ reads per-disk `ca_path` for Azure at all — Azure HTTP transport switched from curl to Poco, after which per-disk `ca_path` may be ignored and the CA must come from global `openSSL.client`. Do not assume symmetry with S3. | user note; chain `012→013` |
| Object-storage patches that widen a shared signature (`makeHTTPSession`-like) produce broad mechanical diffs; always verify the include closure rebuilt before trusting the binary. | Findings A + the port shape |
| Port-with-fix may be one commit when the fix is inseparable from making the feature correct/safe; the body must still separate port from fix. | Finding E |

## What this retrospective is NOT

- Not a re-litigation of the A/B fix design. Fix A keys by **context pointer
  identity** (safe, fail-safe; two disks with the same CA file get separate
  pools). A content-fingerprint variant (so identical CAs share a pool) is
  deferred — open question, not a blocker.
- Not the 013 port (next in the object-storage chain).
- Not finding D's deeper cleanup (`S3Settings::deserialize` try/catch — flagged in
  the dossier §3.D, out of scope here).
- Not a rule-of-three abstraction. This is the **first** integration-test-bearing
  port-with-fix; three such cases is the trigger to template the
  test-harness recipe (inversion config + runtime certs + skip_access_check +
  fast-retry). We are at 1.
- Not the inventory/worklog update — that is a separate worklog change.

## Pointers

- Dossier: `docs/aiven/patches/012-s3-custom-ca-path.md`
- Test: `tests/integration/test_aiven_s3_custom_ca_path/`
- Source patch: `8fc1c96ae0` (`v25.8.18.1-lts-aiven`)
- Fix A: `src/Common/HTTPConnectionPool.cpp` (`EndpointPoolKey` + `Hasher` + `getPoolImpl`)
- Fix B: `src/IO/S3/PocoHTTPClient.{cpp,h}` (`makeCAContext`, `ca_context`)
- Build-hazard workaround script: `tmp/touch_includers.py`
- Predecessor retrospective: `docs/aiven/uplifts/26.3/14-hook-v3-third-regression-retrospective.md`

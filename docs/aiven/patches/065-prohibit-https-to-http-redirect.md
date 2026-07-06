# Patch 065 — prohibit-https-to-http-redirect

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `6fe07b9acd` | Aliaksei Khatskevich (author/committer), co-authored by Joe Lynch, 2026-01-29 | (the version we are porting FROM) |
| 26.3-aiven | `patch-port(065)` (`1b3f4784678`) | parent agent, 2026-06-03 | `still-needed` — clean cherry-pick (offsets only); faithful unconditional carry; ships an integration test |

`byte_equivalent: true` — `git cherry-pick --no-commit` applied both hunks with
only line-number offsets; no manual conflict resolution.

## 1. Purpose

A redirect that downgrades the scheme from `https` to `http` is a secure-transport
downgrade / SSRF vector: a trusted HTTPS endpoint can bounce ClickHouse to an
arbitrary internal plain-HTTP resource, defeating the operator's expectation that
an `https://` URL stays encrypted end-to-end. The patch makes ClickHouse refuse to
follow such a redirect, throwing `UNACCEPTABLE_URL` (error code 491).

Two hunks, both keyed off the **origin** scheme being `https` and the redirect
target scheme being `http`:

- `src/IO/ReadWriteBufferFromHTTP.cpp` — `callWithRedirects` (the `url()` storage /
  table-function / `URLCluster` / HTTP-dictionary read path). The check sits between
  the existing "Too many redirects" guard and `current_uri = uri_redirect;`, so it
  fires before the downgraded URL is ever fetched.
- `src/IO/S3/PocoHTTPClient.cpp` — `makeRequestInternalImpl` on a
  `HTTP_TEMPORARY_REDIRECT`, right after `remote_host_filter.checkURL`.

```cpp
if (initial_uri.getScheme() == "https" && uri_redirect.getScheme() == "http")
    throw Exception(
        ErrorCodes::UNACCEPTABLE_URL,
        "Redirect from HTTPS to HTTP is not allowed for security reasons. "
        "Initial URL: {}, redirect URL: {}.",
        initial_uri.toString(), uri_redirect.toString());
```

`initial_uri` is the **original** request URI in both legs (in `PocoHTTPClient` the
loop reassigns only the local `uri` variable, not `request.GetUri()`), so the guard
blocks any downgrade away from the secure origin, not merely a single hop.

## 2. Upstream-drift / validity findings

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`.

- **Upstream 26.3 has no equivalent.** `rg`-search of both files shows no existing
  `https`→`http` downgrade check on the base; the only `getScheme() == "http"`
  occurrence in `PocoHTTPClient.cpp` is unrelated session setup. The patch's premise
  holds → **still needed**.
- **Error code exists.** `UNACCEPTABLE_URL` is `M(491, UNACCEPTABLE_URL)` in
  `src/Common/ErrorCodes.cpp`, so the `extern const int` declarations resolve.
- **Anchors match.** Both insertion points are present on 26.3 with only small line
  offsets (`callWithRedirects` redirect loop; the `HTTP_TEMPORARY_REDIRECT` block).
  `request.GetUri().GetURIString()` is already used a few lines above in
  `makeRequestInternalImpl`, so the S3 leg's API usage is idiomatic.

## 3. C++ / security review

- **Invariant protected:** an `https://` request never silently continues over
  plaintext `http`. Closes a secure-transport-downgrade / SSRF hole.
- **Blast radius (clause v):** the prohibition is unconditional, a default-behavior
  change — but narrow. It fires only on a genuine scheme downgrade *during a
  redirect*; it does **not** block plain-`http` URLs used directly, nor
  `http`→`http` or `https`→`https` redirects. Legitimate workloads relying on an
  HTTPS endpoint redirecting to plaintext are essentially nonexistent and inherently
  insecure. Decision (ratified): **faithful unconditional carry** (not gated behind
  a server setting), materially narrower than 018's all-URLs HTTPS enforcement.
- **Exception safety:** the throw happens before `current_uri` is advanced / before
  `uri = location`, so no partially-followed-redirect state leaks; the buffer is
  abandoned cleanly via the normal exception path.
- **No new identifiers, settings, or AST/parser changes.**

## 4. Test design

New integration test `tests/integration/test_aiven_https_to_http_redirect/`
(per the `test_aiven_<slug>` convention). Harness reuses the
`test_storage_url_http_headers` mechanic — helper HTTP servers run as localhost
subprocesses **inside** the node container (no extra Docker service):

- `redirect_servers.py` runs three servers in one process: an **HTTPS origin**
  (self-signed cert) that 302-redirects `/data` → `http://…/data`; an **HTTP
  origin** that 302-redirects to the same target (same-scheme control); and an
  **HTTP data** server returning two `JSONEachRow` rows.
- `configs/client_ssl.xml` sets `<openSSL><client><verificationMode>none` +
  `AcceptCertificateHandler`, so the outbound `url()` HTTPS handshake to the
  self-signed origin succeeds and execution actually reaches the redirect-follow
  code (`makeHTTPSession` uses `SSLManager::defaultClientContext()`).

Cases:

| Test | Assertion |
|---|---|
| `test_https_to_http_redirect_rejected` | `SELECT … FROM url('https://…/data', …) SETTINGS max_http_get_redirects=2` is rejected with `UNACCEPTABLE_URL` / "Redirect from HTTPS to HTTP". |
| `test_http_to_http_redirect_allowed` | control — a same-scheme `http`→`http` redirect still returns the two rows. Guards against over-blocking. |

**Evidence of causation (worktree-flip, §7.3 of the integration-tests runbook):**

- **Post-patch (src == index): 2 passed.** `tmp/patch-065/test-postpatch.log`.
  Observed rejection: `Code: 491. DB::Exception: … Redirect from HTTPS to HTTP is
  not allowed for security reasons. Initial URL: https://localhost:8443/data,
  redirect URL: http://localhost:8000/data`.
- **Pre-patch (worktree restored to HEAD, index untouched): 1 failed, 1 passed.**
  `tmp/patch-065/test-prepatch.log`. The rejection case fails with
  `Client expected to be failed but succeeded! stdout: 1` — i.e. unpatched
  ClickHouse **follows** the downgrade and the `SELECT` returns data (the
  vulnerability). The control still passes. The divergence is the query *succeeding*
  pre-patch vs *throwing 491* post-patch — a real causation pair, not a
  feature-absence artifact.

Scope note: only the `url()` (`ReadWriteBufferFromHTTP`) leg is exercised at
runtime. The `PocoHTTPClient` (S3) hunk is the structurally identical idiom; an
https-S3-mock issuing a `307`→http redirect was judged heavier than the "lighter
test" decision warranted, so it is covered by review rather than a second harness.

## 5. Rollback considerations

- Revert safety: removing the two hunks restores the prior follow-the-redirect
  behavior; no data/migration impact.
- User-facing footprint: any `url()`/S3 flow that previously relied on an
  `https`→`http` redirect now errors with `UNACCEPTABLE_URL`. This is the intended
  hardening; an operator who genuinely needs the old behavior would have to patch it
  out (there is deliberately no opt-out setting — see §3 clause-v decision).

## 6. Per-uplift notes

### 25.8-aiven (historical)

Source `6fe07b9acd` (author/committer Aliaksei Khatskevich, co-authored by Joe
Lynch, 2026-01-29). Two hunks adding the `https`→`http` downgrade guard to
`ReadWriteBufferFromHTTP::callWithRedirects` and
`PocoHTTPClient::makeRequestInternalImpl`. No test shipped with the source.

### 26.3-aiven (this uplift)

- Clean `git cherry-pick --no-commit` (offsets only; `byte_equivalent: true`).
- Faithful unconditional carry — not gated behind a server setting (clause-v
  decision in §3).
- New integration test authored in-parent with the worktree-flip evidence pair
  (§4); post-patch 2 passed, pre-patch 1 failed / 1 passed.

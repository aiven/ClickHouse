---
description: 'A request that starts over HTTPS is never redirected to plain HTTP, in the URL read path and the S3 client'
sidebarTitle: '065: No HTTPS-to-HTTP redirects'
slug: '/aiven/patches/065-prohibit-https-to-http-redirect'
title: 'Patch 065: No HTTPS-to-HTTP redirects'
doc_type: 'reference'
aiven_identity: '065'
aiven_absorbs: []
aiven_changelog_category: 'Improvement'
aiven_settings: []
---

# Patch 065 — No HTTPS-to-HTTP redirects {#patch-065-prohibit-https-to-http-redirect}

## Summary {#summary}

When a request that started with an `https://` URL receives a redirect to an `http://` URL,
ClickHouse throws `UNACCEPTABLE_URL` instead of following it. This applies to the URL read path
(`url`, `urlCluster`, the `URL` engine, HTTP dictionaries and everything else that reads through
`ReadWriteBufferFromHTTP`) and to redirects inside the S3 client. Same-scheme redirects and
requests that start over plain HTTP are unchanged.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.8 | `6fe07b9acd` | carried |
| 26.3 | `patch-port(065)` `5ab89878ee5` | `still-needed`, clean cherry-pick; integration test added |
| 26.8 | `patch-port(065)` | `still-needed-but-rewrite`; next to the new redirect callback; error messages mask credentials |

## Background (ClickHouse) {#background}

`ReadWriteBufferFromHTTP::callWithRedirects` follows HTTP redirects up to
`max_http_get_redirects`. Each hop goes through `callImpl`, which checks the remote host filter
for the URL it is about to request. The S3 client (`S3::PocoHTTPClient::makeRequestInternalImpl`)
follows `307 Temporary Redirect` responses itself and checks the remote host filter for the
`Location` header.

Neither path looks at the scheme of the redirect target. A server that answers an HTTPS request
can therefore send the client on to a plain HTTP URL, and the rest of the exchange is
unencrypted, although the user asked for `https://`.

## Component tour {#component-tour}

- **`ReadWriteBufferFromHTTP::callWithRedirects`** (`IO/ReadWriteBufferFromHTTP.cpp`). Compares
  the scheme of `initial_uri` with the redirect target, after the redirect count check and before
  `redirect_callback`.
- **`S3::PocoHTTPClient::makeRequestInternalImpl`** (`IO/S3/PocoHTTPClient.cpp`). The same check
  on a `307`, after the remote host filter.

## Problem {#problem}

Without the patch, an `https://` URL does not guarantee an encrypted connection to whatever
answers in the end. A redirect can move the request to plain HTTP, and to a different host, with
only the remote host filter in the way. With `018` enabled, the initial URL must be `https://`,
but a redirect would still leave HTTPS without `065`.

## Decisions {#decisions}

### Compare with the initial scheme {#decision-initial-scheme}

- **Chosen:** reject when the request started over HTTPS and the target is HTTP, at any hop.
- **Rejected:** comparing each hop with the previous one. An HTTPS → HTTPS → HTTP chain must fail
  as well; comparing with the start covers every hop in one check.

### Ungated {#decision-ungated}

- **Chosen:** always on. It only stops a downgrade that the user did not ask for, and leaves
  every same-scheme redirect alone.
- **Rejected:** a setting. A switch would let a session turn the protection off.

### Check before the redirect callback {#decision-before-callback}

- **Chosen:** the scheme check runs before `redirect_callback` (new in 26.8, used by the web
  disk to validate index pages), so no callback sees a URL that is going to be rejected anyway.

## Approach {#approach}

```cpp
if (initial_uri.getScheme() == "https" && uri_redirect.getScheme() == "http")
    throw Exception(
        ErrorCodes::UNACCEPTABLE_URL,
        "Redirect from HTTPS to HTTP is not allowed for security reasons. "
        "Initial URL: {}, redirect URL: {}.",
        maskedURI(initial_uri), maskedURI(uri_redirect));
```

The S3 client masks both URLs with `maskURICredentials`, the helper behind `maskedURI`.

## Concept {#concept}

A redirect produces a new URL that no check at query start has seen. Every property promised for
the original URL, such as an allowed host or an encrypted transport, has to be checked again for
each hop. Upstream does this for the host filter; the patch adds the scheme.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-but-rewrite`

- Upstream has no scheme check for redirects on 26.8.
- New in 26.8, `redirect_callback` in `callWithRedirects`, set only by the web disk's index pages.
  The scheme check runs before it.
- Both error messages print the URLs masked: userinfo and presigned-URL parameters are replaced,
  as in the other messages of `ReadWriteBufferFromHTTP`.

## Customer impact {#customer-impact}

- **Who:** queries and dictionaries reading an `https://` URL whose server redirects to an
  `http://` URL; S3 endpoints that answer with such a `307`.
- **What they see:** `UNACCEPTABLE_URL` instead of data. Same-scheme redirects and plain HTTP
  URLs work as before.
- **New versus 26.3:** none in behaviour.

## Security review {#security-review}

- **Transport:** a request started over HTTPS stays on HTTPS across all redirects in the two
  paths that follow redirects.
- **Messages:** the exception text contains no credentials from either URL.
- **Scope:** HTTP downgrade only; host checks stay with the remote host filter.

## Tests {#tests}

- **Path:** `tests/integration/test_aiven_https_to_http_redirect/`
- **Proves:** an HTTPS server that redirects to a plain HTTP data server is rejected with
  `UNACCEPTABLE_URL`; the same request with credentials in the URL is rejected and the server's
  message shows `[HIDDEN]` instead of the password; an HTTP → HTTP redirect still returns the
  data.
- **Not covered:** the S3 client's `307` check; it has no test.
- **Run:** `python -m ci.praktika run "integration" --test test_aiven_https_to_http_redirect`
- **Evidence:** on the parent both rejection cases fail because the redirect is followed and rows
  are returned; with the patch all three cases pass (2026-11-24).

## Rollback {#rollback}

- **Downgrade:** not supported; services only move forward.
- **Persistent state:** none.
- **Disable without rebuild:** none.

## Retirement {#retirement}

Upstream rejects HTTPS-to-HTTP redirects in both paths. Re-check on every uplift.

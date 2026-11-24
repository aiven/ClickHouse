---
description: 'Per-disk ca_path for S3: an S3 disk trusts a given CA bundle for TLS, with connection pools kept apart per trust anchor'
sidebarTitle: '012: S3 per-disk CA path'
slug: '/aiven/patches/012-s3-custom-ca-path'
title: 'Patch 012: S3 per-disk CA path'
doc_type: 'reference'
aiven_identity: '012'
aiven_absorbs: []
aiven_changelog_category: 'New Feature'
aiven_settings: []
---

# Patch 012 — S3 per-disk CA path {#patch-012-s3-custom-ca-path}

## Summary {#summary}

An S3 disk, or an `<s3>` endpoint section, accepts `<ca_path>`: a CA bundle that becomes the only
trust anchor for that disk's TLS connections. The global `openSSL.client` configuration is left
alone, so one disk can reach an object store with a private CA while every other outbound
connection keeps the system trust store. Connection pools are keyed by the TLS context as well
as by the endpoint, so a connection verified against one CA is never handed to a client that
trusts a different one.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.3 | carried | same trust path; connection pools shared across trust anchors |
| 25.8 | `8fc1c96ae0` | carried; context rebuilt on every request |
| 26.3 | `patch-port(012)` `5b6c031e717` | `still-needed-but-rewrite`; pool key includes the context, context built once per client |
| 26.8 | `patch-port(012)` | `still-needed-but-rewrite`; connection creation restructured upstream |

## Background (ClickHouse) {#background}

Outbound HTTP in ClickHouse goes through `makeHTTPSession` (`IO/HTTPCommon.cpp`), which takes a
connection from a global `HTTPConnectionPools` registry. The registry holds one
`EndpointConnectionPool` per `EndpointPoolKey`: connection group, host, port, HTTPS flag and
proxy. Idle keep-alive connections go back into that pool and are reused by the next caller with
the same key.

An HTTPS connection carries a `Poco::Net::Context`: the trusted CAs, the verification mode and the
TLS options. Without an explicit one, `HTTPSClientSession` uses the process-wide default client
context built from `openSSL.client`.

The S3 disk talks to the object store through `S3::PocoHTTPClient`, configured by
`PocoHTTPClientConfiguration`, which `S3::ClientFactory::createClientConfiguration` builds from
`S3Settings`.

## Component tour {#component-tour}

- **`S3Settings`** (`IO/S3Settings.{h,cpp}`). New `ca_path` field, read from the disk section and
  from `<s3>` endpoint sections.
- **`createClientConfiguration` and `PocoHTTPClientConfiguration`** (`IO/S3/Client.*`,
  `IO/S3/PocoHTTPClient.h`). A new `ca_path` argument after `enable_s3_requests_logging`; every
  caller passes it, empty except the S3 disk.
- **`PocoHTTPClient`** (`IO/S3/PocoHTTPClient.cpp`). `makeCAContext` builds `ca_context` once in
  the constructor; every request and redirect passes it to `makeHTTPSession`.
- **`makeHTTPSession`** (`IO/HTTPCommon.*`). A trailing `context` parameter, defaulting to the
  default client context.
- **`HTTPConnectionPool`** (`Common/HTTPConnectionPool.*`). The context is part of
  `EndpointPoolKey` and is kept by `EndpointConnectionPool`; `createConnection` passes it to every
  new HTTPS connection.

## Problem {#problem}

Without the patch, an S3 disk whose endpoint has a certificate from a private CA can only be
reached by trusting that CA for the whole process (`openSSL.client.caConfig`) or by turning off
verification. With a per-disk context but without the pool-key change, two disks on the same
endpoint share one pool, so the first context to open a connection decides which CA every later
user of that connection trusted.

## Decisions {#decisions}

### Keep the patch on 26.8 {#decision-keep}

- **Chosen:** carry `012`. A control-plane integration test configures an S3 disk with a private
  CA through `ca_path` alone, without the CA in the global client configuration.
- **Rejected:** dropping it and moving that test to `openSSL.client.caConfig` (rung 1). This works
  and removes the hot-path change, but the decision was to keep what control-plane tests use.
- **Not done:** an upstream contribution (rung 2). Upstream has no per-endpoint CA option; a
  contribution would need the same pool-key change.

### Pool key by context pointer {#decision-pool-key}

- **Chosen:** `EndpointPoolKey` stores the context pointer. The pool keeps a reference to the
  context, so the address cannot be freed and reused while the key exists. The context is built
  once per client, so one disk keeps one pool.
- **Rejected:** keying by the CA file path or contents. Two contexts with equal files but different
  options would still collide, and the key would need file I/O.

### One creation helper {#decision-create-connection}

- **Chosen:** `EndpointConnectionPool::createConnection` creates every pooled connection: through
  a proxy, direct with per-address retry, and when a connection is stored back into the pool.
- **Rejected:** passing the context at each of the three call sites. A fourth site added upstream
  would silently fall back to the default context.

### Trust only the bundle {#decision-only-bundle}

- **Chosen:** `loadDefaultCAs = false`, `VERIFY_RELAXED`, depth 9. A disk with `ca_path` trusts
  the bundle and nothing else, so it cannot also reach an endpoint with a public certificate.
- **Rejected:** adding the system store. It widens the trust of exactly the disk that asked to be
  pinned.

## Approach {#approach}

```cpp
ConnectionPtr createConnection()
{
    if constexpr (std::is_same_v<Session, Poco::Net::HTTPSClientSession>)
    {
        if (context)
            return PooledConnection::create(this->getWeakFromThis(), group, getMetrics(), host, port, context);
    }
    return PooledConnection::create(this->getWeakFromThis(), group, getMetrics(), host, port);
}
```

An empty context means the default client context: the pool key holds `nullptr` and the session
is created exactly as upstream creates it. `makeHTTPSession` passes the context through without
looking up the default, which would take `SSLManager`'s global lock on every request and could
fail a plain HTTP request on a broken TLS client configuration. Callers other than the S3 client
keep the five-argument form.

## Concept {#concept}

A connection pool is a cache, and its key has to include everything that makes two connections
different. For a TLS connection that is not just the address: it is also what the client agreed
to trust when it verified the peer. Adding per-client trust to a shared pool without adding it to
the key turns a configuration option into a reuse bug.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-but-rewrite`

- `EndpointConnectionPool::prepareNewConnection` now creates connections in two places, through a
  proxy and directly with a retry over resolved addresses; the store-back path is the third.
  All three use `createConnection`.
- Seven new callers of `createClientConfiguration` (data lake catalogs, dictionaries, Kafka IAM
  authentication, unit tests) pass an empty `ca_path`.
- Call sites that only passed the default context explicitly are not changed; the parameter has a
  default.
- `S3Settings::serialize` and `deserialize` have no callers on 26.8. They are unchanged, so the
  binary form does not gain a field.
- New in 26.8, `CertificateReloader` reloads `caConfig` without a restart. It replaces the
  certificate store inside the default client context, so the pointer, and with it the key of
  default-context pools, does not change. A per-disk context is not reloaded; a new bundle needs a
  restart.

## Customer impact {#customer-impact}

- **Who:** services whose S3 disk configuration sets `ca_path`. The managed configuration does
  not set it for any production disk.
- **What they see:** nothing. Connections of other disks and clients are unchanged; the default
  context keeps one pool per endpoint as before.
- **New versus 26.3:** a disk's context is not refreshed by the CA reload; the global one is.

## Security review {#security-review}

- **Trust scope:** a disk with `ca_path` trusts only that bundle. Other disks and clients keep the
  global configuration.
- **Isolation:** pools are keyed by context, so a connection is never reused under a different
  trust anchor. The integration test checks this on one endpoint.
- **Input:** `ca_path` comes from server configuration only. A missing or unreadable file fails
  when the client is created, never by falling back to the default context.

## Tests {#tests}

- **Path:** `tests/integration/test_aiven_s3_custom_ca_path/`
- **Proves:** MinIO serves HTTPS with a self-signed certificate created at test start, and the
  global client verifies strictly without that CA. A disk with `ca_path` writes and reads. The
  negative case first leaves an idle verified connection from that disk in the pool, then a disk
  without `ca_path` on the same host and port must fail certificate verification. So the setting
  is what makes the connection work, and the second disk does not get the first disk's pooled
  connection, whatever order the cases run in.
- **Run:** `python -m ci.praktika run "integration" --test test_aiven_s3_custom_ca_path`
- **Evidence:** on the parent the disk with `ca_path` fails with `certificate verify failed`. With
  a pool key that ignores the context, the negative case run alone fails: the write succeeds over
  the borrowed connection. With the patch both cases pass, together and alone, and `url()` over
  plain HTTP is unaffected (2026-11-24).

## Rollback {#rollback}

- **Downgrade:** not supported; services only move forward.
- **Persistent state:** none.
- **Disable without rebuild:** remove `ca_path` from the disk; the disk then uses the global
  client configuration.

## Retirement {#retirement}

Upstream gains a per-endpoint CA option with pools keyed by trust anchor, or no configuration and
no control-plane test sets `ca_path`. Re-check on every uplift.

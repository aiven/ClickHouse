---
description: 'Server setting enforce_https_for_url_storage: the URL engine, url/urlCluster and HTTP dictionaries accept only https:// endpoints'
sidebarTitle: '018: Enforce HTTPS for URL storage'
slug: '/aiven/patches/018-enforce-https-url-storage'
title: 'Patch 018: Enforce HTTPS for URL storage'
doc_type: 'reference'
aiven_identity: '018'
aiven_absorbs: []
aiven_changelog_category: 'Improvement'
aiven_settings: ['enforce_https_for_url_storage']
---

# Patch 018 — Enforce HTTPS for URL storage {#patch-018-enforce-https-url-storage}

## Summary {#summary}

The server setting `enforce_https_for_url_storage` (`Bool`, default `false`) makes the `URL` table
engine, the `url` and `urlCluster` table functions and HTTP dictionary sources reject every endpoint
that is not `https://`. Every URL the engine requests is checked before the request is sent. With
the setting off, behaviour is the same as upstream.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.8 | `ae35b0cc72` | carried, unconditional check |
| 26.3 | `patch-port(018)` `8bbc7156916` | `still-needed-but-rewrite`; gated by a server setting, `urlCluster` added |
| 26.8 | `patch-port(018)` | `still-needed-but-rewrite`; checked at each request, index-page wildcards covered |

## Background (ClickHouse) {#background}

`StorageURL` and `StorageURLCluster` validate their URL in the constructor with the remote host
filter. Reads go through `StorageURLSource::getFirstAvailableURIAndReadBuffer`, which builds one
`Poco::URI` for each URL it requests. Writes go through `StorageURLSink::initBuffers`. On 26.8,
`url` and the `URL` engine read paths with listable wildcards (`*`, `**`) through a web object
storage (`StorageWebConfiguration`) instead of `StorageURL`. HTTP dictionaries build their URL
once, in the source registration.

Upstream has no switch that forbids plain HTTP for these engines.

## Component tour {#component-tour}

- **`checkHTTPSForURLStorage`** (`Storages/StorageURL.{h,cpp}`). Throws `BAD_ARGUMENTS`
  `URL storage supports only HTTPS protocol, rejected URL: …` when the setting is on and the scheme
  is not `https`. The URL is masked with `maskURICredentials`.
- **Callers:** the `URL` engine factory, only for `CREATE` (early error);
  `getFirstAvailableURIAndReadBuffer` (each read request);
  `IStorageURLBase::tryGetLastModificationTime`; `StorageURLSink::initBuffers` (each write); and
  `StorageWebConfiguration::check` (each base URL of the index-page wildcard path).
- **`registerDictionarySourceHTTP`** (`Dictionaries/HTTPDictionarySource.cpp`). Throws
  `UNSUPPORTED_METHOD` `Only https scheme is supported for HTTPDictionarySource`.

## Problem {#problem}

A managed service has to guarantee that no tenant reads or writes data through these engines over
an unencrypted channel. Without the patch, any `http://` URL is accepted.

## Decisions {#decisions}

### Server setting, not a query setting {#decision-server-setting}

- **Chosen:** a `ServerSetting`. It has no `SET` path, so a session, a profile, a `SETTINGS` clause
  or a dictionary `<settings>` block cannot turn it off.
- **Rejected:** a query setting with a constraint. One missing constraint would reopen plain HTTP.

### Check where the request is made {#decision-per-request}

- **Chosen:** check the `Poco::URI` that is about to be requested, in the read and write paths.
- **Rejected:** a constructor-only check. The URL string a storage receives is not always the URL
  it requests, and 26.8 adds a read path that does not go through the `StorageURL` constructor.

### Loading existing metadata does not throw {#decision-loading}

- **Chosen:** the early check runs in the `URL` engine factory only for `CREATE`
  (`args.mode <= LoadingStrictnessLevel::CREATE`), the same rule as the wildcard experimental check
  next to it. Tables stored as `AS url(...)` or `AS urlCluster(...)` and loaded from metadata get no
  early check. An existing `http://` table, created while the setting was off, loads after the
  setting is turned on; every read or write of it is rejected before a request is sent.
- **Rejected:** checking in the `StorageURL` and `StorageURLCluster` constructors (26.3). They also
  run when metadata is loaded, and the table then fails to load and cannot be dropped.
- **Exception:** the index-page wildcard path keeps its check in `StorageWebConfiguration::check`,
  also when metadata is loaded, because it has no check per request. Such a table cannot be created
  while the setting is on.

### Name the rejected URL {#decision-message}

- **Chosen:** the message names the rejected URL with credentials masked, so a user with several
  URLs or a glob can see which one was rejected. HTTP dictionaries have a single URL and keep their
  message.

### The check propagates {#decision-propagates}

- **Chosen:** in `getFirstAvailableURIAndReadBuffer` the check runs outside the `try` that moves on
  to the next URL after a network error, so a rejected URL fails the query.
- **Rejected:** treating a rejected URL like an unreachable one. The query would succeed or fail
  depending on which URLs respond.

### Keep the dictionary error unchanged {#decision-dictionary-error}

- **Chosen:** HTTP dictionaries keep `UNSUPPORTED_METHOD` and their 26.3 message.
- **Rejected:** the 26.3 change of `HTTPDictionarySource::Configuration::url` from `std::string` to
  `Poco::URI`. The check does not need it, and it touches every load method.

### Delegated schemes are out of scope {#decision-delegated-schemes}

- **Chosen:** URLs that the `URL` engine and `url` hand to another engine by scheme (`file://`,
  `s3://`, `az://`, `hdfs://`, new in 26.8) follow that engine's rules, the same as calling it
  directly. The setting governs the requests the URL engine itself sends.

## Approach {#approach}

```cpp
void checkHTTPSForURLStorage(const ContextPtr & context, const Poco::URI & uri)
{
    if (!context->getServerSettings()[ServerSetting::enforce_https_for_url_storage] || uri.getScheme() == "https")
        return;

    String masked = uri.toString();
    maskURICredentials(masked);
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "URL storage supports only HTTPS protocol, rejected URL: {}", masked);
}
```

Redirects from `https://` to `http://` are rejected by `065`.

## Concept {#concept}

A check on input is only as good as its distance from the action. A URL string passes through glob
expansion, `url_base` resolution, partition substitution and new read paths before a request is
sent. Checking the exact `Poco::URI` handed to the HTTP client keeps the guarantee independent of
how the URL got there.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-but-rewrite`

- Upstream still has no HTTPS enforcement for URL storage.
- New in 26.8: index-page wildcards in `url` and the `URL` engine read through
  `StorageWebConfiguration`, which never reaches the `StorageURL` constructor. Its `check` now
  validates each base URL; links from index pages are limited to the same origin by upstream.
- New in 26.8: scheme dispatch to File, S3, AzureBlobStorage and HDFS, see
  [Delegated schemes are out of scope](#decision-delegated-schemes).
- 26.8 refuses to start when the configuration contains an unknown element
  (`UNKNOWN_ELEMENT_IN_CONFIG`). The managed configuration sets this setting, so it must stay
  declared for as long as the configuration sets it.

## Customer impact {#customer-impact}

- **Who:** services where the managed configuration enables the setting.
- **What they see:** `BAD_ARGUMENTS` `URL storage supports only HTTPS protocol, rejected URL: …`
  for `http://` URLs in the `URL` engine, `url` and `urlCluster`; `UNSUPPORTED_METHOD` for HTTP
  dictionaries with an `http://` URL. `https://` URLs work as before.
- **New versus 26.3:** index-page wildcards (new in 26.8) are covered as well. The message names
  the rejected URL. An existing `http://` table loads and is rejected when it is used, instead of
  failing to load. For `url` and `urlCluster` the error comes with the first request rather than
  when the storage is built; no request is sent in either case.

## Security review {#security-review}

- **Transport:** with the setting on, every request sent by these engines uses HTTPS; `065` keeps
  redirects on HTTPS.
- **Override:** no session path; only the server configuration changes it.
- **Scope:** HTTP requests of the URL engines and HTTP dictionaries. Other engines, including
  those reached by scheme dispatch, are governed by their own settings.

## Tests {#tests}

- **Path:** `tests/integration/test_aiven_enforce_https_url/`
- **Proves:** with the setting on, `http://` is rejected by `url`, the `URL` engine, `urlCluster`,
  the index-page wildcard path and HTTP dictionaries, and by every URL the engine requests; the
  message names the URL without its password; an `https://` URL is not rejected by the check;
  `SET enforce_https_for_url_storage` fails with `UNKNOWN_SETTING`. A `URL` engine table and an
  `AS url(...)` table with `http://` URLs, created with the setting off, load after the setting is
  turned on and a restart, are rejected on `SELECT`, and can be dropped. With the setting off,
  `http://` reads return data.
- **Run:** `python -m ci.praktika run "integration" --test test_aiven_enforce_https_url`
- **Evidence (2026-11-24):** with the patch, 11 passed. With the per-request and web
  configuration checks removed, the two cases that depend on them fail: the query returns data
  over HTTP, or the server fetches the index page over HTTP. With the checks in the constructors
  and the message without the URL, the masking case and the restart case fail: the `AS url(...)`
  table does not load (`ASYNC_LOAD_WAIT_FAILED`). The parent binary does not start with the test
  configuration (`UNKNOWN_ELEMENT_IN_CONFIG`).

## Rollback {#rollback}

- **Downgrade:** not supported; services only move forward.
- **Persistent state:** none. Tables created with `https://` URLs are unaffected by turning the
  setting off.
- **Disable without rebuild:** set `enforce_https_for_url_storage` to `false` in the server
  configuration.

## Retirement {#retirement}

Upstream offers an equivalent server-level switch for these engines. The managed configuration then
moves to it, and this setting is removed only once no configuration sets it.

---
description: 'Dropped: per-disk ca_path for Azure Blob Storage disks; no configuration depends on it and the global client CA covers the need'
sidebarTitle: '013: Azure per-disk CA path (dropped)'
slug: '/aiven/patches/013-azure-custom-ca-path'
title: 'Patch 013: Azure per-disk CA path (dropped)'
doc_type: 'reference'
aiven_identity: '013'
aiven_absorbs: []
aiven_changelog_category: 'New Feature'
aiven_settings: []
---

# Patch 013 — Azure per-disk CA path (dropped) {#patch-013-azure-custom-ca-path}

## Summary {#summary}

The patch let an Azure Blob Storage disk trust its own CA bundle through `<ca_path>`, the Azure
counterpart of patch `012`. No production disk sets it, and the control-plane tests that use a
private CA for Azure also put that CA into `openSSL.client.caConfig`, which already makes the
connection work. It is not carried on 26.8.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.8 | `2f70d49490` | carried; set the CA on the Curl transport, which 25.8 no longer used for Azure, so the setting had no effect |
| 26.3 | `patch-port(013)` `57c1a9a3c2c` | rewritten on the Poco transport, mirroring `012` |
| 26.8 | `patch-drop(013)` | dropped, `never-effective` |

## Background (ClickHouse) {#background}

Azure disks send their requests through `PocoAzureHTTPClient`, which uses the same
`makeHTTPSession` and connection pools as the S3 client. Without a per-disk context, HTTPS
connections use the default client context from `openSSL.client`, whose `caConfig` adds CAs to
the system trust store.

## Problem {#problem}

The patch addressed an Azure endpoint with a certificate from a private CA. Azure endpoints in
production have publicly trusted certificates. The only private-CA Azure endpoints are emulators
in tests, and those tests trust the emulator CA globally.

## Decisions {#decisions}

### Drop {#decision-drop}

- **Chosen:** drop. Nothing that runs depends on the setting: no production disk configures it,
  and every test that configures it also trusts the same CA globally. On 25.8 the setting was not
  read at all, without anybody noticing.
- **Rejected:** port it next to `012`. It would add a second per-disk context to the Azure client
  and its share of the pool changes, for no configuration that needs it.
- **Alternative for a future need (rung 1):** `openSSL.client.caConfig`. On 26.8 it is reloaded
  without a restart.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `never-effective`

- The Azure transport is unchanged since 26.3; the rewrite would still apply. The drop is about
  demand, not drift.
- Patch `012` keeps `makeHTTPSession`'s context parameter and the pool key by context, so a later
  Azure port would only need the client side.
- Patches `015` and `016` reference `ca_path` only in context lines.

## Customer impact {#customer-impact}

- **Who:** an Azure disk with `<ca_path>` in its configuration; none in production.
- **What they see:** the key is ignored. TLS uses the global client configuration, so an endpoint
  with an untrusted certificate fails verification; it is never reached unverified.

## Security review {#security-review}

n/a — removes an option; the remaining trust path is the global client configuration.

## Tests {#tests}

The 26.3 test `test_aiven_azure_custom_ca_path` is not ported. It only checked that the value is
read.

## Rollback {#rollback}

- **Downgrade:** not supported; services only move forward.
- **Persistent state:** none.

## Retirement {#retirement}

Dropped. Re-add only for a production Azure endpoint that needs a CA no other connection may
trust.

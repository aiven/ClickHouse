---
description: 'Server setting that makes Azure object storage created from SQL assume its container exists, so CREATE makes no request to Azure'
sidebarTitle: '028: Skip Azure container creation'
slug: '/aiven/patches/028-skip-create-azure-container'
title: 'Patch 028: Skip Azure container creation at CREATE'
doc_type: 'reference'
aiven_identity: '028'
aiven_absorbs: []
aiven_changelog_category: 'Improvement'
aiven_settings:
  - 'aiven_skip_azure_container_creation'
---

# Patch 028 — Skip Azure container creation at CREATE {#patch-028-skip-create-azure-container}

## Summary {#summary}

With the server setting `aiven_skip_azure_container_creation`, Azure object storage created
from SQL — an `AzureBlobStorage` table, the `azureBlobStorage` table functions, a data lake
table on Azure — assumes its container already exists: it neither checks for the container nor
creates it. Aiven provisions containers outside ClickHouse, so the check is unnecessary: it
makes `CREATE TABLE` wait for Azure, against an endpoint that is not reachable yet
for every retry, and it creates containers nobody provisioned. With the setting on, such a
statement makes no request to Azure, and a missing container is reported by the first write;
with it off, nothing changes.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.8 | `8ed6167986` | carried, unconditional one-line change |
| 26.3 | dropped, then `patch-port(028)` `c2e4c37adca` | first dropped as covered by upstream, which holds only for restart and `ATTACH`; re-ported behind the new default-off `aiven_skip_azure_container_creation` |
| 26.8 | `patch-port(028)` | `still-needed-and-applies`, code unchanged; the test now counts requests |

## Background (ClickHouse) {#background}

An Azure client in ClickHouse is built by `AzureBlobStorage::getContainerClient`. Unless the
connection parameters say `container_already_exists`, or the caller asks for a read-only
client, it first asks Azure whether the container exists (`GetProperties`) and creates it if
not (`CreateBlobContainer`). Azure disks set `container_already_exists` from their config;
object storage created from SQL has no such option.

Upstream already passes "read-only" when a table is loaded at server start, attached,
replayed from a `Replicated` database's DDL log, or recreated by `RESTORE` (which runs its
`CREATE` as a secondary query), so those paths make no request. A plain `CREATE TABLE` is not
read-only, so it does — including when a backup tool restores a service by replaying each
table's `CREATE` statement. Since 26.8 a transport error or a server error during the check is
logged ("Assuming the container exists") instead of failing the statement, but only after the
Azure client's retries have run out.

## Component tour {#component-tour}

- **`StorageAzureConfiguration::createObjectStorage`** (`Storages/ObjectStorage/Azure/Configuration.cpp`).
  The single place where every SQL-created Azure object storage gets its client: the
  `AzureBlobStorage` engine (`registerStorageObjectStorage.cpp`), the `azureBlobStorage` and
  `azureBlobStorageCluster` table functions, data lake tables and catalogs on Azure, and
  `ObjectStorageQueue` on Azure. The patch sets `container_already_exists` here, before
  `getContainerClient` runs.
- **`aiven_skip_azure_container_creation`** (`Core/ServerSettings.cpp`). The gate, read through
  `Context::getServerSettings`.

## Problem {#problem}

Without the patch, every `CREATE TABLE ... ENGINE = AzureBlobStorage(...)` sends a request to
Azure before the statement can finish. When the endpoint is not reachable yet — while the
backup tool replays a service's table definitions, for example — each statement waits for the
client's retries; a restore with many such tables waits once per table. On 26.3 the
statement then failed; on 26.8 it continues. When the endpoint is reachable but the container
is missing, ClickHouse silently creates a container that Aiven did not provision.

## Decisions {#decisions}

### Gated behind a server setting {#decision-gated}

- **Chosen:** a default-off server setting, enabled by the managed configuration.
- **Rejected:** the 25.8 unconditional line. It removes upstream's auto-creation for every
  user of the binary and changes the behaviour of upstream tests that rely on it.
- **Rejected:** a session or table setting. Whether containers are provisioned outside
  ClickHouse is a property of the deployment, not of a query.
- **Rung:** 4, a runtime guard. No upstream option covers SQL-created Azure storage
  (rung 1), and the behaviour is a deployment choice that upstream has no reason to change
  for everyone (rung 2).

### Only SQL-created object storage {#decision-scope}

- **Chosen:** gate only `StorageAzureConfiguration::createObjectStorage`.
- **Rejected:** also changing Azure disks or `BACKUP` to Azure. Disks already have
  `container_already_exists` in their config, and backups already set it.

## Approach {#approach}

When the setting is on, `createObjectStorage` sets `container_already_exists = true` in the
configuration's connection parameters before building the client, so `getContainerClient` builds
it without any request. The write is idempotent and only ever sets `true`; the one later
rebuild of the client, on a settings change, is read-only anyway. When the setting is off, the
function is unchanged.

## Concept {#concept}

A "read-only" shortcut keyed on loading mode protects only the paths that upstream marks as
secondary: restart, `ATTACH`, DDL replay and `RESTORE`. A restore that replays plain `CREATE`
statements is not one of them, so the shortcut can never cover it; that is how the patch was
wrongly dropped once.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-and-applies`

- `createObjectStorage` and `getContainerClient` are unchanged since 26.3.
- Upstream 26.8 sets `container_already_exists` itself for OneLake and for the one-argument
  catalog form of the engine, not for a regular `AzureBlobStorage` definition.
- New upstream behaviour: the container check treats a transport error as "the container
  exists" instead of failing the statement. Against an endpoint that refuses connections, a
  `CREATE` on the parent commit succeeded after 4.3 s of retries. The patch still matters for
  the wait and for containers created without provisioning; the stall that made the statement
  fail is gone.

## Customer impact {#customer-impact}

- **Who:** services whose configuration enables the setting, which is every managed service.
- **What they see:** `CREATE TABLE ... ENGINE = AzureBlobStorage(...)` succeeds without
  contacting Azure, even when the endpoint is not reachable, and does not create a missing
  container. The first `INSERT` into a missing container fails; on 26.8 the client sees only
  `Code: 1001 ... std::exception` (`STD_EXCEPTION`), because the Azure error text is lost on
  the way. An `INSERT INTO TABLE FUNCTION azureBlobStorage(...)` into a missing container
  fails the same way instead of creating it.
- **How to restore upstream behaviour:** disable the server setting and restart.
- **New versus 26.3:** none.
- **Upstream tests edited:** none.

## Security review {#security-review}

n/a — the patch removes a request ClickHouse would otherwise make and never widens what a user
can reach: the endpoint, credentials and remote-host checks are unchanged, and a missing
container is still reported on first use. With the setting on, ClickHouse can no longer
create containers from SQL, which narrows what a user can do.

## Tests {#tests}

- **Paths:** `tests/integration/test_aiven_skip_azure_container_creation/`
- **Proves:** with the setting off, `CREATE TABLE` makes container requests and creates a
  missing container. With it on, `CREATE TABLE` makes no request, counted by the query's
  `AzureGetProperties` and `AzureCreateContainer` profile events, both against Azurite and
  against an endpoint nothing listens on; a missing container is not created by `CREATE
  TABLE` or by an `INSERT` through the table function; the first `INSERT` fails.
- **Run:** `python -m ci.praktika run "integration" --test test_aiven_skip_azure_container_creation`
- **Evidence:** the node with the setting enables `skip_check_for_incorrect_settings`, so the
  parent binary starts and ignores the unknown setting. On the parent the three tests with the
  setting on fail (2 requests for a missing container, 1 for the unreachable endpoint, the
  table function creates the container) and the one with it off passes; with the patch all
  four pass, three runs in a row (2026-11-24).

## Rollback {#rollback}

- **Downgrade:** not supported; services only move forward.
- **Persistent state:** none; the patch only skips a request.
- **Disable without rebuild:** set `aiven_skip_azure_container_creation` to false and restart.

## Retirement {#retirement}

An upstream option to treat containers as pre-provisioned for SQL-created Azure storage, like
the disks' `container_already_exists`. Re-check upstream on every uplift. The release that
drops the patch keeps `aiven_skip_azure_container_creation` declared with
`SettingsTierType::OBSOLETE`; an unknown element stops the server from starting.

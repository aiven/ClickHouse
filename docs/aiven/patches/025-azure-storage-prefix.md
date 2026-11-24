---
description: 'Disk option storage_prefix that places every blob key of an Azure disk configured with storage_account_url or connection_string under a path prefix'
sidebarTitle: '025: Azure storage prefix'
slug: '/aiven/patches/025-azure-storage-prefix'
title: 'Patch 025: Azure disk storage_prefix'
doc_type: 'reference'
aiven_identity: '025'
aiven_absorbs: []
aiven_changelog_category: 'Improvement'
aiven_settings: []
---

# Patch 025 — Azure disk `storage_prefix` {#patch-025-azure-storage-prefix}

## Summary {#summary}

An Azure disk configured with `storage_account_url` or `connection_string` gets a
`storage_prefix` option. Every blob key the disk writes, reads, lists or removes is placed under
that prefix, so several services can share one container under disjoint prefixes. Upstream can
only express a prefix through the path of the `endpoint` URL. Without `storage_prefix` in the
config, nothing changes.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.8 | `fdb7a142d1` | carried |
| 26.3 | `patch-port(025)` `56f738e58fe` | carried; the file moved to `src/Disks/DiskObjectStorage/ObjectStorages/`, same three lines |
| 26.8 | `patch-port(025)` | `still-needed-and-applies`, code unchanged; the test pins the key layout for a `/` prefix |

## Background (ClickHouse) {#background}

`AzureBlobStorage::processEndpoint` turns a disk config into an `Endpoint`: the storage URL, the
container and a key prefix. It accepts three forms. With `endpoint`, everything after the
container in the URL path is the prefix, and the upstream option `endpoint_subpath` appends to
it. With `storage_account_url` or `connection_string`, the container comes from `container_name`
and the prefix is always empty.

`ContainerClientWrapper` then prepends the prefix to every blob name, adding a trailing `/` if
it is missing, and strips it again from listed names. A listed name without the prefix is a
`LOGICAL_ERROR`.

## Component tour {#component-tour}

- **`AzureBlobStorage::processEndpoint`** (`Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.cpp`).
  After the three config forms are parsed and before the container name is validated, the
  patch reads `<storage_prefix>` into the prefix when the key is present.
- **Callers:** the Azure disk (`AzureObjectStorage`, `ObjectStorageFactory`), including disks
  defined in a query with `SETTINGS disk = disk(type = azure_blob_storage, ...)`, and the
  per-endpoint request settings. The `AzureBlobStorage` table engine and the
  `azureBlobStorage` table functions do not go through it.

## Problem {#problem}

The managed configuration writes Azure disks in the `storage_account_url` form and needs each
service under its own prefix in a shared container. Without the patch the disk ignores
`storage_prefix` and writes at the container root. Nothing reports it: the data round-trips,
but it lands outside the service's prefix, where it is neither isolated from other services nor
found by tools that look under the prefix.

## Decisions {#decisions}

### Carry the config option {#decision-carry}

- **Chosen:** keep the three-line config read, unchanged.
- **Rejected for this uplift:** switching the managed configuration to the `endpoint` form,
  which upstream already supports (rung 1). It would re-render every Azure service's disk during
  the uplift, and it is unverified that the blob keys, the account name used for signing and the
  `/` prefix come out the same. See [Retirement](#retirement).
- **Not done:** an upstream contribution (rung 2). The option is generic and carries no Aiven
  policy, so it is a candidate; no pull request is filed.

### `storage_prefix` with a prefixed `endpoint` {#decision-override}

- **Chosen:** verbatim. With the `endpoint` form, `storage_prefix` replaces the prefix from the
  URL and `endpoint_subpath`, as on 26.3.
- **Rejected:** failing at startup when both are present. The managed configuration never uses
  the `endpoint` form, so the case only arises in hand-written configs, and 26.3 accepts it.

## Approach {#approach}

```cpp
if (config.has(config_prefix + ".storage_prefix"))
    prefix = config.getString(config_prefix + ".storage_prefix");
```

The value is used as given. `ContainerClientWrapper` adds a missing trailing `/`, and nothing
removes a leading one: `storage_prefix = '/'` gives keys that start with `/`.

## Concept {#concept}

A key prefix is part of the stored data, not of the configuration. Once a disk has written under
a prefix, the way that prefix is turned into keys must stay the same on every later version,
including odd values such as `/`; otherwise the next version looks for the data under keys
where it does not exist.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-and-applies`

- `processEndpoint` is unchanged since 26.3 and the anchor before `validateContainerName` is
  intact; the commit cherry-picks cleanly.
- Upstream 26.8 has no prefix option for the `storage_account_url` and `connection_string`
  forms. `endpoint_subpath` exists since 2025 but is read only for the `endpoint` form.

## Customer impact {#customer-impact}

- **Who:** every managed service with an Azure disk.
- **What they see:** data under the service's prefix in the shared container, as on 26.3. A
  service without a prefix gets `storage_prefix` `/` from the managed configuration, and its keys
  start with `/`, as on 26.3.
- **New versus 26.3:** none.
- **Upstream tests edited:** none.

## Security review {#security-review}

n/a — the option only chooses where in the configured container the disk keeps its keys. The
credentials, the container and the account do not change, and a query-defined disk could
already choose any prefix through the `endpoint` form. Prefixes isolate services sharing a
container only as far as each service's credentials are scoped; the option does not restrict
access by itself.

## Tests {#tests}

- **Paths:** `tests/integration/test_aiven_azure_storage_prefix/`
- **Proves:** parts written through an Azure disk with `storage_prefix` land under the prefix;
  without the option they stay at the container root; with `storage_prefix = '/'` every key
  starts with a single `/`. Keys are read with the Azure SDK directly, which does not apply the
  prefix, and the data round-trips in every case.
- **Run:** `python -m ci.praktika run "integration" --test test_aiven_azure_storage_prefix`
- **Evidence:** on the parent the two cases with a prefix fail (keys at the container root) and
  the control passes; with the patch all three pass (2026-11-24).

## Rollback {#rollback}

- **Downgrade:** not supported; services only move forward.
- **Persistent state:** the blob keys. A binary without the patch reads a prefixed disk at the
  container root and does not find its data.
- **Disable without rebuild:** none; the option is read from the disk config.

## Retirement {#retirement}

Either upstream accepts a prefix option for the `storage_account_url` and `connection_string`
forms, or the managed configuration moves to the `endpoint` form after showing that it produces
the same keys, including for the `/` prefix, and the same signing. Re-check on every uplift. The
option is a disk config key, not a setting, so no `OBSOLETE` declaration is needed; a removed key
is ignored and the disk silently writes at the container root, so the managed configuration must
move first.

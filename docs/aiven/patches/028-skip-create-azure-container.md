# Patch 028 — skip-create-azure-container

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `8ed6167986` | Tilman Moeller (author), co-authored by Joe Lynch, 2025-12-18 | (the version we are porting FROM) |
| 26.3-aiven | — (no commit) | parent agent, 2026-06-03 | **`obsoleted-by-upstream`** — dropped; see §2 |

## 1. Purpose (of the original 25.8 patch)

A 1-line change in `StorageAzureConfiguration::createObjectStorage`
(`src/Storages/ObjectStorage/Azure/Configuration.cpp`):

```cpp
connection_params.endpoint.container_already_exists = true;
```

The 25.8 commit message frames this as a **performance optimization**: Aiven's
Azure containers are provisioned externally and always exist, so skipping the
container existence-check and creation attempt eliminates "1-2 Azure API calls per
storage initialization" and avoids handling the "container already exists" exception.

The maintainer's recollection of the *intent* was **startup resilience**: if Azure
is unreachable on a server restart, ClickHouse must still start cleanly rather than
failing while initializing an Azure-backed table. (As §2 shows, that exact intent is
the subject of a separate upstream commit, not this Aiven patch.)

## 2. Upstream-drift / validity findings — `obsoleted-by-upstream`

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`.

Both the stated motivation (restart resilience) **and** the perf motivation for the
restart/attach path are already provided by upstream commit
`7e7cbbdd224` ("Do not make requests to Azure on server restart", Alex Katsman /
ClickHouse, 2025-01-21), which is present in the 26.3 tree.

### 2.1 The upstream mechanism

`createObjectStorage` now receives an `is_readonly` flag derived from the table
loading mode (`src/Storages/ObjectStorage/registerStorageObjectStorage.cpp:82-84`):

```cpp
// We only want to perform write actions (e.g. create a container in Azure) when the table is being created,
// and we want to avoid it when we load the table after a server restart.
configuration->createObjectStorage(context, /* is_readonly */ args.mode != LoadingStrictnessLevel::CREATE, std::nullopt),
```

On a restart, tables attach with `mode != LoadingStrictnessLevel::CREATE`, so
`is_readonly = true`. That flag short-circuits `getContainerClient`
(`src/Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.cpp:302-304`):

```cpp
if (params.endpoint.container_already_exists.value_or(false) || readonly)
{
    return params.createForContainer();   // pure client builder, NO network
}
```

`createForContainer` is a non-networking client constructor; the network calls
(`containerExists`, `CreateBlobContainer`) live only in the branches below it. So on
restart **no Azure request is made regardless of patch 028** — startup does not
depend on Azure reachability. Upstream even ships a dedicated integration test:
`tests/integration/test_restart_with_unavailable_azure/`.

### 2.2 What 028 would actually change on 26.3

Patch 028 sets `container_already_exists = true` *unconditionally* in
`createObjectStorage`. Because the restart/attach path (`readonly = true`) already
takes the short-circuit, the **only** residual effect is on the `readonly = false`
path — i.e. `CREATE TABLE` / writable Azure tables:

- **Without 028 (current 26.3):** at CREATE, `getContainerClient` checks
  `containerExists` and auto-creates the container if missing
  (`AzureBlobStorageCommon.cpp:307-332`).
- **With 028:** at CREATE, it skips the check/creation and assumes the container
  exists — so a `CREATE` against a non-existent container no longer auto-provisions
  it and instead fails at first write.

That residual is a behavior change **unrelated to the restart/startup motivation**,
and for general users it is a mild regression (loss of auto-create-on-create). It is
only beneficial in Aiven's deployment model where containers are pre-provisioned.

### 2.3 Conclusion

The restart-resilience goal is fully covered by upstream `7e7cbbdd224` (with its own
test). Carrying 028 buys nothing for that goal and introduces an unrelated CREATE-time
behavior change. Disposition: **`obsoleted-by-upstream`**, dropped (no code, no
commit). Mirrors the 023/027 pattern.

## 3. C++ / security review

No code carried. The decision rests on the upstream `is_readonly` separation of
"attach on restart" from "create": it gates *all* container network side-effects on
the restart path (not just creation), which is strictly more principled than 028's
unconditional `container_already_exists = true`. No security implication — if
anything, retaining upstream's auto-create-on-CREATE is the safer default for general
users.

## 4. Test design

None. There is no code change. The obsolescence is established by code analysis of
the `is_readonly` flag (`registerStorageObjectStorage.cpp`) and the `|| readonly`
short-circuit in `getContainerClient` (`AzureBlobStorageCommon.cpp`), plus the
existing upstream integration test `test_restart_with_unavailable_azure` which
already proves a restart with an unreachable Azure endpoint succeeds.

## 5. Rollback considerations

N/A — nothing applied.

## 6. Per-uplift notes

### 25.8-aiven (historical)

Source `8ed6167986` (author Tilman Moeller, co-authored by Joe Lynch, 2025-12-18):
the 1-line `connection_params.endpoint.container_already_exists = true;` in
`StorageAzureConfiguration::createObjectStorage`, framed as a perf optimization
(skip 1-2 Azure API calls; matches the `BackupIO_AzureBlobStorage` pattern).

### 26.3-aiven (this uplift)

- `obsoleted-by-upstream`: dropped, no commit. The 26.3 `is_readonly` mechanism
  (`7e7cbbdd224`) already skips all Azure requests on restart/attach; 028's residual
  effect would only suppress container auto-creation on CREATE — an unrelated
  behavior change.
- Decision ratified by the maintainer (drop the code, document only).

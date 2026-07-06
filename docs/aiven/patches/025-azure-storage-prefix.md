# Patch 025 — azure-storage-prefix

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `fdb7a142d1` | Tilman Moeller (author), co-authored by Kevin Michel, 2025-12-16 | (the version we are porting FROM) |
| 26.3-aiven | `patch-port(025)` (see inventory for SHA) | parent agent, 2026-06-03 | **`still-needed`** — clean 3-line additive port at the relocated path |

## 1. Purpose

Azure storage does not support a prefix before the stored object key, which makes
shared containers (one container per Azure account, partitioned by project / backup
site) unusable. This adds a `storage_prefix` disk-config option usable alongside
`storage_account_url` or `connection_string`. Previously a prefix could only be
expressed by embedding it in the `endpoint` URL path; `storage_prefix` makes it
explicit and readable.

The change is 3 lines in `processEndpoint`:

```cpp
if (config.has(config_prefix + ".storage_prefix"))
    prefix = config.getString(config_prefix + ".storage_prefix");
```

placed after the `storage_account_url` / `connection_string` / `endpoint` branch and
before `validateContainerName`. The resulting `Endpoint::prefix` flows into
`ContainerClientWrapper::blob_prefix`, which is prepended to every blob key
(`blob_prefix + blob_name`, with a trailing `/` appended if missing).

## 2. Upstream-drift / validity findings

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`.

- **Still needed.** 26.3 has no `storage_prefix` support — `processEndpoint` accepts
  only `endpoint` / `connection_string` / `storage_account_url`, and there is no
  config read for `storage_prefix` (verified by grep).
- **Path move.** The file moved on 26.3 from `src/Disks/ObjectStorages/...` to
  `src/Disks/DiskObjectStorage/ObjectStorages/...`. The insertion context (the
  `else throw "Expected either ..."` followed by `validateContainerName`) is
  otherwise unchanged, so the hunk is carried as a direct edit rather than a
  cherry-pick.
- **Clause (v) screen: not applicable.** The option is opt-in: the new code only
  runs when `config.has(config_prefix + ".storage_prefix")` is true. Existing disk
  configurations (no `storage_prefix`) keep their exact prior behavior — confirmed
  by the control test, which shows keys remain at the container root without the
  option.

## 3. C++ / security review

- **Reach.** `processEndpoint` is invoked from the config-driven Azure disk path
  (`ObjectStorageFactory::registerAzureObjectStorage`, `AzureObjectStorage`), not
  from the `azureBlobStorage(...)` table function. So `storage_prefix` is an
  operator/DDL-level disk option, not untrusted query input.
- **Isolation property.** Distinct prefixes give disjoint key namespaces within one
  container — the intended multi-tenant/backup-site isolation. The wrapper's
  `ListBlobs` strips `blob_prefix` and asserts every returned key actually carried it
  (`LOGICAL_ERROR` otherwise), so a misconfiguration cannot silently leak keys from
  outside the prefix into a disk's view.
- **No new attack surface.** Purely additive config read of an operator-supplied
  string; no parsing of untrusted input, no relaxation of an existing check.

## 4. Test design

Integration test `tests/integration/test_aiven_azure_storage_prefix/` (Azurite),
because the prefix's effect on blob keys is only observable against a real Azure
backend.

- `test_storage_prefix_applied_to_blob_keys`: writes parts through a dynamic Azure
  disk with `storage_prefix = 'aiven_project_a'`, then uses the raw Azure SDK
  `BlobServiceClient` (which does NOT apply ClickHouse's prefix wrapper, so it sees
  full keys) to assert every blob key lives under `aiven_project_a/`.
- `test_no_storage_prefix_leaves_keys_at_root`: control — same write without
  `storage_prefix`; asserts no blob is placed under the prefix (keys at root).

### Evidence (worktree-flip pair, 2026-06-03)

- **post-patch:** `2 passed in 25.79s` (prefix applied; control at root).
- **pre-patch** (worktree reverted to HEAD, index kept the staged change, binary
  rebuilt and re-run; the binary is volume-mounted into the test node so no image
  rebuild was needed): `1 failed, 1 passed` —
  `test_storage_prefix_applied_to_blob_keys` failed with
  `AssertionError: blobs not under prefix 'aiven_project_a/': [...]` (keys at the
  container root, proving the option is a no-op without the patch); the control
  still passed. Worktree restored and rebuilt to the post-patch binary afterward.

## 5. Rollback considerations

Self-contained 3-line revert; no submodule or schema coupling. Removing the option
reverts to "prefix only via endpoint URL path".

## 6. Per-uplift notes

### 25.8-aiven (historical)

Source `fdb7a142d1` (author Tilman Moeller, co-authored by Kevin Michel, 2025-12-16):
the same 3-line addition, at the pre-move path.

### 26.3-aiven (this uplift)

- `still-needed`; ported as a direct edit at the relocated path. Built clean.
- Verified with an Azurite integration test + worktree-flip evidence pair (§4).
- Local-run note: the praktika integration runner executes pytest inside a
  docker-in-docker runner whose image store is a persistent named volume
  (`clickhouse_integration_tests_volume`), separate from the host. The first-time
  pull of `clickhouse/integration-test` inside dind takes ~4.5 min (overlay
  extraction), exceeding the framework's 180s pull cap, so the dind volume must be
  pre-warmed once before the test can run locally. This is an environment quirk, not
  a patch issue, and required no change to upstream test-harness code.

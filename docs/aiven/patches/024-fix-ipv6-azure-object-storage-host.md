# Patch 024 — fix-ipv6-azure-object-storage-host

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `1abdc712c0` | Tilman Moeller (author), co-authored by Kevin Michel, 2025-12-16 | (the version we are porting FROM) |
| 26.3-aiven | `patch-port(024)` (see inventory for SHA) | parent agent, 2026-06-03 | **`still-needed` (source leg only)** — submodule leg folded into `patch-port(016)`; see §2 |

## 1. Purpose

Fixes IPv6 Azure object-storage hosts. Two halves:

1. **Azure SDK leg** — the Azure SDK does not fully support IPv6 in hostnames
   because the escaping brackets (`[...]`) are not parsed/removed at the right time.
   This lives in the `aiven/azure-sdk-for-cpp` fork.
2. **ClickHouse leg** — `validateStorageAccountUrl` (in
   `src/Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.cpp`)
   rejected any host that didn't look like a DNS name. The patch broadens the
   validation regex to also accept a bracketed IPv6 literal and an optional port.

```cpp
// before
R"(http(()|s)://[a-z0-9-.:]+(()|/)[a-z0-9]*(()|/))"
// after
R"(http(()|s)://(\[[a-fA-F0-9:]+\]|[a-z0-9-.]+)(:\d+)?(()|/)[a-z0-9]*(()|/))"
```

The new alternation `(\[[a-fA-F0-9:]+\]|[a-z0-9-.]+)` accepts either a bracketed
IPv6 literal or the previous DNS-style host, and `(:\d+)?` allows an explicit port.

## 2. Upstream-drift / validity findings

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`.

The source commit `1abdc712c0` touches two paths; they have **different**
dispositions on 26.3:

- **`contrib/azure` gitlink bump → ALREADY-APPLIED (folded into `patch-port(016)`).**
  The 26.3 `aiven/azure-sdk-for-cpp` branch stacks the Aiven changes linearly, with
  the IPv6 fix as the branch HEAD `98519bd3` ("Improve IPv6 support").
  `patch-port(016)` (`d70d3ee2fb1`) already pinned the `contrib/azure` gitlink to
  `98519bd3` (`0f7a2013f7d7` → `98519bd324221c1b2e3c7576317a6a09fa1825ea`). Same
  mechanism as patch 023 / `contrib/aws`: a submodule gitlink is atomic to one
  commit, so pinning to the HEAD brought in the IPv6 fix. Nothing to re-bump.

- **`AzureBlobStorageCommon.cpp` regex → STILL NEEDED (this is what we port).**
  The pre-patch regex is live on 26.3 (verified at line 101 of the file, which moved
  from `src/Disks/ObjectStorages/...` to `src/Disks/DiskObjectStorage/ObjectStorages/...`
  on 26.3). Upstream 26.3 has no equivalent IPv6 broadening. A literal cherry-pick
  does not apply because of the path move; the change is carried as a 1-line direct
  edit at the new path.

## 3. C++ / security review

- **Reach.** `validateStorageAccountUrl` is invoked only from `processEndpoint`
  (`AzureBlobStorageCommon.cpp`), which is called from the config-driven Azure
  object-storage **disk** creation path (`ObjectStorageFactory::registerAzureObjectStorage`
  and `AzureObjectStorage`), not from the `azureBlobStorage(...)` table function.
  The validation runs before any network connection.
- **Invariant preserved.** The regex still rejects non-URL garbage (anchored
  `FullMatch`, so the whole string must match `http(s)://host[...]`). The only
  broadening is the host alternative and the optional port; it does not weaken
  scheme or structural checks. Negative control in the test confirms a non-URL
  string is still rejected.
- **No new attack surface.** This is input-acceptance broadening on an
  operator-supplied disk URL, not on untrusted query input. It enables a previously
  unusable deployment shape (IPv6 Azure endpoints) rather than relaxing a security
  boundary.

## 4. Test design

`tests/queries/0_stateless/9024_ipv6_azure_storage_account_url.sh` (`no-fasttest`,
requires `USE_AZURE_BLOB_STORAGE`).

- Exercises the regex through a dynamic Azure disk (`disk(type = azure_blob_storage,
  storage_account_url = '...')`), which routes through `processEndpoint` →
  `validateStorageAccountUrl`. Validation runs before the connection, so a bogus
  `[::1]:1` endpoint is enough to reach it.
- Asserts presence/absence of the `Blob Storage URL is not valid` message (robust to
  the phrase appearing on multiple error lines):
  - bracketed IPv6 host → `ACCEPTED` (post-patch) / `REJECTED` (pre-patch).
  - non-URL string → `REJECTED` (both, negative control).

### Evidence (worktree-flip pair, 2026-06-03)

- **post-patch:** `[ OK ]` — `ACCEPTED` / `REJECTED`.
- **pre-patch** (worktree reverted to HEAD regex, index kept staged change, rebuilt
  + server restarted): `[ FAIL ]` — `REJECTED` / `REJECTED` (the bug: bracketed IPv6
  rejected). Worktree then restored and rebuilt back to the post-patch binary.

## 5. Rollback considerations

The source leg is a self-contained 1-line regex revert. The submodule leg cannot be
rolled back independently of 016 (it shares the pinned `contrib/azure` commit).

## 6. Per-uplift notes

### 25.8-aiven (historical)

Source `1abdc712c0` (author Tilman Moeller, co-authored by Kevin Michel, 2025-12-16):
`contrib/azure` gitlink bump + the `validateStorageAccountUrl` regex change, in one
commit.

### 26.3-aiven (this uplift)

- Submodule leg: `already-applied` via `patch-port(016)` (`d70d3ee2fb1`,
  azure HEAD `98519bd3`).
- Source leg: ported as a 1-line direct edit at the relocated path. Built clean;
  validated with a stateless test and a worktree-flip evidence pair (§4).
- See the submodule-forks runbook registry row for `contrib/azure`.

# Patch 028 — skip-create-azure-container

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `8ed6167986` | Tilman Moeller (author), co-authored by Joe Lynch, 2025-12-18 | (the version we are porting FROM) — unconditional 1-line change |
| 26.3-aiven | — (initially dropped 2026-06-03) | parent agent | first adjudicated **`obsoleted-by-upstream`** — dropped; **later reclassified** (see §2) |
| 26.3-aiven | (staged) | T3 worker (rewrite) | **`still-needed-but-rewrite` (gated)** — re-instated behind default-off server setting `aiven_skip_azure_container_creation` |

The current uplift's row stays "(staged)" until the human commits.

## 1. Purpose

Aiven pre-provisions its Azure blob containers out of band. The original 25.8 patch
made `StorageAzureConfiguration::createObjectStorage` assume the container already
exists, so that initializing an `AzureBlobStorage` table engine does **not** probe Azure
(`GetProperties`) or attempt to create the container (`CreateBlobContainer`). The 25.8
commit framed this as a perf optimization (skip 1–2 Azure API calls per storage
initialization and avoid handling the "container already exists" exception); the durable
operational motivation is that the create-time probe is unnecessary in Aiven's model and,
against an unreachable or plaintext endpoint, can **stall `CREATE TABLE`** until timeout.

Source SHA on `v25.8.18.1-lts-aiven`: `8ed616798670b71829c85b25f96096e2b41df179`
(carried on `v25.8.24.21-lts-aiven`).
Original author: Tilman Moeller <tilman.moeller@aiven.io>, 2025-12-18 (committer Joe Lynch).
Original change (one unconditional line in `createObjectStorage`):

```cpp
connection_params.endpoint.container_already_exists = true;
```

## 2. Upstream-drift / validity findings — `still-needed-but-rewrite` (gated)

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`.

### 2.0 History: why it was first dropped, and why that was wrong for the CREATE path

On 2026-06-03 patch 028 was adjudicated `obsoleted-by-upstream` on the theory that
upstream commit `7e7cbbdd224` ("Do not make requests to Azure on server restart",
present in 26.3) already covers it. That commit gives `createObjectStorage` an
`is_readonly` flag derived from the loading mode
(`src/Storages/ObjectStorage/registerStorageObjectStorage.cpp`):

```cpp
// We only want to perform write actions (e.g. create a container in Azure) when the table is being created,
// and we want to avoid it when we load the table after a server restart.
configuration->createObjectStorage(context, /* is_readonly */ args.mode != LoadingStrictnessLevel::CREATE, std::nullopt),
```

and `getContainerClient` short-circuits on it
(`src/Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.cpp`):

```cpp
if (params.endpoint.container_already_exists.value_or(false) || readonly)
    return params.createForContainer();   // pure client builder, NO network
```

That correctly covers the **restart / attach** leg: on restart, tables attach with
`mode != LoadingStrictnessLevel::CREATE`, so `is_readonly = true` and no Azure request is
made — proven by upstream's own `tests/integration/test_restart_with_unavailable_azure/`.

**But the drop was wrong for the CREATE leg.** On `CREATE TABLE ... ENGINE =
AzureBlobStorage(...)` the mode **is** `CREATE`, so `is_readonly = false`, and with a
connection-string/account-key endpoint `container_already_exists` is unset (`nullopt`).
`getContainerClient` therefore falls through the `|| readonly` short-circuit and
**actively probes** (`containerExists` → `GetProperties`) and, if the container is
missing, **creates** it (`CreateBlobContainer`). Against the Astacus fixture's Azurite —
an unreachable / plaintext endpoint at fixture-setup time — this network round-trip
**hangs the `CREATE`** until timeout, breaking fixture setup. The `is_readonly` mechanism
does nothing here because CREATE is, by definition, not readonly. So 028's behavior is
**still needed for the CREATE path**.

### 2.1 Mechanism on HEAD (re-verified)

Identifiers present on `v26.3.10.62-lts` HEAD (`tmp/patch-028/ident-grep.log`):
`createObjectStorage`, `container_already_exists`, `getContainerClient`,
`getServerSettings`, `ServerSettingsBool` all > 0. The insertion site is intact:
`StorageAzureConfiguration::createObjectStorage(ContextPtr context, bool is_readonly, …)`
body is `assertInitialized();` → `getRequestSettings(...)` →
`getContainerClient(connection_params, is_readonly)`. Setting
`connection_params.endpoint.container_already_exists = true` before the
`getContainerClient` call makes it take the no-network `createForContainer()` branch,
short-circuiting **both** the probe and the create.

### 2.2 Why a gated rewrite, not a verbatim re-apply (clause (v))

Re-applying the original line *unconditionally* is a **default-behavior change with broad
blast radius**: it silently removes container auto-creation-on-CREATE for *every*
`AzureBlobStorage` table (engine and `azureBlobStorage` table function), which a broad
class of upstream tests/users rely on. Per `docs/aiven/AGENTS.md §7` / dispatch-template
clause (v), such a change must NOT ship on by default. The parent ratified gating it
behind a new default-off **server setting** `aiven_skip_azure_container_creation` (a fleet
deployment invariant — "Aiven pre-provisions Azure containers" — conceptually identical
to the sibling external-storage posture knob `enforce_https_for_url_storage`, patch 018;
it must not be a per-session knob). With the gate **off**, the staged binary is
behavior-identical to upstream 26.3.

### 2.3 Conclusion

`still-needed-but-rewrite` (gated). The CREATE-path behavior is re-instated, gated behind
default-off `aiven_skip_azure_container_creation`. `byte_equivalent` is `false` by design
(the source was one unconditional line; the staged change is the same effect gated on a
new server setting, plus the setting declaration).

## 3. C++ review

Apply `docs/aiven/skills/cpp-review-checklist.md`. One bullet per section.

- 1 Lifetime + ownership: ✓ — no new ownership; reads a `bool` server setting via
  `context->getServerSettings()` and mutates a local `connection_params` field already
  owned by `*this`.
- 2 Exception safety: ✓ — the gated line is a plain field assignment that cannot throw;
  it runs after `assertInitialized();` and before any client construction, so it does not
  change rollback semantics.
- 3 Thread-safety + concurrency: ✓ — `ServerSettings` are read through `Context`'s
  already-thread-safe accessor; no new lock, no shared mutable state introduced.
- 4 Performance + memory: ✓ — control path (table-engine init), not a per-row hot path;
  when on it *removes* 1–2 Azure round-trips, when off it adds one boolean settings read.
- 5 Settings as public API: ✓ — new `DECLARE(Bool, aiven_skip_azure_container_creation,
  false, …)` in `src/Core/ServerSettings.cpp`; default `false` preserves upstream
  behavior; read with the matching `ServerSettingsBool` type.
- 6 Error handling: n/a — the patch removes/keeps an existing branch; it introduces no new
  error code or message.
- 7 Upstream / vendored code: ✓ — only `src/Core/ServerSettings.cpp` and
  `src/Storages/ObjectStorage/Azure/Configuration.cpp` touched; no `contrib/**`,
  `.claude/**`, `.github/workflows/**`, or root `AGENTS.md`.
- 8 Behavior under settings: ✓ — with the setting off (default) `createObjectStorage` does
  exactly what upstream does (no extra allocation, no extra Azure call, no log spam); the
  integration test's `node_default` proves auto-create still fires when off.

## 4. Test design

(a) **New integration test that fails on the parent (pre-patch) binary and passes after the patch.**

- Test path: `tests/integration/test_aiven_skip_azure_container_creation/`
  (`test.py` + `configs/skip_azure_container_creation.xml`).
- Shape: two ClickHouse instances on the **same post-patch binary**, differing only by a
  server-config overlay — `node_default` (no overlay, gate off) and `node_skip`
  (overlay sets `aiven_skip_azure_container_creation = true`). A shared Azurite is the
  object store; a raw `BlobServiceClient` is the out-of-band ground-truth observer.
- Differential observable: `CREATE TABLE ... ENGINE = AzureBlobStorage(<conn>, '<container>',
  'data.parquet', 'Parquet')` against a container that does **not** pre-exist.
  - `node_default` (gate off): upstream auto-creates the container → it now **exists**.
  - `node_skip` (gate on): the no-network branch is taken → the container **does NOT exist**.
- Pre-patch run output (the FAIL — only the `node_skip` assertion, against the
  worktree-flipped pre-patch binary; the overlay also carries
  `skip_check_for_incorrect_settings` so the pre-patch server starts and merely *ignores*
  the unknown setting, degrading to upstream auto-create):

  ```text
  test_aiven_skip_azure_container_creation/test.py:127: in test_gate_on_skips_container_creation
      assert not _container_exists(
  E   AssertionError: gate ON: CREATE TABLE must NOT create container 'cont-missing-skip'
  E   assert not True
  FAILED test_aiven_skip_azure_container_creation/test.py::test_gate_on_skips_container_creation
  1 failed, 1 deselected, 7 warnings in 25.98s
  ```

- Post-patch run output (the PASS — both assertions):

  ```text
  test_aiven_skip_azure_container_creation/test.py::test_gate_off_autocreates_container PASSED [ 50%]
  test_aiven_skip_azure_container_creation/test.py::test_gate_on_skips_container_creation PASSED [100%]
  2 passed, 9 warnings in 24.29s
  ```

- Why this test distinguishes the Aiven gate from upstream behavior (per AGENTS.md §7): the
  two instances run the same binary and differ *only* by the new server setting, and the
  observable (was the missing container auto-created?) is read out-of-band by the raw Azure
  client — so the divergence is attributable to nothing but `aiven_skip_azure_container_creation`.

## 5. Rollback considerations

- Reverting is safe: no schema migration, no on-disk format change. The setting defaults to
  `false`, so a binary that drops the patch behaves identically to one with the setting off.
- No persistent state: the patch only affects whether a create-time Azure probe/create call
  is issued; it does not create ZK nodes, files, or caches that survive a restart.
- To disable the behavior without rebuilding: remove (or set `false`)
  `aiven_skip_azure_container_creation` in the server config and restart. Because it is a
  server-level setting it cannot be toggled per session.

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — patch first carried on 25.8-aiven.

### 25.8-aiven (historical)

Source `8ed6167986` (author Tilman Moeller, co-authored by Joe Lynch, 2025-12-18): the
1-line unconditional `connection_params.endpoint.container_already_exists = true;` in
`StorageAzureConfiguration::createObjectStorage`, framed as a perf optimization (skip 1–2
Azure API calls; matches the `BackupIO_AzureBlobStorage` pattern).

### 26.3-aiven (this uplift)

- First adjudicated `obsoleted-by-upstream` (2026-06-03, dossier-only drop), then
  **reclassified `still-needed-but-rewrite` (gated)** after recognizing the original drop
  ignored the non-readonly CREATE path (which upstream `7e7cbbdd224`'s `is_readonly`
  mechanism does not cover — CREATE is by definition not readonly).
- Cherry-pick was: **rewritten** (hand-authored, NOT `git cherry-pick`). The unconditional
  line is replaced by a default-off `ServerSetting` gate:
  - `src/Core/ServerSettings.cpp`: `DECLARE(Bool, aiven_skip_azure_container_creation,
    false, …)` adjacent to the other `aiven_*` Bools.
  - `src/Storages/ObjectStorage/Azure/Configuration.cpp`: `#include <Core/ServerSettings.h>`,
    a `namespace ServerSetting { extern const ServerSettingsBool
    aiven_skip_azure_container_creation; }` block (mirroring `StorageURL.cpp`'s extern of
    `enforce_https_for_url_storage`), and in `createObjectStorage`, after
    `assertInitialized();`, a guarded `if (... aiven_skip_azure_container_creation ...)
    connection_params.endpoint.container_already_exists = true;`.
- Scope: the table-engine path only (`StorageAzureConfiguration::createObjectStorage`).
  Disk-, queue-, and backup-side Azure paths are deliberately untouched (the disk path
  already has a config-level `container_already_exists`; backup already sets it).
- Upstream-drift conclusion: `still-needed-but-rewrite` (gated) — see §2.
- Test added at: `tests/integration/test_aiven_skip_azure_container_creation/`.
- Motivation that resurfaced the patch: the Astacus fixture's Azurite (unreachable /
  plaintext at fixture-setup time) made the 26.3 CREATE-time probe/create hang, breaking
  fixture setup — exactly what 028 was meant to prevent.
- `byte_equivalent: false` (expected — a rewrite gated on a new setting, not a textual
  cherry-pick).
- Build/test were warm-cache: post-patch build ~107s; pre-patch (worktree-flip) rebuild
  ~50s; flip-back rebuild ~111s; post-patch test 24.3s (2 passed), pre-patch test 26.0s
  (1 failed as designed).
- Anything surprising: the original `obsoleted-by-upstream` adjudication was a genuine
  miss on the CREATE leg — a reminder that an `is_readonly`-style short-circuit only covers
  the readonly path, and CREATE is not readonly.

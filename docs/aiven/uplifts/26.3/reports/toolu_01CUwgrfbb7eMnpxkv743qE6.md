<!-- subagentStop archive | source=transcript | status=completed | type=general-purpose | ts=2026-06-02T09:01:26Z -->

---
outcome: escalate
patch_slug: azure-signature-delegation
source_sha: 38e54d35892bb869d9aa43805c75ae26da43e445
proposed_commit:
  staged_files: []
  commit_message: |
    patch-port(016): Allow delegating Azure signature to a separate process

    Original author: Tilman Moeller <tilman.moeller@aiven.io>, 2025-12-13.
    Co-authored-by: Kevin Michel <kevin.michel@aiven.io>

    (cherry picked from commit 38e54d35892bb869d9aa43805c75ae26da43e445)

    NOT YET STAGED — port halted at the cherry-pick conflict (Step 2) on an
    uncovered upstream drift; see escalation below. Once ratified, the body
    will additionally note: submodule bump to 98519bd…; 013 reconciliation;
    delegated_signature = false; and the dropped AzureObjectStorageConnectionInfo
    hunk (feature removed upstream).
  byte_equivalent: false
tests:
  added: no_source_change
  kind: n/a
  paths: []
  upstream_reference: []
  pre_patch_fail_verified: false
  post_patch_pass_verified: false
  justification: |
    No source change was staged in this dispatch. The port halted at Step 2
    (cherry-pick conflict resolution) on an uncovered drift, before any C++
    resolution, build, or test authoring. The working tree was returned to the
    clean preflight baseline (only the documented carry-overs remain), so
    staged_files is empty and nothing under src/** or tests/** was modified.
escalation_reason: policy_call
---

## Tier results

- Tier 1 (textual cherry-pick): **fail** — `git cherry-pick --no-commit -x` produced one conflict the parent's reconciliation policy does not cover: a `modify/delete` on `src/Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorageConnectionInfo.cpp` (the entire `ObjectStorageConnectionInfo` feature family was removed upstream between 25.8 and 26.3). All other conflicts matched the parent's policy exactly.
- Tier 2 (semantic patch-id): **n/a** — not reached (Tier 1 blocked).
- Tier 3 (build + test): **n/a** — not reached. No build, no test.

## Evidence

Preflight (Step 0): on branch `v26.3.10.62-lts-aiven-dev`; source commit type `commit`; scratch dir + `tmp/patch-016/dispatch-start.txt` recorded.

**Preflight status note (one deviation from the enumerated carry-overs):** `git status --porcelain` showed the documented carry-overs (`M contrib/{NuRaft,ai-sdk-cpp,arrow,brotli,cppkafka,crc32c,msgpack-c,snappy}`, `M docs/aiven/AGENTS.md`, `M docs/aiven/skills/dispatch-prompt-template.md`) **plus** `M docs/aiven/uplifts/26.3/log.md`, which the prompt did not enumerate. Its diff is a single-line append to the per-uplift work-log table (AGENTS §8 names this exact file as the work log) — manifestly the same "uncommitted governance edit, not yours to touch" family. I left it untouched and did not treat it as a blocking surprise.

Drift analysis (Step 1): net-new symbols (`AzureDelegatedKeyPolicy`, `isDelegatedSignature`, `delegated_signature`) absent as expected; the existing symbols present; the moved files live under `src/Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/`; `RequestSettings` ends at `ca_path` (013) and `Endpoint::account_name` exists separately — all as the parent predicted. **But 1 of the 4 wiring sites does not exist:** `AzureObjectStorageConnectionInfo.cpp`.

Cherry-pick conflict set (`tmp/patch-016/cherrypick.log`), annotated:

```
Auto-merging .gitmodules                                          ← covered (submodule repoint)
CONFLICT (content): ...AzureBlobStorageCommon.cpp                 ← covered (013 reconciliation)
CONFLICT (content): ...AzureBlobStorageCommon.h                   ← covered (013 reconciliation)
CONFLICT (file location): ...AzureDelegatedKeyPolicy.cpp -> DiskObjectStorage/...   ← covered (path move)
CONFLICT (file location): ...AzureDelegatedKeyPolicy.h  -> DiskObjectStorage/...    ← covered (path move)
Auto-merging ...AzureObjectStorage.cpp                            ← covered (clean wiring)
Auto-merging ...ObjectStorageFactory.cpp                          ← covered (clean wiring)
CONFLICT (modify/delete): src/Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorageConnectionInfo.cpp
    deleted in HEAD and modified in 38e54d35892   ← *** NOT COVERED BY PARENT POLICY ***
Auto-merging src/Storages/ObjectStorage/Azure/Configuration.cpp   ← covered (clean wiring)
```

The uncovered hunk (`tmp/patch-016/uncovered-hunk.txt`) — the patch's sole change to that file:

```c++
 std::shared_ptr<AzureBlobStorage::ContainerClient> AzureObjectStorageConnectionInfo::makeClient(const AzureClientInfo & info)
 {
     auto params = AzureBlobStorage::ConnectionParams{};
+    params.delegated_signature = false;
     AzureBlobStorage::processURL(info.endpoint, info.container_name, params.endpoint, params.auth_method);
```

Drift recount (`tmp/patch-016/drift-conclusion.txt`) — the feature is entirely gone upstream, not merely relocated:

```
[1] *ConnectionInfo* on 25.8 source (38e54d35892): 8 files
    .../AzureBlobStorage/AzureObjectStorageConnectionInfo.{cpp,h}
    .../IObjectStorageConnectionInfo.{cpp,h}
    .../S3/S3ObjectStorageConnectionInfo.{cpp,h}
    .../Web/WebObjectStorageConnectionInfo.{cpp,h}
[2] *ConnectionInfo* on upstream v26.3.10.62-lts: (none)
[3] *ConnectionInfo* on current HEAD:             (none)
[4] Remaining refs on HEAD: StorageObjectStorageSource.cpp:800: ObjectStorageConnectionInfoPtr connection_info;
```

Cleanup: `git cherry-pick --abort` reported "no cherry-pick or revert in progress" (a `--no-commit` pick leaves no sequencer state), so I restored the affected paths with a path-scoped `git reset` (not `--hard`) + `git restore --worktree --source=HEAD` + `rm` of the two new files and the modify/delete leftover. Final `git status --porcelain` shows **only** the known carry-overs; `git submodule status contrib/azure` → ` 0f7a2013f7d…` (clean, no `+`/`-`). No forbidden git op was used (no `commit`, `push`, `rebase`, `reset --hard`, `stash`, `checkout <branch>`, or branch creation).

## What I did

- Read the full dispatch prompt, `docs/aiven/AGENTS.md`, the halt-and-escalate schema, the build/integration/testing runbooks, the cpp-review checklist, the dossier template, the source diff, and both reference dossiers (015 S3 twin, 013 Azure CA).
- Step 0 preflight: branch ✓, source commit ✓, scratch dir + start timestamp ✓. Flagged (but did not act on) the unenumerated `docs/aiven/uplifts/26.3/log.md` carry-over.
- Step 1 drift analysis: confirmed net-new symbols absent, existing symbols present, `RequestSettings`/`Endpoint`/`getRequestSettings` shapes match the parent's 013-reconciliation assumptions — **except** that `AzureObjectStorageConnectionInfo.{cpp,h}` does not exist on 26.3.
- Step 2 cherry-pick (`--no-commit -x`): enumerated the complete conflict set; every conflict maps to a sanctioned policy call except the `modify/delete` on `AzureObjectStorageConnectionInfo.cpp`.
- Established via cross-ref recount that the whole `ObjectStorageConnectionInfo` family was removed upstream (8 files on 25.8 → 0 on `v26.3.10.62-lts`).
- Aborted cleanly back to the preflight baseline. Did **not** build, test, write the dossier, or stage anything.

## Proposed next step

**Need a policy decision (ratify a 4th reconciliation deviation), then the port is mechanical.**

The blocking question: the patch's `params.delegated_signature = false;` line targets `AzureObjectStorageConnectionInfo::makeClient`, but the entire `ObjectStorageConnectionInfo` feature was removed from upstream 26.3. How should this hunk be reconciled?

My recommended resolution (provably a semantic no-op, but it is a hunk drop the parent did not sanction, so I will not make it unilaterally):

1. Resolve the conflict by **accepting HEAD's deletion** — i.e. drop the hunk: `git rm src/Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorageConnectionInfo.cpp` (the file is already absent on 26.3; this just clears the modify/delete conflict). **No replacement wiring is needed elsewhere.**
2. Rationale: parent policy call #3 already mandates the in-class initializer `bool delegated_signature = false;` on `ConnectionParams`, expressly "so a future aggregate-init site cannot read garbage." `makeClient`'s `ConnectionParams{}` is exactly such a site. With the in-class default, the per-site `= false` write is redundant, so dropping it leaves `delegated_signature` safe at *every* construction site (the 3 surviving wiring sites — `AzureObjectStorage.cpp`, `ObjectStorageFactory.cpp`, `Configuration.cpp` — use designated-init `.delegated_signature = isDelegatedSignature(...)`; any other `ConnectionParams{}` is `false` by the in-class default).
3. To proceed, please **amend the reconciliation policy** to add a 4th call ("drop the `AzureObjectStorageConnectionInfo.cpp` hunk — feature removed upstream; redundant given in-class `= false`") and **extend Step 3's allowed-delta list** to include this dropped hunk (otherwise the decomposition gate would independently STOP `semantic_conflict` on it).

With that ratification, a resumed dispatch can complete deterministically: redo the cherry-pick; resolve the two `AzureBlobStorageCommon.{h,cpp}` content conflicts and the `AzureDelegatedKeyPolicy.{h,cpp}` path-move per the existing policy calls; `git rm` the deleted file; repoint `contrib/azure` to `98519bd324221c1b2e3c7576317a6a09fa1825ea` (+ `.gitmodules` url/branch); build; author the two-case integration test; write the dossier (documenting both the `delegated_signature = false` and the dropped-hunk deviations); stage.

One question worth a glance during ratification: confirm the S3 twin (patch 015) did **not** silently encounter the analogous `S3ObjectStorageConnectionInfo.cpp` removal — the 015 dossier's call-site list does not mention it, which is consistent with the S3 source patch simply not touching that file, but it's worth a sanity check that 015 isn't carrying an undocumented dropped hunk of the same shape.

---

**Today you learned:** A `git cherry-pick --no-commit` that conflicts leaves the conflicts in the index but **no sequencer state**, so `--abort` fails with "no cherry-pick in progress" — back out with path-scoped `git reset` + `git restore --worktree --source=HEAD` + `rm`, never `reset --hard`.

**Rule of thumb:** "All four wiring sites are mechanical" is a parent assumption to *verify in Step 1*, not trust — an entire upstream feature can vanish between LTS lines (here, the whole `ObjectStorageConnectionInfo` family), turning a presumed one-liner into an uncovered `modify/delete`.

**Next rabbit hole:** When did upstream remove the `ObjectStorageConnectionInfo` distributed-cache credential-forwarding path, and did patch 015's S3 port quietly absorb the parallel `S3ObjectStorageConnectionInfo.cpp` deletion?

 Now I'm putting together the final halt-and-escalate report to send back to the user.

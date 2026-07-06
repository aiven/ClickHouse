# Patch 001 — advertise-host-from-config

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | `f2d32562fa0` (sibling: "Advertise host from config for Replicated databases") | (unknown — to be researched at 25.X dossier merge) | (the original Aiven carry; same author group) |
| 25.8-aiven | `ac84fa6f7c4cd23bcc2c9c2dec5862e62e9e5c52` | Tilman Moeller (author), Aliaksei Khatskevich (committer), Kevin Michel (co-author) | the version we are porting FROM |
| 26.3-aiven | `patch-drop(001)` | T3.8 parent preflight (this dispatch) | `obsoleted-by-upstream` — upstream `9dd658aea06` landed semantically-identical fix; see §2 |

## 1. Purpose

Use the explicitly configured `<interserver_http_host>` from server config instead of the system hostname when constructing the `Host ID` (`hostname:port:database_uuid`) for `DatabaseReplicated` registration in ZooKeeper. In containerized/cloud environments the system hostname (e.g., `pod-abc123`) is not the network-accessible address, causing replica communication to fail. The `Host ID` is also used for replica discovery and DDL coordination, so any mismatch between the system hostname and the externally-reachable address breaks replication.

Source SHA on `v25.8.18.1-lts-aiven`: `ac84fa6f7c4cd23bcc2c9c2dec5862e62e9e5c52` (from `docs/aiven/uplifts/26.3/inventory.md` row 001).
Original author: Tilman Moeller <tilman.moeller@aiven.io>, 2025-12-02.
Original purpose (verbatim from `git log --format=%B`):

```
Advertise host from config for replicated databases

In containerized/cloud environments, the system hostname (e.g., pod-abc123) is not the network-accessible address. This causes replica communication to fail.

Use the explicitly configured interserver_http_host from config instead of system hostname.

The hostname is part of the Host ID hostname:port:database_uuid
and replicated database uses the Host ID for Zookeeper registration, replica discovery and DDL coordination.

Co-authored-by: Kevin Michel <kevin.michel@aiven.io>
```

## 2. Upstream-drift findings

> **Conclusion: `obsoleted-by-upstream`.** Upstream commit `9dd658aea06f6618f331f15d85bfa3771f590c3c` ("Fix DatabaseReplicated to respect interserver_http_host config" by `xiaohuanlin <xiaohuanlin1993@gmail.com>`, 2025-10-10, fixes GitHub issue #88361) landed the semantically-identical fix in vanilla ClickHouse between LTSes. Confirmed as an ancestor of `v26.3.10.62-lts` via `git merge-base --is-ancestor 9dd658aea06 v26.3.10.62-lts` → YES. **No patch carry is needed for 26.3.**

### Commands run (T3.8 parent preflight, 2026-05-27)

```bash
# (i) Patched-line stability — what's at the source patch's @@ -126,7 +126,8 @@ on HEAD?
sed -n '124,134p' src/Databases/DatabaseReplicated.cpp
# Result: lines 124-134 on HEAD are unrelated code (FailPoints namespace + constants).
# The getHostID function moved to line 140. Patched line no longer exists at the source line number.

# (ii) Context-window stability — diff source-patch's context against HEAD's
diff \
  <(git show ac84fa6f7c^:src/Databases/DatabaseReplicatedSettings.cpp | sed -n '124,134p') \
  <(sed -n '124,134p' src/Databases/DatabaseReplicated.cpp)
# Result: completely different content. The patch's @@ context anchors are unreachable.

# (iii) Semantic-equivalence on HEAD — does the patch's behavior already exist?
rg -n -A 6 'static inline String getHostID' src/Databases/DatabaseReplicated.cpp
# Result: lines 140-145 contain:
#   static inline String getHostID(ContextPtr global_context, const UUID & db_uuid, bool secure)
#   {
#       auto host_port = global_context->getInterserverIOAddress();
#       UInt16 port = secure ? ... ;
#       return Cluster::Address::toString(host_port.first, port) + ':' + toString(db_uuid);
#   }
# This is SEMANTICALLY IDENTICAL to the source patch's post-image, which reads:
#   const auto host = global_context->getInterserverIOAddress().first;
#   return Cluster::Address::toString(host, port) + ':' + toString(db_uuid);

# Upstream-equivalent search:
git log --oneline -S 'getInterserverIOAddress' --reverse -- src/Databases/DatabaseReplicated.cpp
# Result: 9dd658aea06 "Fix DatabaseReplicated to respect interserver_http_host config"

git merge-base --is-ancestor 9dd658aea06 v26.3.10.62-lts && echo YES
# Result: YES
```

### Findings

- **Source patch** (`ac84fa6f7c`, Tilman Moeller / Aiven, 2025-12-02): replaces `getFQDNOrHostName()` with `global_context->getInterserverIOAddress().first` in `getHostID`. 1 file / +2 / -1.
- **Upstream-equivalent** (`9dd658aea06`, `xiaohuanlin <xiaohuanlin1993@gmail.com>`, 2025-10-10, fixes GH #88361): replaces `getFQDNOrHostName()` with `auto host_port = global_context->getInterserverIOAddress(); ... host_port.first` in the same `getHostID` function. Same call site, same callee, same observable effect.
- **Convergent fix:** the two patches were authored independently for different motivations (Aiven cited containerized environments; upstream cited node-replacement-with-same-IP scenarios in GH #88361), but landed the same semantic change. Upstream landed first by ~2 months but did not propagate to the `v25.8` LTS because that LTS branched before October 2025. The Aiven patch was needed for 25.8; it becomes redundant for 26.3.
- **File-level drift:** `src/Databases/DatabaseReplicated.cpp` saw 1,177 commits between `v25.8.18.1-lts` and `v26.3.10.62-lts` (heavy churn). The `getHostID` function itself moved from line 126 to line 140 and gained an additional intermediate variable (`host_port`), but its semantics under the patched call site are unchanged.
- **Conclusion:** `obsoleted-by-upstream`. Drop the patch from the 26.3 carry list. No worker dispatch needed. Audit trail: this dossier + upstream SHA citation + inventory annotation.

## 3. C++ review

`n/a — patch is not being carried in this uplift (obsoleted-by-upstream). Upstream's equivalent passed upstream review; review re-application not required.`

## 4. Test design

`n/a — no patch source change is being staged. The upstream-equivalent SHA brought the behavior into vanilla 26.3; any regression test should be authored upstream against the upstream call site, not as an Aiven-specific test.`

A latent concern for completeness: the upstream and the Aiven patches do NOT introduce a regression test of their own (neither `9dd658aea06` nor `ac84fa6f7c` ship test files). The behavior — "ZK `host_id` reflects `interserver_http_host` when explicitly set" — is only exercised in production environments where the override differs from the system hostname. In default stateless test config the two values are typically equal, so a stateless test would either be `no_trigger_on_current_lts` or would require a custom config override (raising into integration-test territory). Documenting here that the absence of a regression test against `9dd658aea06` is an upstream gap; if Aiven cares about defending the behavior across future rebases, the right move is an integration test (e.g., `test_aiven_replicated_database_host_id_from_config/`) that sets `<interserver_http_host>` to a distinct sentinel value and asserts `system.zookeeper` reflects it. **Defer** until the third occurrence of "behavior covered only by upstream merge, no regression test" — that's the trigger for codifying the integration-test gap-filling pattern.

## 5. Rollback considerations

`n/a — no patch carry, nothing to roll back.`

For audit completeness: if `9dd658aea06` were ever reverted upstream (it shouldn't be — it's a behavioral correctness fix), the Aiven patch would need to be re-introduced for 26.3. The dossier's §0 Lineage row retains the `25.8-aiven` SHA precisely so future uplifts can locate the re-application path.

## 6. Per-uplift notes

### 25.3-aiven (historical)

Aiven sibling SHAs found in `git log`: `f2d32562fa0`, `fd271f76d11`, `b2fecc67a57`, `e89d64a1ba2`, `ac84fa6f7c4` (this is the 25.8-aiven one). To be researched at the 25.X dossier merge.

### 25.8-aiven (historical, the version we ported FROM)

Carried as `ac84fa6f7c4`. Aiven-only fix; upstream had not yet landed `9dd658aea06` at the time `v25.8` branched.

### 26.3-aiven (this uplift)

- **Cherry-pick was: dropped (obsoleted-by-upstream).**
- Upstream-drift conclusion: `obsoleted-by-upstream` (from §2).
- Test added at: n/a (no patch source change).
- Time-to-resolve: ~10 minutes wall-clock — parent preflight (`(i)/(ii)/(iii)` discipline from T3.7 Finding A), upstream-equivalent search, dossier authoring. No worker dispatch consumed.
- **Anything surprising:** the T3.7 Finding A discipline (writing out three independent stability checks instead of collapsing into "should apply cleanly") immediately caught this as `obsoleted-by-upstream` instead of letting the worker discover it mid-dispatch. Throughput win: ~25 minutes of worker time saved (vs the prior workflow where a worker would have attempted the cherry-pick, hit a context-window-shift conflict at minimum, and then had to run the upstream-equivalent search before reaching the same conclusion). Counts as the second observation of the parent-preflight-discipline mitigation working — see T3.8 retrospective for the rule-of-three counter update.

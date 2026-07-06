# Patch 023 — fix-ipv6-s3-object-storage-host

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `abd678347b` | Tilman Moeller (author), co-authored by Kevin Michel, 2025-12-15 | (the version we are porting FROM) |
| 26.3-aiven | — (no separate commit) | parent agent, 2026-06-03 | **`already-applied`** — folded into `patch-port(015)` (`5da90310af3`); see §2 |

## 1. Purpose

Fixes IPv6 S3 object-storage hosts: the AWS SDK does not fully support IPv6 in
hostnames because the escaping brackets (`[...]`) are not parsed/removed at the
right time. On 25.8 the fix was a pure `contrib/aws` gitlink bump
(`39c331979c5` → `30c8374334b`) — the change lives entirely in the
`aiven/aws-sdk-cpp` fork; there is no ClickHouse-side source hunk.

## 2. Upstream-drift / validity findings

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`.

- **Already present in the tree.** The 26.3 `aiven/aws-sdk-cpp` fork branch
  (`aiven/clickhouse-v26.3.10.62`) stacks the Aiven changes linearly:
  base `22f694afbdc7` → `2bdb77a6d9c` ("Allow delegating S3 signature to a separate
  process", = patch 015) → **`c930cb8e8c51`** ("Fix IPv6 S3 object storage host",
  = this patch, the branch HEAD).
- **`patch-port(015)` (`5da90310af3`) already pinned the gitlink to the branch
  HEAD `c930cb8e8c51`** (verified: its diff is
  `22f694afbdc7` → `c930cb8e8c51`). Because a submodule gitlink is atomic to a
  single commit and the fork stacks 015→023 linearly, pinning to the HEAD
  necessarily brings in the IPv6 fix. So the IPv6 S3 host fix is already in the
  working tree.
- **Net effect:** there is nothing left to bump (a `patch-port(023)` would be an
  empty `c930cb8 → c930cb8` no-op). The 26.3 tree is semantically correct; only the
  1:1 patch→commit mapping is collapsed into 015. Restoring a separate 023 commit
  would require amending 015 (forbidden by the branch policy), and would yield no
  behavioral difference.

## 3. C++ / security review

No ClickHouse-side code. The behavioral change is confined to the AWS SDK fork
(IPv6 bracket parsing in host handling) and is already compiled into the pinned
`contrib/aws` commit. No new attack surface; it only makes IPv6 S3 endpoints
usable.

## 4. Test design

None. The change is internal to the AWS SDK fork and has no isolated
ClickHouse-side surface; the source patch shipped no test. Validation is implicit:
the pinned fork commit `c930cb8` builds and links as part of every `contrib/aws`
build since `patch-port(015)`.

## 5. Rollback considerations

Rolling back 023 is not meaningful in isolation — it would require moving the
`contrib/aws` gitlink off `c930cb8` to the intermediate `2bdb77a6d9c`, which would
also be a manual fork-pointer change. No ClickHouse source to revert.

## 6. Per-uplift notes

### 25.8-aiven (historical)

Source `abd678347b` (author Tilman Moeller, co-authored by Kevin Michel,
2025-12-15). Single-line `contrib/aws` gitlink bump `39c331979c5` → `30c8374334b`.

### 26.3-aiven (this uplift)

- `already-applied`: the IPv6 S3 host fix is the HEAD of the prepared
  `aiven/aws-sdk-cpp` 26.3 branch (`c930cb8e8c51`), which `patch-port(015)`
  (`5da90310af3`) already pinned the `contrib/aws` gitlink to.
- No commit, build, or test for 023 — documentation only.
- See the submodule-forks runbook registry row for `contrib/aws`.

# Patch 007 — recover-lost-replica-deflate-qpl-setting

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | (unknown — to be researched at 25.X dossier merge) | (unknown) | (unknown — recoverable from `aiven/ClickHouse` history if needed) |
| 25.8-aiven | `5228bf2cd4` | Tilman Moeller (author) / Aliaksei Khatskevich (committer) | (the version we are porting FROM) |
| 26.3-aiven | `patch-drop(007)` | T3.1 worker, dispatch 2026-05-22 | drift-superseded — drop accepted; see §2 |

The drop was accepted and committed as `patch-drop(007)` (no code carried; reason in §2).
Find it with `git log --grep '^patch-drop(007)'`.

## 1. Purpose

When a new replica is added to an existing `DatabaseReplicated` cluster, the
`recoverLostReplica` function reads each table's metadata from ZooKeeper and
re-issues the `CREATE TABLE` statement locally. ZooKeeper stores only the
`CREATE TABLE` statement plus the per-table settings — not the global
`Settings` that were active in the session where the table was first created.

In 25.8, if a table had been created with `CODEC(DEFLATE_QPL)`, the
`CompressionCodecFactory` gate required the global setting
`enable_deflate_qpl_codec = 1` to even parse the codec name. Recovery on the
new replica ran with default settings, so the recovery `CREATE TABLE` failed
and the replica became permanently stuck. The Aiven patch flips
`enable_deflate_qpl_codec` to `1` in the recovery `query_context` so that
recovery can re-parse the codec the user already chose at table-creation time.
The "why" is durable: a recovery path should never refuse to recreate a table
the user already has on disk on the source replica.

Source SHA on `v25.8.18.1-lts-aiven`: `5228bf2cd4` (from
`docs/aiven/uplifts/26.3/inventory.md` row 007).
Original author: `tilman.moeller@aiven.io` (per `git log --format=%ae`).
Original purpose (verbatim from the source commit body):

> Add missing settings to recoverLostReplica
>
> When adding a new replica to an existing DatabaseReplicated cluster,
> the recoverLostReplica() function reads table metadata from ZooKeeper
> and recreates tables. However, the metadata stored in ZooKeeper contains
> only the CREATE TABLE statement and table settings, not the global
> settings that were active during original table creation.
>
> If a table was created with the DEFLATE_QPL codec (which requires the
> enable_deflate_qpl_codec global setting), a new replica would fail to
> create the table during recovery because this setting is not enabled.
>
> This fix explicitly enables enable_deflate_qpl_codec in the recovery
> query context, ensuring that tables using this codec can be successfully
> recreated on new replicas.
>
> Co-authored-by: Kevin Michel <kevin.michel@aiven.io>

## 2. Upstream-drift findings

> Mandatory section. The point of this section is to verify the patch is still
> SEMANTICALLY correct against `v26.3.10.62-lts`, not just textually applicable.

### Commands run

```bash
# File-level drift (count + spot-check).
git log v25.8.18.1-lts..v26.3.10.62-lts --oneline -- src/Databases/DatabaseReplicated.cpp | wc -l
# → 138 commits touched the file between the two LTSes.

# Function-level drift (recoverLostReplica specifically).
git log v25.8.18.1-lts..v26.3.10.62-lts -L :recoverLostReplica:src/Databases/DatabaseReplicated.cpp --oneline | grep -E '^[0-9a-f]{7,}' | head
# → recoverLostReplica's body has been touched many times (thread-group plumbing,
#   logging, callback runners, internal-query bookkeeping) but the two context
#   lines this patch sits between are unchanged.

# The context lines this patch anchors against still exist in 26.3.
git grep -n 'database_replicated_allow_explicit_uuid\|database_replicated_allow_replicated_engine_arguments' v26.3.10.62-lts -- src/Databases/DatabaseReplicated.cpp
# → 1558:        query_context->setSetting("database_replicated_allow_explicit_uuid", 3);
# → 1559:        query_context->setSetting("database_replicated_allow_replicated_engine_arguments", 3);

# Does `enable_deflate_qpl_codec` still gate anything in 26.3?
git grep -n 'enable_deflate_qpl_codec' v26.3.10.62-lts -- src/Core/ src/Compression/
# → src/Core/Settings.cpp:7828:  MAKE_OBSOLETE(M, Bool, enable_deflate_qpl_codec, false)
# → No other reference. The setting is OBSOLETE — "Obsolete setting, does nothing."

# Does the DEFLATE_QPL codec still exist in 26.3?
git grep -n 'DEFLATE_QPL\|DeflateQpl\|deflate_qpl' v26.3.10.62-lts -- src/Compression/
# → src/Compression/CompressionInfo.h:49:  /// DeflateQpl  = 0x99, /// Removed, don't reuse for another codec
# → No registerCodecDeflateQpl in src/Compression/CompressionFactory.cpp.

# When and how was the codec removed?
git log --all --oneline -- src/Compression/CompressionCodecDeflateQpl.cpp | head
# → 034e9714a63 Remove QPL and QAT  (Robert Schulze, 2025-12-15)
git merge-base --is-ancestor 034e9714a63 v26.3.10.62-lts && echo IN_26.3
# → IN_26.3
git merge-base --is-ancestor 034e9714a63 v25.8.18.1-lts && echo IN_25.8 || echo NOT_IN_25.8
# → NOT_IN_25.8

# Did upstream add a recovery-specific fix that would supersede this patch?
git log v25.8.18.1-lts..v26.3.10.62-lts --grep='recoverLostReplica' --oneline
git log v25.8.18.1-lts..v26.3.10.62-lts --grep='deflate_qpl' --oneline
# → No commits with either grep target. (The Schulze commit removes the codec
#   entirely; it does not patch the recovery path.)
```

Full logs are stored under `tmp/patch-007/` (drift-file-oneline.txt,
drift-func-log.txt, setting-grep.txt, qpl-grep.txt).

### Findings

- Upstream changes to touched files between prior and current LTS:
  - `src/Databases/DatabaseReplicated.cpp`: 138 commits, but the `recoverLostReplica`
    block between `database_replicated_allow_explicit_uuid = 3` and
    `database_replicated_allow_replicated_engine_arguments = 3` is structurally
    unchanged — the original textual patch would still apply cleanly.
- Upstream changes that touched the patch's behavior (symbols, error codes, callers):
  - Upstream commit `034e9714a63` ("Remove QPL and QAT", Robert Schulze, 2025-12-15)
    deleted the `DEFLATE_QPL` codec end-to-end: `src/Compression/CompressionCodecDeflateQpl.{h,cpp}`,
    the `contrib/qpl` and `contrib/idxd-config` submodules, the `qpl-cmake`
    plumbing, `registerCodecDeflateQpl`, and the codec opcode (now reserved as
    `Removed, don't reuse`). The same commit demoted the gate setting
    `enable_deflate_qpl_codec` to `MAKE_OBSOLETE(..., false)`, whose macro body
    is literally documented as `"Obsolete setting, does nothing."` in
    `src/Core/SettingsObsoleteMacros.h`.
  - Consequence: in 26.3 there is no codec named `DEFLATE_QPL` to register at
    table-creation time, the gate setting has no remaining reader, and no other
    component depends on the setting being `1`. The patch's `setSetting(...)`
    call would compile, run without throwing, and have no observable effect.
- Conclusion: **`irrelevant-by-removal`** — upstream removed the feature this
  patch was protecting (`DEFLATE_QPL` codec) in 26.3. Carrying the patch would
  silently no-op and create maintenance debt at every future uplift. Proposed
  action: DROP. Recorded upstream-equivalent SHA for next uplift's lineage:
  `034e9714a63 Remove QPL and QAT` (not an "equivalent fix" — a removal of the
  whole feature surface; future uplifts should skip this patch unconditionally).

A pre-existing user-impact concern is worth flagging to the human reviewer
(out-of-scope for this dispatch): users upgrading from 25.x with on-disk
data compressed by `DEFLATE_QPL` cannot read it on 26.3. This is an upstream
behavior, not introduced by dropping this patch — but it confirms that
"fixing" the recovery path in 26.3 is moot, because 26.3 cannot recreate
a `CODEC(DEFLATE_QPL)` table even if `recoverLostReplica` enabled every
setting in existence.

## 3. C++ review

Applied to the SOURCE patch (since no port was performed). The aim is to
record what the review would have concluded on a hypothetical port, so the
next uplift's worker sees the substantive answers.

- 1 Lifetime + ownership: `n/a — the patch is a single setSetting() on the local query_context shared_ptr; no new ownership relations.`
- 2 Exception safety: `✓ — setSetting() throws only if the setting name is unknown or the value type mismatches; on throw the surrounding try-block in recoverLostReplica already restores prior state via the existing rollback path.`
- 3 Thread-safety + concurrency: `✓ — recoverLostReplica runs on the per-database recovery thread (DatabaseReplicated::startupBackgroundActivity → ddl_worker → recovery loop); query_context is a freshly-constructed local Context, not the shared Context, so setSetting is safe without locks.`
- 4 Performance + memory: `n/a — control path that runs once per replica recovery; not on a per-row hot path.`
- 5 Settings as public API: `🚨 — in 26.3 the setting enable_deflate_qpl_codec is MAKE_OBSOLETE (src/Core/Settings.cpp:7828) and its setSetting body is "does nothing". Calling setSetting on it succeeds without throwing but has no observable effect. This is the headline reason for proposing DROP in §2.`
- 6 Error handling: `n/a — patch does not introduce a new error path.`
- 7 Upstream / vendored code: `✓ — patch does not touch contrib/**, .claude/**, root AGENTS.md, .github/workflows/**, or CONTRIBUTING.md.`
- 8 Behavior under settings: `🚨 — the patch enables an OBSOLETE setting in 26.3. AGENTS.md §7 requires "if you cannot demonstrably distinguish the Aiven gate, escalate" — here the Aiven gate has been fully removed upstream, so the gate distinction is vacuous.`

## 4. Test design

(c) **No new test — patch is being proposed for DROP, not port.**

- Existing test path: `n/a — no behavior to exercise in 26.3.` The historical
  behavior this patch protected (the codec-name parser refusing `DEFLATE_QPL`
  at recovery time) no longer exists; there is nothing left to test on either
  side of the gate.
- Why this test is not warranted: The patch's hypothetical post-port effect is
  unobservable. `recoverLostReplica` calls `query_context->setSetting("enable_deflate_qpl_codec", 1)`,
  which in 26.3 routes through `MAKE_OBSOLETE` and is a documented no-op; no
  downstream code in 26.3 reads the setting; the codec name `DEFLATE_QPL` is
  not registered in `CompressionCodecFactory`. A "passes after, fails before"
  test cannot be constructed because neither side of "before" and "after"
  observably differs.
- Why a new test is not warranted: see above. Per AGENTS.md §7, "silently
  shipping no test is forbidden" — but the correct response here is not to
  ship the patch with no test, it is to NOT ship the patch and document why.

If the human decides to KEEP the patch despite the drift finding (e.g. as
documentation that historically Aiven needed this gate flipped, in case the
codec is restored), the test design would still be undesignable and the
worker would re-dispatch with `escalation_reason: test_design_blocked`. The
cleaner outcome is the DROP proposed in §2.

## 5. Rollback considerations

- Revert safety: trivial — the patch is one line in a control path. No schema
  migration, no on-disk format change, no ZK node changes. A revert is just
  removing the `setSetting` call.
- State that survives restart: none. The setting is per-`query_context`,
  freshly constructed at each `recoverLostReplica` invocation.
- Setting to disable the behavior at runtime: `n/a` in 26.3 — the setting
  this patch flips is itself a no-op, so the behavior cannot be observed at
  all, let alone disabled.

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

`n/a — to be filled at 25.X dossier merge.` The patch appears for the first
time in `v25.8.18.1-lts-aiven` per the T2.2 inventory.

### 25.8-aiven (historical, may be empty)

Original carry. Author: Tilman Moeller. Committer: Aliaksei Khatskevich.
Co-author: Kevin Michel. The fix gated on `enable_deflate_qpl_codec` being
flipped to `1` so that `CompressionCodecFactory` would parse `DEFLATE_QPL`
during recovery `CREATE TABLE`. No 25.X test was authored alongside the patch
(per the inventory — this is one of the patches T2.2 marked `cherry_pick_clean=yes`
but the inventory does not list an accompanying test).

### 26.3-aiven (this uplift)

- Cherry-pick was: NOT performed. Worker stopped at Step 1 (upstream-drift
  analysis) with conclusion `irrelevant-by-removal`.
- Upstream-drift conclusion: `irrelevant-by-removal`. Upstream commit
  `034e9714a63` removed the QPL codec, its setting gate, and the contrib
  submodules. The setting `enable_deflate_qpl_codec` is `MAKE_OBSOLETE` and
  documented as "does nothing".
- Test added at: `n/a — no source change in this dispatch; see §4.`
- Time-to-port (subagent wall-clock): ~15 minutes (Step 1 was sufficient to
  reach a hard policy conclusion; no Step 2/3/4/5/6 work was performed).
- Anything surprising: the patch was `cherry_pick_clean=yes` per T2.2, which
  would have been a green flag had we relied on textual analysis alone. The
  semantic drift (whole-feature removal between LTSes) is invisible to
  `git cherry-pick` and would have produced a silently-broken port. This is
  the canonical justification for the mandatory Step 1 (upstream-drift
  analysis) in the T3 procedure: 53/78 patches had textual conflicts in T2.2,
  but this is the first observed case of "textually clean, semantically
  obsolete". Future workers should treat `cherry_pick_clean=yes` as
  insufficient evidence of portability.

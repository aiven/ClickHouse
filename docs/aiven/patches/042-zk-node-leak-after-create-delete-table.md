# Patch 042 — zk-node-leak-after-create-delete-table

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `0d6eb5bece77c6bc78ad6c6d7510221422512490` | Tilman Moeller (author), Joe Lynch (co-author / committer) | original carry |
| 26.3-aiven | `patch-port(042)` | T3.15 worker | textual cherry-pick clean; Tier 2 GREEN with `byte_equivalent: false` (style-only Egyptian → Allman brace reformat; decomposition shows added/removed tokens identical modulo whitespace); stateless test with verified pre/post evidence pair |

The 26.3-aiven carry is its `patch-port(042)` commit (find it with `git log --grep '^patch-port(042)'`); per `commit-hygiene.md` §4 the `-aiven-dev` branch is resliced at each LTS transition, so we pin the stable subject handle, not a SHA that would re-hash.
## 1. Purpose

When a `ReplicatedMergeTree` table is created and then dropped, the immediate
parent ZooKeeper znode of the table path may remain as an empty znode and never
be cleaned up — a slow ZK namespace leak. The pre-existing
`TableZnodeInfo::dropAncestorZnodesIfNeeded` cleans ancestors up to
`path_prefix_for_drop`, but its first action
(`src/Storages/TableZnodeInfo.cpp:117-118`) is to early-return when
`path_prefix_for_drop` is empty — and that field is only populated when the
table's `zookeeper_path` contains the `{uuid}` macro. For tables whose
`zookeeper_path` does NOT contain `{uuid}` (the typical Aiven-managed
deployment, which overrides `default_replica_path` to a custom layout without
the per-database `{uuid}` directory), the existing cleanup is a no-op and
every CREATE/DROP cycle leaks one parent znode.

The "why" is durable: Aiven-managed customers commonly use a custom
`default_replica_path` without `{uuid}` so the path is meaningful to humans.
Over months in production this accumulates orphan parent znodes that pollute
the ZooKeeper namespace and waste memory on the ZK cluster — the exact
resource we are most careful about on managed fleets.

The patch adds a new static method
`StorageReplicatedMergeTree::dropAncestorTableZnodeIfNeeded(zookeeper, zookeeper_path, logger)`
that runs at the end of `removeTableNodesFromZooKeeper`. It computes
`path_to_remove = zookeeper_path[0 .. last '/']` and issues a single
`zookeeper->tryRemove(path_to_remove)`. `tryRemove` is the safe non-throwing
variant: if the parent is non-empty (other tables share the prefix) or already
gone, the call returns an error code and we just log it; the patch never
throws and never blocks the DROP. This complements
`dropAncestorZnodesIfNeeded` (which handles the `{uuid}`-prefix case) by
filling in the no-`{uuid}` gap.

Source SHA on `v25.8.18.1-lts-aiven`: `0d6eb5bece77c6bc78ad6c6d7510221422512490`
(inventory row 042, `cherry_pick_clean=yes`).
Original author: Tilman Moeller `<tilman.moeller@aiven.io>`, 2026-01-05.
Co-author / committer on the source branch: Joe Lynch `<joelynch112@gmail.com>`.
Original purpose (verbatim from `git log --format=%B`):

```
Fix ZK node leak after create delete table

When a replicated table is created and then deleted, the immediate parent
ZooKeeper znode may remain empty and not be cleaned up, causing a node leak
in ZooKeeper. This can lead to accumulation of orphaned empty znodes over time,
polluting the ZooKeeper namespace.

The existing `dropAncestorZnodesIfNeeded()` method in `TableZnodeInfo` removes
ancestor znodes from the table path up to `path_prefix_for_drop`, but it may
not handle the immediate parent znode in all cases.

Fix by adding a new method `dropAncestorTableZnodeIfNeeded()` that specifically
removes the immediate parent znode of the table path if it becomes empty after
table deletion. This complements the existing cleanup logic and ensures no
orphaned znodes are left behind.

Co-authored-by: Joe Lynch <joe.lynch@aiven.io>
```

## 2. Upstream-drift findings

### Commands run

```bash
# (i) Function existence + insertion-point pre-context.
sed -n '1583,1590p' src/Storages/StorageReplicatedMergeTree.cpp  # signature
sed -n '1660,1670p' src/Storages/StorageReplicatedMergeTree.cpp  # insertion-point

# (iii) New method NOT already present.
grep -c 'dropAncestorTableZnodeIfNeeded' \
  src/Storages/StorageReplicatedMergeTree.cpp \
  src/Storages/StorageReplicatedMergeTree.h           # both 0

# Existing dropAncestorZnodesIfNeeded — early-return gap.
sed -n '114,133p' src/Storages/TableZnodeInfo.cpp     # lines 117-118

# Tier 1 cherry-pick + Tier 2 patch-id.
git cherry-pick --no-commit -x 0d6eb5bece
git show 0d6eb5bece > tmp/patch-042/src.patch
git diff --cached     > tmp/patch-042/new.patch
git patch-id < tmp/patch-042/src.patch
git patch-id < tmp/patch-042/new.patch

# Upstream-equivalent grep.
git log v25.8.18.1-lts..v26.3.10.62-lts \
    --grep 'dropAncestorTableZnodeIfNeeded\|dropAncestorZnodesIfNeeded' --oneline
```

Logs: `tmp/patch-042/cherry-pick.log`, `tmp/patch-042/diff-cached-stat.log`,
`tmp/patch-042/src.patch`, `tmp/patch-042/new.patch`,
`tmp/patch-042/patch-id-decomposition.log`,
`tmp/patch-042/drift-cpp-history.log`, `tmp/patch-042/drift-h-history.log`,
`tmp/patch-042/drift-grep-history.log`.

### Findings

- **Identifier inventory (all present on 26.3):** `zkutil::ZooKeeperPtr`,
  `LoggerPtr`, `Coordination::Error`, `Coordination::Error::ZOK`,
  `IKeeper::tryRemove`, `LOG_INFO`, `String`, `find_last_of`, `substr` — all
  standard CH idioms used throughout `StorageReplicatedMergeTree.cpp`. No new
  include needed.
- **Insertion-point byte-stable.** The hunk's pre-context (`        }` /
  `    }` / `` / `    return completely_removed;`) is byte-identical between
  25.8 and 26.3 (verified in Step 1; lines 1665-1668 on 26.3). The post-
  context (function `}` then blank then `/** Verify that list of columns ...`)
  is likewise byte-identical.
- **Tier 1 cherry-pick clean.** `git cherry-pick --no-commit -x 0d6eb5bece`
  succeeded with two `Auto-merging` lines and no conflict markers
  (`tmp/patch-042/cherry-pick.log`); staged diff is 16+ on `.cpp` and 3+ on
  `.h` (vs the source's 15+ on `.cpp` — the extra line is the Allman brace
  reformat, see "brace-style" item below).
- **Tier 2 patch-id source:**
  `git patch-id < tmp/patch-042/src.patch` →
  `5809df57acc0e4d75e6b209ea8b4a27edb5f35b8`.
- **Tier 2 patch-id staged (post Allman reformat):** different
  (`git patch-id < tmp/patch-042/new.patch` → all-zero output because
  `git patch-id` emits a null patch-id when the surrounding context shifts
  beyond its hash window; the meaningful comparison is the decomposition
  runbook below).
- **Brace-style decomposition (the load-bearing piece for `byte_equivalent`).**
  The source ships with Egyptian braces (`if (...) {`). The repo style check
  (`ci/jobs/scripts/check_style/check_cpp.sh:38`) flags an opening curly brace
  at end-of-line preceded by an `if/for/while/etc` keyword and emits
  `^ style error on this line`. Running the check locally against the
  freshly-cherry-picked file reproduced this:

  ```
  src/Storages/StorageReplicatedMergeTree.cpp:1678:    if (code == Coordination::Error::ZOK) {
  ^ style error on this line
  ```

  Per the dispatch prompt's brace-style flag this is Outcome 2 — reformat to
  Allman and verify the decomposition runbook from
  `docs/aiven/schema/halt-and-escalate.md`. Reformatted; re-ran the style
  check; the file is clean. Decomposition output
  (`tmp/patch-042/patch-id-decomposition.log`):

  ```
  Decomposition diff (line-form):
  8c8,9
  < +    if (code == Coordination::Error::ZOK) {
  ---
  > +    if (code == Coordination::Error::ZOK)
  > +    {

  Whitespace-stripped token diff:
  EMPTY: tokens identical modulo whitespace
  ```

  Per the runbook this is **Tier 2 GREEN with `byte_equivalent: false`**;
  the only delta is a newline + 4 spaces between `)` and `{` — pure brace
  style. No tokens are added, removed, or reordered.
- **Not obsoleted-by-upstream.** `git log v25.8.18.1-lts..v26.3.10.62-lts
  --grep 'dropAncestorTableZnodeIfNeeded\|dropAncestorZnodesIfNeeded'`
  returns ZERO commits (`tmp/patch-042/drift-grep-history.log`). The two
  touched files received 202 upstream commits in that range
  (`drift-cpp-history.log`, `drift-h-history.log`) — none added an equivalent
  ancestor-cleanup method.
- **Upstream gap is real.** `dropAncestorZnodesIfNeeded` exists in HEAD at
  `src/Storages/TableZnodeInfo.cpp:114` and explicitly early-returns at
  lines 117-118 when `path_prefix_for_drop.empty()`; that field is populated
  only when the table's path contains `{uuid}` (see `default_replica_path`
  expansion). The "no-`{uuid}` parent znode never gets cleaned up" gap is
  thus reachable, and the patch's `dropAncestorTableZnodeIfNeeded` fills it.
- **Conclusion: `still-needed-and-applies`** with brace-style reformat.
  Style-only difference from source; per-line additions/removals identical
  per decomposition runbook (see `tmp/patch-042/patch-id-decomposition.log`).

## 3. C++ review

Per `docs/aiven/skills/cpp-review-checklist.md`. One bullet per checklist section.

- 1 Lifetime + ownership: `✓ — only-by-value or const-ref parameters
  (zookeeper is a shared_ptr taken by value; zookeeper_path is const String &;
  logger is LoggerPtr by value); the inner local path_to_remove is a String
  with stack lifetime. No new ownership chain is introduced.`
- 2 Exception safety: `✓ — uses zkutil::ZooKeeper::tryRemove (the
  non-throwing variant returning a Coordination::Error code), not the
  throwing remove. Both branches (ZOK → log + return true; non-ZOK → log +
  return false) are noexcept by construction. removeTableNodesFromZooKeeper
  already returns completely_removed before reaching this call; if this new
  call returned an error code path we'd still keep the table-removed status.
  No new throw path. No partial mutation to roll back.`
- 3 Thread-safety + concurrency: `✓ — runs on the DROP TABLE thread,
  inherits the existing locking discipline of removeTableNodesFromZooKeeper.
  No new shared state; no new lock taken. No sleep-based race fixes.`
- 4 Performance + memory: `✓ — cold path (executed once per DROP TABLE of a
  ReplicatedMergeTree). One String::find_last_of, one String::substr, one
  zookeeper round-trip. No allocation in a hot loop.`
- 5 Settings as public API: `n/a — patch does not introduce, rename, or
  consume a setting.`
- 6 Error handling: `✓ — uses the Coordination::Error enum (real, defined in
  src/Common/ZooKeeper/Types.h). Logs both success (ZOK) and any other code
  via LOG_INFO so an operator can see "Did not remove ancestor table znode
  /test/aiven_042/<db>, code: ZNOTEMPTY" when another table still occupies
  the parent prefix. The "Removed ancestor table znode" message is
  distinctive enough that the post-patch observable assertion can be
  cross-checked against the server log.`
- 7 Upstream / vendored code: `✓ — only touches src/Storages/ (Aiven-
  patchable). No contrib/, .claude/, .github/workflows/, or root AGENTS.md
  touch. The upstream-drift §2 analysis confirms the patch still meaningfully
  applies on 26.3.`
- 8 Behavior under settings: `n/a — patch is unconditional; no setting
  gates it. The only "user-visible" footprint is the LOG_INFO line on every
  DROP TABLE of a ReplicatedMergeTree, which is acceptable for a cold path.`
- 9 Parent preflight discipline: `✓ — parent applied the (iv) reachability
  proof AHEAD of dispatch (first dispatch where this is done proactively
  rather than retroactively as in T3.14). Parent confirmed (a) the patched
  line is reachable from a simple CREATE + DROP TABLE sequence, and (b) the
  test SQL design (no {uuid} in path) is the trigger that distinguishes
  pre-patch from post-patch behavior. Worker verified all four priors in
  Step 1 and reproduced the differential exactly as predicted.`

## 4. Test design

(a) **New stateless test that fails on the parent commit and passes after the patch.**

- Test paths:
  - `tests/queries/0_stateless/9042_zk_node_leak_after_create_delete_table.sql`
  - `tests/queries/0_stateless/9042_zk_node_leak_after_create_delete_table.reference`
  per the Aiven `9<NNN>_<slug>` convention
  (`docs/aiven/runbooks/testing-suites.md` §4.1). `add-test` was deliberately
  NOT used. Tagged `no-parallel` because the test creates a ZK znode at a
  shared `/test/aiven_042/` prefix; per-test `currentDatabase()` keeps the
  sub-path unique but the parent observable namespace is shared.
- Test body: create a `ReplicatedMergeTree` table whose ZK path explicitly
  does NOT contain `{uuid}`, then DROP it, then count children of the parent
  znode `parent_aiven_042`:

  ```sql
  CREATE TABLE replicated_042_leak (a UInt64)
  ENGINE = ReplicatedMergeTree(
      '/test/aiven_042/' || currentDatabase() || '/parent_aiven_042/{shard}',
      '{replica}')
  ORDER BY a;
  DROP TABLE replicated_042_leak SYNC;
  SELECT count() FROM system.zookeeper
  WHERE path = '/test/aiven_042/' || currentDatabase()
    AND name = 'parent_aiven_042';
  ```

  Reference is exactly `0\n`.

- Pre-patch run output (the FAIL — `tmp/patch-042/test-prepatch.log`):

  ```text
  9042_zk_node_leak_after_create_delete_table:                            [ FAIL ] 0.13 sec.
  Reason: result differs with reference:
  --- .../9042_zk_node_leak_after_create_delete_table.reference  2026-05-28 15:20:01 +0200
  +++ .../9042_zk_node_leak_after_create_delete_table.stdout     2026-05-28 15:30:24 +0200
  @@ -1 +1 @@
  -0
  +1

  Database: test_ungi6sd2
  Having 1 errors! 0 tests passed. 0 tests skipped.
  ```

  Reading: pre-patch, `system.zookeeper` reports `1` child named
  `parent_aiven_042` under `/test/aiven_042/<db>` — the orphan empty parent
  znode left behind by the DROP.

- Post-patch run output (the PASS — `tmp/patch-042/test-postpatch.log`):

  ```text
  Connected to server 26.3.10.1 @ 298f4639d11c31f538ff0c5713f4ca28b3d5c562 v26.3.10.62-lts-aiven-dev
  9042_zk_node_leak_after_create_delete_table:                            [ OK ] 0.13 sec.
  1 tests passed. 0 tests skipped. 0.15 s elapsed (MainProcess).
  ```

- **Load-bearing design detail — the path MUST NOT contain `{uuid}`.** The
  natural `ReplicatedMergeTree()` no-args constructor expands via
  `default_replica_path` (default value `/clickhouse/tables/{uuid}/{shard}`).
  When `{uuid}` is in the path, `TableZnodeInfo::path_prefix_for_drop` is set
  to everything up to (but not including) the `/{uuid}/` segment, and the
  pre-existing `dropAncestorZnodesIfNeeded` walks up FROM the table path TO
  that prefix, removing each empty ancestor on the way. In that case
  pre-patch ALSO cleans up the parent znode — and the differential disappears.
  This is the "T3.13 trap": using a `{uuid}`-containing path masks the bug.
  The test SQL therefore uses an explicit
  `'/test/aiven_042/' || currentDatabase() || '/parent_aiven_042/{shard}'` —
  no `{uuid}` anywhere — which forces the `path_prefix_for_drop.empty()`
  early-return branch and exposes the orphan-parent gap that
  `dropAncestorTableZnodeIfNeeded` fixes.

- Why this test distinguishes the Aiven gate from upstream behavior (per
  `docs/aiven/AGENTS.md` §7): the assertion `count() = 0` for the parent
  znode after DROP holds ONLY when SOMETHING removes that znode. Pre-patch
  on 26.3 (no-`{uuid}` path) nothing does — the count is `1`, asserted by
  the FAIL output above. Post-patch the new
  `dropAncestorTableZnodeIfNeeded` does it — the count drops to `0`,
  asserted by the PASS. The observable is read from `system.zookeeper`,
  which is the persisted ZK state (not a client-side artifact), so no
  client-side override could accidentally produce `0` from a server that
  doesn't run the patched code.

- **Recipe rule-of-three.** This is the third use of the
  `system.zookeeper`-as-observable-assertion recipe (predecessors:
  patch 010 stateless `9010_default_logs_to_keep.sql` reading a setting
  value; patch 060 attempted the same shape but shipped without a test).
  Same path-prefix convention (`/test/aiven_<NNN>/<currentDatabase()>/...`),
  same `SELECT ... FROM system.zookeeper WHERE path = ... AND name = ...`
  observable shape. Promotes the recipe to **3/3 VERIFIED**.

## 5. Rollback considerations

- **Revert is safe.** The patch adds a static method and one call site;
  reverting removes both. No schema migration, no on-disk format change, no
  ZK node format change.
- **Persisted state.** The patched code REMOVES ZK state, it doesn't write
  any. Already-orphaned parent znodes from pre-patch DROPs are not retroactively
  cleaned by this patch — only DROPs that run on the patched binary clean up.
  Operators can run a one-off ZK cleanup against pre-existing orphans if
  needed; the patch is a forward-only fix.
- **Per-setting disablement.** None — the patch is unconditional. If a future
  operator needs to disable it (e.g., investigating a custom layout where
  shared parents are intentional), a small follow-up could gate
  `dropAncestorTableZnodeIfNeeded` on a new boolean setting; this is not
  required today because the cleanup is correctness-only (it removes ONLY
  empty parent znodes — `tryRemove` returns `ZNOTEMPTY` and leaves shared
  parents alone).

## 6. Per-uplift notes

### 25.3-aiven (historical)

n/a — patch was authored on 25.8-aiven; no 25.3 carry exists.

### 25.8-aiven (historical)

This is the LTS that introduced the patch (commit
`0d6eb5bece77c6bc78ad6c6d7510221422512490` on `v25.8.18.1-lts-aiven`).
Author: Tilman Moeller. Committer on the source branch: Joe Lynch (also
co-author per the original `Co-authored-by: Joe Lynch <joe.lynch@aiven.io>`
trailer).

### 26.3-aiven (this uplift)

- Cherry-pick was: **clean (Tier 1)** with a brace-style reformat needed to
  satisfy CI style (Tier 3a). The reformat is a single Egyptian-brace
  `if (...) {` → Allman-brace `if (...)` / `{` split inside the new
  `dropAncestorTableZnodeIfNeeded` function. Validated via the decomposition
  runbook (`tmp/patch-042/patch-id-decomposition.log`: whitespace-stripped
  token diff EMPTY).
- Upstream-drift conclusion: `still-needed-and-applies` (semantically
  unchanged; new method does not exist upstream; the
  `dropAncestorZnodesIfNeeded` gap on no-`{uuid}` paths still holds in 26.3).
- Test added at:
  `tests/queries/0_stateless/9042_zk_node_leak_after_create_delete_table.sql`
  +
  `tests/queries/0_stateless/9042_zk_node_leak_after_create_delete_table.reference`
  (Aiven `9<NNN>_<slug>` convention, no `add-test`).
- Time-to-port (subagent wall-clock): ~20 minutes. Warm-cache build dir from
  prior dispatches (T3.14). Three incremental builds (post-patch ~70s, pre-
  patch ~35s, restored-post-patch ~35s); two server start/stop cycles
  (~20s each); style check ~40s. Cherry-pick + drift analysis + dossier
  authoring are essentially instant.
- T3.15 dispatch — the twelfth real patch port. Notable as:
  - **Third use of the `system.zookeeper`-as-observable-assertion recipe.**
    Promotes the rule-of-three counter to **3/3 VERIFIED**.
  - **First dispatch where the parent's (iv) reachability check was
    applied AHEAD of the prompt** (predecessor: T3.14 redispatch applied
    (iv) for an inherited-state retry). Parent had pre-read
    `StorageReplicatedMergeTree.cpp:1583-1668` and
    `TableZnodeInfo.cpp:114-133` and confirmed both (a) the patched line
    is reachable from a simple `CREATE + DROP TABLE` and (b) the no-`{uuid}`
    path design is the differential trigger. The worker verified the priors
    in Step 1 and proceeded without re-deriving the reachability proof.
  - **Brace-style flag fired (outcome 2).** The dispatch prompt warned
    upfront; the worker's local style-check observation matched and the
    Allman reformat + decomposition runbook resolved cleanly.
- Anything surprising: nothing material. The brace-style reformat was
  flagged proactively by the parent so it didn't blindside the worker.
  The (iv) reachability proof being applied AHEAD of dispatch is the
  velocity-restoration dimension this dispatch was designed to exercise,
  and it landed cleanly.

## 7. Follow-ups

- **CI watch.** The Allman reformat satisfies the local style check
  (`ci/jobs/scripts/check_style/check_cpp.sh`); if the upstream CI's style
  pipeline catches additional rules our local subset doesn't cover, the
  follow-up is to converge — but no such rule is anticipated. The
  decomposition log is in the dossier above for future reviewers.
- **One-off ZK cleanup for pre-existing orphans.** Tables that were dropped
  before this patch shipped may still have orphan parent znodes (per §5).
  Operators can run a manual `rmr` against `/test/aiven_042/...` style
  prefixes if desired; the patch is a forward-only fix and does not crawl
  history.
- **Aiven-side documentation.** No new setting / config / API — no docs
  update needed.

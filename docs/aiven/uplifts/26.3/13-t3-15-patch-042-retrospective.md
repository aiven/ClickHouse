# T3.15 — patch 042 dispatch retrospective

> **What this is:** The reflection-after-cost step (spec §10 step 12) applied to the fifteenth real patch dispatch — the third use of the `system.zookeeper`-as-observable-assertion stateless-test recipe, the first dispatch to surface the "Egyptian-vs-Allman brace style mismatch" outcome, the first dispatch where the parent applied the `(iv) reachability` discipline **proactively** (rather than retro-applied as in T3.14), and the first dispatch to encounter the `spawn E2BIG` environmental issue from the editor's `StrReplace` against a large source file.
> **Date:** 2026-05-28.
> **Dispatch reference:** `docs/aiven/uplifts/26.3/log.md` row at `2026-05-28T13:17:30Z`; subagent id `toolu_01FigZuPjPKDHr5pMUFgpoQ6`; report archive `docs/aiven/uplifts/26.3/reports/toolu_01FigZuPjPKDHr5pMUFgpoQ6.md` (177 lines; backfilled offline from on-disk JSONL `29362a7f-552d-4803-ae0b-b2c1484a81ad.jsonl` after the hook v3 third regression delivered an `unknown / unknown` row — see retro 14).
> **Outcome of T3.15:** `success` (worker). Five files staged → committed in `f56743cd153`: `src/Storages/StorageReplicatedMergeTree.{cpp,h}` (+16 / +3 lines), `tests/queries/0_stateless/9042_zk_node_leak_after_create_delete_table.{sql,reference}` (new), `docs/aiven/patches/042-zk-node-leak-after-create-delete-table.md` (new dossier, 401 lines). Tier 1 cherry-pick: clean auto-merge. Tier 2 patch-id: `byte_equivalent: false`; decomposition log shows the whitespace-stripped token diff is EMPTY (pure brace-style difference, no semantic change). Tier 3 evidence pair: pre-patch FAIL with `expected 0 / actual 1`, post-patch OK.

## Headline

T3.15 was the cleanest dispatch in the post-T3.10 saga: parent preflight applied the `(i)/(ii)/(iii)/(iv)` discipline in full — the fourth check **predicted reachability of the patched code from a stateless trigger** by tracing `dropAncestorZnodesIfNeeded` at `TableZnodeInfo.cpp:117-118` and confirming the existing path EARLY-RETURNS when `path_prefix_for_drop.empty()` (i.e., the ZK path lacks a `{uuid}` macro). That gap is exactly what patch 042 fills, and the test trigger MUST exercise a path WITHOUT `{uuid}` to reach the patched code. The worker honored the fixed test design verbatim and produced a clean pre/post evidence pair on the first try.

**Two rule-of-three counters advanced.** The `system.zookeeper`-as-observable-assertion recipe (introduced in T3.7 Finding D for patch 010) reached **2 of 3** with patch 042. The `(iv) reachability` discipline (introduced in T3.13 escalation, retro-applied in T3.14) reached **2 of 3 of proactive use** with patch 042 — the parent wrote out the reachability proof in the dispatch prompt BEFORE the worker ran, and the worker confirmed it via the `grep -c dropAncestorTableZnodeIfNeeded ...` pre-cherry-pick check (`0 / 0` → patched code is genuinely absent on HEAD, the trigger will reach the new code path once applied).

**Two new findings surfaced.** The Egyptian-vs-Allman brace-style mismatch (Outcome 2 of the dispatch prompt's brace-style flag) produced its FIRST explicit occurrence in T3.x. The `spawn E2BIG` issue from `StrReplace` against `src/Storages/StorageReplicatedMergeTree.cpp` (a ~7400-line file) surfaced for the first time — the worker pivoted to a Python helper script and produced an output byte-identical to what `StrReplace` would have produced. Both findings are n=1 of 3 and tracked here for future calibration.

## What worked in T3.15

1. **Parent preflight applied the `(i)/(ii)/(iii)/(iv)` discipline in full.** Dispatch prompt at `tmp/patch-042/dispatch-prompt.md` wrote out:
   - (i) Patched-line stability: `removeTableNodesFromZooKeeper` at line 1583+ — STABLE.
   - (ii) Context-window stability: lines 1665-1668 byte-stable on 26.3 — STABLE.
   - (iii) Identifier inventory: `dropAncestorTableZnodeIfNeeded` absent from both `.cpp` and `.h` on HEAD (`grep -c` returns `0 / 0`) — function is genuinely new.
   - (iv) **Reachability proof:** `dropAncestorZnodesIfNeeded` at `TableZnodeInfo.cpp:114-133` has an early-return at lines 117-118 when `path_prefix_for_drop.empty() || ... == path.size()`. The existing cleanup logic skips parent-znode removal when the ZK path lacks a `{uuid}` macro substitution. **Therefore the test trigger MUST use an explicit ZK path WITHOUT `{uuid}` to reach the patched code path.** The dispatch prompt embedded this as a hard constraint on test design.

   The worker honored the (iv) constraint and authored `tests/queries/0_stateless/9042_zk_node_leak_after_create_delete_table.sql` with the explicit path `'/test/aiven_042/' || currentDatabase() || '/parent_aiven_042/{shard}'` (no `{uuid}`) — the parent znode `parent_aiven_042` becomes the leak-test target. Pre-patch the count is `1` (znode survives the drop), post-patch the count is `0` (new method cleans it up).
2. **`system.zookeeper` recipe applied a second time.** Patch 010 (T3.7) used `SELECT value FROM system.zookeeper WHERE path = '...'` to assert a default-value-change. Patch 042 uses `SELECT count() FROM system.zookeeper WHERE path = '...' AND name = '...'` to assert a leak-presence. Two shape variants (value vs. count) but the same observability primitive: ClickHouse's own ZK introspection makes ZK-state assertions trivial to write as `0_stateless/*.sql`.
3. **Worktree-flip evidence pair was textbook.** Worker followed the runbook (`docs/aiven/runbooks/build-and-test.md §4`) precisely: save staged-patch diff → flip out via `git restore --worktree --source=HEAD --` → rebuild → start server with `--path=./tmp/ch-smoke` overrides → run test → observe FAIL → stop server → flip back via `git checkout --` → clean tmp data → rebuild → start server → run test → observe PASS → stop server → verify `git diff` worktree-vs-index = 0 lines. No interim corruption, no flake.
4. **Tier 2 decomposition resolved the byte-equivalence mismatch cleanly.** Source patch-id (`5809df57acc0e4d75e6b209ea8b4a27edb5f35b8`) differs from staged patch-id because the source ships Egyptian braces (`if (...) {`) and the worker reformatted to Allman (`if (...) \n {`). The decomposition runbook (`docs/aiven/schema/halt-and-escalate.md` Tier 2) explicitly accommodates this case: whitespace-stripped `+`/`-` diff is computed and verified EMPTY → `byte_equivalent: false` is recorded with the decomposition log as evidence. This is the **first explicit Outcome-2 in T3.x** — a documented style-only divergence between source and staged.
5. **Local style check (`ci/jobs/scripts/check_style/check_cpp.sh`) caught the issue BEFORE commit.** Worker ran the style check post-cherry-pick and observed the Egyptian-brace rejection. The pre-commit style check is now empirically validated as a useful guard against shipping pre-CI rejections.

## What surfaced

### A. `system.zookeeper`-as-observable-assertion recipe: rule-of-three 2 of 3 (working)

**Symptom (positive):** The recipe introduced in T3.7 Finding D for patch 010 ("setting-default tuning, assert via `system.zookeeper`") generalized cleanly to patch 042's "ZK-leak post-DROP, assert via `system.zookeeper`". Same primitive (`SELECT … FROM system.zookeeper WHERE path = '…'`), two distinct assertion shapes:
  - Patch 010: `SELECT value FROM system.zookeeper WHERE path = '...'` → asserts a setting got persisted with the expected value.
  - Patch 042: `SELECT count() FROM system.zookeeper WHERE path = '...' AND name = '...'` → asserts a znode is absent (count=0) or present (count=1) after a state-modifying operation.

**Diagnosis:** the common shape is **"ZK state mutation triggered by SQL, observed via SQL."** When a patch's behavioral effect manifests in ZK metadata, the stateless test for it should default to `system.zookeeper` rather than reaching for integration-test infrastructure. This is significantly cheaper to author and run (single-shot SQL vs. multi-node cluster).

**Decision:** the recipe is at 2 of 3. **Do not yet codify** into `docs/aiven/runbooks/testing-suites.md` as a canonical recipe. **Track for the third occurrence**, then promote to VERIFIED with a dedicated subsection in the runbook citing patches 010 + 042 + (TBD).

**Rule-of-three count:** 2 of 3. Patch 010 (1/3), patch 042 (2/3). Patch 060 attempted to use the recipe via `ALTER TABLE` + `SELECT FROM system.zookeeper WHERE path = '...metadata'` but the test was rejected by `MergeTreeData::checkAlterIsPossible` (see retro 11) — that is NOT a counterexample to the recipe, it's a counterexample to "patch 060's source change is observable from `MergeTree` ALTER paths" (which is what retro 11 establishes as a *no-regression* property, not a recipe failure).

### B. `(iv) reachability check` discipline: rule-of-three 2 of 3 (proactive use)

**Symptom (positive — proactive application):** T3.13 escalated `test_design_blocked` because the test trigger never reached the patched code (`StorageMaterializedView.cpp:222-225` BAD_ARGUMENTS gate routed around `RefreshTask::RefreshTask` for non-replicated targets). The escalation surfaced the (iv) check as a NEW preflight dimension: **"does the test trigger reach the patched code on HEAD?"** — a distinct question from (iii) identifier inventory ("is the patched code on HEAD at all?"). T3.14 retro-applied (iv) by switching to `ReplicatedMergeTree()` target. T3.15 was the **first proactive use** — the parent's preflight at `tmp/patch-042/dispatch-prompt.md` wrote out the (iv) reachability proof BEFORE the worker ran.

**Diagnosis:** the (iv) check has two distinct sub-checks:
  - (iv-a) **Code-path reachability**: the test trigger's call path actually reaches the patched function body on HEAD (not gated by an upstream sanity check that returns early).
  - (iv-b) **Differential observability**: the test trigger produces a DIFFERENT observable outcome pre-patch vs. post-patch (i.e., the patched behavior is *demonstrable*, not silently fixed under default config).

T3.13 escalated on (iv-a). T3.15 verified both (iv-a) — `removeTableNodesFromZooKeeper` is on the DROP TABLE path for ReplicatedMergeTree — and (iv-b) — patch 042 fills the gap in `dropAncestorZnodesIfNeeded`'s early-return at `path_prefix_for_drop.empty()`, so a ZK path WITHOUT `{uuid}` produces the diverging observable (count=1 pre-patch, count=0 post-patch).

**Decision:** the discipline is at 2 of 3 of proactive use. **Mark PROVISIONAL** in the dispatch-prompt template. Codify once the third proactive use produces a similarly-decisive outcome. The most likely codification target is `docs/aiven/skills/dispatch-prompt-template.md` "Parent preflight checklist" section, sub-numbered as `(iv) Reachability proof` with the two sub-checks above.

**Rule-of-three count (proactive use):** 2 of 3. T3.14 retro-applied = 1; T3.15 proactive = 2; next dispatch is the calibration slot.

### C. Egyptian-vs-Allman brace style mismatch: Outcome 2 of the dispatch prompt's brace-style flag (first occurrence)

**Symptom:** Source patch `0d6eb5bece77` ships the new `dropAncestorTableZnodeIfNeeded` function body with **Egyptian braces** (`if (code == Coordination::Error::ZOK) {`). The local style check `ci/jobs/scripts/check_style/check_cpp.sh` rejects this style and requires **Allman braces** (opening brace on a new line). The CI's `Style check` job would have failed the patch as-is.

**Diagnosis:** The Aiven repo's style is Allman (per `AGENTS.md`). The upstream patch author shipped Egyptian, indicating either the upstream subdir had relaxed style at the time of the source SHA or the author bypassed style review. Independent of how it landed upstream, our local rule is Allman and the style check enforces it.

**Resolution:** Worker invoked the dispatch prompt's brace-style decision tree (Outcome 2): **reformat to Allman**, validate via Tier 2 decomposition that the change is whitespace-only. Worker ran the decomposition (`tmp/patch-042/patch-id-decomposition.log`):

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

Set `byte_equivalent: false` in the report YAML with the decomposition log as evidence. Tier 2 GREEN.

**Decision:** the dispatch-prompt template's brace-style flag (Outcome 1: pass-through; Outcome 2: reformat-to-Allman + decomposition) is empirically validated for the first time. Mark as 1 of 3. Track for next occurrence.

**Rule-of-three count:** 1 of 3. First explicit Outcome-2 in T3.x. Future calibration: if a third patch produces the same style mismatch, codify the Allman-reformat step into `docs/aiven/runbooks/build-and-test.md` as a checked sub-step of every cherry-pick.

### D. `spawn E2BIG` from `StrReplace` against `src/Storages/StorageReplicatedMergeTree.cpp` (first occurrence)

**Symptom:** Worker attempted to reformat the source's Egyptian brace via the editor's `StrReplace` tool. The pre-tool-use hook returned `spawn E2BIG` — the file is ~7400 lines and the `StrReplace` invocation exceeded the OS argv size limit. The worker pivoted to a Python helper script invoked via Shell to perform the byte-equivalent edit.

**Diagnosis:** `StrReplace` (the editor's exact-string-replacement tool) invokes a child process whose argument list contains the full `old_string` and `new_string`. For very large source files where the surrounding context required for uniqueness pushes the strings into the multi-kilobyte range, the argv limit (`ARG_MAX`, typically ~2 MB total) trips. The Python-via-Shell pivot bypasses this because the script reads/writes the file in-process and only the small Python script body goes through argv.

**Resolution:** Worker's pivot was correct and reproducible. The output is verifiable via decomposition (which Tier 2 then validated as whitespace-only).

**Decision:** add a "Known gotchas" subsection to `docs/aiven/runbooks/build-and-test.md` (or `integration-tests.md`) documenting the `E2BIG` workaround pattern: **when `StrReplace` fails on a large source file with `spawn E2BIG`, pivot to a Python script via Shell, verify byte-equivalence via decomposition.** Mark as 1 of 3 — codify after second occurrence.

**Rule-of-three count:** 1 of 3. First explicit `E2BIG` in T3.x. Track for next large-source-file edit.

### E. Hook v3 third regression delivered `unknown / unknown` for T3.15 too

**Symptom:** The `log.md` row at `2026-05-28T13:17:30Z` was originally populated as `unknown / unknown / n/a` despite the worker producing a healthy YAML report (confirmed by the report archive being writable via offline backfill).

**Diagnosis:** see retro 14 for the full diagnostic chain. Briefly: Cursor's `subagentStop` hook input contains `message_count: 0` and no assistant content, so the hook's body-extraction depends on reading the on-disk JSONL via `dirname(.transcript_path)/subagents/<youngest>.jsonl`. If the JSONL isn't fully flushed by the time the hook fires, the body-extraction returns empty and the row degrades to `unknown / unknown`.

**Resolution:** T3.15's row was backfilled offline from the on-disk JSONL (`29362a7f-552d-4803-ae0b-b2c1484a81ad.jsonl`); the report archive at `reports/toolu_01FigZuPjPKDHr5pMUFgpoQ6.md` carries `backfilled=true` and cites the source JSONL UUID.

**Forward signpost:** retro 14 consolidates the hook v3 third regression across T3.8-T3.15. T3.15 is one of three explicit instances of the regression manifesting (T3.13 + T3.14 + T3.15). The probe block landed in `acb4d88fc70` and removed after diagnosis in the current Phase A cleanup commit confirmed the JSONL-fallback as the permanent path.

## Forward decisions for T3.16+

- **`system.zookeeper` recipe**: keep PROVISIONAL at 2/3. Promote to VERIFIED after the third use. Do NOT eagerly codify into `testing-suites.md` yet.
- **`(iv) reachability` discipline**: keep PROVISIONAL at 2/3 of proactive use. Promote after third proactive use. The most natural codification target is `dispatch-prompt-template.md` "Parent preflight checklist" as a fourth bullet alongside (i)/(ii)/(iii).
- **Brace-style mismatch (Outcome 2)**: keep at 1/3. The dispatch-prompt template's brace-style flag worked correctly; document the outcome inline in retro 13 and track for the next dispatch.
- **`E2BIG` from `StrReplace`**: 1 of 3. Document the Python-via-Shell pivot inline in retro 13 and track for next large-source-file edit. Defer codification until n=3.

## Learning log

**Today you learned:** stateless tests that assert ZK state via `system.zookeeper` are the *cheapest* way to defend a patch when the patch's behavioral effect manifests in ZK metadata. Two shape variants exist (value-assertion vs. count-assertion) and both work cleanly. Reaching for integration-test infrastructure (which adds 10x author time and 100x runtime) is only justified when the trigger requires multi-node interaction (e.g., DDL replication, replica catch-up, leadership election) that single-node stateless tests cannot exercise.

**Rule of thumb:** when a patch's behavior is "X happens in ZK after a DDL operation," default to a stateless `9NNN_*.sql` test against `system.zookeeper` and only escalate to integration if the trigger requires it. The mental decision tree: (1) can a single SQL session reproduce the trigger? (2) is the observable effect inspectable via SQL (system tables, DDL output, query results)? If both yes → stateless. If either no → integration.

**Next rabbit hole:** the third `system.zookeeper` test will promote the recipe to VERIFIED. The most likely candidate is a future patch in the **DDL-coordination / quorum** family (e.g., `quorum_status`, `pending_mutations`, `replication_queue`) — these all live in ZK and are inspectable via `system.zookeeper` paths. Worth scanning the inventory for such patches when choosing T3.16.

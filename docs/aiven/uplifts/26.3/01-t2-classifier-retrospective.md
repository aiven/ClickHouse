# T2 — classifier dispatch retrospective

> **What this is:** The reflection step from spec §10 step 12, applied to the T2 classifier dispatches (initial T2.1 plus a re-run T2.2 forced by an upstream force-push). Captures what worked, what surfaced, and what changes the empirical data forces on T3+.
> **Dates:** T2.1 on 2026-05-22T12:24Z; T2.2 on 2026-05-22 (later, after `v25.8.18.1-lts-aiven` was force-pushed).
> **Dispatch references:** `docs/aiven/uplifts/26.3/log.md` row at `2026-05-22T12:24:05Z` (T2.1, subagent id `toolu_019bjhJRVovPjShrqdjWCitE`); T2.2 row appended on re-run.

## Headline

Both T2 classifier dispatches (`explore`, readonly) returned `outcome: success` well under the 10-minute budget. The dispatch shape (parent renders prompt → dispatches via `Task` → receives halt-and-escalate report → parent materializes the inventory file) is validated for read-only work, and survived an upstream force-push without code changes — the regeneration pattern is idempotent.

- **T2.1**: 77-row inventory at `v25.8.18.1-lts-aiven` tip `3f401037cfb`.
- **T2.2**: 78-row inventory at new tip `ea9c5d6420f` (force-pushed by release management on 2026-05-21 to rebuild the branch with proper `Author / Committer` distinction; one new patch landed in the rebuild).

## What worked

1. **AGENTS.md inlined verbatim**: the read-trigger workaround held across both dispatches. The subagent never had to read a separate file to obey Aiven invariants; the dispatch prompt had everything.
2. **`format-patch | apply --check` substitute for `cherry-pick --no-commit`**: zero working-tree mutation, identical "does this patch apply cleanly?" semantics, compatible with the `explore` subagent type. The 25/53/0 distribution (yes/no/error) in T2.2 mirrors T2.1 (25/52/0) modulo the new patch — stable signal.
3. **`tests.added: no_source_change` (new schema value)**: solved the contradiction where a read-only subagent had no compliant way to populate the `tests` block. The schema amendment was tested end-to-end on both dispatches.
4. **`subagentStop` hook fired**: a row landed in `docs/aiven/uplifts/26.3/log.md`. Day-1 observability is alive (see Finding D for limitations).
5. **Walking-skeleton discipline held**: we did NOT pre-create 78 dossier stubs. The inventory is a single file; per-patch dossiers will be born at dispatch time in T3+.
6. **Regeneration is idempotent**: T2.2 was a clean re-dispatch of the same prompt with `expected count: 77` swapped to `78`. No file mutations, no code changes — the inventory just regenerated against the new tip. This validates the design's "branch moves → re-dispatch" pattern as ergonomic enough to use casually.

## What surfaced

### A. 32% / 68% clean-vs-conflict split

The T2.2 classifier reports 25 patches that apply textually clean and 53 that would conflict against `v26.3.10.62-lts-aiven-dev`. This is the dominant signal (basically unchanged from T2.1's 25/52):

- **The cherry-pick-and-verify happy path is the exception, not the rule.** Most patches will require active conflict resolution.
- **Upstream-drift analysis (§14 Q6) is load-bearing.** The textual conflicts are themselves usually mechanical (line shifts, neighboring-code edits), but a patch that applies cleanly is NOT automatically semantically correct — and a patch that does NOT apply cleanly might still be the right thing once we understand WHY upstream changed.
- **The T3 dispatch prompt must front-load conflict-resolution and drift-analysis steps.** "Cherry-pick → run test" is insufficient as the per-patch lifecycle.

### B. Author/committer distinction — corrected understanding

> **Correction:** the original T2.1 retrospective claimed "author attribution is corrupted by chained ports". That was wrong, and the wrong text has been removed. Here is what's actually true.

T2.1 captured only one identity column (`author` = `git log --format=%ae`) and I (the parent agent) misread the GitHub UI's "X authored, Y committed" as meaning the column was confusing author with committer. It was not. `%ae` is the author email, full stop. The author count distribution was correct in T2.1 (63 tilman / 9 khatskevich / 4 joelynch / 1 vitlibar) and is essentially the same in T2.2 (63 / 10 / 4 / 1, the +1 being the new "Fix MV refresh task race condition" patch).

What the force-push between T2.1 and T2.2 actually changed is the **committer** (`%ce`) — not the author:

- **Before the rebuild** (T2.1, branch tip `3f401037cfb`): the committer on most SHAs was `tilman.moeller@aiven.io`, because tilman was the porter who landed the patches on the branch during the 25.3 → 25.8 cycle.
- **After the rebuild** (T2.2, branch tip `ea9c5d6420f`): committers are now `joelynch112@gmail.com` (57) and `alex.khatskevich@aiven.io` (21), reflecting whoever actually merged each patch into the release line. Authors were preserved through the rebuild (the rebase used `--committer-date-is-author-date`-like discipline; original author + author-date intact, committer + committer-date refreshed).

T2.2 adds a `committer` column to surface the distinction. Net effect for T3 workers: **`author` is the design owner** (ask them about intent); **`committer` is the integration owner** (ask them about the merge context, conflict resolution history, who reviewed). Both are useful, neither is "corrupt".

**Process correction for forward porting (still valid):** patches landing on `v26.3.10.62-lts-aiven-dev` during this uplift should preserve original authorship through `git cherry-pick --no-commit -x` + `git commit -c CHERRY_PICK_HEAD` (or equivalent), so the chain remains intact for the next LTS uplift. This is already in `docs/aiven/AGENTS.md` §5 conceptually; T3 dispatch prompt will include explicit commands.

### C. One upstream-authored patch in the range

Patch 074 (T2.2 sha `ab1fb1df41`, "Fix ArrowFlight support for IPv6 in listen_host.") is authored by `vitlibar@clickhouse.com` (an upstream ClickHouse maintainer) with an author date of **2025-09-04** — months before any other patch in the inventory. It sits in the `v25.8.18.1-lts-aiven` branch but its origin is upstream `master`, not aiven downstream work.

Implication for T3: when this patch comes up for porting, the FIRST thing the worker checks is whether upstream `v26.3.10.62-lts` already contains an equivalent fix (`git log v25.8.18.1-lts..v26.3.10.62-lts -- src/Server/.../ArrowFlight*`). If yes → drop the patch with `escalation_reason: policy_call` and a dossier note. The classifier did its job; T3 worker does the upstream-equivalence check.

### D. Observability gap in `subagentStop` hook

The log row shows `Outcome: unknown` and `Patch slug: unknown`. The hook didn't parse the YAML front-matter out of the subagent response. This is a known limitation (spec §14 Q5 anticipated it). For now: human-readable observability via the saved verbatim report at `tmp/classifier/report.md` (or, post-merge, via `docs/aiven/uplifts/26.3/inventory.md` itself).

**Decision deferred:** hook improvement is NOT done now. The fix shape per spec §14 Q5 — write the full halt-and-escalate report to a sibling file under `docs/aiven/uplifts/26.3/reports/` and log "where to find it" in `log.md` — should be implemented before T3.2 (the SECOND patch) so the first patch's report is captured properly and we don't accumulate `unknown`s.

### E. Pending merges and branch volatility

State at T2.1 dispatch:

- `khatskevich/mv_race_258` — 1 unique commit (pending)
- `khatskevich/peerdb_258` — 2 unique commits (pending)

State at T2.2 dispatch (after May 21 force-push):

- `khatskevich/mv_race_258` — **merged**, now row 078 ("Fix MV refresh task race condition", `ea9c5d6420`).
- `khatskevich/peerdb_258` — still unmerged, and now `git rev-list --count origin/v25.8.18.1-lts-aiven..origin/khatskevich/peerdb_258` shows **58** commits unique to it. That number is misleading — it reflects that `peerdb_258` sits on top of the OLD (pre-rebuild) branch line, which is no longer reachable from the production tip. Once `peerdb_258` is rebased onto the new tip, its truly-unique commit count will return to the original 2.

**Lesson:** the production branch can be force-pushed at any time (legitimately, by release management), and consumer branches (`khatskevich/*`) drift apart from it until rebased. The inventory must be re-generated on every relevant change; T6 documentation explicitly mentions checking inventory currency before starting a new T3 dispatch.

### F. Force-push triggers a clean regeneration

T2.2 was the first real test of "the upstream moved; what now?". The handling that worked:

1. `git fetch origin v25.8.18.1-lts-aiven` reported `(forced update)` — early signal that SHAs are about to change.
2. Author distribution diff (T2.1 vs T2.2) was the right sanity check: `+1 khatskevich` confirmed exactly one new patch; everything else was a committer change.
3. The dispatch prompt needed exactly one edit (count `77 → 78`); the rest of the prompt regenerated naturally.
4. The new inventory file rewrote cleanly with the same preamble structure plus a new "Classification history" section to track T2.1 vs T2.2.

This sequence (fetch → diff → edit prompt count → re-dispatch → re-materialize) is the durable pattern. It belongs in a future runbook (`docs/aiven/runbooks/refresh-inventory.md`) but does not warrant authoring now — Rule of Three; we'll write the runbook after the third regeneration if it happens.

## Concrete decisions for T3

| Decision | Driver |
|---|---|
| First T3 dispatch picks a `cherry_pick_clean=yes` patch with low LOC. T2.2 candidates: 007 (1 LOC, recoverLostReplica), 037 (2 LOC, sensors), 039 (8 LOC, thread fuzzer), 040 (6 LOC, replicas_status), 044 (2 LOC, curl ipv6). Final pick is a human decision; see `docs/aiven/patches/<NNN>-<slug>.md` once authored. | Walking-skeleton discipline |
| T3 dispatch prompt MUST include the upstream-drift analysis step (§14 Q6) with concrete commands, not just a reference. | T2 empirical 68% conflict rate |
| T3 dispatch prompt MUST instruct: every cherry-pick uses `git cherry-pick --no-commit -x` (the `-x` adds the `(cherry picked from commit <sha>)` provenance trailer; `--no-commit` keeps human as committer per AGENTS.md §5). Author preservation is then a `git commit -c CHERRY_PICK_HEAD` for the human at the end. | T2 finding B (author preservation forward) |
| T3 dispatch prompt MUST include the C++ / database review checklist (§14 Q7) with content authored for this first dispatch. | T2 finding (binary goes to production) |
| Before T3.2, fix the `subagentStop` hook to write the report file and log a real outcome value. | T2 finding D |
| Re-run the classifier (cheap, idempotent) whenever `v25.8.18.1-lts-aiven` is force-pushed or gains commits. | T2 finding F (regeneration is the cheaper-than-tracking strategy) |

## What this retrospective is NOT

- Not a refactor of T0/T1 (those are done; they served their purpose).
- Not a re-classification of the inventory (the inventory is mechanically correct; we just need to read it with the author/committer distinction in mind).
- Not a list of which 10 patches to port (Epic 1 ships 10 patches as system-validation; the SEQUENCE is decided one at a time at the reflection step after each port).

## Pointers

- Inventory: `docs/aiven/uplifts/26.3/inventory.md`
- Classifier report (verbatim, T2.2): `tmp/classifier/report.md` (scratch, not committed)
- Dispatch prompt (verbatim, T2.2): `tmp/classifier/dispatch-prompt.md` (scratch, not committed)
- Plan: `docs/aiven/plans/2026-05-22-classifier-subagent.md`
- Spec sections affected by this retrospective: §13 (risks), §14 Q6 (drift), §14 Q5 (subagentStop fix), §14 Q7 (C++ checklist).

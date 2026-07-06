# T3.8 — patch 001 drop retrospective

> **What this is:** The reflection-after-cost step (spec §10 step 12) applied to the eighth real patch dispatch — the first patch to be **dropped** (`obsoleted-by-upstream`) rather than carried, and the first patch where the T3.7 Finding A `(i)/(ii)/(iii)` parent-preflight discipline produced a decisive outcome **without consuming any worker time**.
> **Date:** 2026-05-27.
> **Dispatch reference:** `docs/aiven/uplifts/26.3/log.md` row at `2026-05-27T09:37Z` (subagent id reconstructed from on-disk JSONL `53d78719-fd0c-44b1-bad3-03d8cdb130c2.jsonl`; no auto-archived report — this dispatch was a parent-only preflight, not a worker run).
> **Outcome of T3.8:** `dropped`. Two files committed in `298f4639d11` (no source change): `docs/aiven/patches/001-advertise-host-from-config.md` (new dossier, 112 lines, status `obsoleted-by-upstream`) and a row in `docs/aiven/uplifts/26.3/inventory.md` with strikethrough annotation. No tests added. No cherry-pick committed. Total wall-clock: ~10 minutes.

## Headline

T3.8 was the **first not-carried outcome** in the T3.x series. Parent preflight, applying the `(i)/(ii)/(iii)` discipline introduced in T3.7 Finding A, identified within 10 minutes that Aiven patch 001 (`ac84fa6f7c`, "Advertise host from config for replicated databases", 2025-12-02) is semantically-identical to upstream commit `9dd658aea06` ("Fix DatabaseReplicated to respect interserver_http_host config", `xiaohuanlin <xiaohuanlin1993@gmail.com>`, 2025-10-10, GitHub issue #88361), which is already an ancestor of `v26.3.10.62-lts`. **The patch is unnecessary for 26.3.**

The discipline saved ~25 minutes of worker dispatch time — the prior workflow (T3.1-T3.7) would have dispatched a worker, who would have hit a context-window-shift conflict at minimum and then run the same upstream-equivalent search before reaching the same conclusion.

**`(i)/(ii)/(iii)` discipline: 1 of 3 of the WORKING mitigation.** First explicit empirical confirmation that writing out the three checks (patched-line stability, context-window stability, semantic-equivalence on HEAD) produces a decisive result that the inventory's `cherry_pick_clean=no` flag alone does not.

## What worked in T3.8

1. **The `(i)/(ii)/(iii)` discipline caught `obsoleted-by-upstream` BEFORE any worker dispatch.** Parent's preflight steps from `docs/aiven/patches/001-advertise-host-from-config.md §2`:
   - **(i) Patched-line stability FAILED:** `sed -n '124,134p'` on HEAD showed lines 124-134 are an unrelated `FailPoints` namespace block; the patched `getHostID` function moved from line 126 to line 140 of `src/Databases/DatabaseReplicated.cpp` between LTSes.
   - **(ii) Context-window stability FAILED:** `diff <(git show ac84fa6f7c^:...) <(...)` over the patch's `@@ -126,7 @@` window produced completely divergent context — heavy file-level churn (1,177 commits between `v25.8.18.1-lts` and `v26.3.10.62-lts` touched this file).
   - **(iii) Semantic-equivalence on HEAD TRUE:** `rg -n -A 6 'static inline String getHostID'` showed lines 140-145 already contain `auto host_port = global_context->getInterserverIOAddress();` — the patch's post-image, structurally rewritten to use an intermediate variable.

   Discipline (i)+(ii) together signaled "this is not a simple textual cherry-pick"; discipline (iii) escalated to "AND it doesn't need to be one." The combination unlocked the upstream-equivalent search (`git log -S 'getInterserverIOAddress' --reverse`) which surfaced `9dd658aea06` as the upstream equivalent.
2. **Upstream-equivalent search produced a citable SHA + GitHub issue link.** The dossier records `9dd658aea06` with a `git merge-base --is-ancestor 9dd658aea06 v26.3.10.62-lts` → `YES` verification. Audit trail is durable: a future maintainer asking "why didn't 26.3 carry patch 001?" reads §2 of the dossier and gets the answer in two minutes.
3. **Convergent-fix story is recorded for institutional memory.** The two patches were authored independently for different motivations (Aiven cited containerized environments where the system hostname is `pod-abc123`; upstream cited node-replacement scenarios in GH #88361), but landed the same semantic change. Upstream landed first by ~2 months but did not propagate to `v25.8` (which had already branched). The Aiven patch was needed for 25.8; it becomes redundant for 26.3. This is the **canonical shape** for the `obsoleted-by-upstream` classification — Aiven-first + upstream-second + upstream-eventual-equivalence.
4. **Inventory annotation convention introduced.** The inventory row for 001 was annotated with strikethrough + status note. The preamble of `docs/aiven/uplifts/26.3/inventory.md` gained a new "Status annotations" subsection documenting the convention so future inventories are consistent.
5. **No worker time consumed.** Parent-only preflight resolved the patch. The throughput win vs the implied T3.x workflow ("every patch costs one worker dispatch") is significant — at ~25 min saved per dropped patch and an inventory of 80+ patches with unknown drop rate, even a 10% drop rate saves ~3.5 hours of worker time over the uplift.

## What surfaced

### A. `(i)/(ii)/(iii)` discipline rule-of-three: 1 of 3 (WORKING) and 1 of 3 (DECISIVE without worker)

**Symptom (positive — the mitigation worked):** T3.7 Finding A introduced the discipline as a defensive measure against the parent's "should apply cleanly" prose contradicting the inventory's `cherry_pick_clean` column. T3.8 was the **first explicit application** in a preflight: writing out (i)/(ii)/(iii) as three independent checks instead of collapsing them into one. The discipline produced **a different outcome than the prior workflow would have** — it caught `obsoleted-by-upstream` from the parent seat instead of mid-worker.

**Symptom (calibration — a new shape):** T3.7's discipline was framed as a defense against false-positive "should apply cleanly" predictions (where the inventory said `clean=no` but prose said clean). T3.8 added a NEW dimension: the discipline can also produce a **NO-DISPATCH decision** when (i) and (ii) fail simultaneously and (iii) confirms upstream-equivalence. The (i)+(ii)+(iii) tri-test is therefore more general than its T3.7 framing — it's a four-state classifier:

  - (i)=pass, (ii)=pass, (iii)=pass → **clean cherry-pick expected**; dispatch.
  - (i)=pass, (ii)=fail, (iii)=pass → **context drift but semantically still-needed**; dispatch with explicit context-resolution instructions.
  - (i)=fail, (ii)=fail, (iii)=pass → **heavy drift but semantically still-needed**; dispatch with hunk-by-hunk re-targeting.
  - (i)=fail, (ii)=fail, (iii)=upstream-already → **`obsoleted-by-upstream`**; drop, no dispatch. ← T3.8.

**Decision:** add to `docs/aiven/skills/dispatch-prompt-template.md` "Parent preflight checklist" the four-state classification. Mark as PROVISIONAL — promote to VERIFIED after the second decisive-classification outcome (a second `obsoleted-by-upstream` drop or a similarly non-trivial classification).

**Rule-of-three count (WORKING discipline):** 1 of 3. First explicit empirical application. T3.7 introduced; T3.8 applied to a real decision; T3.9/T3.10/T3.11/... will calibrate.

**Rule-of-three count (DECISIVE-without-worker outcome):** 1 of 3. First patch dropped via parent-only preflight. If a second patch follows the same pattern within the next ~10 dispatches, codify as a discipline outcome alongside "dispatch" and "escalate" in the parent's playbook.

### B. `obsoleted-by-upstream` is a first-class outcome that needs schema support

**Symptom:** The inventory's row schema (`files_count`, `loc_changed`, `cherry_pick_clean`, `subject`) has no column for "patch was dropped, never carried, citation goes here." The current ad-hoc strikethrough + status note solves it for one patch but won't scale.

**Diagnosis:** The implicit assumption in T2 classifier and the inventory schema is "every patch in the source LTS will be carried to the target LTS." T3.8's drop violates that. The dossier model (`docs/aiven/patches/<slug>.md` with a §2 Upstream-drift findings section) accommodates it cleanly, but the inventory does not.

**Decision:** add a `status` column to `docs/aiven/uplifts/26.3/inventory.md` schema with the value set: `pending | ported | dropped-obsoleted-by-upstream | dropped-no-longer-applies | dropped-out-of-scope`. Defer the schema change to a Bootstrap commit; track for the next inventory edit.

**Rule-of-three count:** 1 of 3. First explicit non-`ported` outcome in this uplift's inventory. T3.9 and later will calibrate whether the column extension is essential or whether the dossier-status alone is enough.

### C. Heavy file-level churn (1,177 commits between LTSes) is no longer rare

**Symptom:** `src/Databases/DatabaseReplicated.cpp` saw 1,177 commits between `v25.8.18.1-lts` and `v26.3.10.62-lts`. The `getHostID` function moved 14 lines and gained an intermediate `host_port` variable, but its semantics under the patched call site are unchanged.

**Diagnosis:** the parent's prior heuristic — "small patches against unchanging files are cheap to dispatch" — is no longer reliable. Heavy churn files require the `(i)/(ii)/(iii)` discipline as a precondition for any dispatch prediction.

**Decision:** when the parent preflight detects high commit-count drift in the patched file (heuristic threshold: >300 commits between LTSes per the affected file), prefer to write the (i)/(ii)/(iii) block in full prose rather than abbreviate it. Defer codifying the threshold until n=3.

**Rule-of-three count:** 1 of 3. First explicit high-churn file in the T3.x series.

## Forward decisions for T3.9+

- **The (i)/(ii)/(iii) discipline is now PROVISIONALLY part of every parent preflight.** Promote to MANDATORY after T3.10 or T3.11, depending on whether it produces n=2 or n=3 of the working mitigation.
- **The four-state classification (clean / drift / heavy-drift / obsoleted-by-upstream) is documented in this retro.** Codify into `dispatch-prompt-template.md` if T3.9 produces a second non-clean outcome.
- **The `status` column extension for the inventory is deferred.** Bring up at the Bootstrap squash step (Phase D in the packaging plan) so it lands as one cohesive inventory-schema commit alongside the strikethrough convention.

## Learning log

**Today you learned:** the `(i)/(ii)/(iii)` discipline is not just a defense against false-positive "clean" predictions — it's a four-state classifier that can produce a NO-DISPATCH decision when (i)+(ii) fail and (iii) reveals upstream-equivalence. This is the first observed `obsoleted-by-upstream` outcome in the T3.x series.

**Rule of thumb:** before any worker dispatch, write out (i)/(ii)/(iii) as three independent checks. If (i) and (ii) both fail, do NOT collapse into "dispatch the worker and let them figure it out" — instead, escalate (iii) to a full upstream-equivalent search (`git log -S '<signature symbol>' --reverse`) and decide drop-vs-carry from the parent seat. Worker time is the expensive resource; parent preflight time is the cheap one.

**Next rabbit hole:** the dossier's §4 raises a latent concern: neither the upstream nor the Aiven patches ship a regression test. If Aiven cares about defending this behavior across future rebases (e.g., when 26.3 → 27.x), the right move is an integration test setting `<interserver_http_host>` to a sentinel and asserting `system.zookeeper` reflects it. Defer until the THIRD occurrence of "behavior covered only by upstream merge, no regression test" — that's the trigger for codifying the integration-test gap-filling pattern as a discipline.

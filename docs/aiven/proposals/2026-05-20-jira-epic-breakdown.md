# Jira ticket breakdown — Aiven LTS uplift acceleration + 26.3 release

> **Document type:** Project-management breakdown of the approved design.
> **Date:** 2026-05-20.
> **Status:** Draft for creating Jira tickets. Tickets are created in Jira before any execution starts.
> **Source of truth:** This document is derived from the design spec at `docs/aiven/proposals/2026-05-19-uplift-orchestration-design.md` (§16 "Epic structure and session boundary"). If this document and the spec disagree, the spec wins; reconcile by editing this document.

## How to use this document

1. Skim the **Overview** to understand the two-epic structure.
2. For each ticket, copy the title, description, scope, and acceptance criteria into a new Jira Story.
3. Set the parent Epic, size estimate, and dependencies per the table at the end of each epic section.
4. The estimates are rough until T3 (first patch) lands; refine sizing of T4/T5/E2-* after T3 produces real per-patch data.

## Overview

Two epics, executed sequentially, in two separate Cursor/Superpowers sessions.

| # | Epic | Scope | Session |
|---|---|---|---|
| 1 | Accelerate ClickHouse LTS uplift with AI | Build the orchestration system + port N=10 patches end-to-end through it as system validation | Current session (this brainstorm + execution) |
| 2 | Add 26.3 to Aiven ClickHouse | Port remaining ~60 patches + fast-forward release branch + tag release | New session, opened after Epic 1 retrospective lands |

**Why split this way:**
- Epic 1 is *system construction + validation*. It produces both durable infrastructure AND a partial 26.3 uplift (patches 1–10). The 10 patches are not "extra scope" — they are the empirical evidence that the system works.
- Epic 2 is *execution at scale* of the validated system. Mostly mechanical; the parent agent's role shrinks to a thin dispatcher. The session boundary is deliberate (see spec §16.3): context hygiene + mode shift + the committed system being the handoff substrate.

**Common conventions for both epics:**

- Issue type: **Story** for each ticket below (each "T" = one Story under the parent Epic).
- Labels: `aiven-lts-uplift`, `v26.3`, plus per-ticket labels noted in the ticket.
- Component: as your team uses; suggest `Forks` or `Backports` if available.
- Sizing scale: t-shirt (XS / S / M / L / XL).
- DoD = Definition of Done; every Story has explicit checkable AC.
- Every ticket references the spec and (where applicable) the implementation plan by path. If the spec or plan changes, link the change PR in the ticket.

---

## Epic 1: Accelerate ClickHouse LTS uplift with AI

### Goal

Build the smallest orchestration system that lets one Cursor `Task` subagent forward-port one ClickHouse patch with a fresh context, halt-and-escalate exit contract, durable per-patch dossiers, safety hooks on irreversible git operations, and a human-only commit gate. Validate the system end-to-end by porting N=10 patches.

### Why N=10 (the headline target)

- N=3 = Rule-of-Three minimum. Validates the system works at all.
- N=10 = enough patch-shape variety (clean cherry-pick, conflict, settings-touching, build-system, security, IO, etc.) to confirm the system is not tuned to the first three patches. Enough to extract 1–2 stable skill files. Enough to produce meaningful retrospective metrics for Epic 2 estimation.
- If patches 6–8 show clear stability (no new artifacts written, escalation rate flat, time-per-patch flat), Epic 1 can finish at N=8. If patches 8–9 are still surfacing new artifacts, extend to N=12. The number is a target, not a hard wall.

### Definition of Done (Epic 1)

The Epic is Done when ALL of the following are true:

1. Bootstrap commit landed on `v26.3.10.62-lts-aiven-dev` with parent = `v26.3.10.62-lts` (upstream tag).
2. Env validation evidence recorded (Task 0 of T1).
3. 6 hook scripts present + smoke-tested (18/18 assertions pass).
4. Classifier subagent dispatched successfully (T2); report returned in valid halt-and-escalate schema.
5. Patches 1–10 each have: a dossier at `docs/aiven/patches/<NNN>-<slug>.md`, a human-made commit on `v26.3.10.62-lts-aiven-dev`, a row in `docs/aiven/uplifts/26.3/log.md`. **Every commit has either a new test that demonstrably exercises the patch (with pre-patch-fail / post-patch-pass evidence) OR a documented no-test justification naming an existing upstream test by path** (spec §2 invariant #7).
6. At least one skill file extracted under `docs/aiven/skills/` (Rule-of-Three trigger fired at N=3).
7. `docs/aiven/uplifts/26.3/00-introduction.md` and `docs/aiven/README.md` written; the introduction document is sized to brief a fresh Epic 2 session (handoff test: an engineer with no access to this brainstorm can dispatch patch 11 reading only the introduction + log).
8. Retrospective committed: median time/patch, tool-call distribution, escalation rate, escalation-reason histogram, named refinements for Epic 2.
9. Spec refinements (if any) merged.

### Tickets (Epic 1)

---

#### T0 — Land the design spec and the bootstrap plan

**Type:** Task (or Story, if your team prefers)
**Size:** XS
**Depends on:** —
**Labels:** `aiven-lts-uplift`, `v26.3`, `planning`

**Description:**
Commit the approved design spec and the bootstrap implementation plan to a Jira-linked branch so subsequent tickets reference durable on-disk content rather than chat history. This is the only ticket whose deliverable is "files committed to git" without code execution.

**Scope:**
- Review the spec at `docs/aiven/proposals/2026-05-19-uplift-orchestration-design.md`.
- Review the plan at `docs/aiven/plans/2026-05-20-bootstrap-orchestration.md`.
- Review this Jira-breakdown document.
- Commit these three documents to a feature branch off `v26.3.10.62-lts-aiven-dev` and open a PR (or commit directly if your team's convention allows).

**Acceptance criteria:**
- [ ] All three documents are committed to git.
- [ ] The commit message references this Jira ticket ID.
- [ ] At least one reviewer (human) has approved the spec.

**References:**
- `docs/aiven/proposals/2026-05-19-uplift-orchestration-design.md`
- `docs/aiven/plans/2026-05-20-bootstrap-orchestration.md`
- This document.

---

#### T1 — Bootstrap the orchestration system

**Type:** Story
**Size:** M (1–3 days of focused work; includes Task 0 env validation and 18-assertion smoke test)
**Depends on:** T0
**Labels:** `aiven-lts-uplift`, `v26.3`, `infra`

**Description:**
Install the orchestration scaffolding so subsequent patch dispatches have an exit contract, safety hooks, and observability. Critically, **validate that the dev environment can build and run tests BEFORE any code changes happen** (Task 0 of the plan) — bootstrapping a system that cannot validate patches is worthless.

**Scope:**
- **Task 0:** Validate build + test infrastructure on the current branch tip: confirm `build/programs/clickhouse` exists and runs; run 3 smoke `clickhouse local --query` queries; `ninja -n` dry-run; compile a small snippet via `.claude/tools/cppexpr.sh`. Document evidence at `tmp/bootstrap/env-validation.md`.
- **Task 1:** `git reset --hard v26.3.10.62-lts` (upstream tag); verify untracked files survive.
- **Tasks 2–11:** Author `docs/aiven/AGENTS.md` (8 items including testing invariant), `docs/aiven/schema/halt-and-escalate.md` (worker exit contract with `tests` block), `.cursor/hooks.json` + 6 hook scripts (G1, G2, G3, G4, G5, G7 + `subagentStop` observability).
- **Task 12–13:** chmod scripts; write and run `tmp/bootstrap/smoke-test.sh` (18 assertions across all hooks).
- **Task 14:** Show staged state to human; propose commit message; HUMAN commits.

**Acceptance criteria:**
- [ ] `tmp/bootstrap/env-validation.md` exists and shows all 4 env checks PASS.
- [ ] HEAD = `v26.3.10.62-lts` immediately before the bootstrap commit; the commit's parent = the tag.
- [ ] 18/18 hook smoke tests pass.
- [ ] `docs/aiven/AGENTS.md` has 8 sections (including §7 testing invariant); ≤100 lines.
- [ ] `docs/aiven/schema/halt-and-escalate.md` includes the `tests` block in the YAML form AND constraint #6 (tests-block satisfaction for `outcome: success`).
- [ ] `.cursor/hooks.json` is valid JSON and references all 6 hook scripts.
- [ ] Bootstrap commit message names the design spec by path AND references the Task 0 env validation summary (clickhouse version).
- [ ] No other files committed (no patches, no README, no introduction doc).

**References:**
- Spec §10 (the bootstrap walkthrough) and §15 (acceptance criteria).
- Plan: `docs/aiven/plans/2026-05-20-bootstrap-orchestration.md`.

---

#### T2 — Validate dispatch with classifier subagent (lightweight inventory only)

**Type:** Story
**Size:** S (half-day to one day)
**Depends on:** T1
**Labels:** `aiven-lts-uplift`, `v26.3`, `validation`

**Description:**
Exercise the subagent dispatch / halt-and-escalate / report cycle on a low-stakes read-only task before trusting it with a real cherry-pick. The classifier reads the 78-commit inventory from the prior LTS (`origin/v25.8.18.1-lts-aiven` — current production aiven LTS line, per Aiven release-management decision 2026-05-22; count includes the May 21 force-push that landed `khatskevich/mv_race_258`) and returns a **lightweight, mechanical** table per patch — metadata only, no judgment calls. The deep judgments (testability, semantic-conflict likelihood) are made fresh at dispatch by T3/T4/T5's workers, not pre-computed here. See spec §10 step 6 for the rationale (walking-skeleton applied to the inventory).

**Output columns** (mechanical only):

| Column | Source | Note |
|---|---|---|
| `NNN` | sequential | Stable numeric prefix; assigned by chronological introduction date |
| `sha` | `git rev-parse <commit>` | full SHA |
| `date` | `git log --format=%ai` | author date |
| `author` | `git log --format=%ae` | original patch author (preserved through cherry-picks) |
| `committer` | `git log --format=%ce` | person who landed the SHA on `v25.8.18.1-lts-aiven` (added in T2.2 after the May 21 force-push made the author/committer distinction meaningful and useful) |
| `files_changed` | `git show --stat` | count |
| `loc` | `git show --stat` | insertions + deletions |
| `subject` | `git log --format=%s` | first-line commit subject |
| `cherry_pick_clean` | `git format-patch -1 <sha> --stdout \| git apply --check` (readonly equivalent — `cherry-pick` mutates the index, incompatible with `explore` subagent) | yes/no/error |

Explicitly **NOT** in the table:
- `testability` — judgment call, made per-dispatch by each patch's worker.
- `subsystem` — too coarse to be useful as a sortable column; subsystem inferred from `files_changed` when picking.
- `conflict-risk` (deep) — `cherry_pick_clean` is its mechanical proxy; deeper analysis happens at dispatch.

**Scope:**
- Dispatch a Cursor `Task` subagent with `subagent_type: explore` (read-only).
- Inputs to the subagent: `git log v25.8.18.1-lts..origin/v25.8.18.1-lts-aiven` (the tag-to-aiven range; NOT `v26.3.10.62-lts..origin/v25.8.18.1-lts-aiven` which would include ~866 upstream stable backports), the halt-and-escalate schema, the column list above.
- Output: inventory table committed at `docs/aiven/uplifts/26.3/inventory.md` AND a halt-and-escalate report in the subagent's final response.
- Verify: report conforms to schema (no manual handwave); the `subagentStop` hook logged a row to `docs/aiven/uplifts/26.3/log.md`.

**Acceptance criteria:**
- [ ] Inventory table committed at `docs/aiven/uplifts/26.3/inventory.md` with 78 rows and all 9 columns above (including `committer`, added in T2.2 after the force-push made author/committer distinction meaningful).
- [ ] `cherry_pick_clean` is yes/no for every row (no blanks); the count of `yes` is recorded in the dispatch report as a sanity signal.
- [ ] Subagent's final response conforms to the halt-and-escalate schema (YAML front matter parses; `outcome: success` justified by non-empty deliverable).
- [ ] `docs/aiven/uplifts/26.3/log.md` has one row from the `subagentStop` hook.
- [ ] First-patch candidate identified (human + parent agent agree on which patch is T3's target). Prefer the smallest patch with `cherry_pick_clean: yes`.

**References:**
- Spec §10 step 6 (rationale for lightweight classifier).
- Spec §14 open-question #3 (column-set additions if T3 reveals gaps).

---

#### T3 — Port first patch end-to-end (the walking skeleton)

**Type:** Story
**Size:** M (1–3 days; high variance because we are learning the workflow)
**Depends on:** T2
**Labels:** `aiven-lts-uplift`, `v26.3`, `patch`

**Description:**
Walking-skeleton patch: pick the simplest candidate from T2's classifier table (low LOC, simple subsystem, has a clear testability story), hand-author its dossier with the §6 strawman, dispatch the worker subagent, review the result, human commits. This ticket exists to **drive out unknowns**: what's missing from the dossier template, the dispatch prompt, the schema, the AGENTS.md. The post-execution reflection is the source-of-truth for what artifacts the system actually needs.

**Scope:**
- Pick the first patch from the classifier table.
- Hand-author the dossier at `docs/aiven/patches/<NNN>-<slug>.md` with: lineage, rationale, expected porting steps, expected risk, **Testing section** mirroring the schema's `tests` block.
- Compose the dispatch prompt: include AGENTS.md (auto-loaded), the dossier path, the procedure (cherry-pick `--no-commit` + tier-1/2/3 verification + test design + report).
- Dispatch a `generalPurpose` subagent.
- Receive the halt-and-escalate report; review staged state including any new test under `tests/queries/0_stateless/`.
- HUMAN commits per the proposed commit message.
- Append row to `docs/aiven/uplifts/26.3/log.md`.
- **Reflect:** what artifact's absence cost time? Write that artifact NOW (don't carry it forward). Likely candidates: a missing AGENTS.md item, a clearer schema field, a runbook entry, the patch-dossier template.

**Acceptance criteria:**
- [ ] 1 commit lands on `v26.3.10.62-lts-aiven-dev` for the first patch.
- [ ] Dossier at `docs/aiven/patches/<NNN>-<slug>.md` is committed with the Testing section filled in.
- [ ] If `tests.added: yes`: the dossier and the commit message include the pre-patch-fail / post-patch-pass evidence.
- [ ] If `tests.added: no_justified`: the dossier names an existing upstream test by path AND the worker has confirmed that test passes against the staged patch.
- [ ] `docs/aiven/uplifts/26.3/log.md` has a new row.
- [ ] Reflection artifact written (could be empty if the cycle truly revealed nothing — but the absence is documented in the dossier's notes).

**References:**
- Spec §10 steps 7–11 and §11 (the reflection deferred-items table).

---

#### T4 — Port patches 2–3, extract the first skill file

**Type:** Story
**Size:** M
**Depends on:** T3
**Labels:** `aiven-lts-uplift`, `v26.3`, `patch`, `rule-of-three`

**Description:**
Two more patches via the same workflow as T3. At N=3, the Rule-of-Three trigger fires: the procedure embedded into the dispatch prompts has now been used three times; whatever is duplicated identically across all three prompts gets extracted into the first skill file under `docs/aiven/skills/` (likely `docs/aiven/skills/cherry-pick.md`). Skill content is **only** what was actually duplicated — do not invent generality.

**Scope:**
- Pick patches 2 and 3 from the classifier table.
- Repeat T3's workflow for each.
- After patch 3 lands: diff the three dispatch prompts; extract the identically-duplicated procedure into `docs/aiven/skills/<name>.md`; commit it.
- Subsequent dispatch prompts (T5+) reference the skill file by path instead of inlining the procedure.

**Acceptance criteria:**
- [ ] 2 additional commits land on `v26.3.10.62-lts-aiven-dev` (3 total since bootstrap).
- [ ] Dossiers committed for patches 2 and 3 with Testing sections filled in.
- [ ] First skill file committed under `docs/aiven/skills/`.
- [ ] Log shows 3 rows total.
- [ ] Reflection note: if NO skill emerges (the three prompts diverged), that's a falsification signal; document why in the log and re-evaluate.

**References:**
- Spec §2 invariant #6 (walking-skeleton + Rule of Three).
- Spec §11 deferred-items table.

---

#### T5 — Port patches 4–10

**Type:** Story
**Size:** L (1–2 weeks; the largest ticket; absorbs variance)
**Depends on:** T4
**Labels:** `aiven-lts-uplift`, `v26.3`, `patch`

**Description:**
Seven more patches via the now-stable workflow, ideally with declining per-patch wall-clock as skills accumulate. Patches in this ticket should cover patch-shape variety: at least one clean cherry-pick, at least one with a conflict, at least one settings-touching, at least one with a non-trivial test-shape problem. The classifier table helps pick variety; do not just take the easiest seven.

**Scope:**
- Pick patches 4–10 from the classifier table, biased toward variety.
- Same workflow as T3/T4 for each.
- If the same artifact-absence is observed twice in a row, extract a second skill file (the Rule-of-Three threshold is in spirit a tripwire, not a rigid number).
- If escalation rate exceeds 30% (more than 2 of 7 patches escalate), open a sub-task to address the escalation pattern before proceeding.

**Acceptance criteria:**
- [ ] 7 additional commits land on `v26.3.10.62-lts-aiven-dev` (10 total since bootstrap).
- [ ] Dossiers committed for patches 4–10 with Testing sections filled in.
- [ ] Log shows 10 rows total.
- [ ] Per-patch median time is computed from the log and recorded.
- [ ] Escalation-reason histogram available for T7.
- [ ] At least N=10 OR clear "stable at N=8" rationale documented (per Epic 1 scope flexibility note).

**References:**
- Spec §16.1 (Epic 1 scope flexibility).
- Spec §13 (risks: walking-skeleton discipline tripwire).

---

#### T6 — Write introduction document + README

**Type:** Story
**Size:** S (half-day to one day)
**Depends on:** T5
**Labels:** `aiven-lts-uplift`, `v26.3`, `documentation`

**Description:**
With 10 patches of real data, write the introduction document and README. **These are written AFTER the patches, not before** — they are evidence-driven from what patches 1–10 revealed about onboarding gaps. The introduction document is the handoff substrate for the Epic 2 session (a new Cursor session must be able to dispatch patch 11 reading only these two files + the log + the committed system).

**Scope:**
- Write `docs/aiven/uplifts/26.3/00-introduction.md` covering: current branch state, where the log lives, what skills exist, how to pick the next patch, how to dispatch.
- Write `docs/aiven/README.md` as the human entry point: what this directory is, where to start.
- Test the handoff: walk through the introduction document as if you had no other context. Patches still to be ported (11–71) should be dispatchable purely from the document.

**Acceptance criteria:**
- [ ] `docs/aiven/uplifts/26.3/00-introduction.md` committed.
- [ ] `docs/aiven/README.md` committed.
- [ ] Handoff test passes: a peer (or you-with-fresh-mind) can read the introduction and identify the next dispatchable patch + how to dispatch it.

**References:**
- Spec §11 (deferred-items table: introduction and README triggers).
- Spec §16.3 (session boundary: handoff verification).

---

#### T7 — Retrospective + Epic 2 handoff

**Type:** Story
**Size:** S
**Depends on:** T6
**Labels:** `aiven-lts-uplift`, `v26.3`, `retrospective`

**Description:**
Empirical close-out of Epic 1. Produce the retrospective document with measured per-patch data; draft the Epic 2 starting prompt; identify and merge any spec refinements observed during execution; close the Epic 1 Cursor session.

**Scope:**
- Compute from the log: median time/patch, p95 time/patch, tool-call distribution, escalation rate, escalation-reason histogram.
- Compare against historical (pre-AI) per-patch baseline if available.
- Name 1–5 refinements for Epic 2 (e.g., "the postToolUse counter is needed because subagentStop logging missed X").
- Draft the Epic 2 starting prompt (will live at the top of the new session); save it at `docs/aiven/uplifts/26.3/epic-2-starting-prompt.md`.
- If any spec section is now obviously wrong: open a sub-task to merge the refinement.
- Last step: close this Cursor session.

**Acceptance criteria:**
- [ ] Retrospective document committed at `docs/aiven/uplifts/26.3/retrospective-epic-1.md`.
- [ ] Epic 2 starting prompt drafted and committed.
- [ ] Spec refinements (if any) merged.
- [ ] Epic 1 Jira Epic closed with a link to the retrospective.

**References:**
- Spec §16.3 (session boundary).
- Spec §14 (open questions to revisit).

---

### Epic 1 dependency graph

```
T0 -> T1 -> T2 -> T3 -> T4 -> T5 -> T6 -> T7
```

Strictly sequential. Cannot parallelise without violating the spec's "one patch at a time, observe carefully" invariant.

### Epic 1 sizing roll-up

| Ticket | Size |
|---|---|
| T0 | XS |
| T1 | M |
| T2 | S |
| T3 | M |
| T4 | M |
| T5 | L |
| T6 | S |
| T7 | S |

**Wall-clock estimate (low confidence until T3 produces data):** 2–4 weeks. T5 is the buffer; if the system stabilises early, T5 finishes faster; if T4 reveals an escalation pattern that needs structural fixes, T5 absorbs the cost.

---

## Epic 2: Add 26.3 to Aiven ClickHouse

### Goal

Port the remaining ~60 patches (those not covered by Epic 1's N=10), fast-forward the release branch after sign-off, tag the release.

### Definition of Done (Epic 2)

1. All 71 inventoried patches either ported (with dossier + commit + log row) OR formally deferred (`status: dropped-in-v26.3` recorded in the dossier with rationale).
2. `v26.3.10.62-lts-aiven` release-line branch fast-forwarded to the end of `v26.3.10.62-lts-aiven-dev` after human sign-off.
3. Tag `v26.3.10.62-lts-aiven` exists at the sign-off commit.
4. Epic 2 retrospective: cycle-time comparison against Epic 1 (and historical baseline if available).

### Initial ticket sketch (refined by T7)

These tickets are **drafts** — the actual Epic 2 ticket structure is determined by T7's retrospective once we know per-patch median time and escalation patterns from Epic 1. Sizing is XS until we have data.

- **E2-T1** — Continue porting (patches 11–~70). Size: XXL placeholder; will be split into N batches of 10–15 patches by T7. Each batch is a separate ticket.
- **E2-T2** — Fast-forward `v26.3.10.62-lts-aiven` release-line branch. Size: XS.
- **E2-T3** — Tag `v26.3.10.62-lts-aiven` release. Size: XS.
- **E2-T4** — Epic 2 retrospective. Size: S.

### Epic 2 session notes

Epic 2 starts in a **new Cursor / Superpowers session**. The current session is closed after T7. Spec §16.3 documents the why:

1. Context hygiene (this brainstorm + Epic 1 dispatches are clutter for Epic 2).
2. The committed system is the handoff substrate (`docs/aiven/AGENTS.md`, schema, hooks, skills, the introduction document, the log).
3. Mode shift (Epic 1 is design + learning; Epic 2 is execution at scale).

The Epic 2 starting prompt (drafted in T7) is on the order of:

> Read `docs/aiven/uplifts/26.3/00-introduction.md` and `docs/aiven/uplifts/26.3/log.md`. Dispatch the classifier subagent on the unported patches. Propose the next patch and its dossier for human approval.

That single prompt is enough because Epic 1 built the system that does the work.

---

## Cross-epic notes

### What is NOT in either epic

- **Test infrastructure overhaul.** We assume the existing `tests/queries/0_stateless/` framework is good enough. If T3 reveals it isn't, that's a sub-task on T3, not a separate epic.
- **CI integration.** Each commit goes through CI as normal; we do not change the CI pipeline as part of this work.
- **Automated regression testing of patches.** Per-patch tier-3 verification covers it; we do not build a separate matrix.
- **The fork's overall release process beyond tagging.** Once the tag is in place, downstream release engineering takes over.

### Risks to flag in Jira

1. **Patch 1 (T3) may reveal that the schema is wrong.** If so, T3's reflection produces a spec PR that lands before T4. This is expected and not a blocker; budget for it.
2. **Test-shape difficulty for patches gating on shared error codes.** Spec §11 has a deferred skill (`test-shape.md`); if a worker hits `test_design_blocked`, the skill is extracted earlier than the N=3 trigger.
3. **Cursor subagent dispatch limits or instability.** Mitigation: spec §13 risks table; observability hooks capture per-dispatch wall-clock so we can detect degradation.
4. **The Epic 1 → Epic 2 handoff document (T6) is incomplete.** Mitigation: the handoff test in T6's acceptance criteria.

### When in doubt

The spec at `docs/aiven/proposals/2026-05-19-uplift-orchestration-design.md` is the source of truth. This Jira document translates the spec into PM format; it does not extend the spec. If a question arises that this document doesn't answer, check the spec.

---

## Reviewer checklist (before tickets are created in Jira)

- [ ] Spec §16 sizing rationale (N=10, scope flexibility) is acceptable.
- [ ] Epic-1 ticket boundaries make sense for our sprint cadence.
- [ ] T5's "L" sizing absorbs sufficient variance for the unknown.
- [ ] T7's retrospective deliverables are concrete enough to be checked.
- [ ] Epic-2 sketch is acceptable as a placeholder pending T7 data.
- [ ] Risks are tracked (Jira sub-tasks or labels as your team uses).

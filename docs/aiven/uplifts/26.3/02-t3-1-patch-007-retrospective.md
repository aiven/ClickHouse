# T3.1 — patch 007 dispatch retrospective

> **What this is:** The reflection-after-cost step (spec §10 step 12) applied to the first real patch dispatch. Captures the system's first encounter with an `irrelevant-by-removal` upstream-drift outcome, the schema gap it surfaced, and what changes the data forces on T3.2+.
> **Date:** 2026-05-22.
> **Dispatch reference:** `docs/aiven/uplifts/26.3/log.md` row at the corresponding `subagentStop`; subagent id `5fb41651-b731-4129-933b-1d93ee3f0aca`.
> **Outcome of T3.1:** `escalate / policy_call / irrelevant-by-removal`. Patch 007 dropped by human decision (commit `d2ee1c7e656`).

## Headline

The first real patch dispatch validated the system end-to-end without writing a single line of C++. Patch 007 was textually clean (`cherry_pick_clean=yes` from T2.2) but **semantically dead in 26.3** — upstream commit `034e9714a63` ("Remove QPL and QAT", 2025-12-15) removed the feature this patch was protecting. The worker stopped at Step 1 (upstream-drift analysis), produced a 228-line dossier documenting the finding, and proposed DROP. Wall time: ~15 minutes against a 60-minute budget.

This is the canonical case the §14 Q6 (upstream-drift analysis) decision was made for. **Empirically validated on patch one.**

## What worked

1. **The embedded procedure halted at exactly the right step.** Step 1 (drift analysis) is supposed to fire BEFORE Step 2 (cherry-pick) precisely so we don't ship semantically-dead patches; the worker did not run `git cherry-pick` at all.
2. **Author preservation by design, not by accident.** The proposed commit message included author, committer-on-source, and the `Co-authored-by` trailer of the original 25.8 commit — even though the patch is being DROPPED. Future uplifts that look at the 26.3 history will find the full chain even for non-ports.
3. **Schema `no_source_change` value held up under a new scenario.** It was authored for read-only classifier subagents; the patch worker re-used it for a write-capable "drift blocked the port" case. The reinterpretation was defensible enough that we kept it and amended the schema rather than rejecting the worker's report (see Finding B).
4. **Worker independently surfaced a production concern.** The user-upgrade question ("what happens to existing `DEFLATE_QPL`-compressed parts on a 25.x → 26.3 upgrade?") was not part of the dispatch prompt; the worker raised it because the codec opcode is now reserved as "Removed, don't reuse". This is the kind of orthogonal-but-relevant finding the system should encourage.
5. **Independent verification by parent agent was easy.** All four key claims (removal-commit ancestry, setting status, codec opcode, factory registration) were a single `git grep` / `git merge-base` each. The dossier's "Commands run" section in §2 doubles as a re-runnable proof.
6. **`opus-4-7-thinking-xhigh` was the right model choice for T3.1.** The drift reasoning required tracing upstream commit semantics across two LTSes, distinguishing "removal of the feature surface" from "equivalent fix to the recovery path", and recognizing that `MAKE_OBSOLETE` makes `setSetting` a documented no-op. Lower-cost models could be used for T4+ on well-understood patches.

## What surfaced

### A. `cherry_pick_clean=yes` is **insufficient** evidence of portability

T2.2 marked 25 of 78 patches as `cherry_pick_clean=yes`. The classifier's check was a textual one: `git format-patch -1 <sha> --stdout | git apply --check`. This passes when the anchor lines (surrounding context) still exist in the target tree. It says nothing about whether the *target* of the patch's effect is still present.

Patch 007 is the canonical example: the two anchor lines (`database_replicated_allow_explicit_uuid = 3` and `database_replicated_allow_replicated_engine_arguments = 3`) are unchanged at `src/Databases/DatabaseReplicated.cpp:1558-1559` in 26.3, so the diff applies textually. But the *thing the patch enables* (`enable_deflate_qpl_codec`) was removed.

**Decision for the inventory preamble:** Add a one-paragraph warning that `cherry_pick_clean=yes` only certifies textual portability, NOT semantic relevance. Future workers should read the dossier (§2) before treating a "clean" patch as a quick win. **Updated in this retrospective's commit.**

**Decision for the T3.x dispatch prompt template:** Step 1 (upstream-drift) commands must include a check for each *identifier the commit message names*, not just identifiers in the diff. The current patch-007 prompt happened to encode the right command (`git grep -n 'enable_deflate_qpl_codec' v26.3.10.62-lts -- 'src/Core/Settings.cpp' 'src/Core/SettingsChangesHistory.cpp' 'src/Compression/'`) because the prompt author read the commit body. The next patch's prompt needs an explicit rule: "extract identifiers from the source commit's body; for each, run `git grep <id> v26.3.10.62-lts` and confirm the identifier still has a non-obsolete definition."

### B. Schema gap: `tests.added: no_source_change` ambiguity

The schema (constraint 6, third sub-case) said `proposed_commit.staged_files` MUST be empty for `tests.added: no_source_change`. The worker had `staged_files: [docs/aiven/patches/007-...md]` — the dossier — because the dossier IS the deliverable of an `irrelevant-by-removal` outcome.

Two readings:

- **Strict:** the worker is in schema violation; its report is malformed; we re-dispatch.
- **Spirit:** "source change" means files affecting the compiled binary or tests, not docs. The dossier is a documenting commit; no source changed.

We adopted the spirit reading, and amended the schema in this retrospective's commit:

```diff
- `proposed_commit.staged_files` MUST also be empty in this case
- (consistency check).
+ `proposed_commit.staged_files` MUST contain at most:
+   (a) zero entries (read-only subagent: classifier, validator, etc.); OR
+   (b) one or more dossier files under `docs/aiven/patches/`
+       (a write-capable patch worker whose upstream-drift analysis
+       concluded `irrelevant-by-removal` or `obsoleted-by-upstream`).
```

The amendment also defines "source" explicitly (under `src/**`, `tests/**`, `programs/**`, `base/**`, `utils/**`) so the next edge case has a clear test.

This is the design's first "reflection-after-cost" amendment to a schema. Working as intended.

### C. Worker misread `git status` porcelain

The worker said the working tree contained "submodule pointer drift" in `contrib/**`. Looking at the actual `git status --short` output, those lines start with ` ? ` (space, question-mark, space) — a single `?` plus the path is **untracked** in porcelain v1. Modified submodules would be ` M ` (space, M, space).

This is a minor compatibility bug in the worker's heuristic, NOT a behavior issue (the worker proceeded correctly). For future dispatches: the preflight step could pre-grep for `^[ MAD]` (the meaningful prefixes) rather than relying on the worker's mental model of porcelain semantics. Minor; queue for the T3.x template improvement.

### D. The `subagentStop` log row is still `Outcome: unknown`

Already known from T2 (Finding D in that retrospective). The hook does not parse YAML front-matter. For T3.1 the human-readable outcome is recorded in:

- The dossier `docs/aiven/patches/007-recover-lost-replica-deflate-qpl-setting.md` (§6).
- The commit message of `d2ee1c7e656`.
- The inventory row 007 (now a markdown link to the dossier).

Decision (re-affirming the T2 retrospective): fix the hook before T3.2. Per spec §14 Q5, the fix is: write the verbatim report to `docs/aiven/uplifts/26.3/reports/<subagent-id>.md` (with stripped sensitive content) and log its path + parsed outcome in `log.md`. Estimated effort: 30–60 minutes; do before dispatching T3.2.

### E. Production concern recorded (out of scope for this dispatch)

The worker surfaced: users upgrading from 25.x to 26.3 with on-disk parts compressed by `DEFLATE_QPL` cannot read those parts in 26.3 (opcode `0x99` reserved as "Removed, don't reuse"). This is an **upstream** behavior, not introduced by dropping patch 007. The human will investigate separately. Recording here so the link isn't lost: it's a release-management blocker question, not a patch-uplift question.

## Concrete decisions for T3.2+

| Decision | Driver |
|---|---|
| **Pick the next patch from T2.2's `cherry_pick_clean=yes` set, but no longer treat that as "low risk".** Candidates remaining: 037 (2 LOC, sensors), 039 (8 LOC, thread fuzzer), 040 (6 LOC, replicas_status), 044 (2 LOC, curl ipv6). | T3.1 finding A |
| **T3.x dispatch prompt template must include explicit "extract identifiers from commit body; grep each in v26.3.10.62-lts" instructions in Step 1.** No more relying on the human prompt-author to encode them. | T3.1 finding A |
| **Fix `subagentStop` hook before T3.2.** Write full report to `docs/aiven/uplifts/26.3/reports/<subagent-id>.md`; log path + parsed outcome. | T2 finding D + T3.1 finding D (now twice observed) |
| **Schema amendment shipped in this retrospective's commit** (constraint 6, third sub-case, allows dossier-only `staged_files` for write-capable workers on `irrelevant-by-removal` / `obsoleted-by-upstream`). | T3.1 finding B |
| **Inventory preamble updated to warn against treating `cherry_pick_clean=yes` as a green flag.** | T3.1 finding A |
| **`opus-4-7-thinking-xhigh` is the default model for T3.x until empirical data shows we can step down.** Wall time was 25% of budget; the quality of reasoning was excellent. | T3.1 finding (model worked) |

## What this retrospective is NOT

- Not a re-litigation of the DROP decision — that decision is recorded in `d2ee1c7e656` and is final.
- Not a refactor of T1/T2 — those are done.
- Not the production-impact investigation for on-disk `DEFLATE_QPL` data — that's a separate ticket (Finding E).
- Not a rule-of-three abstraction — we have ONE patch port. Three is the trigger for refactoring the dispatch prompt into a reusable template; we're at 1.

## Pointers

- Dossier: `docs/aiven/patches/007-recover-lost-replica-deflate-qpl-setting.md`
- Drop commit: `d2ee1c7e656`
- Skills (committed pre-dispatch): `docs/aiven/skills/cpp-review-checklist.md`, `docs/aiven/skills/patch-dossier-template.md` (commit `ca431efda31`)
- Dispatch prompt (verbatim, scratch): `tmp/patch-007/dispatch-prompt.md`
- Worker scratch logs: `tmp/patch-007/drift-file-oneline.txt`, `drift-func-log.txt`, `setting-grep.txt`, `qpl-grep.txt`, `qpl-grep-broad.txt`
- Schema (amended in this retrospective's commit): `docs/aiven/schema/halt-and-escalate.md` constraint 6
- T2 retrospective (predecessor): `docs/aiven/uplifts/26.3/01-t2-classifier-retrospective.md`
- Upstream removal commit: `034e9714a63 Remove QPL and QAT` (Robert Schulze, 2025-12-15)

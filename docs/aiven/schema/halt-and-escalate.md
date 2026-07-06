# Halt-and-escalate report schema (worker exit contract)

Every subagent dispatched in the Aiven LTS uplift system MUST return its final
response in this shape. The schema is the worker's exit contract; the parent
agent (and any tooling) reads structured fields from it.

## Form

````markdown
---
outcome: success | escalate
patch_slug: <slug-only, no NNN prefix>
source_sha: <full SHA on previous LTS, or empty if classifier subagent>
proposed_commit:
  staged_files:
    - <path>
    - <path>
  commit_message: |
    <subject MUST be `patch-port(NNN): <source subject>` (ship) or
     `patch-drop(NNN): <reason>` (drop), per commit-hygiene.md §1(C);
     then the body with the Original-author line and provenance trailers>
  byte_equivalent: true | false
tests:
  added: yes | no_justified | no_source_change | no_trigger_on_current_lts
  kind: stateless | integration | unit | upstream-existing | n/a
  paths:
    - tests/queries/0_stateless/<NNNNN>_<slug>.sql
    - tests/queries/0_stateless/<NNNNN>_<slug>.reference
  upstream_reference:
    - tests/queries/0_stateless/<existing_test>.sql
  pre_patch_fail_verified: true | false
  post_patch_pass_verified: true | false
  justification: |
    <required for added=no_justified (explain why no new test is feasible),
     for added=no_source_change (state the read-only role and that no
     source change was made in this dispatch), and for
     added=no_trigger_on_current_lts (cite the recount evidence file path
     and name the root cause of the no-trigger condition)>
escalation_reason: none | textual_conflict | semantic_conflict | build_fail_api_rename | test_fail_ambiguous | test_design_blocked | external_dependency | policy_call | other
---

## Tier results

- Tier 1 (textual cherry-pick): pass | fail | n/a — <one-line summary>
- Tier 2 (semantic patch-id):    pass | fail | n/a — <one-line summary>
- Tier 3 (build + test):         pass | fail | n/a — <one-line summary>

## Evidence

<commands run, exit codes, key log excerpts; max ~50 lines.
 For tests.added=yes: MUST include the pre-patch test FAIL output AND the
 post-patch test PASS output — this is the evidence-of-causation pair.>

## What I did

<narrative bullets: files touched, commands run, decisions made>

## Proposed next step

<for success: "Ready for human commit. Suggested: git commit -F <message-file>"
 (the proposed_commit.commit_message body MUST include the
 "Original author: <name> <email>, <date>." line — source-author preservation
 is via the message body, NOT via `git commit --author=` or `-c CHERRY_PICK_HEAD`;
 see `docs/aiven/skills/dispatch-prompt-template.md` for rationale);
 for escalate: a concrete suggested resolution or "need policy decision: <question>">
````

## Constraints

1. `outcome: success` requires ALL of:
   - non-empty `proposed_commit.staged_files`
   - all three tiers reporting `pass`
   - the `tests` block satisfied (constraint 6 below)

   The schema rejects "fake success".

2. `outcome: escalate` requires `escalation_reason != none` and a non-empty
   "Proposed next step".

3. `byte_equivalent: true` means `git patch-id` of the staged result matches
   `git patch-id` of the source commit. False means the cherry-pick reshaped
   (different patch-id) — this triggers `semantic_conflict` escalation unless
   the only difference is context lines (which the worker must verify by the
   decomposition runbook below).

4. `escalation_reason: other` is allowed but tracked. If `other` accumulates
   across N=3 patches, the enum is revised.

5. The worker does NOT commit. `proposed_commit.commit_message` is the
   verbatim message the human will pass to `git commit -F <file>`. The
   subject line MUST follow the `patch-port(NNN): <source subject>` form
   (ship) or `patch-drop(NNN): <reason>` form (drop) — see
   `docs/aiven/runbooks/commit-hygiene.md §1(C)`. The message body MUST
   include an `Original author: <name> <email>, <date>.` line; the local
   human becomes the author of record. Do NOT use `git commit --author=`
   or `git commit -c CHERRY_PICK_HEAD` — these would make the source author
   the author of record, bypassing the body-line policy. See
   `docs/aiven/skills/dispatch-prompt-template.md` for full rationale.

6. **`tests` block satisfaction for `outcome: success`:**
   - If `tests.added: yes`: `paths` MUST be non-empty AND both
     `pre_patch_fail_verified` and `post_patch_pass_verified` MUST be `true`.
     The Evidence section MUST include the actual command outputs proving
     the pre-patch fail and post-patch pass.
   - If `tests.added: no_justified`: `justification` MUST be non-empty AND
     `upstream_reference` MUST be non-empty (a path under
     `tests/queries/` or `tests/integration/`) AND the worker MUST have
     run that test against the staged patch state and observed it pass —
     evidence required in the Evidence section.
   - If `tests.added: no_source_change`: the worker made zero **source**
     changes in this dispatch. "Source" here means files under `src/**`,
     `tests/**`, `programs/**`, `base/**`, `utils/**`, or any other path
     that contributes to the compiled binary or to test fixtures. Files
     under `docs/aiven/patches/**` (per-patch dossiers) and `docs/aiven/`
     metadata are **not** source. `paths` MUST be empty,
     `upstream_reference` MUST be empty, `kind` MUST be `n/a`, both
     `pre_patch_fail_verified` and `post_patch_pass_verified` MUST be
     `false`, and `justification` MUST name the role/scenario producing
     the no-source-change outcome and explicitly state "no source change
     in this dispatch". `proposed_commit.staged_files` MUST contain at
     most:
       (a) zero entries (read-only subagent whose deliverable is a
           report only — classifier, validator, etc.); OR
       (b) one or more dossier files under `docs/aiven/patches/` (a
           write-capable patch worker whose upstream-drift analysis
           concluded `irrelevant-by-removal` or `obsoleted-by-upstream`
           and recorded the finding in a dossier instead of porting).
     This value is NOT a way for a patch worker to skip testing of an
     actual port — using this value when *any* file under `src/**` or
     `tests/**` is in `staged_files` is a schema violation.
   - If `tests.added: no_trigger_on_current_lts`: the patch ports a
     **correct defensive change** whose bug condition does not manifest
     on the current LTS, so the test designed against the trigger cannot
     produce an evidence-of-causation pair right now. Use this value when
     ALL of the following hold:
       (a) the source change is real and structurally sound (Tier 1 + Tier 2
           pass; build clean);
       (b) the worker designed a stateless test against the patch's defended
           condition (the file is shipped, `paths` non-empty);
       (c) the worker re-verified the absence of any trigger on the current
           LTS by re-deriving the trigger-set from ground truth (NOT from
           a parent's preflight figure) and persisting the recount evidence
           to `tmp/patch-<NNN>/` under a clearly-named file
           (e.g., `truly-missing-<context>.txt`);
       (d) the worker tried a reasonable extended candidate list against
           the pre-patch binary (≥ ~10 candidates across the relevant
           parameter range, e.g., compatibility values or input vectors)
           and recorded each result.
     In this case: `paths` MUST be non-empty (the forward-insurance test
     IS shipped); `pre_patch_fail_verified: false`; `post_patch_pass_verified: true`;
     `upstream_reference` empty; `justification` MUST cite the recount
     evidence file path AND name the root cause of the no-trigger condition
     (e.g., "all 740 names in `settings_changes_history` resolve via
     `Settings::has` on 26.3"); `proposed_commit.commit_message` MUST
     include a body line that explicitly notes "forward-insurance only —
     no evidence-of-causation pair on this LTS" and references the dossier
     section that documents the trigger absence.
     This value is NOT a generic escape hatch. It is reserved for
     patches whose defensive guard is correct in principle but
     vacuously-true on the current base. Using this value when ANY
     candidate in the worker's discovery loop triggered the pre-patch
     failure is a schema violation; in that case `tests.added: yes` with
     the triggering candidate is required.
   - Anything else MUST set `outcome: escalate` with
     `escalation_reason: test_design_blocked` and explain in
     "Proposed next step" what makes a meaningful test undesignable.

## Escalation reasons

- `textual_conflict`: tier 1 (cherry-pick) failed; conflict markers present in
  the working tree beyond identifier rename or whitespace.
- `semantic_conflict`: tier 1 passed but tier 2 failed — the cherry-pick
  reshaped the diff non-trivially. Manual audit required.
- `build_fail_api_rename`: tier 3 build failed because the patch references a
  symbol that has been renamed/removed/restructured in the new base.
- `test_fail_ambiguous`: tier 3 test failed and the worker cannot determine
  whether the failure is in the patch, the test, or upstream.
- `test_design_blocked`: the worker cannot design a test that demonstrably
  exercises the patch's specific change. Common cause: the patch gates on
  a shared error code (e.g. `SUPPORT_IS_DISABLED`) that upstream also
  throws for unrelated reasons, and the worker cannot construct a setup
  that distinguishes the two paths. Prefer this escalation over shipping
  a weak test.
- `external_dependency`: the patch redirects a vendored submodule in
  `.gitmodules` (or its pinned commit) to an **Aiven fork** whose required
  state does not yet exist — e.g. `contrib/aws` → Aiven's `aws-sdk-cpp` fork,
  which must first have the SDK bumped to a specific upstream version and the
  ~2 Aiven patches re-applied on top. Editing `.gitmodules` is allowed (it is
  not on the never-touch list), but the worker cannot build or verify against a
  fork ref that has not been prepared, and preparing the fork is **outside this
  checkout**. STOP — do NOT point `.gitmodules` at a not-yet-existing ref and do
  NOT attempt a partial build. In "Proposed next step" report, specifically:
  (1) which submodule/path; (2) the exact upstream version/tag the fork must be
  based on; (3) the Aiven patches that must be re-applied on top of it;
  (4) the target commit/ref the `.gitmodules` pointer should land on once the
  fork is ready. The human prepares the external fork first; the port resumes
  afterward. See the clause (vi) preflight screen in
  `docs/aiven/skills/dispatch-prompt-template.md`.
- `policy_call`: a decision is needed that the worker is not authorized to
  make. Two recurring shapes:
  - "should this patch be dropped because upstream now does the same thing?"
  - "this patch changes a *default* behavior with broad blast radius (e.g.
    auto-transforming `MergeTree` into `ReplicatedMergeTree`) — should it be
    gated behind a new default-disabled server setting (e.g.
    `enforce_table_replication`) rather than shipped on by default?" Do NOT
    ship a broad default-behavior change on by default, and do NOT try to
    prove safety by running the whole suite; escalate for the gating
    decision. See the clause (v) preflight screen in
    `docs/aiven/skills/dispatch-prompt-template.md`.
- `other`: anything not in the enum above. Include rationale in
  "Proposed next step".

## Patch-id decomposition runbook (tier 2)

When the staged result's patch-id differs from the source's patch-id, run:

```bash
git show <source-sha> > /tmp/src.patch
git diff --cached     > /tmp/new.patch
diff <(grep -E '^[-+]' /tmp/src.patch | grep -v '^[-+]\{3\}') \
     <(grep -E '^[-+]' /tmp/new.patch | grep -v '^[-+]\{3\}')
```

- **Empty diff** → only context lines shifted. Tier 2 GREEN. Set
  `byte_equivalent: false` (the patch-ids genuinely differ) but the
  one-line tier-2 note explains that decomposition shows the
  added/removed lines are identical.
- **Non-empty diff** → cherry-pick reshaped. Tier 2 FAIL. Set
  `escalation_reason: semantic_conflict`.

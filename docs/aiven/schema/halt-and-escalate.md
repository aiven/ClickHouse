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
    <verbatim message the human should use, including any provenance trailers>
  byte_equivalent: true | false
tests:
  added: yes | no_justified
  kind: stateless | integration | unit | upstream-existing
  paths:
    - tests/queries/0_stateless/<NNNNN>_<slug>.sql
    - tests/queries/0_stateless/<NNNNN>_<slug>.reference
  upstream_reference:
    - tests/queries/0_stateless/<existing_test>.sql
  pre_patch_fail_verified: true | false
  post_patch_pass_verified: true | false
  justification: |
    <only when added=no_justified; explain why no test is feasible>
escalation_reason: none | textual_conflict | semantic_conflict | build_fail_api_rename | test_fail_ambiguous | test_design_blocked | policy_call | other
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

<for success: "Ready for human commit. Suggested: git commit -c CHERRY_PICK_HEAD";
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
   verbatim message the human will pass to `git commit -c CHERRY_PICK_HEAD`
   (or `git commit -F <file>`).

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
- `policy_call`: a decision is needed that the worker is not authorized to
  make (e.g., "should this patch be dropped because upstream now does the
  same thing?").
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

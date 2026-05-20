# Aiven LTS Uplift Bootstrap Implementation Plan (T1)

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bootstrap the orchestration system per design spec §10 steps 0–5 and §15 acceptance criteria. Produce a single human-made commit on top of upstream tag `v26.3.10.62-lts` containing the durable system files (`docs/aiven/AGENTS.md`, schema) and the Cursor hook glue (`.cursor/hooks.json` + scripts). Before any of that, validate that the build + test infrastructure actually works on the current branch (Task 0) — bootstrapping a system that cannot validate patches is worthless.

**Architecture:** No code changes to ClickHouse source. All new files are either durable orchestration content under `docs/aiven/` or tool-glue under `.cursor/`. The bootstrap is a one-shot setup; subsequent patch dispatches are separate plans.

**Tech Stack:** bash 5+, jq, Cursor hooks.json schema v1, markdown, the existing ClickHouse `build/` and `.claude/tools/cppexpr.sh`.

**Policy notes** (read before executing):
- **Task 0 (env validation) runs on the current branch tip BEFORE Task 1 resets**. If Task 0 fails (build broken, binary missing, smoke queries fail), STOP and escalate to the human for env repair — do not proceed with the reset and bootstrap.
- This plan ships fewer scripts than the spec mentions in §9.2: the `postToolUse` tool-call counter is **deferred** to "observed need" — we ship `subagentStop` only and revisit if its data is insufficient. The plan still satisfies §15 because §15 requires "six hook scripts present", which matches the 5 guardrails (G1/G2 combined, G3, G4, G5, G7) plus the `subagentStop` log script.
- The agent never runs `git commit`. Each task ends in a `git add` step; the final task proposes the commit to the human, who runs `git commit` themselves.
- After Task 1 (reset), the spec file at `docs/aiven/proposals/2026-05-19-uplift-orchestration-design.md` and this plan file are preserved because they are untracked. Verify in Task 1 step 4 that they survive.

**Reference:** `docs/aiven/proposals/2026-05-19-uplift-orchestration-design.md` (the spec). All content below is the spec made executable.

---

## File map

### Created in this plan

- `docs/aiven/AGENTS.md` — always-on invariants, ~80 lines (8 items including the testing invariant)
- `docs/aiven/schema/halt-and-escalate.md` — worker exit contract, ~120 lines (includes `tests` block)
- `.cursor/hooks.json` — hook registry, ~50 lines
- `.cursor/hooks/deny-irreversible-git.sh` — G1, G2
- `.cursor/hooks/ask-destructive.sh` — G3
- `.cursor/hooks/deny-upstream-file-writes.sh` — G4
- `.cursor/hooks/deny-best-of-n-runner.sh` — G5
- `.cursor/hooks/deny-agent-commits.sh` — G7
- `.cursor/hooks/log-subagent-completion.sh` — observability
- `docs/aiven/uplifts/26.3/.gitkeep` — empty directory marker
- `docs/aiven/patches/.gitkeep` — empty directory marker
- `tmp/bootstrap/smoke-test.sh` — hook smoke tests (run-only, not committed)
- `tmp/bootstrap/env-validation.md` — Task 0 evidence (run-only, not committed; summarised in commit message)

### Modified in this plan

- None. The reset in Task 1 wipes all previously-staged work; everything created here is new on top of upstream tag.

### Preserved across the reset (must verify)

- `docs/aiven/proposals/2026-05-19-uplift-orchestration-design.md` (the spec)
- `docs/aiven/proposals/2026-05-fork-uplift-process-research.md` (prior research; can be deleted after spec is committed)
- `docs/aiven/proposals/2026-05-ai-assisted-fork-maintenance.md` (prior design; can be deleted after spec is committed)
- This plan file itself
- The two older proposals are kept for now as reference; T1's final commit may or may not include them per human's choice.

---

## Task 0: Validate build + test infrastructure (on current branch, BEFORE reset)

**Files:**
- Create: `tmp/bootstrap/env-validation.md` (run-only; not committed)

**Why this task runs first.** The whole orchestration system exists to validate patches by building and running tests. If the dev env's build or test runner is broken on the current branch tip, every subsequent patch dispatch will fail at tier-3 verification — the bootstrap would have shipped a system that cannot do its only job. Validating now, on a known-customized branch, is the cheapest catch.

**Pre-flight assumption:** the existing `build/` directory was produced from the current branch (`v26.3.10.62-lts-aiven-dev` at `e1c11930c28...` or its predecessor). The binary `build/programs/clickhouse` exists. If either is false, Step 0.1 catches it and STOPs.

- [ ] **Step 0.1: Detect build directory and binary**

```bash
test -f build/build.ninja && echo "ninja manifest: OK"
test -x build/programs/clickhouse && echo "clickhouse binary: OK"
file build/programs/clickhouse | head -1
```

Expected:
- `ninja manifest: OK`
- `clickhouse binary: OK`
- The `file` output identifies an ELF executable.

If any check fails, STOP. Record the failure in `tmp/bootstrap/env-validation.md` and escalate to the human:

> Cannot proceed with bootstrap. The dev environment lacks a configured `build/` directory or a built `clickhouse` binary. Human action: run `cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug` and `ninja -C build clickhouse`, then re-run Task 0.

- [ ] **Step 0.2: Verify binary runs and answers a basic query**

```bash
mkdir -p tmp/bootstrap
build/programs/clickhouse local --query 'SELECT 1 AS one' 2>&1 | tee tmp/bootstrap/clickhouse-smoke-1.txt
build/programs/clickhouse local --query 'SELECT version() AS version' 2>&1 | tee tmp/bootstrap/clickhouse-smoke-2.txt
build/programs/clickhouse local --query 'SELECT count() AS n_funcs FROM system.functions' 2>&1 | tee tmp/bootstrap/clickhouse-smoke-3.txt
```

Expected:
- Query 1 prints `1`.
- Query 2 prints a version string starting with `26.3.`.
- Query 3 prints an integer ≥ 100 (ClickHouse has hundreds of functions).

If any query errors or prints nothing, STOP and record the failure. Common causes: missing shared libraries, glibc mismatch, corrupted binary.

- [ ] **Step 0.3: Verify ninja can plan a build (dry-run)**

```bash
( cd build && ninja -n clickhouse 2>&1 | tee ../tmp/bootstrap/ninja-dry-run.txt | tail -3 )
```

Expected:
- One of: `ninja: no work to do.` (binary already up-to-date) OR a short list of files that would be built. No errors, exit 0.

If ninja errors (e.g., "missing file", "manifest error"), STOP. The build configuration is inconsistent with the source tree.

- [ ] **Step 0.4: Verify the compiler env via cppexpr.sh**

The `.claude/tools/cppexpr.sh` tool compiles a C++ snippet against the existing ClickHouse build. If it produces output, the compiler + headers + build deps are all functional.

```bash
.claude/tools/cppexpr.sh -i Core/Block.h 'OUT(sizeof(DB::Block))' 2>&1 | tee tmp/bootstrap/cppexpr-result.txt
```

Expected:
- Output contains a line like `sizeof(DB::Block) -> NN` where NN is some positive integer (typically a few hundred bytes).
- Exit code 0.

If cppexpr.sh errors, STOP. The compile env is broken and we cannot validate patches.

- [ ] **Step 0.5: Document the validation evidence**

Write `tmp/bootstrap/env-validation.md` with this exact content:

```markdown
# Bootstrap Task 0 evidence

**Run date:** $(date -u +%Y-%m-%dT%H:%M:%SZ)
**Branch:** $(git rev-parse --abbrev-ref HEAD)
**HEAD SHA:** $(git rev-parse HEAD)
**Build dir:** build/
**Compiler env validation:** cppexpr.sh succeeded (see cppexpr-result.txt)

## Smoke queries

| Query | Output | Status |
|---|---|---|
| SELECT 1 | (see clickhouse-smoke-1.txt) | PASS |
| SELECT version() | (see clickhouse-smoke-2.txt) | PASS |
| SELECT count() FROM system.functions | (see clickhouse-smoke-3.txt) | PASS |

## Ninja dry-run

See ninja-dry-run.txt. Result: <"no work to do" OR list of files>.

## Conclusion

Build + test infrastructure is functional on the current branch. Safe to proceed
with Task 1 (reset to upstream tag) and the bootstrap.
```

Generate it with shell substitution:

```bash
cat > tmp/bootstrap/env-validation.md <<EOF
# Bootstrap Task 0 evidence

**Run date:** $(date -u +%Y-%m-%dT%H:%M:%SZ)
**Branch:** $(git rev-parse --abbrev-ref HEAD)
**HEAD SHA:** $(git rev-parse HEAD)
**Build dir:** build/
**Compiler env validation:** cppexpr.sh succeeded

## Smoke queries

| Query | Output | Status |
|---|---|---|
| SELECT 1 | $(cat tmp/bootstrap/clickhouse-smoke-1.txt | tr '\n' ' ') | PASS |
| SELECT version() | $(cat tmp/bootstrap/clickhouse-smoke-2.txt | tr '\n' ' ') | PASS |
| SELECT count() FROM system.functions | $(cat tmp/bootstrap/clickhouse-smoke-3.txt | tr '\n' ' ') | PASS |

## Ninja dry-run

\`\`\`
$(cat tmp/bootstrap/ninja-dry-run.txt | tail -3)
\`\`\`

## Conclusion

Build + test infrastructure is functional on the current branch. Safe to
proceed with Task 1 (reset to upstream tag) and the bootstrap.
EOF
```

- [ ] **Step 0.6: Verify the evidence file exists and was written**

```bash
test -f tmp/bootstrap/env-validation.md && echo "evidence: OK"
wc -l tmp/bootstrap/env-validation.md
```

Expected:
- `evidence: OK`
- Line count between 20 and 40.

If the file is missing, STOP. Do not proceed to Task 1.

- [ ] **Step 0.7: Hand-off to Task 1**

`tmp/bootstrap/env-validation.md` is the scratch evidence. The bootstrap commit message (Task 14) will summarise its key facts in one line. Now proceed to Task 1.

---

## Task 1: Reset working tree to upstream tag

**Files:**
- Modify: working-tree state (no specific file)

**Pre-flight:** Verify you are on `v26.3.10.62-lts-aiven-dev` and that the only critical untracked file is the spec.

- [ ] **Step 1: Confirm current branch and untracked-file safety**

```bash
git rev-parse --abbrev-ref HEAD
ls -la docs/aiven/proposals/2026-05-19-uplift-orchestration-design.md
ls -la docs/aiven/plans/2026-05-20-bootstrap-orchestration.md
```

Expected:
- Current branch: `v26.3.10.62-lts-aiven-dev`
- Both files exist with non-zero size.

If either file is missing, STOP — the plan or spec needs to be re-created before reset.

- [ ] **Step 2: Reset to upstream tag**

```bash
git reset --hard v26.3.10.62-lts
```

Expected output: `HEAD is now at <abbrev-sha> <subject>` where `<subject>` is the upstream tag commit's subject. No errors.

- [ ] **Step 3: Verify reset**

```bash
git rev-parse HEAD
git rev-parse v26.3.10.62-lts
```

Expected: both commands print the same full SHA. They should also match.

- [ ] **Step 4: Verify untracked files survived**

```bash
test -f docs/aiven/proposals/2026-05-19-uplift-orchestration-design.md && echo "spec OK"
test -f docs/aiven/plans/2026-05-20-bootstrap-orchestration.md && echo "plan OK"
git status --short
```

Expected:
- `spec OK`
- `plan OK`
- `git status --short` shows untracked entries under `docs/aiven/` (the spec, the plan, and possibly the two older proposals if they weren't tracked at the tag).

- [ ] **Step 5: No commit (agent does not commit; nothing to commit yet)**

Nothing to stage; nothing to commit. Move to Task 2.

---

## Task 2: Create the directory skeleton

**Files:**
- Create: `docs/aiven/schema/.gitkeep` (placeholder until schema file added)
- Create: `docs/aiven/skills/.gitkeep`
- Create: `docs/aiven/runbooks/.gitkeep`
- Create: `docs/aiven/uplifts/26.3/.gitkeep`
- Create: `docs/aiven/patches/.gitkeep`
- Create: `.cursor/hooks/.gitkeep`

The `.gitkeep` placeholders ensure empty directories survive `git add`.

- [ ] **Step 1: Create directories**

```bash
mkdir -p docs/aiven/schema
mkdir -p docs/aiven/skills
mkdir -p docs/aiven/runbooks
mkdir -p docs/aiven/uplifts/26.3
mkdir -p docs/aiven/patches
mkdir -p .cursor/hooks
```

- [ ] **Step 2: Create `.gitkeep` placeholders**

```bash
touch docs/aiven/schema/.gitkeep
touch docs/aiven/skills/.gitkeep
touch docs/aiven/runbooks/.gitkeep
touch docs/aiven/uplifts/26.3/.gitkeep
touch docs/aiven/patches/.gitkeep
touch .cursor/hooks/.gitkeep
```

- [ ] **Step 3: Verify**

```bash
find docs/aiven .cursor/hooks -type d
```

Expected: lists all six directories above plus `docs/aiven` itself, `docs/aiven/proposals` and `docs/aiven/plans` (which already exist).

- [ ] **Step 4: Stage the placeholders (do not commit)**

```bash
git add docs/aiven/schema/.gitkeep docs/aiven/skills/.gitkeep docs/aiven/runbooks/.gitkeep docs/aiven/uplifts/26.3/.gitkeep docs/aiven/patches/.gitkeep .cursor/hooks/.gitkeep
git status --short
```

Expected: six `A` entries (added).

---

## Task 3: Author `docs/aiven/AGENTS.md`

**Files:**
- Create: `docs/aiven/AGENTS.md`

- [ ] **Step 1: Write the file**

Write `docs/aiven/AGENTS.md` with this exact content:

```markdown
# Aiven fork — agent invariants

> Auto-loaded into any agent (parent or subagent) that touches the `docs/aiven/` subtree.
> Read once; obey always. Procedures live elsewhere; this file is for invariants only.

## 1. Orientation

You are working on Aiven's downstream fork of ClickHouse. The upstream tag is
`v26.3.10.62-lts`. Aiven-side work happens on the `v26.3.10.62-lts-aiven-dev`
branch. The release-line branch `v26.3.10.62-lts-aiven` is fast-forwarded only
after human sign-off; never act on it.

## 2. Branch invariants

- Only act (read or write source files) when HEAD is on a `*-aiven-dev` branch.
- Never commit to `master`, `main`, or `v*-aiven` (release-line).
- If you find yourself on the wrong branch, STOP and report.

## 3. Never-touch list (upstream-owned)

Do not Write or Edit any file matching:

- `.claude/**`
- root `AGENTS.md` (this `docs/aiven/AGENTS.md` is fine to read; not to edit)
- `CONTRIBUTING.md`
- `.github/workflows/**`
- `contrib/**`

These paths are upstream-owned and must merge cleanly on the next LTS rebase.
Hooks enforce this; the rule here is for your mental model.

## 4. Git operations

- Do not use `git rebase`, `git commit --amend`, `git push --force`, or
  `git push -f`. Add new commits instead.
- Hooks deny these; if you observe a deny, do not retry — STOP and report.

## 5. Agent does not commit

- Use `git cherry-pick --no-commit` to stage changes from a source commit.
- Use `git add` to stage new files (e.g., a new stateless test).
- DO NOT run `git commit`. The hook denies it.
- Your success state is "everything verified and staged". The human runs
  `git commit` themselves.

## 6. Halt-and-escalate contract

If you hit any condition you cannot resolve mechanically — any non-trivial
conflict, any build failure naming a renamed upstream symbol, any test failure
you cannot explain — STOP and return your final response in the format
specified at `docs/aiven/schema/halt-and-escalate.md`.

Even on success, your final response must conform to that schema.

## 7. Tests are required

Every patch you port MUST satisfy one of:

(a) **A new test that fails on the parent commit and passes after the patch.**
    Default is a stateless test under `tests/queries/0_stateless/`. Integration
    tests are appropriate only when the behavior is genuinely cluster-level.
    Your halt-and-escalate report's Evidence section MUST include both
    command outputs: the pre-patch run showing the test FAIL, and the
    post-patch run showing the test PASS. This is the evidence-of-causation
    pair.

(b) **A documented justification naming an existing upstream test.**
    Acceptable only when the patch is build-system-only, a config rename,
    or upstream behavior is already exercised by a test you identify by
    path. The dossier records the existing test's path; the report sets
    `tests.added: no_justified` and explains.

Aiven patches often gate on shared error codes (`SUPPORT_IS_DISABLED`,
`BAD_ARGUMENTS`, etc.). A test that merely asserts the error code is
insufficient — upstream may throw the same code for unrelated reasons. Your
test must demonstrably distinguish the Aiven gate: use an object/setup that
the Aiven gate rejects but for which the upstream gate (if any) would NOT
fire, and assert both error_code AND a substring of the error message
specific to the Aiven check.

If you cannot design such a test after reasonable effort, escalate with
`test_design_blocked` — silently shipping no test is forbidden.

## 8. Navigation

- Inventory and per-uplift work log: `docs/aiven/uplifts/<version>/`
- Durable per-patch dossiers: `docs/aiven/patches/<NNN>-<slug>.md`
- The schema for your exit report: `docs/aiven/schema/halt-and-escalate.md`
- Skills you may be asked to invoke (procedural details): `docs/aiven/skills/`
- The design spec (background): `docs/aiven/proposals/2026-05-19-uplift-orchestration-design.md`

Your work product per patch:

1. Staged source changes (via `git cherry-pick --no-commit` and `git add`),
   including any new test file under `tests/queries/0_stateless/`.
2. An updated dossier at `docs/aiven/patches/<NNN>-<slug>.md` with the
   Testing section completed (test paths, pre/post verification, or
   no-test justification with upstream-test reference).
3. A halt-and-escalate report in your final response, with the `tests`
   block populated.
```

- [ ] **Step 2: Verify**

```bash
wc -l docs/aiven/AGENTS.md
test $(wc -l < docs/aiven/AGENTS.md) -le 100 && echo "size OK" || echo "size FAIL"
grep -c "^## " docs/aiven/AGENTS.md
```

Expected:
- `wc -l` reports a value between 80 and 100.
- `size OK`
- `grep -c "^## "` reports 8 (eight sections; testing is §7, navigation moved to §8).

- [ ] **Step 3: Stage**

```bash
git add docs/aiven/AGENTS.md
```

---

## Task 4: Author `docs/aiven/schema/halt-and-escalate.md`

**Files:**
- Create: `docs/aiven/schema/halt-and-escalate.md`
- Remove: `docs/aiven/schema/.gitkeep` (no longer needed once schema file exists)

- [ ] **Step 1: Write the file**

Write `docs/aiven/schema/halt-and-escalate.md` with this exact content:

````markdown
# Halt-and-escalate report schema (worker exit contract)

Every subagent dispatched in the Aiven LTS uplift system MUST return its final
response in this shape. The schema is the worker's exit contract; the parent
agent (and any tooling) reads structured fields from it.

## Form

```markdown
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
```

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
````

- [ ] **Step 2: Verify**

```bash
wc -l docs/aiven/schema/halt-and-escalate.md
test $(wc -l < docs/aiven/schema/halt-and-escalate.md) -ge 110 && echo "size OK" || echo "size FAIL"
grep -c "^## " docs/aiven/schema/halt-and-escalate.md
```

Expected:
- `wc -l` reports a value between 110 and 140.
- `size OK`
- `grep -c "^## "` reports 4 (Form, Constraints, Escalation reasons, Patch-id decomposition runbook — the inner `## Tier results`, `## Evidence`, etc. inside the fenced code block may also match; if grep counts >4, that's fine, the spurious matches are inside fences).

- [ ] **Step 3: Stage and remove placeholder**

```bash
git rm --cached docs/aiven/schema/.gitkeep 2>/dev/null || rm -f docs/aiven/schema/.gitkeep
git add docs/aiven/schema/halt-and-escalate.md
```

---

## Task 5: Author `.cursor/hooks.json`

**Files:**
- Create: `.cursor/hooks.json`

- [ ] **Step 1: Write the file**

Write `.cursor/hooks.json` with this exact content:

```json
{
  "version": 1,
  "hooks": {
    "beforeShellExecution": [
      {
        "command": ".cursor/hooks/deny-irreversible-git.sh",
        "failClosed": true
      },
      {
        "command": ".cursor/hooks/ask-destructive.sh",
        "failClosed": false
      },
      {
        "command": ".cursor/hooks/deny-agent-commits.sh",
        "failClosed": true
      }
    ],
    "preToolUse": [
      {
        "command": ".cursor/hooks/deny-upstream-file-writes.sh",
        "matcher": "Write|Edit",
        "failClosed": true
      }
    ],
    "subagentStart": [
      {
        "command": ".cursor/hooks/deny-best-of-n-runner.sh",
        "matcher": "best-of-n-runner",
        "failClosed": true
      }
    ],
    "subagentStop": [
      {
        "command": ".cursor/hooks/log-subagent-completion.sh",
        "failClosed": false
      }
    ]
  }
}
```

- [ ] **Step 2: Verify JSON**

```bash
jq . .cursor/hooks.json > /dev/null && echo "JSON OK"
jq -r '.hooks | keys[]' .cursor/hooks.json
```

Expected:
- `JSON OK`
- Lists the four event types: `beforeShellExecution`, `preToolUse`, `subagentStart`, `subagentStop`.

- [ ] **Step 3: Stage**

```bash
git add .cursor/hooks.json
```

---

## Task 6: Author `deny-irreversible-git.sh` (G1, G2)

**Files:**
- Create: `.cursor/hooks/deny-irreversible-git.sh`

- [ ] **Step 1: Write the file**

```bash
cat > .cursor/hooks/deny-irreversible-git.sh <<'EOF'
#!/usr/bin/env bash
# Aiven LTS uplift hook: deny irreversible git operations.
# Covers G1 (push), G2 (rebase, force-push).
# Amend is denied separately by deny-agent-commits.sh (G7).
set -euo pipefail

input=$(cat)
command=$(echo "$input" | jq -r '.command // empty')

# Force-push (any form): deny.
if [[ "$command" =~ ^git[[:space:]]+push ]] && [[ "$command" =~ (--force|-f([[:space:]]|$)) ]]; then
  echo '{"permission":"deny","agent_message":"Force-push denied (G2). AGENTS.md forbids rewrite-history; add new commits instead."}'
  exit 0
fi

# Any push: deny.
if [[ "$command" =~ ^git[[:space:]]+push ]]; then
  echo '{"permission":"deny","agent_message":"git push denied (G1). Humans push, agents never. Stage your changes and propose them in the halt-and-escalate report."}'
  exit 0
fi

# Rebase: deny.
if [[ "$command" =~ ^git[[:space:]]+rebase ]]; then
  echo '{"permission":"deny","agent_message":"git rebase denied (G2). Add new commits instead of rewriting history."}'
  exit 0
fi

echo '{"permission":"allow"}'
EOF
```

- [ ] **Step 2: Verify content**

```bash
head -5 .cursor/hooks/deny-irreversible-git.sh
test $(wc -l < .cursor/hooks/deny-irreversible-git.sh) -ge 20 && echo "size OK"
```

Expected: shebang line + comment + `set -euo pipefail`. `size OK`.

- [ ] **Step 3: Stage**

```bash
git add .cursor/hooks/deny-irreversible-git.sh
```

---

## Task 7: Author `ask-destructive.sh` (G3)

**Files:**
- Create: `.cursor/hooks/ask-destructive.sh`

- [ ] **Step 1: Write the file**

```bash
cat > .cursor/hooks/ask-destructive.sh <<'EOF'
#!/usr/bin/env bash
# Aiven LTS uplift hook: ask before destructive operations.
# Covers G3: git reset --hard, git clean -fd, rm -rf <path>.
set -euo pipefail

input=$(cat)
command=$(echo "$input" | jq -r '.command // empty')

# git reset --hard: ask.
if [[ "$command" =~ ^git[[:space:]]+reset[[:space:]].*--hard ]]; then
  echo '{"permission":"ask","user_message":"Destructive: git reset --hard will discard tracked changes. Confirm before proceeding.","agent_message":"Hook G3 flagged this as destructive; waiting for human approval."}'
  exit 0
fi

# git clean -f / -fd: ask.
if [[ "$command" =~ ^git[[:space:]]+clean[[:space:]].*-[fd] ]]; then
  echo '{"permission":"ask","user_message":"Destructive: git clean -f removes untracked files. Confirm before proceeding.","agent_message":"Hook G3 flagged this as destructive; waiting for human approval."}'
  exit 0
fi

# rm -rf <absolute-ish path>: ask.
if [[ "$command" =~ ^rm[[:space:]].*-[a-z]*r[a-z]*f[a-z]*[[:space:]]+/[^[:space:]]+ ]]; then
  echo '{"permission":"ask","user_message":"Destructive: rm -rf with an absolute path. Confirm.","agent_message":"Hook G3 flagged this as destructive; waiting for human approval."}'
  exit 0
fi

echo '{"permission":"allow"}'
EOF
```

- [ ] **Step 2: Verify**

```bash
head -3 .cursor/hooks/ask-destructive.sh
test $(wc -l < .cursor/hooks/ask-destructive.sh) -ge 20 && echo "size OK"
```

- [ ] **Step 3: Stage**

```bash
git add .cursor/hooks/ask-destructive.sh
```

---

## Task 8: Author `deny-upstream-file-writes.sh` (G4)

**Files:**
- Create: `.cursor/hooks/deny-upstream-file-writes.sh`

- [ ] **Step 1: Write the file**

```bash
cat > .cursor/hooks/deny-upstream-file-writes.sh <<'EOF'
#!/usr/bin/env bash
# Aiven LTS uplift hook: deny writes to upstream-owned paths.
# Covers G4. Runs on preToolUse for Write|Edit (matcher in hooks.json).
set -euo pipefail

input=$(cat)
# The path field varies by tool; try common locations.
path=$(echo "$input" | jq -r '.input.path // .input.target_file // .input.file_path // empty')

if [[ -z "$path" ]]; then
  # If we can't determine the path, allow (the matcher already narrowed to Write/Edit).
  echo '{"permission":"allow"}'
  exit 0
fi

# Normalize: drop leading ./ if present.
path="${path#./}"

# Match against upstream-owned paths.
if [[ "$path" =~ ^\.claude/ ]] \
   || [[ "$path" == "AGENTS.md" ]] \
   || [[ "$path" == "CONTRIBUTING.md" ]] \
   || [[ "$path" =~ ^\.github/workflows/ ]] \
   || [[ "$path" =~ ^contrib/ ]]; then
  echo "{\"permission\":\"deny\",\"agent_message\":\"Write to upstream-owned path denied (G4): $path. Aiven content goes under docs/aiven/, .cursor/, or other prefixed locations.\"}"
  exit 0
fi

echo '{"permission":"allow"}'
EOF
```

- [ ] **Step 2: Verify**

```bash
head -3 .cursor/hooks/deny-upstream-file-writes.sh
test $(wc -l < .cursor/hooks/deny-upstream-file-writes.sh) -ge 20 && echo "size OK"
```

- [ ] **Step 3: Stage**

```bash
git add .cursor/hooks/deny-upstream-file-writes.sh
```

---

## Task 9: Author `deny-best-of-n-runner.sh` (G5)

**Files:**
- Create: `.cursor/hooks/deny-best-of-n-runner.sh`

- [ ] **Step 1: Write the file**

```bash
cat > .cursor/hooks/deny-best-of-n-runner.sh <<'EOF'
#!/usr/bin/env bash
# Aiven LTS uplift hook: deny best-of-n-runner subagents (G5).
# best-of-n-runner creates worktrees/branches; our policy is single-branch.
set -euo pipefail

input=$(cat)
subagent_type=$(echo "$input" | jq -r '.subagent_type // empty')

if [[ "$subagent_type" == "best-of-n-runner" ]]; then
  echo '{"permission":"deny","user_message":"best-of-n-runner denied (G5): our LTS-uplift policy is single-branch, single-worker. Use generalPurpose or explore instead."}'
  exit 0
fi

echo '{"permission":"allow"}'
EOF
```

- [ ] **Step 2: Verify**

```bash
head -3 .cursor/hooks/deny-best-of-n-runner.sh
test $(wc -l < .cursor/hooks/deny-best-of-n-runner.sh) -ge 12 && echo "size OK"
```

- [ ] **Step 3: Stage**

```bash
git add .cursor/hooks/deny-best-of-n-runner.sh
```

---

## Task 10: Author `deny-agent-commits.sh` (G7)

**Files:**
- Create: `.cursor/hooks/deny-agent-commits.sh`

- [ ] **Step 1: Write the file**

```bash
cat > .cursor/hooks/deny-agent-commits.sh <<'EOF'
#!/usr/bin/env bash
# Aiven LTS uplift hook: deny agent commits (G7).
# Agent stages with `git cherry-pick --no-commit` and `git add`; human runs git commit.
set -euo pipefail

input=$(cat)
command=$(echo "$input" | jq -r '.command // empty')

# Any git commit: deny.
if [[ "$command" =~ ^git[[:space:]]+commit ]]; then
  echo '{"permission":"deny","agent_message":"git commit denied (G7). Agent does not commit. Stage your changes and propose the commit in your halt-and-escalate report; the human runs git commit themselves."}'
  exit 0
fi

# git cherry-pick WITHOUT --no-commit: deny.
if [[ "$command" =~ ^git[[:space:]]+cherry-pick ]] && ! [[ "$command" =~ --no-commit ]]; then
  echo '{"permission":"deny","agent_message":"git cherry-pick without --no-commit denied (G7). Use: git cherry-pick --no-commit <SHA>. The human commits the staged result."}'
  exit 0
fi

echo '{"permission":"allow"}'
EOF
```

- [ ] **Step 2: Verify**

```bash
head -3 .cursor/hooks/deny-agent-commits.sh
test $(wc -l < .cursor/hooks/deny-agent-commits.sh) -ge 18 && echo "size OK"
```

- [ ] **Step 3: Stage**

```bash
git add .cursor/hooks/deny-agent-commits.sh
```

---

## Task 11: Author `log-subagent-completion.sh` (observability)

**Files:**
- Create: `.cursor/hooks/log-subagent-completion.sh`

- [ ] **Step 1: Write the file**

```bash
cat > .cursor/hooks/log-subagent-completion.sh <<'EOF'
#!/usr/bin/env bash
# Aiven LTS uplift hook: log subagent completion to docs/aiven/uplifts/26.3/log.md.
# Observability minimum-viable. Captures timestamp, subagent type, and outcome from the report (if parsable).
set -euo pipefail

input=$(cat)
ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
subagent_type=$(echo "$input" | jq -r '.subagent_type // "unknown"')
subagent_id=$(echo "$input" | jq -r '.subagent_id // .id // "unknown"')

# Try to parse outcome from the subagent's final response.
final_response=$(echo "$input" | jq -r '.final_response // .response // empty')

outcome="unknown"
patch_slug="unknown"
escalation_reason="n/a"

if [[ -n "$final_response" ]]; then
  # Extract YAML front-matter fields if present.
  outcome=$(echo "$final_response" | awk '/^outcome:/ {print $2; exit}' || echo "unknown")
  patch_slug=$(echo "$final_response" | awk '/^patch_slug:/ {print $2; exit}' || echo "unknown")
  escalation_reason=$(echo "$final_response" | awk '/^escalation_reason:/ {print $2; exit}' || echo "n/a")
fi

# Ensure the log file exists with a header.
log_file="docs/aiven/uplifts/26.3/log.md"
if [[ ! -f "$log_file" ]]; then
  mkdir -p "$(dirname "$log_file")"
  cat > "$log_file" <<HEADER
# 26.3 uplift work log

Appended automatically by the subagentStop hook. Human-edit only to add commentary rows.

| Timestamp (UTC) | Patch slug | Subagent type | Subagent id | Outcome | Escalation reason |
|---|---|---|---|---|---|
HEADER
fi

# Append the row.
echo "| $ts | $patch_slug | $subagent_type | $subagent_id | $outcome | $escalation_reason |" >> "$log_file"

# Allow execution to continue (no permission field expected for subagentStop with continue semantics).
echo '{}'
EOF
```

- [ ] **Step 2: Verify**

```bash
head -3 .cursor/hooks/log-subagent-completion.sh
test $(wc -l < .cursor/hooks/log-subagent-completion.sh) -ge 30 && echo "size OK"
```

- [ ] **Step 3: Stage**

```bash
git add .cursor/hooks/log-subagent-completion.sh
```

---

## Task 12: Make hook scripts executable

**Files:**
- Modify: file modes of `.cursor/hooks/*.sh`

- [ ] **Step 1: chmod +x**

```bash
chmod +x .cursor/hooks/*.sh
ls -l .cursor/hooks/*.sh
```

Expected: all six `.sh` files show `-rwxr-xr-x` (or similar with `x` bits set).

- [ ] **Step 2: Stage mode changes**

git tracks the executable bit. The previous `git add` calls added the files; running `git add` again ensures the mode change is staged.

```bash
git add .cursor/hooks/*.sh
git diff --cached --summary | grep -E "mode change|create mode"
```

Expected: six `create mode 100755` lines.

---

## Task 13: Smoke-test each hook with synthetic input

**Files:**
- Create: `tmp/bootstrap/smoke-test.sh` (run-only; never committed)

This task verifies the hooks behave correctly without going through Cursor's event loop. Each test pipes a synthetic JSON input to the hook and asserts the output.

- [ ] **Step 1: Create the smoke-test directory**

```bash
mkdir -p tmp/bootstrap
```

- [ ] **Step 2: Write the smoke-test script**

```bash
cat > tmp/bootstrap/smoke-test.sh <<'EOF'
#!/usr/bin/env bash
# Smoke tests for .cursor/hooks/*.sh. Asserts each hook's behaviour on synthetic input.
set -euo pipefail

pass=0
fail=0

assert_eq() {
  local name="$1" actual="$2" expected="$3"
  if [[ "$actual" == "$expected" ]]; then
    echo "  PASS: $name"
    pass=$((pass+1))
  else
    echo "  FAIL: $name"
    echo "    expected: $expected"
    echo "    actual:   $actual"
    fail=$((fail+1))
  fi
}

echo "--- G1/G2: deny-irreversible-git.sh ---"
got=$(echo '{"command":"git push origin main"}' | .cursor/hooks/deny-irreversible-git.sh | jq -r '.permission')
assert_eq "git push -> deny" "$got" "deny"

got=$(echo '{"command":"git push --force origin main"}' | .cursor/hooks/deny-irreversible-git.sh | jq -r '.permission')
assert_eq "git push --force -> deny" "$got" "deny"

got=$(echo '{"command":"git rebase main"}' | .cursor/hooks/deny-irreversible-git.sh | jq -r '.permission')
assert_eq "git rebase -> deny" "$got" "deny"

got=$(echo '{"command":"git status"}' | .cursor/hooks/deny-irreversible-git.sh | jq -r '.permission')
assert_eq "git status -> allow" "$got" "allow"

echo "--- G3: ask-destructive.sh ---"
got=$(echo '{"command":"git reset --hard v26.3.10.62-lts"}' | .cursor/hooks/ask-destructive.sh | jq -r '.permission')
assert_eq "git reset --hard -> ask" "$got" "ask"

got=$(echo '{"command":"git clean -fd"}' | .cursor/hooks/ask-destructive.sh | jq -r '.permission')
assert_eq "git clean -fd -> ask" "$got" "ask"

got=$(echo '{"command":"git status"}' | .cursor/hooks/ask-destructive.sh | jq -r '.permission')
assert_eq "git status -> allow" "$got" "allow"

echo "--- G4: deny-upstream-file-writes.sh ---"
got=$(echo '{"input":{"path":".claude/skills/foo.md"}}' | .cursor/hooks/deny-upstream-file-writes.sh | jq -r '.permission')
assert_eq ".claude/ write -> deny" "$got" "deny"

got=$(echo '{"input":{"path":"AGENTS.md"}}' | .cursor/hooks/deny-upstream-file-writes.sh | jq -r '.permission')
assert_eq "root AGENTS.md write -> deny" "$got" "deny"

got=$(echo '{"input":{"path":"contrib/foo/bar.cpp"}}' | .cursor/hooks/deny-upstream-file-writes.sh | jq -r '.permission')
assert_eq "contrib/ write -> deny" "$got" "deny"

got=$(echo '{"input":{"path":"docs/aiven/AGENTS.md"}}' | .cursor/hooks/deny-upstream-file-writes.sh | jq -r '.permission')
assert_eq "docs/aiven/AGENTS.md write -> allow" "$got" "allow"

got=$(echo '{"input":{"path":"src/Storages/foo.cpp"}}' | .cursor/hooks/deny-upstream-file-writes.sh | jq -r '.permission')
assert_eq "src/ write -> allow" "$got" "allow"

echo "--- G5: deny-best-of-n-runner.sh ---"
got=$(echo '{"subagent_type":"best-of-n-runner"}' | .cursor/hooks/deny-best-of-n-runner.sh | jq -r '.permission')
assert_eq "best-of-n-runner -> deny" "$got" "deny"

got=$(echo '{"subagent_type":"generalPurpose"}' | .cursor/hooks/deny-best-of-n-runner.sh | jq -r '.permission')
assert_eq "generalPurpose -> allow" "$got" "allow"

echo "--- G7: deny-agent-commits.sh ---"
got=$(echo '{"command":"git commit -m foo"}' | .cursor/hooks/deny-agent-commits.sh | jq -r '.permission')
assert_eq "git commit -> deny" "$got" "deny"

got=$(echo '{"command":"git commit --amend"}' | .cursor/hooks/deny-agent-commits.sh | jq -r '.permission')
assert_eq "git commit --amend -> deny" "$got" "deny"

got=$(echo '{"command":"git cherry-pick abc1234"}' | .cursor/hooks/deny-agent-commits.sh | jq -r '.permission')
assert_eq "git cherry-pick (no flag) -> deny" "$got" "deny"

got=$(echo '{"command":"git cherry-pick --no-commit abc1234"}' | .cursor/hooks/deny-agent-commits.sh | jq -r '.permission')
assert_eq "git cherry-pick --no-commit -> allow" "$got" "allow"

echo ""
echo "=== Smoke test summary: $pass passed, $fail failed ==="
exit $fail
EOF
chmod +x tmp/bootstrap/smoke-test.sh
```

- [ ] **Step 3: Run the smoke tests**

```bash
./tmp/bootstrap/smoke-test.sh
```

Expected: all assertions PASS; exit code 0; final line reads `=== Smoke test summary: 18 passed, 0 failed ===`.

If any assertion FAILs, STOP. Inspect the script, fix the hook, re-run. Do not proceed to Task 14 until all 18 assertions pass.

- [ ] **Step 4: Do not stage the smoke-test script**

`tmp/` is for ephemeral scratch (workspace rule); the smoke-test script does not get committed.

---

## Task 14: Verify state and propose human commit

**Files:**
- (none modified; this task verifies and hands off)

- [ ] **Step 1: Show the staged state**

```bash
git status
git diff --cached --stat
```

Expected staged files:

```
new file:   .cursor/hooks.json
new file:   .cursor/hooks/.gitkeep
new file:   .cursor/hooks/ask-destructive.sh        (mode 100755)
new file:   .cursor/hooks/deny-agent-commits.sh     (mode 100755)
new file:   .cursor/hooks/deny-best-of-n-runner.sh  (mode 100755)
new file:   .cursor/hooks/deny-irreversible-git.sh  (mode 100755)
new file:   .cursor/hooks/deny-upstream-file-writes.sh (mode 100755)
new file:   .cursor/hooks/log-subagent-completion.sh (mode 100755)
new file:   docs/aiven/AGENTS.md
new file:   docs/aiven/patches/.gitkeep
new file:   docs/aiven/runbooks/.gitkeep
new file:   docs/aiven/schema/halt-and-escalate.md
new file:   docs/aiven/skills/.gitkeep
new file:   docs/aiven/uplifts/26.3/.gitkeep
```

(Total: 14 new files.)

Untracked but **not staged** (intentionally):
- `docs/aiven/proposals/2026-05-19-uplift-orchestration-design.md` (the spec — decide separately)
- `docs/aiven/plans/2026-05-20-bootstrap-orchestration.md` (this plan — decide separately)
- `docs/aiven/proposals/2026-05-fork-uplift-process-research.md` (prior research)
- `docs/aiven/proposals/2026-05-ai-assisted-fork-maintenance.md` (prior design)
- `tmp/bootstrap/smoke-test.sh` (scratch; never committed)
- `tmp/bootstrap/env-validation.md` and `tmp/bootstrap/clickhouse-smoke-*.txt`, `tmp/bootstrap/ninja-dry-run.txt`, `tmp/bootstrap/cppexpr-result.txt` (Task 0 evidence; never committed)

- [ ] **Step 2: Verify Task 0 evidence still present**

Task 0's evidence file must exist on disk before we commit — the commit message references it.

```bash
test -f tmp/bootstrap/env-validation.md && echo "Task 0 evidence: OK" || echo "Task 0 evidence: MISSING"
head -10 tmp/bootstrap/env-validation.md
```

Expected: `Task 0 evidence: OK` and the file's first 10 lines showing the run date, branch, and HEAD SHA.

If the evidence file is missing, STOP. The bootstrap commit must reference real env validation.

- [ ] **Step 3: Decide whether to include spec/plan in this commit**

Two options for the human to decide:

- **Option A (recommended):** Commit only the orchestration system files (the 14 staged entries). The spec and plan get a separate commit (so they have their own clean history entry). The two prior proposals can be deleted in a follow-up commit if you want to keep history clean, or kept as untracked reference.
- **Option B:** Commit everything in `docs/aiven/` (system + spec + plan) plus the hooks. One large commit.

The plan recommends A because it keeps the bootstrap commit minimal and verifiable against §15.

- [ ] **Step 4: Print the proposed commit message and command**

Capture the env validation summary line for inclusion in the commit message:

```bash
env_summary=$(grep -E '^\*\*HEAD SHA' tmp/bootstrap/env-validation.md | head -1)
clickhouse_version=$(grep -oE '26\.3\.[0-9.]+' tmp/bootstrap/clickhouse-smoke-2.txt | head -1)
echo "ENV_SUMMARY=${env_summary}"
echo "CLICKHOUSE_VERSION=${clickhouse_version}"
```

Then print the proposed commit message:

```
Proposed commit message:

  Aiven LTS uplift orchestration: bootstrap (T1)

  Introduce the orchestration system per design spec §10 steps 0-5.

  - docs/aiven/AGENTS.md: always-on invariants for subagents under the
    docs/aiven/ subtree (~80 lines, 8 sections including a testing
    invariant: every patch ships with a test or a documented
    justification naming an existing upstream test).
  - docs/aiven/schema/halt-and-escalate.md: worker exit contract; every
    dispatched subagent returns its final response in this shape. The
    schema includes a `tests` block; `outcome: success` is rejected
    unless the tests block is satisfied with evidence-of-causation.
  - .cursor/hooks.json + 6 hook scripts: safety hooks G1, G2, G3, G4,
    G5, G7 (deny push, deny rebase/force-push, ask before destructive,
    deny writes to upstream-owned paths, deny best-of-n-runner subagent,
    deny agent commits) + subagentStop observability hook.

  Hooks smoke-tested via tmp/bootstrap/smoke-test.sh (18 assertions pass).

  Env pre-validated (Task 0): clickhouse <CLICKHOUSE_VERSION> on
  build/ runs SELECT 1 / version() / count(functions) and cppexpr.sh
  compiles a Block-size snippet against the existing build.

  Reference: docs/aiven/proposals/2026-05-19-uplift-orchestration-design.md
  Plan:      docs/aiven/plans/2026-05-20-bootstrap-orchestration.md

Proposed command (HUMAN runs this; agent must not):

  git commit -F - <<'EOF'
  ...same body as above with <CLICKHOUSE_VERSION> substituted...
  EOF
```

The agent substitutes `<CLICKHOUSE_VERSION>` from `clickhouse_version` above (e.g., `26.3.10.62`) before printing the human-facing message.

- [ ] **Step 5: Hand off to human**

The agent prints the message above and stops. The human:

1. Reviews the staged files (`git diff --cached`).
2. Decides A or B from Step 3.
3. Runs `git commit` themselves with the proposed message (or edits it).
4. Verifies `git log -1` shows the commit.

The bootstrap is complete when the human's commit lands on `v26.3.10.62-lts-aiven-dev`. After that, T2 (classifier subagent dispatch) begins as a separate plan.

---

## Acceptance criteria (matches spec §15)

After human commits, verify:

- [ ] **Task 0 evidence exists**: `tmp/bootstrap/env-validation.md` is present and shows all four checks (binary, smoke queries, ninja dry-run, cppexpr) PASS for the pre-reset branch tip.
- [ ] `git log --oneline v26.3.10.62-lts..HEAD` shows ONLY commits authored as part of this design's execution (at most: the T0 planning-docs commit and the T1 bootstrap commit). No prior experimental work between the upstream tag and the bootstrap commit.
- [ ] `wc -l docs/aiven/AGENTS.md` is ≤100.
- [ ] All 8 sections from spec §7 are present in `docs/aiven/AGENTS.md` (`grep -c "^## " docs/aiven/AGENTS.md` == 8). Section 7 is the testing invariant; section 8 is navigation.
- [ ] `docs/aiven/schema/halt-and-escalate.md` exists, matches spec §6 schema, AND includes the `tests` block in the YAML form, AND includes constraint #6 (tests block satisfaction for `outcome: success`).
- [ ] All six hook scripts are present, executable (`ls -l .cursor/hooks/*.sh` shows `x` bits), and pass the smoke test (`./tmp/bootstrap/smoke-test.sh` exits 0 with 18/18 passes).
- [ ] `.cursor/hooks.json` is valid JSON (`jq . .cursor/hooks.json > /dev/null`).
- [ ] The bootstrap commit message names this proposal by path (`git log -1 --format=%B | grep -q 2026-05-19-uplift-orchestration-design.md`) AND references the Task 0 env validation summary.
- [ ] No other files committed beyond the 14 listed in Task 14 Step 1.

If any of the above fails, the bootstrap is incomplete. Open a follow-up to fix before T2.

---

## What this plan deliberately does NOT do

- Run any subagent. T1 is pure infrastructure; T2 dispatches the first subagent (classifier).
- Author any patch dossier or the per-patch dossier template. Per the walking-skeleton invariant, dossiers are born when patches are dispatched. **However, the dossier template (born at T3) MUST contain a "Testing" section that mirrors the schema's `tests` block: stateless test paths, pre/post verification, or no-test justification with upstream reference. T3's plan will encode this.**
- Author a `docs/aiven/skills/test-shape.md` skill. Deferred until either (a) a worker hits `test_design_blocked` once, or (b) three consecutive workers ask test-design questions. Per spec §11.
- Write the introduction document (`docs/aiven/uplifts/26.3/00-introduction.md`). That's a T6 deliverable, evidence-driven from patches 1–10.
- Ship the `postToolUse` tool-call counter hook. Deferred to "observed need"; if `subagentStop` logging is insufficient after 3–5 patches, we add it.
- Commit anything. The agent stages; the human commits.
- Run a full ClickHouse build from scratch. Task 0 verifies the env via a small compile (cppexpr.sh) + smoke queries + ninja dry-run; a full build happens naturally when T3's first patch needs a tier-3 verification.

---

## Execution Handoff

Plan complete and saved to `docs/aiven/plans/2026-05-20-bootstrap-orchestration.md`. Two execution options:

1. **Subagent-Driven (recommended for validation):** Dispatch a fresh `generalPurpose` subagent per task. Each subagent gets one task's instructions, executes the steps, reports completion. The parent (this chat) reviews between tasks. This exercises the dispatch pattern on infrastructure work before we trust it with a real cherry-pick — it's a complementary validation to T2 (the classifier subagent).

2. **Inline Execution:** Execute the plan in this session using `superpowers:executing-plans`. Faster, but does not exercise the dispatch pattern.

For T1, the trade-off is: subagent-driven is slower (per-task dispatch overhead) but gives us a second validation of the dispatch infrastructure before T2. Inline is faster but means T2 is the first real dispatch test.

Which approach?

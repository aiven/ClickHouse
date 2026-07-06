# Aiven LTS uplift orchestration — agent-assisted design (26.3 cycle)

> **Document type:** Validated design spec (brainstorming output).
> **Date:** 2026-05-19.
> **Status:** Approved for implementation. Supersedes the two prior proposals (`2026-05-ai-assisted-fork-maintenance.md`, `2026-05-fork-uplift-process-research.md`) as the binding design for the 26.3 uplift. Those documents remain as reference for context and history.
> **Audience:** The engineer who will execute the bootstrap and run the first patch; reviewers of the system before it ships.
> **One-sentence framing:** Forward-port ClickHouse Aiven patches one at a time using a fresh Cursor `Task` subagent per patch, with a strict halt-and-escalate exit contract, durable per-patch dossiers that cherry-pick across uplifts, safety hooks on irreversible git operations, and a human-only commit gate.

## 1. Goal

Build the smallest system that lets one Cursor `Task` subagent forward-port one patch with a fresh context, then grow only what the next patch demands. The 70-patch ship is the system's training curriculum: by patch 30 the orchestration is qualitatively different than by patch 3, because each patch's reflection adds exactly the artifact whose absence hurt.

## 2. Key invariants

These never change regardless of which patch is in flight.

1. **Fresh context per patch.** Each patch gets a brand-new subagent. No carryover. The parent (this chat, plus you as planner) retains meta-state; each worker arrives clean.
2. **Sequential and observable.** One patch at a time, on `v26.3.10.62-lts-aiven-dev`. No worktrees, no parallel branches, no sub-branches. We want to observe the worker's behavior closely and refine the system; parallelism would hide the signal.
3. **Halt-and-escalate is a first-class outcome.** A worker that detects a non-mechanical conflict or a test failure it cannot explain MUST stop and return a structured report. Falling through with a guess is forbidden.
4. **No tool lock-in for durable content.** Skills, templates, per-patch dossiers, runbooks all live as plain markdown under `docs/aiven/`. Tool-specific glue (Cursor hooks) is a thin layer that points *at* the durable content.
5. **Human commits, agent proposes.** The agent applies the patch, runs the build, runs the test, and stages every file. The agent never executes `git commit`. The human reviews the staged state and types `git commit` themselves. This is the final approval gate that no automation can bypass.
6. **No skill file before the work demands it.** The walking-skeleton discipline (Cockburn) plus the Rule of Three (Fowler) apply at the system layer: we do not write a skill file, a template, or an introduction document before we have done the underlying task at least once. The first patch authors itself; the first skill is extracted after we have ported by hand and seen the pain.
7. **Every patch ships with tests, or a documented justification.** A patch without a test that demonstrably exercises *its specific change* is not "done". The default expectation is a stateless test under `tests/queries/0_stateless/` that fails on the parent commit and passes after the patch (an evidence-of-causation pair). If no test is feasible (build-system-only change, behavior already covered by an existing upstream test that we explicitly identify by path), the dossier records the justification. The worker cannot claim `outcome: success` without satisfying this invariant — the schema enforces it.

## 3. Architecture

```
You + this chat (parent)                       Cursor Task subagent (per patch)
─────────────────────────                      ────────────────────────────────────
                                                                                 
1. Read inventory                                Auto-loaded into context:       
2. Pick next patch                       ──►     - AGENTS.md (root)              
3. Hand-author dossier                           - docs/aiven/AGENTS.md          
4. Compose dispatch prompt                                                       
5. Dispatch subagent              ─────────►     Embedded in dispatch prompt:    
   (Task: explore or                             - patch dossier path            
    generalPurpose)                              - procedure (cherry-pick steps) 
                                                 - halt-and-escalate schema      
                                                                                 
6. Receive halt-and-escalate                     Acts in same workspace:         
   report                         ◄─────────     - git cherry-pick --no-commit   
7. Review staged state                           - ninja -C build                
8. Type git commit (human-only)                  - clickhouse-test               
9. Append to log.md                              - write/update dossier          
10. Reflect: what was missing?                   - return structured report      
    Build it if anything was.                                                    
```

**Industry pattern:** orchestrator-worker over a quilt-style patch queue. The orchestrator (you as planner, this chat as planning assistant) decides what runs next; the worker (subagent) decides only *how to execute one patch*, must verify before claiming success, and must halt if it cannot resolve mechanically. This composes Anthropic's multi-agent reference architecture (orchestrator/worker), the Linux-stable backport workflow (per-patch metadata, upstream-candidacy flags), and Mercurial Queues / quilt (each patch is a first-class durable artifact).

## 4. Directory layout

```
ClickHouse/
├── AGENTS.md                                    # upstream-owned (read-only for us)
├── .cursor/                                     # TOOL-GLUE ONLY (Cursor-specific)
│   ├── hooks.json                               # ~30 lines, references scripts below
│   └── hooks/
│       ├── deny-irreversible-git.sh             # G1, G2 (push, rebase, amend, force)
│       ├── ask-destructive.sh                   # G3 (reset --hard, rm -rf, git clean)
│       ├── deny-upstream-file-writes.sh         # G4 (preToolUse on Write/Edit)
│       ├── deny-best-of-n-runner.sh             # G5 (subagentStart deny)
│       ├── deny-agent-commits.sh                # G7 (no git commit; cherry-pick must use --no-commit)
│       └── log-subagent-completion.sh           # observability (subagentStop)
└── docs/aiven/
    ├── AGENTS.md                                # bare-minimum invariants (~50 lines)
    ├── schema/
    │   └── halt-and-escalate.md                 # the worker exit contract
    ├── patches/
    │   └── <NNN>-<slug>.md                      # DURABLE per-patch dossiers (cross-uplift)
    └── uplifts/26.3/
        └── log.md                               # observability + work log (appended on each dispatch)
```

Files that **do not yet exist after the first dispatch** because they emerge from need:

- `docs/aiven/README.md` — human entry point. Written when a second person enters the system.
- `docs/aiven/uplifts/26.3/00-introduction.md` — kickoff briefing. Written after patch 1 reveals what the next-patch dispatcher had to figure out cold.
- `docs/aiven/uplifts/26.3/inventory.md` — full inventory file. Written when patch 2 is being dispatched and we need a referenceable list (single-row inline is enough for patch 1).
- `docs/aiven/uplifts/26.3/escalations/` — directory of escalation reports. Created only when the first escalation fires.
- `docs/aiven/skills/<name>.md` — procedure files. Extracted only after N=3 patches have been ported and the procedure embedded into prompts has stabilised.
- `docs/aiven/runbooks/<name>.md` — operational docs. Written only when operations hurts.

Locking these *out* of the day-1 bootstrap is the walking-skeleton discipline applied to documentation itself. Their first version is **evidence-driven**, not speculation-driven.

## 5. Naming convention for per-patch dossiers

Dossiers live at `docs/aiven/patches/<NNN>-<slug>.md` with `<NNN>` a **stable numeric prefix assigned at first carry**. The 78 patches that exist in the 25.8 inventory (per T2.2 classifier run 2026-05-22) inherit `001`–`078` by chronological introduction date; new patches that land later get the next unused number (`079`, `080`, …); dropped patches keep their slot (`status: superseded` or `status: dropped-in-vXX.Y`) and the number is never reused.

This mirrors how Linux stable backports use stable IDs rather than position numbers. A dossier file is **the patch's home directory across LTS uplifts**: when we move from 25.8 to 26.3, the same file gets a new row appended to its §0 Lineage table and a new section under §"Per-uplift notes". The file *cherry-picks* across the fork's history, just like the patch it documents.

## 6. The halt-and-escalate schema (worker exit contract)

Lives in `docs/aiven/schema/halt-and-escalate.md`. Every subagent dispatched in this system MUST return its final response in this shape.

```markdown
---
outcome: success | escalate
patch_slug: <slug>
source_sha: <SHA on previous LTS, or empty if not applicable e.g. classifier subagent>
proposed_commit:
  staged_files:
    - <path>
    - <path>
  commit_message: |
    <verbatim message the human should use, including any provenance trailers>
  byte_equivalent: true | false      # staged source matches source patch byte-for-byte
tests:
  added: yes | no_justified
  kind: stateless | integration | unit | upstream-existing
  paths:                                # files added (kind=stateless/integration/unit)
    - tests/queries/0_stateless/<NNNNN>_<slug>.sql
    - tests/queries/0_stateless/<NNNNN>_<slug>.reference
  upstream_reference:                   # populated only when kind=upstream-existing
    - tests/queries/0_stateless/<existing_test>.sql
  pre_patch_fail_verified: true | false # for added tests: confirmed test FAILS on parent commit
  post_patch_pass_verified: true | false # confirmed test PASSES with patch staged
  justification: |                      # populated only when added=no_justified
    <why no test is feasible; e.g. "build-system only", "config rename only">
escalation_reason: none | textual_conflict | semantic_conflict | build_fail_api_rename | test_fail_ambiguous | test_design_blocked | policy_call | other
---

## Tier results
- Tier 1 (textual cherry-pick): pass | fail | n/a — <one-line summary>
- Tier 2 (semantic patch-id):    pass | fail | n/a — <one-line summary>
- Tier 3 (build + test):         pass | fail | n/a — <one-line summary>

## Evidence
<commands run, exit codes, key log excerpts; max ~50 lines>

## What I did
<narrative bullets: files touched, commands run, decisions made>

## Proposed next step
<for success: "Ready for human commit. Suggested: git commit -c CHERRY_PICK_HEAD";
 for escalate: a concrete suggested resolution or "need policy decision: <question>">
```

**Properties enforced by the schema:**

- `outcome: success` requires non-empty `proposed_commit.staged_files` AND all three tiers `pass` AND a satisfied `tests` block (see below). The schema rejects "fake success".
- `escalation_reason` is a small enum; `other` is allowed but tracked, and if `other` accumulates we revise the enum after N=3 patches (see §10). `test_design_blocked` is the explicit escape hatch when a worker cannot design a test that exercises the patch's specific change — preferable to silently shipping no test.
- The agent cannot record a `target_sha` because the agent does not commit. The human's `git commit` produces the SHA, and the human appends it to the log row out-of-band.
- The `byte_equivalent` flag distinguishes "patch landed byte-for-byte" (strong evidence the cherry-pick is correct) from "the cherry-pick reshaped" (which requires audit; the agent must escalate via `semantic_conflict` if reshaping is non-trivial).
- The `tests` block satisfaction rule for `outcome: success`:
  - `tests.added == yes` requires non-empty `paths`, AND `pre_patch_fail_verified: true`, AND `post_patch_pass_verified: true`. Both verifications must show command output in the Evidence section. This is the **evidence-of-causation pair** that proves the test actually exercises the patch.
  - `tests.added == no_justified` requires non-empty `justification` AND one of: (a) non-empty `upstream_reference` (identifying a specific existing test that already covers the behavior), OR (b) a `kind` of `upstream-existing` for which the worker has confirmed the test still runs and passes.
  - Anything else MUST escalate (typically `test_design_blocked`).

## 7. `docs/aiven/AGENTS.md` content

**Auto-load mechanism (empirical, verified 2026-05-21).** Cursor injects this file's content into an agent's context when the agent **reads any file under `docs/aiven/`**, NOT at agent startup. Verified by a readonly `explore` subagent dispatched via the `Task` tool: at startup it had only the root `AGENTS.md` (via `always_applied_workspace_rule`); `docs/aiven/AGENTS.md` appeared in its context only after a subsequent read under that subtree. **Consequence:** subagent dispatch prompts (§10, T3+) MUST inline this file's content verbatim, because a freshly dispatched worker that has not yet read anything in the subtree does not see these invariants. Hooks (§8) remain the load-bearing enforcement for destructive actions; this file is procedural guidance and must be in the worker's context from message 0.

Strictly invariants and navigation. No procedures. Target: ~100 lines.

Eight items:

1. **Orientation.** "You are working on Aiven's downstream fork of ClickHouse. Upstream tag is `v26.3.10.62-lts`. Aiven work happens on `v26.3.10.62-lts-aiven-dev`."
2. **Branch invariants.** Only act (read or write) when HEAD is on `*-aiven-dev`. Never commit to master. Never commit to `*-aiven` (release-line; only fast-forwarded after human sign-off).
3. **Never-touch list.** Do not write or edit: `.claude/**`, root `AGENTS.md`, `CONTRIBUTING.md`, `.github/workflows/**`, `contrib/**`. These are upstream-owned and must merge cleanly on the next LTS rebase.
4. **Git operations.** Do not use `git rebase`, `git commit --amend`, `git push --force`, or `git push -f`. Add new commits instead. Hooks enforce this; the rule here is for the agent's mental model.
5. **Agent does not commit.** Use `git cherry-pick --no-commit` to stage. The human runs `git commit`. The worker's success state is "everything verified and staged"; the human's commit is what makes it real.
6. **Halt-and-escalate contract.** If you hit any condition you cannot resolve mechanically (any non-trivial conflict, any build failure naming a renamed upstream symbol, any test failure you cannot explain), STOP and return the report per the schema at `docs/aiven/schema/halt-and-escalate.md`.
7. **Tests are required.** Every patch must ship with either (a) a new test that fails on the parent commit and passes after the patch (evidence-of-causation pair, output included in the report's Evidence section), or (b) a documented justification *naming an existing upstream test* whose behavior covers the patch. The default is (a). Aiven patches that gate on shared error codes (e.g. `SUPPORT_IS_DISABLED`) require care: the test must demonstrably distinguish the Aiven gate from any upstream gate that throws the same code. If you cannot design such a test, escalate with `test_design_blocked` — silently shipping no test is forbidden.
8. **Navigation.** Inventory and per-uplift work log live under `docs/aiven/uplifts/<version>/`. Durable per-patch dossiers live under `docs/aiven/patches/`. Your work product per patch: staged source changes, an updated dossier with the Testing section completed, the halt-and-escalate report.

Anything not on this list is *procedure* (lives in the dispatch prompt or a future skill file) or *operational detail* (lives in a future runbook). The cost of `AGENTS.md` content is paid by every subagent under the subtree; we keep it tight.

## 8. Safety guardrails (hard hooks)

Day-1 ships these as `.cursor/hooks.json` + scripts under `.cursor/hooks/`. Tool-glue exception accepted: hooks are inherently tool-specific. The *intent* (deny push, deny rebase, deny upstream-file writes) and the *script bodies* are portable to any other agent tool we might switch to later.

| # | Hook event | Matcher / scope | Behavior | Why |
|---|---|---|---|---|
| **G1** | `beforeShellExecution` | `^git push\b` (any form) | `permission: deny` | Push is irreversible at the remote. Humans push, agents never. |
| **G2** | `beforeShellExecution` | `^git rebase\b` OR `^git push\b.*(--force\|-f\b)` | `permission: deny` | Rewrite-history operations on shared branches. Workspace `AGENTS.md` rule turned into a guardrail. |
| **G3** | `beforeShellExecution` | `^git reset\s.*--hard\b` OR `^git clean\s.*-[fd]\b` OR `^rm\s.*-rf\s+/` | `permission: ask` | Destructive but legitimately needed (recovery scenarios). Prompts the human; doesn't outright forbid. |
| **G4** | `preToolUse` | tool=`Write` or `Edit`; path matches `^\.claude/\|^AGENTS\.md$\|^CONTRIBUTING\.md\|^\.github/workflows/\|^contrib/` | `permission: deny` | Upstream-owned paths (proposal §3 merge-safety invariant from prior session). |
| **G5** | `subagentStart` | `subagent_type: best-of-n-runner` | `permission: deny` | The `best-of-n-runner` creates branches/worktrees; conflicts with our "no extra branches" policy. Other subagent types (`generalPurpose`, `explore`, `code-reviewer`) are allowed. |
| **G7** | `beforeShellExecution` | `^git commit\b` OR `^git cherry-pick\b(?!.*--no-commit)` | `permission: deny` | Agent does not commit; cherry-pick must use `--no-commit` to leave changes staged for the human. |

G6 (branch-confusion ask before `git checkout master\|v.*-aiven$`) is **deferred** until we observe whether the agent actually attempts wrong-branch operations. The `subagent_type` of `generalPurpose` should not switch branches; if we observe it doing so, we promote G6 to hard.

**Failure-open vs. failure-closed.** All denying hooks (G1, G2, G4, G5, G7) set `failClosed: true`. If the hook script crashes, we block the action. Asking hooks (G3) fail open by default. Safety-first.

### 8.1 Hook implementation sketches

Concrete scripts are deferred to implementation, but the shape:

**`deny-irreversible-git.sh` (G1, G2):**

```bash
#!/usr/bin/env bash
input=$(cat)
command=$(echo "$input" | jq -r '.command // empty')

if [[ "$command" =~ ^git[[:space:]]+push ]] && [[ "$command" =~ (--force|-f([[:space:]]|$)) ]]; then
  echo '{"permission":"deny","agent_message":"Force-push denied. AGENTS.md forbids rewrite-history; add new commits instead."}'
  exit 0
fi
if [[ "$command" =~ ^git[[:space:]]+push ]]; then
  echo '{"permission":"deny","agent_message":"git push denied. Humans push, agents never. Stage your changes and propose them in the halt-and-escalate report."}'
  exit 0
fi
if [[ "$command" =~ ^git[[:space:]]+rebase ]]; then
  echo '{"permission":"deny","agent_message":"git rebase denied. Add new commits instead of rewriting history."}'
  exit 0
fi

echo '{"permission":"allow"}'
```

**`deny-agent-commits.sh` (G7):**

```bash
#!/usr/bin/env bash
input=$(cat)
command=$(echo "$input" | jq -r '.command // empty')

if [[ "$command" =~ ^git[[:space:]]+commit ]]; then
  echo '{"permission":"deny","agent_message":"Agent does not commit. Stage the changes (git cherry-pick --no-commit, git add as needed) and propose the commit in your halt-and-escalate report; the human runs git commit."}'
  exit 0
fi
if [[ "$command" =~ ^git[[:space:]]+cherry-pick ]] && ! [[ "$command" =~ --no-commit ]]; then
  echo '{"permission":"deny","agent_message":"Use git cherry-pick --no-commit. The human commits; the agent only stages."}'
  exit 0
fi

echo '{"permission":"allow"}'
```

The other hooks follow the same shape. Each script is short (<30 lines), independently testable, and free of external dependencies beyond `jq` (already common on the dev box; verify in `sessionStart` hook in a future iteration if we want to be paranoid).

## 9. Observability (minimum viable)

Two hooks on day 1, no more.

### 9.1 `subagentStop` log hook (`log-subagent-completion.sh`)

Appends one row to `docs/aiven/uplifts/26.3/log.md`:

```
| ISO timestamp | patch_slug | subagent_type | duration_s | tool_calls | files_edited | shell_cmds | outcome | escalation_reason |
```

The hook reads a transient counter file (see §9.2), consolidates, appends. `log.md` becomes the single source of truth for system improvement.

### 9.2 `postToolUse` counter hook (inline shell)

Increments per-subagent counters in `.cursor/.tmp/subagent-<id>.tsv`. Three counters: total tool calls, files edited (when tool=`Write`/`Edit`), shell commands (when tool=`Shell`). The `subagentStop` hook reads and consolidates this file.

### 9.3 What we deliberately do NOT instrument on day 1

- **Token-cost-per-patch attribution.** Cursor's billing API is not exposed to hooks. Read the Cursor dashboard; correlate against the log's wall-clock entries. Build a wrapper only if and when patch-level cost becomes a serious optimisation lever.
- **Per-tool-call latency.** Overkill for current scale.
- **External telemetry pipelines.** The markdown log is enough; if we ever need a dashboard, we generate it from the log.
- **Real-time alerting on escalations.** The escalation rate is reviewable end-of-cycle; live alerts make sense only after we run a second uplift cycle and have a baseline.

The principle: collect the cheapest signal that lets us answer "is the system getting better across patches?", and grow instrumentation only when we want to answer a question we cannot answer from the log.

## 10. The first dispatch — concrete walkthrough

When the user says "go" after this design is approved, the deterministic sequence is:

```
Step 0.  Validate build + test infrastructure on the current branch tip
         BEFORE resetting: confirm `build*/programs/clickhouse` exists and runs,
         build one small target (e.g. `clickhouse_common_io`) to verify the
         compiler env, and run 3 smoke `clickhouse local --query` invocations.
         If anything fails: STOP. Bootstrap of an orchestration system that
         can never validate a patch is worthless. Document the failure and
         escalate to the human for env repair.
Step 1.  git reset --hard v26.3.10.62-lts                       # blank slate
Step 2.  Write docs/aiven/AGENTS.md                              # ~70 lines, invariants only
Step 3.  Write docs/aiven/schema/halt-and-escalate.md            # the schema, ~100 lines
Step 4.  Write .cursor/hooks.json + 6 hook scripts under .cursor/hooks/
         and chmod +x the scripts
Step 5.  Stage everything; HUMAN commits "Aiven LTS uplift orchestration:
         bootstrap" (single commit)
Step 6.  Dispatch FIRST subagent: explore (readonly, validates dispatch shape)
            Input:   git log <prior-upstream-LTS-tag>..origin/<prior-aiven-LTS-branch>
                     For 26.3 cycle (per release-management decision 2026-05-22):
                       v25.8.18.1-lts..origin/v25.8.18.1-lts-aiven  (78 commits
                       as of T2.2 dispatch; was 77 at T2.1, +1 from the
                       force-push of 2026-05-21 that merged khatskevich/mv_race_258)
                     NOTE: this is the tag-to-aiven range, NOT
                     v26.3.10.62-lts..origin/v25.8.18.1-lts-aiven (944 commits)
                     which would include ~866 upstream stable backports we do not
                     want to port. The correct frame is "patches Aiven added to
                     the prior LTS", not "everything that diverges between bases".
                     plus the halt-and-escalate schema.
            Output:  lightweight inventory table — one row per patch with
                     mechanical metadata: (NNN, sha, date, author,
                     files-changed, LOC, subject, cherry_pick_clean).
                     The `cherry_pick_clean` column is a yes/no from a readonly
                     apply-check: `git format-patch -1 <sha> --stdout |
                     git apply --check`. This is the readonly equivalent of
                     `git cherry-pick --no-commit -n <sha>` followed by
                     `git cherry-pick --abort`, with identical semantics for
                     "does this patch apply cleanly?" but ZERO working-tree
                     mutation, which is required because the classifier is an
                     `explore` (readonly) subagent. Purely mechanical, no judgment.
                     NO pre-computed `testability` column: that judgment is
                     made fresh by each dispatch's worker in T3/T4/T5 when
                     the patch is actually being ported. Deep judgment
                     ahead of need violates walking-skeleton discipline.
                     Returned in the halt-and-escalate schema format.
Step 7.  Together: pick first patch from classifier table
Step 8.  Hand-author docs/aiven/patches/<NNN>-<slug>.md          # minimal: source SHA, why, risk
         HUMAN commits "Aiven patch <NNN> dossier (initial)"
Step 9.  Dispatch SECOND subagent: generalPurpose (load-bearing worker)
            Input:   docs/aiven/AGENTS.md content INLINED VERBATIM in the
                     dispatch prompt (per §7 — Cursor's nested-AGENTS.md
                     auto-load is read-triggered, not startup-time, so a
                     freshly dispatched subagent without prior subtree
                     reads has no Aiven invariants in context),
                     dossier path,
                     halt-and-escalate schema path
                     (docs/aiven/schema/halt-and-escalate.md),
                     embedded procedure (the per-patch lifecycle:
                     cherry-pick → upstream-drift analysis (per §14 Q6)
                     → build → test → propose commit / escalate).
            Output:  halt-and-escalate report
Step 10. Receive report. Review staged state + proposed commit message.
Step 11. If outcome: success — HUMAN runs git commit. Append row to log.md.
         If outcome: escalate — write an entry; decide manually.
         (No matter the outcome, the subagentStop hook has logged a row.)
Step 12. REFLECT. What artifact was missing whose absence cost time?
         Write that artifact NOW, named for the gap it filled.
         (Likely candidates: a procedure skill file, the introduction doc,
          a specific runbook entry.)
```

**Two subagents in the first cycle** is a feature, not overhead: we exercise the dispatch / schema / report cycle on a readonly task (the classifier) before trusting it with the load-bearing cherry-pick. We also get a permanent classifier the system reuses on patches 2…70 and on future LTS uplifts.

## 11. Deferred until observed need

This is the explicit list of things we know we *eventually* want but refuse to build pre-evidence:

| Artifact | Becomes visible only when |
|---|---|
| `docs/aiven/README.md` | A second person enters the system and asks "where do I start?" |
| `docs/aiven/uplifts/26.3/00-introduction.md` | Patch 1's reflection reveals what the next-patch dispatcher had to figure out cold. |
| `docs/aiven/uplifts/26.3/inventory.md` (full file) | Patch 2 is being dispatched and we need a referenceable list. |
| First skill file (`docs/aiven/skills/cherry-pick.md` or similar) | Procedure embedded into dispatch prompts has stabilised across N=3 patches and is duplicated identically. |
| `docs/aiven/skills/test-shape.md` | A worker hits `test_design_blocked` once, OR three consecutive workers ask "how do I make a test that distinguishes the Aiven gate from upstream's gate on the same error code?". The skill codifies the disambiguation rubric (use a permitted-by-default object that the Aiven gate specifically rejects; assert error_code AND error message substring; test on a setup where the upstream gate cannot fire). |
| G6 (branch-confusion ask) | Observed wrong-branch attempt. |
| Automation / followup hooks (auto-format, auto-run-after-success) | Observed operational pain that automation would relieve. |
| Sanitizer matrix integration | First ASan-relevant failure or release-tagging phase. |
| Parallel dispatch (multiple workers in parallel) | After we trust the orchestrator unsupervised; not during the 26.3 cycle's first ~30 patches. |

Each of these has its own falsification test embedded in the trigger: we know exactly when to build them, and we know we are mis-applying the discipline if we build them before.

## 12. Rationale — why this design rather than something else

Five principal-engineer-level reasons:

1. **Orchestrator-worker over a closed-form workflow.** The per-patch lifecycle is deterministic (read source SHA → cherry-pick → verify → test → propose commit → escalate-or-pass). The planner does not need to "discover" which skill applies when; the lifecycle dictates it. This is the canonical multi-agent shape (Anthropic 2024 reference architecture) and is the right pattern for closed-form tasks.

2. **Schema as verification rubric.** The halt-and-escalate schema is not merely a report format. It forces the worker to verify its own claims (non-empty `staged_files`, all tiers green) before it can return `outcome: success`. The schema *is* the safety mechanism, not a layer on top of safety mechanisms.

3. **Walking skeleton at the system layer.** No skill file, no template, no introduction document gets written before the work that demands it has been done once. The 70-patch cycle is the curriculum that designs the system. This is Cockburn's *Crystal Clear* (walking skeleton) and Fowler's Rule of Three combined, applied to documentation and tooling rather than only to abstractions.

4. **Safety hooks on irreversible operations.** The cost-of-slip × probability-of-slip calculus says: lock down the actions whose mistakes you cannot recover from (push, force-push, rebase, amend, commit-on-master, upstream-file-writes). Leave everything else soft. This is the same trust-boundary input-validation pattern that good API design uses.

5. **Human commits as final approval gate.** The single most effective way to keep a human meaningfully in the loop without losing automation's leverage is to make the most important act (committing source code) exclusively theirs. The agent prepares everything; the human's `git commit` is the moment of accountability. This is the *human-on-the-loop* pattern (Tesla Autopilot, aviation autonomy) tightened to *human-as-committer*.

## 13. Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Worker silently produces a wrong cherry-pick that passes all three tiers | Medium (revised from Low after T2 empirical data 2026-05-22: 53/78 = 68% of patches do NOT apply textually-clean; conflict-resolution is the common path, not the exception) | High | (a) Schema requires `byte_equivalent` flag — non-byte-equivalent forces `semantic_conflict` escalation. (b) Tier-3 is a real test, not just compile. (c) Human review of staged state is mandatory before commit. (d) Upstream-drift analysis (§14 Q6) MUST run before declaring success — empirically validated as load-bearing. |
| Schema enum accumulates `other` escalation reasons, becoming useless | Medium | Low–Medium | After N=3 patches with `other`, revise the enum (open-question §14). |
| Hook scripts have bugs that block legitimate operations | Medium | Low | Each hook script kept <30 lines; tested before bootstrap commit; `failClosed: true` only on the denying hooks (asking hooks fail open). |
| Subagent dispatch consumes too many tokens to be cost-effective | Medium | Medium | Day-1 observability captures wall-clock + tool calls; we re-evaluate after 5 patches against Cursor dashboard dollar data. |
| Walking-skeleton discipline is abandoned under shipping pressure (skills written prematurely) | Medium | Medium | Reflection step (step 11) is named; absence of a "what was missing" entry in `log.md` is a tripwire. |
| Worker tries to push despite hook (e.g., via SSH directly bypassing shell tool) | Very low | High | The agent does not have shell-bypassing primitives in `Task` subagents; all shell happens through the `Shell` tool which is matched by `beforeShellExecution`. |
| Hook scripts not portable across our machines (jq missing, etc.) | Low | Low | `sessionStart` hook can verify dependencies (future iteration); initial scripts use only POSIX + jq. |

## 14. Open questions (to revisit at first reflection step after patch 1)

1. **Aiven test-numbering convention.** **RESOLVED 2026-05-25** after T3.2 (patch 040) and T3.3 (patch 011) shipped tests: Aiven stateless tests use `9<NNN>_<slug>.{sh,sql}` where `<NNN>` is the patch dossier number. The convention does NOT use upstream's `add-test` allocator (avoids polluting upstream's monotonic prefix sequence and avoids rebase collisions when upstream eventually claims the `04XXX` numbers we'd have used). The `9XXXX` partition is virgin territory upstream-wide. Rationale, adoption record, and how to author such a test by hand: `docs/aiven/runbooks/testing-suites.md` §4.1.
2. **Commit-message trailer set.** The human commits using `git commit -c CHERRY_PICK_HEAD`, which preserves the original author + message. The set of provenance trailers (`(cherry picked from commit …)`, `Aiven-Patch-Slug:`, `Upstream-Candidate:`, `Signed-off-by:`) is hypothesis; verify by patch 3 whether the trailer set pays its weight or is decoration.
3. **Classifier subagent output format.** The classifier is deliberately **lightweight and mechanical** (see §10 step 6): per-patch metadata that requires no judgment + a `cherry_pick_clean` boolean from dry-run apply. The deeper judgments (testability, semantic-conflict likelihood, dependency-with-other-patches) are made *fresh* at dispatch by each patch's worker, not pre-computed. Open question: does the lightweight column set need any additions (e.g., `touches_tests/`, `touches_settings/`)? Decide after T3.
4. **`escalation_reason` enum sufficiency.** If patch 1 escalates and `other` is the only fit, revise the enum *now* with the observed category, not at N=3.
5. **Whether `subagentStop` hook should also write the halt-and-escalate report to a file** under `docs/aiven/uplifts/26.3/reports/`, or only summarise in `log.md`. Lean: write the full report when `outcome: escalate`, summary-only when `outcome: success`. Decide after patch 1.
6. **Upstream-drift analysis in the T3 embedded procedure.** **Empirically validated by T2 classifier (2026-05-22; figures from T2.2 post-rebuild): 53/78 = 68% of patches in `v25.8.18.1-lts..origin/v25.8.18.1-lts-aiven` do NOT apply textually-clean to `v26.3.10.62-lts`. The question is no longer "do we need this step" but "what does it look like".** A patch that applies textually-clean against the new base is NOT automatically semantically-correct. Upstream may have refactored the surrounding code (same names, different contract), already merged an equivalent or superseding fix, or removed the use-case the patch was solving. The T3 worker's embedded procedure therefore MUST include an upstream-drift step BEFORE declaring success: given the patch's purpose statement from the dossier, examine `git log <prev-LTS-tag>..<new-LTS-tag> -- <files-touched-by-patch>` to (a) detect any upstream change that obviates the patch, (b) detect any upstream refactor that invalidates the patch's assumptions, and (c) record the finding in the dossier. The dossier gains a section `Upstream-drift: none | superseded-by <sha> | invalidates-assumption <description> | obsolete`. If drift is found, the worker escalates with `escalation_reason: semantic_conflict` and a "Proposed next step" describing the drift. Concrete shape of this step (commands, output schema, threshold for "drift") is unknown until patch 1 forces us to write it — leaving it as an open question rather than pre-specifying it.

7. **C++ / database review checklist for the T3 worker.** The compiled binary goes to production; a patch worker that's a "generic competent C++ engineer" is insufficient. ClickHouse code has its own idioms (columnar batch processing, custom allocators, template-heavy hot paths, lock-free structures in places) and database code has its own concerns (durability, consistency, concurrency under load, error recovery, observability). The T3 dispatch prompt MUST include a concrete checklist the worker walks through before declaring `outcome: success`. NOT a "be a senior engineer" framing (that does not change behavior) — a list of specific invariants to verify on every patch: (a) lifetimes/ownership (smart pointers vs raw, RAII discipline); (b) exception safety (rollback semantics, exception-safe stages); (c) thread safety (`Context`, caches, immutable-shared-read invariants); (d) allocation discipline (existing arena/`StringRef` usage in that area, no new string churn in hot paths); (e) batch processing (Columns/Chunks over per-row); (f) settings discipline (user-facing behavior changes should have a setting if the area uses settings); (g) error-code disambiguation (the rubric already in `docs/aiven/AGENTS.md` §7); (h) performance reasoning (complexity, allocations, copies, locality). The checklist lives at `docs/aiven/skills/cpp-review-checklist.md` (a future skill, NOT yet authored — walking-skeleton discipline says we write it for the first patch and refine it from observed pain). The T3 dispatch prompt either inlines the checklist verbatim (same mechanism as the AGENTS.md inline, for the same read-trigger reason) or, once we trust skill-loading, references it by path. Concrete checklist content is deferred to T3 plan; this open question records the requirement so it cannot be forgotten.

## 15. Bootstrap acceptance criteria

The bootstrap commit is acceptable if and only if:

- **Env validation (Step 0) passed**: a recorded run of `build*/programs/clickhouse local --query 'SELECT version()'` returned a version string, a small target built clean, and three smoke `clickhouse local --query` invocations succeeded. The evidence is captured in `tmp/bootstrap/env-validation.md` (scratch, not committed) and summarised in the bootstrap commit message.
- The bootstrap commit's chain to the upstream tag contains only Aiven-orchestration commits (planning docs, bootstrap) — no prior experimental work, no upstream rewrites. Equivalent expressed in git: `git log --oneline v26.3.10.62-lts..HEAD` lists only commits authored as part of this design's execution (T0 planning-docs commit, T1 bootstrap commit, and nothing else).
- `docs/aiven/AGENTS.md` is ≤80 lines and contains all 8 items from §7 (including the testing invariant).
- `docs/aiven/schema/halt-and-escalate.md` is the canonical schema from §6, including the `tests` block and the `outcome: success` test-satisfaction rule.
- All six hook scripts are present, executable (`chmod +x`), and pass a smoke test against synthetic inputs.
- `.cursor/hooks.json` is valid JSON, schema-version 1, with the six hooks defined.
- The bootstrap commit message names this proposal by path.
- No other files are committed (no README, no introduction doc, no inventory). The walking skeleton starts here.

The first patch dispatch happens **after** the bootstrap commit lands, not as part of it.

## 16. Epic structure and session boundary

This design is shipped across two Jira epics. The epic boundary doubles as the session boundary between two Cursor/Superpowers sessions.

### 16.1 Epic 1 — Accelerate ClickHouse LTS uplift with AI

**Scope.** Build the orchestration system + port N=10 patches end-to-end through it. The N=10 figure is the headline target; if patches 6–8 show clear stability (no new artifacts written during their reflection step, escalation rate flat, time-per-patch flat), epic-1 can finish at N=8 instead. Conversely, if the system is still surfacing new artifacts at N=9, we extend to N=12.

**Why N=10** (not N=3, not N=30):
- N=3 = walking-skeleton minimum (Rule of Three). Validates the system works at all.
- N=10 = enough patch-shape variety (clean cherry-pick, conflict, settings-touching, build-system, security, IO, etc.) to be confident the system is not tuned to early-patch idiosyncrasies. Also enough to extract 1–2 stable skills with confidence.
- N=30+ = too much epic-1 scope; the marginal learning per patch flattens after ~10 and we are better served by shifting effort to epic-2 execution.

**Per-patch authoring is dispatched-driven, not pre-built.** A dossier file is born when the patch is dispatched; patches in the inventory but not yet dispatched have *no* dossier file. This is the walking-skeleton invariant from §2 applied to the inventory itself.

**Acceptance criteria** (in addition to §15 bootstrap acceptance):
- Patches 1–10 each have: a dossier under `docs/aiven/patches/<NNN>-<slug>.md`, a human-made commit on `v26.3.10.62-lts-aiven-dev`, a row in `docs/aiven/uplifts/26.3/log.md`.
- At least one skill file extracted under `docs/aiven/skills/` (N=3 trigger from §11).
- `docs/aiven/uplifts/26.3/00-introduction.md` and `docs/aiven/README.md` written, sized to brief a fresh epic-2 session.
- Retrospective committed: median time/patch, tool-call distribution, escalation rate, escalation-reason histogram, named refinements for epic-2.
- Spec refinements (if any) merged.

**Rough ticket breakdown** (sized in t-shirt; real estimates need patch-1 data):

| # | Ticket | Size | DoD |
|---|---|---|---|
| T0 | Land design spec | XS | Spec committed on a Jira-linked branch |
| T1 | Bootstrap the orchestration system | M | Bootstrap commit landed; hook smoke tests green |
| T2 | Validate dispatch with classifier subagent | S | Classifier table produced; report schema-valid |
| T3 | Port first patch end-to-end | M | 1 commit + 1 dossier + 1 log row + reflection note |
| T4 | Port patches 2–3, extract first skill | M | 3 total commits + 3 dossiers + first skill file |
| T5 | Port patches 4–10 | L | 10 total commits + 10 dossiers; median time trending down |
| T6 | Write introduction document + README | S | Both files committed, evidence-driven |
| T7 | Retrospective + epic-2 handoff | S | Retrospective committed; epic-2 starting prompt drafted |

### 16.2 Epic 2 — Add 26.3 to Aiven ClickHouse

**Scope.** The remaining ~60 inventoried patches plus any system refinements observed during execution. Tag `v26.3.10.62-lts-aiven` at the end.

**Acceptance criteria:**
- All inventoried patches either ported or formally deferred (`status: dropped-in-v26.3` with rationale in the dossier).
- `v26.3.10.62-lts-aiven` release-line branch fast-forwarded to the end of `v26.3.10.62-lts-aiven-dev` after human sign-off.
- Tag `v26.3.10.62-lts-aiven` exists at the sign-off commit.
- Retrospective: cycle-time comparison against historical baseline (if available).

Epic-2's ticket breakdown is drafted as part of T7, informed by epic-1's measured per-patch data.

### 16.3 Session boundary

**Epic-2 starts in a new Cursor session.** The current session (this brainstorm + its execution) ends when the epic-1 retrospective is committed.

Three reasons:

1. **Context hygiene.** By the end of epic-1, the parent agent's context will contain this brainstorm, 10 patch dispatches, reflections, and skill extractions. Useful while doing them; clutter once done. A fresh session loads only the committed system files.
2. **The committed system is the handoff substrate.** `docs/aiven/AGENTS.md`, the schema, the hooks, the skills, the log, and especially the introduction document. The whole point of writing the introduction document at T6 is that a fresh session can read it and pick up.
3. **Mode shift.** Epic-1 is design + validation + learning (the parent is partly an architect). Epic-2 is execution at scale (the parent is a thin dispatcher). The prompts and context that suit one do not suit the other.

**Handoff verification.** The cleanest test that the handoff works: an engineer who opens the epic-2 session should NOT have access to this brainstorm. If the introduction document plus the committed system is enough for them to dispatch patch 11, the handoff works. If they need to read the brainstorm to figure out what to do, the introduction document is incomplete and is part of epic-1's unfinished work.

**Epic-2 starting prompt** (drafted in T7, refined when epic-2 starts): something on the order of *"Read `docs/aiven/uplifts/26.3/00-introduction.md` and `docs/aiven/uplifts/26.3/log.md`. Dispatch the classifier subagent on the unported patches. Propose the next patch and its dossier for human approval."* The system built in epic-1 is what does the work; the new session is just an orchestrator over it.

## 17. Appendix — what is and is not load-bearing

To make sure we never lose track of which parts of this design are core and which are conveniences:

**Load-bearing (touch only with strong reason and a follow-up reflection):**
- The seven invariants in §2 (including the testing invariant).
- The halt-and-escalate schema (§6) and the worker exit contract — especially the `tests` block and constraint #6.
- The eight items in `docs/aiven/AGENTS.md` (§7), including the testing invariant.
- The six hard guardrails G1–G5, G7 (§8). G6 deferred.
- The naming convention (stable numeric prefix, §5).
- The directory layout (§4) — specifically the durable-vs-ephemeral split.
- The "validate env before bootstrap" Step 0 (§10).

**Convenience (change without ceremony if observation suggests):**
- The `subagentStop` log columns (§9.1).
- The transient counter mechanism (§9.2).
- The exact classifier output columns (§10).
- The number of subagents we dispatch in the first cycle (§10 — could be one, if we choose to hand-pick patch 1 instead).

**Deferred (don't build until §11's trigger fires):**
- README, introduction, full inventory, runbooks, skill files, automation hooks, sanitizer matrix integration, parallel dispatch.

---

**Today you learned**
- The strongest single safety mechanism in an agent system is **the schema the agent must satisfy to claim success** — not the prompts, not the rules, not even the hooks. Hooks prevent irreversible actions; the schema prevents lying about success.
- "Human commits, agent proposes" is a stronger invariant than "agent commits but doesn't push" because it makes the most consequential action exclusively the human's. The cost is one extra `git commit` per patch — a price you'd happily pay for the accountability.

**Rule of thumb**
- For every artifact you are tempted to build pre-evidence (a skill, a runbook, an introduction doc): name the *trigger* that would make you build it for real. If you cannot name a trigger, you are speculating; defer.

**Next rabbit hole**
- The interaction between the halt-and-escalate schema and the per-patch dossier is the next thing to get right. Specifically: when the worker writes to `docs/aiven/patches/<NNN>-<slug>.md`, what's the minimum information it needs to add, and what's the maximum it should be allowed to change? The answer determines whether the dossier is the worker's notebook or the worker's output — and that distinction will surface naturally during patch 1's execution.

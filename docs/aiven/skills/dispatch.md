---
description: 'Worker prompt template and checklist for dispatching one Aiven patch port'
sidebarTitle: 'Dispatch a patch worker'
slug: '/aiven/skills/dispatch'
title: 'Skill — dispatch a patch worker'
doc_type: 'guide'
---

# Skill — dispatch a patch worker {#skill-dispatch-a-patch-worker}

Parent fills placeholders, then pastes this checklist into the worker prompt.
Keep the prompt short; link runbooks instead of inlining them.

## Worker prompt (template) {#worker-prompt}

```text
You are a patch worker for the Aiven LTS uplift.

Read and obey:
- docs/aiven/AGENTS.md
- docs/aiven/skills/dispatch.md (this checklist)
- docs/aiven/runbooks/porting-doctrine.md
- docs/aiven/schema/halt-and-escalate.md

Pins (from the uplift tracker):
  Work branch:    <<WORK_BRANCH>>
  Upstream base:  <<UPSTREAM_BASE>>
  Source tip:     <<SOURCE_TIP>>
  Prior dev line: <<PRIOR_DEV>>

Unit: <<UNIT_TITLE>>
Patch: <<NNN>> (<<SLUG>>); absorbs: <<ABSORBED or "none">>
Source commits: <<SOURCE_REFS>>  (prefer commit subjects; SHAs optional)
Dossier: docs/aiven/patches/<<NNN>>-<<SLUG>>.md
Ordering constraint: <<ORDERING or "none">>
Parent policy calls: <<PARENT_POLICY_CALLS or "none">>
Test naming: tests/queries/0_stateless/aiven_<<NNN>>_<<SLUG>>.*
  or tests/integration/test_aiven_<<SLUG>>/

Do NOT commit or create branches. Stage only. Final response = halt report
with a ready-to-run commit message under tmp/.
```

## Checklist (worker) {#checklist}

### 0. Preflight {#preflight}

- [ ] HEAD is `*-aiven-dev` (no feature branch)
- [ ] Bring the dossier in: `git checkout <<PRIOR_DEV>> -- <dossier>` if it
      exists there, otherwise create it from `dossier-template.md`
- [ ] Note touched files from `git show --stat` of each source commit

### 1. Screen (mandatory before apply) {#screen}

- [ ] What is this patch **for** on this line? (`porting-doctrine.md`,
      screening changes purpose)
- [ ] Grep key identifiers on current HEAD
- [ ] `git log <<UPSTREAM_BASE>>..HEAD -- <touched files>` — summarize
- [ ] Search for upstream equivalent fix, and for new doors into the same surface
- [ ] Conclusion: `still-needed-and-applies` | `still-needed-but-rewrite` |
      `obsoleted-by-upstream` | `irrelevant-by-removal`
- [ ] Write the screen phase of the dossier (`dossier-template.md`, section
      map): Summary, Lineage row, Background, Problem, Decisions (including the
      rung chosen), Drift on this uplift, and Security review's threat model
      and ways in — or its `n/a` line

If drop → stage the dossier alone, propose `patch-drop(<<NNN>>)`, halt (no code).

### 2. Stage {#stage}

- [ ] `git cherry-pick --no-commit <<SOURCE_REFS>>` **or** manual port
- [ ] Fold follow-up fixes and absorbed identities into this one change
      (`porting-doctrine.md`, consolidate at port time)
- [ ] Resolve conflicts minimally; record non-trivial rewrites in halt report
- [ ] Rename shipped `9NNN_*` tests from the prior line to `aiven_<NNN>_<slug>`
- [ ] Rename any fork-local setting to `aiven_*`; record old → new in Lineage
- [ ] Parent-instructed cleanups only if listed in policy calls

### 3. Review {#review}

- [ ] Skim `skills/cpp-review-checklist.md` — note real hits in halt report

### 4. Build {#build}

- [ ] Toolchain from `runbooks/build-and-test.md`
- [ ] Incremental `ninja -C <build> clickhouse` (log to file; summarize)

### 5. Test {#test}

- [ ] Design per `runbooks/testing.md` (+ `integration.md` if needed)
- [ ] TLS fixtures: cert **generator** + gitignore — never commit PEMs
- [ ] FAIL on parent / PASS with patch **or** justified `tests.added` enum
- [ ] Respect **stop budget** in `runbooks/workflow.md` — escalate, do not grind

### 6. Dossier + halt {#dossier-and-halt}

- [ ] Write the port phase: component tour → approach → concept → customer
      impact → security review → tests → rollback → retirement, this line's Lineage outcome, the
      `aiven_*` frontmatter keys, and a revised Summary. Fill the teaching
      sections from the code you read, not from the source commit message.
- [ ] Run the dossier self-check (`dossier-template.md`)
- [ ] Commit shape: `git diff --cached --name-only` shows `src/**`, `tests/**`
      and exactly one `docs/aiven/patches/*.md`, nothing else under `docs/aiven/`
- [ ] Done checklist in `workflow.md` mentally checked
- [ ] Write `tmp/…/commit-patch-NNN.txt` — readable why/what/verify
- [ ] No secrets in any artefact (`safety.md`)
- [ ] Halt report; `git status` shows intended staged set only
- [ ] **Do not** `git commit` or `git push`

## Parent preflight (before dispatch) {#parent-preflight}

| Clause | Check |
|---|---|
| (i) Hot files | Do not parallelize workers on the same high-churn files |
| (ii) Context | One subsystem per worker prompt |
| (iii) Symbols | Spot-check identifiers still exist or note rewrite |
| (iv) Reachability | Behavior still triggerable; else drop or `no_trigger_on_current_lts` |
| (v) Blast radius | Default-behavior → `policy_call` before dispatch |
| (vi) Submodule fork | Fork tip ready, or bake SHA into policy calls, else escalate |

## Source-author note {#source-author-note}

Preserve authorship when cherry-picking. For manual ports, put
`Original author: <email>` in the proposed commit body.

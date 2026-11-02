---
description: 'Day-to-day parent and worker loop for porting Aiven patches onto a ClickHouse LTS line'
sidebarTitle: 'Workflow'
slug: '/aiven/runbooks/workflow'
title: 'Runbook — workflow (parent + worker)'
doc_type: 'guide'
---

# Runbook — workflow (parent + worker) {#runbook-workflow}

Day-to-day loop for an LTS uplift. Keep this file short; deep commands live in
other runbooks.

## Parent loop {#parent-loop}

1. Confirm HEAD is `*-aiven-dev` and read the pins from the tracker.
2. Pick the next unit from the tracker's execution-plan view: lanes run in
   parallel, each lane in ascending ticket order. Order inside a ticket follows
   the unit's ordering constraint, then the owner's judgement
   (`execution-sequencing.md`).
3. For each unit:
   - Find its dossier on the prior `-dev` line
     (`git ls-tree --name-only <prior-dev> -- docs/aiven/patches/`). The worker
     brings it in while staging; a new patch starts from
     `skills/dossier-template.md`.
   - Build a worker prompt from `skills/dispatch.md`, pins filled in.
   - Dispatch **one** worker; do not parallelize conflicting file sets.
4. Review the halt report against **Definition of done** below. If `staged_ok`
   and Done holds, commit with `git commit -F tmp/…/commit-….txt`. If escalate
   or budget exhausted, resolve the policy, fork or test design, then
   re-dispatch.
5. Set the tracker row to `landed`. Keep full worker text under
   `tmp/uplift-<ver>/reports/` (not committed). The human reviews the branch
   and pushes; a fix found in review is a new commit, never an amend.

## Definition of done (one patch) {#definition-of-done}

A port is Done, and the parent may commit it, only when **all** hold:

| # | Criterion |
|---|---|
| 1 | Drift conclusion chosen and recorded as this line's row in the dossier Lineage table |
| 2 | Dossier is true for the ported code, every required section of the [section map](../skills/dossier-template.md#section-map) is present, and its self-check passes |
| 3 | Staged set is `src/**`, `tests/**` and exactly one `docs/aiven/patches/*.md` ([commit shape](../AGENTS.md#commit-shape)) |
| 4 | Build of affected targets succeeded (log under build dir or `tmp/`) |
| 5 | Tests: FAIL/PASS evidence pair **or** a justified `tests.added` enum value |
| 6 | Halt report is `staged_ok` (or explicit `dropped` with the dossier staged) |
| 7 | `tmp/…/commit-….txt` exists — human-readable why/what/verify (not jargon) |
| 8 | No secrets in staged files, dossier, halt report, or commit body (see `safety.md`) |

Local green on the named test is enough to commit; full Buildkite need not pass
on every single port. A patch whose dossier has a full
[Security review](../skills/dossier-template.md#section-map) still wants an
extra human look before merge to `-aiven`, and that reviewer checks the
section against the code.

## Worker stop budget (do not grind) {#worker-stop-budget}

Escalate with a clear reason instead of burning the context window:

| Limit | Action |
|---|---|
| **2** distinct unexpected conflict regions (after the expected site) | `escalate` / `conflict` |
| **2** full rebuild/fix-test redesign cycles without a new hypothesis | `escalate` / `test_design_blocked` or `policy_call` |
| Build or test wall time clearly runaway vs similar ports | stop, log path, escalate |
| Need a default-behavior / fleet-wide product call | `policy_call` immediately |
| Submodule fork tip missing | `external_dependency` immediately |

Parent may grant one more cycle in policy calls; workers must not self-extend
past these caps silently.

## Worker loop (summary) {#worker-loop}

1. Screen: purpose on this line, then drift → conclusion enum.
2. Stage: `git cherry-pick --no-commit <source>` or manual port, plus the
   dossier from the prior `-dev` line.
3. Test (FAIL/PASS pair) or justified skip.
4. Build affected targets; log to build dir / `tmp/`.
5. Make the dossier true for the ported code (teaching tone — see template).
6. Halt report + commit message file. **Do not commit or push.**

## Discovery {#discovery}

```bash
# patch commits on a line, by identity
git log --oneline --grep='^patch-'

# everything above the upstream base, to catch work without an identity
git log --oneline --no-merges ${UPSTREAM_BASE}..${SOURCE_TIP}

# a dossier and the full history of its patch across lines
ls docs/aiven/patches/*protected-users*
git log --follow --oneline -- docs/aiven/patches/022-protected-users-and-roles.md
```

## Batching {#batching}

- Group by subsystem (`StorageKafka`, object storage, Access, Refreshable MV, …).
- Warm up on small / clean ports before hard rewrites.
- Hold blast-radius default changes for explicit `policy_call`.
- Replication-core / high-churn files last when possible.

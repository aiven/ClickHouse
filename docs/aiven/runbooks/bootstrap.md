---
description: 'How to carry the Aiven uplift system onto a fresh upstream LTS line before any patch is ported'
sidebarTitle: 'Bootstrap a new LTS'
slug: '/aiven/runbooks/bootstrap'
title: 'Runbook — bootstrap a new LTS'
doc_type: 'guide'
---

# Runbook — bootstrap a new LTS {#runbook-bootstrap-a-new-lts}

Carry the reusable Aiven infrastructure onto a fresh upstream peel. Patch ports
and dossiers are **not** part of bootstrap: each dossier arrives with its own
patch.

## What the bootstrap carries {#what-the-bootstrap-carries}

| Path | Notes |
|---|---|
| `docs/aiven/AGENTS.md` | Law |
| `docs/aiven/README.md` | Map |
| `docs/aiven/schema/**` | Contracts |
| `docs/aiven/runbooks/**` | Procedures and doctrine |
| `docs/aiven/skills/**` | Checklists |
| `docs/aiven/patches/README.md` | Dossier rules; the directory is otherwise empty |
| `.cursor/hooks/**`, then `.cursor/hooks.json` | Push and rebase deny hook (`chmod +x` before `hooks.json` exists) |
| `.buildkite/**` | Aiven CI (pipeline, provision, sccache, …) |

**Not** carried: `docs/aiven/patches/<NNN>-*.md`, `src/**`, `tests/**`,
`contrib/**`, and anything describing one uplift — plans, ledgers, pins,
reports, retrospectives. Those live in the tracker or in `tmp/`.

## Steps {#steps}

1. Create `v<VER>-lts-aiven` and `v<VER>-lts-aiven-dev` from the **peel** of
   the frozen upstream tag (human).
2. Record the pins in the tracker: upstream base, work branch, source tip of
   the previous line, and the previous `-dev` line the dossiers come from.
3. On `-aiven-dev`, land the bootstrap as two commits, so CI can later be
   squashed into an earlier Buildkite commit without dragging docs along:
   - `ci:` — `.buildkite/**` and related CI-only tweaks
   - `docs(aiven):` — `docs/aiven/**` and `.cursor/**`

   Copy the hook scripts, `chmod +x` them and verify with static stdin
   (`runbooks/safety.md`) **before** writing `hooks.json` — a fail-closed hook
   that cannot execute blocks all Shell use the moment `hooks.json` names it.
4. Snapshot the previous line into the tracker: one row per commit above its
   upstream base, then group the rows into target units. Every Aiven commit
   must be claimed by a unit; the unclaimed filter is the completeness check.
5. Point Buildkite at the new branch; keep CI green before mass ports.

## Already folded in {#already-folded-in}

Lessons from earlier uplifts that the current tree already encodes. Do not
re-litigate these:

| Lesson | Where it lives now |
|---|---|
| `patch-port(NNN):` / `patch-drop` / `patch-new` / `patch-fix` subjects | `commit-hygiene.md` |
| Verbatim source subject + `Original author:` | `commit-hygiene.md`, `dispatch.md` |
| Command-literals-first commit proposals | `commit-hygiene.md`, halt report |
| Workers stage, the parent commits, humans push | `AGENTS.md`, `commit-hygiene.md` |
| No feature branches; worktree not branch | `AGENTS.md` |
| Submodule fork = human prep; agent read-only | `submodule-forks.md`, clause (vi) |
| Stateless `aiven_<NNN>_<slug>` + integration `test_aiven_<slug>/` | `testing.md`, Buildkite helpers |
| Evidence FAIL/PASS pair + worktree flip | `build-and-test.md` |
| Schema: `no_justified` / `no_source_change` / `no_trigger_on_current_lts` | `halt-and-escalate.md` |
| Parent preflight (i)–(vi) | `dispatch.md` |
| Policy gate for default-behavior blast radius | `AGENTS.md`, clause (v) |
| Planning state and pins only in the tracker | `AGENTS.md`, `README.md` |
| Dossier ships inside its patch commit | `AGENTS.md`, `commit-hygiene.md` |
| `.buildkite/**` lands in its own `ci:` commit | this file, `commit-hygiene.md` |
| Reports and scratch in `tmp/`, never committed | `README.md`, `workflow.md` |
| Definition of done + worker stop budget | `workflow.md` |
| Secrets / hook verify (static stdin only) | `safety.md` |
| Policies that outlive one uplift | `porting-doctrine.md` |

## New lessons {#new-lessons}

When a port teaches something **not** in the table above, land it as its own
`docs(aiven):` commit — in `porting-doctrine.md` if it is a rule for every port,
in the relevant runbook if it is a procedure. A lesson about one patch belongs
in that patch's dossier, inside the patch commit.

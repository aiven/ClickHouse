# Aiven LTS uplift system (lean)

`docs/aiven/` is the orchestration layer for carrying Aiven's downstream patches
onto each ClickHouse LTS. Agents read [`AGENTS.md`](AGENTS.md) first (the law);
this file is the map.

## Model

1. **Parent** owns the uplift on `*-aiven-dev`.
2. **Worker** ports one patch (or a small dependency chain), proves causation with
   a test, updates the dossier, returns a halt-and-escalate report.
3. **Human** commits. Agents stage only.

Identity of a patch is the **dossier slug** (`022-protected-users`), discovered
via `git log --grep '^patch-port('` / `patch-new(` / `patch-drop(`. Commit SHAs
are optional evidence tips — they rot after reslices; do not treat them as keys.

**No inventory.md by default.** Source of truth is git subjects + dossiers.
Add an inventory later only if a classifier still earns its keep.

## Layout

| Path | Purpose | Carry to next LTS? |
|---|---|---|
| [`AGENTS.md`](AGENTS.md) | Invariants | yes (A) |
| [`schema/`](schema/) | Exit-report contract | yes (A) |
| [`runbooks/`](runbooks/) | How to bootstrap, build, test, sequence | yes (A) |
| [`skills/`](skills/) | Short dispatch / dossier / C++ checklists | yes (A) |
| `patches/<NNN>-<slug>.md` | Durable dossiers | yes (import) |
| `uplifts/<version>/` | Thin per-uplift stub (intro + execution plan) | no (B) |
| `.cursor/hooks*` | Enforce no-commit / no-push / never-touch | yes (A) |
| `.buildkite/**` | Aiven CI pipeline | yes (A) |

Not carried as living bootstrap: `proposals/`, `plans/`, worker `reports/` dumps
(use `tmp/`), fat inventories, retrospective novels. Prior LTS history stays on
the previous `-aiven` branch in git.

## Start here (26.8)

1. [`AGENTS.md`](AGENTS.md) — invariants  
2. [`uplifts/26.8/00-introduction.md`](uplifts/26.8/00-introduction.md) — this line's pins  
3. [`runbooks/bootstrap.md`](runbooks/bootstrap.md) — what category A includes  
4. [`runbooks/workflow.md`](runbooks/workflow.md) — day-to-day parent/worker loop  
5. [`skills/dispatch.md`](skills/dispatch.md) — worker checklist  

## What this lean tree already includes

26.3’s many `docs(aiven):` follow-ups are **folded in** (not deferred): subject
grammar, naming, schema enums, no-feature-branch, submodule forks, preflight
(i)–(vi), evidence pairs, command-literal commits, pins-in-one-place. See the
table in [`runbooks/bootstrap.md`](runbooks/bootstrap.md).

Still lean vs 26.3: no inventory, no `log.md`, no `reports/` in git, no
proposals/plans novels, short checklists instead of 900-line templates.

**New** A follow-ups only when 26.8 teaches something not already in that table.

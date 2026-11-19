---
description: 'Contract for the report every Aiven patch worker returns on exit'
sidebarTitle: 'Halt-and-escalate report'
slug: '/aiven/schema/halt-and-escalate'
title: 'Halt-and-escalate report'
doc_type: 'reference'
---

# Halt-and-escalate report {#halt-and-escalate-report}

Every worker's **final** response must be this report (success or fail).
Keep it short; put long logs under `tmp/`, not in the report body.

## Format {#format}

```markdown
# Halt report — patch <NNN> (<slug>)

## Status
one of: `staged_ok` | `halt` | `escalate`

## Outcome
one of: `ported` | `dropped` | `blocked` | `policy_call` | `external_dependency`

## Identity
- slug: `<NNN>-<slug>`
- absorbed identities: `…` or none
- dossier: `docs/aiven/patches/<NNN>-<slug>.md` — brought from `<prior-dev>` | new
- source subject / tip (optional SHA): `…`
- proposed commit subject: `patch-port(<NNN>): <verbatim source subject>` |
  `patch-fix(<NNN>): …` | `patch-drop(<NNN>): …` | `patch-new(N<nn>): …`
- proposed commit body path: `tmp/…/commit-….txt` (human why/what/verify prose;
  see `runbooks/commit-hygiene.md`)

## Drift conclusion
one of: `still-needed-and-applies` | `still-needed-but-rewrite` |
`obsoleted-by-upstream` | `irrelevant-by-removal`

## What changed (files)
- path — one line each (empty if `tests.added: no_source_change`)

## Tests
- added: `yes` | `no_justified` | `no_source_change` | `no_trigger_on_current_lts`
- kind: `stateless` | `integration` | `unit` | `n/a`
- paths: `…` (e.g. `aiven_022_protected_users`)
- pre-patch: FAIL evidence (command + decisive lines) **or** n/a
- post-patch: PASS evidence **or** n/a
- justification / upstream_reference: …

## Build
- configure/build: ok / fail (log path under build dir or `tmp/`)

## Conflicts / rewrites
- none | bullet list of non-trivial resolutions (not a novel)

## Escalation (if Status ≠ staged_ok)
- reason code: `conflict` | `build_fail` | `test_fail` | `test_design_blocked` |
  `policy_call` | `external_dependency` | `other`
- what the human must decide or prepare
- recommended next command(s)

## Staged?
- `git status` summary: clean staged set for the proposed commit (yes/no)
```

## `tests.added` meanings {#tests-added-meanings}

| Value | When | Required evidence |
|---|---|---|
| `yes` | New Aiven test | Pre FAIL + post PASS pair |
| `no_justified` | Covered by existing upstream test | Path + why; ran it green on staged tree |
| `no_source_change` | Read-only worker (classifier, etc.) | State role; no `src/**` staged |
| `no_trigger_on_current_lts` | Ported code cannot be triggered on this LTS (API gone / feature removed) but kept for forward insurance | Cite recount evidence under `tmp/` |

## Rules {#rules}

- Prefer **slug + proposed subject** over SHAs.
- Do not paste multi-page logs; link `tmp/…` or build log paths.
- `staged_ok` means the parent can commit as proposed without further worker
  effort.
- `dropped` still stages the dossier, with the drop reason in its Lineage
  table, and proposes a `patch-drop(<NNN>)` commit containing only that file.
- Every patch commit stages exactly one dossier and nothing else under
  `docs/aiven/` (`AGENTS.md`, commit shape).

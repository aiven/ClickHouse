# Halt-and-escalate report

Every worker's **final** response must be this report (success or fail).
Keep it short; put long logs under `tmp/`, not in the report body.

## Format

```markdown
# Halt report — patch <NNN> (<slug>)

## Status
one of: `staged_ok` | `halt` | `escalate`

## Outcome
one of: `ported` | `dropped` | `blocked` | `policy_call` | `external_dependency`

## Identity
- slug: `<NNN>-<slug>`
- source subject / tip (optional SHA): `…`
- proposed commit subject: `patch-port(<NNN>): <verbatim source subject>` |
  `patch-drop(<NNN>): …` | `patch-new(N<nn>): …`
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

## `tests.added` meanings

| Value | When | Required evidence |
|---|---|---|
| `yes` | New Aiven test | Pre FAIL + post PASS pair |
| `no_justified` | Covered by existing upstream test | Path + why; ran it green on staged tree |
| `no_source_change` | Read-only worker (classifier, etc.) | State role; no `src/**` staged |
| `no_trigger_on_current_lts` | Ported code cannot be triggered on this LTS (API gone / feature removed) but kept for forward insurance | Cite recount evidence under `tmp/` |

## Rules

- Prefer **slug + proposed subject** over SHAs.
- Do not paste multi-page logs; link `tmp/…` or build log paths.
- `staged_ok` means the human can commit as proposed without further agent work.
- `dropped` still updates the dossier and proposes the docs-side commit per
  `runbooks/commit-hygiene.md`.

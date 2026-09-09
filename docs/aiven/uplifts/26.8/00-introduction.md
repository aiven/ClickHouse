# 26.8 uplift — introduction

Per-uplift pins and status for carrying Aiven patches onto upstream **26.8**.
System map: [`../../README.md`](../../README.md). Law: [`../../AGENTS.md`](../../AGENTS.md).

## Pins (update if the frozen tag changes)

| Role | Value |
|---|---|
| Upstream tag | `v26.8.2.7-lts` |
| Upstream peel | `c2930599c56702022276de06657ed51d83f31a05` |
| Work branch | `v26.8.2.7-lts-aiven-dev` |
| Release line | `v26.8.2.7-lts-aiven` |
| Source overlay | `v26.3.32.14-lts` .. `v26.3.32.14-lts-aiven` |
| Source overlay (planning) | `v26.3.26.3-lts` .. `v26.3.26.3-lts-aiven` — advanced 2026-09, adding `N06` |
| Scratch | `tmp/uplift-26.8/` |

## Status

| Phase | State |
|---|---|
| Peel + branches | done |
| Buildkite CI foundation | in progress / landed on `-aiven-dev` |
| Lean `docs/aiven` bootstrap | **this tree** (review) |
| Dossier import | **not yet** (separate commit) |
| Inventory | done — `inventory.md` (87 identities: 70 planned, 5 landed, 8 dropped, 4 folded) |
| Patch ports | in progress |

## Method

Major-line uplift (diverged from 26.3). Full re-port via parent/worker loop
(`runbooks/workflow.md`). Do not bulk-replay with cherry-pick scripts.

## Companion files (this directory)

| File | Role |
|---|---|
| `inventory.md` | Per-patch ledger: group, ticket, status, basis |
| `execution-plan.md` | Ticket structure, ordering constraints, decisions |
| `http-endpoint-inventory.md` | Focused policy matrix for unified patch `040` |
| `customer-impact.md` | Aggregated fleet-visible delta; appended at port time |

Source of truth for the uplift is `inventory.md` + `execution-plan.md`. Jira
mirrors them: one issue per ticket, holding a status and a link to the plan.

Engineering rules common to every port are in
`../../runbooks/porting-doctrine.md`, kept out of this directory because they
outlive the uplift.

Progress = `git log --grep='^patch-port('` / dossiers. No `log.md`.
Worker full reports → `tmp/uplift-26.8/reports/` (not committed).

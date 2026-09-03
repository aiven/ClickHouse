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
| Source overlay | `v26.3.26.3-lts` .. `v26.3.26.3-lts-aiven` |
| Scratch | `tmp/uplift-26.8/` |

## Status

| Phase | State |
|---|---|
| Peel + branches | done |
| Buildkite CI foundation | in progress / landed on `-aiven-dev` |
| Lean `docs/aiven` bootstrap | **this tree** (review) |
| Dossier import | **not yet** (separate commit) |
| Inventory | **skipped** (use git subjects + dossiers) |
| Patch ports | in progress |

## Method

Major-line uplift (diverged from 26.3). Full re-port via parent/worker loop
(`runbooks/workflow.md`). Do not bulk-replay with cherry-pick scripts.

## Companion files (this directory)

| File | Role |
|---|---|
| `execution-plan.md` | Subsystem order (fill when ports start) |
| `http-endpoint-inventory.md` | Focused policy matrix for unified patch `040` |

Progress = `git log --grep='^patch-port('` / dossiers. No `log.md`.
Worker full reports → `tmp/uplift-26.8/reports/` (not committed).

# Aiven LTS uplift system

This subtree (`docs/aiven/`) is the AI-orchestration system Aiven uses to carry
its downstream patch set forward onto each new ClickHouse LTS release. It exists
because the uplift is a large, repetitive, high-risk task — re-applying dozens of
fork patches onto a moved upstream tree — that is well suited to dispatched AI
workers operating under tight, auditable invariants.

If you are an agent that just landed in this subtree, read
[`AGENTS.md`](AGENTS.md) first — it is the invariant contract and is auto-loaded.
This README is the map; `AGENTS.md` is the law.

## The model in one paragraph

A **parent agent** drives one uplift. It classifies the fork's patch backlog
(T2), then for each patch dispatches a **worker subagent** (T3.X) that ports the
single patch, writes a test proving causation, and returns a structured
halt-and-escalate report. The parent reviews, the human commits. Every dispatch
leaves a durable paper trail: a per-patch **dossier**, a **work-log** row, and —
when something is learned — a **retrospective**.

## Directory layout

| Path | What lives here | Carried to next LTS? |
|---|---|---|
| [`AGENTS.md`](AGENTS.md) | Invariants every agent must obey (branch rules, never-touch list, test requirement, escalation contract). | yes |
| [`schema/`](schema/) | Machine-readable contracts — notably the worker's exit report (`halt-and-escalate.md`). | yes |
| [`skills/`](skills/) | Procedural how-tos the parent/worker invoke (dispatch-prompt template, C++ review checklist, dossier template). | yes |
| [`runbooks/`](runbooks/) | Operational guides (build & test, commit hygiene, test-suite choice, safety rules, execution sequencing). | yes |
| [`proposals/`](proposals/) · [`plans/`](plans/) | Design rationale and the original epic/plan breakdown. | yes |
| `patches/<NNN>-<slug>.md` | Durable per-patch **dossiers** — the lineage, purpose, verification, and outcome of each patch across uplifts. | yes (wholesale) |
| `uplifts/<version>/` | Everything specific to one uplift: the patch **inventory**, the dispatch **log**, **retrospectives**, and worker **reports**. | **no** (frozen record) |

The split in the last column is the load-bearing distinction. Reusable
infrastructure (the first six rows, **category A**) cherry-picks cleanly to the
next LTS; the per-uplift record (**category B**) stays behind as history; the
patch code + dossier + inventory annotation travel together as atomic
**category C** commits. The full policy, including the commit-subject grammar
`patch-port(NNN):` / `patch-drop(NNN):` and the next-LTS bootstrap checklist,
lives in [`runbooks/commit-hygiene.md`](runbooks/commit-hygiene.md).

## Starting a new uplift

When a new upstream LTS tag arrives, follow
[`runbooks/commit-hygiene.md`](runbooks/commit-hygiene.md) §3 — it gives the exact
cherry-pick sequence to carry the bootstrap forward, the wholesale dossier
import, and the list of version-hardcoded strings to re-point. Then create
`uplifts/<new-version>/` and dispatch the T2 classifier.

## Where to read next

- The invariants: [`AGENTS.md`](AGENTS.md)
- The worker exit contract: [`schema/halt-and-escalate.md`](schema/halt-and-escalate.md)
- How a dispatch prompt is built: [`skills/dispatch-prompt-template.md`](skills/dispatch-prompt-template.md)
- Commit categories & history hygiene: [`runbooks/commit-hygiene.md`](runbooks/commit-hygiene.md)
- The design rationale: [`proposals/2026-05-19-uplift-orchestration-design.md`](proposals/2026-05-19-uplift-orchestration-design.md)
- A concrete uplift's intro: `uplifts/<version>/00-introduction.md`

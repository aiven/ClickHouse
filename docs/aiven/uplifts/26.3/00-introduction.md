# 26.3 uplift — introduction

This directory is the per-uplift record for carrying Aiven's fork patch set onto
upstream tag `v26.3.10.62-lts`. Work happens on branch
`v26.3.10.62-lts-aiven-dev`; the release-line branch `v26.3.10.62-lts-aiven` is
fast-forwarded only after human sign-off.

For the system as a whole, read [`../../README.md`](../../README.md) and the
invariants in [`../../AGENTS.md`](../../AGENTS.md). This file is the orientation
for *this* uplift specifically.

## Scope

The fork carried **78 patches** on the previous line (`v25.8.18.1-lts-aiven`),
catalogued by the T2 classifier in [`inventory.md`](inventory.md). This uplift
processed the first tranche — the patches whose behavior was most likely to have
drifted under upstream and that were highest-value to verify first.

## Status

**12 of 78 patches processed** — 10 ported, 2 dropped. Identify them on the
branch with `git log --grep '^patch-port('` and `git log --grep '^patch-drop('`.

| Outcome | Patches |
|---|---|
| Ported | 005, 006, 010, 011, 040, 042, 049, 060, 073, 077 |
| Dropped | 001 (`obsoleted-by-upstream`), 007 (`irrelevant-by-removal`) |

Each processed patch has a durable dossier under
[`../../patches/`](../../patches/) (its `§0` lineage cell names the carry commit
by its stable `patch-port(NNN)` / `patch-drop(NNN)` handle). The remaining 66
inventory rows are un-processed and require no decision for this handover.

## How to navigate this uplift

- [`inventory.md`](inventory.md) — the full 78-patch catalogue (immutable T2
  output) with per-uplift drop annotations inline in the subject column.
- [`execution-plan.md`](execution-plan.md) — the suggested dispatch order and
  subsystem grouping for the remaining patches.
- [`log.md`](log.md) — the dispatch work log, one row per subagent run.
- `NN-...-retrospective.md` — what each dispatch (or cross-cutting incident)
  taught us; numbered in dispatch order. Start at
  [`01-t2-classifier-retrospective.md`](01-t2-classifier-retrospective.md).
- `reports/` — the verbatim worker halt-and-escalate summaries the log rows link
  to.

## Reading the dispatch numbering

Dispatches are labelled **T2** (the one-time classifier) and **T3.X** (per-patch
port workers, in dispatch order). A single patch can span several T3.X
dispatches when verification needed a redesign (e.g. patch 060 spans T3.10–T3.12,
patch 049 spans T3.13–T3.14); the retrospective filenames reflect those spans.
The authoritative record of *what each dispatch did* is its retrospective — the
log is the index, the retrospectives are the story.

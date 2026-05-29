# 26.3 execution plan — order & grouping

Derived per [`../../runbooks/execution-sequencing.md`](../../runbooks/execution-sequencing.md).
This is the suggested dispatch order for the remaining patches in
[`inventory.md`](inventory.md). It is **advisory** — dossiers and retrospectives
remain the authoritative per-patch record.

**Status:** 12 of 78 processed (10 ported, 2 dropped) — see
[`00-introduction.md`](00-introduction.md). The 66 below are the remaining work.

## Phased grouping

Phases run in order of increasing risk/coupling; within a phase the subsystem
groups are independent and parallelize across sessions. `A→B` denotes a
dependency chain (port A before B).

| Phase | Group | Patches | Notes |
|---|---|---|---|
| **1** | Warm-up (small, mostly clean, independent) | 037, 039, 044, 035, 047, 056, 020, 043 | tiny diffs; validate the pipeline + build retro signal cheaply |
| **2** | Object storage (S3/Azure) | 012→013, 023→024, 025, 027, 028, 015→016, 026, 048 | one subsystem; 026 (343 loc) / 015 (297 loc) are the heavy ones |
| **2** | Kafka | 030, 029, 031, 032→076, 033 | port 032 before 076 (offset-reset extension) |
| **2** | Dictionaries / PostgreSQL | 045, 061, 036, 034 | 034 (322 loc) last |
| **3** | TLS / SSL / access | 017, 018, 065, 067, 059, 014, 021, 019, **022** | 022 protected-users (847 loc) last — warmest context |
| **3** | ZK/Keeper, system tables, settings | 038, 054, 069, 053, 055, 068, 041 | |
| **3** | Engine/flag toggles | 052, 070→071, 074→075 | YTsaurus & ArrowFlight are sibling pairs |
| **4** | Replication core (highest value + drift risk) | 002, 003, 008, 009, 046, 072, 050, **066**, 078 | most likely to have drifted on 26.3; 066 = 1011 loc — warm context + care |
| **4** | `.tmp` family (dependency chain) | 062→063→064 | strict order |

## Held — human gating decision first (clause v)

These trip (or may trip) the clause-(v) blast-radius screen
([`../../skills/dispatch-prompt-template.md`](../../skills/dispatch-prompt-template.md)).
Get the gating decision (a `policy_call` — typically a default-off server
setting) **before** any worker dispatch; schedule the code once the gate is
decided.

| Patch | Why held |
|---|---|
| **004** | Replace `MergeTree` with `ReplicatedMergeTree` in Replicated databases — the canonical clause-(v) case (`enforce_table_replication`-style gate) |
| **058** | Disallow replication parameter customization — changes a default |
| **057** | Remove all cloud-specific settings (610 loc) — removes settings broadly |
| **062** | Prohibit `.tmp` table creation — prohibits-by-default; re-screen (gates the `.tmp` chain 062→063→064) |
| **051** | Disable various table engines/functions — default-behavior toggle; re-screen |

## How to use this with the pipeline

- Pick the next group; within it follow the `→` dependency order and do the
  small/clean patches before the large ones.
- Independent Phase-2/3 groups can run concurrently in separate sessions.
- When a held patch's gating decision lands, slot its code wherever the relevant
  subsystem context is warm (e.g. 062 with the `.tmp` chain, 004/058 with the
  replication-core phase).

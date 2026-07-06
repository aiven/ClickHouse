# Execution sequencing — ordering the patch backlog

The inventory (`uplifts/<version>/inventory.md`) is sorted by patch number, which
is just author-date order. That is **not** a good execution order. Before
dispatching the T3.X workers for an uplift, derive a deliberate order and
grouping and record it in `uplifts/<version>/execution-plan.md`. This runbook is
the reusable method; the per-uplift plan is its output.

## Principles

1. **Group by subsystem, not by number.** Both the parent preflight (clauses
   i–iv in `skills/dispatch-prompt-template.md`) and the worker must load the
   C++ context for the patch's area (`StorageKafka`, the object-storage layer,
   `DatabaseReplicated`, …). Porting a whole subsystem consecutively amortizes
   that context across the group — the same batch-processing logic ClickHouse
   applies to data, applied to the pipeline. Sibling dossiers cross-reference,
   and drift discovered in one patch informs the rest of the group.
2. **Dependency order within a group.** Some patches are sequential commits
   building one feature; porting them out of order causes cherry-pick conflicts
   or semantic gaps. Detect chains by consecutive source SHAs and by shared
   touched files (the inventory's `files`/`sha` columns).
3. **Warm up on small / clean / low-risk patches first.** Validate the pipeline
   on cheap patches (small `loc`, `cherry_pick_clean=yes`, no behavior change)
   before betting it on the largest one. This also produces early retrospective
   signal while the stakes are low.
4. **Defer blast-radius patches to a human policy decision.** Any patch that
   trips the clause-(v) screen (a default-behavior change with broad blast
   radius — see `skills/dispatch-prompt-template.md`) is *held*: get the gating
   decision (`policy_call`, e.g. a default-off server setting) **before** any
   worker dispatch, so it never blocks a whole group mid-flight.
5. **Largest / most-coupled last, with warm context.** The biggest patches and
   the most replication-coupled ones (the highest-value, highest-drift-risk
   area) go at the end of their group or the end of the run, when the relevant
   subsystem context is already warm from the smaller siblings.

## Phasing model

Order phases by increasing risk and coupling; within a phase the subsystem
groups are independent and parallelize across sessions:

1. **Warm-up** — small, clean, independent patches.
2. **Independent subsystems** — object storage, Kafka, dictionaries, engine/flag
   toggles. Cohesive and low cross-coupling; run in parallel.
3. **Moderately coupled** — security/TLS/access, system tables & settings.
4. **Replication core** — `DatabaseReplicated`, refreshable-MV, the heaviest and
   most drift-prone patches, done last with warm context.

**Held (cross-cutting):** the clause-(v) set, scheduled into code only after its
gating decision lands — independent of the phase order.

## Output

Write the result to `uplifts/<version>/execution-plan.md` as a phase → group →
patch-list table, with dependency chains (`A→B`) and the held clause-(v) set
called out explicitly. The plan is **advisory**: it sets dispatch order, but the
per-patch dossier and retrospective remain the authoritative record of what
actually happened. Re-derive it per uplift — subsystem clustering and dependency
chains shift as upstream and the fork evolve.

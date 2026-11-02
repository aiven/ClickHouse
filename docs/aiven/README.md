# Aiven LTS uplift system {#aiven-lts-uplift-system}

`docs/aiven/` is how Aiven carries its downstream patches onto each ClickHouse
LTS. Agents read [`AGENTS.md`](AGENTS.md) first (the law); this file is the map.

## Where each kind of fact lives {#where-facts-live}

Every fact has one home. The sorting question is whether it describes the
current uplift, one patch, or every port.

| Kind of fact | Home | Why there |
|---|---|---|
| What the source line carries, grouping, order, status, owner, pins | Uplift tracker (Notion) | Changes weekly and dies with the uplift |
| Why one patch exists, how it works, what it changes for customers, how it was decided | `patches/<NNN>-<slug>.md` | Must travel with the code, reviewed in the same commit |
| Rules that apply to every port | `runbooks/porting-doctrine.md` | Outlives every uplift |
| Procedures, checklists, report contract | `runbooks/`, `skills/`, `schema/` | Outlives every uplift |
| Which commits a port replaces | The port commit itself | Fixed once written |

The tracker owns a patch while its uplift is in flight. Once the patch lands,
its outcome is frozen into the dossier's Lineage table in the same commit, and
the dossier is authoritative from then on. Nothing is maintained in both places.

There is no `uplifts/` directory, no inventory file and no execution plan in
the repository. A file that cannot exist on the next LTS line cannot be part of a
port commit, and a port commit is what carries the knowledge forward.

## Model {#model}

1. **Parent** owns the uplift on `*-aiven-dev` and picks work from the tracker.
2. **Worker** ports one patch (or a small dependency chain), proves causation
   with a test, makes the dossier true for the ported code, and returns a
   halt-and-escalate report.
3. **Parent** commits once the definition of done holds. **Human** reviews the
   branch and pushes.

The identity of a patch is its dossier slug (`022-protected-users-and-roles`).
Commit subjects carry the number (`patch-port(022):`), so
`git log --grep='^patch-'` finds every patch commit on a line. SHAs are evidence,
not keys: every rebase re-dates them.

## Layout {#layout}

| Path | Purpose | Carried to the next LTS |
|---|---|---|
| [`AGENTS.md`](AGENTS.md) | Invariants | in the bootstrap commit |
| [`schema/`](schema/) | Exit-report contract | in the bootstrap commit |
| [`runbooks/`](runbooks/) | How to bootstrap, port, build, test, sequence | in the bootstrap commit |
| [`skills/`](skills/) | Dispatch, dossier and C++ review checklists | in the bootstrap commit |
| `patches/<NNN>-<slug>.md` | Durable per-patch dossiers | inside each patch's own commit |
| `.cursor/hooks*` | Deny push and rebase | in the bootstrap commit |
| `.buildkite/**` | Aiven CI pipeline | in its own `ci:` commit |

Not carried: worker reports and scratch work (`tmp/`), conflict diaries,
retrospectives. Prior LTS history stays on the previous `-aiven` branch in git.

## Start here {#start-here}

1. [`AGENTS.md`](AGENTS.md) — invariants
2. The uplift tracker — pins and the next unit of work
3. [`runbooks/bootstrap.md`](runbooks/bootstrap.md) — what a new line carries
4. [`runbooks/workflow.md`](runbooks/workflow.md) — day-to-day parent/worker loop
5. [`runbooks/porting-doctrine.md`](runbooks/porting-doctrine.md) — rules for every port
6. [`skills/dispatch.md`](skills/dispatch.md) — worker checklist

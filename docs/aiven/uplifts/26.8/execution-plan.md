---
description: 'Ticket structure, ordering constraints and decisions for the Aiven ClickHouse 26.8 patch uplift'
sidebarTitle: 'Execution plan'
slug: '/aiven/uplifts/26.8/execution-plan'
title: 'Aiven 26.8 uplift execution plan'
doc_type: 'guide'
---

# Aiven 26.8 uplift execution plan {#aiven-26-8-uplift-execution-plan}

Order and ownership for carrying the Aiven patch set onto 26.8. Method:
[`../../runbooks/execution-sequencing.md`](../../runbooks/execution-sequencing.md).
The per-patch ledger is [`inventory.md`](inventory.md); each patch's dossier
under [`../../patches/`](../../patches/) records why and how it was ported. The
engineering rules that apply to every port, whatever its subsystem, are in
[`../../runbooks/porting-doctrine.md`](../../runbooks/porting-doctrine.md);
what tenants and operators see is aggregated in
[`customer-impact.md`](customer-impact.md).

This file and the inventory are the source of truth. Jira holds one issue per
ticket — a title, a status, and a link to the ticket's section here. Where a
Jira description disagrees with this file, this file wins.

## Why there is now a global inventory {#why-a-global-inventory}

The earlier revision of this plan said there was no global patch inventory
table, and the sequencing runbook still says not to maintain one "unless
needed." That was right when it was written: ports were taken one at a time,
and a 70-row table would have been unmaintained ceremony.

The condition has since been met. There are 70 open identities across ten
tickets, the source line moved under us once already (adding `N06`), and six
commits on that line carry no patch number at all. At that size the ledger is
cheaper than rediscovering the set, so [`inventory.md`](inventory.md) exists.
The runbook's default still stands for small uplifts.

## Structure {#structure}

A **ticket is a track**: a coherent slice of one or two subsystems, owned by one
person. Its **bullets are the commit boundaries** — one dossier, one patch
commit plus its test.

All of this work happens on **one dev branch**, not a branch per ticket.
Documentation commits stay separate from code commits, as
[`../../runbooks/commit-hygiene.md`](../../runbooks/commit-hygiene.md) requires,
but they share the branch: a per-ticket branch would either stack on this one,
which gets no CI, or duplicate it. Ticket boundaries are therefore an ownership
and reporting device, not a branching one.

| # | Ticket | Lane | Bullets | Size |
|---|---|---|---|---|
| 0 | Planning — **exists, in progress** | any | 3 | M |
| 1 | Protected access control | security | 3 | XL |
| 2 | Attack-surface reduction | security | 4 | L |
| 3 | Object storage | storage | 3 | XL |
| 4 | Transport security | storage | 3 | L |
| 5 | Replication core | replication | 3 | XL |
| 6 | Replication smalls, merges, settings | replication | 4 | XL |
| 7 | Kafka | integrations | 3 | M |
| 8 | Materialized views | integrations | 2 | XL |
| 9 | Integrations and dictionaries | integrations | 3 | L |
| 10 | TimeSeries, ops and packaging | integrations | 2 | M |

Four lanes run in parallel with little cross-talk; the integrations lane holds
four tickets and is the natural place to add a second owner.

### Ticket numbers are the order within a lane {#ticket-order}

Ticket 0 goes first for everyone. After that, **a lane works its tickets in
ascending number order, and lanes do not wait for each other**: the security
lane finishes Ticket 1 before opening Ticket 2 while the storage lane is
somewhere in Ticket 3, which is the whole point of having lanes.

Inside a ticket, bullets are ordered by their own dependencies — see
[ordering constraints](#ordering-constraints) — and otherwise by whoever owns
the ticket. **A note inside a ticket about which bullet to take is scoped to
that ticket and never establishes what the next port is overall.** This is
written down because the ambiguity is real: an earlier revision described a
Ticket 2 bullet as "the natural next single port," language inherited from the
pre-ticket phase when ports were picked one at a time by theme, and it read as
a claim about global order.

### Sizing: lines of code are a decoy {#sizing}

A ticket's size is its largest bullet, and size is **not** lines of code. `006`
was one file and two lines, and it consumed a full session: an integration test,
a pre-patch binary to prove the RED, a cross-LTS `assert_cast` audit, and a
fleet query. `N07` is six files and brings a two-node integration fixture. Line
counts sit in the inventory because they were free to collect; they are not the
estimate.

Size on the two axes that drive cost:

- **Drift** — how far upstream moved under the patch: `clean` (applies as-is),
  `moderate`, or `rewrite`. The inventory's `26.3 outcome` is a prior, not the
  answer. Drift is only knowable once someone looks.
- **Test tier** — `stateless`, `integration`, or `untestable`. This is the
  multiplier: an integration test means Docker, fixtures, multi-node
  orchestration, and usually a second binary to demonstrate the failure.

One rule carries most of the weight: **any bullet needing an integration test is
at least L, whatever its line count.** The S/M/L/XL labels above are the
earlier loc-derived estimates, kept so the table is not blank, and each is
re-scored when its ticket is picked up — drift cannot be assessed from a ledger
row.

## Policy: defense in depth {#defense-in-depth}

**Defense in depth is the guiding principle for this uplift.** A feature Aiven
does not offer should be unreachable by more than one mechanism, so that no
single default, flag, or config key is load-bearing on its own.

The motivating failure is recorded under [`026`](#soft-delete-supersedes-026):
its wrapper was safe only because production happened to run
`metadata_type = local` — "an accident of configuration, not a property of the
design." A default-off experimental setting is the same kind of accident. It is
one config key away from being on, and nothing in the build prevents it.

**This principle does not replace per-patch analysis.** Every patch is still
screened separately, and the mantra is not a licence to apply one blanket
treatment across a group. Two patches that both "disable a feature" can need
different mechanisms — a build-time gate, a runtime rejection, code removal, or
config — and the screening decides which. Where the answer is "more than one",
say which layers and why.

### `RESTORE` is disabled completely {#restore-disabled}

Decision: `RESTORE` is off, not merely gated. Aiven does not offer it, so it
should not be reachable in a shipped build.

Scoping note for the port: `N06`'s existing flag is a *single*
`REGISTER_BACKUP_RESTORE` gating both verbs at one funnel
(`BackupsWorker::start`, raising `SUPPORT_IS_DISABLED`), and it defaults **on**.
The stated policy names only `RESTORE`, so the ticket must settle whether
`BACKUP` follows it. If `BACKUP` must remain reachable, the coupled flag has to
be split rather than flipped — that is the port's first decision, and it needs
an explicit answer before code.

### Executable UDFs are patched out, not gated {#executable-udfs-removed}

Decision: executable UDFs are **fully removed**, not left behind a flag that
defaults on.

`N06` was authored against 26.3, where one config path existed, and it gates
`Context::loadOrReloadUserDefinedExecutableFunctions`. **26.8 has a second
door** the 26.3 patch cannot know about: `Context::loadUserDefinedExecutableFunctionDrivers`
loads a process-wide driver registry from
`<user_defined_executable_function_drivers_config>`, which turns
`CREATE FUNCTION … ENGINE = DriverName(…) AS '…'` into a runnable executable
UDF. That is a strictly worse surface than the XML one, because it is reachable
from SQL rather than requiring config or filesystem access.

Today it is inert only because `allow_experimental_executable_udf_drivers`
defaults to `false` — precisely the accident-of-configuration pattern above.
Carrying `N06` verbatim would gate one door and leave the other standing behind
a single boolean.

The port therefore enumerates the surface before choosing mechanisms. Known
entry points on 26.8, to be confirmed and each given an explicit disposition:

| Surface | Where |
|---|---|
| XML executable UDFs | `Context::loadOrReloadUserDefinedExecutableFunctions` |
| Driver-based executable UDFs (SQL `CREATE FUNCTION … ENGINE =`) | `Context::loadUserDefinedExecutableFunctionDrivers`, `UserDefinedExecutableFunctionDriverRegistry` |
| UDF execution machinery | `Functions/UserDefined/UserDefinedExecutableFunctionDriverInvoker`, `Processors/Sources/ShellCommandSource` |
| `executable` table function and table engine | `TableFunctionExecutable`, `StorageExecutable` |
| `executable` / `executable_pool` dictionary sources | covered by `045`, cross-check rather than duplicate |
| Bridge subprocesses | `BridgeHelper/IBridgeHelper` — separate concern, record the decision |

Note that `clickhouse-local` reaches the first of these too
(`programs/local/LocalServer.cpp`), so a gate placed only in
`programs/server/Server.cpp` would be incomplete.

## Policy: the cheapest rung that holds {#escalation-ladder}

Every carried line is a tax paid again on every uplift and every `CH Inc sync`.
So the mechanism a patch uses is a first-class decision, not an implementation
detail, and the ladder below runs cheapest-first:

1. **Managed config** — no code, no merge tax, no dossier. The cheapest patch
   is the one never carried.
2. **Upstream contribution** — the right rung for anything generically useful:
   a plain bug fix, or a feature carrying no Aiven-specific policy. Note what
   this does *not* mean. It never blocks a port on a review queue we do not
   control: the patch still lands on 26.8 by the normal route, and the extra
   work is a pull request opened and linked from the dossier. The row retires at
   27.x, not here.
3. **Build-time gate at the registration point** — additive, localized, and
   structural: the feature is absent from the binary rather than refused at
   runtime.
4. **Runtime guard in Aiven-owned code** — a new file or a single funnel we own.
5. **Edit to an upstream hot path** — last resort. Perpetual conflict surface,
   and the rung that makes uplifts expensive.

**The rule is soft but recorded**: every screening states which rung it chose
and why the cheaper rungs do not serve. It is not a veto, and rung choice is
per-patch — the same caveat as [defense in depth](#defense-in-depth).

Our own ledger is the argument. `002` and `017` were rung 1 all along, which is
why upstream growing a setting retired them; `044` turned out to sit on no rung
at all, having never done anything. Rung 2 is the one currently unused and most
underweighted: several planned rows read as plain upstream bug fixes rather than
Aiven policy, and each one accepted upstream deletes a row from the 27.x ledger
for good.

**Rung choice happens at screening, in the planned order.** There is no separate
triage pass and no pulling patches out of sequence to upstream them: a row's
rung is decided when its ticket reaches it, by whoever has that subsystem in
their head at the time, which is also the only way the judgement gets made well.
When a screening concludes the patch is a plain bug fix, that port opens the
upstream pull request and links it in the dossier before the patch commit lands.

### Reconciling the two policies {#reconciling-policies}

Read alone, the two policies pull opposite ways: defense in depth wants more
than one layer, the ladder wants the fewest and cheapest. They divide cleanly.

**Defense in depth decides how many layers**, and applies only to features
Aiven does not offer, where the cost of a single failed layer is a reachable
surface. **The ladder decides which layers**, cheapest-first, and applies to
everything.

Neither licenses skipping the other, and the failure mode to watch for is a
patch justified by whichever policy is more convenient. Rung 1 is not a layer
for a feature we do not offer — a config key is exactly the layer defense in
depth says must not be load-bearing alone. That is not a contradiction; it is
the same observation from both ends, and [`017`](inventory.md#conditional-drops)
is the case where it already bit.

## Policy: consolidate at port time {#consolidate}

A 26.8 port is **the implementation we would write from scratch today**, not a
replay of how the 26.3 line arrived at one. The base commit, its follow-up
fixes, and any gap this uplift's screening finds all land as **one commit**,
under one identity, with one dossier.

The reason is the next uplift. A patch carried as base-plus-two-fixes forces
27.x to reconstruct why each fix exists and in what order it applies, and
anyone who stops reading at the base commit ships a known defect. Folding
pushes that cost onto the port that already has the subsystem in its head,
which is the only place it is cheap.

Four consequences:

- The identity-less `patch-fix` commits (`patch-fix(022,079)`,
  `patch-fix(046)`, `patch-fix(050)`) are **folded into their parents**, never
  carried as separate commits.
- Where screening finds a gap that is **new on 26.8** — one no prior line had —
  the guard ships in the same commit as the feature it protects. `022`'s cascade
  guard is the current example.
- The dossier records the lineage in prose. The history is not lost; only the
  commit-by-commit archaeology is.
- **A hunk whose only job is to patch another Aiven patch's code is not
  carried.** Rewrite the target patch to be correct at birth instead. See the
  `019`/`022` case in [ordering constraints](#ordering-constraints).

## Landed {#landed}

| Order | Patch | Note |
|---|---|---|
| 1 | `040-harden-default-http-endpoints` | First security warm-up; absorbs `N05`; policy in [`http-endpoint-inventory.md`](http-endpoint-inventory.md) |
| 2 | `011-restrict-show-create-access` | Rewritten for `StorageSystemTables`; adds `system.databases` as a third door |
| 3 | `073-compatibility-unknown-history-setting` | No trigger on 26.8, ported as insurance; covers the `MergeTree` replay door 26.3 missed, and ships a tripwire on the history data |
| 4 | `006-replicated-database-attach-with-shard-macro` | Startup-failure fix. Rewrite: the 26.3 one-liner guards an `assert_cast` with `query.attach` alone, so the engine check had to be factored out |

## Ticket 0 — Planning (exists, in progress) {#ticket-0}

No separate screening ticket is created; this work lands under the existing
in-progress planning ticket. It still goes first: Tier 1 and Tier 2 below are
11 of the 70 open items, so if half drop, that is a measurable slice of the
uplift deleted for a few days of read-only work — and it changes what every
other ticket is sized against.

- **Screen the obsolescence watch list.** Tier 1 — `029` `031` `032` `033`
  `048`, all already partial at 26.3, which is the strongest predictor of
  further absorption. Tier 2 — `020` `037` `039` `049` `056` `060`, all small
  with weak recorded justification; `039` (disable thread fuzzer) is analysed
  here rather than dropped pre-emptively. Record a decision per patch in
  [`inventory.md`](inventory.md), including a changed *purpose*, not only
  changed applicability.
- **Verify the conditional drops** — see
  [conditional drops](inventory.md#conditional-drops). `002` and `017` were
  dropped at 26.3 because upstream grew a setting, and both settings default to
  **off**, so each drop is valid only if Aiven's managed config sets the key.
  `017` is a TLS enforcement control, so it is checked first, together with the
  `postgresql_require_secure_transport` sibling the original patch never
  covered. Then triage `001`, `027` and `074` into absorbed-in-code versus
  replaced-by-config, and record the config key on every row that turns out to
  be conditional. Read-only work with a security finding at the end of it, which
  is why it belongs here and not in a ticket that ships code.
- **Resolve `009`'s predicate scope** — route all `MOVE PARTITION`, or narrow
  to `TO DISK`/`TO VOLUME` and avoid re-enabling the leader-only `TO TABLE`
  path with its DDL-queue head-of-line stall. The one open technical decision.

## Ticket 1 — Protected access control {#ticket-1}

The three heavyweight access patches. All touch the Access subsystem, which
drifted substantially at 26.3 and will have drifted again.

- **`022` + `079` protected users and roles** — **screened**, dossier written:
  [022 protected users and roles](../../patches/022-protected-users-and-roles.md).
  One commit, with
  `patch-fix(022,079)`, the `checkProtectedTargets` follow-up and the new
  cascade guard all folded in per [consolidate at port time](#consolidate).
  Still needed in full: no upstream equivalent exists on 26.8. The entity layer
  is free — `IAccessEntity.h`, `User.*` and `Role.*` are byte-identical between
  the lines — but the storage `CheckFunc` threading, which carries the TOCTOU
  guarantee, must be re-derived: `DiskAccessStorage::insertImpl` went from 30 to
  82 lines and `removeImpl` and `updateImpl` were both rewritten.

  Screening found a gap that is new on 26.8. Upstream added
  `IAccessStorage::removeReferencesToRemovedIDs`, a cascade that runs after any
  `remove()` and rewrites every entity referencing the dropped id — through
  `updateImpl`, which the patch does not guard. `User::removeDependencies`
  strips `default_roles`, `granted_roles`, `grantees` and `settings`, so a
  principal with plain `ACCESS MANAGEMENT` can strip a protected entity's grants
  by dropping a role or settings profile it references. Stripping a *profile*
  can lift a constraint, so this is not merely tidying dangling ids. Guard it by
  denying the triggering `DROP` at the initiator, above the `ON CLUSTER`
  dispatch, when a protected entity depends on the target — not by a `CheckFunc`
  on `updateImpl`, which has no user identity in scope and would leave protected
  entities holding the dangling references upstream added the cascade to clean.
- **`019` + `020` `avnadmin` indirect database creation.** `020` is a one-line
  tail validated by `019`'s test, so it is one commit with `019`, not two.
  Because `022` lands first, **`019` must write its `cluster_database` `DROP`
  check with `PROTECTED_ACCESS_MANAGEMENT` from the start.** The 26.3 line
  reached that state by having `022` patch `019`'s line afterwards; that hunk is
  not carried.
- **`014` default-profile escape**, plus the recursion hardening recorded but
  not shipped at 26.3 (unbounded parent-chain recursion while holding
  `SettingsProfilesCache::mutex`).

## Ticket 2 — Attack-surface reduction {#ticket-2}

Everything that removes or guards a reachable surface. The first bullet is
large but indivisible; the other three are small and independent.

- **`REGISTER_*` expose-gate family** — `051` foundation, `052` `070` `071`
  `075` engine registrations, `045` dictionary sources, `N01` + `N06` gates for
  WebAssembly UDFs, executable UDFs, `BACKUP`/`RESTORE`, and custom disks. One
  mechanism; the work is re-deriving the flag and engine list against 26.8's
  registry, once, so this is not split into eight commits. `N06` needs a first
  dossier — none exists on the 26.3 line.

  `N06` is **not a verbatim carry**: per
  [defense in depth](#defense-in-depth), `RESTORE` is
  [disabled completely](#restore-disabled) rather than gated behind a
  default-on flag, and executable UDFs are
  [patched out](#executable-udfs-removed) rather than gated — 26.8 has a second
  executable-UDF door that the 26.3 patch does not cover. Both need their
  surface enumerated before mechanisms are chosen, so budget this bullet above
  its 6-file/47-loc source.
- **`062` prohibit `.tmp` table creation** (`063` and `064` already squashed in).
- **`077` hide secrets in `system.mutations.command`** — continues the landed
  `011` thread: same group, same theme of metadata exposed to tenants, small,
  and both a 26.3 dossier and retrospective already exist. Screening found real
  drift, so do not scope it from the 26.3 diff: on 26.8 both read sites copy a
  precomputed `MutationCommand::ast_text` that is reparsed for execution, so the
  redaction cannot go where 26.3 put it.
- **`N07` `GRANT … EXCEPT`** — identity assigned by this uplift; needs a first
  dossier.

## Ticket 3 — Object storage {#ticket-3}

- **`026` as soft delete on object storage** — the concept only, no wrapper
  history. See [the concept note](#soft-delete-supersedes-026). The ticket most
  likely to be mis-scoped in both directions.
- **`015` + `016` signature delegation**, including both contrib fork rebases
  per [`../../runbooks/submodule-forks.md`](../../runbooks/submodule-forks.md).
  Schedule risk: depends on `aiven/aws-sdk-cpp` and `aiven/azure` being rebased
  onto 26.8's pins, which is external to this repo.
- **`024` + `025` + `028` + `056` Azure surface** — IPv6 host regex, storage
  prefix, gated container-creation skip, freeze metadata. Re-read `028`'s
  dossier: it was wrongly dropped once at 26.3.

## Ticket 4 — Transport security {#ticket-4}

- **`012` + `013` custom CA path (S3, Azure).** `013` is a mirror of `012` and
  inherits its context-keyed pool, so it follows in the same ticket. `012` also
  carries two real bug fixes beyond the CA path: pool trust isolation, and a
  per-request context rebuild.
- **`021` external DB SSL (PostgreSQL, MySQL)** plus contrib pin
  re-verification — the 26.3 fork was dropped when upstream absorbed the
  connector fix, so confirm 26.8's pin before assuming either shape.
- **`018` + `065` + `059` + `067` TLS enforcement and certificate handling.**

## Ticket 5 — Replication core {#ticket-5}

- **`004` replace `MergeTree` with `Replicated`**, plus coordination of the
  `internal_replication` config flip. Never flip the config before this lands.
- **`008` replication queue size limit** — 13 files, gated, with its own
  two-node integration test.
- **`009` replicate `MOVE PARTITION`** — after planning settles the predicate
  scope.

## Ticket 6 — Replication smalls, merges, settings {#ticket-6}

- **`003` + `005` + `010` + `042` + `060` + `072` replicated-DB small patches**
  (after the planning screen). Deliberately one bullet: six patches of 2–29 loc
  in one subsystem. If screening drops half, this shrinks rather than leaving a
  scatter of closed-as-obsolete work.
- **`046` + `047` + `048` + `053` merges, fetches, storage policy**, including
  `patch-fix(046)`.
- **`054` + `058` settings-surface patches.**
- **`057` remove cloud-specific settings** — included as work rather than left
  as an open decision, but **its purpose is to be revisited** before the port is
  scoped. At 16 files and 610 loc it is the largest item here and the only patch
  deep-screened at 26.3 without reaching a conclusion, so treat the revisit as a
  gate: establish what the patch is *for* on 26.8 before writing code, since the
  cloud-specific settings it removed have themselves churned.

## Ticket 7 — Kafka {#ticket-7}

The highest-obsolescence group on the board — three of six were already partial
at 26.3 — so all three bullets are provisional until the planning screen
reports.

- **`029` + `031` + `033` settings plumbing** (after the planning screen).
- **`032` + `076` offset reset** — two mechanisms for one user need; decide
  whether both ship.
- **`030` `kafka_num_consumers` = 0.**

## Ticket 8 — Materialized views {#ticket-8}

- **`066` MV refresh in a sharded environment** — the largest single patch in
  the set (~1000 loc at 26.3, `078` squashed in).
- **`050` + `N02` + `049` refreshable MVs on ZooKeeper**, including
  `patch-fix(050)`. `N02` may supersede `050` outright — check before porting
  both.

## Ticket 9 — Integrations and dictionaries {#ticket-9}

- **`035` + `055` + `080` named collections** — `080` is the ZooKeeper
  propagation fix for the same surface.
- **`036` + `061` dictionary sources** — `036` carries the named-collection
  enforcement whose MySQL sibling was restored under `021`.
- **`034` unlock PostgreSQL database.**

## Ticket 10 — TimeSeries, ops and packaging {#ticket-10}

- **`N03` + `N04` TimeSeries external targets** — same subsystem, same fixture.
- **`037` + `038` + `039` + `041` + `068` + `069` ops, packaging,
  observability** (after the planning screen). Same bundling logic as Ticket 6:
  all small, all independent, two of them drop candidates.

## Soft delete supersedes `026` {#soft-delete-supersedes-026}

Aiven's requirement is that ClickHouse never physically deletes remote blobs:
removals must become marker files an out-of-band GC reconciles. The 26.3 line
reached that requirement in two stages, and **26.8 skips the first stage
entirely**.

The retired stage is the **wrapped disk** — a `backup` disk type decorating an
already-constructed disk, plus machinery to suppress the physical deletions the
layers underneath still performed. Three commits belong to it: `patch-port(026)`
"Add Backup disk type", `patch-fix(026)` "keep backup-wrapped disks from
physically deleting soft-deleted blobs", and `patch-port(026)` "make
`removeObjectImpl` idempotent". **None of them is ported.**

Its own retraction commit explains why it could not be salvaged: by the time the
wrapper wrapped a disk, the raw object storage had already been handed to four
owners it could not reach — the inner disk in the global `DisksMap`, its
`BlobCopierThread`, its `BlobKillerThread` one level deeper than the wrapped
disk, and the `plain`/`plain_rewritable` metadata storages, which capture the
object storage by value and call `removeObjectsIfExist` directly. The last has
no fix at the disk level; it survives only because production runs
`metadata_type = local`, "an accident of configuration, not a property of the
design."

The surviving stage wraps **at construction**: `RegisterDiskObjectStorage`
applies `SoftDeleteObjectStorage` as the object storage is created, before it
enters the router, so no component can hold an undecorated one. Configuration
moves onto the disk itself (`<soft_delete>1</soft_delete>` on an
`object_storage` disk). The invariant becomes structural rather than maintained
by disabling things after the fact.

Two scoping rules follow:

1. **Scope from the final file state on the source branch, not from any diff.**
   The retraction commit reads 247+/691− because most of its deletions remove
   `026`'s machinery — machinery 26.8 will never have. The 26.8 change is
   roughly the additive half: `SoftDeleteObjectStorage`, the
   `RegisterDiskObjectStorage` wiring, the `BlobKillerThread` changes, the disk
   config key, and tests.
2. **The `026` dossier is stale as a scoping source.** It documents the wrapper
   throughout, including a test design that creates `type = backup` disks. Use
   the retraction commit message for rationale and the branch's final file state
   for implementation.

`026` **keeps its number.** The identity tracks the requirement, not the
implementation, which is why `still-needed-but-rewrite` is a routine outcome
rather than a renumbering trigger; `013` is the precedent, having changed
mechanism completely at 26.3 while keeping its number. A number absent from the
26.8 set also reads as *dropped*, which would misreport a requirement that
ships.

## Ordering constraints {#ordering-constraints}

- `013` after `012` — mirror, and inherits its trust isolation.
- `020` after `019` — one-line tail, validated by `019`'s test.
- `045`, `052`, `070`, `071`, `075`, `N01`, `N06` after `051` — foundation.
- `079` with `022`, not after it — one commit, not a pair.
- `019` after `022`, by consolidation rather than compilation. On 26.3, `022`
  upgraded a privilege check *inside* `019`'s `InterpreterDropQuery` block from
  `ACCESS_MANAGEMENT` to `PROTECTED_ACCESS_MANAGEMENT`; that block is Aiven code
  and does not exist on 26.8, so the hunk has no anchor. Either order compiles.
  This one lets `019` write the check correctly at birth instead of carrying a
  patch-a-patch hunk.
- `N07` and `022`/`079` share exactly one file,
  `src/Interpreters/Access/InterpreterGrantQuery.cpp`; their parsers do not
  overlap. Either order works, so take `022`/`079` first — it is the 44-file
  edit, and `N07`'s six files are the cheaper side to rebase. They sit in
  different tickets (1 and 2) but the same lane, so this is a scheduling note,
  not a blocker.
- `004` before the `internal_replication` config flip, never after.
- `073` before anything editing `src/Core/SettingsChangesHistory.cpp` so its
  tripwire is armed first — **already satisfied**, `073` landed.
- The planning screen before the screening-dependent bullets in Tickets 5, 6,
  7 and 10.
- `015` + `016` gated on external contrib fork rebases.

## Definition of done {#definition-of-done}

Per bullet, the standing definition in
[`../../runbooks/workflow.md`](../../runbooks/workflow.md) holds: a dossier with
Component tour, Concept and Customer impact; tests in the `aiven_*` namespace;
FAIL-on-parent and PASS-with-patch evidence; documentation and code in separate
commits. This uplift adds two: append the patch's impact to
[`customer-impact.md`](customer-impact.md), and where screening chose rung 2,
link the upstream pull request.

The uplift is done when:

- **No row in [`inventory.md`](inventory.md) is still `planned`.** Every
  identity reaches a terminal status, including the ones screening drops.
- The [conditional drops](inventory.md#conditional-drops) are verified.
- Every `landed` row has a dossier under [`../../patches/`](../../patches/).
- The un-numbered reconciliation is re-run against the *then-current* source
  tip, not the one planning started from. The tip moved once during planning
  already, and a `patch-fix` carrying no identity is invisible to a
  number-driven sweep.
- The `aiven_*` tests pass as one lane.
- `CH Inc sync` is clean, or each surviving conflict is recorded with the reason
  it is accepted.
- [`customer-impact.md`](customer-impact.md) covers every landed patch.

The last two decide what the *next* uplift costs. The `aiven_*` lane is the
fork's regression net — the only artifact that tells 27.x whether a port still
holds — which makes the tests the durable deliverable and the patches the
perishable one.

## Source discovery {#source-discovery}

The source line advances. It moved from `v26.3.26.3-lts-aiven` to
`v26.3.32.14-lts-aiven` during planning, adding exactly one patch (`N06`). No
tip is pinned here, precisely because it will move again — but fetch the current
one before reconciling. A local checkout can easily be a tag or two behind, in
which case a range against the tip fails to resolve rather than returning a
wrong answer, so the trap is a wasted step and not a bad ledger.

A rebase re-dates every Aiven SHA, so a range between two tips looks like
hundreds of new commits when it is the same stack replayed — 198 in that case.
Compare **patch identities**, not SHAs, and cite patches by identity in
dossiers and commit messages, because the SHA will change under you:

```bash
# identities on a branch
git log <aiven-tip> -250 --format='%s' \
  | rg -o '^(patch-port|patch-new|patch-fix|patch)\([0-9,N]+\)' | sort -u

# reconcile against the ledger to catch un-numbered work
git log --oneline --no-merges <upstream-base>..<aiven-tip>
```

---
description: 'Engineering rules that apply to every Aiven ClickHouse patch port, with the patch that taught each one'
sidebarTitle: 'Porting doctrine'
slug: '/aiven/runbooks/porting-doctrine'
title: 'Runbook — porting doctrine'
doc_type: 'guide'
---

# Runbook — porting doctrine {#runbook-porting-doctrine}

Design and C++ rules that apply to **every** port, whatever the subsystem and
whichever LTS. Order and ownership live in the uplift tracker; per-patch
reasoning lives in the dossier. This file holds what we keep getting wrong.

Repository-wide style and safety rules are in
[`../AGENTS.md`](../AGENTS.md) and are not restated here. Everything below is
specific to carrying a fork patch onto a newer line.

## How a rule gets in here {#how-a-rule-gets-in}

**One rule per port, each with the patch that taught it.** A rule with no
incident behind it is someone's preference, and a doctrine file that accretes
preferences stops being read. If a port surfaces a trap that would catch the
next person, add it with the evidence in its own `docs(aiven):` commit;
otherwise change nothing. Patches are cited by identity, not linked, because a
dossier exists on a line only once its patch has landed there.

## Screening {#screening}

### Screening changes purpose, not just applicability {#screening-changes-purpose}

*Taught by `002`, `017`, `026`, `044` and `057` on the 26.3 uplift.*

A tracker row is not a work order. When a patch is picked up, screening asks
**what is this for on the new line** before asking whether it still applies —
and the answer can invalidate the row.

`002` and `017` were not obsolete in the sense of "upstream fixed the bug";
upstream grew a *setting*, so the patch's purpose moved from code into Aiven's
managed configuration. `044` turned out never to have done anything on either
line. `026`'s purpose held but its whole design was retracted. `057` was
deep-screened without reaching a conclusion at all.

So a screening outcome is legitimately any of: port as-is, port rewritten,
replace with config, drop as obsolete, drop as never-effective, or re-scope the
requirement. Record it in the dossier and the tracker; do not force the finding
to fit the inherited placement.

### Conditional drops must be verified once {#conditional-drops}

*Taught by `017`.*

Most drops hold by construction: where upstream absorbed the behavior in code
or removed the feature, the newer line is strictly newer and the drop stays
valid. One class does **not**. Where upstream replaced the patch with a
*setting*, the patch's purpose moved out of the binary and into Aiven's managed
configuration, and the drop is valid only if that configuration actually sets
the key. Nothing in the build enforces it, and such settings usually default to
off.

`017` enforced TLS on the MySQL handler. Upstream replaced it with
`mysql_require_secure_transport`, default `false` — a TLS control that used to
be compiled in became one config key away from absent. The drop was verified:
the managed configuration sets the key and also its
`postgresql_require_secure_transport` sibling, which the original MySQL-only
patch never covered. The property still lives in configuration rather than the
binary, so nothing in this repository will tell us if it regresses.

Rule: every drop whose reason is "upstream grew a setting" records the key, its
default, and whether the managed configuration was checked. Until checked, the
drop is a hypothesis.

### Identity tracks the requirement, not the implementation {#identity-tracks-requirement}

*Taught by `013` and `026`.*

A patch keeps its number when its mechanism changes completely, which is why
`still-needed-but-rewrite` is a routine outcome rather than a renumbering
trigger. `013` changed mechanism at 26.3 and kept its number; `026`'s
soft-delete requirement survived the retraction of its wrapped-disk design. A
number absent from a line reads as *dropped*, which would misreport a
requirement that ships.

Allocate new identities in one place only — the tracker. Two lines minting
numbers independently will collide: `N07` was assigned to two different
patches by the 26.3 line and the 26.8 uplift, and a number-keyed reconciliation
cannot see it. Only comparing titles does.

### Compare identities, not SHAs {#compare-identities}

*Taught by the 26.3 source line moving during 26.8 planning.*

A rebase re-dates every Aiven SHA, so a range between two tips of the same line
looks like hundreds of new commits when it is the same stack replayed — 198 in
that case. Compare **patch identities**, and cite patches by identity in
dossiers and commit messages. Re-run the reconciliation against the source
line's *current* tip before sign-off, because a `patch-fix` carrying no
identity is invisible to a number-driven sweep:

```bash
# identities on a branch
git log <aiven-tip> -250 --format='%s' \
  | rg -o '^(patch-port|patch-new|patch-fix|patch)\([0-9,N]+\)' | sort -u

# everything above the base, to catch un-numbered work
git log --oneline --no-merges <upstream-base>..<aiven-tip>
```

## Choosing a mechanism {#choosing-a-mechanism}

### Defense in depth for features Aiven does not offer {#defense-in-depth}

*Taught by `026` and `N06`.*

A feature Aiven does not offer should be unreachable by more than one
mechanism, so that no single default, flag or config key is load-bearing on its
own. `026`'s wrapper was safe only because production happened to run
`metadata_type = local` — "an accident of configuration, not a property of the
design." A default-off experimental setting is the same kind of accident: one
config key away from being on, with nothing in the build to prevent it.

`N06` is the case where this bit across a version boundary. It gated the XML
path for executable UDFs on 26.3; 26.8 added a second door — a driver registry
that makes executable UDFs reachable from SQL — guarded only by an experimental
setting defaulting to `false`. Carrying the patch verbatim would have gated one
door and left the other behind a single boolean.

This does not replace per-patch analysis. Two patches that both "disable a
feature" can need different mechanisms — a build-time gate, a runtime
rejection, code removal, or config — and the screening decides which. Where the
answer is "more than one", the dossier says which layers and why. Enumerate the
whole surface on the new line before choosing; a version bump can add doors the
original patch could not know about.

### The cheapest rung that holds {#escalation-ladder}

*Taught by `002`, `017` and `044`.*

Every carried line is a tax paid again on every uplift and every `CH Inc sync`,
so the mechanism a patch uses is a first-class decision. Cheapest first:

1. **Managed config** — no code, no merge tax, no dossier. The cheapest patch
   is the one never carried.
2. **Upstream contribution** — for anything generically useful: a plain bug
   fix, or a feature with no Aiven-specific policy. It never blocks a port on a
   review queue we do not control; the patch still lands by the normal route
   and the pull request is linked from the dossier.
3. **Build-time gate at the registration point** — additive, localized, and
   structural: the feature is absent from the binary rather than refused.
4. **Runtime guard in Aiven-owned code** — a new file or a single funnel we own.
5. **Edit to an upstream hot path** — last resort. Perpetual conflict surface,
   and the rung that makes uplifts expensive.

The rule is soft but recorded: every screening states which rung it chose and
why the cheaper rungs do not serve. `002` and `017` were rung 1 all along, which
is why upstream growing a setting retired them; `044` sat on no rung at all,
having never done anything. Rung choice happens at screening, in the planned
order — no separate triage pass.

### Reconciling the two {#reconciling-policies}

Read alone, the two rules pull opposite ways: defense in depth wants more than
one layer, the ladder wants the fewest and cheapest. They divide cleanly.
**Defense in depth decides how many layers**, and applies only to features Aiven
does not offer. **The ladder decides which layers**, and applies to everything.

The failure to watch for is a patch justified by whichever rule is more
convenient. Rung 1 is not a layer for a feature we do not offer — a config key
is exactly the layer defense in depth says must not be load-bearing alone.
`017` is where both observations meet.

### Prefer one fail-closed funnel, at the registration point {#fail-closed-funnel}

*Taught by `040` and the retracted design behind `026`.*

Where a feature must be unreachable, put the refusal at **one** choke point
with an explicit error code, and put that choke point as early as the
architecture allows — at registration or construction, not at each use site.

`026`'s retraction is the cautionary case: by the time its wrapper decorated a
disk, the raw object storage had already been handed to four owners it could not
reach, and the design survived only because production happened to run one
particular metadata type. The replacement wraps as the object storage is
*created*, so no component can hold an undecorated one. The invariant became
structural instead of maintained.

Corollaries:

- Scattered checks at use sites are a promise to find every use site, now and
  forever. You will not.
- A build-time gate at the registration point beats a runtime check, because
  the feature is absent rather than refused.
- Keep the refusal reversible on purpose: `040` keeps every handler compiled and
  adds the explicit `http_handlers` types, so an operator can restore an
  endpoint without a rebuild. Unreachable by default is the goal; unreachable
  forever is a different, larger decision.

## Shaping the commit {#shaping-the-commit}

### Consolidate at port time {#consolidate}

*Taught by `022`, which arrived on 26.3 as three commits across two identities.*

A port is **the implementation we would write from scratch today**, not a
replay of how the prior line arrived at one. The base commit, its follow-up
fixes, and any gap the screening finds land as **one commit**, under one
identity, with one dossier.

The reason is the next uplift. A patch carried as base-plus-two-fixes forces
the next port to reconstruct why each fix exists and in what order it applies,
and anyone who stops reading at the base commit ships a known defect. Folding
pushes that cost onto the port that already has the subsystem in its head,
which is the only place it is cheap.

- Identity-less `patch-fix` commits are **folded into their parents**, never
  carried as separate commits.
- A gap that is **new on this line** ships its guard in the same commit as the
  feature it protects. `022`'s dependency-cascade guard on 26.8 is the example.
- The dossier's Lineage table records what was folded. The history is not
  lost; only the commit-by-commit archaeology is.
- **A hunk whose only job is to patch another Aiven patch's code is not
  carried.** Rewrite the target patch to be correct at birth instead. On 26.3,
  `022` upgraded a privilege check inside `019`'s code; on 26.8, porting `022`
  first let `019` write the check correctly from the start.

### Fork-local settings are named `aiven_*` {#setting-naming}

*Taught by `019`, the first adopter.*

Every setting this fork introduces carries an `aiven_` prefix, renamed as its
patch is ported. Measured on the 26.3 line, only 6 of 21 fork-local settings
were prefixed. Three reasons, in order of weight:

1. **Half a convention is worse than none.** Once some settings say `aiven_`,
   an unprefixed neighbour reads as upstream. Establishing that
   `user_with_indirect_database_creation` was fork-local took a full-history
   `git log -S`, because later ports were re-authored without cherry-pick
   trailers.
2. **Collision risk is real.** `cluster_database` is extremely generic for a
   global server setting, and the `queue_size*` family sits beside upstream's
   `parts_to_delay_insert` naming in the same conceptual area. A collision is a
   merge conflict at best and a silent semantic change at worst.
3. **It makes the fork's configuration surface enumerable.** One
   `LIKE 'aiven%'` against `system.server_settings` returns all of it.

The rename is only safe because a mismatch fails loud:
`ServerSettings::checkUnknownSettings` runs at startup and on every config
reload and throws on an unknown top-level key, so a stale config carrying the
old name fails the reload instead of silently reverting the setting to its
default. For `019` that is load-bearing — an empty `cluster_database` would turn
its forced `ON CLUSTER` rewrite into a no-op. Check the guard still exists
before extending the convention to a setting whose default is dangerous.

- **No dual-name aliases.** Accepting both spellings is the silent fallback the
  rename depends on not having.
- **The configuration carrying the new name lands before or with the binary**,
  since the tripwire fires on the old one.
- Each dossier records the old and new names, so `git log -S` against a prior
  line still works.
- SQL surface is not renamed. Privileges and statements the fork adds are
  customer-visible and stay as they are.

### Size on drift and test tier, not lines of code {#sizing}

*Taught by `006`.*

`006` was one file and two lines, and it consumed a full session: an
integration test, a pre-patch binary to prove the failure, a cross-LTS
`assert_cast` audit, and a fleet query. Size a unit on the two axes that drive
cost:

- **Drift** — how far upstream moved under the patch: `clean`, `moderate`, or
  `rewrite`. The prior line's outcome is a prior, not the answer; drift is only
  knowable once someone looks.
- **Test tier** — `stateless`, `integration`, or `untestable`. This is the
  multiplier: an integration test means Docker, fixtures, multi-node
  orchestration, and usually a second binary to demonstrate the failure.

**Any unit needing an integration test is at least L, whatever its line count.**

## Writing the code {#writing-the-code}

### Never guard an `assert_cast` with a proxy boolean {#assert-cast}

*Taught by `006`.*

`assert_cast` only checks the type under `DEBUG_OR_SANITIZER_BUILD`. In a
release build it degrades to `static_cast`, so a wrong cast is undefined
behavior that no test observes — the reads land past the object and may stay
benign for years. A guard that is a *separate* boolean can drift from the cast
it protects, and when it does, the failure is invisible exactly where it
matters.

Make the check and the use one expression, so they cannot drift:

```cpp
/// The cast is the type check, so it cannot drift from the use.
if (const auto * replicated = dynamic_cast<const DatabaseReplicated *>(database.get()))
{
    info.shard = replicated->getShardName();
}
```

Two details that are easy to get wrong:

- Prefer `dynamic_cast` to `typeid_cast` when a **subclass must not silently
  skip** the branch. `typeid_cast` matches the exact type, so a future subclass
  would fall through the guard rather than enter it.
- Keep the owning pointer in a local. `dynamic_cast` on a temporary's
  `.get()` leaves you with a pointer borrowed from something already destroyed.

When reviewing a port that widens a condition around an `assert_cast`, ask what
the *new* callers are. `006`'s upstream-line version widened a guard to
`query.attach`, which its commit message called a "hypothetical edge" — it is
in fact every table on the server-startup path.

### Read a gate from the reloaded config, not from startup settings {#reloadable-gates}

*Taught by 26.8's executable-UDF driver registry, screened for `N06`.*

`SYSTEM RELOAD CONFIG` does **not** refresh the startup-time server settings
held on the shared context. A gate read from there silently ignores an operator
toggling it, until the next restart — the feature stays on (or off) while the
configuration says otherwise, which is the worst kind of gate: one that reports
success.

Load a fresh `ServerSettings` from the configuration you were passed:

```cpp
ServerSettings reloaded_server_settings;
reloaded_server_settings.loadSettingsFromConfig(config);
if (!reloaded_server_settings[ServerSetting::allow_the_feature])
    return;
```

This is equivalent at startup — it is the same configuration object — and
correct on reload.

### Never ship a `SettingsChangesHistory` entry the build cannot back {#settings-history}

*Taught by `073`.*

`SettingsChangesHistory` is a **second, implicit schema** for the settings list.
`compatibility` is implemented by replaying it backwards and writing each
recorded change back by name, so a build whose history names a setting it does
not have carries a latent runtime failure. Historically this arrived through a
backport that took a history entry without the commit declaring the setting.

If a port adds, renames or removes a fork setting, the history and the settings
list must move together. `aiven_073_compatibility_unknown_history_setting` is
the tripwire on exactly this and must stay green; treat a failure as a
correctness bug in your patch, not as a flaky test. Port `073` before anything
that edits `src/Core/SettingsChangesHistory.cpp`, so the tripwire is armed first.

### Minimize the merge surface, and read what you had to edit {#merge-surface}

*Taught by `011`.*

Every line a patch changes in an upstream file is a conflict re-litigated on
every uplift and every `CH Inc sync`. This is why the
[ladder](#escalation-ladder) prefers config, upstreaming, and additive gates
over edits to upstream hot paths.

Two habits follow:

- **Prefer new files and single funnels we own** to edits threaded through
  upstream code. Same behavior, a fraction of the recurring cost.
- **Treat an upstream test you had to edit as a finding, not a chore.** It is
  evidence your patch changed upstream-visible behavior, and it is the cheapest
  early warning that the blast radius is wider than the dossier claims. `011`
  had to grant `CREATE TABLE` in an existing upstream test; that edit *was* the
  user-visible change, discovered before any tenant found it. List such edits in
  the dossier's Customer impact.

## When an uplift is done {#uplift-done}

*Taught by the 26.3 tip moving during 26.8 planning.*

- No tracker unit is still `planned`; every identity reached a terminal status,
  including the ones screening dropped, and each has a dossier on the line.
- Conditional drops are verified.
- The reconciliation is re-run against the source line's *then-current* tip.
- The `aiven_*` tests pass as one lane.
- `CH Inc sync` is clean, or each surviving conflict is recorded with the reason
  it is accepted.

The last two decide what the next uplift costs. The `aiven_*` lane is the fork's
regression net — the only artifact that tells the next port whether a patch
still holds — which makes the tests the durable deliverable and the patches the
perishable one.

---
description: 'Engineering rules that apply to every Aiven ClickHouse patch port, with the patch that taught each one'
sidebarTitle: 'Porting doctrine'
slug: '/aiven/runbooks/porting-doctrine'
title: 'Runbook — porting doctrine'
doc_type: 'guide'
---

# Runbook — porting doctrine {#runbook-porting-doctrine}

C++ and design rules that apply to **every** port, whatever the subsystem.
Order and ownership live in the uplift's `execution-plan.md`; per-patch
reasoning lives in the dossier. This file holds what we keep getting wrong.

Repository-wide style and safety rules are in
[`../AGENTS.md`](../AGENTS.md) and are not restated here. Everything below is
specific to carrying a fork patch onto a newer line.

## How a rule gets in here {#how-a-rule-gets-in}

**One rule per port, each with the patch that taught it.** A rule with no
incident behind it is someone's preference, and a doctrine file that accretes
preferences stops being read. If a port surfaces a trap that would catch the
next person, add it with the evidence; otherwise change nothing.

## Never guard an `assert_cast` with a proxy boolean {#assert-cast}

*Taught by [`006`](../patches/006-replicated-database-attach-with-shard-macro.md).*

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

## Read a gate from the reloaded config, not from startup settings {#reloadable-gates}

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
correct on reload. This uplift adds a family of gates, so the rule applies
broadly rather than to one patch.

## Prefer one fail-closed funnel, at the registration point {#fail-closed-funnel}

*Taught by [`040`](../patches/040-harden-default-http-endpoints.md) and the
retracted design behind `026`.*

Where a feature must be unreachable, put the refusal at **one** choke point
with an explicit error code, and put that choke point as early as the
architecture allows — at registration or construction, not at each use site.

`026`'s retraction is the cautionary case: by the time its wrapper decorated a
disk, the raw object storage had already been handed to four owners it could not
reach, and the design survived only because production happened to run one
particular metadata type — "an accident of configuration, not a property of the
design." The replacement wraps as the object storage is *created*, so no
component can hold an undecorated one. The invariant became structural instead
of maintained.

Corollaries:

- Scattered checks at use sites are a promise to find every use site, now and
  forever. You will not.
- A build-time gate at the registration point beats a runtime check, because
  the feature is absent rather than refused.
- Keep the refusal reversible on purpose: `040` keeps every handler compiled and
  adds the explicit `http_handlers` types, so an operator can restore an
  endpoint without a rebuild. Unreachable by default is the goal; unreachable
  forever is a different, larger decision.

## Never ship a `SettingsChangesHistory` entry the build cannot back {#settings-history}

*Taught by [`073`](../patches/073-compatibility-unknown-history-setting.md).*

`SettingsChangesHistory` is a **second, implicit schema** for the settings list.
`compatibility` is implemented by replaying it backwards and writing each
recorded change back by name, so a build whose history names a setting it does
not have carries a latent runtime failure. Historically this arrived through a
backport that took a history entry without the commit declaring the setting.

If a port adds, renames or removes a fork setting, the history and the settings
list must move together. `aiven_073_compatibility_unknown_history_setting` is
the tripwire on exactly this and must stay green; treat a failure as a
correctness bug in your patch, not as a flaky test.

## Minimize the merge surface, and read what you had to edit {#merge-surface}

Every line a patch changes in an upstream file is a conflict re-litigated on
every uplift and every `CH Inc sync`. This is why the
`execution-plan.md` ladder prefers config, upstreaming, and additive gates over
edits to upstream hot paths.

Two habits follow:

- **Prefer new files and single funnels we own** to edits threaded through
  upstream code. Same behavior, a fraction of the recurring cost.
- **Treat an upstream test you had to edit as a finding, not a chore.** It is
  evidence your patch changed upstream-visible behavior, and it is the cheapest
  early warning that the blast radius is wider than the dossier claims. `011`
  had to grant `CREATE TABLE` in an existing upstream test; that edit *was* the
  user-visible change, discovered before any tenant found it. List such edits in
  the dossier's Customer impact.

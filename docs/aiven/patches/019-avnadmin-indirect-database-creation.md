---
description: 'Dossier for the Aiven patch that lets one configured admin user create and drop Replicated databases over plain SQL, with cluster parameters auto-filled and a narrowly-scoped privilege elevation'
sidebarTitle: '019: Indirect database creation'
slug: '/aiven/patches/019-avnadmin-indirect-database-creation'
title: 'Patch 019: avnadmin indirect database creation'
doc_type: 'reference'
---

# Patch 019 — `avnadmin` indirect database creation {#patch-019-avnadmin-indirect-database-creation}

## Lineage {#lineage}

| LTS | How to find | Outcome |
|---|---|---|
| 24.3 | `0777d9ce369`, 2024-12-30, on `v24.3.5.46-lts-aiven` | **origin.** The interception, the `GRANT` shortcut and the three server settings |
| 24.8 | `acc5dbdc011`, 2025-04-21, on `v24.8.13.16-lts-aiven` | adds the forced `ON CLUSTER` `DROP` enforcement and `skip_distributed_checks`; absent on `v24.8.8.17`, so it landed between those point releases |
| 25.3, 25.8 | `05d8148a57ef` (base) + `c5b03b2c0e5a` (`CHECK`, identity `020`) on `v25.8.18.1-lts-aiven` | carried forward; **no** `query_id` commit — that failure mode did not exist on these lines |
| 26.3 | `patch-port(019)` at `15b469ab133`, `patch-port(020)` at `dac0dbf66a6`, `patch-port(019)` (`query_id`) at `45d490db20f` on `v26.3.32.14-lts-aiven` | ported as three commits |
| 26.8 | `patch-port(019):` | `still-needed-but-rewrite`, **one commit** |

Note for future archaeology: the 25.8 and later commits were re-authored rather
than cherry-picked, so they carry no trailer pointing back to `0777d9ce369`.
Provenance for this patch has to be established with `git log -S` against the
oldest Aiven line that contains the symbol, not by following trailers.

**All three server settings are renamed on this line**, per
[fork-local settings are named `aiven_*`](../uplifts/26.8/execution-plan.md#setting-naming),
of which this patch is the first adopter. Search prior lines by the old names:

| 24.3 … 26.3 | 26.8 onwards |
|---|---|
| `user_with_indirect_database_creation` | `aiven_user_with_indirect_database_creation` |
| `reserved_replicated_database_prefixes` | `aiven_reserved_replicated_database_prefixes` |
| `cluster_database` | `aiven_cluster_database` |

There is no in-place rename to perform: none of the three exists on 26.8 yet, so
they are simply introduced prefixed. No alias accepts the old spelling — see the
policy for why that is deliberate. Two error messages name a setting inside
their text and have to be renamed along with it: the
`SETTING_CONSTRAINT_VIOLATION` raised when the reference database is not
configured, and the `LOGICAL_ERROR` that says `default` where it means whatever
`aiven_cluster_database` is set to.

On 26.8 this patch carries identity `019` and absorbs `020`, per
[consolidate at port time](../uplifts/26.8/execution-plan.md#consolidate). That
identity bundles four historically separate changes: the base feature
(2024-12-30), the forced `ON CLUSTER` `DROP` enforcement with its skip flag
(2025-04-21), the `CHECK` privilege (`020`, a single line inside this patch's own
privilege set), and the `query_id` root-cause fix (2026-06-22).

Two rewrites are new on this line and are **not** carried from 26.3 — see
[`skip_distributed_checks`](#skip-distributed-checks-must-be-narrowed) and
[the elevation primitive](#the-elevation-and-its-primitive), plus
[the hardcoded named collection](#the-fourth-undeclared-dependency). A fourth
simplification concerns the `query_id` workaround. `15b469ab133` patched the two
call sites it knew about by clearing the id before the internal `executeQuery`;
`45d490db20f` added the root-cause fix in `ProcessList::insert` three weeks
later, together with a third call site the per-site approach had missed — but it
did **not** remove the per-site clears, so 26.3 ships both forms at once. Carry
only the root-cause fix: once `insert` regenerates a colliding id for internal
queries the three call-site clears are redundant, and leaving them in disguises
where the invariant actually lives.

One `setCurrentQueryId("")` call does survive, and it is worth being clear that
it is not one of those three. Deriving the internal context from the global one
(see [the elevation primitive](#the-elevation-and-its-primitive)) means it
inherits no `query_id` at all, and `ProcessList::insert` rejects an empty one
outright with a `LOGICAL_ERROR`. So that call *initialises* an identity rather
than avoiding a collision — a consequence of dropping `setGlobalContext`, not of
the `query_id` story. Collisions remain handled once, at the root.

## Background (ClickHouse) {#background}

Four mechanisms have to be held in mind at once.

**`CREATE DATABASE` is a privilege, and a database's engine is an argument.**
`CREATE DATABASE d ENGINE = Replicated('<zk_path>', '<shard>', '<replica>')`
takes a ZooKeeper path and shard/replica identities as literal arguments, and
can carry `SETTINGS`. Whoever holds `CREATE DATABASE` therefore chooses not
just *whether* a database exists but what engine backs it and where in
ZooKeeper it lives.

**`ON CLUSTER` is a separate authority from the DDL itself.** It pushes the
statement onto the distributed DDL queue so every host runs it. Upstream gates
that on two things beyond the statement's own privilege: the
`allow_distributed_ddl` setting, and the global `CLUSTER` privilege. Both are
checked in `executeDDLQueryOnCluster`, independently of the
`AccessRightsElements` the caller passes as `params.access_to_check`.

**Internal queries are ordinary queries with a flag.** `executeQuery` accepts
`QueryFlags{ .internal = true }`, which suppresses quota accounting, query log
and similar bookkeeping. It does **not** suppress process-list registration:
`executeQuery` passes `internal` to `ProcessList::insert` as data rather than
using it as a gate. This matters — see [drift](#drift-on-this-uplift).

**A `Context` resolves privileges from its `user_id`.** `getAccess` derives the
effective rights from that field, so a context whose `user_id` is empty
resolves to unrestricted access. Cloning a context and clearing the user is
therefore an escalation, which is precisely how this patch performs one.

## Component tour {#component-tour}

**`src/Core/ServerSettings.cpp`** declares the three settings that gate
everything: `aiven_user_with_indirect_database_creation`,
`aiven_reserved_replicated_database_prefixes` and `aiven_cluster_database`. All three
default to `""`, so an unconfigured server is meant to behave as upstream.

**`src/Interpreters/InterpreterCreateQuery.{h,cpp}`** is where the feature
lives. Entry point is `execute`. The patch adds `createReplicatedDatabaseByClient`
(builds and runs the canonical statement), `checkDatabaseNameAllowed` (reserved
prefixes) and extracts `checkMaxDatabaseNumToThrow` out of `createDatabase` so
the new path can reuse the database-count limit without duplicating it.

**`src/Interpreters/InterpreterDropQuery.cpp`**, entry point
`executeSingleDropQuery`, holds the mirror half: the forced `ON CLUSTER` rewrite
for `DROP DATABASE`. It sits above the existing `ON CLUSTER` dispatch, which is
the only placement that works — the dispatch returns before
`executeToDatabaseImpl` and its inline `DROP_DATABASE` check are ever reached.

**`src/Interpreters/Access/InterpreterGrantQuery.cpp`**, entry point `execute`,
expands the new shortcut into a concrete `GRANT` string and runs it as an
internal query. It has to do this before `eraseNotGrantable` and the
`TABLE ENGINE` validation, both of which would otherwise mangle the synthetic
`AccessType::ALL` element the parser produced.

**`src/Parsers/Access/ParserGrantQuery.cpp`**, with `ASTGrantQuery.h` and
`CommonParsers.h`, adds the `DEFAULT REPLICATED DATABASE PRIVILEGES` keyword and
the `default_replicated_db_privileges` AST flag. This is the file that
`022`/`079` and `N07` also contend for; see the
[ordering constraints](../uplifts/26.8/execution-plan.md#ordering-constraints).

A hazard worth knowing before adding a keyword anywhere in the fork. `Keyword`
and the string table it indexes are both generated from the same macro list in
`CommonParsers.h`, so inserting a keyword renumbers every enumerator after it.
That is fine for a clean build and fine in CI, but an incremental local build can
recompile the string table while leaving untouched translation units holding the
old numbering, and then unrelated keywords silently resolve to the wrong string —
`CREATE USER … IDENTIFIED` stops parsing, `DEFAULT ROLE` matches only `DEFAULT`.
Nothing warns; the build succeeds. If a keyword insertion produces syntax errors
in statements you never touched, suspect stale objects before suspecting the
patch, and force a rebuild of everything that references `Keyword`.

**`src/Interpreters/executeDDLQueryOnCluster.{h,cpp}`** gains
`skip_distributed_checks` on `DDLQueryOnClusterParams`, suppressing the
`allow_distributed_ddl` and `CLUSTER` checks named above.

**`src/Interpreters/Context.{h,cpp}`** gains `setGlobalContext`, which clears
`user_id` on the cloned context used for the internal work.

**`src/Databases/DatabaseReplicated.{h,cpp}`** retains the *pre-expansion* shard
argument as `shard_macros` alongside the expanded `shard_name`, exposed via
`getShardMacros`, so the generated statement can reuse the macro text rather
than a value already expanded for this host.

**`src/Interpreters/ProcessList.cpp`**, entry point `insert`, carries the
`query_id` fix from the second commit.

## Problem {#problem}

A customer administrator cannot create a database over plain SQL, because the
only privilege that would allow it, `CREATE DATABASE`, allows far more than the
managed service can permit. Holding it lets a principal choose a non-`Replicated`
engine — producing a database that exists on one replica and silently diverges
from the rest of the cluster — or pick an arbitrary ZooKeeper path, colliding
with or reading another database's coordination state, or attach engine
`SETTINGS` that the service's replication contract depends on. There is no
narrower upstream privilege that says "may create a database, but only a
correctly-parameterised `Replicated` one".

So database creation has to go through the control plane's API instead, and
every tool, migration script or ORM that expects `CREATE DATABASE` to work over
SQL breaks against an Aiven service.

`DROP DATABASE` has the converse problem. Executed locally it removes the
database from the node that received it and leaves it on every other replica,
so a tenant who deletes a database is left with a cluster that disagrees about
whether it exists, and storage that is never reclaimed.

## Approach {#approach}

One configured user gets an intercepted `CREATE DATABASE` that the server
executes on their behalf, in a form the service can guarantee.

1. **Three config-only settings, all defaulting to `""`.** With
   `aiven_user_with_indirect_database_creation` empty, no interception happens at all.
2. **Interception is by exact user name.** In `execute`, when the statement is a
   `CREATE DATABASE`, the query is not internal, and the caller's name equals
   `aiven_user_with_indirect_database_creation`, the patch first rejects everything it
   cannot guarantee — a non-`Replicated` engine, any engine arguments, any engine
   `SETTINGS`, and an explicit `ON CLUSTER` — and then hands off to
   `createReplicatedDatabaseByClient`.
3. **The server writes the statement, not the user.** That function composes
   `CREATE DATABASE <name> ON CLUSTER <aiven_cluster_database> ENGINE = Replicated(…)`
   with the ZooKeeper path derived from the database name and the shard argument
   reused from the reference database's retained macro text, runs it as an
   internal query on a cloned context with the user cleared, and then grants the
   caller a curated privilege set. It also appends a `SETTINGS` clause naming a
   named collection — see [the fourth dependency](#the-fourth-undeclared-dependency).
4. **The curated set is a new statement,** `GRANT DEFAULT REPLICATED DATABASE
   PRIVILEGES`, expanding to table and dictionary DML/DDL, `SELECT`, `SHOW`,
   `INSERT`, `OPTIMIZE`, `TRUNCATE`, `dictGet`, `SYSTEM SYNC REPLICA` and
   `CHECK`, `WITH GRANT OPTION`. It includes `DROP DATABASE` — except when the
   target is `aiven_cluster_database` itself, which must not be droppable — and
   deliberately never includes `CREATE DATABASE`, access management or
   `SYSTEM SHUTDOWN`.
5. **Reserved prefixes** from `aiven_reserved_replicated_database_prefixes` are refused
   for everyone, not just the configured user, so service-owned name spaces
   cannot be shadowed.
6. **`DROP DATABASE` is forced cluster-wide** for principals that are not exempt,
   and `DETACH DATABASE` is refused outright, which closes the divergence
   described above.

### The elevation and its primitive {#the-elevation-and-its-primitive}

Point 3 is a real privilege elevation and worth being explicit about, because it
is easy to misread as a bug. `createReplicatedDatabaseByClient` returns from
`execute` **before** `checkAccess(getRequiredAccess())` runs, so for this one
user the `CREATE DATABASE` privilege is never checked. That is the feature: the
curated set at point 4 deliberately withholds `CREATE DATABASE`, and the
interception is what replaces it. The safety of the whole patch therefore rests
on the elevation being confined to the two internal statements the server itself
composed, which is what the non-escalation test exists to pin down.

The primitive used to perform it should not be carried as-is. `setGlobalContext`
does not set the global context; it clears `user_id` so the clone resolves to
unrestricted access. As a public method on `Context` it becomes a permanent
escalation primitive available to any future caller, in one of the most
contended headers in the tree. Prefer deriving the internal context from the
global one with the existing idiom, so `Context` needs no new entry point at
all; if one is genuinely required, make it a scoped guard whose name says what
it does.

### `skip_distributed_checks` must be narrowed {#skip-distributed-checks-must-be-narrowed}

Forcing `ON CLUSTER` onto a user creates an obligation: having rewritten their
local statement into a distributed one, the server must not then charge them for
authority they never asked for. A user with `DROP DATABASE` but no `CLUSTER`
privilege would otherwise be refused a `DROP` that upstream would have allowed.
`skip_distributed_checks` discharges that obligation by suppressing the
`allow_distributed_ddl` and `CLUSTER` checks.

The reasoning is sound; its application is not. On the prior lines the flag is
set unconditionally in the branch that dispatches `DROP DATABASE` to the
cluster, and that branch is reached by **two** routes: because the rewrite above
set `drop.cluster`, or because the user wrote `ON CLUSTER` themselves. The
second route does not depend on `aiven_cluster_database` being configured. The
consequence is that on a server with the feature switched off, any principal
holding `DROP DATABASE` on the target can run
`DROP DATABASE d ON CLUSTER <any configured cluster>` without the `CLUSTER`
privilege and regardless of `allow_distributed_ddl`.

It is bounded — `params.access_to_check` still requires `DROP DATABASE`, and that
check is not skipped — but `CLUSTER` is the privilege whose entire purpose is
deciding who may fan DDL out across hosts, so this is a genuine relaxation
reaching users the feature was never about. The port sets the flag only on the
forced route, by threading a boolean out of the rewrite block rather than
setting it at the dispatch.

The exposure is not new: the flag arrived on 2025-04-21 and has shipped on every
Aiven line since, so this is a live finding against 24.8, 25.3, 25.8 and 26.3 as
well as a correction to make here. It needs its own ticket against those lines
rather than being quietly fixed only on the way to 26.8.

### The fourth, undeclared dependency {#the-fourth-undeclared-dependency}

The composed statement ends with `SETTINGS collection_name='cluster_secret'`,
and that name is a string literal in the source. So the feature has a fourth
piece of required configuration beyond its three settings — a named collection
that must exist, under exactly that name — and unlike the three it is neither
declared, nor defaulted, nor discoverable from `system.server_settings`. A
server with all three settings correctly set still fails every `CREATE DATABASE`
through this path if that collection is missing, and the error will be about the
collection rather than about the feature.

Two smaller oddities travel with it. The patch refuses user-supplied engine
`SETTINGS` and then injects its own, which is coherent — the point is that the
server chooses them — but only if the injected set is the service's policy
rather than an accident. And the interception is otherwise carefully
config-driven, so a bare literal in the middle of it is the one place the
feature cannot be reconfigured without a rebuild.

Not a defect on its own — the collection does exist in production, and the
downstream suite exercises this path constantly, which is why it has never been
felt.

**Resolved on this line.** The `SETTINGS` clause is now derived from the
reference database the same way the shard macro already was, through a new
`getCollectionName` accessor, and omitted entirely when that database names no
collection. No knob was added. Note that the accessor has to be defined in the
`.cpp`: the settings-name namespace is generated by macro there and is not
visible to the header, so an inline definition does not compile.

## Concept {#concept}

A `skip_*` or `force_*` parameter is justified per **route into the call site**,
never per feature. The justification for skipping a check usually comes from one
specific path — here, "we rewrote the statement, so we must not bill the user for
the rewrite" — but the parameter gets set at the point where several paths have
already merged, and it then applies to all of them. Feature gates make this
harder to see, because the gate is usually checked upstream of where the
relaxation actually lands, so "the feature is off by default" can be true of the
patch and false of the line.

The second, smaller idea: when a feature must exceed the caller's privileges,
have the server *compose and execute the privileged action itself* on a separate
context, rather than widening the caller's grants. The blast radius is then the
two statements you wrote, not everything the new grant permits — and it is
auditable, because the statement text is right there in the source.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-but-rewrite`. No upstream equivalent exists — neither
`aiven_cluster_database` nor any comparable notion of a server-composed database
creation appears in 26.8 — and the host code is structurally intact, so the
rewrite is driven by the two corrections above rather than by drift.

- **The `query_id` failure mode is live on 26.8.** `executeQuery` still registers
  internal queries: it passes `internal` to `ProcessList::insert` as an argument
  and does not gate the call on it. `ProcessList::insert` has no branch that
  regenerates an id for internal queries, and its duplicate-id guard is not
  exempted for them, while the cross-user arm of that guard is conditioned on no
  setting at all. So the second commit is mandatory, not polish. The 25.8 line
  needed no such commit because internal queries were not registered there,
  which is why this is the one part of the patch with no prior-line template.
- **The two lines the patch relaxes are unmoved**, at the `allow_distributed_ddl`
  check and the `CLUSTER` check in `executeDDLQueryOnCluster`, with
  `params.access_to_check` still enforced separately below them.
- **The `DROP` graft point is unmoved**: the `ON CLUSTER` dispatch for databases
  still precedes `executeToDatabase`, and `getRequiredAccessForDDLOnCluster`
  still declares only the plain `DROP_DATABASE` element — so a check placed
  downstream in `executeToDatabaseImpl` would be bypassable, and the prior
  lines' placement above the dispatch remains correct.
- **The `createDatabase` refactor still applies**: the
  `max_database_num_to_throw` block the patch factors out is intact.
- **Verify at port time** that the `DatabaseReplicated` constructor signature and
  `registerDatabaseReplicated` argument handling still match the shape the
  `shard_macros` retention assumes, and that a plain user issuing the new `GRANT`
  shortcut directly still cannot escalate — that guarantee rests on 26.8
  enforcing grant options for internal queries, which is worth asserting rather
  than assuming.

## Downstream contract {#downstream-contract}

The control plane does not merely configure this feature, it *calls* it, and its
own test suite exercises it against a live cluster. Four properties follow, and
the port must not break any of them.

- **The `GRANT` shortcut has a second caller.** The control plane issues
  `GRANT DEFAULT REPLICATED DATABASE PRIVILEGES ON <db>.* TO <user> WITH GRANT OPTION`
  itself, to keep privileges identical however a database came to exist. So the
  statement is a supported external interface, not an internal detail of
  `createReplicatedDatabaseByClient`, and it must work when issued by an
  ordinary admin session. Note the explicit `WITH GRANT OPTION`: the parser
  accepts it because the shortcut falls through to the shared grant-option
  parsing, but the expansion hardcodes `WITH GRANT OPTION` and ignores the flag.
  The suffix is therefore accepted and redundant — and the curated set is
  granted with grant option whether or not the statement says so.
- **The reference database is `default` in production.** The managed
  configuration sets `aiven_cluster_database` to `default`, and a downstream test
  asserts that the customer admin cannot drop it. That is exactly the branch
  which omits `DROP DATABASE` from the curated set when the target *is* the
  reference database, so that branch is load-bearing in production and not an
  edge case. Related: the `LOGICAL_ERROR` raised when the reference database is
  not `Replicated` names `default` literally, which is misleading now the name is
  configurable — the message should name the configured value.
- **Reserved prefixes are supplied as backquoted identifiers.** Production
  configures three, and one of them has no trailing separator, so it reserves
  every name beginning with that word rather than a delimited namespace. The
  setting is parsed with `parseIdentifierOrStringLiteral`, so quoted and bare
  forms both work, but the *quoted* form is the one that actually ships.
- **Database names arrive hostile and are tested that way.** The downstream suite
  creates databases through this path under a dozen adversarial names —
  backquoted names containing spaces, punctuation, a backquote, a backslash, and
  `\b`, `\f`, `\n`, `\r`, `\t`, `\0`. That surface is wider than the grantee name
  our own test covers, because the database name reaches three different sinks:
  the ZooKeeper path through `escapeForFileName`, and two separate `backQuote`
  interpolations in the generated `CREATE` and `GRANT`.

The settings expectations downstream are keyed by ClickHouse version, and the
configuration model can disable a setting per version, so the `aiven_` rename in
[the naming policy](../uplifts/26.8/execution-plan.md#setting-naming) is
expressible: declare both spellings and disable each for the versions where it
does not belong. The mechanism strips settings rather than renaming them, which
is why the transition is two declared fields rather than one renamed field.

## Customer impact {#customer-impact}

**Who.** The single user named by `aiven_user_with_indirect_database_creation` gains
the new behaviour. The reserved-prefix and forced-`DROP` rules apply to every
principal without the exempting privilege. On an unconfigured server nothing
changes.

**What they see.** `CREATE DATABASE d` succeeds over plain SQL and the caller
comes out holding the curated set on `d.*` `WITH GRANT OPTION`, so they can
re-grant it to their own users. Statements the server cannot guarantee are
refused with specific errors: `ACCESS_DENIED` "Only Replicated database can be
created through SQL." for another engine, `UNSUPPORTED_PARAMETER` for engine
arguments, for engine `SETTINGS`, and for an explicit `ON CLUSTER`, and
`ACCESS_DENIED` "Database name cannot start with '…'" for a reserved prefix.
On the `DROP` side, `DETACH DATABASE` is refused with `ACCESS_DENIED` "Database
detach is not allowed.", and naming a cluster other than `aiven_cluster_database` is
refused with `ACCESS_DENIED` "Cannot execute query on specified cluster."; an
ordinary `DROP DATABASE` silently becomes cluster-wide.

**New versus the previous Aiven LTS.** `CHECK` is in the curated set, so a
holder can run `CHECK TABLE` on tables in its own database — it is not implied
by `SELECT` or `SHOW` and previously required a separate grant. And the
narrowing described [above](#skip-distributed-checks-must-be-narrowed) is a
tightening a tenant can observe: a user who writes `ON CLUSTER` explicitly on a
`DROP DATABASE` again needs the `CLUSTER` privilege and
`allow_distributed_ddl`, as upstream requires and as 26.3 did not.

**Configuration keys change name.** The three settings gain an `aiven_` prefix
on this line, so a server configuration written for 26.3 does not merely fail to
enable the feature — `ServerSettings::checkUnknownSettings` rejects the old
top-level keys at startup and on every config reload, so a stale configuration
fails loudly instead of silently reverting `DROP DATABASE` to local-only.

**How to restore.** Clear the three server settings and the feature is inert
without a rebuild; databases already created remain ordinary `Replicated`
databases and grants already issued remain ordinary grants.

## Tests {#tests}

- Path: `tests/integration/test_aiven_indirect_database_creation/`
- What the tests prove (causation): the module carries 11 cases, and the two that
  carry the argument are `test_f_non_escalation`, which proves the cloned-context
  elevation does not leak into the invoking session — the invariant the whole
  approach rests on — and `test_h_grantee_injection`, which proves the generated
  SQL cannot be steered by a hostile grantee name. The rest cover the happy path,
  refusal for a non-configured user, the exact contents of the granted set
  (including `CHECK`, which is how `020` is validated), reserved prefixes and
  their Replicated-only scope, forced `ON CLUSTER` on a non-exempt `DROP`, that
  the `DROP` privilege itself is still not bypassed, and that both `CREATE` and
  `DROP` behave as upstream when the settings are empty.
- The feature needs Keeper and a reference `Replicated` database, so this is
  integration rather than stateless; the config also has to declare the named
  collection the generated statement hardcodes, which is the only reason that
  [undeclared dependency](#the-fourth-undeclared-dependency) is invisible here.
- **Added on this line**, 25 cases from 11. Four of the five additions came from
  reading the [downstream contract](#downstream-contract).
  1. `test_e3_explicit_on_cluster_still_requires_cluster_grant` covers the
     narrowing, and covers the gap a neighbouring downstream test leaves. That
     test does assert an unprivileged user's `DROP DATABASE … ON CLUSTER …` is
     refused on every replica, but its actor holds *no* privileges, so it is
     refused by the `DROP DATABASE` element in `access_to_check` and never
     reaches the question of `CLUSTER`. Ours holds `DROP DATABASE` and lacks
     `CLUSTER`, with the feature off, and then grants `CLUSTER` to prove that was
     the missing privilege and not something incidental.
  2. `test_i_database_name_injection`, eleven adversarial database names. This is
     the wider surface: the name reaches the ZooKeeper path through
     `escapeForFileName` and two independent `backQuote` interpolations, where
     the pre-existing grantee case covers only one sink.
  3. `test_d3_reserved_prefix_quoted_and_bare_forms` covers the quoted form
     production actually ships, and a bare word with no trailing separator.
  4. `test_c2_reference_database_is_not_droppable` pins the branch that omits
     `DROP DATABASE` when the target is the reference database — load-bearing in
     production, and previously asserted only downstream.
  5. `test_a` now also asserts the collection name was copied from the reference
     database rather than hardcoded.
- **Still owed.** The `query_id` fix arrived on 26.3 with a `gtest`. Prefer
  provoking the collision through SQL, per the repository's preference for
  functional tests, and keep the `gtest` only if it cannot be reached that way.
  Note the collision is in fact exercised end-to-end here — the inner `GRANT`
  spawns a further internal query under a still-live id, which is the third site
  the per-site workaround missed — so `test_f`'s assertion that the inner grant
  landed is already a functional guard on it, just not a named one.
- The test config's comments no longer name the private control plane by
  repository name; they were rewritten as behaviour, because tests ship publicly.

## Rollback {#rollback}

Safe to revert. Clearing the three settings disables the feature without a
rebuild, and reverting the binary leaves nothing to reconcile: the databases it
created are ordinary `Replicated` databases and the privileges it granted are
ordinary grants in access storage, so no on-disk or ZooKeeper state is orphaned.
The one consequence worth stating is that those grants outlive the revert — a
tenant keeps the curated set on databases created while the feature was on,
including the `GRANT OPTION`, and removing them is a separate `REVOKE`.

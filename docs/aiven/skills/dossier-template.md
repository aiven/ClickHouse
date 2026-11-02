---
description: 'Template and writing rules for an Aiven patch dossier, the durable per-patch explanation that ships with the code'
sidebarTitle: 'Dossier template'
slug: '/aiven/skills/dossier-template'
title: 'Skill — dossier template'
doc_type: 'guide'
---

# Skill — dossier template {#skill-dossier-template}

A dossier is `docs/aiven/patches/<NNN>-<slug>.md`. Write for a **human teammate**
who knows ClickHouse but may not know this Aiven corner — teach briefly, then
prove.

## What a dossier is {#what-a-dossier-is}

The durable explanation of one patch identity. It describes the patch **as it
is on this line**, and it moves with the code: staged in the same commit as the
port, updated by every later `patch-fix`, and carried onto the next line inside
that line's first commit of the patch. Because each patch commit touches the
dossier, `git log --follow` on the file is the patch's full history across lines.

So the path and the frontmatter slug carry no version, and the body never links
to anything that cannot exist on the next line — no tracker URLs, no uplift
files. Link another dossier only if it is already on this line; otherwise cite
its identity in code font.

Keep conflict novels and hunk diaries out (`tmp/` or the prior line's git).

## Section map {#section-map}

The **screen** phase is written before any code is staged, because those
sections *are* the screening decision. A screening that drops the patch stops
there and stages the dossier alone as `patch-drop(<NNN>)`. The **port** phase is
written with the code, from the code as ported — not from the source commit
message.

| Section | Phase | Required |
|---|---|---|
| Summary | screen, revised at port | yes |
| Lineage | screen (row), port (outcome) | yes |
| Background | screen | yes |
| Component tour | port | yes, unless the patch drops |
| Problem | screen | yes |
| Decisions | screen, extended at port | when a reviewer could contest a choice |
| Approach | port | yes, unless the patch drops |
| Concept | port | yes — one line if nothing generalizes |
| Drift on this uplift | screen | yes |
| Customer impact | port | yes — one line if none |
| Security review | screen (threat model, ways in), port (the rest) | yes — one line if no security surface |
| Tests | port | yes, unless the patch drops |
| Rollback | port | yes, unless the patch drops |
| Retirement | port | yes |

Length follows risk, not diff size. A config-only patch can be a page; an
access-control rewrite earns its length. The Summary must stand alone either way.

## Public, not private {#public-not-private}

Dossiers live under `docs/`, so they follow the repo-wide documentation rules:
a frontmatter block before the first heading, and an explicit `{#kebab-anchor}`
on **every** heading. Keep the anchors stable — they are linkable URLs.

`docs/` ships in the fork's public repository, so never name the private
control-plane repository, its packages, its modules or its test file paths.
Write "the control plane" and restate what it does — the SQL it issues, the
property it depends on, the assertion its tests make. Product-visible names are
fine, because a customer already sees them: `avnadmin`, `aiven_admin_role`,
server settings, error codes. The test is whether a reader outside Aiven could
learn the private project's internal layout from the sentence. If yes, rewrite
it as behaviour.

## Frontmatter fields {#frontmatter-fields}

The first five keys are the repo-wide documentation fields. The `aiven_*` keys
are durable facts about the patch — they change only when the code does — so
indexes, release notes and the fork-settings list can be generated from them.
They never hold planning state; status, ticket and owner live in the tracker.

| Key | Value |
|---|---|
| `aiven_identity` | `'<NNN>'` or `'N<nn>'` |
| `aiven_absorbs` | identities folded into this one, or `[]` |
| `aiven_changelog_category` | one of the ClickHouse PR template categories: `New Feature`, `Experimental Feature`, `Improvement`, `Performance Improvement`, `Backward Incompatible Change`, `Build/Testing/Packaging Improvement`, `Critical Bug Fix`, `Bug Fix` |
| `aiven_settings` | fork-local settings this patch defines, as `'<name>'`, or `'<old> -> <new>'` when this line renamed it; `[]` if none |

## Template {#template}

````markdown
---
description: '<one line, no trailing period>'
sidebarTitle: '<NNN>: <very short label>'
slug: '/aiven/patches/<NNN>-<slug>'
title: 'Patch <NNN>: <short title>'
doc_type: 'reference'
aiven_identity: '<NNN>'
aiven_absorbs: []
aiven_changelog_category: '<category>'
aiven_settings: []
---

# Patch <NNN> — <short title> {#patch-<NNN>-<slug>}

## Summary {#summary}

Three sentences, in this order: what the patch changes, why Aiven carries it,
and what a customer or operator observes. A reader who stops here should know
whether this patch concerns them.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| <prior LTS> | `patch-port(<NNN>)` `<sha>`, `patch-fix(<NNN>)` `<sha>` | ported as two commits |
| <this LTS> | `patch-port(<NNN>)` | `still-needed-but-rewrite`, one commit; absorbs `<MMM>` |

One row per LTS, appended when the patch reaches a new LTS and never rewritten
afterwards. Outcome is the drift conclusion or the drop reason. SHAs are
evidence only — a rebase re-dates them. Name any identity this port absorbs,
and any fork-local setting renamed (old → new).

## Background (ClickHouse) {#background}

A few sentences of **necessary** context — not a textbook chapter. Name the
subsystem (`Access`, `CertificateReloader`, `RefreshTask`, S3 disk, …) and the
invariant or API surface the reader must hold in mind. Define jargon once
(e.g. what `<ca_path>` does, what `ON CLUSTER` skips).

## Component tour {#component-tour}

Orient the reader in the code they are about to read. For each file the patch
touches, say **which layer it belongs to** and **how the layers relate** —
interpreter vs analyzer vs storage vs system table, foreground vs background,
user-facing vs server-internal. Name the entry point (`executeImpl`,
`fillData`, `generate`, …) so the reader can find it without grepping.

Background explains the *problem*; the tour explains the *terrain*. Keep it to
a short paragraph per file.

## Problem {#problem}

The **unpatched** world. What goes wrong for Aiven (or customers) *without*
this patch? Symptom, blast radius, who hits it. Prefer "a service user can
read another tenant's DDL" over "improve security."

Do not describe the patched behavior here — that is Customer impact. If a
sentence starts with "after this patch" or "the user now sees," it belongs
there.

## Decisions {#decisions}

### <The decision, stated as a fact> {#decision-<kebab>}

- **Chosen:** what we do, in one sentence.
- **Rejected:** the alternative a reviewer would propose, and why it loses.
- **Rung:** the mechanism from the porting-doctrine ladder, and why the cheaper
  rungs do not serve.
- **Supersedes:** the earlier decision this replaces, if any.

Record the choices a reviewer would otherwise re-open: the mechanism, the
layers chosen for a feature Aiven does not offer, a scope boundary, a behaviour
deliberately kept from the prior line or changed from it. A decision is not
edited when it changes — add the new one below with **Supersedes**, and leave
the old one readable.

## Approach {#approach}

What we change and **why that fixes it**. Call out gates/settings (especially
default-off), intentional non-goals, and anything a reviewer might misread as
a bug (e.g. "client TLS still loads; only server certs skip").

## Concept {#concept}

The one transferable idea behind this patch — what the engineer should still
know six months from now, after forgetting the diff. A ClickHouse mechanism, an
invariant, or a rule of thumb that generalizes past this patch (e.g. "metadata
is reachable through both a `SHOW` interpreter and a `system.*` table",
"`checkAccess` throws, `isGranted` returns a bool, and scans must not throw").

If the patch taught you nothing generalizable, say so in one line instead of
padding. If it taught a rule for every port, propose it for
`porting-doctrine.md` in a separate commit.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-and-applies` | `still-needed-but-rewrite` |
`obsoleted-by-upstream` | `irrelevant-by-removal`

Short bullets only if something moved (symbol rename, new parameter, a new
door into the same surface). This section describes **this line only** and is
replaced on the next uplift; earlier conclusions stay in Lineage and in git.

## Customer impact {#customer-impact}

The **patched** world, plus the delta against the prior Aiven line. What a
tenant, operator, or control-plane query notices *after* this patch lands.
Changelog, not justification (justification is Problem).

Cover, when they apply:

- **Who** is affected (role, grant, or endpoint).
- **What they see** (`ACCESS_DENIED`, empty cell, `404`, a setting to flip).
- **How to restore** the previous behavior without a rebuild (grant,
  `http_handlers` rule, existing setting).
- **New versus the prior Aiven line**, if this is a rewrite. A newly gated
  column or door belongs here even when the policy itself did not change.
- **Upstream tests edited**, and why — each one is evidence of upstream-visible
  behaviour change.
- **Example**, when a query can show the change: one short session, run as a
  named kind of user, with the result before and after the patch.

```sql
-- As a user with SHOW TABLES on db.t but no CREATE TABLE.
SHOW CREATE TABLE db.t;
-- Before: CREATE TABLE db.t (`id` UInt64) ENGINE = MergeTree ORDER BY id
-- After:  Code: 497. DB::Exception: ... (ACCESS_DENIED)
```

Take the statements from the patch's own test and the output from a real run,
trimmed with `...`, so the example stays true for as long as the test passes.
Use placeholder names and no real hosts or credentials. When no query shows the
change — a build flag, a race, a background task — show the observable instead
(a `system.*` query, a log line, a config snippet) or leave the example out.

If nothing user-facing changes, one line: `none — same as <prior LTS>` or
`none — no user-visible surface`.

## Security review {#security-review}

Required in full when the patch touches a security surface: access control or
grants, authentication, TLS, HTTP handlers or other endpoints, secrets in
output (`system.*` tables, logs, error messages), config loading, physical
deletion of data, a feature that runs code (UDFs, `ShellCommand`, executable
dictionary sources), SQL stored now and run later, reading server-local files,
decoding client-supplied data (compression, input formats), `BACKUP` and
`RESTORE`, or a build-time feature gate. Otherwise one line:
`n/a — <why there is no security surface>`.

Screen phase — these decide the mechanism:

- **Threat model:** who the attacker is and what they gain — a tenant SQL user
  (including `avnadmin`), an unauthenticated network client, anyone who can
  write a file the server reads as config, another tenant. Follow the chain to
  its end, not just the first step: a SQL user who can define a function that
  runs a program has a process on the host, and from there the container and
  whatever it can reach. Say which link this patch closes, and assume its
  check is bypassed anyway: which layer stops the attacker next? When the
  service does not offer a feature, removing it at build time is the layer
  that cannot be bypassed.
- **Identity:** whose identity does the guarded code act as, and where does
  that identity come from? It must come from authentication, never from a
  field the client sends. SQL that the server stores now and runs later —
  views, dictionaries, defaults, mutations, background tasks — runs as
  whoever the server runs it as. If that is the server itself and the result
  can reach the user who wrote it, the user has borrowed the server's
  privileges.
- **Ways in:** every path to the protected thing on this line, as a table
  (Path | Kind | Covered by). Kinds to check: `SHOW` interpreter, `system.*`
  table, implicit grants, `ON CLUSTER` or DDL replay, replication, cascades
  and background tasks, config, HTTP handler, `clickhouse-local`. A feature
  is often registered under several names — engine, table function, its
  `Cluster` variant, a dictionary source, a disk setting — so a removal lists
  every name. Rebuild the table on every uplift — new upstream versions add
  paths and names.

Port phase:

- **Default:** is the protected state what a server gets with no extra
  config? If protection needs a setting, config section or build flag, say who
  sets it and which test exercises the protected state.
- **Lifecycle:** does the guard hold across startup, `SYSTEM RELOAD CONFIG`,
  shutdown, restart, `ON CLUSTER` and DDL replay, replication, and state
  written by an older or newer binary?
- **Preservation:** can a statement that does not mention the property change
  it — for example, `ALTER USER x SETTINGS …` clearing a protection flag? Name
  the test that proves it cannot.
- **Release build:** anything that behaves differently there. `assert_cast` is
  a `static_cast`, and `chassert` and `#ifndef NDEBUG` checks compile out, so a
  wrong type is undefined behaviour in release, not an exception.
- **Over-blocking:** which upstream tests of the subsystem were run, and the
  result. A guard that refuses legitimate statements is a defect too, and the
  `aiven_*` tests alone do not show it.
- **Untrusted input:** identifiers or strings from SQL, config or a file the
  server reads that reach a file path, a Keeper path or generated SQL — how
  they are escaped (`backQuote`, `escapeForFileName`) and which test uses
  hostile names. Check the value in the form that is used, not a normalized
  copy of it: a check on `lexically_normal()` passes `a//b` while the raw
  string is what gets joined, and `fs::path::operator/` with an absolute right
  side discards the left one. Reject non-canonical input rather than
  canonicalizing it in one place.
- **Accepted risks:** each with why it is accepted and that a follow-up exists
  in the tracker. Never how to trigger it: the dossier is public, so the
  details of an unfixed weakness stay in the tracker until the fix lands.

A fix for a vulnerability that is not public yet gets no public patch, dossier
or commit message at all — the code itself discloses it. It is carried
privately until disclosure, usually when upstream ships its fix. Then it lands
here like any other patch, and its Retirement names the upstream version that
carries the fix.

## Tests {#tests}

- **Paths:** `aiven_<NNN>_<slug>.*` and/or `test_aiven_<slug>/`
- **Proves:** the causal claim, in one sentence — what fails without the patch.
- **Run:** the exact command (`./tests/clickhouse-test aiven_<NNN>_<slug>`, or
  the integration invocation from `runbooks/integration.md`).
- **Evidence:** FAIL on parent / PASS with patch, recorded in the halt report —
  or `no_justified` naming the upstream test that covers it.
- **Certificates:** the generator script, if TLS is involved; never committed PEMs.

## Rollback {#rollback}

- **Revert:** is reverting the commit safe on a running fleet?
- **Persistent state:** what outlives a revert — on disk, in Keeper, in access
  entities — and how to clean it up before a downgrade.
- **Disable without rebuild:** the setting, grant or config that makes the
  patch inert, or `none`.

## Retirement {#retirement}

What would make this patch unnecessary: an upstream setting or feature that
would cover it, a control-plane change that would obviate it, or a condition to
re-check on the next uplift. If nothing could, one line: `permanent — <reason>`.
````

## Tone {#tone}

Plain language, short paragraphs, active voice. Explain *concepts* when they
unlock the review; skip filler and checklist theatre.

Example pair for Problem and Customer impact (do not copy into a real dossier):

- Problem: "Any user who can see a table can read its full `CREATE`
  statement."
- Customer impact: "That user now gets `ACCESS_DENIED` on `SHOW CREATE`
  and empty `system.tables.create_table_query`. Restore with `CREATE TABLE`.
  Versus the prior line, `system.databases.engine_full` also goes empty."

## Self-check before staging {#self-check}

1. Does the Summary alone tell a reader whether this patch concerns them?
2. Does every sentence in Customer impact name something observable? If it can
   move into Problem unchanged, it belongs in Problem. If a query can show the
   change, is there an Example taken from a real run?
3. Was Component tour written from the code as ported, not from the source
   commit message?
4. Would a reader outside Aiven learn anything about the control plane's
   internal layout? If so, restate it as behaviour.
5. Does any link point at a file that cannot exist on the next line?
6. Is every contested choice a Decision, with what it rejected?
7. If the patch has a security surface, does Security review follow the attack
   chain to its end and list every way in on this line — without describing
   how to exploit a weakness that is not fixed yet?

## Bringing a dossier onto a new line {#bringing-a-dossier-onto-a-new-line}

First find out whether the prior `-dev` line has a dossier at all:

```bash
git ls-tree --name-only <prior-dev> -- docs/aiven/patches/ | rg '/<NNN>-'
```

**No prior dossier** is the common case — the dossier system began with the
26.8 uplift, so older lines carry none. Create the file from the template and
write it from the source commits and the code as ported; the source commit
messages are input, not text to copy.

**A prior dossier exists**: `git checkout <prior-dev> -- <file>` while staging
the port, then:

1. Lineage: if the prior `-dev` line is the **same LTS** (a retarget to a later
   patch release), update that LTS's row in place — it is still the same
   uplift. If it is an older LTS, append a row.
2. Replace Drift on this uplift, and rebuild the Ways in table of Security
   review from this line's code.
3. Re-read Component tour, Approach, Tests and Customer impact against the code
   as ported; anything the rewrite changed is rewritten, not annotated.
4. Add a Decision for any choice this port made differently.
5. Drop sections that described the prior line's implementation only.
6. Bring it up to this template: add the `aiven_*` frontmatter keys, Summary,
   Decisions, Security review and Retirement if absent, and rename the Lineage column
   "How to find" to "Commits".
7. Remove every link into a file that no longer exists on this line. A link to
   a policy points at its section in `../runbooks/porting-doctrine.md`; content
   that only lived in an uplift file and concerns this patch alone moves into
   the dossier, usually as a Decision.

Keep existing heading anchors as they are, even when you reword a heading —
they are linkable URLs. Patch-specific sections the template does not list,
such as a downstream contract or a known residual, are welcome; place them
where they read best.

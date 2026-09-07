# Skill — dossier template

Create `docs/aiven/patches/<NNN>-<slug>.md`. Write for a **human teammate** who
knows ClickHouse but may not know this Aiven corner — teach briefly, then prove.

Keep conflict novels and hunk diaries out of the dossier (`tmp/` or prior-LTS
git). Stable identity is the **slug**.

Dossiers live under `docs/`, so they follow the repo-wide documentation rules:
a frontmatter block before the first heading, and an explicit `{#kebab-anchor}`
on **every** heading. Keep the anchors stable — they are linkable URLs.

```markdown
---
description: '<one line, no trailing period>'
sidebarTitle: '<NNN>: <very short label>'
slug: '/aiven/patches/<NNN>-<slug>'
title: 'Patch <NNN>: <short title>'
doc_type: 'reference'
---

# Patch <NNN> — <short title> {#patch-<NNN>-<slug>}

## Lineage {#lineage}

| LTS | How to find | Outcome |
|---|---|---|
| 26.3 | `git log --grep='^patch-port(<NNN>)'` on `v26.3…-aiven` | ported / dropped / … |
| 26.8 | `patch-port(<NNN>):` (staged or landed) | still-needed-and-applies / … |

Optional tip SHA (evidence only): `…`

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

This is not the Background section repeated: Background explains the *problem*,
the tour explains the *terrain*. Keep it to a short paragraph per file.

## Problem {#problem}

The **unpatched** world. What goes wrong for Aiven (or customers) *without*
this patch? Symptom, blast radius, who hits it. Prefer “a service user can
read another tenant's DDL” over “improve security.”

Do not describe the patched behavior, the restore path, or the upgrade delta
here — that is Customer impact. If a sentence starts with “after this patch”
or “the user now sees,” it belongs there.

## Approach {#approach}

What we change and **why that fixes it**. Call out gates/settings (especially
default-off), intentional non-goals, and anything a reviewer might misread as
a bug (e.g. “client TLS still loads; only server certs skip”).

## Concept {#concept}

The one transferable idea behind this patch — what the engineer should still
know six months from now, after forgetting the diff. A ClickHouse mechanism, an
invariant, or a rule of thumb that generalizes past this patch (e.g. “metadata
is reachable through both a `SHOW` interpreter and a `system.*` table”,
“`checkAccess` throws, `isGranted` returns a bool, and scans must not throw”).

State it in a few lines and, where it helps, name the trap it prevents. If the
patch taught you nothing generalizable, say so in one line instead of padding.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-and-applies` | `still-needed-but-rewrite` |
`obsoleted-by-upstream` | `irrelevant-by-removal`

Short bullets only if something moved (symbol rename, new parameter, …).

## Customer impact {#customer-impact}

The **patched** world, plus the upgrade delta. What a tenant, operator, or
control-plane query notices *after* this patch lands. Changelog, not
justification (justification is Problem).

A mechanical check: if you can paste a sentence into Problem without
changing its meaning, delete it here. Customer impact must name an
observable (`ACCESS_DENIED`, empty cell, `404`) or it is Problem restated.

Cover, when they apply:

- Who is affected (role, grant, or endpoint).
- What they see (`ACCESS_DENIED`, empty cell, `404`, a setting to flip).
- How to restore the previous behavior without a rebuild (grant,
  `http_handlers` rule, existing setting).
- What is **new versus the previous Aiven LTS** of this patch, if this is a
  rewrite. A newly gated column or door belongs here even when the policy
  itself did not change.

If nothing user-facing changes, one line: `none — same as <prior LTS>` or
`none — no user-visible surface`. Do not pad.

Example pair (do not copy into a real dossier as filler):

- Problem: “Any user who can see a table can read its full `CREATE`
  statement.”
- Customer impact: “That user now gets `ACCESS_DENIED` on `SHOW CREATE`
  and empty `system.tables.create_table_query`. Restore with `CREATE TABLE`.
  Versus 26.3, `system.databases.engine_full` also goes empty.”

## Tests {#tests}
- Path(s): `aiven_<NNN>_<slug>.*` and/or `test_aiven_<slug>/`
- What the test proves (causation): …
- Evidence: FAIL on parent / PASS with patch (halt report) — or `no_justified` / …
- If TLS/certs: generator script used (no committed PEMs) — see `runbooks/integration.md`

## Rollback {#rollback}

Safe revert? On-disk / ZK state left behind? Setting to disable without rebuild?
```

**Tone:** plain language, short paragraphs, active voice. Explain *concepts*
when they unlock the review; skip filler and checklist theatre.

When importing old fat dossiers: keep background/problem/approach + lineage +
customer impact; leave multi-page conflict diaries on the previous branch.

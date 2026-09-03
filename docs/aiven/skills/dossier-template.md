# Skill — dossier template

Create `docs/aiven/patches/<NNN>-<slug>.md`. Write for a **human teammate** who
knows ClickHouse but may not know this Aiven corner — teach briefly, then prove.

Keep conflict novels and hunk diaries out of the dossier (`tmp/` or prior-LTS
git). Stable identity is the **slug**.

```markdown
# Patch <NNN> — <short title>

## Lineage

| LTS | How to find | Outcome |
|---|---|---|
| 26.3 | `git log --grep='^patch-port(<NNN>)'` on `v26.3…-aiven` | ported / dropped / … |
| 26.8 | `patch-port(<NNN>):` (staged or landed) | still-needed-and-applies / … |

Optional tip SHA (evidence only): `…`

## Background (ClickHouse)

A few sentences of **necessary** context — not a textbook chapter. Name the
subsystem (`Access`, `CertificateReloader`, `RefreshTask`, S3 disk, …) and the
invariant or API surface the reader must hold in mind. Define jargon once
(e.g. what `<ca_path>` does, what `ON CLUSTER` skips).

## Component tour

Orient the reader in the code they are about to read. For each file the patch
touches, say **which layer it belongs to** and **how the layers relate** —
interpreter vs analyzer vs storage vs system table, foreground vs background,
user-facing vs server-internal. Name the entry point (`executeImpl`,
`fillData`, `generate`, …) so the reader can find it without grepping.

This is not the Background section repeated: Background explains the *problem*,
the tour explains the *terrain*. Keep it to a short paragraph per file.

## Problem

What goes wrong for Aiven (or customers) without this patch? Be concrete:
symptom, blast radius, who hits it. Prefer “managed fleet cannot …” over
“improve security.”

## Approach

What we change and **why that fixes it**. Call out gates/settings (especially
default-off), intentional non-goals, and anything a reviewer might misread as
a bug (e.g. “client TLS still loads; only server certs skip”).

## Concept

The one transferable idea behind this patch — what the engineer should still
know six months from now, after forgetting the diff. A ClickHouse mechanism, an
invariant, or a rule of thumb that generalizes past this patch (e.g. “metadata
is reachable through both a `SHOW` interpreter and a `system.*` table”,
“`checkAccess` throws, `isGranted` returns a bool, and scans must not throw”).

State it in a few lines and, where it helps, name the trap it prevents. If the
patch taught you nothing generalizable, say so in one line instead of padding.

## Drift on this uplift

Conclusion: `still-needed-and-applies` | `still-needed-but-rewrite` |
`obsoleted-by-upstream` | `irrelevant-by-removal`

Short bullets only if something moved (symbol rename, new parameter, …).

## Tests
- Path(s): `aiven_<NNN>_<slug>.*` and/or `test_aiven_<slug>/`
- What the test proves (causation): …
- Evidence: FAIL on parent / PASS with patch (halt report) — or `no_justified` / …
- If TLS/certs: generator script used (no committed PEMs) — see `runbooks/integration.md`

## Rollback

Safe revert? On-disk / ZK state left behind? Setting to disable without rebuild?
```

**Tone:** plain language, short paragraphs, active voice. Explain *concepts*
when they unlock the review; skip filler and checklist theatre.

When importing old fat dossiers: keep background/problem/approach + lineage;
leave multi-page conflict diaries on the previous branch.

---
description: 'Commit categories, subjects and message bodies for the Aiven ClickHouse fork'
sidebarTitle: 'Commit hygiene'
slug: '/aiven/runbooks/commit-hygiene'
title: 'Runbook — commit hygiene'
doc_type: 'guide'
---

# Runbook — commit hygiene {#runbook-commit-hygiene}

## Categories (do not mix in one commit) {#categories}

| Cat | Name | Contents |
|---|---|---|
| **A** | Framework | `docs/aiven/` outside `patches/<NNN>-*.md`, plus `.cursor/**` |
| **CI** | Pipeline | `.buildkite/**` and CI-only tweaks |
| **P** | Patch | One identity: `src/**` + `tests/**` + exactly one `docs/aiven/patches/<NNN>-<slug>.md` |

A **P** commit never touches another path under `docs/aiven/`. That is what
makes it cherry-pickable: every file in it can exist on the next line. See
[commit shape](../AGENTS.md#commit-shape).

## Subjects {#subjects}

Mandatory for **P**:

- `patch-port(<NNN>): <verbatim source-commit subject>` — porting from a prior line.
- `patch-new(N<nn>): <short human summary>` — new Aiven change on this line.
- `patch-fix(<NNN>): <short summary>` — a fix to a patch already landed on this
  line; updates its dossier in the same commit.
- `patch-drop(<NNN>): <short reason>` — no code; stages only the dossier, with
  the drop recorded in its Lineage table.

A port that absorbs other identities uses the surviving identity in the subject
and in the dossier; the absorbed ones are named in the Lineage table.
Identity-less fixes from the prior line are folded into their parent's port,
never carried as separate commits (`porting-doctrine.md`).

For **A** and **CI**, normal prose subjects: `docs(aiven): …`, `ci: …`.

## Commit message body (human, not robotic) {#commit-message-body}

Write as if briefing a teammate. Prefer:

1. **Why** this change exists (problem in one or two sentences).
2. **What** we did at a useful altitude (not a file list dump).
3. **How to verify** (test name, or "Buildkite lane X").
4. Trailer lines as needed: `Original author: …`, links to CI reports.

Avoid empty process speak ("per runbook", "staged_ok", "halt report attached").
Put those in the agent halt report; the git message should read like ClickHouse
project history.

Example shape for a port:

```text
patch-port(059): Skip loading unused server certificates

When no secure port is configured, CertificateReloader still tried to read
server cert paths and could fail the process on stale managed configs. Guard
tryLoadImpl so server certs load only if tcp_port_secure or https_port is set;
client TLS is unchanged.

Test: test_aiven_lazy_certificates (certs minted by certs/generate_certs.sh).

Original author: …
```

## Command literals first {#command-literals-first}

The worker writes the message as a file, filled with the prose above, and the
parent commits from it, so the message is reviewed before it is used:

```bash
git commit -F tmp/uplift-<ver>/commit-patch-NNN.txt
```

## Who does what {#who-does-what}

- Worker: stage only (`cherry-pick --no-commit`, `git add`), write the message file.
- Parent agent: check the definition of done, then `git commit -F`.
- Human: review the branch, push, PR, fast-forward `-aiven`.

## End-state shape {#end-state-shape}

Before this line becomes the next LTS's cherry-pick source, it should read as:

1. One `ci:` commit and one `docs(aiven):` bootstrap commit
2. One **P** commit per surviving identity, each carrying its dossier
3. Any later `docs(aiven):` commits for doctrine learned during the uplift

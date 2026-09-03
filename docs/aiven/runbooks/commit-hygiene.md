# Runbook — commit hygiene

## Categories (do not mix in one commit)

| Cat | Name | Contents |
|---|---|---|
| **A** | Bootstrap | `docs/aiven/{AGENTS,README,schema,runbooks,skills}/**`, `.cursor/**`, `.buildkite/**` |
| **B** | Per-uplift | Only `docs/aiven/uplifts/<this-version>/**` |
| **C** | Patch port | `src/**` + `tests/**` for one patch; dossier update may share the commit **or** fold into a later docs bundle |
| **D** | Net-new | New Aiven feature on this line — `patch-new(N<nn>):` |

Keep **CI (`.buildkite/**`)** and **docs/aiven + `.cursor/**`** in separate A
commits when both are dirty — easier to squash the CI hunk into an earlier
Buildkite commit without dragging orchestration docs along.

## Subjects (mandatory for C/D)

- `patch-port(<NNN>): <verbatim source-commit subject>` when porting — greppable.
- `patch-drop(<NNN>): <short reason>`
- `patch-new(N<nn>): <short human summary>`

For **A/B** docs/CI: normal prose subjects are fine, e.g.
`ci: …`, `docs(aiven): …`, `bootstrap: …`.

## Commit message body (human, not robotic)

Write as if briefing a teammate. Prefer:

1. **Why** this change exists (problem in one or two sentences).  
2. **What** we did at a useful altitude (not a file list dump).  
3. **How to verify** (test name, or “Buildkite lane X”).  
4. Trailer lines as needed: `Original author: …`, links to CI reports.

Avoid empty process speak (“per runbook”, “staged_ok”, “halt report attached”).
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

## Command literals first

Still hand the human a ready file — but fill it with the prose above:

```bash
git commit -F tmp/uplift-26.8/commit-patch-NNN.txt
```

## Follow-up docs (A / B)

Reusable lessons → **A**: `docs(aiven): …`.  
Uplift pins/plan only → **B**: `docs(aiven/<ver>): …`.  
26.3 follow-ups are already folded into this tree; new A commits only for **new**
26.8 lessons.

## Agent vs human

- Agent: stage only (`cherry-pick --no-commit`, `git add`), propose message file.
- Human: `git commit`, push, PR, fast-forward `-aiven`.

## End-state shape (optional reslice)

Before this line becomes the next LTS’s cherry-pick source:

1. Pure **A** commits (CI vs docs separable if useful)  
2. Pure code commits per patch  
3. One consolidated docs commit (dossiers + `uplifts/<ver>/`)

## Mixing rule

Never combine A with B/C/D in one commit. During active porting, C may include
its dossier update.

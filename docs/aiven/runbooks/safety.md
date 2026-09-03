# Runbook — safety

## Git and hooks

- Agents never `git push` or `git commit`. Humans do.
- On hook **deny**: STOP and report; do not retry with a workaround.
- After editing `.cursor/hooks/*.sh`, run `chmod +x .cursor/hooks/*.sh`. A
  non-executable **failClosed** hook returns no output and blocks **all** Shell
  use in the agent.
- Deny hooks are **failClosed: true** and must always print a JSON permission
  line. Verify with **static stdin only** — never exercise hooks by running real
  `git commit` / `git push` / `git rebase`:

```bash
chmod +x .cursor/hooks/*.sh
echo '{"command":"git status"}' | bash .cursor/hooks/deny-agent-commits.sh
# expect: {"permission":"allow"}

echo '{"command":"git commit -m x"}' | bash .cursor/hooks/deny-agent-commits.sh
# expect: permission deny

echo '{"command":"git push origin HEAD"}' | bash .cursor/hooks/deny-irreversible-git.sh
# expect: permission deny
```

## Secrets and managed-fleet hygiene

Never put real fleet material in tests, dossiers, halt reports, commit messages,
or `docs/aiven/`:

- Production or staging hostnames, service URIs, customer identifiers
- API tokens, passwords, private keys, cloud creds (`ci/local.env`, AWS keys, …)
- Copied PEMs — TLS fixtures **mint** certs via generators (`integration.md`)

Use synthetic names (`minio1`, `aiven-test-ca`, `user_a` / `user_b`). If a log
snippet might contain a secret, redact before pasting into `tmp/` reports that
humans might commit by mistake.

## Other

- Temp artefacts under `tmp/`, not `/tmp`.
- Do not touch upstream-owned paths (see `AGENTS.md`); hooks deny Writes there.

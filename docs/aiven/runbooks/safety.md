---
description: 'Git, hook and secrets rules for agents working on the Aiven fork'
sidebarTitle: 'Safety'
slug: '/aiven/runbooks/safety'
title: 'Runbook — safety'
doc_type: 'guide'
---

# Runbook — safety {#runbook-safety}

## Git and hooks {#git-and-hooks}

- Agents commit; agents never `git push` or `git rebase`. Humans push.
- On hook **deny**: STOP and report; do not retry with a workaround.
- One hook, `deny-irreversible-git.sh`, denies push, force-push and rebase,
  including through git's global options (`git -C <dir> push`). It refuses
  everything when `jq` is missing rather than letting the command through
  uninspected.
- **Make a hook executable before `hooks.json` references it.** Cursor loads
  `hooks.json` as soon as it is written, and a **failClosed** hook that cannot
  execute blocks **all** Shell use — including the `chmod` that would fix it.
  Write the script, `chmod +x` it, test it, and only then add it to
  `hooks.json`. This happened when the hooks were carried onto 26.8.34.13.
- Deny hooks are **failClosed: true** and must always print a JSON permission
  line. Verify with **static stdin only** — never exercise hooks by running a
  real push or rebase:

```bash
chmod +x .cursor/hooks/*.sh
bash .cursor/hooks/test-deny-irreversible-git.sh   # exit status = number of failures
```

  The cases live in that script on purpose. The hook inspects the whole command
  line, so a test typed inline that contains the push or rebase subcommand —
  even as a quoted string — is itself denied. The same goes for any command
  that merely mentions one, such as a `--grep` for it; put such text in a file.

## Secrets and managed-fleet hygiene {#secrets-and-managed-fleet-hygiene}

Never put real fleet material in tests, dossiers, halt reports, commit messages,
or `docs/aiven/`:

- Production or staging hostnames, service URIs, customer identifiers
- API tokens, passwords, private keys, cloud creds (`ci/local.env`, AWS keys, …)
- Copied PEMs — TLS fixtures **mint** certs via generators (`integration.md`)

Use synthetic names (`minio1`, `aiven-test-ca`, `user_a` / `user_b`). If a log
snippet might contain a secret, redact before pasting into `tmp/` reports that
humans might commit by mistake.

## Other {#other}

- Temp artefacts under `tmp/`, not `/tmp`.
- Do not touch upstream-owned paths (see `AGENTS.md`); hooks deny Writes there.

# Aiven fork — agent invariants

> Auto-loaded when working under `docs/aiven/`. Procedures live in `runbooks/`
> and `skills/`; this file is law only.

## Orientation

Pins for **this** LTS (source tag, work branch, upstream peel) live in
`docs/aiven/uplifts/<version>/00-introduction.md`. Do not hardcode version
strings elsewhere.

You work only on `*-aiven-dev`. Release-line `*-aiven` is human fast-forward
after sign-off.

## Branch

- Act only when HEAD is on a `*-aiven-dev` branch.
- Never write on `master` / `main` / `v*-aiven`.
- Do not create branches (`git checkout -b` / `git switch -c`) or `git push`.
  Parallel work uses a separate **worktree**, not a feature branch.
- Wrong branch → STOP and report.

## Never-touch (upstream-owned)

Do not Write/Edit:

- `.claude/**`
- root `AGENTS.md` (this `docs/aiven/AGENTS.md` is fine to edit when intentionally
  changing Aiven law)
- `CONTRIBUTING.md`
- `.github/workflows/**`
- `contrib/**`

`.gitmodules` is editable, but if a patch redirects a submodule to an **Aiven
fork**, STOP with `external_dependency` — see `runbooks/submodule-forks.md`.
The agent never prepares fork repos.

## Secrets

No real Aiven/cloud credentials, private keys, or production endpoints in
tests, dossiers, halt reports, or commit messages. Synthetic fixtures only;
TLS via generators (`runbooks/integration.md`, `runbooks/safety.md`).

## Git

- No rebase, amend, force-push, or push. Add new commits (human does).
- Agent **does not commit**. Stage with `git cherry-pick --no-commit` and
  `git add`. Success = verified and staged; propose the commit message.
- Hooks enforce this; a deny means STOP, never work around.

## Tests

Every ported patch MUST have either:

**(a)** A new test that **FAIL**s on the parent tree and **PASS**es after the
patch (prefer `tests/queries/0_stateless/aiven_<NNN>_<slug>.*`; integration only when
cluster/restart/external services are required — `tests/integration/test_aiven_<slug>/`).
Evidence pair is mandatory in the halt report.

**(b)** Documented `no_justified` naming an existing upstream test (build-only /
config rename / already covered).

Assert Aiven-specific message substrings, not only shared error codes.

Default-behavior changes with broad blast radius → escalate `policy_call`;
gate behind a default-off setting (prefer `aiven_` prefix for **new** settings;
never rename a shipped setting).

## Exit contract

Every worker exit (success or failure) uses
`docs/aiven/schema/halt-and-escalate.md`.

## Navigation

| Need | Where |
|---|---|
| Map | `docs/aiven/README.md` |
| Day-to-day loop | `docs/aiven/runbooks/workflow.md` |
| New LTS carry | `docs/aiven/runbooks/bootstrap.md` |
| Commit categories | `docs/aiven/runbooks/commit-hygiene.md` |
| Build/test commands | `docs/aiven/runbooks/build-and-test.md` |
| Integration local loop | `docs/aiven/runbooks/integration.md` |
| Dispatch checklist | `docs/aiven/skills/dispatch.md` |
| Safety / secrets / hook verify | `docs/aiven/runbooks/safety.md` |
| Dossiers | `docs/aiven/patches/` |
| Repo build/CI rules | root `AGENTS.md` |

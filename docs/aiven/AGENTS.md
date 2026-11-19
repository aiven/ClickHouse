# Aiven fork — agent invariants {#aiven-fork-agent-invariants}

> Auto-loaded when working under `docs/aiven/`. Procedures live in `runbooks/`
> and `skills/`; this file is law only.

## Orientation {#orientation}

`docs/aiven/` holds no version-specific file. Pins for the current uplift
(source tip, work branch, upstream base, prior `-dev` line) and all planning
state — which patches exist, their grouping, order, status and owner — live in
the uplift tracker in Notion. The parent passes the pins into every worker
prompt. Do not write the current uplift's tags, branches or SHAs into
`runbooks/`, `skills/`, `schema/` or this file; use placeholders. Naming a past
line as the evidence behind a rule is history, not a pin, and is fine.

You work only on `*-aiven-dev`. Release-line `*-aiven` is human fast-forward
after sign-off.

## Branch {#branch}

- Act only when HEAD is on a `*-aiven-dev` branch.
- Never write on `master` / `main` / `v*-aiven`.
- Do not create branches (`git checkout -b` / `git switch -c`) or `git push`.
  Parallel work uses a separate **worktree**, not a feature branch.
- Wrong branch → STOP and report.

## Never-touch (upstream-owned) {#never-touch}

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

## Secrets {#secrets}

No real Aiven/cloud credentials, private keys, or production endpoints in
tests, dossiers, halt reports, or commit messages. Synthetic fixtures only;
TLS via generators (`runbooks/integration.md`, `runbooks/safety.md`).

## Git {#git}

- No rebase, amend, force-push, or push. Fix mistakes with a new commit.
- **Humans push.** Everything on the branch is reviewed before it leaves the
  machine, so commits are cheap to make and the push is the gate.
- **Workers stage, the parent commits.** A worker stages with
  `git cherry-pick --no-commit` and `git add` and returns a halt report with a
  commit message file. The parent commits with `git commit -F <file>` once the
  [definition of done](runbooks/workflow.md#definition-of-done) holds.
- A hook denies push and rebase; a deny means STOP, never work around.

## Commit shape {#commit-shape}

A commit whose subject starts with `patch-port(`, `patch-new(`, `patch-fix(` or
`patch-drop(` stages **exactly one** `docs/aiven/patches/*.md` — the dossier of
that identity — and no other path under `docs/aiven/`. The code, its tests and
its explanation travel together, so cherry-picking the commit onto the next
line carries all three.

Changes to `runbooks/`, `skills/`, `schema/`, `README.md` or this file are
separate `docs(aiven):` commits. Never mix them into a patch commit.

## Dossiers {#dossiers}

- Path is `docs/aiven/patches/<NNN>-<slug>.md`, with no version anywhere in
  the path or the frontmatter slug.
- A dossier enters a line inside its patch's first commit there. When a prior
  line has one, bring it in with `git checkout <prior-dev> -- <dossier>` while
  staging the port, then make it true for the code as ported.
- Dossiers are public. Never name the private control-plane repository, its
  modules or its test paths; restate behaviour instead
  (`skills/dossier-template.md`).
- Outside `patches/`, cite a patch by identity in code font (`022`), not by a
  link: a dossier exists on a line only once its patch has landed there.

## Tests {#tests}

Every ported patch MUST have either:

**(a)** A new test that **FAIL**s on the parent tree and **PASS**es after the
patch (prefer `tests/queries/0_stateless/aiven_<NNN>_<slug>.*`; integration only when
cluster/restart/external services are required — `tests/integration/test_aiven_<slug>/`).
Evidence pair is mandatory in the halt report.

**(b)** Documented `no_justified` naming an existing upstream test (build-only /
config rename / already covered).

Assert Aiven-specific message substrings, not only shared error codes.

Default-behavior changes with broad blast radius → escalate `policy_call`;
gate behind a default-off setting (`aiven_` prefix for every fork-local setting —
see `runbooks/porting-doctrine.md`; never keep a dual-name alias).

## Exit contract {#exit-contract}

Every worker exit (success or failure) uses
`docs/aiven/schema/halt-and-escalate.md`.

## Navigation {#navigation}

| Need | Where |
|---|---|
| Map | `docs/aiven/README.md` |
| Planning state and pins | uplift tracker (Notion) |
| Day-to-day loop | `docs/aiven/runbooks/workflow.md` |
| New LTS carry | `docs/aiven/runbooks/bootstrap.md` |
| Rules for every port | `docs/aiven/runbooks/porting-doctrine.md` |
| Commit categories | `docs/aiven/runbooks/commit-hygiene.md` |
| Build/test commands | `docs/aiven/runbooks/build-and-test.md` |
| Integration local loop | `docs/aiven/runbooks/integration.md` |
| Dispatch checklist | `docs/aiven/skills/dispatch.md` |
| Safety / secrets / hook verify | `docs/aiven/runbooks/safety.md` |
| Dossiers | `docs/aiven/patches/` |
| Repo build/CI rules | root `AGENTS.md` |

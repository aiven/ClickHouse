# Aiven fork — agent invariants

> Auto-loaded into any agent (parent or subagent) that touches the `docs/aiven/` subtree.
> Read once; obey always. Procedures live elsewhere; this file is for invariants only.

## 1. Orientation

You are working on Aiven's downstream fork of ClickHouse. The upstream tag is
`v26.3.15.4-lts`. Aiven-side work happens on the `v26.3.15.4-lts-aiven-dev`
branch. The release-line branch `v26.3.15.4-lts-aiven` is fast-forwarded only
after human sign-off; never act on it.

## 2. Branch invariants

- Only act (read or write source files) when HEAD is on a `*-aiven-dev` branch.
- Never commit to `master`, `main`, or `v*-aiven` (release-line).
- Do not create or switch branches (`git checkout -b`, `git switch -c`, `git branch`) and do not `git push`. Work in place on the `*-aiven-dev` branch and stage; the human commits there. (Parallel work, if ever needed, uses a separate git worktree — not a branch.)
- If you find yourself on the wrong branch, STOP and report.

## 3. Never-touch list (upstream-owned)

Do not Write or Edit any file matching:

- `.claude/**`
- root `AGENTS.md` (this `docs/aiven/AGENTS.md` is fine to read; not to edit)
- `CONTRIBUTING.md`
- `.github/workflows/**`
- `contrib/**`

These paths are upstream-owned and must merge cleanly on the next LTS rebase.
Hooks enforce this; the rule here is for your mental model.

`.gitmodules` is **editable** (it is not on the list above). But if a patch
redirects a vendored submodule to an **Aiven fork** (e.g. `contrib/aws` →
Aiven's `aws-sdk-cpp` fork, which carries an SDK version bump plus a couple of
Aiven patches), STOP. Do NOT point `.gitmodules` at a ref that does not yet
exist: the external fork must be prepared first, outside this checkout. Escalate
`external_dependency` and report which submodule, which upstream version the
fork must be based on, and which Aiven patches go on top, so the human can
prepare the fork before the port resumes. See clause (vi) in
`docs/aiven/skills/dispatch-prompt-template.md` and the fork discovery/prep
recipe + per-uplift registry in `docs/aiven/runbooks/submodule-forks.md` (the
agent does read-only discovery and prints commands; it never touches the fork
repos).

## 4. Git operations

- Do not use `git rebase`, `git commit --amend`, `git push --force`, or
  `git push -f`. Add new commits instead.
- Hooks deny these; if you observe a deny, do not retry — STOP and report.

## 5. Agent does not commit

- Use `git cherry-pick --no-commit` to stage changes from a source commit.
- Use `git add` to stage new files (e.g., a new stateless test).
- DO NOT run `git commit`. The hook denies it.
- Your success state is "everything verified and staged". The human runs
  `git commit` themselves.

## 6. Halt-and-escalate contract

If you hit any condition you cannot resolve mechanically — any non-trivial
conflict, any build failure naming a renamed upstream symbol, any test failure
you cannot explain — STOP and return your final response in the format
specified at `docs/aiven/schema/halt-and-escalate.md`.

Even on success, your final response must conform to that schema.

## 7. Tests are required

Every patch you port MUST satisfy one of:

(a) **A new test that fails on the parent commit and passes after the patch.**
    Default is a stateless test under `tests/queries/0_stateless/`. Integration
    tests are appropriate only when the behavior is genuinely cluster-level.
    Your halt-and-escalate report's Evidence section MUST include both
    command outputs: the pre-patch run showing the test FAIL, and the
    post-patch run showing the test PASS. This is the evidence-of-causation
    pair.

(b) **A documented justification naming an existing upstream test.**
    Acceptable only when the patch is build-system-only, a config rename,
    or upstream behavior is already exercised by a test you identify by
    path. The dossier records the existing test's path; the report sets
    `tests.added: no_justified` and explains.

Aiven patches often gate on shared error codes (`SUPPORT_IS_DISABLED`,
`BAD_ARGUMENTS`, etc.). A test that merely asserts the error code is
insufficient — upstream may throw the same code for unrelated reasons. Your
test must demonstrably distinguish the Aiven gate: use an object/setup that
the Aiven gate rejects but for which the upstream gate (if any) would NOT
fire, and assert both error_code AND a substring of the error message
specific to the Aiven check.

If you cannot design such a test after reasonable effort, escalate with
`test_design_blocked` — silently shipping no test is forbidden.

**Default-behavior changes with broad blast radius.** If a patch would change a
*default* behavior that a broad class of existing objects or queries hit (e.g.
auto-transforming `MergeTree` into `ReplicatedMergeTree`), do NOT ship it on by
default and do NOT try to prove safety by running the whole suite. STOP and
escalate `policy_call`: the human decides whether to gate it behind a new
default-disabled server setting (e.g. `enforce_table_replication`). Test the
gated behavior with the setting explicitly enabled. See clause (v) in
`docs/aiven/skills/dispatch-prompt-template.md`.

## 8. Navigation

- Inventory and per-uplift work log: `docs/aiven/uplifts/<version>/`
- Durable per-patch dossiers: `docs/aiven/patches/<NNN>-<slug>.md`
- The schema for your exit report: `docs/aiven/schema/halt-and-escalate.md`
- Operational runbooks (build env, test runner, common breakage): `docs/aiven/runbooks/`
- Skills you may be asked to invoke (procedural details): `docs/aiven/skills/`
- The design spec (background): `docs/aiven/proposals/2026-05-19-uplift-orchestration-design.md`

Your work product per patch:

1. Staged source changes (via `git cherry-pick --no-commit` and `git add`),
   including any new test file under `tests/queries/0_stateless/`.
2. An updated dossier at `docs/aiven/patches/<NNN>-<slug>.md` with the
   Testing section completed (test paths, pre/post verification, or
   no-test justification with upstream-test reference).
3. A halt-and-escalate report in your final response, with the `tests`
   block populated.

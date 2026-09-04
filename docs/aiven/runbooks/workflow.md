# Runbook — workflow (parent + worker)

Day-to-day loop for an LTS uplift. Keep this file short; deep commands live in
other runbooks.

## Parent loop

1. Confirm HEAD is `*-aiven-dev` and pins in `uplifts/<ver>/00-introduction.md`.
2. Pick next work from `uplifts/<ver>/execution-plan.md` (subsystem groups).
   If the plan is empty, derive order with `runbooks/execution-sequencing.md`.
3. For each patch (or tight dependency chain):
   - Open the dossier under `patches/` (create from `skills/dossier-template.md`
     if missing).
   - Build a worker prompt from `skills/dispatch.md` (fill placeholders).
   - Dispatch **one** worker; do not parallelize conflicting file sets.
4. Review the halt report against **Definition of done** below. If `staged_ok`
   and Done holds, human runs the commit file from `tmp/…`. If escalate or
   budget exhausted, resolve policy/fork/test design, then re-dispatch.
5. Keep full worker text under `tmp/uplift-<ver>/reports/` (not committed).
   Progress is `git log --grep='^patch-port('` + dossier lineage — **no**
   `log.md`.

## Definition of done (one patch)

A port is Done for human commit only when **all** hold:

| # | Criterion |
|---|---|
| 1 | Drift conclusion chosen; dossier has Background / Component tour / Problem / Approach / Concept / Customer impact / Tests |
| 2 | Intended paths only are staged (`git status` / `git diff --cached --stat`) |
| 3 | Build of affected targets succeeded (log under build dir or `tmp/`) |
| 4 | Tests: FAIL/PASS evidence pair **or** a justified `tests.added` enum value |
| 5 | Halt report is `staged_ok` (or explicit `dropped` with dossier updated) |
| 6 | `tmp/…/commit-….txt` exists — human-readable why/what/verify (not jargon) |
| 7 | No secrets in staged files, dossier, halt report, or commit body (see `safety.md`) |

Local green on the named test is enough to commit; full Buildkite need not pass
on every single port. High-blast areas (Access, TLS, auth) still want an extra
human look before merge to `-aiven`.

## Worker stop budget (do not grind)

Escalate with a clear reason instead of burning the context window:

| Limit | Action |
|---|---|
| **2** distinct unexpected conflict regions (after the expected site) | `escalate` / `conflict` |
| **2** full rebuild/fix-test redesign cycles without a new hypothesis | `escalate` / `test_design_blocked` or `policy_call` |
| Build or test wall time clearly runaway vs similar ports | stop, log path, escalate |
| Need a default-behavior / fleet-wide product call | `policy_call` immediately |
| Submodule fork tip missing | `external_dependency` immediately |

Parent may grant one more cycle in policy calls; workers must not self-extend
past these caps silently.

## Worker loop (summary)

1. Drift analysis → conclusion enum.
2. Stage: `git cherry-pick --no-commit <source>` or manual port.
3. Test (FAIL/PASS pair) or justified skip.
4. Build affected targets; log to build dir / `tmp/`.
5. Update dossier (teaching tone — see template).
6. Halt report + commit message file. **Do not commit or push.**

## Discovery without inventory

```bash
git log --oneline --grep='^patch-port('
git log --oneline --grep='^patch-new('
git log --oneline --grep='^patch-drop('

git log --oneline --no-merges ${FROM_UPSTREAM}..${FROM_AIVEN}

ls docs/aiven/patches/*protected-users*
```

## Batching

- Group by subsystem (`StorageKafka`, object storage, Access, Refreshable MV, …).
- Warm up on small / clean ports before hard rewrites.
- Hold blast-radius default changes for explicit `policy_call`.
- Replication-core / high-churn files last when possible.

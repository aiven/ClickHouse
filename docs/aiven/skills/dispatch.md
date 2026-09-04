# Skill — dispatch a patch worker

Parent fills placeholders, then pastes this checklist into the worker prompt.
Keep the prompt short; link runbooks instead of inlining them.

## Worker prompt (template)

```text
You are a patch worker for the Aiven LTS uplift.

Read and obey:
- docs/aiven/AGENTS.md
- docs/aiven/uplifts/<<VER>>/00-introduction.md
- docs/aiven/skills/dispatch.md (this checklist)
- docs/aiven/schema/halt-and-escalate.md

Patch: <<NNN>> (<<SLUG>>)
Source: <<SOURCE_REF>>  (prefer commit subject; SHA optional)
Dossier: docs/aiven/patches/<<NNN>>-<<SLUG>>.md
Parent policy calls: <<PARENT_POLICY_CALLS or "none">>
Test naming: tests/queries/0_stateless/aiven_<<NNN>>_<<SLUG>>.* 
  or tests/integration/test_aiven_<<SLUG>>/

Do NOT commit or create branches. Stage only. Final response = halt report
with a ready-to-run commit message under tmp/.
```

## Checklist (worker)

### 0. Preflight

- [ ] HEAD is `*-aiven-dev` (no feature branch)
- [ ] Dossier exists (create from `dossier-template.md` if needed)
- [ ] Note touched files from `git show --stat <<SOURCE_REF>>`

### 1. Drift (mandatory before apply)

- [ ] Grep key identifiers on current HEAD
- [ ] `git log <<FROM_UPSTREAM>>..HEAD -- <touched files>` — summarize
- [ ] Search for upstream equivalent fix
- [ ] Conclusion: `still-needed-and-applies` | `still-needed-but-rewrite` |
      `obsoleted-by-upstream` | `irrelevant-by-removal`

If drop → update dossier lineage, propose `patch-drop`, halt (no code).

### 2. Stage

- [ ] `git cherry-pick --no-commit <<SOURCE_REF>>` **or** manual port
- [ ] Resolve conflicts minimally; record non-trivial rewrites in halt report
- [ ] Rename shipped 26.3 `9NNN_*` tests to `aiven_<NNN>_<slug>` if present
- [ ] Parent-instructed cleanups only if listed in policy calls

### 3. Review

- [ ] Skim `skills/cpp-review-checklist.md` — note real hits in halt report

### 4. Build

- [ ] Toolchain from `runbooks/build-and-test.md`
- [ ] Incremental `ninja -C <build> clickhouse` (log to file; summarize)

### 5. Test

- [ ] Design per `runbooks/testing.md` (+ `integration.md` if needed)
- [ ] TLS fixtures: cert **generator** + gitignore — never commit PEMs
- [ ] FAIL on parent / PASS with patch **or** justified `tests.added` enum
- [ ] Respect **stop budget** in `runbooks/workflow.md` — escalate, do not grind

### 6. Dossier + halt

- [ ] Dossier: background → component tour → problem → approach → concept →
      drift → customer impact → tests (human tone). Fill the teaching
      sections from the code you read, not from the source commit message.
      Customer impact is the operator changelog, including any new door
      versus the previous LTS.
- [ ] Done checklist in `workflow.md` mentally checked
- [ ] Write `tmp/…/commit-patch-NNN.txt` — readable why/what/verify
- [ ] No secrets in any artefact (`safety.md`)
- [ ] Halt report; `git status` shows intended staged set only
- [ ] **Do not** `git commit` or `git push`

## Parent preflight (before dispatch)

| Clause | Check |
|---|---|
| (i) Hot files | Do not parallelize workers on the same high-churn files |
| (ii) Context | One subsystem per worker prompt |
| (iii) Symbols | Spot-check identifiers still exist or note rewrite |
| (iv) Reachability | Behavior still triggerable; else drop or `no_trigger_on_current_lts` |
| (v) Blast radius | Default-behavior → `policy_call` before dispatch |
| (vi) Submodule fork | Fork tip ready, or bake SHA into policy calls, else escalate |

## Source-author note

Preserve authorship when cherry-picking. For manual ports, put
`Original author: <email>` in the proposed commit body.

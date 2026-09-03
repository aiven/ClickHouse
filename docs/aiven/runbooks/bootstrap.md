# Runbook — bootstrap a new LTS

Carry reusable Aiven infrastructure onto a fresh upstream peel. Patch ports are
**not** part of bootstrap.

## Category A paths (cherry-pick / recreate)

| Path | Notes |
|---|---|
| `docs/aiven/AGENTS.md` | Law |
| `docs/aiven/README.md` | Map |
| `docs/aiven/schema/**` | Contracts |
| `docs/aiven/runbooks/**` | Procedures |
| `docs/aiven/skills/**` | Checklists |
| `.cursor/hooks.json` + `.cursor/hooks/**` | Safety hooks (`chmod +x` after copy) |
| `.buildkite/**` | Aiven CI (pipeline, provision, sccache, …) |

**Not** category A: `docs/aiven/uplifts/**`, `docs/aiven/patches/**`, `src/**`,
`tests/**`, `contrib/**`, `proposals/**`, `plans/**`.

## Steps

1. Create `v<VER>-lts-upstream` from the **peel** of the frozen tag, then
   `v<VER>-lts-aiven` and `v<VER>-lts-aiven-dev` (human).
2. On `-aiven-dev`, land category A. Prefer **separate commits** when both are
   present so CI can be squashed into an earlier Buildkite commit later:
   - `.buildkite/**` (and related CI-only tweaks)  
   - `docs/aiven/**` + `.cursor/**`
   After adding hooks: `chmod +x .cursor/hooks/*.sh` and verify with static
   stdin only (`runbooks/safety.md`) — never by running real commit/push.
3. Create `docs/aiven/uplifts/<ver>/00-introduction.md` with pins (see stub).
4. Import dossiers in a **separate** commit when ready
   (`docs(aiven): import patch dossiers from <prev>` — optional thinning).
5. Do **not** import `inventory.md` / `reports/` / retrospectives by default.
6. Point Buildkite at the new branch; keep CI green before mass ports.

## Already folded in (26.3 follow-ups → this tree)

Do **not** re-litigate these; they are load-bearing in the lean docs/hooks:

| Lesson (from 25.8→26.3 docs commits) | Where it lives now |
|---|---|
| `patch-port(NNN):` / `patch-drop` / `patch-new` subjects | `commit-hygiene.md` |
| Verbatim source subject + `Original author:` | `commit-hygiene.md`, `dispatch.md` |
| Command-literals-first commit proposals | `commit-hygiene.md`, halt report |
| No feature branches; worktree not branch | `AGENTS.md` |
| Submodule fork = human prep; agent read-only | `submodule-forks.md`, clause (vi) |
| Stateless `aiven_<NNN>_<slug>` + integration `test_aiven_<slug>/` | `testing.md`, Buildkite helpers |
| Evidence FAIL/PASS pair + worktree flip | `build-and-test.md` |
| Schema: `no_justified` / `no_source_change` / `no_trigger_on_current_lts` | `halt-and-escalate.md` |
| Parent preflight (i)–(vi) | `dispatch.md` |
| Policy gate for default-behavior blast radius | `AGENTS.md`, clause (v) |
| Pins only in `uplifts/<ver>/00-introduction.md` | `AGENTS.md`, this file |
| `.buildkite/**` is category A | this file, `commit-hygiene.md` |
| No inventory / no committed `log.md` / reports in `tmp/` | `README.md`, `workflow.md` |
| Definition of done + worker stop budget | `workflow.md` |
| Secrets / hook verify (static stdin only) | `safety.md` |

## New follow-ups only for new lessons

When a 26.8 port teaches something **not** in the table above, land a pure **A**
commit (`docs(aiven): …`). Optional **B** updates only `uplifts/<ver>/`.

After a mid-flight point-release retarget, refresh pins in
`uplifts/<ver>/00-introduction.md` (B) — do not scatter version strings.

## Version pins

Only `uplifts/<ver>/00-introduction.md` names concrete tags/branches/SHAs.
Everywhere else say “current uplift intro” or use placeholders.

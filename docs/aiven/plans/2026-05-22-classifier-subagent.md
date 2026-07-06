# T2: Classifier Subagent — Lightweight Patch Inventory

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` or `superpowers:executing-plans` to execute this plan task-by-task.

**Goal.** Produce a mechanical, judgment-free inventory of all patches between the previous Aiven LTS release-line branch and the previous upstream LTS tag, in the form of a single committable markdown table. This is the input pool from which T3+ picks patches for porting.

**Architecture.** One readonly `explore` subagent dispatch. The dispatch prompt inlines `docs/aiven/AGENTS.md` content verbatim (per amended spec §7) plus the halt-and-escalate schema, plus the mechanical procedure below. Worker returns a halt-and-escalate report whose Evidence section contains the inventory table; human reviews + stages + commits.

**Tech stack.** `git log`, `git format-patch`, `git apply --check`, basic shell.

---

## Policy notes (read before executing)

- **No mutation by the worker.** The classifier is purely observational. We use `git format-patch <sha> --stdout | git apply --check` rather than the spec's literal `git cherry-pick --no-commit -n <sha>` because the latter mutates the working tree (sets `CHERRY_PICK_HEAD`, stages changes). `apply --check` has identical semantics for "does this patch apply cleanly?" with zero state change, which is what an `explore` subagent should be doing. This deviation is intentional and documented here so a future reader sees why we diverged from spec wording.
- **No per-patch dossier creation.** Dossiers (`docs/aiven/patches/<NNN>-<slug>.md`) are born at T3 when each patch is dispatched, not pre-fabricated in bulk. Walking-skeleton discipline (spec §11) — don't create 77 files before we know which ones we'll need first.
- **Scope is the full prior cycle.** All patches between `v25.8.18.1-lts` (upstream LTS tag) and `origin/v25.8.18.1-lts-aiven` (current production aiven LTS line, per Aiven release-management decision 2026-05-22) get classified. T2.1 (2026-05-22T12:24Z) ran against 77 commits; the branch was force-pushed later that day with one additional patch landed, so T2.2 ran against 78 commits. The full mechanical pass is cheap (~78 × format-patch+apply-check, maybe 1–2 minutes wall) and the re-dispatch was a one-line prompt edit (count 77 → 78) — re-runs are routine.
- **Pending merges acknowledged but excluded.** Two unmerged branches contain pending fixes that will land on `v25.8.18.1-lts-aiven`: `khatskevich/mv_race_258` (1 unique commit) and `khatskevich/peerdb_258` (2 unique commits). These are NOT included in T2's source range. The inventory preamble must note their existence so a future reviewer knows the inventory should be regenerated once those merges land. (Several other `khatskevich/*` branches exist on origin — `database_deletion`, `enable_arrowflight`, `flacky_mv`, etc. — but only `mv_race_258` and `peerdb_258` are flagged as pending-merge-into-production per current Aiven status.)
- **Range choice justified.** The source range is `v25.8.18.1-lts..origin/v25.8.18.1-lts-aiven` (78 commits at T2.2), NOT `v26.3.10.62-lts..origin/v25.8.18.1-lts-aiven` (944 commits). The former is "patches Aiven added to 25.8 LTS"; the latter would include ~866 upstream stable-branch backports we don't want to port. Earlier spec wording (§10 step 6 pre-amendment) used the broader range incorrectly; spec amended in same commit as this plan.
- **Agent never commits.** Worker stages no files (the inventory is in the report Evidence section, not yet on disk). Parent (this chat) writes the inventory file from the report content, stages it, proposes the commit to the human.
- **Dispatch prompt is durable.** The prompt template lives in this plan and gets reused for future LTS uplifts (just swap the SHAs). No skill file yet — we'll see if the prompt stabilizes across 2+ uses before extracting.

---

## File map

### Created in this plan

- `docs/aiven/uplifts/26.3/inventory.md` — the inventory table, ~80–90 markdown rows + header. Committed by human.
- `tmp/classifier/dispatch-prompt.md` — scratch artifact: the exact prompt text sent to the subagent (for audit). Not committed.
- `tmp/classifier/report.md` — scratch artifact: the worker's halt-and-escalate report verbatim. Not committed.

### Modified in this plan

- None (no source code, no spec/plan/AGENTS.md edits).

---

## Task 0: Preflight — verify inputs are reachable

**Files:** none (read-only checks).

The classifier needs to enumerate `v25.8.18.1-lts..origin/v25.8.18.1-lts-aiven`. Confirm both refs resolve before dispatch.

- [ ] **Step 1: Verify refs**

```bash
git rev-parse v25.8.18.1-lts || echo "MISSING upstream tag"
git rev-parse origin/v25.8.18.1-lts-aiven || echo "MISSING prior-LTS branch"
git rev-list --count v25.8.18.1-lts..origin/v25.8.18.1-lts-aiven
```

Expected:
- Both `git rev-parse` calls print a 40-char SHA.
- The `rev-list --count` prints an integer between 50 and 100 (we saw 77 at plan-write time on 2026-05-22).

If either ref is missing, STOP and `git fetch origin --tags` first.

- [ ] **Step 2: Spot-check the first and last commits**

```bash
git log --oneline -1 v25.8.18.1-lts..origin/v25.8.18.1-lts-aiven | head -1
git log --oneline -5 origin/v25.8.18.1-lts-aiven | head -5
```

Sanity: subjects should look like real Aiven patches (we saw "Add REGISTER_YTSAURUS directives", "Disable YTsaurus engine", etc. on 2026-05-22).

- [ ] **Step 3: Sanity-check `git apply --check` works on a known patch**

```bash
# First commit in the range — apply against current HEAD which is on 26.3 LTS-dev.
first_sha=$(git rev-list --reverse v25.8.18.1-lts..origin/v25.8.18.1-lts-aiven | head -1)
git format-patch -1 "$first_sha" --stdout | git apply --check 2>&1; echo "exit=$?"
```

This isn't part of the validation invariant — we expect SOME patches to fail apply against the new base. We just want to confirm the toolchain works (non-zero exit with error text is fine; segfault or "command not found" is not).

---

## Task 1: Author and stage the dispatch prompt (scratch)

**Files:** `tmp/classifier/dispatch-prompt.md` (scratch, not committed).

The prompt template lives below. Task 1 just materializes it on disk so the audit trail captures what was actually sent.

- [ ] **Step 1: Render the prompt**

The parent agent should `cat docs/aiven/AGENTS.md` and substitute its content into the marker `{{AGENTS_MD_CONTENT}}` below. The marker exists because we can't write the actual AGENTS.md content inline in a plan file (the file is large; we'd duplicate it and risk drift).

```
mkdir -p tmp/classifier
# After substitution, the full prompt is written to:
# tmp/classifier/dispatch-prompt.md
```

- [ ] **Step 2: Verify prompt size**

```bash
wc -l tmp/classifier/dispatch-prompt.md
```

Expected: between 200 and 300 lines (AGENTS.md ~100 + procedure ~100 + boilerplate).

---

## Dispatch prompt template

The prompt sent to the `explore` subagent. Square-bracket placeholders are filled at dispatch time.

````markdown
You are an Aiven LTS uplift CLASSIFIER subagent. This is a **purely readonly mechanical task**: enumerate commits in a git range, gather metadata per commit, return a markdown table. No file writes, no git mutations, no judgment about porting.

# Aiven invariants (mandatory — read first)

The following is the full content of `docs/aiven/AGENTS.md`. It is included here verbatim because Cursor's nested-AGENTS.md auto-load is read-triggered, not startup-time (per spec §7 amended 2026-05-21). You must obey these invariants throughout this dispatch:

---
{{AGENTS_MD_CONTENT}}
---

# Halt-and-escalate schema (your exit contract)

Your final response MUST follow the schema at `docs/aiven/schema/halt-and-escalate.md`. The constraints relevant to your task:

- `outcome: success` requires non-empty Evidence with the full inventory table.
- `outcome: escalate` requires `escalation_reason != none`.
- Tests block: set `tests.added: no_justified` with `justification: "Classifier subagent produces an inventory table; no source-code changes, hence no test required."` (you are not porting a patch; you are listing them).

# Your specific task

Inventory all commits in the range `v25.8.18.1-lts..origin/v25.8.18.1-lts-aiven` (this is the previous Aiven LTS release-line on top of the corresponding upstream LTS tag). For each commit, produce one row in a markdown table with these columns:

| Column | How to compute | Format |
|---|---|---|
| `NNN` | Sequential 1-based index, sorted by commit date ascending (oldest=001) | `001`, `002`, ..., zero-padded to 3 digits |
| `sha` | Commit SHA, first 10 chars | `1f7bcb8c62` |
| `date` | Commit date | `YYYY-MM-DD` (UTC) |
| `author` | Author email | full email |
| `files` | Number of files changed | integer |
| `loc` | Lines added + lines removed | integer |
| `subject` | Commit subject line | escape any `|` to `\|` |
| `cherry_pick_clean` | `yes` if the patch applies cleanly to the current HEAD, `no` if it would conflict | `yes` / `no` |

## Exact commands per commit

For each SHA in `git rev-list --reverse v25.8.18.1-lts..origin/v25.8.18.1-lts-aiven`:

```bash
sha=<full SHA>

# Metadata in one call.
git log -1 --format='%H%n%ad%n%ae%n%s' --date=short-local "$sha"

# files, loc.
git diff-tree --no-commit-id --numstat -r "$sha" \
  | awk '{added+=$1; removed+=$2; files++} END {print files, added+removed}'

# cherry_pick_clean: readonly probe.
git format-patch -1 "$sha" --stdout | git apply --check 2>&1
# Exit code 0 -> "yes"; non-zero -> "no".
# NEVER run `git cherry-pick` here — `apply --check` is the readonly equivalent.
```

## Output shape

In your Evidence section, render:

```markdown
# 26.3 uplift — patch inventory (T2 classifier output)

Source range: `v25.8.18.1-lts..origin/v25.8.18.1-lts-aiven` at <classification date>.
Classifier exit-toolchain: `git format-patch -1 <sha> --stdout | git apply --check`.

| NNN | sha | date | author | files | loc | subject | cherry_pick_clean |
|-----|-----|------|--------|-------|-----|---------|-------------------|
| 001 | <sha> | <date> | <author> | <files> | <loc> | <subject> | <yes|no> |
| 002 | ... |
| ... |

## Summary
- Total: <N> patches
- cherry_pick_clean=yes: <N1>
- cherry_pick_clean=no:  <N2>
- Authors (top 5 by patch count): <list>
- Date range: <first> to <last>
```

# Constraints (compliance-critical)

1. **You will not run `git cherry-pick` for any reason.** Even with `--no-commit`. Use `git format-patch | git apply --check` exclusively. Cherry-pick is denied by G7 unless `--no-commit`, but even with `--no-commit` it mutates state, which conflicts with your readonly role.
2. **You will not write any file under `docs/aiven/`.** Your output goes only in the halt-and-escalate report's Evidence section. The parent agent writes the inventory file from your report.
3. **You will not commit, stage, or modify git state.** Hooks G1/G2/G7 enforce this; the rule is here for your mental model.
4. **If you encounter a commit whose metadata cannot be extracted (e.g., orphan SHA, malformed commit), set `cherry_pick_clean: error` for that row and continue.** Do not escalate for individual classifier-row errors; only escalate if the entire range fails to enumerate.
5. **Maximum runtime:** target 10 minutes. If you are still running after that, switch to `outcome: escalate` with `escalation_reason: other` and explain in "Proposed next step".

# Success criterion

Your `outcome: success` requires:
- One table row per commit in the range (N rows where N = `git rev-list --count`).
- All eight columns populated for every row.
- A Summary section with the four bullets above.

That is the entire deliverable. Do not analyze, judge, or pre-port. Mechanical only.
````

---

## Task 2: Dispatch the classifier subagent

**Files:** none (dispatch event). The subagent's report is the deliverable.

- [ ] **Step 1: Confirm the dispatch prompt is rendered**

```bash
test -s tmp/classifier/dispatch-prompt.md && echo "prompt ready: $(wc -l < tmp/classifier/dispatch-prompt.md) lines"
```

- [ ] **Step 2: Dispatch**

Parent agent uses the `Task` tool with:
- `subagent_type: explore` (readonly enforces no mutation).
- `prompt`: contents of `tmp/classifier/dispatch-prompt.md`.
- `readonly: true` (explicit; redundant with `explore` type but defensive).
- Short description (≤6 words): "Classify v25.8 → v26.3 patches".

Per spec §8 hooks, `subagentStart` does NOT deny `explore`. `subagentStop` logs the dispatch to `docs/aiven/uplifts/26.3/log.md` automatically.

- [ ] **Step 3: Receive the report**

The worker returns a halt-and-escalate response. Save it verbatim to `tmp/classifier/report.md` for audit.

---

## Task 3: Validate the report

**Files:** none (read-only validation of the report).

- [ ] **Step 1: Schema compliance**

The report must have:
- `outcome: success` (or `escalate` — see Task 5).
- `tests` block with `added: no_justified` and justification as specified in the prompt.
- Non-empty Evidence.

- [ ] **Step 2: Table integrity**

Extract the inventory table from Evidence and verify:
- Row count equals `git rev-list --count v25.8.18.1-lts..origin/v25.8.18.1-lts-aiven`.
- All NNN values are unique and zero-padded.
- All `cherry_pick_clean` values are in {`yes`, `no`, `error`}.
- No row has any column empty.

```bash
# Quick validation sketch (refine when running):
n_in_range=$(git rev-list --count v25.8.18.1-lts..origin/v25.8.18.1-lts-aiven)
n_in_table=$(grep -cE '^\| [0-9]{3} \|' tmp/classifier/report.md || true)
test "$n_in_range" = "$n_in_table" && echo "row count OK ($n_in_range)" || echo "MISMATCH range=$n_in_range table=$n_in_table"
```

If the counts mismatch, STOP. Either the worker truncated output (re-dispatch with smaller batches) or the report wasn't captured fully.

---

## Task 4: Materialize the inventory file

**Files:** `docs/aiven/uplifts/26.3/inventory.md` (new, to be committed).

- [ ] **Step 1: Extract the inventory section from the report**

The parent agent copies the markdown between `# 26.3 uplift — patch inventory` (start) and the end of the Summary section into `docs/aiven/uplifts/26.3/inventory.md`. Prepend a brief preamble:

```markdown
# 26.3 uplift — patch inventory

Mechanical inventory produced by the T2 classifier subagent. One row per
commit in `v25.8.18.1-lts..origin/v25.8.18.1-lts-aiven` at classification
time. No judgment of testability, complexity, or porting priority —
those decisions are made fresh per dispatch in T3+.

`cherry_pick_clean` is `yes` if `git format-patch -1 <sha> --stdout |
git apply --check` exits 0 against the current HEAD on
`v26.3.10.62-lts-aiven-dev` (the readonly equivalent of a cherry-pick
dry-run). `no` means the patch would conflict at apply time. `error`
means metadata extraction failed for that commit (see classifier report
for details).

Classifier dispatch: <ISO timestamp>
Classifier subagent id: <id from log.md>
Source range count: <N>

[table follows]
```

- [ ] **Step 2: Sanity check**

```bash
wc -l docs/aiven/uplifts/26.3/inventory.md
grep -c '^| 0' docs/aiven/uplifts/26.3/inventory.md
```

Expected:
- Total lines: 90–120 (preamble ~15 + header ~3 + ~77 rows + summary ~10).
- Row count grep matches `git rev-list --count`.

- [ ] **Step 3: Stage**

```bash
git add docs/aiven/uplifts/26.3/inventory.md
git status --short
```

Expected: one `A` entry for the inventory file.

---

## Task 5: Handle escalation outcomes

**Files:** as needed (depends on escalation type).

If the worker returned `outcome: escalate`, the most likely reasons:

- `other`: classifier timeout or unexpected git error. Inspect the report's "Proposed next step", apply the fix (e.g., increase timeout, fetch missing objects), re-dispatch.
- `test_design_blocked`: should not occur — the prompt pre-justifies no-test. If it does, treat as a prompt-quality bug.
- `policy_call`: the classifier discovered something unexpected (e.g., the prior-LTS branch has commits we didn't expect, like merge commits). Reproduce the finding, decide, re-dispatch with updated instructions.

For any escalation, do NOT proceed to Task 4. The inventory file lands only on a successful classification.

---

## Task 6: Propose commit to human

**Files:** none (this task verifies + hands off).

- [ ] **Step 1: Show staged state**

```bash
git status
git diff --cached --stat
```

Expected: one staged file (`docs/aiven/uplifts/26.3/inventory.md`).

- [ ] **Step 2: Print proposed commit message**

```
T2 classifier: patch inventory for v25.8.18.1-lts-aiven → v26.3.10.62-lts-aiven-dev

Mechanical inventory of <N> commits from the previous Aiven LTS
release-line, produced by the T2 classifier subagent (explore,
readonly). One row per commit: NNN, sha, date, author, files,
loc, subject, cherry_pick_clean.

cherry_pick_clean uses the readonly equivalent of a cherry-pick
dry-run: git format-patch -1 <sha> --stdout | git apply --check.
This avoids working-tree mutation (which an explore subagent
cannot do anyway) while preserving the spec's intent (does the
patch apply cleanly against the new base?).

Counts:
- Total:                  <N>
- cherry_pick_clean=yes:  <N1>
- cherry_pick_clean=no:   <N2>
- cherry_pick_clean=error:<N3>

Dispatch log row:
docs/aiven/uplifts/26.3/log.md @ <ISO>

Reference: docs/aiven/proposals/2026-05-19-uplift-orchestration-design.md §10 step 6
Plan:      docs/aiven/plans/2026-05-22-classifier-subagent.md
```

- [ ] **Step 3: Hand off**

The agent prints the message and stops. The human commits with `git commit -F <file>` or `git commit -e` to edit further. After the commit lands, T3 (first patch dispatch) is a separate plan.

---

## Acceptance criteria

After human commits, verify:

- [ ] `docs/aiven/uplifts/26.3/inventory.md` exists, has ~77 data rows + preamble + summary.
- [ ] `git log -1 --format=%B | grep -q "T2 classifier"` returns 0.
- [ ] `docs/aiven/uplifts/26.3/log.md` has a row whose `subagent_type` is `explore` and `outcome` is `success`.
- [ ] No source-code files modified (`git diff --name-only HEAD~1..HEAD` shows ONLY `docs/aiven/uplifts/26.3/inventory.md`).
- [ ] No new per-patch dossiers (`ls docs/aiven/patches/` shows only `.gitkeep`).

---

## What this plan deliberately does NOT do

- Run any cherry-pick (the explore subagent doesn't have that authority; we use `apply --check` instead).
- Author any per-patch dossier. Per walking-skeleton discipline (spec §11), dossiers are born at T3 per dispatch, not pre-fabricated.
- Pre-compute `testability` per patch. The classifier output deliberately omits that column; testability is judged fresh at T3 by each patch's worker.
- Make decisions about which patches to port first. T3 starts by asking the human to pick.
- Commit anything. The agent stages and proposes; the human commits.
- Verify the spec amendment's "AGENTS.md inline" requirement empirically. That happens implicitly when we dispatch the classifier with the inlined prompt; if the classifier's behavior reflects Aiven invariants (refuses to write, returns halt-and-escalate format), the inlining mechanism is validated. If not, we adjust the prompt and re-dispatch.

---

## Execution Handoff

Plan complete. Next step is human approval, then Tasks 0–6 execute inline (parent agent does the dispatch + report handling).

Two pieces of information needed before Task 1:

1. **AGENTS.md content for the prompt.** Parent agent reads `docs/aiven/AGENTS.md` and substitutes for `{{AGENTS_MD_CONTENT}}` in the dispatch prompt. No human action needed.
2. **Confirmation of the source range.** If `v25.8.18.1-lts-aiven` is NOT the right prior-LTS reference (e.g., if Aiven has moved to a newer patch level on the prior LTS), the human says so before Task 1, and the SHAs in the plan get updated.

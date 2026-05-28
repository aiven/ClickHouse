# Runbook — commit hygiene for the Aiven LTS uplift system

> **Why this runbook exists.** When a new upstream LTS arrives, we want a small, well-defined set of cherry-picks to bring the **bootstrap** (the AI-orchestration infrastructure — AGENTS.md, schemas, skills, runbooks, hooks, proposals) into the new branch without re-doing the work. That only works if bootstrap commits are **pure**: no per-uplift content mixed in. This runbook defines the categories, the path rules, and the per-LTS-transition procedure.

## 1. The three commit categories

Every commit on a `*-lts-aiven-dev` branch fits exactly one of:

### (A) Bootstrap

**Paths it may touch (and only these):**

- `docs/aiven/AGENTS.md`
- `docs/aiven/schema/**`
- `docs/aiven/skills/**`
- `docs/aiven/runbooks/**` (including this file)
- `docs/aiven/proposals/**` (durable system-design proposals)
- `docs/aiven/plans/**` (planning docs are timestamped but documentary — they record HOW the bootstrap was built, useful as future reference)
- `.cursor/**` (hooks + tool-specific glue)

**Lifecycle:** cherry-picked from the previous LTS's `-aiven-dev` branch into the new one, as the first ~5–10 commits of the new uplift.

**Forbidden in this category:** anything under `docs/aiven/uplifts/**`, `docs/aiven/patches/**`, `src/**`, `tests/**`, `programs/**`, `base/**`, `utils/**`, `contrib/**`.

### (B) Per-uplift

**Paths it may touch (and only these):**

- `docs/aiven/uplifts/<this-version>/**`

**Lifecycle:** **Not** cherry-picked. Each new uplift creates its own `docs/aiven/uplifts/<version>/` directory and writes its own inventory, retrospectives, and work logs from scratch. The previous uplift's directory stays in git history for archaeological reference.

**Forbidden in this category:** anything outside `docs/aiven/uplifts/<this-version>/`.

### (C) Patch port

**Paths it may touch (typical shape):**

- `src/**` — the cherry-picked source change.
- `tests/**` — the new test (or none, if `tests.added: no_justified`).
- `docs/aiven/patches/<NNN>-<slug>.md` — the durable dossier (created or appended).
- `docs/aiven/uplifts/<this-version>/inventory.md` — annotation of row `<NNN>` (the NNN cell becomes a markdown link to the dossier; nothing else changes in the inventory).

**Lifecycle:** atomic — one commit tells the per-patch story (source + test + dossier + inventory link). Not cherry-picked.

**Commit subject (mandatory form):** every category-C commit's subject line MUST be one of:

- `patch-port(<NNN>): <verbatim source-commit subject>` — a ported (shipped) patch.
- `patch-drop(<NNN>): <one-line reason>` — a patch deliberately not carried forward.

where `<NNN>` is the 3-digit zero-padded patch number from the uplift inventory. Examples:

```text
patch-port(042): Fix ZK node leak after create delete table
patch-drop(001): obsoleted by upstream — RefreshTask gate added in 48ec505823e
```

This is the **only** machine-reliable way to identify patches in the log, and it is strictly more complete than a path filter (`git log -- 'src/**' 'tests/**'`): drops touch no code and would otherwise be invisible. Recipes:

```bash
git log --grep '^patch-port('   # shipped patches
git log --grep '^patch-drop('   # dropped patches
git log --grep '^patch-'        # every patch decision (ship + drop)
```

Rationale for a **subject prefix** rather than a body trailer: the subject is the only part visible in `git log --oneline`, which is the at-a-glance scan the marker is meant to serve. The prefix replaces the older implicit rule ("patches are the commits with no `type(scope):` prefix"), which was fragile and was already applied inconsistently (`Port patch 006:` vs bare source subjects vs `drop patch 007`). The patch's diff identity (`git patch-id`) is unaffected — it keys off the diff, not the subject — so byte-equivalence checks still hold.

> **Scope note (2026-05-28):** this convention is enforced **going forward**. The category-C commits already on `v26.3.10.62-lts-aiven-dev` predate it and are deliberately **not** rewritten — their SHAs are cited across the retrospectives, dossiers, and `log.md`, so rewriting would dangle every citation, and it falls outside the one sanctioned squash window (§4, which leaves category-C commits alone regardless). For the current uplift, identify patches via the path filter above plus the per-patch dossiers in `docs/aiven/patches/`.

**At the next LTS transition, the dossier portion is forward-carried by:**

```bash
# In the new uplift's -dev branch, copy ALL dossiers from the previous LTS:
git checkout v<prev>-lts-aiven -- docs/aiven/patches/
git add docs/aiven/patches/
# Then commit as a single bootstrap-adjacent commit (see §3 below).
```

This is **not** a cherry-pick — it's a wholesale tree-import of files whose history we want to keep but whose content is patch-indexed (the dossier for patch 007 is the dossier for patch 007 forever, with a new §6 row appended each uplift).

**Forbidden in this category:** anything under `docs/aiven/{AGENTS.md, schema, skills, runbooks, proposals, plans}/` (those changes belong to category A, in a separate commit).

## 2. The mixing rule

**A single commit may belong to ONLY ONE of A, B, or C.**

If a workstream produces changes across categories (e.g., the T3.1 patch dispatch produced both a schema clarification (A) and a retrospective (B)), split into **separate commits in dependency order**:

```text
<A commit>   schema clarification
<B commit>   retrospective referencing the schema clarification
```

The B commit references "see the preceding bootstrap commit" in its body; the A commit is independently cherry-pickable.

For C commits, the patch port's source change + dossier + inventory annotation are inseparable (they tell ONE story). They go in ONE commit, accepting that the source portion is "re-port" labour at the next LTS.

## 3. Bootstrapping a new LTS uplift

When a new upstream LTS tag arrives (e.g., `v27.3.X.Y-lts`), here is the cherry-pick sequence:

1. **Branch from the new LTS tag:**
   ```bash
   git switch -c v27.3.X.Y-lts-aiven-dev v27.3.X.Y-lts
   ```

2. **Identify bootstrap commits from the previous `-aiven-dev` line:**
   ```bash
   git log v<prev>-lts..v<prev>-lts-aiven-dev --format='%H %s' \
     -- docs/aiven/AGENTS.md docs/aiven/schema/ docs/aiven/skills/ \
        docs/aiven/runbooks/ docs/aiven/proposals/ docs/aiven/plans/ \
        .cursor/
   ```
   This produces the cherry-pick list. Because mixing is forbidden (§2), the file-path filter is sufficient — each listed commit touches **only** bootstrap paths.

3. **Cherry-pick them in order:**
   ```bash
   git cherry-pick <sha-1> <sha-2> ... <sha-N>
   ```
   Expect zero conflicts on a clean upstream rebase (these paths don't overlap with upstream's tree, modulo `.cursor/` which is also Aiven-only).

4. **Forward-carry dossiers in one wholesale-import commit:**
   ```bash
   git checkout v<prev>-lts-aiven -- docs/aiven/patches/
   git add docs/aiven/patches/
   git commit -m "carry forward per-patch dossiers from v<prev>-lts-aiven"
   ```

5. **Create the new uplift directory and start T2 (re-classify) against the new LTS:**
   ```bash
   mkdir -p docs/aiven/uplifts/27.3
   # ... dispatch the T2 classifier per the spec
   ```

The new uplift's `docs/aiven/uplifts/27.3/` is born empty. The old `docs/aiven/uplifts/<prev>/` stays in history for reference but is not modified.

## 4. The "mixed past commits" debt

The bootstrap commits on `v26.3.10.62-lts-aiven-dev` (the first uplift built with this system) are not all pure — some mix bootstrap content with per-uplift content because this policy was written **after** the first few commits landed. Specifically:

- `243ad308bf7` (T1 bootstrap) — mixes pure bootstrap content with `.gitkeep` files under `docs/aiven/uplifts/26.3/` (minor).
- `f452efe2fc7` (T2 closeout) — mixes spec/plan/jira amendments (A) with inventory + retrospective (B). **Material mix.**
- `fd1cc85dee1` (T3.1 closeout) — mixes the `halt-and-escalate.md` schema amendment (A) with the T3.1 retrospective + inventory annotation (B). **Material mix.** This was the last commit landed before the policy in this runbook was written; future T3.x closeouts will split into separate commits per §1.

Before the next LTS transition, the human will **squash and reorganize** these into the canonical two-category shape:

1. One clean "bootstrap" series (just A).
2. One clean "26.3 work log" series (just B).
3. Patch port commits (C) need no reorganization — each is atomic by design.

The squash is a one-time `git rebase -i` operation done on the `-aiven-dev` branch before it's used as the cherry-pick source for the next uplift. This is the **only** time the `no-rebase` invariant in `docs/aiven/AGENTS.md` §4 is relaxed, and it requires explicit human review of the resulting commit boundaries.

## 5. Worked examples

### Example: T3.1 closeout (this is the first time the policy is enforced)

The T3.1 dispatch produced THREE artifacts:

- `docs/aiven/schema/halt-and-escalate.md` (constraint 6 amendment) — **(A) bootstrap**.
- `docs/aiven/uplifts/26.3/02-t3-1-patch-007-retrospective.md` (new) — **(B) per-uplift**.
- `docs/aiven/uplifts/26.3/inventory.md` (row 007 → markdown link to dossier; preamble update) — **(B) per-uplift**.

Resulting commit shape:

```text
<A commit>   schema: dossier-only staged_files for irrelevant-by-removal /
             obsoleted-by-upstream workers
             + new runbook: commit-hygiene.md (THIS file)

<B commit>   docs(aiven/26.3): T3.1 closeout — retrospective + inventory link
```

Two clean commits in dependency order. A is independently cherry-pickable to 27.x.

### Example: a future T3.2 patch port (drop)

A T3.2 dispatch that drops a patch produces:

- `docs/aiven/patches/<NNN>-<slug>.md` (new dossier with `irrelevant-by-removal` conclusion) — **(C) patch port**.
- `docs/aiven/uplifts/26.3/inventory.md` (row annotation) — would be (B), but...

Per §1, category C is allowed to touch the inventory annotation **inside** the patch-port commit because it's part of the single-patch story. So the commit is pure C:

```text
patch-drop(<NNN>): <one-sentence reason>
             includes: dossier + inventory row annotation
```

### Example: a future T3.3 patch port (ship)

Same as above but the dossier + source + test all ship together as a single C commit:

```text
patch-port(<NNN>): <verbatim source-commit subject>
             includes: src change + test + dossier + inventory annotation
```

## 6. Enforcement

The `commit-hygiene` policy is enforced by **convention**, not by a hook. Reasons:

- A hook that inspected the staged file list would have to know the dynamic `<this-version>` for category B, and the dynamic `<NNN>-<slug>.md` for category C. Brittle.
- The policy is written down here, in the runbook the human reads before structuring a commit.
- Mismatches are easy to spot in `git diff --cached --stat` against the path globs above.

If we add a hook for this later, it would be `beforeCommit` (denying mixed commits with a clear error message) — but only after the policy stabilizes. For now: read this runbook; structure your stage list accordingly; the proposed commit message in the AI's output will name the category.

## 7. What this runbook is NOT

- Not a license to bypass `docs/aiven/AGENTS.md` §4's "no rebase / no amend" rule outside the **one** documented squash event before each LTS transition (§4).
- Not a description of how to ENFORCE these rules automatically — that's deferred until the manual policy proves stable.
- Not a per-commit-message style guide — those rules live in `docs/aiven/AGENTS.md` §5 (or are inherited from the repo's root `AGENTS.md`).

## 8. The skim-past-the-prose failure mode (per T3.5 Findings B / C / G)

**Symptom (observed twice).** The parent agent prepares a commit-message file in `tmp/patch-<NNN>/commit-message.txt` with the full body (subject + `Original author:` line + provenance trailer + dispatch-specific narrative). The parent's hand-off message describes the file in prose ("the prepared message is at `tmp/patch-<NNN>/commit-message.txt` — please run `git commit -F <that-file>`"). The human, skimming the prose, runs `git commit` (bare) or `git commit -m "<subject>"` (subject-only) or `git commit -c CHERRY_PICK_HEAD` (auto-populated source body, no `Original author:` line) — and the prepared body is silently discarded.

**Observed instances:**

- T3.4 `e80c209ade8`: Joe Lynch's authorship lost; `--author=` flag was the policy at the time and was forgotten.
- T3.5 `d37ebebc582`: Khatskevich's authorship lost; the `Original author:` line was in the prepared message body, but the human ran a different `git commit` invocation.
- T3.5 `345b7e4a627` (schema Bootstrap commit): the 66-line rationale for the new `tests.added: no_trigger_on_current_lts` enum value was prepared in `tmp/patch-073/schema-bootstrap-commit-message.txt` but discarded — the commit message is the bare subject only.
- T3.5 commit-ordering: parent suggested "schema-bootstrap commit FIRST, then patch-port commit" in prose; the human's actual order was reverse. Same root cause (prose suggestion not seen).

**Rule of thumb:** humans skim prose and act on the FIRST imperative they see. If the parent's hand-off output buries the canonical command 200 words deep in narrative, the human will commit using whatever invocation came to mind from working memory.

**Mitigation (required for parent agents).** The hand-off message MUST open with the literal command, copy-paste-ready, BEFORE any prose:

GOOD (canonical command on line 1):

```
Run this to commit:

    git commit -F tmp/patch-073/retrospective-commit-message.txt

Then verify with `git log -1 --format='%B'` that the body carries the
`Original author:` line (T3.5 Finding B).

[... rationale narrative below the command ...]
```

BAD (command buried in narrative):

```
The retrospective is ready to commit. I've prepared a commit message
file at tmp/patch-073/... that captures the seven findings. The
commit category is Per-uplift mixed with Bootstrap (which is unusual
per §2 but accepted per the acceleration decision in T3.5). When
you're ready, run:

    git commit -F tmp/patch-073/retrospective-commit-message.txt
```

If the hand-off lists multiple commits in dependency order (e.g., Bootstrap THEN patch-port), the message MUST list them as a numbered command sequence:

```
1. git commit -F tmp/patch-NNN/schema-bootstrap-commit-message.txt
2. git commit -F tmp/patch-NNN/commit-message.txt
```

NOT as prose "first do the schema, then the patch-port".

**Verification step (added to the parent's review checklist).** After the human reports the commit landed, the parent runs `git log -1 --format='%B'` against the new HEAD and confirms the body matches the prepared message. If it doesn't, the parent flags the discrepancy in the next retrospective AND adds it to the rule-of-three counter for this section. No amending past commits per AGENTS.md §4 — the divergence becomes part of the history.

**Why no infrastructure mitigation yet.** A `prepare-commit-msg` git hook that auto-injects the `Original author:` line when `CHERRY_PICK_HEAD` exists would solve the body-line case (T3.5 Finding B) but not the ordering case (Finding G) or the bare-subject schema-commit case (Finding C). The skim-past-the-prose failure spans three orthogonal symptoms; the unified mitigation is "command literals first, prose second". Infrastructure intervention (e.g., a hook) is deferred until a third occurrence justifies the maintenance cost.

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

**Lifecycle (working cycle):** during active porting a patch may be committed as an atomic bundle (source + test + dossier + inventory link) — simplest when landing one patch at a time. **End-state (after the §4 reslice):** the bundle is split — the `src/**` + `tests/**` change becomes a standalone, cherry-pickable `patch-port(<NNN>):` commit, and the dossier + inventory annotation fold into the single consolidated docs commit. See the revised §4 reslice target (decision 2026-06-12).

**Commit subject (mandatory form):** every category-C commit's subject line MUST be one of:

- `patch-port(<NNN>): <verbatim source-commit subject>` — a ported (shipped) patch.
- `patch-drop(<NNN>): <one-line reason>` — a patch deliberately not carried forward.

where `<NNN>` is the 3-digit zero-padded patch number from the uplift inventory. Examples:

```text
patch-port(042): Fix ZK node leak after create delete table
patch-drop(007): superseded by upstream removal of the DEFLATE_QPL setting
```

This is the **only** machine-reliable way to identify patches in the log, and it is strictly more complete than a path filter (`git log -- 'src/**' 'tests/**'`): drops touch no code and would otherwise be invisible. Recipes:

```bash
git log --grep '^patch-port('   # shipped (ported) patches
git log --grep '^patch-drop('   # dropped patches
git log --grep '^patch-new('    # net-new (non-ported) patches — see §1(D)
git log --grep '^patch-'        # every patch decision (port + drop + new)
```

Rationale for a **subject prefix** rather than a body trailer: the subject is the only part visible in `git log --oneline`, which is the at-a-glance scan the marker is meant to serve. The prefix replaces the older implicit rule ("patches are the commits with no `type(scope):` prefix"), which was fragile and was already applied inconsistently (`Port patch 006:` vs bare source subjects vs `drop patch 007`). The patch's diff identity (`git patch-id`) is unaffected — it keys off the diff, not the subject — so byte-equivalence checks still hold.

> **Note (2026-05-29):** the 26.3 category-C commits were retroactively renamed to this `patch-port(NNN):` / `patch-drop(NNN):` form during the pre-handover squash (§4), so on this branch you identify patches directly with `git log --grep '^patch-port('`. The pre-squash history — the original SHAs cited throughout the retrospectives and `log.md` — is preserved under the tag `archive/26.3-aiven-dev-presquash`; resolve any stale citation there.

**At the next LTS transition, the dossier portion is forward-carried by:**

```bash
# In the new uplift's -dev branch, copy ALL dossiers from the previous LTS:
git checkout v<prev>-lts-aiven -- docs/aiven/patches/
git add docs/aiven/patches/
# Then commit as a single bootstrap-adjacent commit (see §3 below).
```

This is **not** a cherry-pick — it's a wholesale tree-import of files whose history we want to keep but whose content is patch-indexed (the dossier for patch 007 is the dossier for patch 007 forever, with a new §6 row appended each uplift).

**Forbidden in this category:** anything under `docs/aiven/{AGENTS.md, schema, skills, runbooks, proposals, plans}/` (those changes belong to category A, in a separate commit).

### (D) Net-new patch

A patch **authored fresh against the current LTS** — it fixes or gates something that
exists only in this version, so there is **no source commit to cherry-pick** and hence
**no source-index `NNN`**. (First instance: the `REGISTER_WEBASSEMBLY_UDF` gate for the
new-in-26.3 WebAssembly UDF subsystem.)

**Paths it may touch (typical shape):**

- `src/**` — the new change.
- `tests/**` — the new test (or none, if `tests.added: no_justified`).
- `docs/aiven/patches/N<nn>-<slug>.md` — the durable dossier.
- `docs/aiven/uplifts/<this-version>/inventory.md` — the row for `N<nn>` in the
  **"Net-new patches"** section (a delimited table appended below the mechanical
  source-indexed table; the mechanical table stays one-row-per-source-commit and is NOT
  extended with net-new entries).

**Identity (`N<nn>`):** a per-cycle, 1-based sequence in a **letter-prefixed namespace**
(`N01`, `N02`, …), allocated in authoring order within this uplift. The `N` prefix keeps
the namespace disjoint from the source-index `NNN` (`001`–`0NN`), which is reserved for
"position in the branch we port FROM" and must not be extended. `N<nn>` is a per-uplift
handle, not a permanent cross-uplift ID — see the renumbering note below.

**Commit subject (mandatory form):**

- `patch-new(N<nn>): <subject>` — a net-new shipped patch.

```text
patch-new(N01): Add REGISTER_WEBASSEMBLY_UDF build-time gate for WASM UDFs
```

This extends the greppable `patch-*` family (see §1(C)):

```bash
git log --grep '^patch-new('   # net-new (non-ported) patches
git log --grep '^patch-'       # every patch decision (port + drop + new)
```

**Lifecycle / forward-carry:** atomic single commit (source + test + dossier + net-new
inventory row), like category C. At the next LTS transition the patch is already a commit
on `v<this>-lts-aiven`, so the T2 classifier enumerates it into the NEW uplift's
mechanical source inventory and assigns it a fresh source-index `NNN` there — from then on
it is ported like any other patch (`patch-port(NNN)`). Its dossier carries forward
wholesale (§3 step 4); the `N<nn>-` filename persists as historical record (the `N<nn>`
handle is meaningful only within the uplift that authored it).

> **Renumbering caveat (applies to all patch IDs).** `NNN` is a *positional index into
> the current uplift's source range*, so it renumbers each cycle as the source branch
> changes. `N<nn>` is likewise per-cycle. Neither is a stable cross-uplift identity; the
> stable identity of a patch is its **slug** (and dossier file), which is why dossiers are
> carried forward by slug, not by number.

**Forbidden in this category:** bootstrap paths (category A) and the per-uplift work-log
files (category B) — those go in separate commits per §2.

## 2. The mixing rule

**A single commit may belong to ONLY ONE of A, B, or C.**

If a workstream produces changes across categories (e.g., the T3.1 patch dispatch produced both a schema clarification (A) and a retrospective (B)), split into **separate commits in dependency order**:

```text
<A commit>   schema clarification
<B commit>   retrospective referencing the schema clarification
```

The B commit references "see the preceding bootstrap commit" in its body; the A commit is independently cherry-pickable.

For C/D commits during the working cycle, the source change + dossier + inventory annotation may share ONE commit (they tell one patch story). At end-state the §4 reslice **splits** them: pure `src/**` + `tests/**` per-patch commits (cherry-pickable for same-major backports) and a single consolidated docs commit. The single-patch story is then preserved by the `patch-port(<NNN>)` / `patch-new(N<nn>)` subject ↔ `<NNN>` / `N<nn>` dossier-and-inventory linkage, not by co-commitment.

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

6. **Re-point the version-hardcoded orientation strings.** Several bootstrap (A) files name the *current* LTS version verbatim instead of deriving it. They are carried forward by step 3 with the old version baked in, so update each by hand once, right after the cherry-pick. This list is the contract — keep it in sync when you add a new hardcoded site:

   | File | What to change |
   |---|---|
   | `docs/aiven/AGENTS.md` §1 Orientation | the upstream tag `v<prev>-lts` and the `v<prev>-lts-aiven-dev` / `v<prev>-lts-aiven` branch names |
   | `docs/aiven/skills/dispatch-prompt-template.md` | the embedded `[TEMPLATE BEGIN]` `AGENTS.md` copy repeats the same tag/branch strings — update them to match the file above |
   | `.cursor/hooks/log-subagent-completion.sh` | the hardcoded `docs/aiven/uplifts/<prev>/log.md` and `.../reports/` paths the `subagentStop` hook appends to |

   Verify with `git grep -n '<prev-version-string>' docs/aiven .cursor` (e.g. `git grep -n '26\.3' docs/aiven .cursor`); the only remaining hits should be inside `docs/aiven/uplifts/<prev>/` and `docs/aiven/patches/*` lineage rows, which are *historical record* and must NOT be rewritten.

The new uplift's `docs/aiven/uplifts/27.3/` is born empty. The old `docs/aiven/uplifts/<prev>/` stays in history for reference but is not modified.

> **Settings naming (forward-only) — reaffirm at every transition.** New
> Aiven-introduced settings follow the `aiven_` prefix convention
> (`docs/aiven/AGENTS.md` §8): a setting that does not exist upstream and is
> introduced fresh on this LTS-aiven line takes the `aiven_` prefix (firm for
> `ServerSetting`s). The rule is **forward-only**: at dossier/setting
> carry-forward (step 4 above), do **NOT** retro-prefix settings Aiven already
> shipped under a non-prefixed name (`enforce_https_for_url_storage`,
> `user_with_indirect_database_creation`, the Kafka settings, the 25.8
> replication-queue thresholds, …) — a shipped name is stored-DDL/external
> contract (see the settings backward-compatibility theme in each uplift's
> `major-upstream-changes.md`). First application: `aiven_enable_replication_queue_size_limit`
> (patch 008) — see [`proposals/2026-06-15-aiven-settings-naming-convention-and-queue-size-guard.md`](../proposals/2026-06-15-aiven-settings-naming-convention-and-queue-size-guard.md).

## 4. The "mixed past commits" debt — RESOLVED 2026-05-29

The first-uplift commits mixed categories (e.g. `f452efe2fc7` T2 closeout and `fd1cc85dee1` T3.1 closeout mixed A+B — pre-squash SHAs, resolvable via the archive tag below) because this policy was written **after** the first few commits landed.

Resolved on 2026-05-29 by a one-time **in-place reslice** before handover: `git reset` to the LTS base `v26.3.10.62-lts` (working tree left untouched), then re-commit by category into the canonical shape:

1. one `bootstrap` commit (all category A);
2. the category-C patch commits, each reworded to the `patch-port(NNN):` / `patch-drop(NNN):` subject (§1(C)) — content byte-identical, re-hashed;
3. one `docs(aiven/26.3)` work-log commit (all category B).

Correctness was proven by an empty `git diff` against the pre-squash tip, which is preserved as the tag `archive/26.3-aiven-dev-presquash`; the branch was then force-pushed. This is the **only** time the `no-rebase` invariant in `docs/aiven/AGENTS.md` §4 is relaxed, and it was done with explicit human review of the resulting commit boundaries.

**For future uplifts (revised target — decision 2026-06-12):** perform the same reslice (`git reset` to the LTS base + re-commit by category, verified by an empty `git diff` against an archive tag — preferred over `git rebase -i` because it partitions the final tree instead of replaying historical diffs, so no conflicts) before the `-aiven-dev` branch becomes the cherry-pick source for the next LTS. The 2026-05-29 reslice above bundled each port's code together with its docs; **from 26.3 onward the target is a three-bucket partition** that isolates code from docs for cleaner forward-backporting:

1. **One bootstrap (A) commit** — all of `docs/aiven/{AGENTS.md, schema, skills, runbooks, proposals, plans}` + `.cursor/`. Stays cleanly separable for forward-carry (§3).
2. **N pure-code commits** — one per shipped patch, touching **only** `src/**` + `tests/**`, subject `patch-port(<NNN>):` (ported) or `patch-new(N<nn>):` (net-new). These are the cherry-pickable units, so the `patch-*` marker MUST live here — it is what `git log --grep` and `git patch-id` key on. No docs in these commits.
3. **One consolidated docs commit** — all `docs/aiven/patches/**` dossiers (ports + drops + net-new) and all of `docs/aiven/uplifts/<this-version>/**` (inventory with every annotation, work-logs, retrospectives, screenings).

**Consequences of the code/docs split (vs the 2026-05-29 bundle):**

- **Drops have no code**, so `patch-drop(<NNN>)` is no longer its own commit — a dropped patch lives entirely inside the consolidated docs commit (its dossier + inventory row). `git log --grep '^patch-drop('` therefore stops being a per-drop history marker; drops are discoverable via the inventory and dossiers instead.
- **Net-new patches split too:** the `patch-new(N<nn>):` code commit (bucket 2) carries the `src/**` change; its dossier + inventory row go in the consolidated docs commit (bucket 3).
- **Why the split is worth it:** pure-code commits keep `git patch-id` byte-equivalence checks clean and let same-major (minor/patch) backports cherry-pick without docs conflicts. Across a *major* LTS jump the code itself still drifts (so re-port remains the norm per §1(C)), but the split removes docs-conflict noise regardless.
- **Working-cycle commits are unaffected:** during active porting you may still land atomic bundles; the reslice re-partitions them into the three buckets at end-state.

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

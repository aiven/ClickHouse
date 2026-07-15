# Dispatch-prompt template

> **Status:** v1 (2026-05-25). Extracted after the rule-of-three trigger: T3.2 / T3.3 / T3.4 dispatch prompts had converged on a stable structure with patch-specific variation isolated to a small set of slots. Per T3.4 retrospective Finding D.

## What this is

A skeleton for the `tmp/patch-<NNN>/dispatch-prompt.md` file the parent agent constructs before every T3.X dispatch. The skeleton encodes everything that should be *constant* across dispatches (Aiven invariants, the schema reference, the embedded procedure, the hard constraints block). Anything *patch-specific* (source SHA, identifiers, diff, expected outcomes, optional cleanup steps) lives in clearly marked `<<placeholders>>` or `[[optional-block]]` sections.

The template is opinionated: it reflects three production dispatches' worth of empirical signal. Sections like "expected cherry-pick outcome", "optional Step 2.5 cleanup", and "source-author preservation in the commit message body" exist because earlier dispatches taught us they had to.

## When to use

- **Always** before a T3.X patch-port dispatch.
- **Never** for read-only subagent dispatches (`explore`, `cursorGuide`) — those don't follow the embedded procedure and would just see noise.

## How to use (parent agent workflow)

1. Create `tmp/patch-<NNN>/` if it doesn't exist.
2. Copy this template's body (section [TEMPLATE BEGIN] → [TEMPLATE END] below) to `tmp/patch-<NNN>/dispatch-prompt.md`.
3. Resolve every `<<placeholder>>` against the patch — see the placeholder reference below.
4. Decide whether each `[[optional-block]]` applies; keep + fill or delete entirely (don't ship an empty optional block).
5. Run the parent C++ preflight (per `skills/cpp-review-checklist.md`); use the results to populate `<<PARENT_PREFLIGHT_FINDINGS>>` and any policy calls.
6. Cross-check the filled prompt against the most-recent prior dispatch (a worked example, e.g., `tmp/patch-077/dispatch-prompt.md` for T3.4) to confirm structural parity.
7. Dispatch the worker (`generalPurpose` subagent, **Opus 4.8 with `xhigh` thinking**, foreground — see "Worker model policy" below).
8. Review the staged state + halt-and-escalate report; surface findings to the human.
9. Human commits using the commit message the worker proposed (which already carries the source author in the body — see "Source-author preservation policy" below).

## Worker model policy

Dispatch T3.X patch-port workers with **Opus 4.8 at `xhigh` thinking** — name it explicitly rather than relying on "parent's model". Two reasons: (1) the worker carries the load-bearing reasoning (conflict resolution, byte-equivalence decomposition, evidence-of-causation test design); inheriting whatever the parent happens to run is a silent failure mode if the parent is ever downgraded for cost. (2) An explicit floor makes the empirical record interpretable — an escalation from a known model tier is a signal about the *patch*, not about model variance.

Empirical basis: T3.1 and T3.2 ran on `opus-4-7-thinking-xhigh` and that tier cleared the bar (retros `02`/`03`); Opus 4.8 supersedes it. This is the forward default for the *next* uplift's bootstrap too — step it down only when a retrospective shows a cheaper tier holding the same bar across rule-of-three dispatches.

## Branch & handover policy (load-bearing)

The worker does **NOT** create, switch, or push branches. It works **in place** on the `*-aiven-dev` integration branch (Step 0 verifies HEAD is `v26.3.15.4-lts-aiven-dev`), stages its changes, and stops. The human commits **on that same branch** — there is no feature branch and no merge/fast-forward handover step.

Rationale: per AGENTS.md §5 the worker never commits, so there is nothing to isolate on a branch; a branch in the same checkout does not isolate the shared working tree anyway, and it only adds a needless `merge --ff-only` step at handover. A parent that hand-writes "create a working branch" into a bespoke prompt (as happened once, pre-codification, producing `patch-port-015-…`) is **deviating from this template** — always dispatch *through* this template so Step 0 + the hard-constraints block keep the worker in place.

If true isolation is ever needed (e.g. concurrent workers on the same repo), use a separate **git worktree** (one directory per worker), not a branch — branches do not isolate the working tree in a single checkout. That is out of scope for the current one-patch-at-a-time cadence.

For a submodule-coupled patch (clause (vi)) whose Aiven fork has **already been prepared and pushed**, the parent overrides the default `external_dependency` escalation by baking the exact gitlink SHA + fork branch into `<<PARENT_POLICY_CALLS>>`, so the worker bumps `.gitmodules` + the gitlink in place rather than halting.

## Source-author preservation policy (load-bearing)

Local committer is always the author of record (`Author: <local human>`). The source-commit author is recorded as a **line in the commit message body**, not via the `--author=` git flag. Rationale: the `--author=` mechanism is easy to forget at commit time (T3.4 Finding C: it was forgotten, Joe Lynch's authorship was lost in `e80c209ade8`); a line in the body survives review and grep regardless of how the commit is invoked.

The worker's `proposed_commit.commit_message` field MUST include the line in the body, immediately after the subject line and a blank line. The subject line itself MUST carry the `patch-port(<NNN>):` prefix mandated by `docs/aiven/runbooks/commit-hygiene.md §1(C)`:

```
patch-port(<<PATCH_NNN>>): <source commit subject>

Original author: <Name> <email>, <YYYY-MM-DD>.

(cherry picked from commit <SOURCE_SHA>)

[Optional context lines — test rename, style cleanup, conflict resolution narrative]
```

The `(cherry picked from commit ...)` line is added by `git cherry-pick -x` automatically; the worker's job is to add the `patch-port(<<PATCH_NNN>>):` subject prefix and the `Original author:` line above it. (A dropped patch — a parent-only decision, not a worker dispatch — uses `patch-drop(<<PATCH_NNN>>): <reason>` instead; see `commit-hygiene.md §1(C)`.)

The worker MUST emit the `Original author:` line UNCONDITIONALLY (even when the source author is the same as the local human). The committer's review then has one less special case to worry about, and the audit trail is uniform across the patch series.

## Cherry-pick-clean policy (per T3.4 Finding B)

The T2 inventory's `cherry_pick_clean` column is a forecast, not a contract. The worker MUST attempt `git cherry-pick --no-commit -x <sha>` regardless of the column's value. Only conflict markers (`UU` lines in `git status`) trigger the manual-resolution branch; a `cherry_pick_clean=no` patch can apply cleanly when `git`'s three-way merge tolerates trailing-context drift that `git apply --check` (the classifier's tool) rejects.

When you author the prompt, write Step 2's `<<CHERRY_PICK_EXPECTED_OUTCOME>>` block based on the parent's preflight reasoning (what *should* happen), not the classifier's verdict.

## Parent preflight discipline — `(i)` / `(ii)` / `(iii)` / `(iv)`

**Status of each clause:**

- `(i)` patched-line stability, `(ii)` context-window stability, `(iii)` identifier inventory + semantic-equivalence — **VERIFIED 2026-05-28** (codified at this commit). Introduced as mental discipline in T3.7 Finding A with codification explicitly deferred until rule-of-three was met; T3.8 produced the first decisive `obsoleted-by-upstream` outcome (patch 001, dropped — see retro 09) and saved ~25 min of worker time; T3.9 through T3.15 all applied the discipline proactively. Counter at codification time: **8 of 3** (well past rule-of-three).
- `(iv)` reachability proof — **PROVISIONAL** (rule-of-three counter: **2 of 3**; codify into the mandatory set on the third proactive use). T3.13 escalation `test_design_blocked` crystallized the discipline retrospectively; T3.14 redispatch and T3.15 patch 042 dispatch were the first two proactive applications.
- `(v)` blast-radius / default-behavior change — **PROVISIONAL** (anticipatory, rule-of-three counter: **0 of 3** — no occurrence yet; introduced ahead of an expected auto-replication patch). Orthogonal to (i)–(iv): it routes a default-on behavior change to a `policy_call` gating decision rather than to a cherry-pick/test state, so it is **not** part of the four-state table below.
- `(vi)` submodule fork-redirect — **PROVISIONAL** (anticipatory, rule-of-three counter: **0 of 3** — no occurrence yet; introduced ahead of expected `.gitmodules` redirects to Aiven forks, e.g. `contrib/aws`). Orthogonal to (i)–(iv): it routes a patch that re-points a vendored submodule to an Aiven fork to an `external_dependency` escalation (the fork must be prepared outside this checkout first), so it is **not** part of the four-state table below.

Before every T3.X dispatch, the parent MUST execute clauses (i), (ii), (iii) and write the results into `<<PARENT_PREFLIGHT_FINDINGS>>`. The (iv) check is RECOMMENDED and will be promoted to MANDATORY on its third proactive use. The (v) screen applies only to patches that change a default behavior; when it fires it routes to a `policy_call` (human-in-the-loop) and is independent of the (i)–(iv) outcome. The (vi) screen applies only to patches that touch `.gitmodules` or a submodule pointer; when it fires it routes to an `external_dependency` escalation and is likewise independent of the (i)–(iv) outcome.

### Clause (i) — patched-line stability

For each `@@ -N,M +N',M' @@` hunk in the source patch, render the current HEAD content of the same logical region and verify the lines the patch *modifies* are unchanged on HEAD:

```bash
git show <SOURCE_SHA>:<file> | sed -n 'N,N+M-1p' > /tmp/src-pre.txt
sed -n 'N',N'+M'-1p' <file>                       > /tmp/head.txt
diff /tmp/src-pre.txt /tmp/head.txt
```

Empty diff → **(i) PASS**. Non-empty → patched-line drift; the patch cannot apply verbatim — expect rewrite or escalation.

### Clause (ii) — context-window stability

Within each `@@` window, do the ±5 context lines on HEAD match the source patch's context?

```bash
diff \
  <(git show <SOURCE_SHA>:<file> | sed -n 'N-5,N+M+5p') \
  <(sed -n 'N-5,N+M+5p' <file>)
```

Empty diff → **(ii) PASS**. Non-empty → context drift; expect a `git cherry-pick` conflict at the affected site. The classifier's `cherry_pick_clean=no` verdict typically reflects (ii) drift, but per T3.4 Finding B the cherry-pick may still apply cleanly because `git`'s three-way merge tolerates drift that `git apply --check` rejects.

### Clause (iii) — identifier inventory + semantic-equivalence on HEAD

For each identifier in the patch (function names, types, enum values, member names), grep on HEAD and assert presence:

```bash
for id in <id1> <id2> ...; do
  printf '%s: ' "$id"
  git grep -c "$id" -- src/ | awk -F: '{s += $NF} END {print s+0}'
done
```

Each identifier MUST be PRESENT on HEAD (count > 0). If any is `0`, investigate (renamed? removed? namespaced?).

Then ask: **does the patch's behavior already exist on HEAD via a different code path?** Use `git log` with the patch's signature substring on the file:

```bash
git log --oneline -S '<signature substring>' --reverse -- <file>
```

If a sibling upstream commit is already an ancestor of the LTS tag, the patch is `obsoleted-by-upstream` — **(iii) DROP**.

**Blind spot for add-new-TU patches (VERIFIED 2026-06-10, patch 054 — the parent's preflight missed this).** When the patch ADDS a new translation unit, "the framework is present on HEAD" (grep counts > 0) is necessary but NOT sufficient. A backported new file bakes in the *base-version* API/typedef conventions, which a clean three-way cherry-pick cannot reveal because the new file has no HEAD counterpart to merge against. Patch 054's new `KeeperMapSettings.{cpp,h}` (authored on 25.8) compiled there but failed on 26.3 in three ways the cherry-pick was blind to: `ASTPtr` had migrated from `std::shared_ptr<IAST>` to `boost::intrusive_ptr<IAST>` (the file's hand-rolled typedef then collided with `Parsers/IAST_fwd.h` and broke every co-including TU), AST construction had moved from `std::make_shared<AST…>` to `make_intrusive<AST…>`, and `IDatabase::alterTable` had gained a `validate_new_create_query` parameter. **Mitigation:** for any patch that adds files, the parent's (iii) check MUST extend to a *compile premise* — sanity-check the new file's API surface against HEAD (grep the typedefs/idioms the new file uses: `ASTPtr` definition, `make_shared<AST` vs `make_intrusive<AST`, the arity of any cross-module method it calls like `alterTable`), and treat a `still-needed-but-rewrite` conclusion as likely. The authoritative detector is the compiler; where feasible the worker compiles the new TU early (the failure is named precisely and costs one ninja cycle, vs. an escalation round-trip). Each such adaptation is a bounded, parent-authorized divergence beyond the conflict-driven merge, and `byte_equivalent: false` is expected.

### Clause (iv) — reachability proof (PROVISIONAL)

Given the chosen test trigger T and the patched function F on HEAD, write out the call chain `T → ... → F`. For each intermediate frame, verify:

- **(iv-a) Code-path reachability.** No upstream sanity check rejects T BEFORE reaching F (examples observed in this uplift: `StorageMaterializedView.cpp:222-225` rejects `Atomic`-engine MV creation; `MergeTreeData::checkProperties` rejects sorting-key alters that don't extend a prefix; `AlterCommands.cpp:653-666` rejects DROP COLUMN of a sorting-key column).
- **(iv-b) Differential observability.** The observable produced by T differs between pre-patch and post-patch. If a wider-scope code change defeats the patch's predicate *equally* on both LTSes — the patch-060 saga — the trigger is iv-b unreachable: the test would pass-pass or fail-fail across the flip, distinguishing nothing.

If either (iv-a) or (iv-b) fails, redesign T or escalate `test_design_blocked` BEFORE the worker dispatch. Worker time spent discovering iv-blockedness empirically is wasted compared to ~5 minutes of parent reading the call chain.

### Clause (v) — blast-radius / default-behavior change (PROVISIONAL, anticipatory: 0 of 3)

Clauses (i)–(iv) ask "can the patch apply, and can we test it." Clause (v) asks a different question: "*should* this patch ship its behavior on by default." It is an orthogonal axis — a patch can apply byte-clean and pass (i)–(iv) and still warrant (v).

**Trigger.** The patch changes a *default* behavior that a broad class of existing objects, queries, or tests would hit, and the new behavior is not already gated by a setting. The tell is: *"enabling this unconditionally would likely change the outcome of many existing tests."* Canonical example: a patch that auto-transforms a plain `MergeTree` engine into `ReplicatedMergeTree` — correct for Aiven's managed fleet, but it silently rewrites every affected table definition and would flip a large fraction of the stateless/stateful suite.

**Mandated action.** The parent MUST NOT dispatch such a patch as a default-on change, and the worker MUST NOT try to prove safety by running the whole suite (it is too expensive, and a green suite is not even the point — the behavior change is intentional). Instead, escalate **`policy_call`** (see `docs/aiven/schema/halt-and-escalate.md`) asking the human: *should this behavior be gated behind a new, default-disabled server setting?* The worker is not authorized to decide this; it is the human-in-the-loop gate.

**Suggested resolution — the default-off setting gate.** Add a server setting that gates the new behavior, default `false`. Example: `enforce_table_replication = false`, which when enabled auto-transforms `MergeTree` → `ReplicatedMergeTree`; production turns it on, the upstream default stays off. Because the default is off, the existing suite's behavior is unchanged, so the patch can land **without** a suite-wide run.

**Testing under the gate.** The new test sets the gate explicitly (`SET enforce_table_replication = 1`, or the server-config equivalent for a server-level setting) and produces a normal evidence-of-causation pair on the gated path: the behavior is absent with the setting off and present with it on. This satisfies AGENTS §7 with a scoped test rather than a full-suite diff. The human still decides the setting's name, its default, and the rollout.

### Clause (vi) — submodule fork-redirect (PROVISIONAL, anticipatory: 0 of 3)

Clauses (i)–(v) assume the patch is a self-contained change to tracked source. Clause (vi) catches the case where the patch instead **re-points a vendored dependency** to an Aiven fork: a hunk in `.gitmodules` changing a `url =` from `github.com/ClickHouse/<x>` to the Aiven org, and/or a change to the submodule's pinned commit. It is orthogonal to (i)–(iv) — the `.gitmodules` hunk may apply byte-clean and still be unbuildable.

**Trigger.** The source patch touches `.gitmodules` (or a submodule's pinned commit) such that a vendored dependency now resolves to an Aiven-owned fork rather than the upstream repo. The tell is a `url =` line pointing at the Aiven org, or a submodule gitlink moving to a commit that is not reachable from the upstream remote. Canonical example: `contrib/aws` → Aiven's `aws-sdk-cpp` fork, where the fork carries an SDK version bump plus ~2 Aiven patches on top.

**Why this can't be ported in-checkout.** The Aiven fork must first exist in the required state (correct upstream SDK base + the Aiven patches re-applied) before `.gitmodules` can point at a real, buildable ref. That preparation happens in a *different* repository, outside this checkout. A worker that naively applies the `.gitmodules` hunk would pin a ref that is missing or stale, and any build would be meaningless.

**Mandated action.** The parent SHOULD detect this in preflight (grep the source diff for `.gitmodules` / submodule-pointer changes) and NOT dispatch it as a normal port. If it is dispatched, the worker MUST STOP and escalate **`external_dependency`** (see `docs/aiven/schema/halt-and-escalate.md`) instead of editing `.gitmodules`. The escalation MUST report, specifically: (1) which submodule/path; (2) the exact upstream version/tag the fork must be based on; (3) the Aiven patches to re-apply on top; (4) the target commit/ref `.gitmodules` should land on once the fork is ready. The human prepares the external fork first; the port resumes against the prepared ref afterward.

See `docs/aiven/runbooks/submodule-forks.md` for the discovery query (enumerate every fork-coupled patch in one pass), the read-only base-hash recipe, the human prep commands (the agent never touches the fork repos), and the per-uplift fork registry. Best practice is to batch-prepare all forks at bootstrap so a fork-coupled patch is dispatched with the gitlink + `branch =` already baked in (which overrides this escalation).

### Four-state classification

The combination of (i)/(ii)/(iii)/(iv) outcomes maps to a small set of dispatch dispositions:

| (i) | (ii) | (iii) | (iv) | Outcome | Worker dispatch? |
|---|---|---|---|---|---|
| pass | pass | pass-semantic | pass | clean cherry-pick + reachable test | **YES** — standard dispatch |
| pass | fail | pass-semantic | pass | context drift but semantically still-needed | **YES** — dispatch with `[[MULTI_OUTCOME_CHERRY_PICK]]` outcome B/C wired into Step 2 |
| fail | fail | pass-semantic | pass | heavy drift / structural rewrite | **YES** — dispatch with `still-needed-but-rewrite` conclusion + hunk-by-hunk re-targeting plan in `<<PARENT_POLICY_CALLS>>` |
| fail | fail | upstream-already | n/a | `obsoleted-by-upstream` | **NO** — drop the patch; author a `0N-…-drop-retrospective.md` instead (T3.8 patch 001 case) |
| pass | pass | pass-semantic | fail | trigger is gated upstream of F, or wider-scope change defeats observability | **NO** — `test_design_blocked` BEFORE dispatch; redesign T or escalate to human policy decision (T3.10/T3.11/T3.12 patch 060 case; T3.13 patch 049 case before redesign) |

The classifier's job is to keep the worker dispatching against a state the worker can actually finish in, not to chase signals the worker would then escalate on.

### What this codifies and what it doesn't

- **Codifies** the four-clause discipline that has empirically saved worker cycles on five of the last seven dispatches.
- **Does NOT codify** when a worker may re-derive (iv) mid-dispatch — the discipline is for the parent's preflight, not the worker's runtime. A worker observing iv-blockedness mid-dispatch escalates `test_design_blocked` and the parent re-applies (iv) before the redispatch.
- **Does NOT codify** the n=1 PROVISIONAL clauses observed in `runbooks/integration-tests.md §7.4` (`keeper_randomize_feature_flags`, `spawn E2BIG`); those are environment-of-execution gotchas, not preflight checks.

## Placeholder reference

Every placeholder marked `<<NAME>>` MUST be resolved to a concrete value before dispatch. Markers marked `[[optional-block: NAME]]` are entire optional sections to keep or delete.

| Placeholder | What goes here | Where to find it |
|---|---|---|
| `<<DISPATCH_TAG>>` | `T3.X` identifier (e.g., `T3.5`) | Increment from the last retrospective |
| `<<DISPATCH_ORDINAL>>` | English ordinal (e.g., `fifth`) | Match `<<DISPATCH_TAG>>` |
| `<<PATCH_NNN>>` | 3-digit zero-padded patch number from inventory | `docs/aiven/uplifts/26.3/inventory.md` NNN column |
| `<<SOURCE_SHA>>` | Full 40-char source SHA from inventory | Inventory `sha` column (10-char) → `git log --format=%H <sha>` for full |
| `<<SOURCE_SHA_SHORT>>` | 10-char short SHA | Inventory `sha` column |
| `<<SUBJECT>>` | Patch subject line | Inventory `subject` column |
| `<<SLUG>>` | kebab-case slug (no NNN prefix) | Derive from subject; reuse pattern from sibling dossiers |
| `<<DOSSIER_PATH>>` | `docs/aiven/patches/<NNN>-<slug>.md` | Compose from `<<PATCH_NNN>>` + `<<SLUG>>` |
| `<<AUTHOR_NAME>>` | Source author name | `git show --no-patch --format='%an' <sha>` |
| `<<AUTHOR_EMAIL>>` | Source author email | `git show --no-patch --format='%ae' <sha>` |
| `<<AUTHOR_DATE>>` | YYYY-MM-DD | `git show --no-patch --format='%ad' --date=short <sha>` |
| `<<COMMITTER_NAME>>` | Committer name on source branch | `git show --no-patch --format='%cn' <sha>` |
| `<<COMMITTER_EMAIL>>` | Committer email on source branch | `git show --no-patch --format='%ce' <sha>` |
| `<<FILES_TOUCHED_TABLE_CELL>>` | Comma-separated file list with ±LOC | `git show --stat <sha>` |
| `<<LOC_CHANGED>>` | Total added + removed | `git diff-tree --numstat <sha>` |
| `<<CHERRY_PICK_CLEAN_FLAG>>` | `yes` / `no` (informational only — see policy above) | Inventory `cherry_pick_clean` column |
| `<<SOURCE_COMMIT_BODY>>` | Full body (subject + body) | `git log -1 --format='%B' <sha>` |
| `<<SOURCE_DIFF>>` | Full unified diff | `git show <sha>` (omit the metadata lines if desired) |
| `<<BEHAVIOR_CHANGES>>` | Numbered list of user-visible behavior changes the patch introduces | Parent's reading of the diff |
| `<<NEW_DIMENSIONS_LIST>>` | Bulleted list of "first time" mechanics this dispatch exercises | Compare against prior retros' "exercised" matrix |
| `<<PARENT_POLICY_CALLS>>` | Numbered list of parent-decided policy items the worker must follow (e.g., style cleanup, test rename, scope limits, known-limitations to document) | Parent's preflight |
| `<<PARENT_PREFLIGHT_FINDINGS>>` | Numbered list of pre-flight checks the parent already ran, with their conclusions baked in as a strong prior | Parent's preflight |
| `<<IDENTIFIER_INVENTORY>>` | Markdown table mapping each identifier in the patch (function names, types, enum values, etc.) to its role | Parse from `<<SOURCE_DIFF>>` |
| `<<IDENTIFIER_GREP_BASH>>` | Bash one-liner that greps each identifier on HEAD | Compose from inventory above |
| `<<DRIFT_FILE_HISTORY_BASH>>` | `git log v25.8.18.1-lts..v26.3.15.4-lts -- <file>` for each touched file | Compose from `<<FILES_TOUCHED>>` |
| `<<UPSTREAM_EQUIVALENT_GREP>>` | `git log --grep '<topic-regex>' v25.8.18.1-lts..v26.3.15.4-lts` | Per-patch topic terms |
| `<<HUNK_CONTEXT_VERIFICATION>>` | Bash that prints the current 26.3 HEAD content of the regions the patch modifies, with expected-shape notes | Per-patch `rg`/`sed` recipe |
| `<<CHERRY_PICK_EXPECTED_OUTCOME>>` | Narrative explaining what should happen in Step 2: clean / conflict at site X / something else. Multiple outcomes (A/B/C) allowed | Parent's preflight reasoning, NOT the T2 flag |
| `<<PATCH_ID_EXPECTATION>>` | Narrative explaining whether the source-SHA's stable patch-id is expected to match the staged diff's stable patch-id. **Default expectation is MATCH** (per T3.5 Finding E): `git patch-id --stable` normalizes unified-diff line numbers AND `index` blob hashes, so any clean cherry-pick whose semantic content is identical produces matching stable ids regardless of file growth between LTSes. Expect MISMATCH only when the cherry-pick required a manual semantic resolution (`[[block: STEP_2_5_OTHER]]` with semantic-edit content) or when the diff intentionally diverges. The worker then proceeds to the decomposition check; a MISMATCH that decomposes to pure context/rename/blob-hash drift is `byte_equivalent: false` but still acceptable. | Parent's preflight reasoning |
| `<<BYTE_EQUIVALENT_EXPECTATION>>` | One of `true` / `false`. Use `true` whenever the dispatch's policy expects matching stable patch-ids (the common case — see `<<PATCH_ID_EXPECTATION>>`). Use `false` only when the parent intentionally instructs the worker to diverge from the source diff (rare; document the divergence in `<<CUSTOM_CLEANUP_TITLE>>` or `<<PARENT_POLICY_CALLS>>`). Pre-T3.5 dispatches over-conservatively defaulted to `false`; T3.5 measured `true` for ~850 lines of file-growth drift. | Parent's preflight reasoning |
| `<<TEST_FORMAT>>` | `.sql` or `.sh` or `integration` | Per `runbooks/testing-suites.md` §2 transport-follows-format rule |
| `<<TEST_DESIGN>>` | Either: "implement verbatim — body and reference content below" + body + reference, OR: "adopt the shipped test, rename per §4.1 (was `<old-prefix>_*`, becomes `9<NNN>_*`)" | Parent + worker decision |
| `<<TEST_PATH_SH_OR_SQL>>` | `tests/queries/0_stateless/9<NNN>_<slug>.{sh,sql}` | Compose from `<<PATCH_NNN>>` + `<<SLUG>>` |
| `<<TEST_PATH_REFERENCE>>` | `tests/queries/0_stateless/9<NNN>_<slug>.reference` | Same |
| `<<PATCHED_FILES>>` | Space-separated list of `src/` files the patch modifies | From source diff |
| `<<PATCHED_FILES_COUNT_NAMED>>` | "ONE" / "TWO" / "THREE" etc. in caps (for the §6 narrative) | Count `<<PATCHED_FILES>>` |
| `<<EXPECTED_STAGED_FILE_COUNT_NAMED>>` | "FIVE" / "SIX" etc. (source files + dossier + test files) | Count |
| `<<EXPECTED_STAGED_FILE_LIST>>` | Numbered list of expected staged files for `outcome: success` | Compose |
| `<<EVIDENCE_FILE_LIST>>` | Files the worker's Evidence section should quote excerpts from | Per-patch (varies based on optional steps) |
| `<<RUNTIME_BUDGET_MINUTES>>` | Budget cap; `max(15, 2 × longest-expected-build + 5)` minutes | Estimate from `LOC_CHANGED` + cache state |

## Optional blocks reference

Optional `[[block: NAME]]` sections that may or may not appear in a given dispatch. Keep + fill, or delete entirely (do NOT ship an empty optional block — it confuses the worker).

| Block | Use when |
|---|---|
| `[[block: STEP_2_5_STYLE_CLEANUP]]` | Source patch uses K&R braces or other repo-non-conformant style; parent's policy call mandates Allman/style cleanup during port (T3.3 case). |
| `[[block: STEP_2_5_TEST_RENAME]]` | Source patch ships its own test under an upstream-style prefix; parent mandates rename to `9<NNN>_<slug>` per Aiven §4.1 (T3.4 case). |
| `[[block: STEP_2_5_OTHER]]` | Any other patch-port cleanup the parent explicitly mandates. Be specific in the block content; the worker is not authorized to invent cleanup. |
| `[[block: MULTI_OUTCOME_CHERRY_PICK]]` | The cherry-pick has multiple plausible outcomes (clean / conflict at site X / drift-superseded). Spell them out as Outcome A/B/C/...; otherwise use a single-outcome narrative. |
| `[[block: SHIPS_OWN_TEST]]` | The source patch already includes test files (`.sql`/`.sh`/`.reference`). Adopt verbatim; rename per Aiven §4.1; do NOT redesign. |
| `[[block: DESIGN_OWN_TEST]]` | The source patch ships no test. The dispatch prompt MUST embed a complete test body (and `.reference`) for the worker to implement verbatim. |
| `[[block: TWO_SITE_DRIFT_NOTE]]` | The patch touches two or more files where one site's textual drift matters and another's doesn't (T3.4 case: Site 1 clean, Site 2 trailing-context drift). Worker should verify each site independently. |
| `[[block: DRIFT_SUPERSEDED_CHECK]]` | The parent's preflight suspects the patch may already be in upstream 26.3 (e.g., the author is an upstream maintainer, or sibling SHAs with the same subject exist). Worker must run `git merge-base --is-ancestor <sibling-sha> v26.3.15.4-lts` and document the result. |

## Worked examples

The most-recent filled prompt for a successful dispatch:

- **T3.4 (patch 077, ships-own-test + textual-conflict-handling)**: `tmp/patch-077/dispatch-prompt.md` (861 lines). Uses: `[[STEP_2_5_TEST_RENAME]]`, `[[MULTI_OUTCOME_CHERRY_PICK]]`, `[[SHIPS_OWN_TEST]]`, `[[TWO_SITE_DRIFT_NOTE]]`, `[[DRIFT_SUPERSEDED_CHECK]]`.
- **T3.3 (patch 011, two-file C++ with style cleanup)**: `tmp/patch-011/dispatch-prompt.md` (828 lines). Uses: `[[STEP_2_5_STYLE_CLEANUP]]`, `[[DESIGN_OWN_TEST]]`.
- **T3.2 (patch 040, deletion-only single-user HTTP test)**: `tmp/patch-040/dispatch-prompt.md` (569 lines). No optional blocks; cleanest base case.

Three filled prompts cover the optional-block matrix between them; future dispatches should derive their optional-block set from the matrix above, not from one specific prior example.

---

[TEMPLATE BEGIN]

You are an Aiven LTS uplift PATCH WORKER subagent for patch `<<PATCH_NNN>>`. Your goal is to port commit `<<SOURCE_SHA>>` ("<<SUBJECT>>") from `v25.8.18.1-lts-aiven` to `v26.3.15.4-lts-aiven-dev`, write a stateless `<<TEST_FORMAT>>` test that produces an honest pre/post evidence pair, write a dossier, and return a halt-and-escalate report. You do NOT commit — the human commits after reviewing your staged state and your report.

This is dispatch `<<DISPATCH_TAG>>` — the `<<DISPATCH_ORDINAL>>` real patch dispatch.

<<NEW_DIMENSIONS_LIST>>

Parent has already performed a C++ review of this patch and made the following pre-flight conclusions you must verify (Step 1) but may treat as a strong prior:

<<PARENT_PREFLIGHT_FINDINGS>>

Parent has also made the following policy calls that you must follow (and document in the dossier — do NOT re-litigate them):

<<PARENT_POLICY_CALLS>>

Apply what worked from prior dispatches: drift-analysis BEFORE cherry-pick, single-axis worktree-flip for pre/post evidence, every dossier section filled, prefer escalation over guessing.

# Aiven invariants (mandatory — read first)

The following is the full content of `docs/aiven/AGENTS.md`. It is included here verbatim because Cursor's nested-AGENTS.md auto-load is read-triggered, not startup-time. You must obey these invariants throughout this dispatch:

---
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
---

# Patch-specific context

| Field | Value |
|---|---|
| NNN | `<<PATCH_NNN>>` |
| Source SHA | `<<SOURCE_SHA>>` |
| Source branch | `origin/v25.8.18.1-lts-aiven` |
| Subject | <<SUBJECT>> |
| Author | <<AUTHOR_NAME>> <<<AUTHOR_EMAIL>>> |
| Committer (on source branch) | <<COMMITTER_EMAIL>> |
| Files touched | <<FILES_TOUCHED_TABLE_CELL>> |
| Author date | <<AUTHOR_DATE>> |
| `cherry_pick_clean` per T2.2 | <<CHERRY_PICK_CLEAN_FLAG>> (informational only — see Step 2) |
| Proposed slug | `<<SLUG>>` |
| Dossier path | `<<DOSSIER_PATH>>` |

Full source-commit body (verbatim):

```
<<SOURCE_COMMIT_BODY>>
```

Source diff (verbatim):

```diff
<<SOURCE_DIFF>>
```

**Behavior changes:**

<<BEHAVIOR_CHANGES>>

# Required reading (before Step 0)

Read these files in full before Step 0. Their content is procedure-defining and is referenced by the steps below.

1. `docs/aiven/schema/halt-and-escalate.md` — the schema your final response MUST conform to. Read every constraint, especially constraint 6 (the `tests` block satisfaction rules).
2. `docs/aiven/runbooks/build-and-test.md` — env setup, compiler env, build commands, stateless test runner. §1, §2, §3, §4, §5 are mandatory.
3. `docs/aiven/runbooks/testing-suites.md` — test taxonomy, transport-follows-format rule, evidence-pair requirement (§5), single-axis worktree-flip technique (§6). §2, §3, §4 (Aiven test-naming convention), §5, §6 are mandatory.
4. `docs/aiven/skills/cpp-review-checklist.md` — the eight-section checklist you apply in §3 of the dossier.
5. `docs/aiven/skills/patch-dossier-template.md` — the section structure for the dossier you create in Step 7.

# Embedded procedure (follow in order — do not skip steps)

## Step 0 — Preflight

1. `git rev-parse --abbrev-ref HEAD` → expect `v26.3.15.4-lts-aiven-dev`. If not, STOP (`escalation_reason: policy_call`).
2. `git status --porcelain` → expect empty modulo known carry-overs (`?? access/` from the server leak; possibly local `M .gitignore`). If anything else, STOP (`escalation_reason: policy_call`).
3. `git cat-file -t <<SOURCE_SHA>>` → expect `commit`.
4. **Capture source author/committer** for the proposed commit message body (per source-author-preservation policy):
   ```bash
   git show --no-patch \
     --format='Author: %an <%ae>%nDate: %ad%nCommitter: %cn <%ce>' \
     --date=short \
     <<SOURCE_SHA>> \
     | tee tmp/patch-<<PATCH_NNN>>/source-author.txt
   ```
   Surface verbatim in Step 9's report. The line `Original author: <name> <email>, <date>.` MUST appear in your `proposed_commit.commit_message` body, immediately after the subject and a blank line. This applies whether or not the source author matches the local human.
5. `mkdir -p tmp/patch-<<PATCH_NNN>>` — already done by parent; verify.
6. Capture dispatch start time: `date -u +%Y-%m-%dT%H:%M:%SZ | tee tmp/patch-<<PATCH_NNN>>/dispatch-start.txt`.

## Step 1 — Upstream-drift analysis (MANDATORY before cherry-pick)

Per T3.1 retrospective: a clean cherry-pick is necessary but NOT sufficient. The drift must be analyzed even when the parent's preflight predicts a clean outcome — the worker re-verifies independently.

### 1a. Identifier inventory

<<IDENTIFIER_INVENTORY>>

### 1b. Grep each identifier on current HEAD

```bash
<<IDENTIFIER_GREP_BASH>>
```

Each identifier MUST be PRESENT on HEAD (count > 0 in the relevant subtree). If any is `0`, investigate (renamed? removed?).

### 1c. File-history scan

```bash
<<DRIFT_FILE_HISTORY_BASH>>
```

Look for any commit that touches the call sites the patch modifies. Most upstream commits should be unrelated (formatting, includes, neighboring code).

### 1d. Upstream-equivalent search

Did upstream introduce an equivalent change between LTSes?

```bash
<<UPSTREAM_EQUIVALENT_GREP>>
```

If upstream merged an equivalent change, the patch is `obsoleted-by-upstream`.

[[block: DRIFT_SUPERSEDED_CHECK
Also check whether sibling SHAs with the same subject are already ancestors of `v26.3.15.4-lts`:

```bash
for sha in <<SIBLING_SHAS>>; do
  git merge-base --is-ancestor "$sha" v26.3.15.4-lts 2>/dev/null \
    && echo "$sha IS in 26.3 (drift-superseded)" \
    || echo "$sha NOT in 26.3"
done | tee tmp/patch-<<PATCH_NNN>>/drift-superseded-check.log
```

If any sibling is an ancestor of 26.3, STOP with `outcome: drift-superseded` and document the matching commit's SHA + author + date in the conclusion.
]]

### 1e. Hunk-context verification

Confirm the lines the patch modifies still exist with the expected pre-patch shape on HEAD:

<<HUNK_CONTEXT_VERIFICATION>>

Both hunks should match the pre-patch context shown in the source diff above. Line numbers may shift (normal); shape must match.

### 1f. Conclusion

Write ONE of these conclusions into `tmp/patch-<<PATCH_NNN>>/drift-conclusion.txt`:

- **`still-needed-applies-cleanly`** — identifiers present, hunk shapes match, no upstream-equivalent. Proceed to Step 2.
- **`still-needed-but-rewrite`** — semantics unchanged but the diff cannot apply verbatim (structural rewrite). Re-author manually; document the move.
- **`obsoleted-by-upstream`** — STOP; `escalation_reason: policy_call`.
- **`irrelevant-by-removal`** — STOP; `escalation_reason: policy_call`.
- **`drift-superseded`** — STOP; `escalation_reason: policy_call`. (Only if `[[DRIFT_SUPERSEDED_CHECK]]` was applicable and triggered.)

## Step 2 — Cherry-pick (Tier 1)

**Policy reminder (T3.4 Finding B):** the T2 inventory's `<<CHERRY_PICK_CLEAN_FLAG>>` flag is a forecast, not a contract. Attempt the cherry-pick regardless; only conflict markers (`UU` in `git status`) trigger the manual-resolution branch.

```bash
git cherry-pick --no-commit -x <<SOURCE_SHA>> \
    2>&1 | tee tmp/patch-<<PATCH_NNN>>/cherrypick.log
git status | tee -a tmp/patch-<<PATCH_NNN>>/cherrypick.log
```

<<CHERRY_PICK_EXPECTED_OUTCOME>>

[[block: MULTI_OUTCOME_CHERRY_PICK
Multiple possible outcomes:

### Outcome A — cherry-pick is clean (auto-merge tolerated any drift)

Exit 0, no `UU` markers, all expected files staged. Verify the staged diff matches the source diff (modulo line-number shifts):

```bash
for f in <<PATCHED_FILES>>; do
  git diff --cached -- "$f" | tee "tmp/patch-<<PATCH_NNN>>/staged-$(basename "$f").log"
done
```

Proceed to Step 2.5 (if any) or Step 3.

### Outcome B — cherry-pick conflicts at <<EXPECTED_CONFLICT_SITE>>

`git status` shows `UU <<EXPECTED_CONFLICT_FILE>>`. Open the file; resolve by hand per parent's policy call. Then:

```bash
git add <<EXPECTED_CONFLICT_FILE>>
git status | tee -a tmp/patch-<<PATCH_NNN>>/cherrypick.log
```

Verify the resolved file has the expected change AND preserves any new context lines from the drift. Proceed to Step 2.5 (if any) or Step 3.

### Outcome C — cherry-pick conflicts at an unexpected site

If conflict markers appear in a file the parent's preflight didn't predict, STOP with `escalation_reason: drift_beyond_parent_preflight`. Do NOT attempt to resolve a multi-site conflict on your own.
]]

Tier 1 result: `pass` if cherry-pick is clean or resolved per the expected outcome.

[[block: STEP_2_5_STYLE_CLEANUP
## Step 2.5 — Apply style cleanup (parent-instructed)

Per parent policy call: <<STYLE_CLEANUP_RATIONALE>>.

**Scope: ONLY <<STYLE_CLEANUP_SCOPE>>.** Do NOT touch any other code.

<<STYLE_CLEANUP_INSTRUCTIONS>>

After your edit, verify:

```bash
git diff --cached -- <<STYLE_CLEANUP_FILE>> | tee tmp/patch-<<PATCH_NNN>>/style-cleanup-staged.log
git diff -- <<STYLE_CLEANUP_FILE>> | tee tmp/patch-<<PATCH_NNN>>/style-cleanup-worktree-diff.log
# Worktree-vs-index MUST be EMPTY.
```

Update `tmp/patch-<<PATCH_NNN>>/drift-conclusion.txt` to mark the final conclusion as `still-needed-but-rewrite` (because of this cleanup).
]]

[[block: STEP_2_5_TEST_RENAME
## Step 2.5 — Rename the shipped test per the Aiven convention (parent-instructed)

Per parent policy call: the source patch ships its own test under `<<SHIPPED_TEST_OLD_PREFIX>>_<<SLUG_AS_SHIPPED>>.{sql,sh,reference}`. Per `docs/aiven/runbooks/testing-suites.md` §4.1 (Aiven test-naming convention), Aiven-authored tests use `9<NNN>_<slug>` — for this patch: `9<<PATCH_NNN>>_<<SLUG>>.{sql,sh,reference}`.

Rationale (record in dossier §6): the test is Aiven-only (no trace in upstream 26.3 — parent verified). Keeping the old prefix would (a) conflict with our own convention and (b) silently collide if anyone submits the test upstream under a different number.

Execute the rename using `git mv` so it travels as a rename, not a delete + add:

```bash
cd tests/queries/0_stateless
git mv <<SHIPPED_TEST_OLD_PREFIX>>_<<SLUG_AS_SHIPPED>>.<<TEST_EXT>>       9<<PATCH_NNN>>_<<SLUG>>.<<TEST_EXT>>
git mv <<SHIPPED_TEST_OLD_PREFIX>>_<<SLUG_AS_SHIPPED>>.reference 9<<PATCH_NNN>>_<<SLUG>>.reference
cd ../../..
git status | tee tmp/patch-<<PATCH_NNN>>/rename.log
```

Verify the new files exist and the old ones are gone:

```bash
ls -la tests/queries/0_stateless/9<<PATCH_NNN>>_<<SLUG>>.{<<TEST_EXT>>,reference}
test ! -e tests/queries/0_stateless/<<SHIPPED_TEST_OLD_PREFIX>>_<<SLUG_AS_SHIPPED>>.<<TEST_EXT>>
```
]]

[[block: STEP_2_5_OTHER
## Step 2.5 — <<CUSTOM_CLEANUP_TITLE>>

Per parent policy call: <<CUSTOM_CLEANUP_RATIONALE>>.

<<CUSTOM_CLEANUP_INSTRUCTIONS>>

After the cleanup, verify the staged state:

```bash
git diff --cached --stat | tee tmp/patch-<<PATCH_NNN>>/cleanup-staged.log
```
]]

## Step 3 — Patch-id verification (Tier 2)

```bash
git show <<SOURCE_SHA>> | git patch-id --stable \
    | tee tmp/patch-<<PATCH_NNN>>/patch-id-source.log
git diff --cached | git patch-id --stable \
    | tee tmp/patch-<<PATCH_NNN>>/patch-id-staged.log
```

<<PATCH_ID_EXPECTATION>>

Run the decomposition check (per `docs/aiven/schema/halt-and-escalate.md`) to confirm any difference is structural (rename, context drift) and NOT semantic:

```bash
diff <(git show <<SOURCE_SHA>>) <(git diff --cached) \
    | grep -E '^[-+]' | grep -v '^[-+]\{3\}' \
    | tee tmp/patch-<<PATCH_NNN>>/decomposition.log
```

If the decomposition shows any semantic difference (a different identifier swap, a removed line beyond what the source removes, an added line beyond what the source adds), STOP with `escalation_reason: semantic_conflict`.

## Step 4 — Build (Tier 3a)

Per `docs/aiven/runbooks/build-and-test.md` §1, §2, §3:

```bash
export PATH="/opt/llvm-21/bin:$PATH"
export CC=/opt/llvm-21/bin/clang
export CXX=/opt/llvm-21/bin/clang++
$CC --version | head -1   # expect clang version 21.x

date -u +%Y-%m-%dT%H:%M:%SZ | tee tmp/patch-<<PATCH_NNN>>/build-start.txt
ninja -C build clickhouse 2>&1 | tee tmp/patch-<<PATCH_NNN>>/build-postpatch.log
echo "ninja exit: ${PIPESTATUS[0]}" | tee -a tmp/patch-<<PATCH_NNN>>/build-postpatch.log
date -u +%Y-%m-%dT%H:%M:%SZ | tee tmp/patch-<<PATCH_NNN>>/build-end.txt
```

Capture cold/warm cache state per T3.2 Finding C:

- Warm cache: first line `[1/1] Linking CXX executable ...`, completes in <60 s.
- Cold cache: first line `[N/many] Building CXX object ...` with N near "many", takes >5 min.

Record in `tmp/patch-<<PATCH_NNN>>/build-cache-state.txt`.

Tier 3a result: `pass` if exit 0. `fail` with `escalation_reason: build_fail_api_rename` if a renamed symbol is named in the failure; otherwise `fail` with `escalation_reason: other`.

## Step 5 — Test design

[[block: SHIPS_OWN_TEST
The source patch SHIPS its own test. Your job:

1. Confirm the renamed test content is byte-equivalent to the source's (verified in Step 3 if `[[STEP_2_5_TEST_RENAME]]` ran).
2. Read the shipped test once and understand what it does.
3. Note any concerns in the dossier (don't modify the test unless a real defect is identified).

The shipped test body (already in the staged set; re-render here for the dossier §4 evidence):

```<<TEST_EXT>>
<<SHIPPED_TEST_BODY>>
```

Reference:

```
<<SHIPPED_TEST_REFERENCE>>
```

**Why this distinguishes pre-patch from post-patch** (record verbatim in dossier §4): <<TEST_RATIONALE>>.

**Known coverage limitations** (record in dossier §4): <<TEST_COVERAGE_GAPS>>.

The test files are already staged. No additional `git add` needed for the test in this step.
]]

[[block: DESIGN_OWN_TEST
The source patch ships NO test. You implement the design parent specifies below VERBATIM — do NOT redesign.

**Format**: `<<TEST_FORMAT>>`. Pattern reference: <<TEST_PATTERN_REFERENCE>>.

**Slug**: `<<SLUG>>`.

**Numbering**: per Aiven `docs/aiven/runbooks/testing-suites.md` §4.1, allocate prefix `9<<PATCH_NNN>>`. Do NOT use upstream's `add-test` allocator.

**Body** (implement exactly — copy/paste, then verify):

```<<TEST_EXT>>
<<DESIGNED_TEST_BODY>>
```

**`.reference` content** (exactly the lines below, final newline):

```
<<DESIGNED_TEST_REFERENCE>>
```

**Why this distinguishes pre-patch from post-patch** (record verbatim in dossier §4): <<TEST_RATIONALE>>.

**Execute:**

```bash
TEST_SH="tests/queries/0_stateless/9<<PATCH_NNN>>_<<SLUG>>.<<TEST_EXT>>"
TEST_REF="tests/queries/0_stateless/9<<PATCH_NNN>>_<<SLUG>>.reference"
# Use Write to create both files with the body and reference above; do NOT use add-test.
git add "$TEST_SH" "$TEST_REF"
```

**Tags**: none. Do NOT add `no-*` tags unless a real reason emerges in Step 6.
]]

## Step 6 — Test execution with pre/post evidence pair (Tier 3b)

Use the **single-axis worktree-flip technique** from `docs/aiven/runbooks/testing-suites.md` §6. Do NOT use `git stash`, `git checkout`, `git switch`, `git apply -R --cached`, or any other technique that touches HEAD, the index, or both worktree+index at once.

`$PATCHED_FILES` for this patch = **<<PATCHED_FILES_COUNT_NAMED>>** file(s):

- <<PATCHED_FILES_LIST_BULLETS>>

The new/renamed test files are NOT in `$PATCHED_FILES` and MUST NOT be flipped.

Derive once and reuse:

```bash
PATCHED_FILES="<<PATCHED_FILES>>"
```

### 6a. Post-patch evidence (test PASSES)

Server is required for stateless tests; start it per `docs/aiven/runbooks/build-and-test.md` §4 if not running. Then:

```bash
export PATH="$PWD/build/programs:$PATH"

CLICKHOUSE_PORT_TCP=9000 CLICKHOUSE_PORT_HTTP=8123 \
  ./tests/clickhouse-test \
    --no-random-settings --no-random-merge-tree-settings \
    --no-stateful --no-shard --no-zookeeper --no-long \
    "9<<PATCH_NNN>>_<<SLUG>>" \
    2>&1 | tee tmp/patch-<<PATCH_NNN>>/test-postpatch.log
```

Expect: `9<<PATCH_NNN>>_<<SLUG>>: OK`. If FAIL, STOP with `escalation_reason: test_fail_ambiguous`.

### 6b. Flip worktree to pre-patch

```bash
git restore --worktree --source=HEAD $PATCHED_FILES

git diff --cached --stat -- $PATCHED_FILES | tee tmp/patch-<<PATCH_NNN>>/flip-pre-verify.log
git diff -- $PATCHED_FILES | head -80 | tee -a tmp/patch-<<PATCH_NNN>>/flip-pre-verify.log
```

### 6c. Incremental rebuild (pre-patch binary)

```bash
ninja -C build clickhouse 2>&1 | tee tmp/patch-<<PATCH_NNN>>/build-prepatch.log
echo "ninja exit: ${PIPESTATUS[0]}" | tee -a tmp/patch-<<PATCH_NNN>>/build-prepatch.log
```

If this build fails, STOP and FLIP BACK before reporting (see §6e).

### 6d. Pre-patch evidence (test FAILS)

Stop and restart the server so it picks up the rebuilt binary. Then:

```bash
# (restart server per build-and-test.md §4)

export PATH="$PWD/build/programs:$PATH"
CLICKHOUSE_PORT_TCP=9000 CLICKHOUSE_PORT_HTTP=8123 \
  ./tests/clickhouse-test \
    --no-random-settings --no-random-merge-tree-settings \
    --no-stateful --no-shard --no-zookeeper --no-long \
    "9<<PATCH_NNN>>_<<SLUG>>" \
    2>&1 | tee tmp/patch-<<PATCH_NNN>>/test-prepatch.log
```

Expect: `9<<PATCH_NNN>>_<<SLUG>>: FAIL`. The diff should show <<TEST_PRE_PATCH_EXPECTED_FAILURE>>.

If the pre-patch test PASSES, STOP with `escalation_reason: test_fail_ambiguous`. Common confounders to investigate before escalating:

- Server is still using the post-patch binary (forgot to restart between 6c and 6d).
- <<TEST_PRE_PATCH_CONFOUNDERS>>

### 6e. Flip worktree back to post-patch (UNCONDITIONAL)

```bash
git restore --worktree $PATCHED_FILES

git diff -- $PATCHED_FILES | tee tmp/patch-<<PATCH_NNN>>/flip-post-verify.log
# Should be EMPTY (worktree == index).
git diff --cached --stat -- $PATCHED_FILES | tee -a tmp/patch-<<PATCH_NNN>>/flip-post-verify.log
```

If `git diff -- $PATCHED_FILES` is non-empty after the flip-back, your state is corrupted — STOP and report immediately with `escalation_reason: other`, do NOT attempt further recovery.

### 6f. Rebuild post-patch binary

```bash
ninja -C build clickhouse 2>&1 | tail -10 | tee tmp/patch-<<PATCH_NNN>>/build-postpatch-restore.log
echo "ninja exit: ${PIPESTATUS[0]}" | tee -a tmp/patch-<<PATCH_NNN>>/build-postpatch-restore.log
```

Tier 3b result: `pass` only if 6a `OK`, 6d `FAIL` with the expected failure diff, 6e clean.

## Step 7 — Author the dossier

Create `<<DOSSIER_PATH>>` per `docs/aiven/skills/patch-dossier-template.md`. Fill EVERY section.

Section-specific notes for this dispatch:

<<DOSSIER_SECTION_NOTES>>

Stage the dossier:

```bash
git add <<DOSSIER_PATH>>
```

## Step 8 — Verify final staged state

```bash
git status                                  | tee tmp/patch-<<PATCH_NNN>>/final-status.log
git diff --cached --find-renames=100 --stat | tee -a tmp/patch-<<PATCH_NNN>>/final-status.log
git diff -- $PATCHED_FILES                  | tee tmp/patch-<<PATCH_NNN>>/final-worktree-diff.log
```

Expected staged file set for `outcome: success` (<<EXPECTED_STAGED_FILE_COUNT_NAMED>> entries):

<<EXPECTED_STAGED_FILE_LIST>>

`final-worktree-diff.log` MUST be EMPTY. If non-empty, STOP and report — the worktree-flip postcondition was not satisfied.

## Step 9 — Halt-and-escalate report

Return your final response per `docs/aiven/schema/halt-and-escalate.md`. Populate every field:

- `outcome: success` (assuming all tiers pass) or `escalate`.
- `patch_slug: <<SLUG>>`
- `source_sha: <<SOURCE_SHA>>`
- `proposed_commit.staged_files`: the entries listed in Step 8.
- `proposed_commit.commit_message`: the subject `patch-port(<<PATCH_NNN>>): <verbatim source-commit subject>` (per `commit-hygiene.md §1(C)`), followed by:
  - An empty line.
  - The line `Original author: <<AUTHOR_NAME>> <<<AUTHOR_EMAIL>>>, <<AUTHOR_DATE>>.` (UNCONDITIONALLY — the human's commit message picks this up; we do not use the `--author=` git flag).
  - An empty line.
  - The `(cherry picked from commit <<SOURCE_SHA>>)` provenance line (auto-added by `cherry-pick -x`; verify it's in the commit message).
  - Any optional context lines (test rename note, style cleanup note, conflict resolution narrative — be specific).
- `proposed_commit.byte_equivalent`: <<BYTE_EQUIVALENT_EXPECTATION>>.
- `tests.added: yes`
- `tests.kind: <<TEST_KIND>>` (`stateless` for `.sh`/`.sql`, `integration` for cluster-level)
- `tests.paths`: the test files.
- `tests.upstream_reference`: empty.
- `tests.pre_patch_fail_verified: true`
- `tests.post_patch_pass_verified: true`
- `tests.justification`: empty.
- `escalation_reason: none` (or appropriate enum).

Tier-results lines (success case):

<<TIER_RESULTS>>

Evidence section MUST include verbatim excerpts (max ~30 lines each, total ~120 lines) from:

<<EVIDENCE_FILE_LIST>>

# Hard constraints (compliance-critical)

1. **No `git commit`, no `git push`, no `git rebase`, no `git reset --hard`, no `git cherry-pick` without `--no-commit`.** Hooks deny these. If you observe a deny, STOP and report.
2. **No `git stash`.** Pre/post evidence comes from the worktree-flip in testing-suites §6.
3. **No `git checkout <sha|branch>`, no `git switch`, no `git restore --staged`, no `git checkout HEAD -- <file>`.** Only `git restore --worktree ...`, `git restore --worktree --source=HEAD ...`, and `git mv` (for Step 2.5 renames if applicable) are allowed.
3b. **No branch creation or switching: no `git checkout -b`, no `git switch -c`, no `git branch <name>`, no `git push`.** You stage in place on the current `*-aiven-dev` branch; the human commits there. Do NOT create a feature branch.
4. **No modifications to files matching the never-touch list** (`.claude/**`, root `AGENTS.md`, `.github/workflows/**`, `contrib/**`, `CONTRIBUTING.md`). Hooks deny these.
5. **`tmp/patch-<<PATCH_NNN>>/` is the only scratch directory.** Do NOT use `/tmp/`.
6. **All command outputs go to log files** under `tmp/patch-<<PATCH_NNN>>/`. Quote relevant excerpts (~30 lines max per quote) in your Evidence section.
7. **Unconditional flip-back.** Step 6e MUST be executed before halting, even on error paths.
8. **Maximum runtime: <<RUNTIME_BUDGET_MINUTES>> minutes wall.** Budget includes: three ClickHouse builds (1 full post-patch + 1 incremental pre-patch + 1 incremental post-patch restore), two test runs, drift analysis, dossier authoring. If at <<RUNTIME_BUDGET_MINUTES - 10>> minutes you are not converging, STOP with `escalation_reason: other`.
9. **`$PATCHED_FILES` is a closed set of <<PATCHED_FILES_COUNT_NAMED>> file(s)** — derive once at the start of Step 6. Use the same variable for both flips. Never glob; never iterate the diff between flips. New/renamed test files are NOT in `$PATCHED_FILES`.
10. **Original-author preservation goes in the commit MESSAGE BODY, not via `git commit --author=`.** Always emit the `Original author: ...` line in your `proposed_commit.commit_message`, regardless of who the source author is.
11. **Prefer escalation over guessing.** Per AGENTS.md §6, ambiguity is a STOP condition. The system learns from your escalations.
12. <<EXTRA_HARD_CONSTRAINTS>>

# Suggested execution order summary

0. **Read required files** (schema, build-and-test runbook, testing-suites runbook §2/§3/§4/§5/§6, cpp-review-checklist, patch-dossier-template).
1. Step 0 — Preflight (HEAD check, clean tree check, source SHA exists, author capture, dispatch start time).
2. Step 1 — Upstream-drift analysis (re-verify parent's pre-flight findings independently).
3. Step 2 — Cherry-pick `--no-commit -x`; handle the expected outcome.
4. Step 2.5 (if applicable) — Style cleanup / test rename / other parent-instructed adjustment.
5. Step 3 — Patch-id verification; decomposition; record `byte_equivalent`.
6. Step 4 — Build post-patch; record cold/warm cache state.
7. Step 5 — Test design: either adopt the shipped test or implement the design verbatim.
8. Step 6 — Pre/post evidence via worktree flip with `$PATCHED_FILES`; verify Tier 3b.
9. Step 7 — Dossier authoring (every section filled).
10. Step 8 — Verify staged state matches the expected file set.
11. Step 9 — Return halt-and-escalate report; commit message body MUST include the `Original author:` line.

Total LOC of source change: <<LOC_CHANGED>>. Time budget is dominated by Steps 4, 6c, 6f (three ClickHouse builds — one full and two incrementals) plus drift analysis and dossier authoring.

Good luck. Surface what you find — the system improves from this dispatch.

[TEMPLATE END]

## Maintenance log

| Date | Change | Rationale |
|---|---|---|
| 2026-05-25 | Initial extraction from T3.4's filled prompt (`tmp/patch-077/dispatch-prompt.md`). Rule-of-three trigger met. | T3.4 retrospective Finding D |
| 2026-05-25 | Source-author preservation moved from `git commit --author=` flag to a line in the commit message body. Always emitted, never special-cased. | T3.4 retrospective Finding C: `--author=` was forgotten at human-commit time, Joe Lynch's authorship was lost in `e80c209ade8`. The body-line approach survives review and grep regardless of how the commit is invoked. |
| 2026-05-25 | `cherry_pick_clean` flag is forecast-only; worker always attempts `git cherry-pick --no-commit` regardless. Only `UU` markers trigger manual resolution. | T3.4 retrospective Finding B: the T2 strict-context check rejects what `git`'s three-way merge tolerates. Two observations (T3.3 clean / T3.4 clean-despite-no-flag); third would firm up the rule but the policy is safe to adopt now. |
| 2026-05-25 | Hard constraint #10 added — explicit "commit body, not `--author=`". | Reinforces Finding C policy at the constraint level (workers re-read this section more carefully than the prose). |
| 2026-05-25 | Optional blocks for Step 2.5 (style cleanup / test rename / other) + multi-outcome cherry-pick narrative + ships-own-test vs design-own-test + two-site drift + drift-superseded check. | Each block reflects a real T3.X case (T3.3 style cleanup, T3.4 test rename + ships-own-test, T3.4 two-site, T3.4-considered-drift-superseded). |
| 2026-05-26 | Added placeholder reference rows for `<<PATCH_ID_EXPECTATION>>` and `<<BYTE_EQUIVALENT_EXPECTATION>>` (they were used at lines 492 and 729 but absent from the table). Default expectation for both flipped from "false" (over-conservative pre-T3.5) to "true" (the typical outcome of any clean cherry-pick). | T3.5 retrospective Finding E: `git patch-id --stable` normalizes line numbers and `index` blob hashes, so `byte_equivalent: true` is achievable even for patches landing in files that grew ~850 lines between LTSes. T3.5 was the first dispatch to measure `true` empirically; previous dispatches over-conservatively wrote `false`. |
| 2026-05-28 | Added "Parent preflight discipline — (i)/(ii)/(iii)/(iv)" section with the four-clause checklist and four-state outcome classifier. (i)/(ii)/(iii) codified as VERIFIED (n=8 of proactive use); (iv) reachability proof codified as PROVISIONAL (n=2 of proactive use). | T3.7 Finding A introduced (i)/(ii)/(iii) as mental-discipline-only with codification deferred to "Bootstrap commit on second occurrence"; subsequent dispatches T3.8-T3.15 all applied it, well past rule-of-three. T3.13 escalation `test_design_blocked` (retro 12) crystallized (iv); T3.14 + T3.15 proactive applications (retro 13) bring counter to 2/3 of PROVISIONAL. Phase C of the packaging plan; see retros 09-13 for the empirical record. |
| 2026-05-28 | `proposed_commit.commit_message` subject MUST now carry the `patch-port(<<PATCH_NNN>>):` prefix (drops use `patch-drop(<<PATCH_NNN>>):`). Updated the body template and the Step 9 field doc. | The pre-existing implicit "patches are the untyped commits" rule was fragile and inconsistently applied (`Port patch 006:` vs bare subjects vs `drop patch 007`), making patch commits hard to identify in a noisy log. A subject prefix is visible in `git log --oneline` and greppable via `^patch-`. Forward-only; existing 26.3 category-C commits are not rewritten. Canonical policy: `commit-hygiene.md §1(C)`. |
| 2026-05-31 | Added clause (vi) — submodule fork-redirect — to the preflight discipline, a new `external_dependency` escalation reason in `halt-and-escalate.md`, and a matching `.gitmodules` invariant in `AGENTS.md` (and its embedded copy here). | Anticipatory (0 of 3), ahead of patches that re-point a vendored submodule (e.g. `contrib/aws`) to an Aiven fork. Such a fork carries an upstream SDK bump plus ~2 Aiven patches and must be prepared outside this checkout before `.gitmodules` can point at a real, buildable ref; the worker must halt and report which submodule / which version / which patches rather than pin a not-yet-existing ref. Orthogonal to (i)–(iv). |
| 2026-06-02 | Added "Branch & handover policy" section + hard constraint 3b (no branch creation/switch/push). | The patch-015 dispatch used a bespoke prompt (not this template) that told the worker to "create a working branch", producing `patch-port-015-…` and a needless `merge --ff-only` handover step. This contradicts AGENTS.md §2/§5 (work in place on `*-aiven-dev`, human commits there). Codified that the worker stages in place and never creates/switches/pushes branches; true isolation (parallel workers) would use a git worktree, not a branch. Corrective: always dispatch through this template so Step 0 + hard constraints keep the worker in place. |
| 2026-06-02 | Clause (vi) + AGENTS §3 `.gitmodules` note now point to the new `docs/aiven/runbooks/submodule-forks.md`. | After preparing the `contrib/aws` (015) and `contrib/azure` (016) forks ad-hoc, the discovery query found a third fork-coupled patch (021, `contrib/mariadb-connector-c`) and that each fork stacks two Aiven commits. Codified the read-only discovery query + human prep recipe + per-uplift registry so all forks are batch-prepared at bootstrap (no mid-dispatch `external_dependency` escalation). Agent stays read-only re: fork repos. |
| 2026-06-10 | Added an "add-new-TU blind spot" note to clause (iii): when a patch adds a translation unit, "framework present" is insufficient — a backported new file bakes in base-version API/typedef conventions that a clean cherry-pick can't reveal. Parent must extend (iii) to a compile premise (check `ASTPtr` def, `make_shared<AST>` vs `make_intrusive<AST>`, cross-module method arity); expect `still-needed-but-rewrite` + `byte_equivalent: false`. | T3.17 patch 054 (first add-new-TU port): the parent's (iii) verdict ("framework present, PASS-SEMANTIC") was right about the framework but missed three 26.3 API restructures inside the new `KeeperMapSettings.{cpp,h}` (`ASTPtr` shared→intrusive, `make_shared<ASTSetQuery>`→`make_intrusive`, `IDatabase::alterTable` 4th arg), which surfaced only at build time and cost an escalation round-trip. Worker correctly stopped at the authorization boundary; parent verified + authorized the three bounded fixes. |

## Pointers

- `docs/aiven/AGENTS.md` — embedded verbatim in the template (above).
- `docs/aiven/schema/halt-and-escalate.md` — worker's exit-report schema.
- `docs/aiven/runbooks/build-and-test.md` — build env, runner, common breakage.
- `docs/aiven/runbooks/testing-suites.md` — test taxonomy, transport rule, worktree-flip technique, Aiven naming convention (§4.1).
- `docs/aiven/runbooks/commit-hygiene.md` — commit categorization (Bootstrap / per-uplift / patch-port).
- `docs/aiven/skills/cpp-review-checklist.md` — parent's preflight checklist.
- `docs/aiven/skills/patch-dossier-template.md` — dossier section structure.
- `docs/aiven/proposals/2026-05-19-uplift-orchestration-design.md` — overall design spec (background).
- Worked examples: `tmp/patch-040/dispatch-prompt.md` (T3.2), `tmp/patch-011/dispatch-prompt.md` (T3.3), `tmp/patch-077/dispatch-prompt.md` (T3.4).

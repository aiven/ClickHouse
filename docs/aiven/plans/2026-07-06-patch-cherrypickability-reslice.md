# Plan — make the 26.3 Aiven patch stack cherry-pickable (reslice + PR delivery)

> Status: **DRAFT / for human approval.** No code, commits, or pushes have been made
> for this plan. This document only defines the strategy. Author: principal-engineer
> research pass, 2026-07-06.
>
> Durable policy this plan executes: [`runbooks/commit-hygiene.md`](../runbooks/commit-hygiene.md)
> §4 (the three-bucket end-state, decision 2026-06-12). This plan is the concrete
> 26.3 execution instance of that policy, plus the two things the policy does not
> yet cover: the **production-hash immutability constraint** and the **group-wise
> PR delivery** into `v26.3.15.4-lts-aiven`.

## 1. Goal and constraints

**Goal.** Restructure the 26.3 Aiven patch stack so that each shipped patch is a
**self-contained, cherry-pickable, code-only commit** (`src/**` + `tests/**`), with
all documentation (dossiers, inventory, work-logs, retrospectives) separated out.
This removes the docs-conflict noise that currently makes every future minor/patch
backport a merge fight, and keeps `git patch-id` byte-equivalence checks clean.

**Hard constraints:**

1. **Production-hash immutability.** The current tip of `v26.3.15.4-lts-aiven-dev`
   (`dc0065278d1` at time of writing) is the hash deployed from the dev channel in
   production. It **must not be rewritten, rebased, amended, or force-moved.** All
   reslice work happens on a *new* branch; the original dev branch is frozen.
2. **No rebase/amend of the shipped line** beyond the single documented reslice event
   (`AGENTS.md` §4 / `commit-hygiene.md` §4). The reslice is done by *partitioning the
   final tree*, not by replaying/rewriting the production branch.
3. **Nothing lands without proof of equivalence.** The refactored tree must be
   byte-identical to the production tip (`git diff` empty), and the Aiven test suite
   must show an identical pass/fail profile before and after (§8).
4. **Delivery is incremental via PR** into `v26.3.15.4-lts-aiven` (which today sits
   exactly at the clean base `v26.3.15.4-lts`, 0 Aiven commits — see §2), reviewed in
   coherent groups, not as one 90-commit dump.
5. Do not push or change any upstream/shared branch until each step is approved.

## 2. Current state (measured 2026-07-06)

Base tag: `v26.3.15.4-lts`. Branch topology:

| Branch | Position |
|---|---|
| `v26.3.15.4-lts-aiven` (PR target) | **at base** — 0 Aiven commits |
| `v26.3.15.4-lts-aiven-dev` (production, frozen) | base + **93 Aiven commits** |

Commit subject-prefix histogram (93 commits): `patch-port` 68, `docs` 10,
`patch-drop` 7, `patch-new` 2, `patch-fix` 2, `patch` 2, `fix` 1, `chore` 1.

**The problem, quantified — how code and docs are mixed *within* commits:**

| Category | Count | Meaning |
|---|---|---|
| **CODE+DOCS** | **71** | bundle `src`/`tests` **with** `docs/aiven/**` → the cherry-pick hazard |
| DOCS-ONLY | 16 | bootstrap-rush docs, work-logs, drop records |
| CODE-ONLY | 4 | 2 `patch-fix`, 1 `patch-port`(076, dossier landed elsewhere), 1 contrib pin `fix` |
| DOCS+OTHER | 2 | large bootstrap doc commits (`.cursor/` + docs) |

Every one of the 71 CODE+DOCS commits touches `docs/aiven/patches/**` (the dossier),
and **35 of them also touch `docs/aiven/uplifts/26.3/**`** — chiefly the single shared
file `inventory.md`. That shared file, edited by 35 commits, is a **guaranteed
rebase/cherry-pick conflict magnet**: any cherry-pick of one patch drags an
`inventory.md` hunk that collides with every other.

**Secondary problem — follow-up fixes and re-ports create multiple commits per patch.**
The dev branch accreted "fixes on top of ports". These must collapse to **one code
commit per patch** in the end-state:

| Patch | Commits on dev | Collapse action |
|---|---|---|
| 004 | `6271d250d92` port + `93ed39fee91` regate | squash → one `patch-port(004)` |
| 021 | `ea643bf3730` port + `703fb970098` restore MySQL NC | squash → one `patch-port(021)` |
| 022 | `88d81a8d69b` port + `2861f3b2de6` bad-cast fix | squash → one `patch-port(022)` |
| 050 | `4e47883345f` port + `16009a9da4e` `patch-fix` race | squash → one `patch-port(050)` |
| 059 | `d10bc819885` port + `dc0065278d1` `patch-fix` TLS reload | squash → one `patch-port(059)` |
| 058 | `1592a945ce2` drop (docs) + `ec7c6d26501` re-port | re-port wins; drop record → docs bucket |
| 009 | `0ea685865b4` drop (docs) + `47b11321a9d` re-port | re-port wins; drop record → docs bucket |
| 028 | `725088d1446` drop (docs) + `cef680c16cf` re-port | re-port wins; drop record → docs bucket |
| 019 | `f1251b8826a` (avnadmin DDL) + `a5e57a7d8ca` (fresh query_id) | **verify**: two distinct changes both tagged 019 — decide 1 vs 2 code commits |

Combined-number ports already exist and stay combined: `patch-port(062,063,064)`
(`80408a34dfb`), `patch-port(066,078)` (`e3fc05941d5`). Net-new: `N01` WASM gate
(`533055d8d64`), `N02` RMV `create_if_not_exists` (`2bc3ed01264`).

**Why the collapse is essentially free.** Both reslice methods (§5) reconstruct from the
**final tree** or replay in **original order**, so a patch's follow-up fixes are already
merged into the source that ends up in its single code commit. We are not re-deriving
diffs out of order.

## 3. Target end-state (three buckets)

Per `commit-hygiene.md` §4, the refactor branch's history becomes exactly:

1. **Bootstrap commit(s) — category A.** All of
   `docs/aiven/{AGENTS.md,schema,skills,runbooks,proposals,plans}` + `.cursor/`.
   Cleanly forward-carried to the next LTS. (May stay as the existing 1–2 bootstrap
   commits rather than being re-collapsed — see §5 note.)
2. **N pure-code commits — one per shipped patch.** Touch **only** `src/**` +
   `tests/**`. Subject `patch-port(<NNN>):` (ported) or `patch-new(N<nn>):` (net-new).
   **These are the cherry-pickable units.** The `patch-*` marker MUST live here
   (`git log --grep` and `git patch-id` key on it). No docs.
3. **One consolidated docs commit — categories B + C-docs.** All
   `docs/aiven/patches/**` dossiers (ports + drops + net-new) and all
   `docs/aiven/uplifts/26.3/**` (inventory with every annotation, work-logs,
   retrospectives, screenings, reports).

**Consequences (already ratified in §4 of the runbook):**

- `patch-drop(<NNN>)` is **no longer a commit** — a drop has no code, so it lives only
  as its dossier + inventory row in bucket 3. Drops become discoverable via the
  inventory/dossiers, not `git log --grep '^patch-drop('`.
- Net-new patches split: code → bucket 2, dossier + inventory row → bucket 3.

## 4. Branch strategy (respecting the production hash)

```
v26.3.15.4-lts  (upstream base)
  |
  |-- v26.3.15.4-lts-aiven         <- PR TARGET (currently == base, empty)
  |
  |-- v26.3.15.4-lts-aiven-dev     <- PRODUCTION, FROZEN (tip = prod hash)
  |        (93 mixed commits; snapshot as tag archive/v26.3.15.4-aiven-dev-prereslice)
  |
  \-- v26.3.15.4-lts-aiven-reslice <- NEW: built fresh from base, the clean 3-bucket history
```

Steps (commands are executed by the human):

1. **Tag the exact deployed hash** (traceability / rollback anchor, never deleted):
   `git tag archive/v26.3.15.4-lts-aiven-dev-prodhash <prod-sha>`
   where `<prod-sha>` = the currently-running dev tip (measured 2026-07-06:
   `dc0065278d174743244dd5d89c5898aa705c64fa`, `patch-fix(059)`; **confirm this is the
   deployed binary before tagging** — if production was cut at an earlier commit, tag
   that instead).
2. **Land the docs-only freshening + this plan** on `-dev` as one commit. This does not
   change the deployed binary (no `src`/`tests`), so the prod hash above stays the
   authoritative artifact reference.
3. **Tag the reproduction target** (equivalence oracle for §4.4) at the new `-dev` tip:
   `git tag archive/v26.3.15.4-aiven-dev-prereslice v26.3.15.4-lts-aiven-dev`.
   This snapshot now contains the freshened docs, so the whole-tree diff can be empty.
4. **Create the reslice branch from the clean base:**
   `git switch -c v26.3.15.4-lts-aiven-reslice v26.3.15.4-lts`.
5. Build the three buckets on the reslice branch (§5); buckets 1 and 3 tree-checkout docs
   from `v26.3.15.4-lts-aiven-dev` (i.e. the freshened tip).
6. **Prove equivalence:** `git diff archive/v26.3.15.4-aiven-dev-prereslice
   v26.3.15.4-lts-aiven-reslice` must be **empty**. If non-empty, the reslice dropped or
   altered content — fix before proceeding.
7. Deliver into `v26.3.15.4-lts-aiven` group-by-group via PR (§7).

`-dev` receives exactly one docs-only commit (the freshening + plan) and is otherwise
frozen; the deployed binary is unchanged and separately tagged. No history is rewritten.

## 5. Reslice method

Two viable mechanisms; recommend **Method A** for the code buckets (preserves
per-patch commit granularity and avoids shared-file attribution guesswork) and a
tree-checkout for the docs bucket.

### Method A — replay-in-order, strip docs, squash follow-ups (recommended for bucket 2)

For each shipped patch, in the **original commit order** (so no new conflicts can
arise — the diffs already applied cleanly in this order on dev):

```bash
git cherry-pick -n <base-port-sha> [<follow-up-fix-sha> ...]   # stage code+docs of the patch and its fixes
git restore --staged docs/ && git checkout -- docs/            # drop ALL doc changes from the stage
git commit -m "patch-port(<NNN>): <verbatim subject>"          # code-only, one commit per patch
```

- Follow-up fixes (§2 table) are cherry-picked `-n` **together with** their base and
  committed once → automatic squash.
- Superseded drops (058/009/028) are simply **not** replayed as code (they had none);
  their dossier/inventory record is picked up by the docs bucket.
- Doc-only and bootstrap commits are skipped here (handled by buckets 1 and 3).

Shared-file safety: because we replay in original chronological order, a `src` file
edited by two patches accumulates edits exactly as it did on dev — no hunk-level
attribution needed, no conflicts.

### Method B — reset-to-base, re-commit the final tree (alternative)

`git reset` the working tree to the final dev tree and re-stage per bucket. Cleaner for
the **docs bucket** (`git checkout v26.3.15.4-lts-aiven-dev -- docs/aiven/ && git commit`),
but for the code bucket it requires a file→patch mapping and can't split a shared file
across two patch commits. Use Method B only for buckets 1 and 3.

### Recommended combination

- **Bucket 1 (bootstrap):** keep the existing bootstrap commit(s) as-is if already pure
  (verify with a path check); otherwise `git checkout <dev> -- docs/aiven/{AGENTS.md,
  schema,skills,runbooks,proposals,plans} .cursor/` into one commit.
- **Bucket 2 (code):** Method A, one commit per patch, follow-ups squashed.
- **Bucket 3 (docs):** `git checkout <dev> -- docs/aiven/patches docs/aiven/uplifts`
  into one commit.

Then the §4 empty-diff check certifies the union equals production.

## 6. How future commits should look

The canonical shapes (restating `commit-hygiene.md` §1 for quick reference):

```text
# bucket 2 — the cherry-pickable unit (code only, no docs)
patch-port(042): Fix ZK node leak after create delete table
patch-new(N02): keep coordinated refreshable MVs working on Apache ZooKeeper

# bucket 3 — one commit, everything documentary
docs(aiven/26.3): consolidated dossiers + inventory + work-logs
```

Rules going forward on the reslice/aiven line:
- A shipped patch = exactly **one** `patch-port(NNN)`/`patch-new(Nnn)` code commit.
  Later fixes to a not-yet-delivered patch are squashed into it (during the working
  cycle) rather than appended as `patch-fix` — `patch-fix` remains only for fixes to an
  **already-delivered** (already-PR'd) patch.
- Never touch `docs/**` in a `patch-*` code commit.
- `inventory.md` and dossiers change **only** in the docs bucket / docs commits.

## 7. Grouping for PR delivery (nice-to-have)

Deliver buckets into `v26.3.15.4-lts-aiven` as ordered PRs. Suggested order and
thematic groups (final grouping is the human's call; drops carry no code so they ride
in the docs PR):

1. **PR-0 Bootstrap** — bucket 1 (AGENTS/schema/skills/runbooks/proposals/plans +
   `.cursor/`). Prereq for everything; smallest review risk.
2. **PR-1 Object storage & backup** — 012, 013, 015, 016, 023, 024, 025, 026, 027, 028,
   018, 065, 067.
3. **PR-2 Kafka** — 029, 030, 031, 032, 033, 076.
4. **PR-3 Dictionaries & named collections** — 021, 034, 035, 036, 045, 055, 061.
5. **PR-4 Access control & security** — 011, 014, 019, 020, 022, 040, 051, 070, 077,
   079, N01.
6. **PR-5 Replication / MergeTree / Keeper** — 003, 004, 005, 006, 008, 038, 041, 042,
   047, 048, 053, 054, 056, 058, 060, 069, 072, 009, 062/063/064.
7. **PR-6 Refreshable materialized views** — 049, 050, 066/078, N02.
8. **PR-7 TLS/certs & misc/build** — 037, 039, 052, 059, 068, 071, 075, 010.
9. **PR-8 Docs** — bucket 3 (all dossiers + `uplifts/26.3/**`, including every drop
   record). Lands last so inventory annotations reference already-merged code.

Notes:
- Group boundaries are for **review coherence**, not correctness; a code commit is
  cherry-pickable regardless of its group.
- If any patch has a genuine ordering dependency (e.g. a shared header touched by two
  patches), keep those two in the same PR in original order.
- Count check: buckets 2's patch commits should total the shipped-patch count
  (`git log --grep '^patch-port(' + '^patch-new('` minus superseded-drop duplicates),
  reconciled against the inventory's shipped rows.

## 8. Baseline test snapshot (before touching anything)

**Why.** The empty-`git diff` in §4 is a *structural* proof that the reslice preserved
the tree. The test snapshot is the *behavioral* proof the human asked for — belt and
suspenders — and it also gives a known-good baseline for the per-PR verification.

**Surface.** 35 Aiven-specific integration test modules under
`tests/integration/test_aiven_*` (enumerated 2026-07-06):

```
test_aiven_azure_custom_ca_path, test_aiven_azure_signature_delegation,
test_aiven_azure_storage_prefix, test_aiven_backup_disk, test_aiven_delta_self_signed,
test_aiven_dictionary_user, test_aiven_early_fetch_pool,
test_aiven_enforce_default_replication_path, test_aiven_enforce_https_url,
test_aiven_external_db_ssl, test_aiven_https_to_http_redirect,
test_aiven_indirect_database_creation, test_aiven_lazy_certificates,
test_aiven_move_partition_to_volume_replicated, test_aiven_mv_refresh_sharded,
test_aiven_mysql_bit_overflow, test_aiven_mysql_dict_named_collection,
test_aiven_per_server_max_bytes_merge_mutate_override,
test_aiven_postgres_dict_named_collection, test_aiven_postgres_unlock,
test_aiven_prohibit_tmp_table, test_aiven_protected_roles, test_aiven_protected_users,
test_aiven_refreshable_mv_create_if_not_exists, test_aiven_refreshable_mv_shard_macro_expansion,
test_aiven_refreshable_mv_zookeeper, test_aiven_replace_mergetree_with_replicated,
test_aiven_replicated_database_attach_with_shard_macro, test_aiven_replication_queue_size_limit,
test_aiven_s3_custom_ca_path, test_aiven_s3_signature_delegation,
test_aiven_skip_azure_container_creation, test_aiven_wait_for_distributed_database_creation,
test_aiven_zero_copy_lock_race, test_aiven_zk_connect_retry
```

(No Aiven-named stateless `tests/queries` tests exist; some patches ship stateless
tests under generic names — those are covered transitively by the empty-diff proof, so
the integration suite is the explicit behavioral baseline.)

**Procedure (the user will run/create the actual runs):**

1. On the **frozen production tip** (`v26.3.15.4-lts-aiven-dev`), build once and run the
   full list, capturing machine-readable results to
   `docs/aiven/uplifts/26.3/reports/reslice-baseline-prod.jsonl` (or `.log`), one file,
   with per-test pass/fail/skip.
   ```bash
   python -m ci.praktika run "integration" --test test_aiven_azure_custom_ca_path \
     test_aiven_... > build/reslice_baseline.log 2>&1
   ```
   (Run in batches; some need external services — Postgres/MySQL/Azurite/S3-mock.)
2. Record the exact pass/fail set as the **baseline manifest**.
3. After the reslice branch is built and the §4 empty-diff passes, **re-run the same
   list on the reslice tip** and diff the manifests. Expectation: **identical** (the
   binary is built from a byte-identical tree).
4. Per PR into `v26.3.15.4-lts-aiven`: after each group merges, run that group's subset
   and confirm it matches the baseline. This localizes any surprise to a single PR.

**Interpretation.** Because the reslice preserves the tree exactly, any test-result
delta between prod and reslice indicates a reslice defect (dropped file, lost test,
mis-squash), not a code change — investigate before delivery. Pre-existing flaky/red
tests should be recorded as-is in the baseline so "no *new* breakage" is provable.

## 9. Decisions (locked 2026-07-06) and remaining confirmations

**Locked by the human (2026-07-06):**

1. **Patch 019 → split into two code commits.** `f1251b8826a` ("avnadmin creating
   database using sql") and `a5e57a7d8ca` ("Mint fresh query_id for internal queries")
   are two distinct changes. Ship as **two** `patch-port` commits (019 + its sibling),
   not one squash. In Method A, cherry-pick each `-n` separately and commit separately;
   confirm the dossier/inventory handle for the second before delivery.
2. **Follow-ups → squashed into the base patch.** For 004, 021, 022, 050, 059 the
   `patch-fix`/restore commits are folded into their `patch-port` (cherry-pick base +
   fix `-n` together, commit once). Granular fix history stays on the frozen `-dev`
   branch + archive tags.
3. **Bootstrap → exactly one commit.** Re-collapse the bootstrap material
   (`docs/aiven/{AGENTS.md,schema,skills,runbooks,proposals,plans}` + `.cursor/`) into a
   single bucket-1 commit (matches §4 literally; supersedes the "keep existing 1–2"
   option).
4. **Reslice method → recommended combination.** Method A for bucket 2 (code),
   tree-checkout (Method B) for buckets 1 and 3.

**Bootstrap-doc freshening (done 2026-07-06, pre-reslice).** The `.10.62 → .15.4`
point-release rebase left the *living* orientation docs pointing at the old base. Fixed
in the working tree: `docs/aiven/AGENTS.md` §1 and the two `skills/` workflow templates
(`dispatch-prompt-template.md`, `patch-dossier-template.md`). Dated design docs
(`plans/`, `proposals/`) and runbook incident/event narratives are point-in-time records
and left unchanged. `submodule-forks.md` (§Registry lines 112–118 + the `git ls-tree`
example) carries real submodule gitlink / fork-branch SHAs that may have drifted in the
rebase — **flagged for a separate data-accuracy pass**, not touched here.

**Equivalence-oracle ordering (important).** The whole-tree empty-diff proof (§4.4) only
holds if the reslice's reproduction target already contains the freshening. Therefore:
land the freshening + this plan as a single **docs-only** commit on `-dev` first (the
production *binary* is unaffected — no `src`/`tests` change), tag the **prereslice**
snapshot at that commit, and reproduce *that* tree in the reslice. The exact deployed
hash is separately tagged for traceability. See §4.

**Remaining confirmations (not blocking):**

5. **Patch 076 dossier** (`b2eb2b9bcb4` is CODE-ONLY). Verify its dossier exists in the
   docs tree (so bucket 3 carries it) and the inventory row is present.
6. **Drop records live in the docs bucket only** (no code). `git log --grep
   '^patch-drop('` therefore returns nothing on the reslice line — acceptable per §4;
   flagged so nobody relies on the old recipe.
7. **PR base** = `v26.3.15.4-lts-aiven` (currently == base) accumulates the full stack.

## 10. Execution checklist (once approved)

- [x] Freshen the living bootstrap docs to `v26.3.15.4` (§9); flag `submodule-forks.md`.
- [ ] Confirm the deployed prod hash; tag `archive/v26.3.15.4-lts-aiven-dev-prodhash`.
- [ ] Commit the docs-only freshening + this plan on `-dev` (one commit).
- [ ] Capture the baseline test manifest on that tip (§8).
- [ ] Tag `archive/v26.3.15.4-aiven-dev-prereslice` on the new `-dev` tip.
- [ ] Create `v26.3.15.4-lts-aiven-reslice` from `v26.3.15.4-lts`.
- [ ] Build bucket 1 (single bootstrap commit), bucket 2 (per-patch code — **019 split
      into two**, other follow-ups squashed), bucket 3 (consolidated docs).
- [ ] Prove `git diff archive/v26.3.15.4-aiven-dev-prereslice reslice` is empty (§4.4).
- [ ] Re-run the Aiven test suite on the reslice tip; diff against baseline (§8.3).
- [ ] Open PR-0 (bootstrap) → `v26.3.15.4-lts-aiven`; then PR-1..PR-8 in order (§7),
      running each group's tests post-merge.
- [ ] Final: `v26.3.15.4-lts-aiven` tree == prereslice tree; full suite matches baseline.

## 11. What this plan explicitly does NOT change

- The production `-dev` branch and its hash (frozen).
- Any patch's behavior or diff identity (`git patch-id` preserved per code commit).
- Upstream branches (no push until each step is approved).
- The durable policy in `commit-hygiene.md` (this plan *executes* it; if execution
  reveals a policy gap, amend the runbook as a separate bootstrap-(A) commit).

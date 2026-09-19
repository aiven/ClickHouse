---
name: aiven-upstream-sync
description: Replay the Aiven internal patch stack onto a branch cut from upstream (master or an LTS tag), resolving each conflict against the upstream commit that caused it and recording that provenance in the patch's commit message. Use when syncing the Aiven fork to a newer upstream base.
argument-hint: [source-branch] [--from <patch-id>] [--only <patch-id>] [--no-build]
---

# Aiven upstream sync

Replay Aiven's internal patch stack onto a branch cut from a newer upstream base, one
patch at a time. Every conflict is attributed to the upstream commit that caused it,
resolved against that commit, and the attribution is appended to the patch's own commit
message so the patch accumulates a provenance trail across syncs.

## Preconditions

Verify all of these before starting. Stop and report if any fails.

1. **The current branch is cut from upstream and carries no Aiven commits.** It may be
   branched from `upstream/master` or from an upstream LTS tag. Confirm:

    ```bash
    git rev-parse --abbrev-ref HEAD                      # must not be master
    git log --oneline -1
    ```

    If the branch already carries Aiven commits, this skill is not resumable over them —
    either continue from the ledger (see _State_) or cut a fresh branch.

2. **The source branch exists and is untouched.** Default source is
   `v26.3.32.14-lts-aiven` — the last known-good stack. **It is read-only. Never commit
   to it, never rebase it, never amend it.**

3. **`upstream` remote is fetched.**

    ```bash
    git fetch upstream master --tags
    ```

4. **A build directory exists and stock upstream builds** — see _Step 0_.

## Key mechanic — the checkout-free apply probe

To ask _"does patch `P` cherry-pick cleanly onto commit `C`?"_ without touching the working
tree:

```bash
git merge-tree --write-tree --merge-base="$P^" "$C" "$P"
```

- exit `0` → applies cleanly (stdout is the resulting tree OID)
- exit `1` → conflicts (stdout lists `CONFLICT (...)` lines and the conflicting paths)

Same semantics as cherry-pick (three-way merge with base `P^`). Use this probe everywhere —
never check out a candidate commit just to test whether a patch applies, and never flip the
working tree during bisect.

## Subagents

Every subagent this skill dispatches runs on a top model — `model: opus` (Opus 5, 1M
context). That applies to conflict investigation and resolution, build- and test-log
analysis, and any escalated investigation. Do not downgrade a dispatch to a smaller model
to save tokens: the failure mode across this whole procedure is a problem that looks benign
in a log or a diff and is not, and a missed signal costs a full build cycle to recover.

## Documentation — read-only, with one exception

`docs/aiven/**` is **read-only** for this skill. Read the dossiers
(`docs/aiven/patches/<NNN>-*.md`), the inventory
(`docs/aiven/uplifts/26.3/inventory.md`), `docs/aiven/patch-rationale.md` and the runbooks
freely — they are the input that makes a resolution trustworthy. Do **not** write outcomes,
breaking commits, or per-patch narrative back into them. That provenance belongs in the
patch's own commit message trailer (see 2d), where it travels with the patch through every
future sync instead of sitting in a file that has to be kept in step with it.

**The one exception is a dropped patch.** When a patch is no longer needed
(`dropped_already_upstream`, `dropped_superseded`, `dropped_code_gone`) there is no commit
left to carry the record, and the documentation would otherwise keep describing a patch the
fork no longer has. Update:

- `docs/aiven/patch-rationale.md` — move the patch to the "absent on purpose" list with the
  reason. Applies to `dropped_superseded` and `dropped_code_gone`; a
  `dropped_already_upstream` backport was never an Aiven feature and has no rationale entry
  to move.
- the patch's dossier, if one exists — that it was dropped, why, and the upstream commit or
  PR that made it unnecessary.

Nothing else under `docs/aiven/**` is written by a sync.

## Step 0 — Baseline the branch before any Aiven commit

Never start patching an unverified base; a failure later is uninterpretable without this.

1. Build stock upstream, redirecting to a log in the build directory, then have a
   **subagent** (`model: opus`) analyse the log and return a summary only:

    ```bash
    ninja -C build clickhouse > build/build_baseline.log 2>&1
    ```

    Do not pass `-j`. See `docs/aiven/runbooks/build-and-test.md` for toolchain
    requirements (`CC`/`CXX` must point at the real `clang` ≥ 21, **not** the `ccache`
    wrappers) and §2 on `cmake --fresh` forcing a full `contrib` rebuild.

2. Run the baseline test set and record which tests already fail on stock upstream. That
   list is the **known-failure baseline**; every later run is diffed against it.
3. Record the baseline in the ledger. **Gate: do not proceed with an unexplained baseline
   failure.**

## Step 1 — Derive the patch list and pre-scan it

Derive the source branch's upstream base and the master branch point — do not hardcode:

```bash
SRC=v26.3.32.14-lts-aiven
BASE=$(git merge-base "$SRC" v26.3.32.14-lts)          # upstream base of the stack
FORK=$(git merge-base "$SRC" upstream/master)          # where the LTS line was cut from master
```

The patch list, in apply order. The branch order already encodes real dependencies (`026`
before `patch-fix(026)`, `022` and `079` as a pair), so **never reorder it**:

```bash
git rev-list --reverse --no-merges "$BASE".."$SRC"
```

`--no-merges` drops merge commits on the source branch; their content arrives via the
underlying commits. Picking one directly would require `-m 1`.

**Pre-scan every patch against `HEAD` before doing any work**, to get the conflict map
upfront:

```bash
HEAD_TREE=$(git rev-parse HEAD^{tree})
for P in $(git rev-list --reverse --no-merges "$BASE".."$SRC"); do
  short=$(git rev-parse --short "$P"); subj=$(git log -1 --format=%s "$P")
  if tree=$(git merge-tree --write-tree --merge-base="$P^" HEAD "$P" 2>/dev/null); then
    if [ "$tree" = "$HEAD_TREE" ]
    then echo "already  $short $subj"      # change is already in the base — do not pick
    else echo "clean    $short $subj"
    fi
  else echo "conflict $short $subj"
  fi
done
```

`tree = HEAD_TREE` is the **already-in-the-base** detector: the merge produced the tree
that is already checked out, so the patch is a no-op here. See _Already in the base_.

Write each patch's result into the ledger as its `predicted` state.

## Step 2 — The per-patch loop

For each patch `P` in order:

### 2a. Probe, then apply

```bash
tree=$(git merge-tree --write-tree --merge-base="$P^" HEAD "$P" 2>/dev/null); rc=$?
```

- `rc = 0` **and** `tree = $(git rev-parse HEAD^{tree})` → the change is already in the
  base. **Do not pick it.** Go to _Already in the base_.
- `rc = 0` and the tree differs → `git cherry-pick "$P"`, run the **check** (see _Checks_),
  record `carried_clean`, move on.
- `rc = 1` → conflict; go to 2b. A conflict does **not** rule out the change being present
  already — an adapted backport conflicts textually with the native version. Run the
  _Already in the base_ checks before resolving anything.

### 2b. On conflict — attribute before resolving

Find the upstream commit that broke the patch before resolving anything. A resolution is
only trustworthy once you know what upstream was trying to do.

```bash
git cherry-pick --abort            # if a cherry-pick is in progress
```

Bisect the **whole range**, with no pathspec restriction. Do not try to narrow the search to
the files that conflicted at `HEAD`: the breaking change may have lived in a different file
back then — code moves between translation units, headers change shape — and restricting the
commit set would make bisect skip the very commit you are looking for and report a later one.

Write the probe script once:

```bash
mkdir -p tmp/sync
cat > tmp/sync/probe.sh <<'SH'
#!/usr/bin/env bash
# usage: probe.sh <patch_sha>   — bisect run script, exits 0 = applies cleanly
P="$1"
C=$(git rev-parse BISECT_HEAD)
git merge-tree --write-tree --merge-base="$P^" "$C" "$P" >/dev/null 2>&1
SH
chmod +x tmp/sync/probe.sh
```

Then bisect. `--no-checkout` keeps the working tree and build cache untouched;
`--first-parent` lands on the merge commit, i.e. the **PR** that broke the patch, and keeps
bisect out of a PR's incomplete intermediate states:

```bash
git bisect start --no-checkout --first-parent HEAD "$GOOD"
git bisect run tmp/sync/probe.sh "$CURRENT_SHA"
git bisect log >> tmp/sync/bisect_<patch-id>.log
git bisect reset
```

`$GOOD` is `$FORK` on the first round, and the previous round's breaking commit on every
round after that — so the search range shrinks as the patch is brought forward.

**Validate the `good` end first.** Bisect assumes the patch applies at `$FORK`. Probe that
directly; if it already conflicts there, the premise is false — the patch was never valid
at `$FORK`. Skip bisect, resolve directly against `HEAD`, and note it in the ledger.

### 2c. Resolve

Delegate the resolution to a **subagent** (`model: opus`). Give it the patch's dossier
(`docs/aiven/patches/<NNN>-*.md`), the conflicting hunks, the breaking upstream commit with
its PR description, and the rules below. Require it to return **reasoning and a proposed
resolution for review** — it must never land a resolution silently.

Resolution rules:

- **Preserve intent, not text.** If upstream renamed, split or relocated the code the
  patch hooks into, re-express the patch at the new hook point. Do not force the old shape
  back.
- **Aiven behaviour changes stay gated.** A patch that changes default behaviour keeps its
  `aiven_`-prefixed server setting, defaulting off, so a gate-off build stays
  behaviour-identical to stock ClickHouse.
- **Feature removal stays compile-time**, via `REGISTER_*` CMake flags defaulting ON.
- **House style**: Allman braces; inline code formatting for ClickHouse identifiers in
  prose; function names written as `f`, not `f()`; "exception", not "crash", for logical
  errors.
- **Never use `sleep` to paper over a race.**
- If the breaking commit shows upstream **implemented equivalent behaviour**, stop. This is
  supersession, not a conflict — go to _Supersession_.

### 2d. Update the patch and append provenance

Resolve against the **breaking commit**, producing an updated version of the patch. Use a
scratch worktree so the main working tree and build directory are never flipped to an
ancient commit:

```bash
git worktree add --no-checkout tmp/sync/wt "$BREAKING_SHA"
```

Submodules will be stale there — irrelevant for resolving text conflicts. Remove the
worktree when the patch is done.

Commit the resolved patch with its **original message plus an appended trailer**, one line
per upstream commit this patch has had to be reconciled with:

```
Conflict-resolved-against: <short-sha> <upstream commit subject>
```

If the patch already carries such trailers, **append**, never replace.

Set the patch's `current_sha` in the ledger to the new commit. Updating a patch that has
not yet landed on the target branch is **not** an amend of published history — the repo
rule against amend and rebase governs landed commits. Once a patch has landed, further
fixes go in as new `patch-fix(NNN)` commits.

### 2e. Retry

Probe the updated patch against `HEAD` again.

- Clean → cherry-pick it, run the check, record `carried_with_conflicts` plus every
  breaking SHA found.
- Still conflicting → go back to **2b** with `GOOD="$BREAKING_SHA"`, find the next breaking
  commit, resolve it, append the next trailer line, probe again.

The loop is deliberately plain: one breaking commit found and resolved per round, with one
trailer line appended per round, until the patch applies at `HEAD`. It terminates because
each round moves `$GOOD` forward, so the range strictly shrinks; the number of rounds equals
the number of _distinct_ breaking commits for that patch.

**Iteration cap: 5 rounds.** Past that, stop looping: resolve once directly against
`HEAD`, record `reworked`, and note the abandoned rounds in the ledger. If that is also
intractable — the subsystem was rewritten and the patch's intent no longer maps onto
`HEAD` — escalate to the user with the bisect log and the candidate hook points. Do not
guess.

## Already in the base

A patch may describe a change the new base **already contains**. Cherry-picking it then
either conflicts against the native version or silently duplicates the change. Check this
before resolving any conflict.

The commit convention signals the common case — a subject of the form

```
Backport #<PR> to <line>: <subject>
```

is an upstream PR backported into an older release line. When the new base is newer than
that PR, the base has it natively and the backport must be **dropped**, not carried. The
`to <line>` label is stale on the new base; never treat it as evidence the commit is still
needed.

Not limited to backports: a patch Aiven contributed upstream, or one upstream implemented
independently, lands in the same place.

### Detection, cheapest first

1. **Tree-equality probe** — works for any patch, no text matching, and is conclusive. The
   only check that still works when the change is present but was reformatted or moved:

    ```bash
    tree=$(git merge-tree --write-tree --merge-base="$P^" HEAD "$P") \
      && [ "$tree" = "$(git rev-parse HEAD^{tree})" ] && echo "already present"
    ```

2. **Upstream PR ancestry**, for `Backport #<PR>` subjects. Extract the PR number and look
   for its merge commit in the base's history:

    ```bash
    PR=<number>
    git log --merges --oneline --grep="Merge pull request #${PR} " HEAD | head
    ```

    A hit that is an ancestor of `HEAD` means the change is in. Check the commit **body**
    as well as the subject — a backport may cite the backport PR rather than the original,
    and upstream's own backport merges follow `backport/<line>/<original-PR>`.

3. **Patch-id equivalence**:

    ```bash
    git cherry -v HEAD "$P^..$P"    # leading '-' means an equivalent patch is in HEAD
    ```

    Weak on its own: a backport adapted to an older line is rarely textually identical to
    the master version, so a `+` here does **not** mean the change is absent.

4. **Identifier presence.** Grep `HEAD` for the patch's distinctive identifiers — an
   `aiven_`-prefixed setting, a new function name, a `REGISTER_*` flag. If they already
   exist at `HEAD`, the patch is present or superseded; investigate which.

### Outcome

Record `dropped_already_upstream` with the upstream PR or commit that supplies the change,
and do not cherry-pick. Note the drop in the dossier if one exists. Unlike
_Supersession_, this needs no change to `docs/aiven/patch-rationale.md` — the change was
never an Aiven feature.

**Partial presence is `reworked`, not a drop.** If the base has the upstream fix but the
Aiven commit also carried an adaptation on top of it, carry the adaptation forward and drop
only the backported portion. Verify by reading the diff, not the subject line.

## Supersession

When the breaking commit implements equivalent behaviour, the patch is done, not broken.

Record `dropped_superseded`, the `superseded_by_upstream_commit`, and the reason. Update
the patch's dossier and the "absent on purpose" list in `docs/aiven/patch-rationale.md`.
Do not cherry-pick the patch.

**Partial** supersession — upstream shipped _some_ of the patch — is `reworked`: carry the
remainder. Never collapse it into "not needed".

## Checks

Run after every successful cherry-pick, tiered:

| Tier              | When                                                      | What                                                                                                                                                |
| ----------------- | --------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Compile**       | every patch                                               | `ninja -C build clickhouse > build/build_<patch-id>.log 2>&1` (no `-j`)                                                                             |
| **Targeted**      | every patch                                               | the patch's own `tests/queries/0_stateless/9<NNN>_*` and `tests/integration/test_aiven_*` tests                                                     |
| **Evidence pair** | every patch that was conflicted or reworked               | patch in → test passes; patch reverted → same test fails. Use the single-axis worktree-flip technique in `docs/aiven/runbooks/testing-suites.md` §6 |
| **Full suite**    | batch boundaries, and after any non-mechanical resolution | stateless smoke tier + full `test_aiven_*` set, diffed against the Step 0 baseline                                                                  |

Redirect build and test output to uniquely-named logs under `build/` so runs can go in
parallel, and always have a **subagent** (`model: opus`) analyse each log and return a
concise summary — never read a full build log into the main context. Scratch files go in
`tmp/`, never `/tmp`.

Suggested batch boundaries, following the stack's own themes: Kafka (`029`–`033`, `076`),
object storage (`012`–`016`, `024`–`026`, `028`), `DatabaseReplicated` (`003`–`010`,
`072`), access control (`011`, `014`, `019`–`022`, `079`), build gates (`051`, `052`,
`070`, `071`, `075`, `N01`, `N06`), MV refresh (`049`, `050`, `066`, `078`).

### Two verification hazards

- **Green incremental builds can be false.** This build directory has no ninja header
  dependency records (`#deps 0`). After any patch that changes a **struct layout** or a
  **function signature** in a widely-included header, an incremental build is _unproven_ —
  stale `.o` files link cleanly and corrupt at runtime, far from the edit. Force-recompile
  the include closure; see `docs/aiven/runbooks/build-and-test.md` §7.
- **A new `.cpp` needs a reconfigure.** ClickHouse globs its sources, so a patch adding a
  source file needs a plain `cmake -B build` (never `--fresh`) before `ninja` sees it.

## State

Keep a resumable ledger. SQLite in `tmp/`, transient, **not** committed:

```sql
CREATE TABLE patches (
  seq                           INTEGER PRIMARY KEY,  -- apply order, from the source branch
  patch_id                      TEXT,                 -- 046, N02, or a subject for un-numbered commits
  source_sha                    TEXT,                 -- immutable, on the read-only source branch
  current_sha                   TEXT,                 -- evolves as the patch is reconciled
  dossier                       TEXT,                 -- docs/aiven/patches/<NNN>-*.md
  predicted                     TEXT,                 -- from the Step 1 pre-scan
  status                        TEXT,                 -- see below
  breaking_commits              TEXT,                 -- accumulated, mirrors the commit trailers
  rounds                        INTEGER,
  superseded_by_upstream_commit TEXT,
  drop_reason                   TEXT,
  notes                         TEXT
);
```

`status` values: `pending`, `carried_clean`, `carried_with_conflicts`, `reworked`,
`dropped_already_upstream`, `dropped_superseded`, `dropped_code_gone`, `deferred`.
`dropped_already_upstream` and `dropped_superseded` stay distinct because only the latter
requires editing `docs/aiven/patch-rationale.md`.

Seed the ledger by **reading** `docs/aiven/uplifts/26.3/inventory.md`, so the previous
uplift's outcomes carry forward. The ledger is run state: it drives the loop, makes the run
resumable, and backs the final report. It is not committed, and its contents are not written
back into `docs/aiven/**` — each carried patch's outcome lives in that patch's commit
message trailer.

## Closing out a sync

1. For each **dropped** patch, record the removal in `docs/aiven/` — see _Documentation_.
   This is the only documentation write a sync makes.
2. Full suite green, diffed against the Step 0 baseline.
3. Report to the user from the ledger: patches carried clean, carried with conflicts,
   reworked, dropped with reasons, and anything deferred for a human decision. The report is
   the hand-off — do not write it into `docs/aiven/**`.

## Invariants

- **Aiven commits are always on top** of a pristine upstream base, in source-branch order.
  `docs/aiven/patch-rationale.md` is defined in terms of this.
- **The source branch is read-only.**
- **`docs/aiven/**` is read-only**, except to record a dropped patch. A carried patch's
  provenance lives in its commit message trailer, never in a doc.
- **Never rebase the target branch**; to change base, cut a fresh branch and replay.
- **A patch never lands with an unresolved or guessed conflict.** If it cannot be resolved
  with evidence, it is `deferred` and escalated — never force-applied.

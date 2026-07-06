# Per-patch dossier template

> The structure for `docs/aiven/patches/<NNN>-<slug>.md`. Worker creates this file at T3 dispatch time (born-at-dispatch, not pre-allocated). The dossier is the patch's home directory across LTS uplifts: each uplift appends a new row to §0 and a new section to §6 — the SAME file follows the patch forever.
>
> Worker fills every section. Empty sections are not acceptable; use `n/a — <reason>` if a section truly doesn't apply.

---

```markdown
# Patch <NNN> — <slug>

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | <sha> | <author/committer> | (original carry, or "ported from 24.x") |
| 25.8-aiven | <sha> | <author/committer> | byte-equivalent / conflict-resolved / drift-superseded / dropped |
| 26.3-aiven | <sha-or-staged> | <T3 worker> | byte-equivalent / conflict-resolved / drift-superseded / dropped |

The current uplift's row stays "(staged)" until the human commits.

## 1. Purpose

<2–4 sentences. What does this patch DO and WHY does Aiven need it. The "why" is the durable part — code may change every uplift; the motivation should not.>

Source SHA on `v25.8.18.1-lts-aiven`: `<sha>` (from `docs/aiven/uplifts/26.3/inventory.md` row <NNN>).
Original author: `<author>` (per `git log --format=%ae`).
Original purpose: `<git log --format=%B | head -20>` (quoted, not paraphrased).

## 2. Upstream-drift findings

> Mandatory section. The point of this section is to verify the patch is still
> SEMANTICALLY correct against `v26.3.15.4-lts`, not just textually
> applicable. Run the commands; record the findings; don't ship without them.

### Commands run

```bash
git log v25.8.18.1-lts..v26.3.15.4-lts -- <files-touched-by-patch>
git log v25.8.18.1-lts..v26.3.15.4-lts --grep '<key-identifier-from-patch>' --oneline
```

### Findings

- Upstream changes to touched files between prior and current LTS:
  - `<file>`: `<one-sentence summary of changes, or "no changes">`
- Upstream changes that touched the patch's behavior (symbols, error codes, callers):
  - `<symbol/code>`: `<one-sentence summary, or "no changes">`
- Conclusion: one of:
  - `still-needed-and-applies` — proceed with cherry-pick.
  - `still-needed-but-rewrite` — semantics unchanged but the cherry-pick can't apply; manual port required (describe).
  - `obsoleted-by-upstream` — upstream merged an equivalent or superseding fix at `<sha>`. Drop this patch. Document the upstream equivalent for the next uplift.
  - `irrelevant-by-removal` — upstream removed the feature this patch was protecting (`<feature>` no longer exists in 26.3). Drop this patch.

## 3. C++ review

Apply `docs/aiven/skills/cpp-review-checklist.md` to this patch. One bullet per checklist section. `n/a` is allowed; `✓` requires one-sentence evidence.

- 1 Lifetime + ownership: <result>
- 2 Exception safety: <result>
- 3 Thread-safety + concurrency: <result>
- 4 Performance + memory: <result>
- 5 Settings as public API: <result>
- 6 Error handling: <result>
- 7 Upstream / vendored code: <result>
- 8 Behavior under settings: <result>

## 4. Test design

One of:

(a) **New test that fails on the parent commit and passes after the patch.**

- Test path: `tests/queries/0_stateless/9<NNN>_<slug>.{sh,sql,reference}` per the Aiven test-naming convention (`docs/aiven/runbooks/testing-suites.md` §4.1), where `<NNN>` is THIS patch's dossier number (e.g., `9011_*` for patch 011). For integration tests: `tests/integration/<test_dir>/`. Do NOT use upstream's `add-test` allocator.
- Pre-patch run output (the FAIL):

  ```text
  <captured output showing failure>
  ```

- Post-patch run output (the PASS):

  ```text
  <captured output showing success>
  ```

- Why this test distinguishes the Aiven gate from upstream behavior (per AGENTS.md §7): `<one sentence>`.

(b) **Documented justification — no new test, with reference to existing upstream test.**

- Existing test path: `<path>`.
- Why this test covers the patch's behavior: `<one sentence>`.
- Why a new test is not warranted: `<one sentence — e.g. patch is CMake-only, or config-rename without behavior change>`.

## 5. Rollback considerations

- If this patch must be reverted in production: is the revert safe (no schema migration, no on-disk format change)?
- Does the patch introduce any state that survives a clickhouse-server restart (ZK nodes, files on disk, in-memory caches)?
- If applicable: what setting can be used to disable the new behavior without rebuilding?

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

<one paragraph or n/a>

### 25.8-aiven (historical, may be empty)

<one paragraph or n/a>

### 26.3-aiven (this uplift)

- Cherry-pick was: clean / conflict-resolved (describe) / rewritten (describe) / dropped.
- Upstream-drift conclusion: <from §2>.
- Test added at: `<path>` (or "no test, see §4(b)").
- Time-to-port (subagent wall-clock + human review): `<minutes>` — annotate
  whether the build directory was **cold-cache** (fresh `cmake` reconfigure
  or first build of the day) or **warm-cache** (sccache hot from a prior
  dispatch). T3.2 surfaced that warm-cache numbers are ~3× faster than
  cold and the two should not be averaged when sizing future budgets.
- Anything surprising: <one sentence>.
```

---

## Notes for the worker

- The dossier is created at T3 dispatch and lives forever. **Future uplifts will inherit this file.**
- §0 Lineage and §1 Purpose are durable across uplifts. §3–§5 are per-uplift but copied forward as starting points. §6 grows by one section per uplift.
- The worker MUST stage the dossier with `git add docs/aiven/patches/<NNN>-<slug>.md` as part of the halt-and-escalate "staged files" list. The human commits the dossier together with the source change and the test.
- If §2 concludes `obsoleted-by-upstream` or `irrelevant-by-removal`, the worker still creates the dossier (with the patch source-change reverted out of the index) and uses `escalation_reason: policy_call` so the human reviews the drop decision.

# Patch 043 — remove-ssbs-check-from-no-armv81-or-higher

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `6b54a78936` | Tilman Moeller (author) / Joe Lynch (committer) | (the version we are porting FROM) |
| 26.3-aiven | `patch-drop(043)` | parent agent, 2026-05-31 | superseded-by-upstream-equivalent — drop; see §2 |

The drop was committed as `patch-drop(043)` (no code carried; reason in §2).
Find it with `git log --grep '^patch-drop(043)'`.

## 1. Purpose

The CPU-capability gate in `cmake/cpu_features.cmake` for `NO_ARMV81_OR_HIGHER`
greps `/proc/cpuinfo` of the *build machine* to ensure it can run the
intermediate code-generation binaries (e.g. `protoc`, `llvm-tablegen`) that the
build compiles for ARMv8.1+. The 25.8 check required **both** `atomic` (LSE) and
`ssbs` (Speculative Store Bypass Safe) flags to be present.

`ssbs` is optional in ARMv8.0 and only mandatory from ARMv8.5, so legitimate
ARMv8.1+ machines that lack `ssbs` were *falsely rejected* with a `FATAL_ERROR`.
The Aiven patch removes the `ssbs` requirement, keeping only `atomic` — a
sufficient ARMv8.1+ indicator. The "why" is durable: the build-host capability
gate must not reject hosts that actually satisfy the minimum ISA.

Source SHA on `v25.8.18.1-lts-aiven`: `6b54a78936` (from
`docs/aiven/uplifts/26.3/inventory.md` row 043).
Original author: `tilman.moeller@aiven.io`.
Original purpose (verbatim from the source commit body):

> Remove SSBS check from `NO_ARMV81_OR_HIGHER`
>
> The CPU validation check for ARMv8.1+ was requiring both `atomic` and `ssbs`
> features to be present in /proc/cpuinfo. However, SSBS (Speculative Store
> Bypass Safe) is optional in ARMv8.0 and only mandatory in ARMv8.5, which means
> some valid ARMv8.1 CPUs may not have this feature.
>
> This caused false rejections during build validation, preventing builds on
> legitimate ARMv8.1+ CPUs that don't have SSBS. ...

## 2. Upstream-drift findings

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`, not just textually applicable.

### Commands run

```bash
# What does the gate line look like on the current base?
git show v26.3.10.62-lts:cmake/cpu_features.cmake | sed -n '67p'
# → COMMAND grep -P "^(?=.*atomic)" /proc/cpuinfo
#   i.e. the `(?=.*ssbs)` lookahead is ALREADY GONE on the base.

# Who removed ssbs on the base, and is it upstream?
git log -1 -S 'ssbs' -- cmake/cpu_features.cmake --format='%H %ad %an%n  %s'
# → 545c27008d450cfd55bc4f04e807bc4c88d2d08a  Fri Oct 3 04:43:36 2025  Konstantin Bogdanov
#     Remove SSBS check from `NO_ARMV81_OR_HIGHER`     (identical subject)

# Is that commit in the new base but not the old one?
git merge-base --is-ancestor 545c27008d4 v26.3.10.62-lts && echo IN_26.3
# → IN_26.3
git merge-base --is-ancestor 545c27008d4 v25.8.18.1-lts || echo NOT_IN_25.8
# → NOT_IN_25.8
```

Full logs under `tmp/patch-043/`.

### Findings

- The current base `v26.3.10.62-lts` already carries an **identical** change:
  upstream commit `545c27008d4` ("Remove SSBS check from `NO_ARMV81_OR_HIGHER`",
  Konstantin Bogdanov, 2025-10-03) rewrites the same grep on the same line from
  `"^(?=.*atomic)(?=.*ssbs)"` to `"^(?=.*atomic)"` — byte-for-byte the Aiven
  patch's post-image.
- That commit is an ancestor of `v26.3.10.62-lts` but **not** of
  `v25.8.18.1-lts`. So the divergence is purely temporal: 25.8 branched before
  the upstream fix, Aiven carried an equivalent fix on `25.8-aiven`
  (`6b54a78936`, 2026-01-05), and 26.3 inherited the upstream one. The two LTS
  bases have now **converged** on the same line.
- Cherry-picking `6b54a78936` onto 26.3 HEAD would be a no-op at best (the
  target line is already in the desired state) or produce a context conflict at
  worst; either way it carries **zero** semantic change.
- Conclusion: **`superseded-by-upstream-equivalent`** — drop. Recorded
  upstream-equivalent SHA for the next uplift's lineage: `545c27008d4`. Future
  uplifts should skip this patch unconditionally as long as the upstream commit
  remains in the base.

## 3. C++ review

`n/a — cmake/build-system change, no C++.` The only "review" dimension that
applies is §7 (upstream/vendored ownership): the change is to
`cmake/cpu_features.cmake`, a ClickHouse-owned build file (not under
`contrib/**`), so it is editable — but moot here, since no change is carried.

## 4. Test design

(c) **No new test — patch is being proposed for DROP, not port.**

- Existing test path: `n/a`. The change is a build-host capability gate; it has
  no runtime-observable behavior and no test harness exercises `/proc/cpuinfo`
  grepping.
- Why no test is warranted: there is no code delta to test on this base — HEAD
  already equals the patch's post-image (see §2). A "fails before / passes
  after" pair is impossible because "before" and "after" are identical.
- Per AGENTS §7, "silently shipping no test is forbidden" — the correct response
  here is not to ship the patch with no test, it is to NOT ship the patch and
  document why (this dossier).

## 5. Rollback considerations

- Revert safety: `n/a` — no code is carried by the drop.
- State that survives restart: none.
- If upstream ever *reverts* `545c27008d4` (re-adding the `ssbs` lookahead),
  this patch becomes needed again — the next uplift's drift check (the
  `merge-base --is-ancestor` test in §2) will detect that and the patch should
  be re-evaluated for carry.

## 6. Per-uplift notes

### 25.8-aiven (historical)

Original carry. Author: Tilman Moeller. Committer: Joe Lynch. No 25.X test was
authored alongside the patch (build-system change).

### 26.3-aiven (this uplift)

- Cherry-pick was: NOT performed. Parent stopped at Step 1 (upstream-drift
  analysis) with conclusion `superseded-by-upstream-equivalent`.
- Upstream-equivalent SHA: `545c27008d4` (in `v26.3.10.62-lts`, not in
  `v25.8.18.1-lts`).
- Test added at: `n/a — no source change in this dispatch; see §4.`
- Anything surprising: this is the first observed `superseded-by-upstream-equivalent`
  drop (distinct from 007's `irrelevant-by-removal`, where the whole *feature*
  was deleted). Here the feature survives; only the patch is redundant because
  upstream landed the identical change between the two LTS bases. The tell-tale
  was a `git log -S` on the modified token showing an upstream commit with the
  **same subject line** already present on the base.
```


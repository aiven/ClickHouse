# Patch 041 — swap-drift

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | (unknown — predates this dossier) | Tilman Moeller | (historical) |
| 25.8-aiven | `a05665ce57c1a6b4b6d5fda656815f6ea774b82c` | Tilman Moeller / Kevin Michel (committer Joe Lynch) | carried |
| 26.3-aiven | (staged — escalated, not yet committed) | T3.20 worker | conflict-resolved (rewrite); see §6 |

The current uplift's row stays "(staged)" until the human commits.

## 1. Purpose

ClickHouse's `MemoryTracker` enforces the server-wide memory limit by gracefully
delaying/rejecting individual queries instead of relying on the OOM killer. It
normally tracks only the process RSS, which **excludes swapped-out pages**. With
swap enabled (even at low swappiness), pages drift to swap; the tracker stops
counting them, permits more allocations, which push more pages to swap — a
self-reinforcing loop ("swap drift") that slowly consumes all swap while the
server still believes it is under its RAM limit. When a swapped page must return
to RAM there is no room anywhere ("swap hell"). This patch folds the process's
swapped bytes into the value fed to the tracker (RSS + swap), and exposes the
swapped bytes as a new `system.asynchronous_metrics` row `MemorySwap` for
monitoring.

Source SHA on `v25.8.18.1-lts-aiven`: `a05665ce57c1a6b4b6d5fda656815f6ea774b82c`.
Original author: `Tilman Moeller <tilman.moeller@aiven.io>` (Co-authored-by
`Kevin Michel <kevin.michel@aiven.io>`; committed on the source branch by
`Joe Lynch <joelynch112@gmail.com>`). Author date: 2026-01-05.
Original purpose (verbatim subject + impact): "Fix swap drift" — take swapped
memory into account in addition to RSS by reading `/proc/self/status` instead of
`/proc/self/statm`, and expose `MemorySwap`.

## 2. Upstream-drift findings

### Commands run

```bash
git grep -c -- <each identifier> src/                       # tmp/patch-041/identifier-inventory.log
git log --oneline v25.8.18.1-lts..v26.3.10.62-lts -- <each of the 7 files>   # tmp/patch-041/file-history.log
git log -S 'updateRSSPlusSwap' -S 'VmSwap' v25.8.18.1-lts..v26.3.10.62-lts -- src/Common/   # tmp/patch-041/upstream-equivalent.log
git show a05665ce57:src/Common/MemoryWorker.cpp ... ; diff ...   # tmp/patch-041/mw-*.txt
```

### Findings

- Upstream changes to touched files between prior and current LTS:
  - `AsynchronousMetrics.cpp`: many commits, incl. `f419762bcd0` adding a
    `/proc/self/status` SigQ reader (explains the 4 pre-existing `/proc/self/status`
    references on HEAD — none in `MemoryStatisticsOS`). No swap accounting added.
  - `MemoryStatisticsOS.cpp`: only `9aa8e4ed277` (`ErrnoException` move). No swap.
  - `MemoryStatisticsOS.h`: no upstream changes.
  - `MemoryTracker.{cpp,h}`: `c0a03d2891b` "Improve memory tracking accuracy and
    coverage" introduced `uncorrected_amount`/`last_corrected_amount`/
    `MemoryTrackingUncorrected` — this is the **dependency** the patch relies on,
    NOT a duplicate fix.
  - `MemoryWorker.cpp`: `d47e737e472` (wrap loop body in `try/catch`),
    `28787fecc9f` (race protection), `dfddf71f251` (`USE_JEMALLOC` purge/decay block).
    This is the **expected drift** that causes the single conflict.
  - `ServerSettings.cpp`: unrelated churn.
- Upstream changes that touched the patch's behavior (symbols/codes):
  - `updateRSSPlusSwap` / `updateAllocatedPlusSwap` / `VmSwap` / exact `"MemorySwap"`
    string: **absent** on HEAD (`tmp/patch-041/identifier-inventory.log`). The
    `-S` search for an upstream equivalent (`tmp/patch-041/upstream-equivalent.log`)
    is **empty** → no upstream commit adds swap accounting / `MemorySwap`.
- Conclusion: **`still-needed-but-rewrite`** — semantics required and unchanged;
  6/7 files apply byte-clean, `MemoryWorker.cpp` requires a manual conflict
  resolution into the post-drift shape (see §6). `tmp/patch-041/drift-conclusion.txt`.

## 3. C++ review

Per `docs/aiven/skills/cpp-review-checklist.md`:

- 1 Lifetime + ownership: ✓ — `MemoryStatisticsOS memory_stat` is a stack local in
  `updateResidentMemoryThread`, lifetime bounded by the thread; no new heap ownership.
- 2 Exception safety: ✓ — the new `memory_stat.get()` call sits **inside** upstream's
  `try { … } catch(...)` loop body, so a throw from reading `/proc/self/status` is
  logged via `tryLogCurrentException` and the loop continues (no invariant broken).
- 3 Thread-safety + concurrency: ✓ — runs on the single `MemoryWorker` resident-memory
  thread under the existing `rss_update_lock`; `MemoryTracker::updateRSSPlusSwap`/
  `updateAllocatedPlusSwap` use the same atomic path as the originals. No new lock, no sleep.
- 4 Performance + memory: ✓ — `MemoryStatisticsOS::get` now scans `/proc/self/status`
  line-by-line (key:value) instead of positional `readIntText` over `/proc/self/statm`;
  it runs once per `rss_update_period_ms` tick on a background thread (not a per-row hot
  path), so the extra parsing cost is negligible. `buf_size` 1024→2048 to fit the larger file.
- 5 Settings as public API: ✓ — touches only the **doc string** of the existing
  `memory_worker_correct_memory_tracker` server setting (`ServerSettings.cpp`); no new
  setting, no default change.
- 6 Error handling: n/a — no new error code; the patch adds a metric and folds a value.
- 7 Upstream / vendored code: ✓ — no `contrib/**`, `.claude/**`, `.github/workflows/**`,
  or root `AGENTS.md` touched. 6 of 7 files applied byte-clean; the surrounding
  `MemoryWorker` refactor (try/catch + jemalloc purge block) was accounted for in the
  manual resolution.
- 8 Behavior under settings: 🚩(note, not blocker) — the `AsynchronousMetrics` path now
  folds swap into the tracker RSS (`updateRSSPlusSwap(data.resident + data.swap)`)
  **unconditionally** (no setting gate). This is an intended behavior change (policy
  call 4). It is a **no-op on swap-less hosts** (`VmSwap = 0` ⇒ `+0`), so it does not
  perturb CI and needs no default-off gate.

### Tech-debt note (policy call 3 — gratuitous duplication)

`MemoryTracker::updateRSSPlusSwap`/`updateAllocatedPlusSwap` are byte-for-byte clones of
`updateRSS`/`updateAllocated`; the "+swap" is performed entirely by the **caller**
(`resident + swap_bytes` / `data.resident + data.swap`). The clones add no logic of their
own. Ported verbatim for source fidelity, but recorded here as upstreaming/tech-debt: a
follow-up could collapse the clones (pass swap as a parameter, or have the caller add swap
before calling the original `updateRSS`/`updateAllocated`).

## 4. Test design

(a) **New stateless test that fails on the parent commit and passes after the patch.**

- Test path: `tests/queries/0_stateless/9041_swap_drift.{sql,reference}` (Aiven
  `9<NNN>` convention; no `add-test`). No tags (policy call 5 — the metric query is
  server-global and database-agnostic, so it is clean tag-free).
- Body: `SELECT count() > 0 FROM system.asynchronous_metrics WHERE metric = 'MemorySwap';`
  Reference: `1`.
- Why this distinguishes pre/post: `count()` always returns exactly one row, so there is
  no empty-vs-row ambiguity. Post-patch the `MemorySwap` metric is registered on every
  Linux async-metrics update ⇒ `count() = 1` ⇒ output `1` (= reference, PASS). Pre-patch
  the metric is never registered ⇒ `count() = 0` ⇒ output `0` (≠ reference `1`, FAIL).
  The assertion is on the metric's **PRESENCE**, so it is independent of whether the host
  has swap configured or any pages swapped — a clean, host-independent Aiven-specific
  differential. Precedent: `03010_virtual_memory_mappings_asynchronous_metrics.sql`,
  `03459_socket_asynchronous_metrics.sql`.

- Post-patch run output (PASS — `tmp/patch-041/test-postpatch.log`):

  ```text
  9041_swap_drift:                                                        [ OK ] 0.13 sec.
  1 tests passed. 0 tests skipped.
  ```

  Independently confirmed against the running post-patch server:
  `SELECT count() FROM system.asynchronous_metrics WHERE metric = 'MemorySwap'` → `1`.

- Pre-patch run output (the FAIL): **NOT captured in this dispatch** — see the
  evidence-gap note in §6. The pre-patch FAIL would require a second header-fanout
  rebuild (the worktree-flip), which exceeds the 45-minute dispatch budget. The
  differential is mechanically certain from the decomposition (pre-patch has no
  `"MemorySwap"` registration anywhere — `tmp/patch-041/identifier-inventory.log` shows
  exact `"MemorySwap"` = 0 on HEAD), but it is not yet empirically demonstrated.

### Coverage gap (intentional, documented)

The test proves the **swap-reading path + metric exposure are wired** (the `MemorySwap`
async metric appears). It does **NOT** prove the tracker correctly **enforces** the limit
including swap — CI cannot deterministically force pages into swap, so the load-bearing
half of the fix (folding swap into the tracker limit to prevent drift) is genuinely
un-testable here. This is analogous to patch-040's default-config-only gap and patch-037's
environment dependency. The observable side-effect (`MemorySwap` registration) is the best
host-independent differential available.

## 5. Rollback considerations

- Revert safety: safe. The patch changes only in-memory accounting and adds one async
  metric; **no schema migration, no on-disk format change, no ZK state**.
- State surviving restart: none.
- Disabling without rebuild: the swap-folding is not behind a dedicated setting; however
  it is a no-op when the host has no swap (`VmSwap = 0`). `memory_worker_correct_memory_tracker`
  continues to gate the `updateAllocated*` correction path as before.

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — predates this dossier.

### 25.8-aiven (historical, may be empty)

n/a — carried as `a05665ce57c…` (committer Joe Lynch); details not reconstructed here.

### 26.3-aiven (this uplift)

- Cherry-pick was: **conflict-resolved (rewrite)**. Exactly one conflict (`UU`) at
  `src/Common/MemoryWorker.cpp`, as forecast; the other 6 files auto-staged byte-clean.
- Conflict resolution (policy call 1): the upstream drift (`d47e737e472` try/catch +
  `dfddf71f251`/`28787fecc9f` `USE_JEMALLOC` purge/decay block) reshaped
  `updateResidentMemoryThread`. Re-targeted the patch's additions into the post-drift
  shape:
  - `MemoryStatisticsOS memory_stat;` under `#if defined(OS_LINUX) … #endif`, placed
    after `std::unique_lock rss_update_lock(…)` and before the `#if USE_JEMALLOC`
    purge-state block (above the `while (true)` loop).
  - `size_t swap_bytes = 0; #if defined(OS_LINUX) swap_bytes = memory_stat.get().swap; #endif`
    after `Stopwatch total_watch;` at the **post-drift +4 indentation** (12 spaces — the
    loop body now sits inside upstream's `try`).
  - `MemoryTracker::updateRSS(resident)` → `updateRSSPlusSwap(resident + swap_bytes)`.
  - All three `MemoryTracker::updateAllocated(resident, …)` (jemalloc `first_run||<0`
    branch, jemalloc `correct_tracker` branch, non-jemalloc `#else` branch) →
    `updateAllocatedPlusSwap(resident + swap_bytes, …)`.
  - Upstream's `try/catch` and the `USE_JEMALLOC` purge/decay block are preserved intact.
- Upstream-drift conclusion: `still-needed-but-rewrite` (§2).
- `byte_equivalent: false`. Patch-id mismatch decomposed
  (`tmp/patch-041/decomposition.log`): **every** added/removed difference vs source is
  **leading-whitespace only** (the +4 indentation of the 6 swap lines in `MemoryWorker.cpp`).
  All identifiers/logic/comments are token-identical ⇒ Tier 2 semantically green, no
  `semantic_conflict`.
- Test added at: `tests/queries/0_stateless/9041_swap_drift.{sql,reference}`.
- Time-to-port: build directory was **warm-cache** (freshly repaired by parent). The
  post-patch incremental build was ~889 ninja steps (`MemoryTracker.h` +
  `MemoryStatisticsOS.h` header fanout), ~25 minutes (11:50→12:14 UTC), `ninja exit: 0`.

#### Two open items at escalation (this dispatch, T3.20)

1. **Step 2.5 style cleanup (brace-drop) NOT applied — tooling blocker.** Policy call 2
   asked to drop the K&R braces the source introduces in `AsynchronousMetrics.cpp`:
   ```cpp
   if (update_rss) {
       MemoryTracker::updateRSSPlusSwap(data.resident + data.swap);
   }
   ```
   → brace-less single-statement form. This edit was **blocked**: the `preToolUse`
   `Write|Edit` hook `.cursor/hooks/deny-upstream-file-writes.sh` failed to **spawn**
   with `E2BIG` (OS argument/environment size limit) because `AsynchronousMetrics.cpp`
   is 109,673 bytes — the runtime passes the file content to the hook process and the
   single-string size exceeds `MAX_ARG_STRLEN` (~128 KB). This is an **infrastructure
   crash, not a policy denial** (the file is not on the never-touch list; the hook's own
   logic would `allow` it). Confirmed file-size-driven by a controlled diagnostic (a tiny
   scratch-file edit succeeded; the four `MemoryWorker.cpp` edits at 23 KB succeeded). The
   worker did **not** bypass the deny hook via shell `sed`/`echo` (forbidden). Net effect:
   the staged `AsynchronousMetrics.cpp` is **byte-identical to source** (K&R braces
   retained). This is functionally identical (braces do not change codegen) but will be
   flagged by the CI Allman/style check. **Remaining manual step for the human:** drop the
   two braces in the `if (update_rss)` block.
2. **Pre-patch FAIL evidence NOT captured — budget.** Capturing it requires the
   worktree-flip's second header-fanout rebuild (~20 min), which would push the dispatch
   past the 45-minute hard cap. The worker did not flip (worktree remains post-patch,
   nothing to restore). Post-patch PASS is captured; the pre-patch differential is
   mechanically certain (`"MemorySwap"` = 0 on HEAD) but not yet empirically shown.
- Anything surprising: the hook `E2BIG` spawn failure on large files is a previously
  unseen tooling limitation worth surfacing to the orchestration layer.

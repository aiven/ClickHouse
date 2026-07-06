# Patch 069 — zk-uptime-keeper-mntr

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| earlier-aiven | `48412e93fad` (and siblings, PR aiven#26 `joelynch/mntr-uptime`) | Joe Lynch | original carry (recurring Aiven patch across uplifts) |
| 25.8-aiven | `1502b77d882a50a4431e124e3876a9adb36b5e4c` | Joe Lynch | carried |
| 26.3-aiven | (staged) | T3 worker (parent-direct) | conflict-free (3-way auto-merge absorbed context drift) |

The current uplift's row stays "(staged)" until the human commits.

## 1. Purpose

Keeper's `mntr` four-letter-word (4LW) command exposes server metrics scraped by
monitoring (the same surface as ZooKeeper's `mntr`). This patch adds a `zk_uptime`
field reporting, in milliseconds, how long the current RAFT role has been held. The
timer resets on process start and on every role transition — `BecomeLeader` and
`BecomeFollower` — so it measures *role* uptime, not process uptime. Aiven's fleet
monitoring uses it to detect Keeper instances that are flapping (re-electing or
re-syncing) by watching for an uptime that keeps resetting to near-zero.

Source SHA on `v25.8.18.1-lts-aiven`: `1502b77d882a50a4431e124e3876a9adb36b5e4c`
(from `docs/aiven/uplifts/26.3/inventory.md` row 069).
Original author: `joelynch112@gmail.com`.
Original purpose (verbatim):

> Add zk_uptime to Keeper mntr four-letter command
>
> Report uptime in milliseconds via the mntr command. The timer resets
> on process start, leader election, and follower reconnection (whenever
> the RAFT role changes via BecomeLeader/BecomeFollower callbacks).

## 2. Upstream-drift findings

### Commands run

```bash
git log --oneline v25.8.18.1-lts..v26.3.10.62-lts -- \
  src/Coordination/FourLetterCommand.cpp src/Coordination/Keeper4LWInfo.h \
  src/Coordination/KeeperServer.cpp src/Coordination/KeeperServer.h        # 40 commits
git log --oneline v25.8.18.1-lts..v26.3.10.62-lts --grep 'uptime'          # (empty)
```

### Findings

- Upstream changes to touched files between prior and current LTS (40 commits; relevant ones):
  - `Keeper4LWInfo.h`: gained a new member `synced_non_voting_follower_count` between
    `synced_follower_count` and `getRole`. This is the only textual drift at the patch's
    insertion site; the 3-way merge placed `uptime_ms{0}` exactly where intended (before
    `getRole`).
  - `KeeperServer.cpp` `getPartiallyFilled4LWInfo`: `5000f02cef8` ("use single leader
    snapshot when filling 4lw counts") reshaped the surrounding lines (`is_standalone`
    assignment moved adjacent to the insertion point). The patch's anchor
    (`is_exceeding_mem_soft_limit` → `return result;`) is intact; auto-merge clean.
  - `test_keeper_four_word_command/test.py`: `bb2d29b81e1` ("Fix data race in follower
    metrics and relax `test_cmd_mntr` assertion") touched this test upstream; the patch's
    added assertion auto-merged cleanly (and the source patch's trailing-newline fix was a
    no-op because upstream already added the final newline).
- Upstream changes that touched the patch's behavior (symbols, error codes, callers):
  - `zk_uptime` / `uptime` in `mntr`: **no equivalent upstream** — `git grep 'uptime'` over
    `src/Coordination/` on HEAD is empty; the `--grep 'uptime'` log between LTSes is empty.
    The patch is genuinely Aiven-only.
- Conclusion: `still-needed-and-applies` — identifiers present, no upstream equivalent,
  cherry-pick auto-merged with no `UU` conflicts. (The stable patch-id differs from the
  source only because of the two context-drift sites above; the decomposition check shows
  no semantic delta.)

## 3. C++ review

- 1 Lifetime + ownership: ✓ — `role_start_time` is a plain `std::atomic` member of
  `KeeperServer`; no heap, no references escaping. `Keeper4LWInfo::uptime_ms` is a value
  copied into the result struct.
- 2 Exception safety: ✓ — additions are `noexcept` operations (atomic store/load,
  `steady_clock::now`, integer arithmetic); no new throw sites, no rollback concern.
- 3 Thread-safety + concurrency: ✓ — `role_start_time` is written from the NuRaft callback
  thread (`callbackFunc`, `BecomeLeader`/`BecomeFollower`) and read from the 4LW-serving
  thread (`getPartiallyFilled4LWInfo`); the `std::atomic` with `memory_order_relaxed` is the
  correct discipline for a single scalar with no companion invariant (a slightly stale
  read across a role flip is acceptable for a monitoring counter). `std::atomic<steady_clock::time_point>`
  is valid: `time_point` wraps a single integral `duration`, so it is trivially copyable and
  lock-free on 64-bit.
- 4 Performance + memory: ✓ — one extra 8-byte atomic member on `KeeperServer`; per-`mntr`
  cost is one atomic load + one subtraction. Negligible.
- 5 Settings as public API: n/a — no new setting; `zk_uptime` is a monitoring output field,
  additive (existing `mntr` consumers ignore unknown keys).
- 6 Error handling: n/a — no error paths added.
- 7 Upstream / vendored code: ✓ — uses NuRaft's existing `cb_func::BecomeLeader`/
  `BecomeFollower` callback enum; no `contrib/` edits.
- 8 Behavior under settings: n/a — unconditional, no gating.

## 4. Test design

(a) **New assertion in an existing integration test that fails on the parent commit and passes after the patch.**

- Test path: `tests/integration/test_keeper_four_word_command/test.py` (`test_cmd_mntr`).
  This is an *upstream* suite, not an Aiven `test_aiven_*` test, because the patch extends an
  existing upstream Keeper 4LW test rather than introducing new cluster topology. `mntr` is a
  Keeper-protocol 4LW command, not SQL-observable, so a stateless `.sql`/`.sh` test cannot
  reach it (AGENTS §7 "genuinely cluster-level" carve-out).
- Pre-patch run output (the FAIL):

  ```text
  >       assert int(result["zk_uptime"]) > 0
  E       KeyError: 'zk_uptime'
  FAILED test_keeper_four_word_command/test.py::test_cmd_mntr - KeyError: 'zk_u...
  ================= 1 failed, 19 deselected, 4 warnings in 7.68s =================
  ```

- Post-patch run output (the PASS):

  ```text
  PASSED                                                                   [100%]
  ================= 1 passed, 19 deselected, 3 warnings in 7.23s =================
  ```

- Why this test distinguishes the Aiven gate from upstream behavior (per AGENTS.md §7): the
  failure is the *specific* missing key `zk_uptime` (a `KeyError`, not a shared error code) —
  upstream `mntr` emits no such field, so the assertion can only pass when this patch's
  `print(ret, "uptime", ...)` is present. Asserting `> 0` further confirms the timer is wired
  to a real clock, not a stubbed constant.

## 5. Rollback considerations

- Revert is safe: the patch adds an output field and an in-memory atomic member only. No
  schema migration, no on-disk format change, no ZK node creation.
- No state survives a restart: `role_start_time` is initialized to `now()` at construction
  and reset on role transitions; nothing is persisted.
- Disabling without rebuild: n/a (no setting). A monitoring consumer that does not want the
  field simply ignores the `zk_uptime` key; `mntr` consumers tolerate unknown keys by design.

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — predates this dossier; the patch originates from Aiven PR #26 (`joelynch/mntr-uptime`,
SHA family `48412e93fad`/`2c9145e00bc`/…) and has been carried forward across uplifts.

### 25.8-aiven (historical, may be empty)

Carried as `1502b77d882a50a4431e124e3876a9adb36b5e4c` (the source for this 26.3 port).

### 26.3-aiven (this uplift)

- Cherry-pick was: clean (3-way auto-merge; no `UU` markers). Auto-merge absorbed two
  context-drift sites (`Keeper4LWInfo.h` new sibling member; `getPartiallyFilled4LWInfo`
  reshaped by `5000f02cef8`); the source patch's trailing-newline fix to `test.py` dropped
  out because HEAD already had the final newline. `byte_equivalent: false` (stable patch-id
  differs), decomposition shows no semantic delta.
- Upstream-drift conclusion: `still-needed-and-applies` (from §2).
- Test added at: `tests/integration/test_keeper_four_word_command/test.py` (assertion added
  to `test_cmd_mntr`; evidence pair captured via worktree-flip).
- Time-to-port: ~15 min wall-clock, **warm-cache** (sccache hot; post-patch build ~64 s,
  pre-patch incremental similar, restore build link-bound ~7 min under integration-container
  load). Integration test runs ~7-8 s each.
- Anything surprising: the restore (Step 6f) link of `libdbms.a` + `clickhouse` took
  noticeably longer than the two compile-bound builds, likely due to concurrent
  integration-test container load on the host; not a correctness signal.

# Patch 047 — per-server-max-bytes-merge-mutate-override

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | n/a — predates tracked lineage (originally carried as patch file `0078-Global_merge_and_mutate_override.patch`) | Tilman Moeller / Kevin Michel | original carry |
| 25.8-aiven | `4a05c78da7015f57f5041b4e50b1925261480db6` | Tilman Moeller (author) / Joe Lynch (committer) | carried |
| 26.3-aiven | `patch-port(047)` (staged) | T3.18 worker | rewritten (function rename; obsoleted lock hunk dropped) |

The current uplift's row stays the stable handle `patch-port(047)` until the human commits.

## 1. Purpose

Adds two **server settings** — `max_bytes_to_merge_override` and
`max_bytes_to_mutate_override`, both default `0` (= unlimited) — that cap the
size of background merge / mutate tasks a node will *create and execute*,
without touching per-table `MergeTree` settings.

Why Aiven needs it (the durable part): during a maintenance upgrade we want a
node to keep doing the *small* merges that keep part counts sane (and to keep
running TTL deletes), but we do NOT want it to start large merges/mutations that
would slow down `SYSTEM SYNC REPLICA … LIGHTWEIGHT` (large `MERGE_PARTS` tasks
block overlapping `GET_PART` tasks). The normal `MergeTree` knobs
(`max_bytes_to_merge_at_*`) can't be used because they are applied per-table via
`ALTER TABLE`, are replicated, and become persisted into the table definition —
they would step onto user-managed objects and apply fleet-wide. A per-server
override is set only on the nodes being upgraded and never persists into table
metadata. It is only safe together with a `LIGHTWEIGHT` sync (if we skip tasks
we must also not wait for them).

Source SHA on `v25.8.18.1-lts-aiven`: `4a05c78da7015f57f5041b4e50b1925261480db6`.
Original author: `Tilman Moeller <tilman.moeller@aiven.io>` (committed by Joe Lynch).
Original purpose (quoted from the source commit body):

> During a maintenance upgrade, we do not wait for the completion of merges and
> mutations since they are not required … Some merges and mutations prevent the
> execution of `GET_PART` tasks that overlap the range of the merge or the
> mutation. … We can't use the normal merge tree settings like
> `max_bytes_to_merge_at_min/max_space_in_pool` because they would be applied
> with `ALTER TABLE` … and become persisted … They are also replicated … The
> patch implements per-server overrides … This is only usable with a
> `LIGHTWEIGHT` sync.

## 2. Upstream-drift findings

> Verifies the patch is still SEMANTICALLY correct against `v26.3.10.62-lts`.

### Commands run

```bash
git grep -c <new-symbols> -- src/ programs/        # all 0 => patch not present
git grep -c getMaxSourcePartsBytesForMerge -- src/ # 12 => renamed fn present
git grep -c getMaxSourcePartBytesForMutation -- src/ # 5  => renamed fn present
git log v25.8.18.1-lts..v26.3.10.62-lts --oneline --grep 'max_bytes_to_merge_override'  # empty
```

### Findings

- Upstream changes to touched files between prior and current LTS:
  - `src/Storages/MergeTree/Compaction/CompactionStatistics.cpp`: the two target
    functions were **renamed** `Size`→`Bytes`:
    `getMaxSourcePartsSizeForMerge` → `getMaxSourcePartsBytesForMerge`, and
    `getMaxSourcePartSizeForMutation` → `getMaxSourcePartBytesForMutation`. The
    mutation function also drifted: it now uses
    `Int64 number_of_free_entries_in_pool_to_execute_mutation`, an explicit
    `static_cast<double>(disk_space)` cast, and a log-comment that reads `<`
    (the source patch read `>=`).
  - `src/Interpreters/Context.cpp`: `setMaxPendingMutationsToWarn` is **already**
    `std::lock_guard` on HEAD (`Context.cpp:5499`) — the source patch's
    `SharedLockGuard`→`std::lock_guard` flip is obsoleted-by-upstream.
  - `programs/server/Server.cpp`, `src/Core/ServerSettings.cpp`,
    `src/Interpreters/Context.h`: anchors stable; additive only.
- Upstream changes that touched the patch's behavior:
  - No upstream equivalent of `max_bytes_to_{merge,mutate}_override` exists
    (grep empty); the per-server override is still genuinely missing on 26.3.
- Conclusion: **`still-needed-but-rewrite`** — the semantics are unchanged and
  still needed, but the source diff cannot apply verbatim because the target
  functions were renamed and one hunk (the lock flip) is already upstream. The
  change was hand-applied into the renamed functions; the lock hunk dropped.

## 3. C++ review

- 1 Lifetime + ownership: ✓ — overrides are plain `UInt64` value members on
  `ContextSharedPart`; no ownership/lifetime concerns. `data.getContext()`
  returns the shared global context already used by the surrounding code.
- 2 Exception safety: ✓ — getters/setters take the lock and do a trivial scalar
  read/write (no-throw); the merge/mutate edits add only arithmetic + `std::min`.
- 3 Thread-safety + concurrency: ✓ — getters use `SharedLockGuard` (shared read)
  and setters use `std::lock_guard` (exclusive write) on `shared->mutex`,
  matching the established `getMax*ToWarn` pattern. The reads happen on the
  background merge/mutate scheduler threads; the write happens on config (re)load.
- 4 Performance + memory: ✓ — one shared-locked scalar read per merge/mutate
  selection (already a heavyweight, infrequent code path); negligible.
- 5 Settings as public API: ✓ — two new server settings declared in
  `ServerSettings.cpp` with help text and default `0`; wired in `Server.cpp`'s
  config-apply block exactly like its neighbors.
- 6 Error handling: ✓ — no new error paths; `0` is interpreted as "unlimited"
  (the `!= 0` guard), preserving the prior behavior when unset.
- 7 Upstream / vendored code: ✓ — no `contrib/` or upstream-owned files touched.
- 8 Behavior under settings: ✓ — default `0` ⇒ clause (v) does not fire; the cap
  is inert unless an operator sets it. The new `#include <Core/ServerSettings.h>`
  in `CompactionStatistics.cpp` is required for `data.getContext()->getMax…`.

## 4. Test design

(a) **New integration test that fails on the parent commit and passes after the patch.**

- Test path: `tests/integration/test_aiven_per_server_max_bytes_merge_mutate_override/`
  (`test.py` + `configs/overrides.xml`), per the Aiven integration-test naming
  convention (`docs/aiven/runbooks/integration-tests.md §6`:
  `test_aiven_<slug>`). Integration (not stateless) because the feature is two
  **server settings** supplied via a config file at startup — unsafe to set on
  the shared stateless server.
- Shape: two single-node servers — `capped` (with `overrides.xml` setting
  `max_bytes_to_merge_override=5 MiB`, `max_bytes_to_mutate_override=2 MiB`) and
  `uncapped` (defaults, `0`=unlimited) — fed identical data.
  - **Merge cap:** `SYSTEM STOP MERGES`, insert 6 × ~2 MiB parts
    (`String CODEC(NONE)` + fixed payload ⇒ deterministic on-disk size, ~12 MiB
    total), `SYSTEM START MERGES`. The uncapped control collapses to **1** active
    part; the capped node can never reach a single ~12 MiB part (any merge whose
    sources sum to > 5 MiB is rejected) so it stays at **>1**. Assertion via
    `system.parts` active count.
  - **Mutate cap:** one ~8 MiB part; an async `ALTER … UPDATE`. On `uncapped` the
    mutation completes (`system.mutations.is_done=1`); on `capped` it stays
    pending (`is_done=0`) because the part exceeds the 2 MiB mutate cap.
- **PITFALL avoided (iv-a):** `OPTIMIZE TABLE` selects parts via
  `selectAllPartsToMergeWithinPartition`, which BYPASSES the cap. The test drives
  the **background** scheduler (insert + bounded-wait), the only path that calls
  `getMaxSourcePartsBytesForMerge` / `getMaxSourcePartBytesForMutation`.
- Pre-patch run output (the FAIL):

  ```text
  capped   parts(count,sum,max) = 6	12733775	2122299
  after merge window: capped = 1 uncapped = 1
  E   AssertionError: max_bytes_to_merge_override should prevent a full merge, but capped node collapsed to 1 active part(s)
  E   assert 1 > 1
  ...
  E   AssertionError: max_bytes_to_mutate_override should keep the mutation pending on a part larger than the cap, but it completed
  E   assert '1' == '0'
  ======================== 2 failed, 3 warnings in 20.48s ========================
  ```

- Post-patch run output (the PASS):

  ```text
  capped   parts(count,sum,max) = 6	12733775	2122299
  after merge window: capped = 6 uncapped = 1
  capped   t_mut(count,sum,max) = 1	8487629	8487629
  ======================== 2 passed, 2 warnings in 17.10s ========================
  ```

- Why this distinguishes the Aiven gate from upstream behavior: on the pre-patch
  binary the two settings are **unknown** and silently ignored by the server, so
  the `capped` node behaves identically to `uncapped` (merges to 1 / completes
  the mutation) — the assertions fail. Only the patched code makes the cap bind.
  Byte-equivalence is impossible (the patch was rewritten onto renamed
  functions), so the evidence is the behavioral pre-fail/post-pass pair.

## 5. Rollback considerations

- Revert safety: safe. The patch adds two opt-in server settings and read-only
  caps in the merge/mutate size calculators. No schema migration, no on-disk
  format change.
- Persisted state: none. The override lives only in `ContextSharedPart` memory,
  set from config at (re)load; nothing survives a restart beyond the config file.
- Disable without rebuild: set both settings back to `0` (the default) in the
  server config and reload — the `!= 0` guard makes them inert.

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — predates tracked dossier lineage; originally carried as the patch file
`0078-Global_merge_and_mutate_override.patch`.

### 25.8-aiven (historical, may be empty)

Carried as `4a05c78da7015f57f5041b4e50b1925261480db6` (author Tilman Moeller,
committer Joe Lynch). At that LTS the target functions were still named
`getMaxSourcePartsSizeForMerge` / `getMaxSourcePartSizeForMutation`, and the
patch additionally flipped `setMaxPendingMutationsToWarn`'s lock from
`SharedLockGuard` to `std::lock_guard`.

### 26.3-aiven (this uplift)

- Cherry-pick was: **rewritten** (not a `git cherry-pick`). The source diff
  cannot apply because upstream renamed the two target functions
  `Size`→`Bytes`. The additive hunks (Server.cpp extern + config-apply,
  ServerSettings.cpp DECLAREs, Context.cpp members + get/set, Context.h decls)
  were hand-applied at the verified anchors; the semantic core was retargeted
  into `getMaxSourcePartsBytesForMerge` (3-arg overload) and
  `getMaxSourcePartBytesForMutation`. The source's
  `setMaxPendingMutationsToWarn` lock-flip hunk was **dropped** — it is already
  `std::lock_guard` on HEAD (`Context.cpp:5499`), obsoleted-by-upstream.
- Preserved HEAD's drift in the mutation function: the explicit
  `static_cast<double>(disk_space)` cast and HEAD's `<` log-comment wording
  (the source used `>=`).
- Upstream-drift conclusion: `still-needed-but-rewrite` (see §2).
- `byte_equivalent: false` — EXPECTED for a planned rewrite; the differing
  `git patch-id --stable` is the function rename + dropped lock hunk + preserved
  HEAD mutation-fn structure, NOT a semantic conflict.
- Test added at: `tests/integration/test_aiven_per_server_max_bytes_merge_mutate_override/`.
- Time-to-port: warm-cache build dir (post-patch full build ~83 s; pre/post
  incremental rebuilds ~28 s each).
- Anything surprising: the `StrReplace` editor tool tripped the
  `deny-upstream-file-writes.sh` hook with `spawn E2BIG` on the large files
  (`Server.cpp`, `Context.cpp`); applied edits via a Python-`Shell` substitution
  per `integration-tests.md §7.4.b`. Also, under the merge cap the background
  selector left the capped node at all 6 parts (it found no cap-eligible merge
  "worthwhile" in the window) rather than fusing pairs — harmless, the load-
  bearing assertion is "capped can never reach 1 active part", which holds.

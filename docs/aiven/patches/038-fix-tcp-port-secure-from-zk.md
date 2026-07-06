# Patch 038 — fix-tcp-port-secure-from-zk

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | n/a | n/a | not present — first carried on `25.8-aiven` (authored 2026-01-05) |
| 25.8-aiven | `a498627944a19ee0016de62297c8ecbcaf3cd342` | Tilman Moeller (author) / Joe Lynch (committer) | original carry |
| 26.3-aiven | (staged) | T3.16 (parent-direct finish after worker interruption) | conflict-free auto-merge (`byte_equivalent: false`, context-drift only) |

The current uplift's row stays "(staged)" until the human commits.

## 1. Purpose

`Context::getTCPPortSecure` is changed to read the secure port from the registered `server_ports` map (via the new non-throwing `tryGetServerPort`) instead of reading it directly from the static configuration file. Aiven needs this because in their deployment the secure port can be obtained from a dynamic source (ZooKeeper) rather than from static config; a config-only read returns the wrong (or empty) value when the port is registered dynamically. The patch also closes a real data race by write-locking `registerServerPort` (`server_ports` was previously mutated without holding `shared->mutex`).

Source SHA on `v25.8.18.1-lts-aiven`: `a498627944a19ee0016de62297c8ecbcaf3cd342` (from `docs/aiven/uplifts/26.3/inventory.md` row 038).
Original author: `tilman.moeller@aiven.io` (committer `joelynch112@gmail.com`), 2026-01-05.
Original purpose (quoted, not paraphrased):

```
Fix tcp_port_secure from ZK

This patch fixes getTCPPortSecure() to read the port from the registered
server ports map instead of directly from the configuration file. This
ensures it works correctly when the port is obtained from ZooKeeper or
other dynamic sources, not just from static configuration.

The patch also improves thread safety by adding proper mutex protection
for the server_ports map and introduces a new tryGetServerPort() helper
method for non-throwing port lookups.

Co-authored-by: Kevin Michel <kevin.michel@aiven.io>
```

## 2. Upstream-drift findings

### Commands run

```bash
git log --oneline v25.8.18.1-lts..v26.3.10.62-lts -- src/Interpreters/Context.cpp src/Interpreters/Context.h
# + per-identifier grep on HEAD: tryGetServerPort, getTCPPortSecure, registerServerPort,
#   getServerPort, server_ports, SharedLockGuard, BAD_GET, CLUSTER_DOESNT_EXIST
git show a498627944 | git patch-id --stable      # source patch-id
git diff --cached | git patch-id --stable        # staged patch-id
```

### Findings

- Upstream changes to touched files between prior and current LTS:
  - `src/Interpreters/Context.cpp`: 494 touching commits in the range, but none in the patched neighborhood. The three patched functions (`getTCPPortSecure`, `registerServerPort`, `getServerPort`) are present verbatim in their pre-patch shape on HEAD (line numbers shifted ~4793→~5437, normal). One benign context-line change: upstream wrapped the neighboring `getTCPPort` return in `static_cast<UInt16>(config.getInt("tcp_port", DBMS_DEFAULT_PORT))` — this is OUTSIDE the patch hunk and is the only non-context entry in the decomposition.
  - `src/Interpreters/Context.h`: `getServerPort` decl present; the patch inserts the `tryGetServerPort` decl after it.
- Upstream changes that touched the patch's behavior (symbols, error codes, callers):
  - `tryGetServerPort`: **0 matches on HEAD** → the patch is genuinely still needed, not superseded by an upstream equivalent.
  - `BAD_GET`: defined in `src/Common/ErrorCodes.cpp` (code 170); NOT yet `extern`'d in `Context.cpp` → the patch's `extern const int BAD_GET;` is new there (no duplicate-extern hazard).
  - `CLUSTER_DOESNT_EXIST` (code 701): still the pre-patch unknown-port error; the patch replaces it with `BAD_GET`.
- Patch-id: source `4ba49008e3a1ce041c940c7d0638e551b388beab` ≠ staged `8f38b92b11da848480ad1318c6fe758073518845` → `byte_equivalent: false`. The `decomposition-strict` (semantic) delta is **empty**; the only diff vs. source is line-number/context drift plus the upstream `static_cast<UInt16>` on the neighboring context line. No semantic difference.
- Conclusion: **`still-needed-and-applies`** — proceed with cherry-pick (auto-merged clean, no conflict markers).

## 3. C++ review

Per `docs/aiven/skills/cpp-review-checklist.md`:

- 1 Lifetime + ownership: ✓ — no ownership change. `tryGetServerPort` returns a copied `std::optional<UInt16>` (value type); no references escape the lock.
- 2 Exception safety: ✓ — `getServerPort` now throws `BAD_GET` only after the lock is released (the `tryGetServerPort` `SharedLockGuard` is scoped to that call); no throw while holding the lock; strong guarantee preserved.
- 3 Thread-safety + concurrency: ✓ — this is the load-bearing fix. `registerServerPort` now write-locks `shared->mutex` with `std::lock_guard` around the `emplace` (previously an unlocked mutation — a real data race against concurrent readers). `tryGetServerPort` read-locks with `SharedLockGuard`. The shared invariant is the `server_ports` map; the read/write lock pairing on the same `shared->mutex` is correct.
- 4 Performance + memory: ✓ — port lookups are rare (startup + occasional `getServerPort()` SQL calls); the added shared-lock is negligible. `UInt16` by value, no allocation.
- 5 Settings as public API: n/a — no new setting.
- 6 Error handling: ✓ — error-code change `CLUSTER_DOESNT_EXIST` → `BAD_GET` for the unknown-port case. `BAD_GET` ("requested a value that does not exist") is the semantically correct code for "no such port name". This is the SQL-observable gate (see §4).
- 7 Upstream / vendored code: n/a — no `contrib/` change.
- 8 Behavior under settings: ✓ — behavior is unconditional (no setting gate); the only user-visible change is the error code for an unknown port name.

## 4. Test design

(a) **New stateless test that fails on the parent commit and passes after the patch.**

- Test path: `tests/queries/0_stateless/9038_fix_tcp_port_secure_from_zk.sql` + `.reference` (Aiven `9<NNN>` naming; `9038` = patch 038).
- The test gates on the **error-code transition** surfaced through the `getServerPort()` SQL function: an unknown port name throws `BAD_GET` post-patch but `CLUSTER_DOESNT_EXIST` pre-patch. A known port (`tcp_port`) resolving through `server_ports` is a sanity anchor that passes both pre and post.
- Pre-patch run output (the FAIL):

  ```text
  9038_fix_tcp_port_secure_from_zk: ... FAIL
  Code: 701. DB::Exception: There is no port named this_port_does_not_exist:
    In scope SELECT getServerPort('this_port_does_not_exist'). (CLUSTER_DOESNT_EXIST)
  (query: SELECT getServerPort('this_port_does_not_exist'); -- { serverError BAD_GET })
  result: 1
  Having 1 errors! 0 tests passed.
  ```

- Post-patch run output (the PASS):

  ```text
  9038_fix_tcp_port_secure_from_zk:                                       [ OK ] 0.25 sec.
  1 tests passed. 0 tests skipped.
  ```

- Why this test distinguishes the Aiven gate from upstream behavior (per AGENTS.md §7): `BAD_GET` is unreachable through `SELECT getServerPort(...)` on any code path other than this patch (its only other producer, `Field.cpp`'s type-mismatch, is not reachable here), so the error code alone unambiguously identifies the Aiven change — no message-substring assertion is required.

**Coverage limitation (honest scoping).** The patch's *headline* behavior — `getTCPPortSecure` reading the bound port from the `server_ports` map instead of static config — is NOT divergent on a stock test server: when `tcp_port_secure` is in config the listener registers the same value (config-read == map-read); when it is absent neither source has it (both return `{}`). The two sources only diverge in Aiven's dynamic-port deployment (port registered from ZK/dynamic source but absent from static config), which a stock `.sql`/integration harness cannot reproduce. The test therefore gates on the deterministic, in-commit, SQL-observable error-code transition rather than on the ZK/dynamic-port path. This is stated plainly so the coverage is not overclaimed.

## 5. Rollback considerations

- Revert safety: **safe.** Pure in-memory code refactor — no schema migration, no on-disk format change, no ZK nodes created.
- Surviving state: **none.** No state survives a server restart; `server_ports` is rebuilt at startup from the listener registration.
- Disable-without-rebuild: n/a — no setting gate. To revert behavior you revert the commit and rebuild. (The only externally visible change is the unknown-port error code, which is not configurable.)

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — patch not present on 25.3-aiven.

### 25.8-aiven (historical, may be empty)

Original carry; authored 2026-01-05 by Tilman Moeller, co-authored by Kevin Michel, committed by Joe Lynch.

### 26.3-aiven (this uplift)

- Cherry-pick was: **clean auto-merge** (`git cherry-pick --no-commit -x`; "Auto-merging" both files, no `UU` markers).
- Upstream-drift conclusion: `still-needed-and-applies` (§2) — no upstream `tryGetServerPort`, patched neighborhood unchanged; only the neighboring `getTCPPort` context line gained an upstream `static_cast<UInt16>`.
- Test added at: `tests/queries/0_stateless/9038_fix_tcp_port_secure_from_zk.sql` (+ `.reference`).
- Time-to-port: dominated by build recovery, not the port itself. The initial worker dispatch was interrupted by an IDE window reload mid-build; the build directory was then in the recurring `CMakeFiles/rules.ninja`-missing corruption state, which forced a `cmake --fresh` and a one-time full `contrib` rebuild (cold-cache, ~4714 steps). After that completed, both worktree-flip rebuilds were **warm-cache incrementals** (85 steps / ~100 s and ~39 s) — confirming the full rebuild was a one-off artifact of `--fresh`, not the norm.
- Anything surprising: (1) the recurring `rules.ninja` deletion between patch builds is what forces `cmake --fresh` (which resets ninja's build graph → full `contrib` rebuild); for code-only patches a plain `ninja` incremental is correct and `--fresh` should be avoided. (2) The stateless runner failed to connect (`ACCESS_DENIED` on `system.build_options`) because the stock `programs/server/users.xml` `default` user has its `<grants>` block commented out and relies on persisted SQL-access-control storage that had been wiped; the fix is to point the test server at a scratch `users.xml` whose `default` user has `GRANT ALL ON *.* WITH GRANT OPTION` (and no `access_management`/`named_collection_control`, which are mutually exclusive with `<grants>`).

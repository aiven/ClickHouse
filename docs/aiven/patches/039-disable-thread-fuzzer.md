# Patch 039 — disable-thread-fuzzer

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `32e9abc159` | Tilman Moeller (author) / Joe Lynch (committer) | (the version we are porting FROM) |
| 26.3-aiven | `patch-port(039)` | parent agent, 2026-05-31 | ported, `no_justified` (compile-time instrumentation toggle; backed by artifact-level nm evidence — see §4) |

## 1. Purpose

Disable the `ThreadFuzzer` pthread-wrapping feature by replacing the
platform/sanitizer conditional with an unconditional
`#define THREAD_FUZZER_WRAP_PTHREAD 0` in `src/Common/ThreadFuzzer.cpp`.

`ThreadFuzzer` is a *test-only* instrumentation facility: when enabled (via
`THREAD_FUZZER_*` env vars / settings, typically in CI stress tests) it injects
sleeps/yields/migrations around thread-synchronization points to surface race
conditions. The "pthread wrapping" sub-feature interposes the public
`pthread_mutex_lock` / `pthread_mutex_unlock` symbols to add those injection
points. It has no effect in normal operation and no SQL surface.

Source SHA on `v25.8.18.1-lts-aiven`: `32e9abc159`.
Original author: `tilman.moeller@aiven.io`. Co-author: Kevin Michel.
Original purpose (verbatim from the source commit body):

> Disable thread fuzzer
>
> This patch disables the ThreadFuzzer pthread wrapping feature by always
> setting THREAD_FUZZER_WRAP_PTHREAD to 0, regardless of platform or sanitizer
> settings.
>
> The pthread wrapping feature has compatibility issues with newer glibc
> versions (especially glibc 2.36+) and is a testing feature that should
> not be enabled in production builds. This patch simplifies the code by
> removing conditional compilation logic and ensuring consistent behavior
> across all platforms.

## 2. Upstream-drift / validity findings

### Commands run

```bash
# HEAD still carries the conditional the patch targets?
git show HEAD:src/Common/ThreadFuzzer.cpp | sed -n '27,38p'
#   #if defined(OS_LINUX) && !defined(THREAD_SANITIZER) && !defined(MEMORY_SANITIZER)
#       #define THREAD_FUZZER_WRAP_PTHREAD 1
#   #else
#       #define THREAD_FUZZER_WRAP_PTHREAD 0
#   #endif
#   → unchanged; patch applies cleanly.

# patch-id equivalence
git show 32e9abc159 | git patch-id --stable                       # 754707e4b29e...
git diff --cached -- src/Common/ThreadFuzzer.cpp | git patch-id --stable  # 754707e4b29e... (MATCH)
```

### Findings

- **Still applies & byte-equivalent.** Clean `cherry-pick -x`; stable patch-id
  identical to source (`byte_equivalent: true`).
- **Not a no-op (contrast 044).** On the Aiven build (Linux, `RelWithDebInfo`,
  no sanitizer) the HEAD conditional evaluates to `1`, so the interposers are
  compiled into the shipped binary. The patch forces `0`, removing them. Proven
  at the artifact level (§4): the `pthread_mutex_lock` / `pthread_mutex_unlock`
  text symbols and 16 tuning statics are present in the pre-patch object and
  absent in the post-patch object.
- **No obsolescence.** Upstream has not gated the wrapping on glibc version; the
  conditional is unchanged from when the patch was written.
- **Rationale corroborated.** The pre-patch object's undefined reference
  `U __pthread_mutex_lock@GLIBC_2.17` shows the interposer overrides the public
  `pthread_mutex_lock` and forwards to the glibc-versioned symbol — exactly the
  symbol-versioning interposition the commit body cites as breaking on
  glibc 2.36+.

## 3. C++ review

- 1 Lifetime + ownership: `n/a — preprocessor toggle; removes a conditionally-compiled block, no objects.`
- 2 Exception safety: `n/a.`
- 3 Thread-safety + concurrency: `✓ — the removed code is the thread-synchronization instrumentation itself; with it compiled out, pthread_mutex_lock/unlock are the plain glibc functions (no interposition). No behavior change in normal (fuzzer-off) operation, where the interposers were inert wrappers anyway.`
- 4 Performance + memory: `✓ (neutral/positive) — removes interposers from the hot pthread path in builds where they were compiled in.`
- 5 Settings as public API: `n/a — no setting. ThreadFuzzer pthread-wrapping is test instrumentation, not user-facing. NOTE: this reduces CI race-detection coverage of the pthread-wrap variety; accepted deliberately for glibc-compat + build simplicity.`
- 6 Error handling: `n/a.`
- 7 Upstream / vendored code: `✓ — touches only src/Common/ThreadFuzzer.cpp; no contrib/**, .claude/**, root AGENTS.md, workflows.`
- 8 Behavior under settings: `n/a — the toggle is compile-time, not runtime-configurable.`

## 4. Test design

(c) **No runtime test — `no_justified` (compile-time instrumentation toggle), backed by artifact-level evidence.**

- Why `no_justified` and not `test_design_blocked`: the change is a compile-time
  `#define` controlling conditional compilation of *test-only* instrumentation.
  It has no runtime-configurable behavior and no SQL-observable surface, so there
  is nothing a stateless/integration test could assert at runtime — this is the
  `§7(b)` "build-system toggle" case, not a case where we want a test but can't
  build one.
- Evidence of causation is taken at the **build-artifact level** instead of via a
  query. `nm` on `ThreadFuzzer.cpp.o`, built both ways from the same tree:

  ```bash
  OBJ=src/CMakeFiles/clickhouse_common_io.dir/Common/ThreadFuzzer.cpp.o
  # post-patch (working tree = patched):
  ninja -C build "$OBJ"; llvm-nm "build/$OBJ" | grep pthread_mutex   # → (empty)
  # pre-patch (working tree = HEAD, index kept staged):
  git show HEAD:src/Common/ThreadFuzzer.cpp > src/Common/ThreadFuzzer.cpp
  ninja -C build "$OBJ"; llvm-nm "build/$OBJ" | grep pthread_mutex   # → interposers present
  git checkout-index -f -- src/Common/ThreadFuzzer.cpp              # restore patched
  ```

  | symbol class | pre-patch | post-patch |
  |---|---|---|
  | `T pthread_mutex_lock`, `T pthread_mutex_unlock` (interposers) | present | **absent** |
  | `*_before/after_{sleep,yield,migrate}_probability` tuning statics | 16 present | **absent** |
  | `U __pthread_mutex_lock@GLIBC_2.17` (forward to real glibc) | present | **absent** |

  Logs: `tmp/patch-039/build-obj-pre.log`, `tmp/patch-039/build-obj-post.log`,
  `tmp/patch-039/nm-post.txt` (empty, by construction).

- Full link verification: `ninja -C build clickhouse` exit 0
  (`build/build-039.log`).

## 5. Rollback considerations

- Revert safety: trivial — restore the platform/sanitizer conditional. No schema,
  on-disk, or ZK state.
- State that survives restart: none.
- Runtime disable: `n/a` — compile-time toggle. To re-enable the wrapping one
  would rebuild without the patch (and accept the glibc-2.36+ risk).

## 6. Per-uplift notes

### 25.8-aiven (historical)

Original carry. Author: Tilman Moeller. Committer: Joe Lynch. Co-author: Kevin
Michel. No 25.X runtime test (compile-time toggle).

### 26.3-aiven (this uplift)

- Cherry-pick: clean (`-x --no-commit`); staged diff patch-id-identical to
  source (`byte_equivalent: true`).
- Build verification: `ninja -C build clickhouse` exit 0 (`build/build-039.log`).
  The `build/` dir was recovered earlier this session via `cmake --fresh` +
  `/opt/llvm-21` (`build-and-test.md` §2/§6); the toolchain stayed warm for this
  patch.
- Test added at: `n/a — no_justified; artifact-level nm evidence in §4.`
- Anything surprising: the nm forward-reference `U __pthread_mutex_lock@GLIBC_2.17`
  is a neat independent corroboration of the commit's glibc-2.36+ rationale — the
  interposer really does override the public symbol and chain to the glibc
  internal, which is the construct that breaks under newer glibc symbol
  versioning.
```


# Runbook — build and test ClickHouse for the Aiven LTS uplift

**Scope.** This runbook documents the verified-or-best-known commands for setting up the build environment, compiling ClickHouse, starting a local server, and running tests during the Aiven LTS uplift work. It is durable (version-agnostic) — workers in `T3+` reference it, humans reference it when an env breaks.

**Status convention.** Each section is labeled:

- **VERIFIED <ISO date>** — the commands were run end-to-end in this repository and succeeded. Trust without re-verifying unless the env changed.
- **PROVISIONAL — verify before use** — commands captured from prior experience that should work but have not been re-run in this lifecycle. The first worker who needs them MUST verify and then promote the section to VERIFIED with the date.

**Update discipline.** When a worker promotes a section from PROVISIONAL → VERIFIED, they update the date and add a line under "What was actually run" with the exact command + relevant output. They do not delete the PROVISIONAL caveats — keep them so a future reader can see what was assumed at first.

---

## 1. Compiler environment

**Status: VERIFIED 2026-05-20**

ClickHouse currently requires clang ≥ 21 (enforced by `cmake/tools.cmake`). The supported toolchain on this machine lives at `/opt/llvm-21`.

```bash
export PATH="/opt/llvm-21/bin:$PATH"
export CC=/opt/llvm-21/bin/clang
export CXX=/opt/llvm-21/bin/clang++
```

Verify before continuing:

```bash
$CC --version | head -1
# Expected: clang version 21.1.8 ... (or a newer 21.x)
```

**What was actually run (2026-05-20):**

```
$ /opt/llvm-21/bin/clang --version | head -1
clang version 21.1.8 (https://github.com/llvm/llvm-project 2078da43e25a4623cab2d0d60decddf709aaea28)
```

**Why this matters.** The exported `CC`/`CXX` are read by `cmake` AND by `.claude/tools/cppexpr.sh`. Pointing at the wrong (older) clang causes the cmake configure to fail with `Compilation with Clang version <N> is unsupported`, even if `which clang` resolves correctly. The export must precede any cmake invocation.

## 2. CMake configure

**Status: VERIFIED 2026-05-20** (with the `--fresh` recovery variant)

Fresh configure (use this when the build dir is clean or in an inconsistent state):

```bash
cmake --fresh -S . -B build -G Ninja \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_C_COMPILER="$CC" \
  -DCMAKE_CXX_COMPILER="$CXX"
```

Plain reconfigure (use when the build dir was last configured with the same toolchain and you only want to pick up CMakeLists changes):

```bash
cmake -B build
```

**When to use `--fresh` vs plain.**

- **Use `--fresh`** if any of the following is true:
  - `build/CMakeCache.txt` is missing.
  - `build/CMakeFiles/rules.ninja` is missing but `build/build.ninja` exists (half-broken state).
  - You just changed `CC` / `CXX` / `CMAKE_BUILD_TYPE`.
  - You see `Compilation with Clang version <N> is unsupported` from a previous toolchain mismatch.
- **Use plain** otherwise. Plain is cheap (~30s); `--fresh` re-runs all configure probes (~15s in the verified run, can be longer on slower machines).

**What was actually run (2026-05-20):**

```
$ cmake --fresh -S . -B build -G Ninja \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_C_COMPILER=/opt/llvm-21/bin/clang \
    -DCMAKE_CXX_COMPILER=/opt/llvm-21/bin/clang++
...
-- Will build ClickHouse 26.3.10.1 revision 54517
-- Configuring done (13.6s)
-- Generating done (2.8s)
-- Build files have been written to: /home/tilman.moeller/projects/ClickHouse/build
```

**Why `--fresh` exists.** When `CC`/`CXX` change, cmake correctly detects the change but still probes the *cached* compiler before applying the new one — and ClickHouse's tools.cmake hard-fails on `< 21`, so the error fires before the new compiler is even tried. `--fresh` deletes the cache up front, sidestepping the chicken-and-egg.

## 3. Build clickhouse

**Status: PARTIALLY VERIFIED 2026-05-20** (dry-run only; full link not re-run in this lifecycle)

Main binary:

```bash
ninja -C build clickhouse
```

Dry-run (cheap sanity check that ninja can plan a build):

```bash
ninja -C build -n clickhouse
```

C++ unit tests target (PROVISIONAL — verify before use):

```bash
ninja -C build unit_tests_dbms
```

**What was actually run (2026-05-20):**

```
$ ninja -C build -n clickhouse
ninja: Entering directory `build'
[0/2] Re-checking globbed directories...
[1/2] Re-running CMake...
```

Exit 0. Dry-run succeeded, proving the build graph is well-formed. A full `ninja -C build clickhouse` was not re-run in this session because the existing `build/programs/clickhouse` binary is still valid for the smoke tier; the first patch dispatch in `T3` will exercise a full build.

**Sanity check on the compiled binary:**

```bash
.claude/tools/cppexpr.sh -i Core/Block.h 'OUT(sizeof(DB::Block))'
# Expected: sizeof(DB::Block) -> <some integer>
```

If `cppexpr.sh` returns a value, the compile/link chain is healthy. This is the cheapest end-to-end probe and is used by Task 0 of the bootstrap plan.

**Build flags as of 2026-05-20** (from cmake output, recorded for posterity):

- Build type: `RelWithDebInfo`
- Target: `aarch64-unknown-linux-gnu`
- `-march=armv8.2-a+simd+crypto+dotprod+ssbs+rcpc+bf16`
- Linker: `lld` via `--ld-path=/opt/llvm-21/bin/ld.lld`

**Rule of thumb.** `ninja -C build clickhouse` is the right target for almost all patch verification. `unit_tests_dbms` is only needed when a patch's tests live under `src/.../tests_gtest/`.

## 4. Start a local server

**Status: PROVISIONAL — verify before use**

The recipe below is captured from prior session work; it has not been re-run end-to-end in this lifecycle. The first worker who needs it must verify, fix any drift, and promote to VERIFIED.

```bash
mkdir -p tmp/ch-smoke/filesystem_caches

./build/programs/clickhouse server \
  --config-file ./programs/server/config.xml \
  -- --path=./tmp/ch-smoke \
     --filesystem_caches_path=./tmp/ch-smoke/filesystem_caches/ \
     --custom_cached_disks_base_directory=./tmp/ch-smoke/filesystem_caches/ \
     --logger.log=./build/server-local.log \
     --logger.level=warning \
  > build/server-local.out 2>&1 &
```

Wait for ready:

```bash
./build/programs/clickhouse client --port 9000 -q "SELECT 1"
# Expected: 1
```

Stop:

```bash
pkill -INT -f "build/programs/clickhouse server"
# or: kill <pid>
```

**Why the `--` and the funny overrides.**

- The `--` separates server CLI flags (before `--`) from runtime overrides (after `--`). The overrides bind to settings the server reads at startup.
- `--path=./tmp/ch-smoke` and `--filesystem_caches_path` / `--custom_cached_disks_base_directory` exist because the default paths under `/tmp` are tmpfs-backed on Fedora and overrun memory once a small dataset is loaded. We redirect to the repo-local `tmp/` (which `AGENTS.md` already declares as the scratch directory).
- `--logger.level=warning` keeps the log readable for a worker that needs to grep failures.

**What to verify when promoting to VERIFIED.**

1. The server reaches "Ready for connections" within ~10–30s.
2. `SELECT 1` returns `1` via TCP port 9000.
3. The PID survives a `clickhouse client -q "SELECT version()"` round-trip.
4. `pkill -INT ...` returns the server cleanly (no stuck processes).

## 5. Run a stateless test

**Status: PROVISIONAL — verify before use**

Per-test invocation:

```bash
export PATH="$PWD/build/programs:$PATH"   # so the runner finds 'clickhouse client'

CLICKHOUSE_PORT_TCP=9000 CLICKHOUSE_PORT_HTTP=8123 \
  ./tests/clickhouse-test \
    --no-random-settings \
    --no-random-merge-tree-settings \
    <test_name_without_extension>
```

Bare-server smoke tier (skips ZK/stateful/shard/long tests):

```bash
./tests/clickhouse-test \
  --no-random-settings --no-random-merge-tree-settings \
  --no-stateful --no-shard --no-zookeeper --no-long \
  <test_name>
```

**Why these flags matter for patch verification.**

- `--no-random-settings` and `--no-random-merge-tree-settings` make the run deterministic. A worker's tier-3 verification MUST be reproducible by the human reviewer; randomized settings make the same test name behave differently between runs.
- `--no-stateful --no-shard --no-zookeeper --no-long` are appropriate for the bare-server smoke tier (single-node, no external dependencies). A worker that needs ZK or sharding asks for a richer env via the halt-and-escalate `env_missing` reason rather than silently skipping coverage.

**What to verify when promoting to VERIFIED.**

1. The runner finds `clickhouse client` (PATH export is in effect).
2. One known-passing test reproduces a PASS (e.g. an existing `0_stateless/00001_select_1.sql`).
3. One known-failing-by-construction test (introduce one, then remove it) reproduces a FAIL.

## 6. Common breakage and fast diagnosis

| Symptom | Likely cause | Fix |
|---|---|---|
| `ninja: error: build.ninja:N: loading 'CMakeFiles/rules.ninja': No such file or directory` | Build dir half-broken (cache missing) | §2 `cmake --fresh -B build ...` |
| `CMake Error ... Compilation with Clang version <N> is unsupported` | `CMakeCache.txt` references an old toolchain that's no longer on disk, or you forgot `export CC/CXX` | §1 export, then §2 `cmake --fresh` |
| `cppexpr.sh` fails with the same "rules.ninja" error | Same as row 1 (cppexpr delegates to ninja) | Same fix |
| Server starts but `SELECT 1` hangs | tmpfs `/tmp` overflowed on Fedora | §4 path overrides; ensure `--path=` points at repo-local `tmp/` |
| `clickhouse-test` can't find `clickhouse client` | PATH not set | §5 `export PATH="$PWD/build/programs:$PATH"` |

## 7. What is deliberately not in this runbook

- **Sanitizer builds (ASan/TSan/UBSan/MSan).** Not in the smoke tier. Will be added under §3 when the first patch needs sanitizer verification or when a release-tagging task requires it.
- **Distributed / sharded test setup.** Not yet needed. Will be added when a worker first hits `env_missing: zookeeper_required`.
- **Cross-compilation, alternate targets.** Not needed for the LTS uplift loop; the build is always native for this machine.
- **Performance benchmarks.** Performance regression detection is a separate workstream; not part of patch verification.

Each omission is intentional and is named here so a future reader knows the gap is known, not forgotten.

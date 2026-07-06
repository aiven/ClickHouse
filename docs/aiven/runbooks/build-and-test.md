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

- **Use `--fresh` ONLY if** one of the following is true:
  - `build/CMakeCache.txt` is missing.
  - You just changed `CC` / `CXX` / `CMAKE_BUILD_TYPE`.
  - You see `Compilation with Clang version <N> is unsupported` from a previous toolchain mismatch.
  - A plain `cmake -B build` reconfigure itself errors out (cache also broken).
- **Use plain `cmake -B build` otherwise**, including the common `build/CMakeFiles/rules.ninja`-missing case (see the warning below). Plain is cheap (~30s); `--fresh` re-runs all configure probes (~15s) **and then forces a full rebuild** (see warning).
- **Also use plain `cmake -B build` when a patch ADDS or REMOVES a source file** (a new `.cpp`/`.h` in `src/`). **Status: VERIFIED 2026-06-10** (patch 054, first add-new-TU port). ClickHouse globs its sources, so a brand-new `.cpp` (e.g. `KeeperMapSettings.cpp`) is **not in `build.ninja` until the glob re-runs** — a plain reconfigure regenerates the graph from the intact cache and ninja then compiles the new TU (observed: a ~48-step warm incremental that included `KeeperMapSettings.cpp.o` + the dependent relink). `--fresh` is NOT needed and would force the full `contrib` rebuild. Run the plain reconfigure once, after the cherry-pick, before the first `ninja` (always `export CC/CXX=/opt/llvm-21/...` first).

> ⚠️ **`--fresh` resets ninja's build graph → full rebuild (incl. all of `contrib`).**
> **Status: VERIFIED 2026-06-10** (patch 038). `cmake --fresh` deletes `CMakeCache.txt` + `CMakeFiles/`, which invalidates ninja's per-target command hashes. The next `ninja -C build clickhouse` then **recompiles everything** — including immutable third-party libs (`grpc`, `protobuf`, `llvm`): observed **~15539 → 7782 → 4714** steps across recovery cycles, ~75 min cold per pass on this host. For comparison, a normal code-only patch build is a true incremental: **patch 069 = 19 steps; patch 038's worktree-flip rebuilds = 85 steps / ~40–100 s.** So:
>
> - For a **code-only patch**, drive the build with **plain `ninja -C build clickhouse`** — never `cmake --fresh`. The big rebuild is a one-off artifact of `--fresh`, not the cost of the patch.
> - When you hit the `rules.ninja`-missing corruption, **try plain `cmake -B build` FIRST** (PROVISIONAL — verify before relying): with an intact `CMakeCache.txt` and unchanged toolchain it regenerates `build.ninja` + `CMakeFiles/rules.ninja` from the cache and ninja then rebuilds only genuinely-stale targets, preserving incrementality. Reserve `--fresh` for when that plain reconfigure also fails. (In patch 038, running a *plain* `cmake -B build` while the wrong `CC/CXX` were in scope re-probed and cached system clang-20, which broke configure entirely — so `export CC/CXX=/opt/llvm-21/...` BEFORE any reconfigure, plain or fresh.)
> - If you must run `--fresh` (genuine cache/toolchain breakage), accept the one-time full rebuild; afterwards incrementals are restored.

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

**Status: VERIFIED 2026-05-24** (full build exercised end-to-end by T3.2; subagent id `083abbef-ded9-4be2-8481-1f12ff4ad588`. Previous dry-run-only verification from 2026-05-20 retained below.)

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

**What was actually observed (2026-05-24, T3.2 patch 040):**

```
$ ninja -C build clickhouse           # post-patch full build (warm cache)
ninja: Entering directory `build'
[N/N] Linking CXX executable programs/clickhouse
$ echo $?
0
```

Three `ninja -C build clickhouse` invocations in T3.2 (one warm-cache full build, two incremental rebuilds after `git restore --worktree`) all exit 0. Wall-clock: 66s / 22s / 14s. The `cmake --fresh -B build ...` recovery in §2 was used at the start because the build directory had a missing `rules.ninja` (matched row 1 of §6 "Common breakage"); recovery was clean.

## 4. Start a local server

**Status: VERIFIED 2026-05-24** (T3.2 patch 040; recipe ran end-to-end with the documented overrides)

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

**What was actually observed (2026-05-24, T3.2 patch 040):**

The T3.2 worker started the server twice during a single dispatch (once after the post-patch build, once after the pre-patch incremental rebuild) and stopped it twice. Both starts reached `SELECT 1 → 1` cleanly. Both stops (`pkill -INT -f "build/programs/clickhouse server"`) returned the server without zombie processes. The Fedora `/tmp` tmpfs concern materialized as expected — the override `--path=./tmp/ch-smoke` avoided it. The promotion criteria are all met:

1. Server reached "Ready for connections" within ~10s. ✓
2. `SELECT 1` returned `1` via TCP port 9000. ✓
3. PID survived round-trip queries. ✓
4. `pkill -INT ...` returned cleanly. ✓

**Known quirk — `preprocessed_configs/` leaks to the repo root anyway.**
With this recipe, every server-managed path (`data/`, `metadata/`, `access/`, `coordination/`, `flags/`, `format_schemas/`, `user_files/`, `disks/`, `local_disk*/`, etc.) correctly lands under `./tmp/ch-smoke/`. There is one exception: `preprocessed_configs/config.xml` is written to the **current working directory** because the very first config-preprocessing pass happens **before** the `--path` CLI override is applied to the config tree (see `src/Common/Config/ConfigProcessor.cpp:976-997`: when `<path>` is `/var/lib/clickhouse/` and that path is not writable, the code falls back to CWD). This is harmless — `.gitignore` covers `/preprocessed_configs/` — but workers should not be surprised when `ls` shows `preprocessed_configs/` at the repo root after running the server. Anything else appearing at the repo root **is** a bug (probably a missing CLI override) and should be investigated, not gitignored.

**Known requirement — the `default` user must have explicit grants for `clickhouse-test`.**

**Status: VERIFIED 2026-06-10** (patch 038).

The stock `programs/server/users.xml` defines `default` with `<access_management>1</access_management>` but its `<grants>` block **commented out**. Such a user has **zero data grants** (`SHOW GRANTS` is empty) and relies entirely on SQL-access-control storage (`access/…`). If that storage is empty/wiped, `default` cannot read system tables, so `clickhouse-test`'s startup probe — `SELECT … FROM system.build_options` — fails with `Code: 497 (ACCESS_DENIED)`, surfacing as a misleading **"All connection tries failed"** after a long retry loop (the server is actually up; `clickhouse client -q "SELECT 1"` and `curl :8123` both work). The §4 recipe worked unmodified for patch 040 only because the storage still held a `GRANT ALL` at that time.

Fix: point the test server at a **scratch** `users.xml` whose `default` user has an explicit grant, and an isolated access dir, via CLI overrides (no edits to the tracked config):

```bash
cp programs/server/users.xml tmp/ch-smoke/users.xml
# In tmp/ch-smoke/users.xml, REPLACE the default user's
#   <access_management>1</access_management> + <named_collection_control>1</named_collection_control>
# with:
#   <grants><query>GRANT ALL ON *.* WITH GRANT OPTION</query></grants>
# (the two are mutually exclusive — the server refuses "Any other access control
#  settings can't be specified with `grants`"; GRANT ALL already includes access mgmt.)

./build/programs/clickhouse server --config-file ./programs/server/config.xml -- \
  --path=./tmp/ch-smoke \
  --filesystem_caches_path=./tmp/ch-smoke/filesystem_caches/ \
  --custom_cached_disks_base_directory=./tmp/ch-smoke/filesystem_caches/ \
  --user_directories.local_directory.path=./tmp/ch-smoke/access/ \
  --user_directories.users_xml.path=./tmp/ch-smoke/users.xml \
  --logger.log=./tmp/ch-smoke/server.log --logger.level=warning \
  > tmp/ch-smoke/server.out 2>&1 &
```

Note `--users.default.grants.query=…` as a top-level CLI override does **not** work — the `<users>` subtree is loaded from the `users_xml` directory, so you must redirect `users_xml.path` to the scratch copy. Verify with `clickhouse client -q "SELECT count()>0 FROM system.build_options"` → `1`.

**Known requirement — `KeeperMap` (and other ZooKeeper-backed engines) need a Keeper-enabled config, and writable paths must be set via a `config.d` drop-in, not late `--` overrides.**

**Status: VERIFIED 2026-06-10** (patch 054, first KeeperMap test).

The bare `programs/server/config.xml` has its `<zookeeper>` block **commented out**, no `<keeper_server>`, and no `keeper_map_path_prefix`, so `KeeperMap` will not initialise (`CREATE … ENGINE = KeeperMap(…)` fails). Two things are needed: (1) an embedded Keeper + a `<zookeeper>` pointing at it + `keeper_map_path_prefix`; (2) writable absolute paths set **early** (see the gotcha below). Both are delivered cleanly by a scratch config directory whose `config.d/*.xml` auto-merges (the server merges `config.d` relative to the `--config-file` directory):

```bash
mkdir -p tmp/ch-smoke/cfg/config.d
cp programs/server/config.xml                  tmp/ch-smoke/cfg/config.xml
cp tmp/ch-smoke/users.xml                       tmp/ch-smoke/cfg/users.xml   # the GRANTED default user from the previous subsection
cp tests/config/config.d/keeper_port.xml        tmp/ch-smoke/cfg/config.d/   # embedded Keeper on :9181
cp tests/config/config.d/zookeeper.xml          tmp/ch-smoke/cfg/config.d/   # <zookeeper> -> 127.0.0.1:9181
cp tests/config/config.d/enable_keeper_map.xml  tmp/ch-smoke/cfg/config.d/   # keeper_map_path_prefix=/test_keeper_map

# Absolute-path override drop-in (see the gotcha below). Use REAL absolute paths.
cat > tmp/ch-smoke/cfg/config.d/zz_path_override.xml <<XML
<clickhouse>
    <path>$PWD/tmp/ch-smoke/</path>
    <tmp_path>$PWD/tmp/ch-smoke/tmp/</tmp_path>
    <user_files_path>$PWD/tmp/ch-smoke/user_files/</user_files_path>
    <format_schema_path>$PWD/tmp/ch-smoke/format_schemas/</format_schema_path>
    <logger><log>$PWD/tmp/ch-smoke/server.log</log>
            <errorlog>$PWD/tmp/ch-smoke/server.err.log</errorlog>
            <level>warning</level></logger>
    <user_directories><local_directory><path>$PWD/tmp/ch-smoke/access/</path></local_directory></user_directories>
</clickhouse>
XML

./build/programs/clickhouse server --config-file ./tmp/ch-smoke/cfg/config.xml \
  > tmp/ch-smoke/server.out 2>&1 &
```

Verify (the embedded Keeper takes a few seconds to elect a leader):

```bash
export PATH="$PWD/build/programs:$PATH"
clickhouse client -q "SELECT name FROM system.zookeeper WHERE path='/' LIMIT 1 FORMAT Null" && echo "ZK OK"
clickhouse client -q "CREATE TABLE _km_probe (k UInt64, v String) ENGINE=KeeperMap('/'||currentDatabase()||'/_km_probe') PRIMARY KEY k; DROP TABLE _km_probe SYNC;" && echo "KeeperMap OK"
```

When running `clickhouse-test` against this server, **drop `--no-zookeeper`** (the server now has ZooKeeper) and set `CLICKHOUSE_HOST=127.0.0.1` (this host has IPv6 disabled — the `[::1]` listen warnings are harmless, but the runner can otherwise try `[::1]`).

> ⚠️ **Gotcha — the `--path` CLI override is applied too late for paths that must be writable during config preprocessing.** The §4 base recipe's relative `--path=./tmp/ch-smoke` (and `--logger.log=…`) is read *after* `BaseDaemon::initialize` has already tried to create `<path>/preprocessed_configs` and the logger's `<errorlog>` directory from the config's compiled-in defaults (`/var/lib/clickhouse/`, `/var/log/clickhouse-server/`). `ConfigProcessor::savePreprocessedConfig` catches only `Poco::Exception`, so the `std::filesystem_error` from the unwritable `/var/lib/clickhouse` **propagates and the server exits before the override lands**. (This is also why the base recipe leaks `preprocessed_configs/` to the repo root — see the quirk above.) The robust fix is the **absolute-path `config.d` drop-in** above (`<path>`, `<tmp_path>`, `<logger>`, `<user_directories>`): drop-ins are merged *during* preprocessing, before any of those directories are created, so the writable paths are in effect from the first pass. Launch with **only** `--config-file` (no `--` runtime path overrides) when you use the drop-in. (PROVISIONAL: a one-line `catch (const std::exception &)` in `savePreprocessedConfig` would make any misconfigured `<path>` degrade gracefully to CWD instead of exiting — a possible upstream-worthy robustness fix.)

## 5. Run a stateless test

**Status: VERIFIED 2026-05-24** (T3.2 patch 040; runner produced both PASS and FAIL outputs against the same test name from different binaries)

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

**What was actually observed (2026-05-24, T3.2 patch 040):**

The runner found `clickhouse client` correctly after the documented `export PATH="$PWD/build/programs:$PATH"`. The exact promotion criteria (per the original PROVISIONAL note) were met by T3.2. The quoted runner output below shows the test under its current name `9040_disable_replicas_status_default`; at the time of the dispatch the test was named `04206_disable_replicas_status_default` (allocated by upstream's `add-test`), and was later renamed when the Aiven test-naming convention was adopted (see `testing-suites.md` §4.1). The semantics — PASS post-patch, FAIL pre-patch, exit codes, timings — are unchanged by the rename.

1. Runner finds `clickhouse client`. ✓
2. A new stateless test (`9040_disable_replicas_status_default.sh`) reproduces PASS against the post-patch binary:
   ```
   9040_disable_replicas_status_default:                                   [ OK ] 0.28 sec.
   1 tests passed. 0 tests skipped. 0.31 s elapsed (Process-3).
   ```
3. The same test reproduces FAIL against the pre-patch binary (built via the worktree-flip technique documented in `testing-suites.md` §6):
   ```
   9040_disable_replicas_status_default:                                   [ FAIL ] 0.28 sec.
   Reason: result differs with reference:
   @@ -1 +1 @@
   -404
   +200
   ```

The pair of runs is the canonical evidence-of-causation. The runner flags listed above (`--no-random-settings --no-random-merge-tree-settings --no-stateful --no-shard --no-zookeeper --no-long`) were sufficient for this patch; no flag in the smoke tier had to be revised.

## 6. Common breakage and fast diagnosis

| Symptom | Likely cause | Fix |
|---|---|---|
| `ninja: error: build.ninja:N: loading 'CMakeFiles/rules.ninja': No such file or directory` | **ROOT CAUSE (VERIFIED 2026-06-11): the `ms-vscode.cmake-tools` extension auto-reconfigures `build/` on Cursor startup** using `/usr/lib64/ccache/clang` (→ system clang-20, no `/opt/llvm-21` on its PATH) in `Debug` mode. CMake sees a "changed compiler", **deletes the cache** (wiping `rules.ninja` + emptying `CMAKE_BUILD_TYPE`), then fails the `Clang >= 21` gate — corrupting `build/` on every window reopen. (Generator files lost; `CMakeCache.txt` left half-reset to clang-20.) | **Prevent** (one-time): the gitignored `.vscode/settings.json` sets `cmake.configureOnOpen/configureOnEdit/automaticReconfigure=false` + `cmake.buildDirectory=${workspaceFolder}/build_ide` so the extension never touches `build/` — see §6.1. **Repair**: `export CC/CXX=/opt/llvm-21/...` then **plain** `cmake -B build`; `cmake --fresh` only if plain errors — **see §2 ⚠️: `--fresh` forces a full `contrib` rebuild.** |
| `CMake Error ... Compilation with Clang version <N> is unsupported` | `CMakeCache.txt` references an old toolchain that's no longer on disk, or you forgot `export CC/CXX` (a plain reconfigure re-probes the system clang-20) | §1 export, then §2 `cmake --fresh` |
| `CMake Error at cmake/linux/default_libs.cmake: build_clang_builtin Function invoked with incorrect arguments` (preceded by `-- Builtins library:` empty / compiler `Target: unknown`) | Plain reconfigure reused a cached `CMAKE_CXX_COMPILER` = bare ccache wrapper (`/usr/lib64/ccache/clang++`) with an **empty** `CMAKE_CXX_COMPILER_TARGET`, so `build_clang_builtin(${TARGET} …)` gets one arg. The plain cache can't repair this | §2 `cmake --fresh` with the **direct** clang-21 paths (`-DCMAKE_C_COMPILER=/opt/llvm-21/bin/clang -DCMAKE_CXX_COMPILER=/opt/llvm-21/bin/clang++`). Patch-055 verified |
| Server **aborts at startup** with libc++ hardening `vector[] index out of bounds` (e.g. in `AccessType.cpp` `AccessTypeToStringConverter::convert` ← `StorageSystemPrivileges::getAccessTypeEnumValues` ← attaching `system.grants`) | A `.o` compiled **before** a header change (here `AccessType.h` gaining an enum entry) was never recompiled: `#deps 0` means no header→cpp dep records, so neither incremental builds nor `cmake --fresh` (empty deps DB + sccache cache-hit on the unchanged `.cpp`) rebuild it → ODR/size mismatch vs a freshly-built consumer TU | Force a closure-wide rebuild: `find src programs -type f \( -name '*.cpp' -o -name '*.cc' \) -print0 \| xargs -0 touch` then `ninja -C build clickhouse`. sccache only *genuinely* recompiles the stale TUs and cache-hits the rest. Patch-055 verified |
| `cppexpr.sh` fails with the same "rules.ninja" error | Same as row 1 (cppexpr delegates to ninja) | Same fix |
| A 2-file/code-only patch triggers a multi-thousand-step rebuild of `contrib` (`grpc`/`protobuf`/`llvm`) | Someone ran `cmake --fresh`, resetting the build graph | Let it finish once (it restores incrementality), then use **plain `ninja`** thereafter — see §2 ⚠️ |
| A patch adds a new `src/*.cpp` but `ninja` never compiles it / link fails with the new file's symbols | ClickHouse globs sources; a brand-new `.cpp` isn't in `build.ninja` until the glob re-runs | Plain `cmake -B build` (glob refresh), then `ninja` — see §2 (never `--fresh`) |
| Server exits immediately; log shows a `std::filesystem`/`filesystem_error` on `/var/lib/clickhouse` or `/var/log/clickhouse-server` before your `--path` takes effect | `--` CLI path overrides apply too late — `savePreprocessedConfig`/logger init run first against compiled-in defaults and only catch `Poco::Exception` | §4 ⚠️ — set `<path>`/`<logger>`/`<user_directories>` to absolute paths via a `config.d` drop-in; launch with only `--config-file` |
| `CREATE … ENGINE = KeeperMap(…)` fails ("KeeperMap is disabled" / "doesn't support SETTINGS") on the smoke server | Bare `config.xml` has `<zookeeper>` commented out, no `<keeper_server>`, no `keeper_map_path_prefix` | §4 — scratch `config.d` with `keeper_port.xml` + `zookeeper.xml` + `enable_keeper_map.xml`; drop `--no-zookeeper` from `clickhouse-test` |
| Server starts but `SELECT 1` hangs | tmpfs `/tmp` overflowed on Fedora | §4 path overrides; ensure `--path=` points at repo-local `tmp/` |
| `clickhouse-test`: "All connection tries failed" / `Code: 497 ... grant SELECT ... ON system.build_options (ACCESS_DENIED)` | `default` user has **no grants** — stock `users.xml` has its `<grants>` block commented out and the SQL-access storage (`access/`) was wiped | §4 — start the test server with a scratch `users.xml` whose `default` user has `GRANT ALL ON *.* WITH GRANT OPTION` |
| `clickhouse-test` can't find `clickhouse client` | PATH not set | §5 `export PATH="$PWD/build/programs:$PATH"` |
| Linker `undefined symbol`, or a clean build that crashes/throws at runtime far from your edit, after changing a widely-included header | ninja did not recompile the header's includers (`#deps 0` — no dependency records) | §7 force-recompile the include closure |
| `Write`/`Edit`/`StrReplace` on a large source file (e.g. `Common/AsynchronousMetrics.cpp`, ~110 KB) fails with `Tool blocked … hook "deny-upstream-file-writes.sh" failed … spawn E2BIG` | **Tooling bug, NOT a policy denial** (the file is editable). The `preToolUse` hook runner passes the file content to the hook process as a single `argv` string; for files near/over the OS `MAX_ARG_STRLEN` (~128 KB) the `spawn` itself fails with `E2BIG`, and the fail-closed hook blocks the edit. Confirmed file-size-driven (VERIFIED 2026-06-11, patch 041): tiny scratch edits and ~23 KB edits to `MemoryWorker.cpp` succeed; the 110 KB file fails. Affects **all** agents (parent + workers) equally | **No agent-side workaround** — do **not** `sed`/`echo` (that bypasses the deny hook; forbidden). Escalate the specific edit to the human, who edits it in the IDE (the editor does not route through the agent hook). **Orchestration fix:** the hook runner should pipe tool input via **stdin** (or skip/truncate content for large files) rather than `argv`/env, so large-file edits don't `E2BIG` |

## 6.1. Protect `build/` from the CMake Tools extension (the `rules.ninja` root cause)

**Status: VERIFIED 2026-06-11** (patch 055 — finally identified the cause of the recurring `rules.ninja`/stale-clang-20 corruption).

**What was happening.** The `ms-vscode.cmake-tools` extension treats `${workspaceFolder}/build` as *its* build directory and, by default, configures it on workspace open (and reconfigures on `CMakeLists` edits). It launches `cmake` with the compiler it discovers on *its* environment — `/usr/lib64/ccache/clang[++]`, which masquerades the **system** clang (20.x on this Fedora host) because `/opt/llvm-21` is not on the extension's PATH — and in `Debug` mode. Since our terminal builds configured `build/` with `/opt/llvm-21` + `RelWithDebInfo`, CMake sees the compiler/flags change, prints *"variables… require your cache to be deleted"*, **deletes `CMakeFiles/` (including `rules.ninja`)**, then aborts on `tools.cmake`'s `Clang >= 21` gate. Net effect on every Cursor reopen: `rules.ninja` gone, `CMAKE_BUILD_TYPE` emptied, `CMAKE_CXX_COMPILER_VERSION` re-cached as `20.1.8`. This is the upstream of *every* `rules.ninja`-missing and stale-clang-20 incident in §6/§2.

**The fix (one-time, gitignored — `.vscode/` is in ClickHouse's `.gitignore`).** A workspace `.vscode/settings.json`:

```jsonc
{
    "cmake.configureOnOpen": false,
    "cmake.configureOnEdit": false,
    "cmake.automaticReconfigure": false,
    "cmake.buildDirectory": "${workspaceFolder}/build_ide"
}
```

Defense in depth: (1) the extension never auto-configures; (2) even if invoked manually it targets a separate `build_ide/`, so the terminal-managed `build/` is untouchable. **Reload the window** (Developer: Reload Window) after creating the file so the extension re-reads it. If you don't use the IDE's CMake integration at all, disabling the `ms-vscode.cmake-tools` extension for this workspace is an even more bulletproof alternative.

## 7. Incremental-build correctness hazard — ninja `#deps 0`

**Status: VERIFIED 2026-06-01** (Phase 2 patch 012; observed in both its linker-error and its runtime-corruption forms).

**The trap.** In this `build/` directory `ninja` has **no header-dependency
records**. Confirm it yourself:

```bash
ninja -C build -t deps programs/CMakeFiles/clickhouse.dir/.../SomeFile.cpp.o
# ... #deps 0, deps mtime 0
```

`#deps 0` means: when you edit a header, ninja does **not** know which `.cpp`
files include it, so it only recompiles the translation unit you edited directly
(if any). Every other includer keeps its **stale** `.o`. The build is "green"
but the binary is inconsistent — a classic **false-green** incremental build.

**Two faces, increasing nastiness:**

1. **Signature change → link error (loud, easy).** Patch 012 widened
   `makeHTTPSession` in `IO/HTTPCommon.h`. The TUs that still called the old
   signature were not recompiled, and the build failed at link with
   `undefined symbol: DB::makeHTTPSession(...)`. Annoying, but it tells you
   exactly what is wrong.

2. **Layout change → silent runtime corruption (quiet, dangerous).** Patch 012
   also added `ca_path` to `PocoHTTPClientConfiguration` in
   `IO/S3/PocoHTTPClient.h`, which shifted the offset of a later member
   (`request_throttler`). A stale `ServerAsynchronousMetrics.cpp.o` read the
   throttler at the **old** offset, so `S3::Client::getPutRequestThrottler`
   returned a garbage `shared_ptr` and the server hit a fatal exception during
   `AsynchronousMetrics` startup. It **linked cleanly** and presented only as an
   integration-test symptom ("server port 9000 never opened") — far from the
   edit. This is the failure mode to fear.

**Diagnosis.**

```bash
# Is the includer's .o older than the header you changed?
ls -l --time-style=full-iso build/.../Stale.cpp.o src/IO/S3/PocoHTTPClient.h

# Which symbols does the suspect .o still reference unresolved/at-old-shape?
llvm-nm -u build/.../Stale.cpp.o | grep -i <symbol>
```

**Fix — force-recompile the transitive include closure of the changed header.**
Either do a clean build of the affected target, or touch every `.cpp` that
(transitively) includes the changed header and rebuild. Patch 012 used a small
helper, `tmp/touch_includers.py`, to compute and `touch` the closure of the
layout-critical headers (`IO/S3/PocoHTTPClient.h`, `IO/S3Settings.h`,
`IO/S3/Client.h` — 129 TUs); after touching, ninja rebuilt them and the
corruption was gone.

**Rule of thumb.** If your edit changes a **struct layout** (add/remove/reorder a
member, change a member type) or a **function signature** in a header that is
widely included, do **not** trust an incremental build. Force-recompile the
include closure (or clean-build the target). For pure function-body or
new-symbol changes, the incremental build is fine — the danger is specifically
layout and signature changes propagating through stale `.o` files.

**Why this matters.** The linker-error face costs minutes. The layout face can
cost an afternoon, because the crash is in code you never touched and the binary
"built successfully". Treat a green incremental build after a header layout
change as **unproven** until the include closure has been recompiled.

## 8. What is deliberately not in this runbook

- **Sanitizer builds (ASan/TSan/UBSan/MSan).** Not in the smoke tier. Will be added under §3 when the first patch needs sanitizer verification or when a release-tagging task requires it.
- **Distributed / sharded test setup.** Not yet needed. Will be added when a worker first hits `env_missing: zookeeper_required`.
- **Cross-compilation, alternate targets.** Not needed for the LTS uplift loop; the build is always native for this machine.
- **Performance benchmarks.** Performance regression detection is a separate workstream; not part of patch verification.

Each omission is intentional and is named here so a future reader knows the gap is known, not forgotten.

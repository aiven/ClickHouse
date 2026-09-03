# Runbook — build and test

Verified commands for local uplift work. Prefer incremental builds. Never pass
`-j` to ninja; never use `nproc`. Log builds/tests under the build dir or `tmp/`;
summarize with a subagent.

## Toolchain

```bash
export PATH="/opt/llvm-21/bin:$PATH"
export CC=/opt/llvm-21/bin/clang
export CXX=/opt/llvm-21/bin/clang++
$CC --version | head -1   # expect clang 21.x
```

Rust (if configure asks): use the pin from upstream docs / prior successful
configure on this machine (26.8 local builds used `nightly-2026-03-22`).

## Configure

```bash
# First time / toolchain change only — --fresh forces a full rebuild later
cmake --fresh -S . -B build_debug -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_C_COMPILER="$CC" \
  -DCMAKE_CXX_COMPILER="$CXX"

# After adding/removing sources (glob refresh) — cheap
cmake -B build_debug
```

Avoid `--fresh` unless the cache/toolchain is actually broken.

## Build

```bash
ninja -C build_debug clickhouse
# log: build_debug/build_clickhouse.log — redirect and summarize
```

## Stateless tests

```bash
# Binary the runner expects; adjust to your layout
export CLICKHOUSE_TESTS_SERVER_BIN_PATH="$PWD/build_debug/programs/clickhouse"

./tests/clickhouse-test aiven_022_your_slug
# or a small named set — see root AGENTS.md for CI/praktika patterns
```

Aiven-owned functional tests use **`aiven_<NNN>_<slug>`** under
`tests/queries/0_stateless/` (glob `aiven_*`; same lane idea as
`test_aiven_<slug>/` for integration).

## Integration (local)

Prefer CI Buildkite shards for full ASAN. For a single suite locally, follow
`tests/integration/README.md` / `python -m ci.praktika run "integration" --test …`
from repo root. Heavy DinD leftovers need the Buildkite cleanup hooks on agents.

## Pre/post evidence pair

1. With patch staged/built: run test → PASS (capture).  
2. Flip worktree to parent (or stash patch): rebuild affected targets.  
3. Run same test → FAIL (capture).  
4. Restore patch. Both captures go in the halt report (decisive lines only).

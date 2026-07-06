# Patch 045 — disable-individual-dictionary-sources

## 0. Lineage

| LTS uplift | First-carry handle on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `d4973afdd7ea55ac538c2153d0b6260ed59467a5` | Tilman Moeller (author) / Joe Lynch (committer) | original carry (the version we port FROM) |
| 26.3-aiven | `patch-port(045)` (staged) | T3 patch worker, 2026-06-08 | conflict-resolved + hardened-rewrite — see §2 and §6 |

The current uplift's row uses the stable handle `patch-port(045)` (NOT a SHA);
it stays "(staged)" until the human commits. Find it with
`git log --grep '^patch-port(045)'`.

## 1. Purpose

Add compile-time toggles (`#cmakedefine01 REGISTER_DICTIONARY_*`) plus `#if`
guards around each dictionary-source registration so a deployment can compile
ClickHouse *without* specific dictionary sources. The motivation is durable:
excluding unused sources reduces binary size, dependency surface, and — most
importantly for Aiven's managed fleet — **attack surface** (e.g. a build that
must not be able to reach arbitrary network/file sources via a `Dictionary`).

Defaults are **enabled**, so a build that passes no flags behaves exactly as
upstream (fully backward compatible); the toggles are opt-*out*.

Source SHA on `v25.8.18.1-lts-aiven`: `d4973afdd7ea55ac538c2153d0b6260ed59467a5`
(from `docs/aiven/uplifts/26.3/inventory.md` row 045).
Original author: `tilman.moeller@aiven.io` (committer `joelynch112@gmail.com`).
Original purpose (verbatim from the source commit body):

> Allow disabling of individual dictionary sources
>
> Add compile-time flags to enable/disable individual dictionary sources, allowing
> builds to exclude specific sources to reduce dependencies, binary size, and
> attack surface. This provides flexibility for creating minimal builds or
> excluding dictionary sources that are not needed in specific deployments.
>
> Previously, all dictionary sources were always compiled and registered,
> regardless of whether they were needed. This patch adds conditional compilation
> guards around each dictionary source registration, controlled by CMake defines
> that default to enabled (maintaining backward compatibility).
>
> Co-authored-by: Joe Lynch <joe.lynch@aiven.io>

### Ground truth — where the defaults live (RPM build harness)

The production default selection is **not** in the ClickHouse repo. At tag
`v25.8.24.21-lts-aiven` the toggles are absent from `src/configure_config.cmake`,
`src/Dictionaries/CMakeLists.txt`, and the root `CMakeLists.txt`. Instead,
Aiven's **RPM build harness** (`clickhouse-25.8.spec`,
<https://github.com/aiven/ClickHouse/tree/v25.8.24.21-lts-aiven>) passes an
explicit `-D` for every toggle. The 25.8 production selection is:

| Toggle | RPM value | Toggle | RPM value |
|---|---|---|---|
| `REGISTER_DICTIONARY_SOURCE_MYSQL` | `=1` | `REGISTER_DICTIONARY_SOURCE_FILE` | `=0` |
| `REGISTER_DICTIONARY_SOURCE_CLICKHOUSE` | `=1` | `REGISTER_DICTIONARY_SOURCE_MONGODB` | `=0` |
| `REGISTER_DICTIONARY_SOURCE_POSTGRESQL` | `=1` | `REGISTER_DICTIONARY_SOURCE_REDIS` | `=0` |
| `REGISTER_DICTIONARY_SOURCE_HTTP` | `=1` | `REGISTER_DICTIONARY_SOURCE_CASSANDRA` | `=0` |
| | | `REGISTER_DICTIONARY_SOURCE_XDBC` | `=0` |
| | | `REGISTER_DICTIONARY_SOURCE_JDBC` | `=0` |
| | | `REGISTER_DICTIONARY_SOURCE_EXECUTABLE` | `=0` |
| | | `REGISTER_DICTIONARY_SOURCE_EXECUTABLEPOOL` | `=0` |
| | | `REGISTER_DICTIONARY_SOURCE_LIBRARY` | `=0` |
| | | `REGISTER_DICTIONARY_SOURCE_YAMLREGEXPTREE` | `=0` |
| | | `REGISTER_DICTIONARY_LAYOUT_SSD` | `=0` |
| | | `REGISTER_DICTIONARY_SOURCE_YTSAURUS` | `=0` |

Because production is fully driven by the RPM `-D` flags, the in-tree default
does **not** change production behavior — it only governs builds (dev/CI) that
pass no flags. The in-tree default-ON contract (deviation 1) is the safety net
that keeps a no-flag build identical to upstream.

## 2. Upstream-drift findings

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`, not just textually applicable.

### Commands run

```bash
# Does 26.3 already have a per-source disable mechanism?
git grep -c "REGISTER_DICTIONARY" HEAD -- src/        # → exit 1 (none)

# Does any *other* uplift patch already introduce the YTSAURUS toggle?
git grep -n "REGISTER_DICTIONARY_SOURCE_YTSAURUS" HEAD # → NONE on HEAD

# Are the register-source entry points still present on HEAD?
for id in registerDictionarySourceMysql registerDictionarySourcePostgreSQL \
          registerDictionarySourceYTsaurus registerDictionarySourceYAMLRegExpTree \
          registerDictionaryCache; do
  printf '%s: ' "$id"; git grep -c "$id" HEAD -- src/ | awk -F: '{s+=$NF} END {print s+0}'
done   # → each = 3
```

### Findings

- 26.3 HEAD has **no** per-source disable mechanism (`git grep REGISTER_DICTIONARY`
  on HEAD returns nothing). The patch is **still needed**; it is not obsoleted.
- All registration entry points still exist on HEAD (`registerDictionarySource*`,
  `registerDictionaryCache`). No upstream rename/removal.
- `src/Common/config.h.in` drifted: upstream appended new `USE_*` lines
  (`USE_CLIENT_AI`, `USE_LIBCOTP`, `USE_WASMEDGE`, `USE_WASMTIME`, `WITH_COVERAGE`)
  and **removed** the old `.incbin` comment that the 25.8 diff used as its
  insertion anchor. The two `.cpp` files apply per the source diff; `config.h.in`
  required **hand-placement** of the toggle block past the `USE_*` drift (placed
  at the end, after the final `USE_*`/`WITH_COVERAGE` line, which is the new
  end-of-`USE_*`-block position).
- No `REGISTER_DICTIONARY_SOURCE_YTSAURUS` collision on HEAD → this uplift
  **owns** the new 16th toggle (see deviation 3 + ownership note below).
- Conclusion: **`still-needed-but-rewrite`** — semantics unchanged; `config.h.in`
  hand-placed and three intentional hardening deviations applied.

### Required deviations from the literal 25.8 patch (intentional hardening)

1. **`option(REGISTER_<NAME> "..." ON)` defaults in `src/configure_config.cmake`.**
   The 25.8 patch carried no in-tree default; the values lived only in the RPM
   harness, and `#cmakedefine01` of an unset variable yields `#define VAR 0`,
   which would silently disable **every** source in any no-flag (dev/CI) build —
   the catastrophic failure mode. We add an `option(... ON)` per toggle.
   `option()` (not `set(... 1)`) is parity-safe: a `-DREGISTER_<NAME>=0/1`
   already in the CMake cache **wins** over the option default, so the RPM
   harness's explicit `-D=0` flags are unaffected, while a no-flag build keeps
   all sources ON (= upstream behavior).
2. **Explicit `#include "config.h"`** at the top of BOTH
   `registerDictionaries.cpp` and `registerCacheDictionaries.cpp`. The 25.8
   patch relied on a *transitive* include of `config.h`. If a future include
   refactor dropped that transitive path, every `#if REGISTER_DICTIONARY_*`
   would evaluate an **undefined** macro as `0` (preprocessor rule) and silently
   turn into `#if 0`, disabling all guarded sources with no compile error. The
   explicit include makes the dependency load-bearing and visible. The generated
   `config.h` is on the include path via
   `include_directories(${CONFIG_INCLUDE_PATH})` (root `CMakeLists.txt:542`,
   generated by `src/CMakeLists.txt:14`).
3. **16th toggle `REGISTER_DICTIONARY_SOURCE_YTSAURUS` + guard.** The RPM harness
   passes `-DREGISTER_DICTIONARY_SOURCE_YTSAURUS=0`, but the 25.8 patch left
   `registerDictionarySourceYTsaurus(source_factory);`
   (`registerDictionaries.cpp` HEAD line 59) **unguarded**, making that flag a
   silent no-op (YTsaurus would register regardless). We add the 16th
   `#cmakedefine01 REGISTER_DICTIONARY_SOURCE_YTSAURUS` to `config.h.in`, wrap the
   `registerDictionarySourceYTsaurus` call in `#if … #endif`, and add its
   `option(... ON)`. **Ownership note:** `git grep REGISTER_DICTIONARY_SOURCE_YTSAURUS`
   on HEAD returns nothing, so no other uplift patch introduces this symbol; this
   uplift (045) owns it. A future uplift must not double-introduce it.

## 3. C++ review

Apply `docs/aiven/skills/cpp-review-checklist.md`. The change is build-system +
preprocessor-guard only; no runtime data path is altered when defaults are ON.

- 1 Lifetime + ownership: `n/a` — no object lifetime touched; guards only elide
  registration calls at compile time.
- 2 Exception safety: `n/a` — no new throwing code; eliding a `registerX` call
  cannot break exception safety of the surrounding scope.
- 3 Thread-safety + concurrency: `n/a` — registration runs once at startup; no
  concurrency semantics changed.
- 4 Performance + memory: ✓ — strictly reductive (a disabled source is not
  compiled/linked/registered → smaller binary, fewer deps). Default build is
  byte-behavior-identical to upstream.
- 5 Settings as public API: ✓ — no server *setting* added; these are
  compile-time CMake options, not user-visible runtime settings. Default-ON
  means no public-API surface changes (deviation 1 / clause (v)).
- 6 Error handling: ✓ — when a source is compiled out, creating a dictionary
  with that `SOURCE(...)` fails with the existing upstream "unknown dictionary
  source" path (no new error code needed).
- 7 Upstream / vendored code: ✓ — all edited files are ClickHouse-owned
  (`src/Common/config.h.in`, `src/configure_config.cmake`, two
  `src/Dictionaries/*.cpp`); none under `contrib/**`. `registerDictionaries.cpp`
  also declares `registerDictionarySourceMongoDBPocoLegacy` (unused/uncalled on
  HEAD) — left untouched.
- 8 Behavior under settings: ✓ — with all toggles default-ON, behavior is
  unchanged; with a toggle `=0`, the corresponding source is absent. Verified
  by the evidence pair in §4.

## 4. Test design

(b) **Documented justification — no new stateless test; build/config-system
change verified by artifact evidence (`tests.added: no_justified`).**

This patch adds no runtime behavior on a default build (all sources stay
registered → upstream-identical). The behavior it *introduces* — eliding a
source's registration — is a **compile-time** property of the produced binary,
gated by a CMake option, not a runtime setting. A stateless `.sql`/`.sh` test
cannot observe it on the default build (everything is present), and producing a
"disabled" binary requires a second CMake configure + rebuild that the stateless
runner cannot express. Per AGENTS §7(b) and the §7(b) build-system carve-out,
the correct evidence is an **artifact causation pair** on the binary itself.

- Existing upstream test that exercises the un-disabled (default) behavior:
  `tests/queries/0_stateless/01018_ddl_dictionaries_create.sql` — creates a
  dictionary with `SOURCE(CLICKHOUSE(...))`, exercising the
  `registerDictionarySourceClickHouse` registration path that this patch guards.
  With defaults ON the registration set is unchanged, so this test's behavior is
  unaffected by the patch. It was **run against the staged (defaults-on) build
  and passed** (`tmp/patch-045/test-defaults-on.log`):
  `01018_ddl_dictionaries_create: [ OK ] 0.23 sec.`. It is named here per the
  `upstream_reference` requirement; the *decisive* evidence is the artifact pair
  below.

### Artifact evidence-of-causation pair

The single isolating axis is the value of one toggle in `config.h`; everything
else (source, compile flags, include paths) is held identical. The OFF object is
produced by the **primary** method the dispatch prompt prefers: a real
`cmake -B build -DREGISTER_DICTIONARY_SOURCE_MYSQL=0` reconfigure (which
regenerates `build/includes/configs/config.h` with that one line flipped via the
`option()` cache-wins rule), then a **targeted** recompile of just
`registerDictionaries.cpp.o`. The second reconfigure did **not** trigger a large
rebuild: this `build/` has no ninja header-dependency records (`#deps 0`), so a
`config.h` change does not cascade — only the explicitly-touched TU is rebuilt.
Artifacts compared with `llvm-objdump -r` (relocation entries to the externally
defined `registerDictionarySource*` symbols).
Logs: `tmp/patch-045/generated-config-defaults.log`,
`tmp/patch-045/defaults-on-calls.log`, `tmp/patch-045/guard-off-calls.log`.

**(A) defaults-on proof** — the generated `build/includes/configs/config.h`
defines all 16 toggles to `1` (no-flag build), and `config.h` is on the include
path via `include_directories(${CONFIG_INCLUDE_PATH})` (root `CMakeLists.txt:542`).
The `registerDictionaries.cpp.o` compiled against it carries relocations to all
16 source-registration symbols (14 guarded + always-on `Null` + the newly-guarded
`YTsaurus`), e.g. `registerDictionarySourceMysql`,
`registerDictionarySourcePostgreSQL`, `registerDictionarySourceYTsaurus` are all
present. (`…YTsaurus` present confirms deviation 3 — the new 16th guard defaults
ON.)

**(B) guard-works proof** — after `-DREGISTER_DICTIONARY_SOURCE_MYSQL=0` and the
targeted rebuild, the same TU drops to **15** source-registration relocations and
`registerDictionarySourceMysql` is **elided**; every other call (File /
ClickHouse / PostgreSQL / … / `YTsaurus`) is untouched. The build dir was then
restored to defaults-on (`-DREGISTER_DICTIONARY_SOURCE_MYSQL=1`, recompile,
relink; verified back to 16 relocations with MySQL present).

- Why this distinguishes the Aiven change from upstream: upstream 26.3 has **no**
  `REGISTER_DICTIONARY_*` macro at all (§2), so the `#if REGISTER_DICTIONARY_SOURCE_MYSQL`
  guard and its observable effect (call present at `=1`, elided at `=0`) exist
  *only* because of this patch. The pair (A present / B elided) is causally
  attributable to the patch alone.

## 5. Rollback considerations

- Revert safety: ✓ — pure compile-time change. Reverting removes the toggles and
  guards; no schema migration, no on-disk format change, no ZK state.
- State surviving restart: none.
- Disabling the new behavior without rebuilding: `n/a` — the toggles are
  compile-time. To re-enable a source that was compiled out, rebuild with the
  toggle `=1` (or no flag, since the in-tree default is ON).

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — patch first carried on the 25.8 line.

### 25.8-aiven (historical)

Original carry. Author: Tilman Moeller. Committer: Joe Lynch
(`d4973afdd7`). No 25.8 stateless test (build-system change; production
selection driven entirely by the RPM harness `-D` flags). The 25.8 patch left
`registerDictionarySourceYTsaurus` unguarded (its RPM `-D=0` flag was a no-op);
this is corrected in the 26.3 carry (deviation 3).

### 26.3-aiven (this uplift)

- Cherry-pick was: **rewritten/conflict-resolved**. The two `.cpp` files match
  the source diff; `src/Common/config.h.in` was hand-placed past upstream `USE_*`
  drift (the `.incbin` anchor was removed upstream). Three intentional hardening
  deviations applied (see §2): `option(... ON)` in-tree defaults, explicit
  `#include "config.h"` in both `.cpp` files, and the new 16th toggle/guard for
  YTsaurus.
- Upstream-drift conclusion: `still-needed-but-rewrite` (§2).
- Test: no new stateless test — `no_justified`, build/config-system change with
  an artifact causation pair (§4) and upstream reference
  `01018_ddl_dictionaries_create.sql`.
- Time-to-port: build was **cold-cache** (full rebuild incl. LLVM contrib after a
  CMake reconfigure that regenerated `config.h`). See `tmp/patch-045/` for
  timestamps.
- Anything surprising: the 25.8 patch's reliance on a *transitive* `config.h`
  include is a latent foot-gun — an undefined `REGISTER_*` macro evaluates to `0`
  in `#if`, so a future include refactor could silently disable **all** sources
  with no compile error. Deviation 2 (explicit include) + deviation 1
  (in-tree default-ON) close that hole.

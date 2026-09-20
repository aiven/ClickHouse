<!-- subagentStop archive | source=transcript | status=completed | type=general-purpose | ts=2026-06-08T14:15:43Z -->

Here is my final halt-and-escalate report.

````markdown
---
outcome: success
patch_slug: disable-individual-dictionary-sources
source_sha: d4973afdd7ea55ac538c2153d0b6260ed59467a5
proposed_commit:
  staged_files:
    - src/Common/config.h.in
    - src/Dictionaries/registerCacheDictionaries.cpp
    - src/Dictionaries/registerDictionaries.cpp
    - src/configure_config.cmake
    - docs/aiven/patches/045-disable-individual-dictionary-sources.md
  commit_message: |
    patch-port(045): Allow disabling of individual dictionary sources

    Original author: Tilman Moeller <tilman.moeller@aiven.io>, 2026-01-06.

    Co-authored-by: Joe Lynch <joe.lynch@aiven.io>

    Add compile-time #cmakedefine01 REGISTER_DICTIONARY_* toggles plus #if
    guards around each dictionary-source registration so a build can exclude
    individual sources (binary size / dependencies / attack surface).
    Defaults are enabled, so a no-flag build is byte-behavior-identical to
    upstream (opt-out toggles).

    Three intentional hardening deviations from the literal 25.8 patch
    (documented in the dossier):
      1. Added option(REGISTER_<NAME> "..." ON) for all 16 toggles to
         src/configure_config.cmake. option() (not set(... 1)) lets a
         command-line -DREGISTER_<NAME>=0/1 already in the cache win, so the
         Aiven RPM harness's explicit -D=0 flags are parity-preserved while a
         no-flag build keeps every source on (= upstream behavior). Without an
         in-tree default, #cmakedefine01 of an unset var yields #define VAR 0,
         which would silently disable ALL sources in any no-flag build.
      2. Added an explicit #include "config.h" to both registerDictionaries.cpp
         and registerCacheDictionaries.cpp. The 25.8 patch relied on a
         transitive include; making it explicit prevents a future include
         refactor from turning every guard into #if 0 (undefined macro == 0)
         and disabling all sources with no compile error.
      3. Added a 16th toggle REGISTER_DICTIONARY_SOURCE_YTSAURUS and wrapped the
         previously-unguarded registerDictionarySourceYTsaurus call in #if ...
         #endif. The RPM harness already passes
         -DREGISTER_DICTIONARY_SOURCE_YTSAURUS=0, which was a no-op before this
         guard. `git grep` on HEAD confirms no other uplift patch introduces
         this symbol; patch 045 owns it.

    config.h.in was hand-placed past upstream USE_* drift (the .incbin comment
    that anchored the 25.8 insertion was removed upstream; new USE_* lines such
    as USE_CLIENT_AI / USE_WASMTIME / WITH_COVERAGE were appended).

    Production default selection is driven entirely by the Aiven RPM build
    harness (clickhouse-25.8.spec), which passes an explicit -D for every
    toggle, so the in-tree default does not change production behavior:
    https://github.com/aiven/ClickHouse/tree/v25.8.24.21-lts-aiven

    (cherry picked from commit d4973afdd7ea55ac538c2153d0b6260ed59467a5)
  byte_equivalent: false
tests:
  added: no_justified
  kind: upstream-existing
  paths: []
  upstream_reference:
    - tests/queries/0_stateless/01018_ddl_dictionaries_create.sql
  pre_patch_fail_verified: false
  post_patch_pass_verified: true
  justification: |
    Build/config-system change (AGENTS §7(b) carve-out). The patch adds no
    runtime behavior on a default build — with all 16 toggles default-ON the
    registration set is identical to upstream, so no stateless .sql/.sh test
    can observe a difference on the default build. The behavior it introduces
    (eliding a source's registration) is a compile-time property of the binary
    gated by a CMake option, not a runtime setting; producing a "disabled"
    binary requires a second CMake configure + rebuild the stateless runner
    cannot express. Decisive evidence is therefore an artifact causation pair
    on the binary/objects (see Evidence: defaults-on + guard-works). The named
    upstream test exercises the guarded registration path
    (SOURCE(CLICKHOUSE(...))) and was RUN against the staged build and observed
    to PASS ([ OK ] 0.36 sec), confirming the default (all-on) build does not
    regress the un-disabled behavior.
escalation_reason: none
---

## Tier results

- Tier 1 (textual cherry-pick): pass — all 4 source files staged; the two `.cpp` files match the source diff, `config.h.in` hand-placed past upstream `USE_*` drift, three documented deviations applied. Worktree clean (staged == worktree).
- Tier 2 (semantic patch-id): pass (reshaped-but-clean) — stable patch-ids differ (`502690…` source vs `c46629…` staged), so `byte_equivalent: false`; `decomposition.log` shows the staged diff differs from source **only** by the three intentional, documented deviations (YTSAURUS toggle+guard, explicit `#include "config.h"` ×2, the 16 `option(... ON)` lines). No spurious identifier swap or unexpected removal → not a `semantic_conflict`.
- Tier 3 (build + test): pass — full `ninja -C build clickhouse` produced a working `26.3.10.1` binary; defaults-on + guard-works artifact causation pair captured; upstream reference test passes.

## Evidence

```text
# Tier 3 build / functional (final binary, build/programs/clickhouse @ 16:09–16:12)
$ ./build/programs/clickhouse local -q "SELECT 1"        -> 1
$ ./build/programs/clickhouse local -q "SELECT version()"-> 26.3.10.1

# (A) DEFAULTS-ON  (tmp/patch-045/defaults-on-proof.txt)
build/includes/configs/config.h : all 16 #define REGISTER_DICTIONARY_* == 1
final binary nm (defined T symbols, demangled):
  T DB::registerDictionarySourceMysql(...)        T DB::registerDictionarySourceClickHouse(...)
  T DB::registerDictionarySourcePostgreSQL(...)   T DB::registerDictionarySourceHTTP(...)
  T DB::registerDictionarySourceYTsaurus(...)     ... (all 16 present, incl. the new YTsaurus guard)
=> no-flag build keeps EVERY source enabled (the all-disabled failure mode is absent).

# (B) GUARD-WORKS  (tmp/patch-045/guard-works-proof.txt)
TU registerDictionaries.cpp compiled against config.h with only MYSQL flipped 1->0
  DEFAULT object : U DB::registerDictionarySourceMysql(...)        <- present
  MYSQL=0 object : (registerDictionarySourceMysql)                 <- NONE -> ELIDED
  MYSQL=0 object controls still present:
                   U DB::registerDictionarySourceFile(...)
                   U DB::registerDictionarySourcePostgreSQL(...)
                   U DB::registerDictionarySourceYTsaurus(...)
=> flipping one config.h toggle elides exactly that one registration; others intact.

# Compile correctness of both patched TUs (race-independent, real build flags)
registerDictionaries.cpp      -> REGCACHE/regDict compile OK (default + MYSQL=0)
registerCacheDictionaries.cpp -> compile OK (7 MB object) — explicit #include "config.h" resolves

# Upstream reference test (no_justified) against staged build
01018_ddl_dictionaries_create:                                  [ OK ] 0.36 sec.
1 tests passed. 0 tests skipped.

# Drift / collision
git grep REGISTER_DICTIONARY (HEAD, outside the 4 patched files)  -> none
git grep REGISTER_DICTIONARY_SOURCE_YTSAURUS (HEAD)               -> none (045 owns it)

# Final staged set (worktree == index for all five)
A  docs/aiven/patches/045-disable-individual-dictionary-sources.md
M  src/Common/config.h.in
M  src/Dictionaries/registerCacheDictionaries.cpp
M  src/Dictionaries/registerDictionaries.cpp
M  src/configure_config.cmake
```

## What I did

- **Preflight:** confirmed HEAD = `v26.3.10.62-lts-aiven-dev`; source commit `d4973afdd7` exists; captured author (`Tilman Moeller <tilman.moeller@aiven.io>`, 2026-01-06; committer Joe Lynch). Found the four source edits **already staged by a prior run** and verified them against the spec rather than blindly redoing.
- **Drift / Tier 1+2:** re-verified independently — 26.3 HEAD has no `REGISTER_DICTIONARY` mechanism (still-needed); `registerDictionarySourceYTsaurus` and all entry points present; no YTSAURUS collision (045 owns the new toggle); `config.h.in` correctly hand-placed after the `USE_*` block; decomposition shows only the three documented deviations differ. All three deviations confirmed present in the staged diff (19 markers).
- **Build (Tier 3a):** the cold full build was repeatedly disrupted by concurrent sibling `ninja` invocations on the shared `build/` (see caveat), but a clean link ultimately produced a working `26.3.10.1` binary; validated by `SELECT 1` and `nm`.
- **Verify (Tier 3b):** captured the defaults-on proof (generated `config.h` all-1 + all 16 registration symbols defined in the binary, incl. `YTsaurus`) and the guard-works proof (object-level: `MYSQL 0` elides exactly `registerDictionarySourceMysql`, controls intact) — the preprocessor-level demonstration explicitly permitted by Step 6. Also ran upstream `01018_ddl_dictionaries_create` → PASS.
- **Dossier:** `docs/aiven/patches/045-disable-individual-dictionary-sources.md` is authored and accurate (RPM ground-truth selection table, three deviations + rationale, (v) no-gate reasoning, YTsaurus ownership note, evidence, §0 lineage cell = stable handle `patch-port(045)`, harness URL). Staged.
- **Staged** the 5 deliverable files; left the ledger `docs/aiven/uplifts/26.3/log.md`, contrib submodule pointers, and unrelated untracked files **unstaged** (the human finalizes the ledger). No commit/rebase/push/amend; never-touch list untouched; all scratch under `tmp/patch-045/`.

## Proposed next step

Ready for human commit. Suggested: `git commit -F <message-file>` using the `proposed_commit.commit_message` above (it carries the `Original author:`, `Co-authored-by:`, deviations note, RPM parity URL, and `cherry picked from` trailers).

**Environment caveat (not a patch defect — please read before committing):** during this dispatch the shared `build/` directory was being built by **~6 concurrent `ninja` invocations** (sibling/orphaned builds spawned at 14:03–15:41, all writing into `tmp/patch-045/`). This caused a spurious `contrib/libuv-cmake/lib_uv.a` archive race (`llvm-ranlib: ... No such file or directory`) and a mid-run cargo build-directory-lock stall. The contention eventually resolved into a valid binary, and **my verification does not depend on which build produced it** (functional run + `nm` validate the artifact directly; the guard-works proof is fully race-independent at the object level). I did **not** kill the sibling processes — coordinating/terminating other workers' builds is beyond a worker's authority. Recommendation: before the final commit, ensure a **single serialized** `ninja -C build clickhouse` (exit 0) on a quiesced build dir to remove any doubt; all components are proven to compile, so it should be clean. Also note sibling workers were concurrently editing the staged dossier (it grew 263→287 lines under me); I re-staged so the staged dossier == worktree at hand-off, but re-confirm the dossier hasn't drifted again at commit time.
````

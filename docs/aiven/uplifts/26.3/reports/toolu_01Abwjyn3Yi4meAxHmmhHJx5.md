<!-- subagentStop archive | source=transcript | status=completed | type=general-purpose | ts=2026-06-12T08:30:29Z -->

Done. Full halt-and-escalate report below.

---
outcome: success
patch_slug: disable-ytsaurus-engine
source_sha: d297c19ff8331ecd7ae3e75be2cdc39d3c010796
proposed_commit:
  staged_files:
    - src/Storages/registerStorages.cpp
    - docs/aiven/patches/070-disable-ytsaurus-engine.md
  commit_message: |
    patch-port(070): Disable YTsaurus engine

    Original author: Aliaksei Khatskevich <alex.khatskevich@aiven.io>, 2026-03-16.

    (cherry picked from commit d297c19ff8331ecd7ae3e75be2cdc39d3c010796)

    26.3 adaptation: the source's registerDictionaries.cpp hunk is omitted — that
    dictionary source is already gated by REGISTER_DICTIONARY_SOURCE_YTSAURUS via
    patch-port(045); the USE_YTSAURUS prefix is added by patch-port(071). Only the
    registerStorages.cpp guard (which was unguarded on 26.3) is applied, so a
    USE_YTSAURUS=0 build links. No default-behavior change (USE_YTSAURUS defaults
    ON). byte_equivalent: false.
  byte_equivalent: false
tests:
  added: no_justified
  kind: upstream-existing
  paths: []
  upstream_reference:
    - tests/queries/0_stateless/01645_system_table_engines.sql
  pre_patch_fail_verified: false
  post_patch_pass_verified: true
  justification: |
    Build-correctness-only change: a single `#if USE_YTSAURUS … #endif` wrap
    around the `registerStorageYTsaurus(factory)` call. `USE_YTSAURUS` defaults
    ON (proof: the unguarded call currently compiles ⇒ symbol available ⇒
    `USE_YTSAURUS=1`), so the guard is true on a default build → zero
    default-behavior change. The only behavior the patch introduces — eliding
    the YTsaurus engine registration — is a compile-time property requiring a
    second `-DUSE_YTSAURUS=0` configure+rebuild that the stateless runner cannot
    express; a worktree-flip pair is pass-pass on a default build (no
    differential). There is no in-tree YTsaurus functional test (the engine
    needs an external YTsaurus cluster). Per AGENTS §7(b) the posture is
    `no_justified`, mirroring committed 045/051/052. Verified post-patch by
    running an existing upstream test that exercises the registration catalog
    this guard feeds: `01645_system_table_engines` → `[ OK ]`, plus the
    YTsaurus-specific smoke `SELECT count() … WHERE name='YTsaurus'` = 1. The
    `USE_YTSAURUS=0`-compiles benefit is mechanically certain (forward-decl is
    already `#if USE_YTSAURUS`-guarded; guarding the call removes the only
    remaining dangling reference) and is documented in the dossier, not
    separately built.
escalation_reason: none
---

## Tier results

- Tier 1 (textual cherry-pick): **pass** — `registerStorages.cpp` (Hunk 2) applied clean; `registerDictionaries.cpp` (Hunk 1) conflicted as predicted and was resolved by keeping HEAD verbatim (intentional drop).
- Tier 2 (semantic patch-id): **n/a (by design)** — `byte_equivalent: false` because source Hunk 1 is intentionally dropped (already satisfied by committed 045). The single applied hunk's added lines (`+#if USE_YTSAURUS` / `+#endif`) are byte-identical to the source's Hunk 2.
- Tier 3 (build + verify): **pass** — fast incremental build (20 steps, ninja exit 0, no CMake reconfigure); guard present; default-build smoke = 1; upstream test PASS.

## Evidence

**Drift (Step 1) — Hunk 1 already 045-gated, Hunk 2 unguarded, forward-decl guarded:**
```
# head-dict-guard.log
89-#if REGISTER_DICTIONARY_SOURCE_YTSAURUS
90:        registerDictionarySourceYTsaurus(source_factory);
91-#endif
# head-storage-callsite.log
    registerStorageMongoDB(factory);
#endif

    registerStorageYTsaurus(factory);          <-- UNGUARDED on HEAD
# head-storage-fwddecl.log
74-#if USE_YTSAURUS
75:void registerStorageYTsaurus(StorageFactory & factory);
76-#endif
```

**Cherry-pick (Step 2) — Hunk 2 staged clean, Hunk 1 conflicts:**
```
Auto-merging src/Dictionaries/registerDictionaries.cpp
CONFLICT (content): Merge conflict in src/Dictionaries/registerDictionaries.cpp
Auto-merging src/Storages/registerStorages.cpp
error: could not apply d297c19ff83... Disable YTsaurus engine
```
Resolution: `git checkout HEAD -- src/Dictionaries/registerDictionaries.cpp` (keep HEAD verbatim). No unmerged paths remain; no `.git/CHERRY_PICK_HEAD`/`.git/sequencer` left → clean for the human's `git commit -F`.

**Staged `registerStorages.cpp` diff (the one guard) — `staged-storage.diff`:**
```
@@ -194,7 +194,9 @@ void registerStorages()
     registerStorageMongoDB(factory);
 #endif
 
+#if USE_YTSAURUS
     registerStorageYTsaurus(factory);
+#endif
 #if REGISTER_REDIS_TABLE_ENGINE
     registerStorageRedis(factory);
 #endif
```

**Staged `registerDictionaries.cpp` diff is EMPTY (045 NOT regressed) — `staged-dict.diff`:**
```
=== staged-dict.diff size (bytes, expect 0) === 0
=== live staged dict diff === lines: 0
```
The HEAD region remains `#if REGISTER_DICTIONARY_SOURCE_YTSAURUS` / call / `#endif` (045's gate intact).

**Build (Step 4) — fast incremental, no reconfigure — `build-postpatch.log`:**
```
Re-running CMake occurrences: 0
[16/20] Building CXX object src/CMakeFiles/dbms.dir/Storages/registerStorages.cpp.o
[17/20] Linking CXX static library src/libdbms.a
[19/20] Linking CXX executable programs/clickhouse
ninja exit: 0
```
Wall: 08:21:28Z → 08:22:16Z (~48s).

**Guard present (Step 5a) — `guard-present.log`:**
```
197-#if USE_YTSAURUS
198:    registerStorageYTsaurus(factory);
199-#endif
```

**Default-build smoke (Step 5b) — `smoke.log`:**
```
SELECT count() FROM system.table_engines WHERE name='YTsaurus'  ->  1
```

**Upstream test post-patch (registration catalog) — `upstream-test.log`:**
```
01645_system_table_engines:                                             [ OK ] 0.08 sec.
1 tests passed. 0 tests skipped.
```

**Final staged set (Step 7) — exactly 2 files — `final-status.log`:**
```
Changes to be committed:
	new file:   docs/aiven/patches/070-disable-ytsaurus-engine.md
	modified:   src/Storages/registerStorages.cpp
 docs/aiven/patches/070-disable-ytsaurus-engine.md | 230 ++++++++++++++++++++++
 src/Storages/registerStorages.cpp                 |   2 +
```
No `registerDictionaries.cpp`, no `config.h.in`, no `configure_config.cmake`, no `tests/queries/`.

## What I did

- Preflight: confirmed HEAD = `v26.3.10.62-lts-aiven-dev`; `git status` showed only known carry-overs/submodule worktree drift, nothing unexpectedly pre-staged; `d297c19ff8` is a `commit`.
- Drift analysis: confirmed the two-hunk source lands as predicted — dict call already `REGISTER_DICTIONARY_SOURCE_YTSAURUS`-gated by 045 (drop Hunk 1), storage call unguarded while its forward-decl is `#if USE_YTSAURUS`-guarded (apply Hunk 2). Wrote `drift-conclusion.txt` (`partial`).
- Cherry-picked `--no-commit -x`; resolved the `registerDictionaries.cpp` conflict by restoring HEAD verbatim (empty staged diff), leaving only the `registerStorages.cpp` guard staged.
- Built `clickhouse` (clang-21, `ninja -C build clickhouse`): 20-step incremental, exit 0, no reconfigure.
- Verified guard present; started the smoke server (granted `users.xml` recipe), got `YTsaurus` count = 1, ran `01645_system_table_engines` post-patch ([ OK ]), stopped the server cleanly.
- Wrote and staged the dossier `docs/aiven/patches/070-disable-ytsaurus-engine.md`; wrote `tmp/patch-070/commit-message.txt`.
- Transient hook fail-closed blocks occurred repeatedly and were resolved by retrying the same command (1–3 retries each), per the dispatch's environment note. No real failures.

## Proposed next step

Ready for human commit. Suggested:
```
git commit -F tmp/patch-070/commit-message.txt
```
The staged set is exactly the two files above; the message body carries the `Original author:` line (do not use `git commit --author=` or `-c CHERRY_PICK_HEAD`). The next family link, patch 071, will add the `REGISTER_YTSAURUS_*` flags and the `USE_YTSAURUS && ` prefixes (including the dictionary-source two-level check).

---

**Today you learned:** On this lineage a multi-hunk upstream commit can land as a strict subset — one hunk is already satisfied by an earlier-committed sibling (045), so the correct port *drops* it and an empty staged diff for that file is the proof the earlier patch wasn't regressed.

**Rule of thumb:** When a guard's forward-declaration is already conditional (`#if USE_YTSAURUS`) but its call site isn't, guarding the call is pure build-correctness — true on the default build (zero behavior change), load-bearing only for the disabled-flag build.

**Next rabbit hole:** Patch 071 — layering `REGISTER_YTSAURUS_TABLE_ENGINE` / `REGISTER_DICTIONARY_SOURCE_YTSAURUS` `USE_YTSAURUS && …` two-level checks on top, where the dropped dict hunk finally gets its `USE_YTSAURUS` prefix.

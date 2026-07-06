<!-- subagentStop archive | source=transcript | status=completed | type=general-purpose | ts=2026-06-10T13:28:59Z -->

Here is my halt-and-escalate report.

---
outcome: escalate
patch_slug: keeper-map-read-only-setting
source_sha: 4c16ca65e99e4c65789d117ec722fbe5a16cb3cb
proposed_commit:
  staged_files:
    - src/Storages/KeeperMapSettings.cpp
    - src/Storages/KeeperMapSettings.h
    - src/Storages/StorageKeeperMap.cpp
    - src/Storages/StorageKeeperMap.h
  commit_message: |
    patch-port(054): Add read-only setting to KeeperMap storage

    Original author: Tilman Moeller <tilman.moeller@aiven.io>, 2026-01-09.

    Co-authored-by: Salvatore Mesoraca <salvatore.mesoraca@aiven.io>
    Co-authored-by: Aliaksei Khatskevich <alex.khatskevich@aiven.io>

    (cherry picked from commit 4c16ca65e99e4c65789d117ec722fbe5a16cb3cb)

    The StorageKeeperMap constructor was manually merged to keep HEAD's
    Aiven-private `override_metadata` parameter and append
    `keeper_map_settings_` as the final ctor argument at all three sites
    (.h decl, .cpp signature, create() make_shared), hence
    byte_equivalent: false.

    NOT YET BUILDABLE — see escalation. Three 26.3 upstream API-restructure
    adaptations are still required beyond the authorized override_metadata
    merge (ASTPtr -> boost::intrusive_ptr, make_intrusive<ASTSetQuery>,
    IDatabase::alterTable 4th arg). This message is a draft pending
    authorization of the expanded rewrite.
  byte_equivalent: false
tests:
  added: no_source_change
  kind: n/a
  paths:
  upstream_reference:
  pre_patch_fail_verified: false
  post_patch_pass_verified: false
  justification: |
    Escalation before a verifiable port: the build does not compile, so no
    honest evidence-of-causation pair can be produced yet. NOTE: this is an
    escalation, not a success claim — the `no_source_change` enum value is the
    closest available marker but is imperfect here (source files ARE staged).
    The accurate state: a tests/ file was NOT authored because the patch must
    first build; the staged src/ changes are a conflict-resolved cherry-pick
    that fails Tier 3a. Test design itself is not blocked (the Step 5 design is
    sound); the blocker is the build. Treat `tests.added` as N/A-pending.
escalation_reason: build_fail_api_rename

## Tier results

- Tier 1 (textual cherry-pick): **pass** — conflicts confined to `StorageKeeperMap.{cpp,h}` (Outcome B); resolved per policy call 1 (kept `override_metadata`, appended `keeper_map_settings_`). One *additional* trivial additive conflict the parent didn't predict: HEAD carries an `extern const int TABLE_WAS_NOT_DROPPED;` the 25.8 source lacks — resolved by keeping both error-code externs. New files `KeeperMapSettings.{cpp,h}` staged cleanly.
- Tier 2 (semantic patch-id): **pass (mismatch, decomposition clean)** — `byte_equivalent: false`; decomposition shows the staged diff differs from source **only** by the `override_metadata` carry-through at the three ctor sites. No other semantic change in the diff.
- Tier 3 (build + test): **fail** — Tier 3a build FAILED with three upstream-API-restructure compile errors (below); Tier 3b not reached.

## Evidence

**Cherry-pick (`tmp/patch-054/cherrypick.log`):**
```
Auto-merging src/Storages/StorageKeeperMap.cpp
CONFLICT (content): Merge conflict in src/Storages/StorageKeeperMap.cpp
Auto-merging src/Storages/StorageKeeperMap.h
CONFLICT (content): Merge conflict in src/Storages/StorageKeeperMap.h
...
new file:   src/Storages/KeeperMapSettings.cpp
new file:   src/Storages/KeeperMapSettings.h
both modified:   src/Storages/StorageKeeperMap.cpp
both modified:   src/Storages/StorageKeeperMap.h
```

**Decomposition (`tmp/patch-054/decomposition.log`) — only `override_metadata` differs:**
```
< -    UInt64 keys_limit_)                    | > -    bool override_metadata)
< +    UInt64 keys_limit_,                    | > +    bool override_metadata,
< -        ... zk_root_path, keys_limit);     | > -        ... keys_limit, override_metadata);
< +        ... zk_root_path, keys_limit, settings); | > +  ... keys_limit, override_metadata, settings);
< -        UInt64 keys_limit_);               | > -        bool override_metadata);
< +        UInt64 keys_limit_,                | > +        bool override_metadata,
```

**Build FAIL (`tmp/patch-054/build-postpatch.log`, `ninja exit: 1`) — three 26.3 API drifts:**
```
FAILED: src/CMakeFiles/dbms.dir/Storages/KeeperMapSettings.cpp.o
src/Storages/KeeperMapSettings.h:12:7: error: type alias redefinition with different types
   ('std::shared_ptr<IAST>' vs 'boost::intrusive_ptr<IAST>')
   12 | using ASTPtr = std::shared_ptr<IAST>;
src/Parsers/IAST_fwd.h:14:7: note: previous definition is here
   14 | using ASTPtr = boost::intrusive_ptr<IAST>;
src/Storages/KeeperMapSettings.cpp:84:12: error: no viable conversion from returned value of type
   'shared_ptr<DB::ASTSetQuery>' to function return type 'ASTPtr' (aka 'intrusive_ptr<IAST>')
   84 |     return settings_ast;
...
FAILED: src/CMakeFiles/dbms.dir/Storages/StorageKeeperMap.cpp.o
src/Storages/StorageKeeperMap.cpp:1729:113: error: too few arguments to function call, expected 4, have 3
 1729 |     ...->alterTable(context_, table_id, new_metadata);
src/Databases/IDatabase.h:353:18: note: 'alterTable' declared here
```
(The `ASTPtr` redefinition also fans out to every TU that includes the new header, e.g. `DatabaseReplicated.cpp.o`.)

There is **no** pre/post test evidence pair — the post-patch binary does not build, so producing one is impossible at this stage. That is the honest reason this is an escalation, not a success.

## What I did

- **Step 0–1 (preflight + drift):** HEAD on `v26.3.10.62-lts-aiven-dev`; tree clean modulo known carry-overs. Independently confirmed the parent's drift priors: `KeeperMapSettings`=0 on HEAD (still needed), framework present (`StorageFactory`, `supports_settings`, `has_builtin_setting_fn`, `TABLE_IS_READ_ONLY`, `checkAlterIsPossible`, `BaseSettings` all >0), `override_metadata`=5, and the upstream-equivalent search EMPTY. Verified the four conflict-region pre-patch shapes. Conclusion: `still-needed-but-rewrite`.
- **Step 2 (cherry-pick):** `git cherry-pick --no-commit -x` → conflicts only in `StorageKeeperMap.{cpp,h}` (Outcome B). Resolved all four conflict markers by hand per policy call 1 (the `.h` ctor decl, `.cpp` ctor signature, `.cpp` create() make_shared, plus the additive ErrorCodes block). Verified the init-list `keeper_map_settings(...)`, the `registerStorageKeeperMap` rewrite, the read-only throws, the new `checkAlterIsPossible`/`alter`, and the includes all landed. Staged the two resolved files.
- **Step 3 (patch-id):** mismatch as expected; decomposition clean (only `override_metadata`). `byte_equivalent: false`.
- **Step 4 (build):** exported clang-21, ran **plain** `cmake -B build` (new-TU glob refresh — it picked up `KeeperMapSettings.cpp`), then `ninja -C build clickhouse`. Build FAILED on three upstream-API drifts the **parent preflight did not catch**, two of them inside the brand-new translation unit:
  1. `ASTPtr` is `boost::intrusive_ptr<IAST>` on 26.3 (was `std::shared_ptr<IAST>` on 25.8). The new `KeeperMapSettings.h` redefines it → type-alias collision.
  2. ASTs are constructed with `make_intrusive<ASTSetQuery>()` on 26.3; the new `KeeperMapSettings.cpp::getSettingsChangesQuery` uses `std::make_shared<ASTSetQuery>()` and returns it as `ASTPtr` → no viable conversion. (This method has **no callers** in the checkout — dead-but-must-compile.)
  3. `IDatabase::alterTable` gained a required 4th param `bool validate_new_create_query`; the patch's new `alter()` passes only 3.
- **Stopped rather than expand the rewrite.** Policy call 1 explicitly bounded the authorized divergence to the `override_metadata` carry-through and instructed STOP on any other semantic change; AGENTS §6 + hard constraint 11 make a build failure naming a restructured upstream symbol an explicit STOP condition. Two of the three fixes are mechanical, but one (#2) is a body rewrite of a new file, and all three exceed the parent's authorization and rest on a preflight premise that proved wrong ("new-file hunks anchor cleanly / PASS-SEMANTIC"). I did **not** write the test or dossier (a "verified" dossier would be dishonest with no buildable binary). Findings persisted to `tmp/patch-054/api-drift-findings.txt`.
- **State left for the human:** the conflict-resolved cherry-pick is staged (4 source files; no conflict markers; cherry-pick sequencer already cleared, so the `-x` provenance line must be supplied via the commit message — included in the draft above). No worktree flip was performed, so no flip-back was needed.

## Proposed next step

**Need policy decision: authorize the expanded `still-needed-but-rewrite` (three 26.3 API adaptations beyond the `override_metadata` merge), or have a human apply them.** All three fixes are high-confidence (determined by the compiler + 26.3's own usage), so this can be resolved quickly:

1. `src/Storages/KeeperMapSettings.h` — replace the manual forward-decl
   ```cpp
   class IAST;
   using ASTPtr = std::shared_ptr<IAST>;
   ```
   with `#include <Parsers/IAST_fwd.h>` (the canonical 26.3 `ASTPtr = boost::intrusive_ptr<IAST>`).
2. `src/Storages/KeeperMapSettings.cpp:79` — `std::make_shared<ASTSetQuery>()` → `make_intrusive<ASTSetQuery>()` (the 26.3 idiom used by `DatabaseReplicatedSettings`, `MergeTreeSettings`, `ParserSetQuery`, …).
3. `src/Storages/StorageKeeperMap.cpp` `alter()` — append the 4th arg: `->alterTable(context_, table_id, new_metadata, /*validate_new_create_query=*/true);` (matching `StorageMergeTree`'s local-alter calls).

If authorized, I can apply these three, rebuild (incremental — `KeeperMapSettings.cpp` is already in the build graph), then complete Steps 5–9 (write `9054_keeper_map_read_only_setting.{sql,reference}`, run the Keeper-enabled server per policy call 4, produce the pre/post evidence pair, author the dossier, and return a `success` report). Estimated additional time: well within the remaining budget. The drift conclusion stays `still-needed-but-rewrite` and `byte_equivalent: false`; the dossier §2/§6 should record these three 26.3 API restructures so the runbooks and the next add-new-TU patch benefit.

**Learning log**
- **Today you learned:** A backported patch that *adds a new translation unit* must be re-checked against base-version API/typedef drift independently of cherry-pick cleanliness — the 25.8 `KeeperMapSettings.{h,cpp}` baked in the old `ASTPtr = std::shared_ptr` and `make_shared<AST>` conventions, which 26.3 replaced with `boost::intrusive_ptr` + `make_intrusive`; a clean 3-way merge can't see that. Also, `IDatabase::alterTable` gained a `validate_new_create_query` param between 25.8 and 26.3.
- **Rule of thumb:** Run a compile of new/modified TUs *before* trusting a "framework is present" preflight — the linter/compiler is the authoritative drift detector for renamed/restructured symbols, and catching it pre-build (via clangd) saves a full ninja cycle.
- **Next rabbit hole:** Confirm whether `getKeeperMapSettingsRef` / `getSettingsChangesQuery` should be retained as dead-but-compiling API or trimmed for this fork, and whether `validate_new_create_query=true` is the semantically correct choice for a KeeperMap settings-only alter (vs `false` as `StorageReplicatedMergeTree` uses on the replicated path).

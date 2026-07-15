# Patch 054 — keeper-map-read-only-setting

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `4c16ca65e99e4c65789d117ec722fbe5a16cb3cb` | Tilman Moeller (committer Joe Lynch) | original carry |
| 26.3-aiven | (staged) | T3.17 worker | conflict-resolved + drift-rewritten (`override_metadata` ctor merge + three 26.3 API adaptations) |

The current uplift's row stays "(staged)" until the human commits.

## 1. Purpose

This patch adds a `read_only` Bool engine setting to the `KeeperMap` storage. When enabled, all write paths (`INSERT`, `TRUNCATE`, mutations via `ALTER ... UPDATE/DELETE`) throw `TABLE_IS_READ_ONLY` (code 242), and `ALTER` is rejected unless it only touches settings (`MODIFY SETTING` / `RESET SETTING`) or the table comment. Aiven needs this to create read-only views of `KeeperMap` data and to prevent accidental modification of production `KeeperMap` tables. The "why" is durable: the implementation follows the `BaseSettings` framework (the same pImpl pattern `MergeTreeSettings` uses), so the motivation outlives any framework reshaping across uplifts.

Source SHA on `v25.8.18.1-lts-aiven`: `4c16ca65e99e4c65789d117ec722fbe5a16cb3cb` (from `docs/aiven/uplifts/26.3/inventory.md` row 054).
Original author: Tilman Moeller <tilman.moeller@aiven.io> (committer Joe Lynch <joelynch112@gmail.com>), author date 2026-01-09.
Original purpose (verbatim):

```
Add read-only setting to KeeperMap storage

This commit adds a `read_only` setting to KeeperMap storage that prevents
write operations (INSERT, UPDATE, DELETE, TRUNCATE, ALTER) when enabled.
This is useful for creating read-only views of KeeperMap data or preventing
accidental modifications in production environments.

The implementation follows the BaseSettings framework pattern used by
MergeTreeSettings, ensuring consistency with the ClickHouse codebase.

Co-authored-by: Salvatore Mesoraca <salvatore.mesoraca@aiven.io>
Co-authored-by: Aliaksei Khatskevich <alex.khatskevich@aiven.io>
```

## 2. Upstream-drift findings

### Commands run

```bash
git grep -c "<id>" -- src/   # for each identifier in the inventory
git log --oneline v25.8.18.1-lts..v26.3.10.62-lts -- src/Storages/StorageKeeperMap.cpp src/Storages/StorageKeeperMap.h
git log --oneline -S 'KeeperMapSettings' v25.8.18.1-lts..v26.3.10.62-lts -- src/
git log --oneline -S 'read_only' v25.8.18.1-lts..v26.3.10.62-lts -- src/Storages/StorageKeeperMap.cpp
```

### Findings

- Upstream changes to touched files between prior and current LTS:
  - `src/Storages/StorageKeeperMap.{cpp,h}`: the `StorageKeeperMap` constructor gained an Aiven-private trailing parameter `bool override_metadata` (5 occurrences across the two files) and a `Coordination::setCurrentComponent(...)` guard at the top of several method bodies. The `write`/`truncate`/`mutate` `checkTable<true>(local_context);` anchors and the simple `registerStorageKeeperMap` `{.supports_sort_order, .supports_parallel_insert}` block are unchanged.
- Upstream changes that touched the patch's behaviour (symbols, error codes, callers):
  - `KeeperMapSettings`: absent on HEAD (grep count 0) → the patch is still needed; no upstream equivalent.
  - `read_only` in `StorageKeeperMap.cpp`: no upstream `-S` hit → no superseding fix.
  - Framework present on 26.3: `StorageFactory` (311), `supports_settings` (52), `has_builtin_setting_fn` (41), `TABLE_IS_READ_ONLY` (32), `checkAlterIsPossible` (51), `BaseSettings` (235).
  - **Three 26.3 API restructures** the 25.8 source predates (the additional rewrite beyond `override_metadata` — durable knowledge for the next add-new-TU patch):
    1. **`ASTPtr` is now `boost::intrusive_ptr<IAST>`**, not `std::shared_ptr<IAST>`. The source's hand-rolled `class IAST; using ASTPtr = std::shared_ptr<IAST>;` in `KeeperMapSettings.h` collides with the canonical `Parsers/IAST_fwd.h` typedef and breaks every TU that includes both (e.g. `DatabaseReplicated.cpp`). Resolution: replace the hand-rolled pair with `#include <Parsers/IAST_fwd.h>` (which also provides `make_intrusive`); keep the other forward decls.
    2. **AST construction idiom is `make_intrusive`, not `std::make_shared`.** Because `ASTPtr` is now intrusive, `std::make_shared<ASTSetQuery>()` no longer converts to `ASTPtr`. Every sibling settings class (`KafkaSettings`, `SetSettings`, RocksDB, RabbitMQ, NATS, MaterializedPostgreSQL, ObjectStorageQueue) uses `make_intrusive<ASTSetQuery>()`; `KeeperMapSettings::getSettingsChangesQuery` was the lone `std::make_shared`. Resolution: `make_intrusive<ASTSetQuery>()`.
    3. **`IDatabase::alterTable` gained a 4th parameter `bool validate_new_create_query`.** The `alter()` body's `alterTable(context_, table_id, new_metadata)` is now too few arguments. Every local-storage caller (`StorageView`, `StorageNull`, `StorageMemory`, `StorageObjectStorage`, `IStorage`) passes `/*validate_new_create_query=*/true`; `false` is only on the replicated path, which this is not. Resolution: append `/*validate_new_create_query=*/true`.
- Conclusion: **`still-needed-but-rewrite`**. The semantics are unchanged but the cherry-pick cannot apply byte-for-byte: (a) the constructor conflicts at three sites due to the Aiven-private `override_metadata` param, resolved by keeping `override_metadata` and appending `keeper_map_settings_` as the final parameter; and (b) three mechanical 26.3 API adaptations (above) are required to compile. `byte_equivalent: false` is therefore expected and correct — the Step 3 decomposition shows the staged diff differs from the source diff ONLY by these four bounded changes plus context/line-number drift, with no other semantic change.

## 3. C++ review

Per `docs/aiven/skills/cpp-review-checklist.md`:

- 1 Lifetime + ownership: ✓ `KeeperMapSettings` is a pImpl (`std::unique_ptr<KeeperMapSettingsImpl> impl`); the copy ctor deep-copies `*impl`. `StorageKeeperMap` holds it by `std::unique_ptr<KeeperMapSettings> keeper_map_settings`, constructed from the by-const-ref ctor arg. No raw ownership, no dangling — the storage owns its own settings copy.
- 2 Exception safety: ✓ The read-only guards (`throw TABLE_IS_READ_ONLY`) fire at the very start of `write`/`truncate`/`mutate`, before any ZooKeeper mutation or sink construction — no partial state. `loadFromQuery` re-throws after annotating `UNKNOWN_SETTING`; no resource leak.
- 3 Thread-safety + concurrency: ✓ `read_only` is read under the storage's existing locking model; `alter()` runs under `AlterLockHolder` (passed by the framework) and mutates `*keeper_map_settings` then persists via `setInMemoryMetadata`. No new shared mutable state beyond what the alter lock already serialises.
- 4 Performance + memory: ✓ One extra `unique_ptr` indirection per write call to read a Bool — negligible; the setting read is on the control path, not per-row. No allocations on the hot insert path beyond what already existed.
- 5 Settings as public API: ✓ `read_only` is a documented engine setting (`"Make the table read-only"`), registered via `has_builtin_setting_fn = KeeperMapSettings::hasBuiltin`. Default `false` keeps it inert. This is the intended public surface.
- 6 Error handling: ✓ Uses the existing `TABLE_IS_READ_ONLY` (code 242) with KeeperMap-specific messages ("Cannot insert into / truncate / mutate / alter read-only KeeperMap table"). The test asserts both the error code and that the gate fires on a KeeperMap `SETTINGS` clause that pre-patch is rejected wholesale — distinguishing the Aiven gate (AGENTS §7).
- 7 Upstream / vendored code: ✓ No `contrib/**` or `.gitmodules` touched. The three API adaptations align `KeeperMapSettings` with the 26.3 idioms used by all sibling settings classes.
- 8 Behaviour under settings: ✓ With `read_only = false` (default), behaviour is identical to pre-patch except that `SETTINGS` is now accepted on the `CREATE`. Existing `KeeperMap` tables and tests are unaffected.

## 4. Test design

(a) **New test that fails on the parent commit and passes after the patch.**

- Test path: `tests/queries/0_stateless/9054_keeper_map_read_only_setting.{sql,reference}` (Aiven convention, `testing-suites.md §4.1`, prefix `9054` = dossier number). Tags `no-ordinary-database, no-fasttest` are justified (policy call 5): `KeeperMap` requires ZooKeeper/Keeper (absent in fasttest) and an Atomic/UUID database; every existing `KeeperMap` stateless test carries them.
- Pre-patch run output (the FAIL):

  ```text
  9054_keeper_map_read_only_setting:                                      [ FAIL ] 0.18 sec.
  Reason: return code:  36
  DB::Exception: Engine KeeperMap doesn't support SETTINGS clause. Currently only the
  following engines have support for the feature: [GCS, OSS, ... (KeeperMap absent) ...].
  (BAD_ARGUMENTS)
  (query: CREATE TABLE 9054_km_ro (...) ENGINE = KeeperMap(...) PRIMARY KEY key
          SETTINGS read_only = 1;)
  ```

- Post-patch run output (the PASS):

  ```text
  9054_keeper_map_read_only_setting:                                      [ OK ] 0.35 sec.
  1 tests passed. 0 tests skipped.
  ```

- Why this test distinguishes the Aiven gate from upstream behaviour (per AGENTS.md §7): Post-patch the `read_only` engine setting exists (`registerStorageKeeperMap` sets `supports_settings = true` + `has_builtin_setting_fn`), so `CREATE ... SETTINGS read_only = 1` is accepted and the write paths throw `TABLE_IS_READ_ONLY` (matched by `{ serverError TABLE_IS_READ_ONLY }`); `ALTER MODIFY SETTING read_only = 0` re-enables writes (count becomes 1). Pre-patch, `StorageKeeperMap` declares `supports_settings = false`, so the `SETTINGS read_only = 1` clause is rejected by `StorageFactory` at CREATE time (code 36, `BAD_ARGUMENTS`, "Engine KeeperMap doesn't support SETTINGS clause") → output diverges from `.reference` right after the first `1` → FAIL. The control table (`9054_km_rw`, no settings) proves the engine works in the environment, isolating the divergence to the `read_only` feature.

**Test environment requirement (for the next KeeperMap dispatch).** The bare `build-and-test.md §4` smoke server has `<zookeeper>` commented out, no `<keeper_server>`, and no `keeper_map_path_prefix`, so `KeeperMap` will not initialise. The working recipe:

```bash
mkdir -p tmp/ch-smoke/cfg/config.d
cp programs/server/config.xml                  tmp/ch-smoke/cfg/config.xml
cp tmp/ch-smoke/users.xml                       tmp/ch-smoke/cfg/users.xml   # granted default user
cp tests/config/config.d/keeper_port.xml        tmp/ch-smoke/cfg/config.d/   # embedded Keeper :9181
cp tests/config/config.d/zookeeper.xml          tmp/ch-smoke/cfg/config.d/   # <zookeeper> -> 127.0.0.1:9181
cp tests/config/config.d/enable_keeper_map.xml  tmp/ch-smoke/cfg/config.d/   # keeper_map_path_prefix
# Add a path-override drop-in (see "Anything surprising" in §6 — the relative
# --path CLI override is applied too late and the std::filesystem error in
# savePreprocessedConfig is not caught):
cat > tmp/ch-smoke/cfg/config.d/zz_path_override.xml <<'XML'
<clickhouse>
    <path>/ABS/PATH/tmp/ch-smoke/</path>
    <tmp_path>/ABS/PATH/tmp/ch-smoke/tmp/</tmp_path>
    <user_files_path>/ABS/PATH/tmp/ch-smoke/user_files/</user_files_path>
    <format_schema_path>/ABS/PATH/tmp/ch-smoke/format_schemas/</format_schema_path>
    <logger><log>/ABS/PATH/tmp/ch-smoke/server.log</log>
            <errorlog>/ABS/PATH/tmp/ch-smoke/server.err.log</errorlog>
            <level>warning</level></logger>
    <user_directories><local_directory><path>/ABS/PATH/tmp/ch-smoke/access/</path></local_directory></user_directories>
</clickhouse>
XML
./build/programs/clickhouse server --config-file ./tmp/ch-smoke/cfg/config.xml > tmp/ch-smoke/server.out 2>&1 &
# probes: SELECT 1 ; SELECT name FROM system.zookeeper WHERE path='/' LIMIT 1 ; CREATE ... ENGINE=KeeperMap(...)
# clickhouse-test: DROP --no-zookeeper; set CLICKHOUSE_HOST=127.0.0.1 (host has IPv6 disabled).
```

## 5. Rollback considerations

- Revert safety: SAFE. The patch adds an optional engine setting (default `false`); reverting it removes the `read_only` capability but does not change on-disk format or require schema migration. Tables created with `SETTINGS read_only = 1` would, after a revert, fail to re-attach only if the binary no longer recognises the setting — operationally the table metadata in ZooKeeper carries the settings clause, so a revert should be paired with toggling `read_only` off first.
- Surviving state: the `read_only` value is persisted in the table's `StorageInMemoryMetadata` / ZooKeeper metadata node (via `setInMemoryMetadata` + `alterTable`). It survives a `clickhouse-server` restart. No extra on-disk files or in-memory caches beyond the existing KeeperMap ZK nodes.
- Disable without rebuilding: set `ALTER TABLE ... MODIFY SETTING read_only = 0` (always permitted, even on a read-only table, by `checkAlterIsPossible`).
- Divergence from upstream-source: the only intentional divergences are (1) the constructor 3-site merge keeping the Aiven-private `override_metadata` and (2) the three 26.3 API adaptations in §2. `getKeeperMapSettingsRef()` is currently unused by other Aiven code in this checkout (added for symmetry / future use); kept verbatim from source.

## 6. Per-uplift notes

### 25.8-aiven (historical)

Original carry. Authored by Tilman Moeller, committed by Joe Lynch on `v25.8.18.1-lts-aiven`. Shipped no test.

### 26.3-aiven (this uplift)

- Cherry-pick was: **conflict-resolved + rewritten**. The `git cherry-pick --no-commit -x` conflicted (`UU`) at `StorageKeeperMap.cpp` and `StorageKeeperMap.h` at the constructor only; the new files `KeeperMapSettings.{cpp,h}` and all non-ctor hunks staged cleanly. Resolved per policy call 1: keep `override_metadata`, append `const KeeperMapSettings & keeper_map_settings_` as the final ctor param at the `.h` decl, `.cpp` signature, and the `create()` `make_shared` call; init-list appends `keeper_map_settings(std::make_unique<KeeperMapSettings>(keeper_map_settings_))`. Then **three mechanical 26.3 API adaptations** (authorised expansion of the rewrite, see §2): `ASTPtr` → `#include <Parsers/IAST_fwd.h>` (intrusive_ptr), `std::make_shared<ASTSetQuery>` → `make_intrusive<ASTSetQuery>`, and the `IDatabase::alterTable` 4th arg `/*validate_new_create_query=*/true`.
- Upstream-drift conclusion: `still-needed-but-rewrite` (§2). `byte_equivalent: false`; the Step 3 decomposition confirms the staged diff differs from source ONLY by the `override_metadata` carry-through and the three API adaptations, nothing else.
- Test added at: `tests/queries/0_stateless/9054_keeper_map_read_only_setting.{sql,reference}`. Pre-patch FAIL / post-patch PASS verified.
- Time-to-port: ~135 min subagent wall-clock (incl. an authorised escalation round-trip for the three API fixes and a Keeper-server config debugging detour). Build directory was **warm-cache** (sccache hot from patch 038); the post-fix build was a ~48-step incremental, the pre-patch flip a ~19-step incremental.
- Anything surprising — **two durable findings for the runbooks:**
  1. **First add-new-TU patch.** `KeeperMapSettings.cpp` is a brand-new translation unit; CMake globs sources, so a brand-new `.cpp` is not in `build.ninja` until a reconfigure. A **plain** `cmake -B build` (NOT `--fresh`) regenerates the build graph from the intact cache and preserves incrementality. The `$PATCHED_FILES` worktree-flip set is the TWO MODIFIED files only — the two new `KeeperMapSettings.*` are never flipped (they don't exist on HEAD; `git restore --source=HEAD` would delete them) and still compile as an unused standalone TU in the pre-patch state.
  2. **Keeper scratch-server `--path` gotcha.** The policy-call-4 recipe's relative `--path=./tmp/ch-smoke` CLI override is applied too late: `ConfigProcessor::savePreprocessedConfig` computes `<path>/preprocessed_configs` from the config's `<path>` (default `/var/lib/clickhouse/`) during `BaseDaemon::initialize`, and its `catch` handles only `Poco::Exception` — the `std::filesystem_error` from the unwritable `/var/lib/clickhouse` propagates and the server exits before the CLI `--path` ever applies. Likewise the logger's `<errorlog>` defaults to `/var/log/clickhouse-server`. The robust fix is a `config.d` drop-in that sets `<path>`, `<logger><log>/<errorlog>`, and `<user_directories><local_directory><path>` to **absolute** writable paths (merged during preprocessing, before any of those dirs are created), and to launch with ONLY `--config-file` (no `--` runtime path overrides). Also set `CLICKHOUSE_HOST=127.0.0.1` for `clickhouse-test` because this host has IPv6 disabled (the `[::1]` listen warnings are harmless).

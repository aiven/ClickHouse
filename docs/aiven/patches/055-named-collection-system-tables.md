# Patch 055 — named-collection-system-tables

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `d6d957fd418bca194ebf26688ffe6c9e7f966e34` | Tilman Moeller (committer Joe Lynch) | original carry |
| 26.3-aiven | (staged) | T3.18 worker + parent adaptation | conflict-resolved + one bounded drift-rewrite (`IStorage` member rename to dodge `-Wshadow-field` against new-in-26.3 engines) |

The current uplift's row stays "(staged)" until the human commits.

## 1. Purpose

This patch adds a `named_collection` `String` column to `system.tables`. For a table created from a named collection (e.g. `ENGINE = MySQL(my_nc)`), the column shows the collection's name; for every other table it shows `''`. Aiven needs this so operators can audit which named collections back which tables — especially for external-storage engines (`MySQL`, `PostgreSQL`, `S3`, Azure Blob) where the connection secrets live in the collection rather than the DDL. The "why" is durable: it is a pure observability addition (one nullable-ish string surfaced from the storage layer), independent of how the storage framework is reshaped across uplifts.

Source SHA on `v25.8.18.1-lts-aiven`: `d6d957fd418bca194ebf26688ffe6c9e7f966e34` (from `docs/aiven/uplifts/26.3/inventory.md` row 055).
Original author: Tilman Moeller <tilman.moeller@aiven.io> (committer Joe Lynch <joelynch112@gmail.com>), author date 2026-01-09.
Original purpose (verbatim):

```
Add named_collection column to system.tables

This patch adds a new `named_collection` column to the `system.tables` system
table, which displays the name of the named collection (if any) that was used
to create each table. This enables users to track which named collections are
associated with their tables, particularly useful for external storage engines
like MySQL, PostgreSQL, S3, and Azure Blob Storage.

Co-authored-by: Aliaksei Khatskevich <alex.khatskevich@aiven.io>
```

## 2. Upstream-drift findings

### Mechanism (what the patch threads)

- `IStorage` gains a 3rd ctor param `std::optional<String> named_collection_ = std::nullopt`, a protected member, and `virtual std::optional<String> getNamedCollection() const`.
- `NamedCollection` gains `const std::string & getName() const`.
- `StorageSystemTables`: a `named_collection` `String` column inserted positionally between `has_own_data` and `loading_dependencies_database`, plus a matching row-fill hunk that reads `table->getNamedCollection`.
- External storages thread the name into the `IStorage` ctor: `StorageMySQL` / `StoragePostgreSQL` (their `Configuration` gains a `named_collection` member + ctor optional param), ObjectStorage (`StorageObjectStorageConfiguration` gets a virtual `getNamedCollection` defaulting to `nullopt`; `S3` + Azure `Configuration` override it; `StorageObjectStorage` overrides `IStorage::getNamedCollection` and forwards `configuration_->getNamedCollection()` to the `IStorage` ctor). Table functions `MySQL`/`PostgreSQL` pass it through. The remaining touched files (`StorageNull`/`StorageProxy`/`StorageBlocks`/`StorageFromMergeTreeDataPart`/`ProjectionsDescription`/`IStorageSystemOneBlock`/`DatabaseMySQL`/`DatabasePostgreSQL`) are trivial ctor-forwarding / signature plumbing.

### Findings

- **Upstream equivalent? NONE.** No `named_collection` column in `StorageSystemTables.cpp` on HEAD; no `getNamedCollection` in `IStorage.h` on HEAD; `git log -S named_collection v25.8.18.1-lts..v26.3.10.62-lts -- src/Storages/System/StorageSystemTables.cpp` is empty. → **still needed**, not obsoleted by upstream.
- **Patched-line stability — the load-bearing interface hunks match HEAD.** `IStorage.h`'s pre-patch ctor on HEAD is exactly `explicit IStorage(StorageID storage_id_, std::unique_ptr<StorageInMemoryMetadata> metadata_ = nullptr);` (the source's pre-patch line); `StorageSystemTables.cpp`'s `has_own_data` and `loading_dependencies_database` schema entries are present and adjacent (insert point intact) and the `res_columns[res_index++]` row-fill loop pattern is present. The core hunks apply.
- **The one genuine 26.3 adaptation — `IStorage` member renamed `named_collection` → `named_collection_name`.** This is the only semantic deviation from the source diff, and it exists because of a 26.3-only collision:
  - The 25.8 source names the new `IStorage` member `named_collection` and (to keep its own subclasses compiling) renames the `named_collection` *parameter* to `named_collection_` in `StorageMySQL`/`StoragePostgreSQL`.
  - 26.3 added storage engines that did **not** exist in 25.8 and carry a `const NamedCollection & named_collection` parameter of their own: `StorageYTsaurus::processNamedCollectionResult`, `StorageArrowFlight::processNamedCollectionResult`, and `StorageMongoDB`. A new inherited `IStorage::named_collection` member shadows each of these parameters, and ClickHouse compiles with `-Wshadow-field -Werror`, so the build fails in files the patch never touches.
  - **Resolution (parent-authorised, within the policy-call-4 non-semantic envelope):** rename the *member* to `named_collection_name` (keeping the accessor `getNamedCollection` and the ctor parameter `named_collection_` verbatim). One rename at the base class resolves every shadow at once — `MySQL`/`PostgreSQL`/`YTsaurus`/`ArrowFlight`/`MongoDB` and any future engine — without touching a single subclass. This is strictly smaller and more rebase-stable than the alternative (chasing a `named_collection_` parameter rename into each new-in-26.3 engine), which is why the worker's initial per-engine `ArrowFlight`/`YTsaurus` parameter renames were reverted in favour of the base-class rename. The source's own `StorageMySQL`/`StoragePostgreSQL` parameter renames are kept verbatim from the cherry-pick (harmless under the new member name; preserves source fidelity).
- **Conclusion: `still-needed`, ported with one bounded drift-rewrite.** `byte_equivalent: false` is expected and correct. The Step 3 decomposition shows the staged diff differs from the source diff ONLY by (a) the member-name rename above and (b) non-semantic context/relocation drift:
  - `StorageBlocks.h`: the file moved upstream from `LiveView/` to `WindowView/`, so its `-`/`+` ctor-forwarding hunk appears at a different alphabetical position — identical content.
  - `S3/Configuration.cpp`: blank-line-count difference only; the one semantic line `named_collection = collection.getName();` matches the source.
  - `StoragePostgreSQL.{cpp,h}`: brace-placement/formatting drift inherited from HEAD, plus the source's documented `named_collection` → `named_collection_` parameter rename.
  - Azure `Configuration.cpp`: the relocated assignment matches the source line.

## 3. C++ review

Per `docs/aiven/skills/cpp-review-checklist.md`:

- **1 Lifetime + ownership:** ✓ `named_collection_name` is a `std::optional<String>` value member of `IStorage`, populated once in the ctor from a by-value `std::optional<String>` argument (moved in). No reference into a collection, no shared ownership, no dangling. `StorageObjectStorage::getNamedCollection` forwards `configuration_->getNamedCollection()`; `configuration_` is a member shared_ptr owned by the storage, so the forwarded value (a fresh `optional<String>` copy) outlives the call trivially.
- **2 Exception safety:** ✓ The accessor is `noexcept`-equivalent (returns a copy of an `optional<String>`); the ctor change only adds a member-initialiser (`std::move`), introducing no new throw site and no partial-construction window. `system.tables` row fill is read-only.
- **3 Thread-safety + concurrency:** ✓ `named_collection_name` is set during construction and never mutated afterwards — effectively immutable storage metadata. Concurrent `SELECT … FROM system.tables` readers call `getNamedCollection` on a const path with no shared mutable state, so no locking is required (the same model `IStorage::getStorageID`-style immutable fields use).
- **4 Performance + memory:** ✓ The column is materialised only when `system.tables` is queried and the `named_collection` column is in the read mask (the `columns_mask` gate). The per-row cost is one `getNamedCollection` virtual call returning a small `optional<String>` copy; there is **no** per-row cost on any data path (inserts/merges/reads of user tables are untouched). One extra `std::optional<String>` (24 bytes on this target) per `IStorage` instance — negligible against the rest of the object.
- **5 Settings as public API:** N/A — this adds an observability column, not a setting. The public surface is the new `system.tables.named_collection` column; its contract (collection name or `''`) is pinned by the `9055` test and the `02117` schema snapshot.
- **6 Error handling:** ✓ No new error paths. A table with no named collection yields `nullopt` → the row-fill inserts the column default (`''`). The pre-patch absence of the column is the SQL-observable signal (code 47 `UNKNOWN_IDENTIFIER`) the test asserts.
- **7 Upstream / vendored code:** ✓ No `contrib/**` or `.gitmodules` touched. The member rename aligns `IStorage` with the 26.3 `-Wshadow-field` regime; the accessor name and external behaviour are unchanged from the source patch.
- **8 Behaviour under settings:** ✓ Inert by construction — every existing table reports `''` and no existing query plan changes. Only a `system.tables` projection that explicitly selects `named_collection` sees new output.

## 4. Test design

(a) **New test that fails on the parent commit and passes after the patch.**

- Test path: `tests/queries/0_stateless/9055_named_collection_system_tables.{sql,reference}` (Aiven convention; prefix `9055` = dossier number). Tag `no-fasttest` is justified: the `MySQL` table engine is not built in the fasttest image. The `MySQL`-engine table is created with **explicit** columns and is never queried, so `StorageMySQL` never opens a connection (it connects only to infer an empty schema or on `SELECT`) — the test is fully deterministic and needs no live MySQL server (empirically validated against the scratch server).
- The evidence-of-causation pair:
  - **Pre-patch (FAIL):** `SELECT name, named_collection FROM system.tables …` → `Code: 47. DB::Exception: … Unknown expression identifier 'named_collection' …` (`UNKNOWN_IDENTIFIER`) — the column does not exist on the parent commit. Captured in `tmp/patch-055/test-prepatch.log`.
  - **Post-patch (PASS):** output matches the reference exactly:

    ```text
    t_9055_mysql	nc_9055
    t_9055_plain	
    ```

- Why this distinguishes the Aiven feature (per AGENTS.md §7): the test pairs a named-collection-backed table (`ENGINE = MySQL(nc_9055)` → expects `nc_9055`) with a control (`ENGINE = MergeTree` → expects `''`). Pre-patch the projection cannot even parse; post-patch both the populated and the empty cases are asserted, so the test fails if the column is dropped **or** if population regresses (e.g. always-empty, or wrong collection name).

(b) **Schema-snapshot references updated (additive, single line).**

- `tests/queries/0_stateless/02117_show_create_table_system.reference`: `system.tables`'s `SHOW CREATE` gains exactly one line, ``named_collection` String,`, inserted between `has_own_data` and `loading_dependencies_database`. Verified by running `02117_show_create_table_system.sql` against the patched server: the diff against the reference was that single line and nothing else; after the edit the test is a clean PASS.
- `tests/queries/0_stateless/02206_information_schema_show_database.reference`: **not affected.** `INFORMATION_SCHEMA.TABLES` selects a fixed projection of `system.tables` columns (`database`, `name`, `engine`, `has_own_data`, …) that does not include `named_collection`, so the view's `SHOW CREATE` is unchanged — confirmed by running `02206` against the patched server (clean PASS, no edit). A broad scan (`grep -rl has_own_data tests/queries/0_stateless/*.reference`) found only `02117` and `02206`, so no other full-schema snapshot of `system.tables` exists to update.

## 5. Rollback considerations

- **Revert safety: SAFE.** The patch only adds a read-only `system.tables` column and a storage-metadata field populated at construction. No on-disk format, no table metadata, no ZooKeeper node changes; nothing is persisted. Reverting removes the column and the accessor with no migration.
- **Surviving state: none.** `named_collection_name` lives only in the in-memory `IStorage` instance and is recomputed at attach time from the DDL/collection. A `clickhouse-server` restart re-derives it.
- **Forward-compat note:** any external tooling that learns to `SELECT named_collection FROM system.tables` would break on a revert — but that is a query-time dependency, not stored state.
- **Divergence from upstream-source:** the only intentional divergence is the `IStorage` member rename `named_collection` → `named_collection_name` (§2); the accessor, the ctor parameter, the column name/position, and all external behaviour are identical to the source patch.

## 6. Per-uplift notes

### 25.8-aiven (historical)

Original carry. Authored by Tilman Moeller, committed by Joe Lynch on `v25.8.18.1-lts-aiven`. Shipped no test.

### 26.3-aiven (this uplift)

- Cherry-pick was **conflict-resolved + one bounded drift-rewrite**. `git cherry-pick --no-commit -x` brought in 25 `src/` files; conflicts were resolved additively (context/relocation drift, §2). The single semantic adaptation is the `IStorage` member rename to satisfy `-Wshadow-field -Werror` against new-in-26.3 engines (`YTsaurus`, `ArrowFlight`, `MongoDB`); the worker's initial per-engine parameter renames were reverted in favour of the one base-class rename.
- Upstream-drift conclusion: `still-needed`; `byte_equivalent: false` (§2). The Step 3 decomposition confirms the staged diff differs from source ONLY by the member rename plus non-semantic drift.
- Test added at `tests/queries/0_stateless/9055_named_collection_system_tables.{sql,reference}`; `02117` reference updated; `02206` confirmed unaffected. Evidence pair (pre-patch code-47 FAIL / post-patch PASS) verified.
- **Anything surprising — a critical durable build finding (also recorded in `build-and-test.md`):** porting `055` exposed how the `#deps 0` hazard interacts with `cmake --fresh` + sccache to ship a **stale-object** binary that crashes at startup.
  1. `IStorage.h` is a very high-fanout header (344 headers / 624 `.cpp` TUs in its transitive include closure). A plain `ninja` after the cherry-pick only recompiled the directly-staged TUs and relinked, leaving stale `.o` files that still referenced the old 2-arg `IStorage` ctor → `ld.lld: undefined symbol: DB::IStorage::IStorage(StorageID, unique_ptr<…>)`. Fixed by touching the `IStorage.h` include closure (`tmp/patch-055/touch_istorage_includers.py`).
  2. The build directory's `CMakeFiles/rules.ninja` was missing, and a **plain** `cmake -B build` could not regenerate it: the cached compiler is the bare ccache wrapper (`/usr/lib64/ccache/clang++`) whose `CMAKE_CXX_COMPILER_VERSION` was stale-cached as `20.1.8` (failing `tools.cmake`'s `>= 21` gate) and whose `CMAKE_CXX_COMPILER_TARGET` was empty (so `default_libs.cmake`'s `build_clang_builtin(${TARGET} …)` got one argument). Per the runbook this is the "plain reconfigure itself errors out" case → recover with `cmake --fresh` using the **direct** clang-21 paths (`-DCMAKE_C_COMPILER=/opt/llvm-21/bin/clang -DCMAKE_CXX_COMPILER=/opt/llvm-21/bin/clang++`). With a warm sccache this was only ~1131 steps, not the full `contrib` rebuild.
  3. The post-`--fresh` binary built green but **crashed at server startup** with a libc++ hardening assertion `vector[] index out of bounds` in `AccessTypeToStringConverter::convert` (`AccessType.cpp:24`), reached from `StorageSystemPrivileges::getAccessTypeEnumValues` while attaching `system.grants`. Root cause: `AccessType.cpp.o` was built **Jun 8**, *before* the protected-users patch (`f796609c6cf`) added `PROTECTED_ACCESS_MANAGEMENT` to `AccessType.h` on **Jun 9** — but with `#deps 0`, neither incremental builds nor `--fresh` (empty deps DB + sccache cache-hit on the unchanged `.cpp`) ever recompiled it. So the stale converter's string table was sized to the *old* (smaller) enum, while the freshly-rebuilt `StorageSystemPrivileges.cpp.o` enumerated the *new* 230-entry enum and called `toString` on the new max value → out-of-bounds index. (Not a 055 bug, not an `AccessType` source bug — a pure build-artifact ODR mismatch; the old binary tolerated it silently because that `.o` predated libc++ hardening being effective on the read.)
  4. **The robust fix for a build dir with broken deps tracking:** `find src programs -name '*.cpp' … | xargs touch` then `ninja` — sccache then *genuinely* recompiles only the TUs whose expanded source actually changed (the stale ones, including `AccessType.cpp`) and cache-hits the rest. After this the server started clean and both the `9055` and `02117` tests passed.
- Build directory was warm-cache (sccache hot). The decisive rebuilds: the `--fresh` recovery ~1131 steps; the touch-all consistency rebuild 3818 steps (~25 min, mostly sccache hits with the genuinely-stale TUs recompiled).

# Patch 057 — remove-all-cloud-specific-settings

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `ac227df851` | Tilman Moeller (author), co-authored by Joe Lynch, Aliaksei Khatskevich, 2026-01-12 | (the version we are screening FROM) |
| 26.3-aiven | — (not dispatched) | parent agent deep-screen, 2026-06-17 | **HELD OPEN** — screened, disposition deferred; see §2/§7 |

> **Status: SCREENED, HELD OPEN (no decision yet).** This dossier records the
> deep screen so the decision can be made later without re-deriving it. No code
> is carried and the patch is neither ported nor formally dropped. The single
> open question that gates the disposition is in §7.

## 1. Purpose

Per the source commit body (`ac227df851`, 16 files, +18/−592):

> remove all cloud-specific settings
> Removed: Cloud mode flags and settings; Distributed cache settings;
> SharedMergeTree settings; Filesystem cache and cache warmer settings;
> Experimental cloud features; Cloud-specific MergeTree part storage settings;
> All cloud references from settings history.

ClickHouse's open-source tree carries many settings that **only function in
ClickHouse Cloud** (the proprietary `SharedMergeTree` engine, distributed cache,
cache-warmer). In an Aiven (OSS-derived) build they are read but **no OSS code
acts on them** — their consumers live in the closed-source Cloud tree. They are
still visible in `system.settings` / `system.merge_tree_settings`. 057 deletes
them to shrink the visible settings surface, drop `cloud_mode` and the few real
gates wired to it, and (the one genuine functional change) hard-disable the
`executable_pool` dictionary source.

Source SHA on `v25.8.x-lts-aiven`: `ac227df851`. Original author:
`tilman.moeller@aiven.io`.

## 2. Upstream-drift / validity findings

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`.

### Commands run

```bash
git show ac227df851                                   # the original 057 diff (saved: tmp/057-original.diff)
rg -n 'cloud_mode|restore_replicated_merge_tree_to_shared_merge_tree|cache_warmer_threads|ignore_cold_parts_seconds' src/Core/Settings.cpp
#  → all target settings still present on 26.3 (patch has its targets)
rg -l 'Setting::cloud_mode' src/                      # 7 files reference cloud_mode
#  057 only removes the reference from 4; ExecutableDictionarySource / FileDictionarySource / LibraryDictionarySource untouched
rg -n 'allow_background_download_during_fetch|allow_background_download_for_metadata_files_in_packed_storage' src/
#  → ReadSettings fields written in Context.cpp, READ NOWHERE in OSS src/
```

### Finding A — the patch does NOT compile/link on 26.3 (drift)

`Setting::cloud_mode` is referenced in **7** files on 26.3; 057 removes it from
only **4** (`BackupEntriesCollector`, `ExecutablePoolDictionarySource`,
`XDBCDictionarySource`, `ObjectStorageQueueMetadata`). Upstream added **three new
gates since 25.8** that 057 leaves with a dangling `extern const SettingsBool
cloud_mode;`:

- `src/Dictionaries/ExecutableDictionarySource.cpp:28,241`
- `src/Dictionaries/FileDictionarySource.cpp:23,94`
- `src/Dictionaries/LibraryDictionarySource.cpp:23,203`

Removing the `cloud_mode` `DECLARE` while these TUs reference it ⇒ **undefined
symbol at link time**. A faithful port must keep chasing every new `cloud_mode`
consumer upstream adds — direct evidence of the unbounded per-uplift cost.

### Finding B — against OSS reality the 610 lines collapse to ONE real change

Tracing each removed symbol to its consumer: the consumer is either cloud-only
or behind an off compile-guard for almost everything. The **only** change that
alters OSS runtime behavior is hard-disabling the `executable_pool` dictionary
source (§3, B1). Inventory:

| Removed by 057 | OSS effect | Verdict |
|---|---|---|
| ~22 `distributed_cache_*` + `default_distributed_cache_*` constexprs | `#if ENABLE_DISTRIBUTED_CACHE` off; sole read site compile-guarded | inert |
| ~40 `shared_merge_tree_*` (Settings + MergeTreeSettings) | SMT engine absent in OSS | inert |
| `cache_warmer_threads`, `ignore_cold_parts_seconds`, `prefer_warmed_unmerged_parts_seconds`, `number_of_partitions_to_consider_for_merge`, `reduce_blocking_parts_sleep_ms`, `min_bytes/rows_for_full_part_storage`, `compact_parts_max_*`, `merge_*_to_prewarm_cache` | cache-warmer / SMT machinery; `adjustDataPartsVectorBasedOnCacheWarmness` cloud-only | inert |
| `parts_kill_delay_period*`, `parts_killer_pool_size`, `license_key`, `storage_shared_set_join_use_inner_uuid` | SMT cleanup / enterprise / SharedSet only | inert |
| `cloud_mode_engine`, `cloud_mode_database_engine` | cloud DDL-rewrite path; dead while `cloud_mode=false` | inert |
| `ReadSettings.h` default flip + the two `filesystem_cache_enable_background_download_*` settings | **Verified:** ReadSettings fields written in `Context.cpp`, read nowhere in OSS | inert |
| `BackupEntriesCollector` `compare_collected_metadata` `!cloud_mode`→`true` | `!cloud_mode==true` in OSS already | inert (identical) |
| `RestorerFromBackup` Replicated→Shared rewrite | gated by `restore_replicated_merge_tree_to_shared_merge_tree` (default false) | inert |
| `CreateQueryUUIDs` SharedSet/SharedJoin inner-UUID | `SharedSet`/`SharedJoin` engines absent | inert |
| `ObjectStorageQueueMetadata` ordered-mode warn/throw | gated by `cloud_mode` | inert |
| `StorageObjectStorageSource` distributed-cache read path | `#if ENABLE_DISTRIBUTED_CACHE` off | inert |
| `XDBCDictionarySource` `cloud_mode` check removal | check never threw (`cloud_mode=false`) | inert |
| All `SettingsChangesHistory.cpp` edits | n/a (audit log) | **anti-pattern** — see Finding C |
| `executable_pool` source now throws unconditionally | XML `executable_pool` dicts work in OSS today; 057 kills them | **REAL change (B1)** |
| `allow_experimental_ts_to_grid_aggregate_function` alias removed (`DECLARE_WITH_ALIAS`→`DECLARE`) | user-facing alias deleted; unrelated to cloud | **REAL change (B2, scope creep)** |

### Finding C — regressions of a *verbatim* (delete-based) port

The danger is the **mechanism (delete) not the intent**. Because 057 deletes
rather than obsoletes:

1. **Build/link break** (Finding A) — guaranteed until the 3 extra `cloud_mode`
   refs are also handled (the fix itself is benign: those gates are dead in OSS).
2. **CI settings-history baseline** — editing `SettingsChangesHistory.cpp`
   (past-release entries) is very likely to fail the history-consistency check;
   no upside even in a faithful port.
3. **`UNKNOWN_SETTING` on session/query settings** — any `SET …` / `SETTINGS …`
   naming a removed setting now **errors** instead of being a harmless no-op
   (managed config, migration tooling, cloud-derived queries). Delete vs.
   `MAKE_OBSOLETE`.
4. **Table-attach failure (most severe, conditional)** — the ~40
   `shared_merge_tree_*` / cloud part-storage MergeTree settings are deleted
   **without** `MAKE_OBSOLETE_MERGE_TREE_SETTING`. A table whose stored metadata
   carries one (cloud→Aiven migration/restore origin, or explicit `SETTINGS`)
   hits `UNKNOWN_SETTING` on metadata parse at startup → **the table fails to
   attach** (data-availability regression). Plain OSS-origin MergeTree tables do
   not carry these, so it is conditional on the migration/restore path.
5. **`executable_pool` XML dicts disabled (B1)** — guaranteed for users of that
   feature; this is the one *intended* hardening, see §7.

### Conclusion (screen)

This is the weakest-value / highest-cost candidate in the set: the cloud settings
are inert no-ops in an OSS build, so deleting them changes nothing functional
while imposing permanent per-uplift conflicts on the four hottest files
(`Settings.cpp`, `ServerSettings.cpp`, `MergeTreeSettings.cpp`,
`SettingsChangesHistory.cpp`), a guaranteed link break, and the regressions
above. Recommended dispositions are in §7; the decision is **deferred (held
open)** pending the §7 question.

## 3. C++ review

`n/a — no code carried (held open).` Key observation: the only OSS-behavioral
change in the patch is `ExecutablePoolDictionarySource.cpp`, where the
`cloud_mode`-gated throw is replaced by an unconditional
`throw … SUPPORT_IS_DISABLED`. Note that DDL-created executable dictionaries are
**already** blocked upstream by the `created_from_ddl && getApplicationType() !=
LOCAL` guard; 057 additionally removes the **XML-config** path. If hard-off is
desired it should be a small, explicitly-gated change (an `aiven_` server
setting), not a hardcoded throw buried in a settings-deletion commit.

## 4. Test design

(b)/(c) **No new test — the patch is HELD OPEN, not ported.** If a minimal
hardening port (B1) is later chosen, it needs an evidence pair: an XML
`executable_pool` dictionary is usable with the guard off and rejected with it on
(integration test, since dictionary registration + config is involved).

## 5. Rollback / production handover considerations

- **If dropped:** no production action required for the inert bulk. The
  dangerous dict sources (`executable` / `executable_pool` / `file` / `library` /
  `odbc`) can already be disabled in the managed fleet by setting **`cloud_mode =
  1`** in server config — i.e. the hardening 057 hardcodes is reachable today via
  config, without touching code. This makes even B1 potentially a config decision.
- **If a minimal hardening port is chosen (B1):** ship it as a default-off
  `aiven_` server setting (own commit), not as a deletion patch.
- **Never** edit `SettingsChangesHistory.cpp`.
- Future-uplift watch: each uplift, re-run the `rg -l 'Setting::cloud_mode' src/`
  count — upstream keeps adding consumers, which is precisely why a faithful port
  rots.

## 6. Per-uplift notes

### 25.8-aiven (historical)

Original carry: `ac227df851` (author Tilman Moeller, co-authored by Joe Lynch,
Aliaksei Khatskevich, 2026-01-12). On the 25.8 base it removed the visible
cloud-only settings surface and hard-disabled `executable_pool`.

### 26.3-aiven (this uplift)

- Cherry-pick: NOT performed. Parent stopped at Step 1 (drift/validity) with
  conclusion: mostly cosmetic + inert; one real change (B1); guaranteed link
  break; delete-based regressions (§2 Finding C).
- Decision (human, 2026-06-17): **HELD OPEN.** Keep the patch un-dispatched and
  documented; revisit once the §7 question is answered.

## 7. Open decision (gates the disposition)

**Does Aiven require the dangerous dictionary sources (`executable`,
`executable_pool`, `file`, `library`, `odbc`) to be hard-off in production?**

- **If NO** → `patch-drop(057)`: the rest is inert, deleting it only adds uplift
  cost + regressions. (And if a softer guarantee is wanted, `cloud_mode=1` in
  managed config disables those sources today.)
- **If YES, and code-level is required** → drop the cosmetic bulk and carry
  **only** the hardening as a small default-off `aiven_` server setting in its own
  commit; obsolete (never delete) any settings; never touch
  `SettingsChangesHistory.cpp`. Discard the `allow_experimental_ts_to_grid_aggregate_function`
  alias removal (B2) regardless.

Until this is answered, 057 stays **held open**.

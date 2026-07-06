# Patch 056 — freeze-include-metadata-version

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | n/a | n/a | not in scope for this uplift's records (no earlier carry identified) |
| 25.8-aiven | `b18cff803a0e1509d38d98da65202cf459750202` | Tilman Moeller (author) / Joe Lynch (committer) | original carry |
| 26.3-aiven | `patch-port(056)` (staged) | T3.17 worker | conflict-resolved (whitespace-only); patch-id byte-equivalent |

The current uplift's row stays the stable handle `patch-port(056)` until the human commits.

## 1. Purpose

`ALTER TABLE … FREEZE` creates a hardlinked copy of each data part under the
server's `shadow/<name>/…` directory. Each part carries a small
`metadata_version.txt` file recording the table metadata version the part was
written against. Before this patch the local-disk freeze path dropped that file
from the frozen copy, so backups taken from the frozen directory were missing
it. Aiven needs the file preserved because, on object storage, `metadata_version.txt`
is a *pointer* to a remote object; using the frozen directory as the source of
"which objects are still referenced" then mis-classifies that object as garbage
and deletes it. The deletion is latent — it only surfaces loudly when a new
replica tries to download the file while syncing. The "why" is durable; the
exact line may move every uplift.

Source SHA on `v25.8.18.1-lts-aiven`: `b18cff803a0e1509d38d98da65202cf459750202`.
Original author: `tilman.moeller@aiven.io` (committed by `joelynch112@gmail.com`).
Original purpose (quoted, not paraphrased):

```
Include metadata_version.txt when freezing

The metadata_version.txt file was added to each part folder to help with
concurrency issues when mutating the table schema, but it was not included
in the frozen files when creating backups.

We need to include this file in the frozen shadow/ folder because we use
these files to know what to backup. When using object storage, this file is
actually a pointer to the file in object storage. We use these pointers to
know which files are still referenced by ClickHouse and should not be deleted.

By omitting this file, we would not know about it, and delete the
metadata_version.txt file in object storage. This was causing latent issues:
ClickHouse doesn't immediately notice the missing file, but instead complains
loudly when adding a new replica - the new replica tries to download this
file when syncing from existing replicas.

The fix sets keep_metadata_version = true in ClonePartParams when freezing
parts, ensuring the metadata_version.txt file is preserved in frozen backups.

Co-authored-by: Kevin Michel <kevin.michel@aiven.io>
```

## 2. Upstream-drift findings

### Commands run

```bash
git log v25.8.18.1-lts..v26.3.10.62-lts -- src/Storages/MergeTree/MergeTreeData.cpp
git log v25.8.18.1-lts..v26.3.10.62-lts -S 'keep_metadata_version = true' -- src/Storages/MergeTree/MergeTreeData.cpp
# identifier inventory on HEAD:
git grep -c keep_metadata_version make_source_readonly ClonePartParams freezePartitionsByMatcher -- src/
```

### Findings

- Upstream changes to touched files between prior and current LTS:
  - `src/Storages/MergeTree/MergeTreeData.cpp`: heavy churn (file grew ~565 lines;
    the freeze block moved from ~8836 to ~9401 and got nested one indentation
    level deeper). No change to the *semantics* of the freeze `ClonePartParams`
    construction — it still builds `{ .make_source_readonly = true }` and passes
    it to `data_part_storage->freeze(...)`.
- Upstream changes that touched the patch's behavior (symbols, error codes, callers):
  - `ClonePartParams::keep_metadata_version`: still a real field
    (`src/Storages/MergeTree/IDataPartStorage.h:254`, default `false`); already
    set `true` in the clone (`IMergeTreeDataPart.cpp:2454`) and mutate
    (`MutateTask.cpp:2752`) paths; the freeze path did NOT set it on HEAD → not
    obsoleted.
  - Drift trap: `chassert(!params.keep_metadata_version)` at
    `MergeTreeData.cpp:9141` is in the *clone* path and guarded by
    `if (params.metadata_version_to_write.has_value())`. The freeze path builds
    its own `params` and never sets `metadata_version_to_write`, so it does not
    reach that assertion. `keep_metadata_version` (copy existing) and
    `metadata_version_to_write` (write new) are mutually exclusive by design.
  - No upstream commit between LTSes introduced an equivalent change (`-S` search empty).
- Conclusion: **still-needed-and-applies** (semantics intact). The textual
  cherry-pick conflicted on **whitespace only** (HEAD nests the freeze block one
  indentation level deeper than 25.8), resolved by hand keeping HEAD's
  indentation; `git patch-id --stable` matches the source, so the result is
  semantically byte-equivalent.

## 3. C++ review

- 1 Lifetime + ownership: n/a — patch sets one `bool` field in a stack-local
  `ClonePartParams` aggregate; no allocations, no ownership transfer, no new lifetimes.
- 2 Exception safety: ✓ — a single aggregate-initializer field; introduces no
  new throwing operation; the freeze flow is otherwise unchanged.
- 3 Thread-safety + concurrency: ✓ — `params` is function-local; no shared/global
  state is read or mutated by the change.
- 4 Performance + memory: ✓ — preserves one extra hardlinked file (a few bytes)
  per frozen part and *skips* a `removeFileIfExists` call; net cost is negligible
  / slightly cheaper.
- 5 Settings as public API: n/a — no new setting. Behavior change is narrow:
  frozen backups now contain one additional file. Not a broad blast-radius
  default change (does not alter live-table behavior or query results), so no
  clause-(v) policy gate is required.
- 6 Error handling: n/a — no new error paths or codes.
- 7 Upstream / vendored code: ✓ — `src/` only; nothing under `contrib/`.
- 8 Behavior under settings: ✓ — independent of session/MergeTree settings;
  the metadata_version.txt is now always preserved by FREEZE on the local-disk path.

## 4. Test design

(a) **New test that fails on the parent commit and passes after the patch.**

- Test path: `tests/queries/0_stateless/9056_freeze_include_metadata_version.sh`
  (+ `.reference`), per the Aiven `9<NNN>_<slug>` convention
  (`docs/aiven/runbooks/testing-suites.md` §4.1). Format is `.sh` because the
  observable is a **file in the frozen output**, not a SQL row — it must be
  inspected on the local filesystem. The frozen part directory is discovered
  from the FREEZE verbose result's `part_backup_path` column (an absolute path),
  i.e. the test harness's own data-path discovery, not a hardcoded path. Plain
  local-disk `MergeTree`, no object storage.

- Pre-patch run output (the FAIL):

  ```text
  9056_freeze_include_metadata_version:                                   [ FAIL ] 0.63 sec.
  Reason: result differs with reference:
  @@ -1,2 +1,2 @@
   live_part_metadata_version: PRESENT
  -frozen_part_metadata_version: PRESENT
  +frozen_part_metadata_version: ABSENT
  ```

- Post-patch run output (the PASS):

  ```text
  9056_freeze_include_metadata_version:                                   [ OK ] 0.58 sec.
  1 tests passed. 0 tests skipped.
  ```

- Why this test distinguishes the patch's contribution: the `live_part_metadata_version: PRESENT`
  line is identical on both binaries, so the difference is *solely* in the frozen
  copy — pre-patch `DataPartStorageOnDiskBase::freeze` removes
  `metadata_version.txt` because `keep_metadata_version` defaults to `false`;
  post-patch it is kept. The stable first line guards against a vacuous pass/pass
  (e.g. if a future change stopped writing metadata_version.txt to live parts).

## 5. Rollback considerations

- Reverting is safe: the change only affects which files a *new* FREEZE writes
  into `shadow/`. No schema migration, no on-disk format change for live tables.
- State that survives restart: frozen `shadow/` directories are on-disk and
  persist (as before); the only difference is they now also contain
  `metadata_version.txt`. Pre-existing frozen backups are unaffected.
- No setting disables the new behavior; FREEZE always preserves the file after
  the patch. A revert (drop the one line) restores the old behavior without a rebuild-blocking migration.

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — no earlier carry identified within this uplift's records.

### 25.8-aiven (historical, may be empty)

Original carry: `b18cff803a0e1509d38d98da65202cf459750202` (author Tilman Moeller,
committer Joe Lynch), co-authored by Kevin Michel. One-line insertion into the
freeze path's `ClonePartParams` initializer.

### 26.3-aiven (this uplift)

- Cherry-pick was: **conflict-resolved** — `git cherry-pick --no-commit -x`
  produced a content conflict in `MergeTreeData.cpp` whose two sides differed
  ONLY in leading indentation (26.3 nests the freeze block one level deeper) plus
  the single added field line. Per `docs/aiven/schema/halt-and-escalate.md` the
  `textual_conflict` escalation explicitly excludes whitespace-only conflicts, so
  it was resolved by hand keeping HEAD's indentation and inserting
  `.keep_metadata_version = true,` above `.make_source_readonly = true`.
- Upstream-drift conclusion: still-needed-and-applies (semantics intact).
- Tier 2 (patch-id): `git patch-id --stable` of the staged diff MATCHES the
  source commit (`3d8926365444c527b71cd5607ecee994d8252604`) — patch-id
  normalizes leading whitespace, so the indentation difference does not affect
  the semantic identity. `byte_equivalent: true`. Whitespace-insensitive
  decomposition (`diff -w`) of the +/- content lines is empty (no semantic reshaping).
- Test added at: `tests/queries/0_stateless/9056_freeze_include_metadata_version.{sh,reference}`.
- Time-to-port: warm-cache build directory (sccache hot from prior session); the
  build dir needed a `cmake --fresh` recovery (missing `CMakeFiles/rules.ninja`,
  the documented breakage row 1) before the first build. Builds: full post-patch
  ~2m14s, pre-patch incremental ~63s, post-patch restore incremental ~25s.
- Anything surprising: (1) the parent preflight predicted byte-identical context
  / clean cherry-pick, but the freeze block had drifted by one indentation level
  on 26.3, producing a whitespace-only conflict (still byte-equivalent by
  patch-id). (2) `clickhouse-client -q "INSERT … VALUES (…)"` blocks waiting on
  stdin in this environment; the test uses `INSERT … SELECT 1` to avoid it.

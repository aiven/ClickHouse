---
description: 'ALTER TABLE ... FREEZE keeps each part''s metadata_version.txt, so the frozen copy is a complete part'
sidebarTitle: '056: FREEZE keeps metadata_version.txt'
slug: '/aiven/patches/056-freeze-include-metadata-version'
title: 'Patch 056: FREEZE keeps metadata_version.txt'
doc_type: 'reference'
aiven_identity: '056'
aiven_absorbs: []
aiven_changelog_category: 'Bug Fix'
aiven_settings: []
---

# Patch 056 — `FREEZE` keeps `metadata_version.txt` {#patch-056-freeze-include-metadata-version}

## Summary {#summary}

`ALTER TABLE ... FREEZE` hardlinks every part into `shadow/<name>/`, but upstream leaves out the
part's `metadata_version.txt`. The patch keeps it, so a frozen part has exactly the files of the
live part. The backup tool snapshots `shadow/` as the set of files that make up each part; on an
object storage disk every one of those files is a pointer to a remote object, and an object no
pointer refers to is treated as garbage.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| 25.8 | `b18cff803a0` | carried |
| 26.3 | `patch-port(056)` `fc1d0b7f8f2` | carried; whitespace-only conflict, same line |
| 26.8 | `patch-port(056)` | `still-needed-and-applies`, code unchanged; now also keeps packed parts hardlinked; upstream `test_partition` adjusted |

## Background (ClickHouse) {#background}

A part is a directory of immutable files, so `FREEZE` can hardlink them instead of copying.
`metadata_version.txt` holds the table metadata version the part was written under; in
`ReplicatedMergeTree` that is the `ALTER` counter, and ClickHouse reads it when it loads the
part.

Every clone of a part — `ATTACH ... FROM`, mutations, replicated fetches, `FREEZE` — goes through
`IDataPartStorage::ClonePartParams`. Its `keep_metadata_version` defaults to `false`, because a
clone attached to another table often needs a different version (`metadata_version_to_write`).
Freeze takes the default, so `DataPartStorageOnDiskBase::freeze` removes the file from the
frozen copy.

On an object storage disk the local part files are metadata files: each lists the remote objects
that hold the file's bytes. A hardlink duplicates the pointer, not the object.

## Component tour {#component-tour}

- **`MergeTreeData::freezePartitionsByMatcher`** (`Storages/MergeTree/MergeTreeData.cpp`). Builds
  the `ClonePartParams` for every frozen part; the patch adds `.keep_metadata_version = true`.
- **`DataPartStorageOnDiskBase::freeze` and `freezeRemote`** (`DataPartStorageOnDiskBase.cpp`).
  Remove `metadata_version.txt` from the frozen copy unless `keep_metadata_version` is set.
- **`DataPartStorageOnDiskPacked::freeze`** (`DataPartStorageOnDiskPacked.cpp`). With packed
  storage the file lives inside `data.packed`. `cloneCopiesWholeArchive` is true when
  `keep_metadata_version` is false, so upstream rewrites the archive without the file; with the
  flag set the archive is hardlinked.

## Problem {#problem}

Without the patch, the frozen set lacks `metadata_version.txt`. On an object storage disk its
remote object is then referenced only by the live part, and a cleanup that trusts the frozen set
deletes it. Nothing fails at once: ClickHouse notices only when the part is read again from
remote storage, for example when a new replica fetches it, and a part restored from such a backup
has lost its metadata version.

## Decisions {#decisions}

### One flag in the freeze path {#decision-flag}

- **Chosen:** set `keep_metadata_version` in the freeze parameters only.
- **Rejected:** changing the default in `ClonePartParams`. Other clone paths must be able to drop
  or rewrite the version.
- **Rejected:** having the cleanup also scan live parts (rung 1). A restore uses the frozen set
  alone, so the file has to be in it.
- **Not done:** an upstream contribution (rung 2). "`FREEZE` produces a complete part" is a
  generic fix; no pull request is filed.

### Adjust the upstream test {#decision-upstream-test}

- **Chosen:** `test_partition_complex` asserts the frozen file list without
  `metadata_version.txt`. It now uses the list the test already has with the file.
- **Rejected:** keeping upstream's expectation. It asserts exactly the behaviour the patch
  changes.

## Approach {#approach}

```cpp
IDataPartStorage::ClonePartParams params
{
    .keep_metadata_version = true,
    .make_source_readonly = true
};
```

Freeze never sets `metadata_version_to_write`, so the assertion that forbids both fields together
is not reached.

## Concept {#concept}

ClickHouse never loads a frozen part itself, so for upstream `shadow/` is a by-product and a
dropped file costs nothing. An external tool that reads `shadow/` as the complete part turns the
same default into lost data. When something outside ClickHouse depends on its on-disk output, the
contents of that output are an interface and have to be checked on every uplift.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-and-applies`

- The freeze parameters are unchanged since 26.3 and the commit cherry-picks cleanly. Upstream sets
  `keep_metadata_version` for clone, mutate and replicated fetch, still not for freeze.
- New in 26.8, packed part storage (off by default; enabled per table with
  `min_bytes_for_full_part_storage` and related settings). Upstream `FREEZE` of a packed part
  rewrites `data.packed` without the file, a full copy of the part's data; with the patch the
  archive is hardlinked.
- New in 26.8, small files stored inline in object storage metadata. It applies only to metadata
  storages that support it, not to the local metadata of an ordinary object storage disk, so
  `metadata_version.txt` there is still a pointer to a remote object.
- The format of the local metadata files is unchanged.

## Customer impact {#customer-impact}

- **Who:** every service; the frozen copy is what backups are taken from.
- **What they see:** nothing directly. Frozen parts include `metadata_version.txt`, and frozen
  packed parts are hardlinks instead of copies.
- **New versus 26.3:** packed parts, which 26.3 does not have.
- **Upstream tests edited:** `tests/integration/test_partition/test.py`,
  `test_partition_complex`: the two checksum assertions after `FREEZE` expect
  `metadata_version.txt` in each frozen part.

## Security review {#security-review}

n/a — the patch keeps one existing file in a copy that already holds every other file of the
part; it adds no input, path or privilege.

## Tests {#tests}

- **Paths:** `tests/queries/0_stateless/aiven_056_freeze_keeps_metadata_version.sh`,
  `tests/integration/test_partition/`
- **Proves:** for a part with full storage, the frozen part has `metadata_version.txt`,
  hardlinked to the live file; for a part with packed storage, the frozen `data.packed` is a
  hardlink of the live archive. The live part's storage type and file are checked too, so the
  test cannot pass vacuously.
- **Run:** `tests/clickhouse-test aiven_056`;
  `python -m ci.praktika run "integration" --test test_partition`
- **Evidence:** on the parent the three frozen-part checks fail (file absent, not a hardlink,
  archive copied); with the patch the test passes, and passes 10 runs with randomized settings.
  `test_partition_complex` fails with the patch and the original expectation and passes with the
  adjusted one; the whole `test_partition` module passes (2026-11-24).

## Rollback {#rollback}

- **Downgrade:** not supported; services only move forward.
- **Persistent state:** none; frozen copies are removed by `UNFREEZE`.
- **Disable without rebuild:** none.

## Retirement {#retirement}

Upstream sets `keep_metadata_version` in the freeze path, or freezes through a path that keeps
the file. Re-check on every uplift; no setting to declare `OBSOLETE`.

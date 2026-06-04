"""Integration test for the Aiven `backup` disk type (patch 026).

The `backup` disk wraps an object-storage disk and turns object deletion into a
*soft delete*: instead of (only) removing the underlying object it writes a local
deletion-marker file per object under
`<backup_base_path>/<escapeForFileName(remote_path)>`. An external Aiven GC later
decides what is safe to physically remove.

We exercise the storage-agnostic soft-delete bookkeeping our single-location
(tiered-storage) deployment relies on, using a local-filesystem object storage
(`object_storage_type=local`) so the test needs no MinIO/S3 and the markers are
directly inspectable inside the node container.

Shape: a single node, no ZooKeeper, with an inline SQL custom disk
(`disk(type=backup, disk=disk(type=object_storage, object_storage_type=local, ...), ...)`).
The inline form lets the server start on both the pre- and post-patch binary; the
divergence is captured at `CREATE TABLE` time:

  * post-patch: the `backup` disk type exists, the table is created, and removing
    its objects (here via `DROP TABLE ... SYNC`) makes `BackupObjectStorage` write
    deletion-marker files under the backup base path, one per removed object key.
  * pre-patch: `disk(type=backup, ...)` is an unknown disk type, so `CREATE TABLE`
    throws `unknown disk type: backup` and the test fails.

The load-bearing invariant (Fix A): the soft-deleted blobs must PHYSICALLY SURVIVE
the drop. On 26.3 blob deletion is deferred through a single in-memory removal
queue owned by `metadata_storage`, drained by a per-disk background
`BlobKillerThread`. Because the backup layer reuses the same `metadata_storage`,
the backup disk and the wrapped/inner disk share that one queue. `wrapWithBackup`
must make the backup disk's killer the SOLE drainer: it unchains the inner killer
(`detachWrapped`) and disables the inner disk's own killer (`disable`). Otherwise
the inner killer routes to the RAW object storage and physically unlinks the blob,
defeating the marker and the external-GC contract.

This test therefore asserts BOTH:

  * the backup disk records per-object deletion markers (soft-delete bookkeeping), and
  * the marked (soft-deleted) blobs still physically exist under the wrapped
    object storage after the drop and after the background killer has run.

The second assertion FAILs without Fix A (inner killer physically deletes the
blob) and PASSes with it — see the dossier (§4) for the evidence pair.
"""

import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node")

# Both directories live inside the node container (under the server data dir),
# so they are inspectable via exec_in_container.
OBJ_DIR = "/var/lib/clickhouse/aiven_backup_obj"
MARKER_DIR = "/var/lib/clickhouse/aiven_backup_markers"

# The extended removal-path tests (merge / mutation / TTL) each get their OWN
# object-storage + marker directory pair so they cannot interfere with each
# other or with the DROP test above (the blob/marker files would otherwise mix
# in a shared directory and make the per-test survival sets ambiguous).
OBJ_DIR_MERGE = "/var/lib/clickhouse/aiven_backup_obj_merge"
MARKER_DIR_MERGE = "/var/lib/clickhouse/aiven_backup_markers_merge"
OBJ_DIR_MUT = "/var/lib/clickhouse/aiven_backup_obj_mut"
MARKER_DIR_MUT = "/var/lib/clickhouse/aiven_backup_markers_mut"
OBJ_DIR_TTL = "/var/lib/clickhouse/aiven_backup_obj_ttl"
MARKER_DIR_TTL = "/var/lib/clickhouse/aiven_backup_markers_ttl"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _list_files(path):
    out = node.exec_in_container(
        ["bash", "-c", f"find {path} -type f 2>/dev/null | sort"]
    ).strip()
    return set(line for line in out.splitlines() if line)


def _unescape_for_file_name(name):
    """Mirror of DB::unescapeForFileName: turn %HH back into the raw byte."""
    res = []
    i = 0
    while i < len(name):
        if name[i] == "%" and i + 2 < len(name):
            res.append(chr(int(name[i + 1 : i + 3], 16)))
            i += 3
        else:
            res.append(name[i])
            i += 1
    return "".join(res)


def _marked_objects():
    """Object paths that currently have a deletion marker under MARKER_DIR."""
    return {_unescape_for_file_name(m.rsplit("/", 1)[-1]) for m in _list_files(MARKER_DIR)}


def test_backup_disk_soft_delete(start_cluster):
    node.query(
        f"""
        CREATE TABLE backup_tbl (a UInt64)
        ENGINE = MergeTree
        ORDER BY a
        SETTINGS disk = disk(
            type = backup,
            path = '{MARKER_DIR}/',
            disk = disk(
                type = object_storage,
                object_storage_type = local,
                path = '{OBJ_DIR}/'))
        """
    )

    node.query("INSERT INTO backup_tbl SELECT number FROM numbers(1000)")

    # The part's blobs are now stored under the wrapped local object storage.
    obj_files = _list_files(OBJ_DIR)
    assert obj_files, "expected object-storage blobs after insert"

    # Dropping the table removes the part; the backup layer turns each object
    # removal into a deletion-marker write. With wait_for_blob_removal (default on)
    # this drains synchronously via the backup disk's BlobKillerThread, but we poll
    # below (test-side, no server-side sleeps) so the assertions stay robust to any
    # async scheduling.
    node.query("DROP TABLE backup_tbl SYNC")

    # Drain: wait until the backup disk's killer has recorded deletion markers for
    # the dropped objects, i.e. it has processed the shared removal queue.
    marked_under_obj = []
    for _ in range(30):
        marked_under_obj = sorted(
            p for p in _marked_objects() if p.startswith(OBJ_DIR + "/")
        )
        if marked_under_obj:
            break
        time.sleep(1)

    # (1) The backup disk recorded deletion markers naming real object keys that
    # live under the wrapped object storage.
    assert (
        marked_under_obj
    ), f"expected deletion-marker files referencing objects under {OBJ_DIR}"

    # (2) The soft-deleted blobs MUST physically survive the drop. A backup disk
    # never unlinks the blob; it only records a marker and lets the external GC
    # decide later. The shared removal queue must be drained ONLY by the backup
    # disk's killer (Fix A unchains + disables the inner disk's killer). Without
    # Fix A the inner killer routes to the RAW object storage and physically
    # unlinks the blob, defeating the marker. Give a (mis)behaving inner killer
    # several scheduling rounds to act before asserting, so survival is not a
    # mere timing artifact.
    surviving = set()
    for _ in range(15):
        surviving = _list_files(OBJ_DIR)
        if not set(marked_under_obj).issubset(surviving):
            break
        time.sleep(1)

    missing = sorted(set(marked_under_obj) - surviving)
    assert not missing, (
        "backup-wrapped object-storage blobs were physically deleted despite the "
        "soft-delete markers (the inner BlobKillerThread defeated the backup "
        f"layer); missing blobs: {missing}"
    )


# ---------------------------------------------------------------------------
# Extended coverage: the other normal ClickHouse removal paths.
#
# The DROP test above proves physical survival for the DROP path. By
# construction *every* normal blob removal on a `DiskObjectStorage` funnels
# through `DiskObjectStorageTransaction` -> the single shared `metadata_storage`
# removal queue -> the per-disk `BlobKillerThread` (`DiskObjectStorageTransaction
# ::waitBlobRemoval` loops `blob_killer->triggerAndWait()`; the same queue is
# drained by the background killer for async removals). Fix A makes the backup
# disk's killer the SOLE drainer of that queue, so the survival guarantee should
# hold for ALL removal paths, not just DROP. The three tests below prove it for
# merge (OPTIMIZE FINAL), mutation (ALTER ... DELETE) and TTL-delete.
#
# Determinism: outdated/source/old parts are normally removed only after
# `old_parts_lifetime` (default 480s). Each table sets `old_parts_lifetime = 1`
# and a tight `cleanup_delay_period` so the background cleanup removes the
# orphaned parts within a few seconds; the tests then POLL `system.parts` until
# the orphaned parts are gone (the blobs are enqueued + drained at that point),
# POLL until deletion markers are recorded for the captured object keys (proves
# the killer actually processed the queue, so a survival pass is real and not a
# timing artifact), and only then assert physical survival on the filesystem.
# No server-side sleeps are used; all waits are test-side polling.
# ---------------------------------------------------------------------------

# Per-table MergeTree settings that force prompt removal of orphaned parts, so
# the removal path is exercised in seconds rather than after `old_parts_lifetime`
# (~480s). Kept per-table (not global config) to avoid perturbing the DROP test.
_PROMPT_CLEANUP = (
    "old_parts_lifetime = 1, "
    "cleanup_delay_period = 1, "
    "cleanup_delay_period_random_add = 1"
)


def _backup_disk_clause(marker_dir, obj_dir):
    return (
        "disk = disk("
        "type = backup, "
        f"path = '{marker_dir}/', "
        "disk = disk("
        "type = object_storage, object_storage_type = local, "
        f"path = '{obj_dir}/'))"
    )


def _active_part_names(table):
    out = node.query(
        "SELECT name FROM system.parts "
        f"WHERE database = currentDatabase() AND table = '{table}' AND active"
    ).strip()
    return set(line for line in out.splitlines() if line)


def _capture_source_part_blobs(table, obj_dir):
    """Object keys (remote_path) backing the currently-active SOURCE parts of
    `table`, captured from `system.remote_data_paths`.

    For `object_storage_type = local` the `remote_path` is the absolute on-node
    path of the blob, so it is directly comparable to a filesystem listing of
    `obj_dir`.

    Scoping is per-part (the blob's `local_path` metadata path must sit under one
    of the active part directories). This deliberately EXCLUDES persistent
    table-level blobs such as `format_version.txt`, which live directly under the
    table's `store/<uuid>/` directory and are never removed by merge / mutation /
    TTL — so they would never get a deletion marker and must not be part of the
    survival/marker sets. (`system.remote_data_paths` has no `table` column, so we
    scope by part name embedded in `local_path` — the same source parts the
    dispatch spec refers to.)
    """
    part_names = _active_part_names(table)
    assert part_names, f"no active source parts for {table}"

    out = node.query(
        "SELECT local_path, remote_path FROM system.remote_data_paths "
        f"WHERE remote_path LIKE '{obj_dir}/%' FORMAT TSV"
    ).strip()

    blobs = set()
    for line in out.splitlines():
        if not line:
            continue
        local_path, remote_path = line.split("\t", 1)
        if any(f"/{part}/" in local_path for part in part_names):
            blobs.add(remote_path)
    return blobs


def _marked_objects_under(marker_dir):
    """Object paths that currently have a deletion marker under `marker_dir`."""
    return {
        _unescape_for_file_name(m.rsplit("/", 1)[-1])
        for m in _list_files(marker_dir)
    }


def _wait_outdated_parts_gone(table, attempts=120):
    """Poll until `table` has no inactive (outdated) parts left on disk.

    Once the orphaned parts disappear from `system.parts` their directories have
    been removed via `DiskObjectStorageTransaction`, i.e. the blobs have been
    enqueued into (and, with the default synchronous `wait_for_blob_removal`,
    drained from) the shared removal queue.
    """
    for _ in range(attempts):
        inactive = node.query(
            "SELECT count() FROM system.parts "
            f"WHERE database = currentDatabase() AND table = '{table}' AND active = 0"
        ).strip()
        if inactive == "0":
            return
        time.sleep(1)
    remaining = node.query(
        "SELECT name FROM system.parts "
        f"WHERE database = currentDatabase() AND table = '{table}' AND active = 0"
    ).strip()
    raise AssertionError(
        f"outdated parts of {table} were not cleaned up in time; remaining: {remaining}"
    )


def _wait_mutation_done(table, attempts=120):
    for _ in range(attempts):
        pending = node.query(
            "SELECT count() FROM system.mutations "
            f"WHERE database = currentDatabase() AND table = '{table}' AND is_done = 0"
        ).strip()
        if pending == "0":
            return
        time.sleep(1)
    raise AssertionError(f"mutation on {table} did not finish in time")


def _referenced_remote_paths(obj_dir):
    """Object keys under `obj_dir` still referenced by some part's metadata."""
    out = node.query(
        "SELECT remote_path FROM system.remote_data_paths "
        f"WHERE remote_path LIKE '{obj_dir}/%'"
    ).strip()
    return set(line for line in out.splitlines() if line)


def _assert_blobs_survive(source_blobs, obj_dir, marker_dir, path_label):
    """Shared survival assertion for the merge/mutation/TTL removal paths.

    `source_blobs` is the set of object keys that backed the source parts before
    the operation. The operation orphans those parts; the subset of `source_blobs`
    that is no longer referenced by any active part afterwards is exactly what
    went through the removal path (`removed`). A blob still referenced after the
    operation (e.g. a column blob hardlinked into the rewritten part by a
    mutation) survives trivially via metadata refcount and is NOT evidence for
    the backup guarantee, so it is excluded from both the marker gate and the
    survival assertion.

    We require `removed` to be non-empty (the path really removed blobs), then
    wait until the backup killer has recorded deletion markers for every blob in
    `removed` (proving the shared removal queue was actually drained for them —
    without this gate a survival pass could be a mere "the killer never ran"
    artifact), then assert those blobs PHYSICALLY survive (markers, not unlink).
    """
    assert source_blobs, (
        f"no source object keys captured under {obj_dir} for the {path_label} path"
    )

    # Blobs that were truly orphaned (no longer referenced) by the operation.
    referenced_after = _referenced_remote_paths(obj_dir)
    removed = source_blobs - referenced_after
    assert removed, (
        f"the {path_label} operation orphaned no source blobs (still referenced: "
        f"{sorted(source_blobs & referenced_after)}); the removal path was not exercised"
    )

    # Gate: deletion markers recorded for every removed source key. Pre-fix the
    # inner killer ALSO runs (both killers drain the shared queue), so markers
    # appear pre-fix too — only the physical-survival assertion below flips,
    # which pins the failure to the inner killer's raw unlink.
    marked = set()
    for _ in range(60):
        marked = _marked_objects_under(marker_dir)
        if removed.issubset(marked):
            break
        time.sleep(1)
    unmarked = sorted(removed - marked)
    assert not unmarked, (
        f"backup disk did not record deletion markers for the {path_label} "
        f"removed blobs (killer never drained the queue?); unmarked: {unmarked}"
    )

    # Give a (mis)behaving inner killer several scheduling rounds to physically
    # unlink before asserting survival, so survival is not a timing artifact.
    surviving = set()
    for _ in range(15):
        surviving = _list_files(obj_dir)
        if not removed.issubset(surviving):
            break
        time.sleep(1)

    missing = sorted(removed - surviving)
    assert not missing, (
        f"backup-wrapped object-storage blobs orphaned by the {path_label} path "
        "were physically deleted despite the soft-delete markers (the inner "
        f"BlobKillerThread defeated the backup layer); missing blobs: {missing}"
    )


def test_backup_disk_merge_soft_delete(start_cluster):
    """Merge path: OPTIMIZE FINAL merges parts; the outdated SOURCE parts' blobs
    must physically survive (markers), not be unlinked."""
    node.query("DROP TABLE IF EXISTS backup_merge_tbl SYNC")
    node.query(
        f"""
        CREATE TABLE backup_merge_tbl (a UInt64)
        ENGINE = MergeTree
        ORDER BY a
        SETTINGS {_backup_disk_clause(MARKER_DIR_MERGE, OBJ_DIR_MERGE)}, {_PROMPT_CLEANUP}
        """
    )

    # Hold merges so the two INSERTs land as two distinct source parts that we
    # can capture before OPTIMIZE merges (and orphans) them.
    node.query("SYSTEM STOP MERGES backup_merge_tbl")
    node.query("INSERT INTO backup_merge_tbl SELECT number FROM numbers(1000)")
    node.query("INSERT INTO backup_merge_tbl SELECT number FROM numbers(1000, 1000)")

    assert (
        node.query(
            "SELECT count() FROM system.parts "
            "WHERE database = currentDatabase() AND table = 'backup_merge_tbl' AND active"
        ).strip()
        == "2"
    ), "expected two active source parts before the merge"

    # Capture the SOURCE parts' object keys (these are the blobs the merge orphans).
    source_blobs = _capture_source_part_blobs("backup_merge_tbl", OBJ_DIR_MERGE)

    node.query("SYSTEM START MERGES backup_merge_tbl")
    # optimize_throw_if_noop guards against a silent no-op (which would orphan
    # nothing and make the survival check vacuous).
    node.query(
        "OPTIMIZE TABLE backup_merge_tbl FINAL",
        settings={"optimize_throw_if_noop": 1},
    )

    _wait_outdated_parts_gone("backup_merge_tbl")
    _assert_blobs_survive(source_blobs, OBJ_DIR_MERGE, MARKER_DIR_MERGE, "merge")


def test_backup_disk_mutation_soft_delete(start_cluster):
    """Mutation path: ALTER ... DELETE rewrites the affected part and orphans the
    old one; the old part's blobs must physically survive (markers)."""
    node.query("DROP TABLE IF EXISTS backup_mut_tbl SYNC")
    node.query(
        f"""
        CREATE TABLE backup_mut_tbl (a UInt64)
        ENGINE = MergeTree
        ORDER BY a
        SETTINGS {_backup_disk_clause(MARKER_DIR_MUT, OBJ_DIR_MUT)}, {_PROMPT_CLEANUP}
        """
    )

    node.query("INSERT INTO backup_mut_tbl SELECT number FROM numbers(2000)")

    # Capture the OLD part's object keys before the mutation rewrites it.
    source_blobs = _capture_source_part_blobs("backup_mut_tbl", OBJ_DIR_MUT)

    # mutations_sync = 2 blocks until the mutation has fully materialized the new
    # part (and thus orphaned the old one).
    node.query(
        "ALTER TABLE backup_mut_tbl DELETE WHERE a < 1000",
        settings={"mutations_sync": 2},
    )

    _wait_mutation_done("backup_mut_tbl")
    _wait_outdated_parts_gone("backup_mut_tbl")
    _assert_blobs_survive(source_blobs, OBJ_DIR_MUT, MARKER_DIR_MUT, "mutation")


def test_backup_disk_ttl_soft_delete(start_cluster):
    """TTL-delete path: a row TTL expires part of the data; the TTL merge
    rewrites the part dropping the expired rows and orphans the source part,
    whose blobs must physically survive (markers).

    A single part holds both expired (`d` in the past) and live (`d` in the
    future) rows. `OPTIMIZE FINAL` materializes the row TTL during the merge,
    dropping the expired rows into a fresh, NON-empty part and orphaning the
    source part. Keeping the rewrite non-empty avoids the empty-TTL-part cleanup
    edge case (an all-expired part rewrites to an empty part whose removal is
    governed by separate, slower bookkeeping)."""
    node.query("DROP TABLE IF EXISTS backup_ttl_tbl SYNC")
    node.query(
        f"""
        CREATE TABLE backup_ttl_tbl (a UInt64, d DateTime)
        ENGINE = MergeTree
        ORDER BY a
        TTL d + INTERVAL 1 SECOND DELETE
        SETTINGS {_backup_disk_clause(MARKER_DIR_TTL, OBJ_DIR_TTL)}, {_PROMPT_CLEANUP}
        """
    )

    # Hold merges so the background TTL merge cannot fire before we capture the
    # source part. One INSERT => one part with mixed expired / live rows.
    node.query("SYSTEM STOP MERGES backup_ttl_tbl")
    node.query(
        "INSERT INTO backup_ttl_tbl "
        "SELECT number, if(number < 1000, now() - INTERVAL 1 DAY, now() + INTERVAL 1 DAY) "
        "FROM numbers(2000)"
    )

    assert (
        node.query(
            "SELECT count() FROM system.parts "
            "WHERE database = currentDatabase() AND table = 'backup_ttl_tbl' AND active"
        ).strip()
        == "1"
    ), "expected one active source part before the TTL merge"

    # Capture the SOURCE part's object keys (the TTL merge orphans these).
    source_blobs = _capture_source_part_blobs("backup_ttl_tbl", OBJ_DIR_TTL)

    node.query("SYSTEM START MERGES backup_ttl_tbl")
    # OPTIMIZE FINAL materializes the row TTL, dropping the expired rows into a
    # new part and orphaning the source part. optimize_throw_if_noop guards
    # against a silent no-op (which would orphan nothing).
    node.query(
        "OPTIMIZE TABLE backup_ttl_tbl FINAL",
        settings={"optimize_throw_if_noop": 1},
    )

    # Sanity: the TTL merge actually dropped the expired rows.
    assert (
        node.query("SELECT count() FROM backup_ttl_tbl").strip() == "1000"
    ), "expected the TTL merge to drop the 1000 expired rows"

    _wait_outdated_parts_gone("backup_ttl_tbl")
    _assert_blobs_survive(source_blobs, OBJ_DIR_TTL, MARKER_DIR_TTL, "TTL")

"""Integration test for the production tiered-storage stack shape with soft delete:
cache -> object_storage(soft_delete).

WHY A SEPARATE TEST (sibling to `test_aiven_soft_delete`):
`test_aiven_soft_delete` builds a bare `object_storage` disk with `soft_delete = 1`.
The real tiered stack additionally has a CACHE layer on top, and a cache disk owns
its OWN `metadata_storage` (a second removal queue) and its OWN `BlobKillerThread`.

This is the shape that used to be unsound: when soft delete was applied by wrapping
an already-built disk, the killer of the disk one level deeper still drained its own
queue through the RAW object storage and physically unlinked blobs that the layer
above had only soft-deleted.

Because `soft_delete` now wraps the object storage at CONSTRUCTION time, the base
object storage IS the `SoftDeleteObjectStorage`, and the cache layer wraps that
(`CachedObjectStorage(SoftDeleteObjectStorage(LocalObjectStorage))`). Every killer in
the stack therefore routes deletions through the soft-delete layer, and the failure
mode is structurally impossible rather than suppressed. This test pins that.

`object_storage_type = local` stands in for S3: the physical delete would go through
the identical `removeObjectsIfExist` path regardless of backend, and local lets us
inspect the "bucket" as plain files with no MinIO/S3.
"""

import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node")

# One object-storage + marker + cache directory triple per removal path so the
# per-test survival/marker sets never mix.
#
# The cache `path` MUST be ABSOLUTE and writable. The integration base config
# (helpers/0_common_instance_config.xml) forces
# `custom_cached_disks_base_directory = /`, and that key takes PRECEDENCE over
# `filesystem_caches_path` for SQL-inline (custom) cache disks
# (src/Disks/DiskObjectStorage/RegisterDiskCache.cpp getCache). So the cache path
# prefix is `/`: a relative path would resolve to filesystem root (unwritable) and
# a relative-path config would be ignored. Any absolute path under the writable
# data dir `/var/lib/clickhouse/` satisfies the `pathStartsWith(path, "/")` guard.
OBJ_DROP = "/var/lib/clickhouse/bc_obj_drop"
MARKER_DROP = "/var/lib/clickhouse/bc_markers_drop"
CACHE_DROP = "/var/lib/clickhouse/bc_cache_drop"

OBJ_MERGE = "/var/lib/clickhouse/bc_obj_merge"
MARKER_MERGE = "/var/lib/clickhouse/bc_markers_merge"
CACHE_MERGE = "/var/lib/clickhouse/bc_cache_merge"

OBJ_MUT = "/var/lib/clickhouse/bc_obj_mut"
MARKER_MUT = "/var/lib/clickhouse/bc_markers_mut"
CACHE_MUT = "/var/lib/clickhouse/bc_cache_mut"

OBJ_TTL = "/var/lib/clickhouse/bc_obj_ttl"
MARKER_TTL = "/var/lib/clickhouse/bc_markers_ttl"
CACHE_TTL = "/var/lib/clickhouse/bc_cache_ttl"

# 1 GiB cache: comfortably larger than the tiny test datasets, so cache eviction
# never interferes. (Cache eviction only removes LOCAL cache files, never the base
# blobs we assert survive, but a generous size keeps the test unambiguous.)
CACHE_MAX_SIZE = 1073741824

# Force prompt removal of orphaned parts so the removal path is exercised in
# seconds rather than after `old_parts_lifetime` (~480s). Per-table, not global.
_PROMPT_CLEANUP = (
    "old_parts_lifetime = 1, "
    "cleanup_delay_period = 1, "
    "cleanup_delay_period_random_add = 1"
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _backup_cache_disk_clause(marker_dir, cache_path, obj_dir):
    """Production stack shape: cache -> object_storage(local) with soft delete.

    Inline nested `disk(...)` flattens post-order, so the base object_storage disk is
    created first (already wrapped in `SoftDeleteObjectStorage`), then the cache disk
    wraps it.
    """
    return (
        "disk = disk("
        "type = cache, "
        f"path = '{cache_path}', "
        f"max_size = {CACHE_MAX_SIZE}, "
        "disk = disk("
        "type = object_storage, object_storage_type = local, "
        f"path = '{obj_dir}/', "
        "soft_delete = 1, "
        f"soft_delete_markers_path = '{marker_dir}/'))"
    )


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


def _marked_objects_under(marker_dir):
    """Object paths that currently have a deletion marker under `marker_dir`."""
    return {
        _unescape_for_file_name(m.rsplit("/", 1)[-1])
        for m in _list_files(marker_dir)
    }


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
    path of the blob, directly comparable to a filesystem listing of `obj_dir`.
    Scoping is per-part (the blob's `local_path` must sit under an active part
    directory), which excludes persistent table-level blobs (e.g.
    `format_version.txt`) that no removal path touches.
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


def _referenced_remote_paths(obj_dir):
    """Object keys under `obj_dir` still referenced by some part's metadata."""
    out = node.query(
        "SELECT remote_path FROM system.remote_data_paths "
        f"WHERE remote_path LIKE '{obj_dir}/%'"
    ).strip()
    return set(line for line in out.splitlines() if line)


def _wait_outdated_parts_gone(table, attempts=120):
    """Poll until `table` has no inactive (outdated) parts left on disk.

    Once the orphaned parts disappear from `system.parts`, their blobs have been
    enqueued into (and, with default synchronous `wait_for_blob_removal`, drained
    from) the removal queues.
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


def _assert_blobs_survive(removed, obj_dir, marker_dir, path_label):
    """Shared survival assertion.

    `removed` is the set of base object keys the operation orphaned (no longer
    referenced by any active part). We (1) require it non-empty (the path really
    removed blobs), (2) gate on the backup disk having recorded deletion markers
    for every removed key (proves a killer drained the shared queue, so a survival
    pass is real evidence and not a "killer never ran" artifact), then (3) assert
    those blobs PHYSICALLY survive under the base object storage.

    On the current binary the base object-storage disk's killer physically unlinks
    the blobs (finding §3.3), so (3) FAILs while (1)/(2) pass.
    """
    assert removed, (
        f"the {path_label} path orphaned no base blobs; the removal path was not exercised"
    )

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

    # Give every killer in the stack several scheduling rounds to unlink before
    # asserting survival, so survival is not merely a timing artifact.
    surviving = set()
    for _ in range(15):
        surviving = _list_files(obj_dir)
        if not removed.issubset(surviving):
            break
        time.sleep(1)

    missing = sorted(removed - surviving)
    assert not missing, (
        f"object-storage blobs orphaned by the {path_label} path were physically "
        "deleted despite the soft-delete markers (a BlobKillerThread in the stack "
        f"bypassed the soft-delete layer); missing blobs: {missing}"
    )


def test_backup_cache_disk_drop_soft_delete(start_cluster):
    """DROP path: dropping the table orphans every blob at once (the accidental
    DROP recovery scenario). The base blobs must physically survive (markers)."""
    node.query("DROP TABLE IF EXISTS bc_drop_tbl SYNC")
    node.query(
        f"""
        CREATE TABLE bc_drop_tbl (a UInt64)
        ENGINE = MergeTree
        ORDER BY a
        SETTINGS {_backup_cache_disk_clause(MARKER_DROP, CACHE_DROP, OBJ_DROP)}
        """
    )
    node.query("INSERT INTO bc_drop_tbl SELECT number FROM numbers(1000)")

    # All blobs backing the table are orphaned by the drop.
    removed = _referenced_remote_paths(OBJ_DROP)
    assert removed, "expected object-storage blobs after insert"

    node.query("DROP TABLE bc_drop_tbl SYNC")

    _assert_blobs_survive(removed, OBJ_DROP, MARKER_DROP, "drop")


def test_backup_cache_disk_merge_soft_delete(start_cluster):
    """Merge path: OPTIMIZE FINAL merges two parts; the outdated SOURCE parts'
    blobs must physically survive (markers)."""
    node.query("DROP TABLE IF EXISTS bc_merge_tbl SYNC")
    node.query(
        f"""
        CREATE TABLE bc_merge_tbl (a UInt64)
        ENGINE = MergeTree
        ORDER BY a
        SETTINGS {_backup_cache_disk_clause(MARKER_MERGE, CACHE_MERGE, OBJ_MERGE)}, {_PROMPT_CLEANUP}
        """
    )

    node.query("SYSTEM STOP MERGES bc_merge_tbl")
    node.query("INSERT INTO bc_merge_tbl SELECT number FROM numbers(1000)")
    node.query("INSERT INTO bc_merge_tbl SELECT number FROM numbers(1000, 1000)")

    assert (
        node.query(
            "SELECT count() FROM system.parts "
            "WHERE database = currentDatabase() AND table = 'bc_merge_tbl' AND active"
        ).strip()
        == "2"
    ), "expected two active source parts before the merge"

    source_blobs = _capture_source_part_blobs("bc_merge_tbl", OBJ_MERGE)

    node.query("SYSTEM START MERGES bc_merge_tbl")
    node.query(
        "OPTIMIZE TABLE bc_merge_tbl FINAL",
        settings={"optimize_throw_if_noop": 1},
    )

    _wait_outdated_parts_gone("bc_merge_tbl")
    removed = source_blobs - _referenced_remote_paths(OBJ_MERGE)
    _assert_blobs_survive(removed, OBJ_MERGE, MARKER_MERGE, "merge")


def test_backup_cache_disk_mutation_soft_delete(start_cluster):
    """Mutation path: ALTER ... DELETE rewrites the affected part and orphans the
    old one; the old part's blobs must physically survive (markers)."""
    node.query("DROP TABLE IF EXISTS bc_mut_tbl SYNC")
    node.query(
        f"""
        CREATE TABLE bc_mut_tbl (a UInt64)
        ENGINE = MergeTree
        ORDER BY a
        SETTINGS {_backup_cache_disk_clause(MARKER_MUT, CACHE_MUT, OBJ_MUT)}, {_PROMPT_CLEANUP}
        """
    )
    node.query("INSERT INTO bc_mut_tbl SELECT number FROM numbers(2000)")

    source_blobs = _capture_source_part_blobs("bc_mut_tbl", OBJ_MUT)

    node.query(
        "ALTER TABLE bc_mut_tbl DELETE WHERE a < 1000",
        settings={"mutations_sync": 2},
    )

    _wait_mutation_done("bc_mut_tbl")
    _wait_outdated_parts_gone("bc_mut_tbl")
    removed = source_blobs - _referenced_remote_paths(OBJ_MUT)
    _assert_blobs_survive(removed, OBJ_MUT, MARKER_MUT, "mutation")


def test_backup_cache_disk_ttl_soft_delete(start_cluster):
    """TTL-delete path: a row TTL expires part of the data; the TTL merge rewrites
    the part dropping the expired rows and orphans the source part, whose blobs
    must physically survive (markers)."""
    node.query("DROP TABLE IF EXISTS bc_ttl_tbl SYNC")
    node.query(
        f"""
        CREATE TABLE bc_ttl_tbl (a UInt64, d DateTime)
        ENGINE = MergeTree
        ORDER BY a
        TTL d + INTERVAL 1 SECOND DELETE
        SETTINGS {_backup_cache_disk_clause(MARKER_TTL, CACHE_TTL, OBJ_TTL)}, {_PROMPT_CLEANUP}
        """
    )

    node.query("SYSTEM STOP MERGES bc_ttl_tbl")
    node.query(
        "INSERT INTO bc_ttl_tbl "
        "SELECT number, if(number < 1000, now() - INTERVAL 1 DAY, now() + INTERVAL 1 DAY) "
        "FROM numbers(2000)"
    )

    assert (
        node.query(
            "SELECT count() FROM system.parts "
            "WHERE database = currentDatabase() AND table = 'bc_ttl_tbl' AND active"
        ).strip()
        == "1"
    ), "expected one active source part before the TTL merge"

    source_blobs = _capture_source_part_blobs("bc_ttl_tbl", OBJ_TTL)

    node.query("SYSTEM START MERGES bc_ttl_tbl")
    node.query(
        "OPTIMIZE TABLE bc_ttl_tbl FINAL",
        settings={"optimize_throw_if_noop": 1},
    )

    assert (
        node.query("SELECT count() FROM bc_ttl_tbl").strip() == "1000"
    ), "expected the TTL merge to drop the 1000 expired rows"

    _wait_outdated_parts_gone("bc_ttl_tbl")
    removed = source_blobs - _referenced_remote_paths(OBJ_TTL)
    _assert_blobs_survive(removed, OBJ_TTL, MARKER_TTL, "TTL")

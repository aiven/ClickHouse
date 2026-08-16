"""Regression test for the STICKY-disable half of the tiered-storage blob-deletion fix
(the `force_disabled` flag in `BlobKillerThread`).

Companion to `test_aiven_backup_disk_cache_layer` (which covers the transitive-disable half).
That sibling uses an inline SQL `disk(type=backup, disk=disk(type=cache, ...))`; the wrapped
layers of an inline disk are internal (`__tmp_internal_...`) and are NOT re-processed by
`SYSTEM RELOAD CONFIG`, so an inline disk cannot exercise the reload path.

This test therefore uses a CONFIG-DEFINED stack (see config.d/reload_storage.xml):
backup(`bc_backup`) -> cache(`bc_cache`) -> object_storage(`bc_base`, local). On reload,
`DiskSelector::updateFromConfig` reuses each disk by name and calls
`DiskObjectStorage::applyNewSettings` IN PLACE (it does NOT reconstruct the disk, i.e.
`wrapWithBackup` does not re-run). `applyNewSettings` re-reads `data_background_cleanup.enabled`
(default true), so without the fix it re-enables the base/cache `BlobKillerThread`s that
`wrapWithBackup` disabled at construction — and the next removal physically unlinks the blobs
the backup layer only soft-deleted.

`object_storage_type = local` stands in for S3 (identical `removeObjectsIfExist` path), so the
"bucket" is inspectable as plain files.

EXPECTED RESULT
  * Without the sticky-disable fix: after `SYSTEM RELOAD CONFIG` the base killer is live again,
    so the DROP's soft-deleted base blobs are physically unlinked -> the survival assertion FAILs.
  * With the fix (`disable()` sets `force_disabled`, honored by `applyNewSettings`): the killers
    stay disabled across the reload -> the blobs survive -> PASS.

The complementary "queue is never populated for a suppressed disk" invariant (which keeps memory
bounded) is asserted directly and deterministically by the `gtest_metadata_local_disk` unit tests
`TestSetRecordRemovalsSuppressesEnqueue` / `TestRecordRemovalsEnqueuesByDefault`.
"""

import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["config.d/reload_storage.xml"],
    stay_alive=True,
)

# Must match config.d/reload_storage.xml. For `object_storage_type = local` the blob
# `remote_path` is the absolute on-node path under OBJ_DIR, so it is directly comparable to a
# filesystem listing; the backup layer writes one deletion marker per removed key under MARKER_DIR.
OBJ_DIR = "/var/lib/clickhouse/bc_reload_obj"
MARKER_DIR = "/var/lib/clickhouse/bc_reload_markers"

# In-container path of the config file shipped via main_configs (copied into config.d).
RELOAD_CONFIG_PATH = "/etc/clickhouse-server/config.d/reload_storage.xml"


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


def _marked_objects_under(marker_dir):
    """Object paths that currently have a deletion marker under `marker_dir`."""
    return {
        _unescape_for_file_name(m.rsplit("/", 1)[-1])
        for m in _list_files(marker_dir)
    }


def _referenced_remote_paths(obj_dir):
    """Object keys under `obj_dir` still referenced by some part's metadata."""
    out = node.query(
        "SELECT remote_path FROM system.remote_data_paths "
        f"WHERE remote_path LIKE '{obj_dir}/%'"
    ).strip()
    return set(line for line in out.splitlines() if line)


def _assert_blobs_survive(removed, obj_dir, marker_dir, path_label):
    """Require `removed` non-empty, gate on the backup disk recording deletion markers for every
    removed key (proves a killer drained the shared queue, so a survival pass is real), then
    assert those blobs PHYSICALLY survive under the base object storage."""
    assert removed, f"the {path_label} path orphaned no base blobs; nothing was exercised"

    marked = set()
    for _ in range(60):
        marked = _marked_objects_under(marker_dir)
        if removed.issubset(marked):
            break
        time.sleep(1)
    unmarked = sorted(removed - marked)
    assert not unmarked, (
        f"backup disk did not record deletion markers for the {path_label} removed blobs "
        f"(killer never drained the queue?); unmarked: {unmarked}"
    )

    # Give a (mis)behaving base killer several scheduling rounds to unlink before asserting.
    surviving = set()
    for _ in range(15):
        surviving = _list_files(obj_dir)
        if not removed.issubset(surviving):
            break
        time.sleep(1)

    missing = sorted(removed - surviving)
    assert not missing, (
        f"backup-wrapped object-storage blobs orphaned by the {path_label} path were physically "
        "deleted despite the soft-delete markers: SYSTEM RELOAD CONFIG re-enabled a wrapped-disk "
        f"BlobKillerThread that disable() should have kept sticky; missing blobs: {missing}"
    )


def test_backup_cache_disk_survives_config_reload(start_cluster):
    node.query("DROP TABLE IF EXISTS bc_reload_tbl SYNC")
    node.query(
        """
        CREATE TABLE bc_reload_tbl (a UInt64)
        ENGINE = MergeTree
        ORDER BY a
        SETTINGS storage_policy = 'bc_reload'
        """
    )
    node.query("INSERT INTO bc_reload_tbl SELECT number FROM numbers(1000)")

    # All blobs backing the table; the drop below orphans them.
    removed = _referenced_remote_paths(OBJ_DIR)
    assert removed, "expected object-storage blobs after insert"

    # Force a config reload that re-applies disk settings IN PLACE. Bumping the cache max_size is a
    # benign delta that guarantees DiskSelector re-processes the stack (and thus calls
    # applyNewSettings on bc_base / bc_cache / bc_backup); without the sticky-disable fix this
    # re-enables the base/cache killers that wrapWithBackup disabled.
    node.replace_in_config(RELOAD_CONFIG_PATH, "1073741824", "2147483648")
    node.query("SYSTEM RELOAD CONFIG")

    # Remove the blobs. Only the backup disk's killer may drain (soft-delete via markers); the
    # base/cache killers must remain disabled across the reload.
    node.query("DROP TABLE bc_reload_tbl SYNC")

    _assert_blobs_survive(removed, OBJ_DIR, MARKER_DIR, "drop-after-reload")

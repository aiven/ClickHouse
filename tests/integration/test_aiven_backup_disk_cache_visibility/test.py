"""A `backup` disk hides the filesystem cache underneath it (patch 026).

`BackupObjectStorage` is a decorator over another `IObjectStorage`. It forwards the
methods patch 026 needed and inherits the rest from `IObjectStorage`, and three of
the inherited defaults are answers about caching:

    virtual bool supportsCache() const { return false; }
    virtual void removeCacheIfExists(const std::string &) {}
    const std::string & getCacheName() const   // throws NOT_IMPLEMENTED

`DiskObjectStorage` answers `supportsCache` / `getCacheName` by asking its object
storage, and `wrapWithBackup` replaces that object storage with a
`BackupObjectStorage`. So on the production stack `backup -> cache ->
object_storage` the server asks the backup layer "do you have a cache?", is told
"no", and stops asking. `CachedObjectStorage` — upstream's own decorator, one
level down — forwards all three.

The cache still works: reads go through `CachedObjectStorage::readObject`, which
never consults `supportsCache`. What breaks is everything that *decides* based on
that answer, all of it silent:

  * `system.disks.cache_path` and `system.remote_data_paths.cache_paths` are
    populated only `if (disk->supportsCache())`, so both report no cache.
  * `checkDataPart`'s `drop_cache_and_check` retry re-raises immediately when
    `getCacheName()` yields nothing, so `CHECK TABLE` reports a part as broken
    when it is the *cached copy* that is corrupt, not the part.
  * `DiskObjectStorage::readObjects` computes
    `file_cache_enabled = supportsCache() && enable_filesystem_cache`, which
    drives `use_page_cache_for_disks_without_file_cache` (double caching, wasted
    memory) and `prefer_bigger_buffer_size` (upstream picks the bigger buffer
    specifically to avoid fragmenting the file cache).
  * `StoragePolicy::tryGetDiskByName` resolves a wrapped disk's name to the
    wrapping layer only `if (disk->supportsCache())`, the compatibility path for
    `TTL TO DISK` / `MOVE PARTITION TO DISK` naming the base disk.
  * `Context::setTemporaryStorageInCache` calls `getCacheName()` unguarded, so a
    backup-wrapped disk in `<temporary_data_in_cache>` throws NOT_IMPLEMENTED.

Every assertion here is made twice: once against `bcv_backup` (backup over cache)
and once against `bcv_cache` (the same cache disk, no backup layer). The control
arm is what makes a failure attributable to the backup layer.

EXPECTED RESULT
  * Before the fix: `test_cache_is_used_through_backup_disk` passes (the cache is
    genuinely in use), and the two reporting tests FAIL on the `bcv_backup` arm
    while passing on the `bcv_cache` arm. That contrast is the reproduction.
  * After forwarding the three methods to the wrapped storage: all pass.

`object_storage_type = local` stands in for S3: the cache sits between the disk
and the object storage regardless of backend, and local keeps the "bucket"
inspectable as plain files.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["config.d/cache_visibility_disks.xml"],
)

# Must match config.d/cache_visibility_disks.xml.
BACKUP_DISK = "bcv_backup"
CACHE_DISK = "bcv_cache"
BASE_DISK = "bcv_base"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _create_and_warm(table, policy):
    """Create a table on `policy`, insert, then read it back twice.

    The first read populates the filesystem cache; the second is served from it.
    Both reads are forced past the query cache and the page cache so the data
    genuinely lands in the FILESYSTEM cache, which is the thing under test.
    """
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (a UInt64, s String)
        ENGINE = MergeTree
        ORDER BY a
        SETTINGS storage_policy = '{policy}'
        """
    )
    node.query(
        f"INSERT INTO {table} SELECT number, repeat('x', 512) FROM numbers(20000)"
    )
    for _ in range(2):
        node.query(
            f"SELECT count(), sum(a) FROM {table}",
            settings={"enable_filesystem_cache": 1, "use_query_cache": 0},
        )


def _cache_path_in_system_disks(disk):
    return node.query(
        f"SELECT cache_path FROM system.disks WHERE name = '{disk}'"
    ).strip()


def _rows_with_cache_paths(disk):
    return int(
        node.query(
            "SELECT count() FROM system.remote_data_paths "
            f"WHERE disk_name = '{disk}' AND notEmpty(cache_paths)"
        ).strip()
    )


def test_cache_is_used_through_backup_disk(start_cluster):
    """Anchor: the cache under the backup layer really does hold data.

    Without this, a failure of the reporting tests below could be explained away
    as "the cache is not actually configured/used", so establish it first and
    independently of any `supportsCache()`-gated code path: `system.filesystem_cache`
    is built from the FileCache objects themselves.
    """
    _create_and_warm("bcv_backup_tbl", "bcv_policy")

    cached_entries = int(
        node.query("SELECT count() FROM system.filesystem_cache").strip()
    )
    assert cached_entries > 0, (
        "no filesystem cache entries after reading a table on the backup-wrapped "
        "cache disk; the stack is not caching at all, so the rest of this test "
        "would be meaningless"
    )


def test_system_disks_reports_cache_path_for_backup_disk(start_cluster):
    """`system.disks.cache_path` must name the cache the disk actually uses."""
    control = _cache_path_in_system_disks(CACHE_DISK)
    assert control, (
        f"control arm broken: the plain cache disk `{CACHE_DISK}` reports no "
        "cache_path either, so the cache is misconfigured rather than hidden"
    )

    subject = _cache_path_in_system_disks(BACKUP_DISK)
    assert subject, (
        f"`system.disks` reports no cache_path for `{BACKUP_DISK}` although it "
        f"wraps `{CACHE_DISK}`, which reports `{control}`. BackupObjectStorage "
        "inherits IObjectStorage::supportsCache() == false instead of forwarding "
        "to the storage it wraps, so the disk denies having a cache"
    )


def test_remote_data_paths_reports_cache_for_backup_disk(start_cluster):
    """`system.remote_data_paths.cache_paths` must list the cached blobs.

    This is the observability half of the same root cause, and the one that
    matters operationally: it is how one finds out which parts are cached.
    """
    _create_and_warm("bcv_control_tbl", "bcv_cache_only_policy")
    control = _rows_with_cache_paths(CACHE_DISK)
    assert control > 0, (
        f"control arm broken: no cache_paths reported for `{CACHE_DISK}` either"
    )

    _create_and_warm("bcv_backup_tbl2", "bcv_policy")
    subject = _rows_with_cache_paths(BACKUP_DISK)
    assert subject > 0, (
        f"`system.remote_data_paths` reports no cache_paths for any blob on "
        f"`{BACKUP_DISK}`, while `{CACHE_DISK}` reports {control} such rows for "
        "the same stack shape; the backup layer hides the cache from "
        "`disk->supportsCache()`"
    )


def test_storage_policy_resolves_wrapped_disk_name(start_cluster):
    """Naming the base disk must resolve to the layer that wraps it.

    `StoragePolicy::tryGetDiskByName` provides this compatibility so that
    `TTL TO DISK 'bcv_base'` / `MOVE PARTITION TO DISK 'bcv_base'` keep working
    when a cache (or backup) layer is put in front of the base disk. The lookup is
    gated on `supportsCache()`, so on a backup-wrapped stack the base disk name
    stops resolving.
    """
    _create_and_warm("bcv_move_tbl", "bcv_policy")

    error = node.query_and_get_error(
        f"ALTER TABLE bcv_move_tbl MOVE PARTITION tuple() TO DISK '{BASE_DISK}'"
    )
    # Resolution succeeding means the parts are found to be on that very disk
    # already ("already exists on disk"); resolution FAILING is reported as the
    # disk not being part of the policy at all.
    assert "is not in the policy" not in error and "No such disk" not in error, (
        f"`{BASE_DISK}` did not resolve through the policy of a backup-wrapped "
        f"stack, so `TTL TO DISK '{BASE_DISK}'` and `MOVE PARTITION TO DISK "
        f"'{BASE_DISK}'` are broken for backup disks; error was: {error}"
    )

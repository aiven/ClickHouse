"""
Aiven patch 046 — integration test for:

  "Add early fetch pool"

ported from v25.8.18.1-lts-aiven (source SHA 7e08e44e51324f1dde3df165ee59c9b9bc90ae88),
augmented for the 26.3 uplift with the `aiven_` settings-naming convention
(see docs/aiven/patches/046-early-fetch-pool.md):

  * MergeTreeSetting  use_early_fetch_pool          -> aiven_use_early_fetch_pool
                      (Bool, default true, old name kept as a DECLARE_WITH_ALIAS alias)
  * ServerSetting     background_early_fetches_pool_size
                                                    -> aiven_background_early_fetches_pool_size
                      (UInt64, default 8, plain rename)

What the patch does
-------------------
`StorageReplicatedMergeTree::scheduleDataProcessingJob` routes a GET_PART/ATTACH_PART
job to a SEPARATE "early" background pool when the queue entry has an empty
`source_replica` (i.e. the large initial-sync fetches a fresh replica issues to
download pre-existing data) AND `aiven_use_early_fetch_pool` is on. Otherwise the
job goes to the normal fetch pool (ongoing insert-driven replication). The early
pool is bounded by the server setting `aiven_background_early_fetches_pool_size`.
This keeps the big early parts from monopolizing the fetch pool and starving the
many small insert-driven GET_PART tasks.

Why integration (not stateless): the behavior is genuinely cluster-level. It only
manifests when a fresh replica performs an initial sync of pre-existing data over
ZooKeeper (empty-`source_replica` GET_PART entries) — unreachable from the single
shared stateless server.

Differential / evidence-of-causation (timing-free observable)
-------------------------------------------------------------
The differential is the per-table MergeTreeSetting `aiven_use_early_fetch_pool`,
observed through `system.replication_queue.postpone_reason`, which records the
`disable_reason` produced by `canExecuteFetch`. The two reasons are textually
distinct per pool:

  * early  pool saturation -> "... because N early fetches already executing, max M."
  * normal pool saturation -> "... because N fetches already executing, max M."

Setup (one syncing replica, two tables differing ONLY in the routing setting):

  * `t_early`  SETTINGS aiven_use_early_fetch_pool = 1  -> initial-sync fetches use
               the EARLY pool, which is configured tiny (size 1) on the syncing
               node via `aiven_background_early_fetches_pool_size`.
  * `t_normal` SETTINGS aiven_use_early_fetch_pool = 0  -> the SAME initial-sync
               fetches fall back to the NORMAL pool (default size 16).

Both tables are first filled with many small parts on `node_source` (with merges
stopped so the parts stay separate), then a fresh replica is attached on
`node_replica`. The fresh replica enqueues one empty-`source_replica` GET_PART per
pre-existing part. Because all of them are ready at once but the early pool has a
single slot, the surplus `t_early` entries are immediately postponed with the
Aiven-specific reason. The reason persists on each queued entry until it is
executed, so a bounded poll reliably observes it (this is NOT a wall-clock race:
the postpone is produced deterministically at scheduling time by the 1-slot pool,
and we only need to observe it before the serial drain finishes).

Assertions (the causation pair is the two-table differential):
  * "post" (t_early,  routing ON):  some queued entry has postpone_reason matching
            "early fetches already executing" — proves the early pool (and thus
            `aiven_background_early_fetches_pool_size`) gates these fetches.
  * "pre"  (t_normal, routing OFF): NO queued entry ever shows "early ..." — the
            same initial-sync fetches went to the normal pool. (A faithful no-op
            port that ignored the setting could not produce this difference.)
  * Corroboration: system.metrics.BackgroundEarlyFetchesPoolSize == 1 on the
    syncing node (the new metric reflects the new server setting).
  * Sanity: both tables eventually converge to the full row count, so the early
    pool bounds concurrency without dropping data.
"""

import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# The "source" replica: created first, holds the pre-existing data the fresh
# replica will download. Default early-pool size (irrelevant: it never syncs).
node_source = cluster.add_instance(
    "node_source",
    with_zookeeper=True,
    stay_alive=True,
    macros={"replica": "src"},
    keeper_randomize_feature_flags=False,
)

# The "syncing" replica: attached AFTER the data exists, so its GET_PART entries
# have empty source_replica (= early fetches). Its early pool is tiny (size 1) so
# the early route is observably gated.
node_replica = cluster.add_instance(
    "node_replica",
    main_configs=["configs/early_fetch_pool.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"replica": "rep"},
    keeper_randomize_feature_flags=False,
)

# Many small parts so the single early-pool slot is contended (surplus entries
# postpone immediately) and the serial drain leaves a comfortable observation
# window. > normal pool size (16) is not required for the early table.
N_PARTS = 40


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _zk_path(name):
    return f"/clickhouse/tables/aiven046/{name}"


def _create_source_table(name, use_early_fetch_pool):
    node_source.query(f"DROP TABLE IF EXISTS {name} SYNC")
    node_source.query(
        f"CREATE TABLE {name} (k UInt64, v String) "
        f"ENGINE = ReplicatedMergeTree('{_zk_path(name)}', '{{replica}}') "
        f"ORDER BY k SETTINGS aiven_use_early_fetch_pool = {use_early_fetch_pool}"
    )
    # Keep the parts separate so the fresh replica must fetch N_PARTS of them.
    node_source.query(f"SYSTEM STOP MERGES {name}")
    for i in range(N_PARTS):
        node_source.query(f"INSERT INTO {name} VALUES ({i}, 'early-fetch-pool-payload-{i}')")
    assert int(node_source.query(f"SELECT count() FROM system.parts WHERE active AND table = '{name}'").strip()) >= N_PARTS


def _attach_fresh_replica(name, use_early_fetch_pool):
    # No merges here either, so the replica's queue stays full of GET_PART entries.
    node_replica.query(f"DROP TABLE IF EXISTS {name} SYNC")
    node_replica.query(
        f"CREATE TABLE {name} (k UInt64, v String) "
        f"ENGINE = ReplicatedMergeTree('{_zk_path(name)}', '{{replica}}') "
        f"ORDER BY k SETTINGS aiven_use_early_fetch_pool = {use_early_fetch_pool}"
    )


def _seen_postpone_reason_substr(node, table, substr, attempts=120, sleep_s=0.5):
    """Bounded poll: return the first postpone_reason on `table` containing `substr`."""
    last = ""
    for _ in range(attempts):
        rows = node.query(
            "SELECT DISTINCT postpone_reason FROM system.replication_queue "
            f"WHERE database = currentDatabase() AND table = '{table}' "
            "AND postpone_reason != ''"
        ).strip()
        if rows:
            last = rows
            for line in rows.splitlines():
                if substr in line:
                    return line
        # Stop early if the table already fully drained without ever showing it.
        synced = int(
            node.query(
                f"SELECT count() FROM {table}"
            ).strip()
        )
        if synced >= N_PARTS and not rows:
            break
        time.sleep(sleep_s)
    return "" if substr not in last else last


def _wait_synced(node, table, attempts=120, sleep_s=0.5):
    for _ in range(attempts):
        n = int(node.query(f"SELECT count() FROM {table}").strip())
        if n >= N_PARTS:
            return n
        time.sleep(sleep_s)
    return int(node.query(f"SELECT count() FROM {table}").strip())


def test_early_fetch_pool_routing_gated_by_setting(start_cluster):
    assert node_source.query("SELECT 1").strip() == "1"
    assert node_replica.query("SELECT 1").strip() == "1"

    # The new metric must reflect the new server setting on the syncing node.
    early_pool_size = int(
        node_replica.query(
            "SELECT value FROM system.metrics WHERE metric = 'BackgroundEarlyFetchesPoolSize'"
        ).strip()
    )
    assert early_pool_size == 1, (
        f"expected BackgroundEarlyFetchesPoolSize == 1 (from "
        f"aiven_background_early_fetches_pool_size), got {early_pool_size}"
    )

    # Build the pre-existing data on the source replica BEFORE the fresh replica
    # exists, so the fresh replica's GET_PART entries have empty source_replica.
    _create_source_table("t_early", use_early_fetch_pool=1)
    _create_source_table("t_normal", use_early_fetch_pool=0)

    # Attach the fresh replica => initial sync => early/normal routing per setting.
    _attach_fresh_replica("t_early", use_early_fetch_pool=1)
    _attach_fresh_replica("t_normal", use_early_fetch_pool=0)

    # "post": routing ON => the early (size-1) pool is saturated => surplus entries
    # postpone with the Aiven-specific early reason.
    early_reason = _seen_postpone_reason_substr(
        node_replica, "t_early", "early fetches already executing"
    )
    print("t_early postpone_reason:", early_reason)
    assert early_reason, (
        "routing ON: expected some t_early queue entry to be postponed with "
        "'early fetches already executing' (proves early-pool routing gated by "
        "aiven_background_early_fetches_pool_size)"
    )

    # "pre"/control: routing OFF => the SAME initial-sync fetches use the normal
    # pool, so the early reason must NEVER appear for t_normal. Give the sync time
    # to run, then assert the early reason was never recorded.
    normal_early_reason = _seen_postpone_reason_substr(
        node_replica, "t_normal", "early fetches already executing", attempts=20
    )
    assert not normal_early_reason, (
        "routing OFF: t_normal fetches must use the NORMAL pool, but an "
        f"'early fetches already executing' reason was observed: {normal_early_reason}"
    )

    # Sanity: the early pool bounds concurrency but does not drop data — both
    # tables converge to the full row count.
    n_early = _wait_synced(node_replica, "t_early")
    n_normal = _wait_synced(node_replica, "t_normal")
    print(f"synced rows: t_early={n_early} t_normal={n_normal}")
    assert n_early == N_PARTS, f"t_early did not fully sync: {n_early}/{N_PARTS}"
    assert n_normal == N_PARTS, f"t_normal did not fully sync: {n_normal}/{N_PARTS}"

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

Differential / evidence-of-causation (observable)
--------------------------------------------------
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

Both tables are first filled with many parts on `node_source` (with merges stopped
so the parts stay separate), then a fresh replica is attached on `node_replica`.
The fresh replica enqueues one empty-`source_replica` GET_PART per pre-existing
part. Because all of them are ready at once but the early pool has a single slot,
the surplus `t_early` entries are postponed with the Aiven-specific reason.

Keeping the observation deterministic (the fix over the original port)
---------------------------------------------------------------------
The postpone_reason is TRANSIENT: it is cleared the instant the queue entry
executes. The original port filled the tables with tiny parts and relied on the
serial drain being slower than the poll — but on a fast/idle runner 40 trivially
small early fetches drain through the single slot in well under one poll interval,
so the observation window collapses and the assertion flakes (seen on Buildkite
build 39 shard 5: `assert '' ...`).

To make the window deterministic we HOLD the single early-pool slot open long
enough to be observed, by throttling t_early's fetches with the per-table
MergeTreeSetting `max_replicated_fetches_network_bandwidth` and giving each part a
real (high-entropy, poorly-compressible) payload. The in-flight early fetch then
occupies the 1-slot pool for ~1s, so the surplus entries stay postponed with the
"early ..." reason across a multi-second window — reliably caught by the poll.

Crucially this uses the PER-TABLE throttler, not the server-level one, so
`canExecuteFetch`'s `replicated_fetches_throttler->isThrottling()` gate (checked
AFTER the pool-saturation branch) stays false: the surplus entries still postpone
with the pool-saturation reason we assert on, not a throttle reason. t_normal is
left un-throttled and routes to the 16-slot normal pool, so it never shows the
early reason — the control side of the differential is unchanged.

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

# Enough parts that the single early-pool slot is contended (surplus entries
# postpone) with a comfortable, throttle-widened observation window. Kept modest
# because each part now carries a real payload (see PAYLOAD_BYTES) that is fetched
# under a bandwidth cap, so the drain time is ~N_PARTS * (PAYLOAD_BYTES / EARLY_FETCH_BW).
N_PARTS = 12

# Per-part payload. High-entropy (randomPrintableASCII) so MergeTree compression
# does not shrink it away — the fetched bytes must be real for the bandwidth cap
# to translate into wall-clock time that holds the early-pool slot.
PAYLOAD_BYTES = 1048576  # 1 MiB

# Per-table fetch bandwidth cap applied to t_early ONLY (bytes/sec). With a 1 MiB
# payload this makes each early fetch take ~1s, holding the single early slot open
# long enough that the surplus entries stay postponed across a multi-second window.
# This is the PER-TABLE throttler, so the server-level isThrottling() gate in
# canExecuteFetch stays false and the pool-saturation reason still wins.
EARLY_FETCH_BW = 1048576  # 1 MiB/s


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _zk_path(name):
    return f"/clickhouse/tables/aiven046/{name}"


def _table_settings(use_early_fetch_pool, fetch_bw):
    settings = [f"aiven_use_early_fetch_pool = {use_early_fetch_pool}"]
    # Throttle only the early table's fetches (per-table throttler) so the single
    # early-pool slot is held open long enough to observe the postpone reason.
    if fetch_bw:
        settings.append(f"max_replicated_fetches_network_bandwidth = {fetch_bw}")
    return ", ".join(settings)


def _create_source_table(name, use_early_fetch_pool, fetch_bw=0):
    node_source.query(f"DROP TABLE IF EXISTS {name} SYNC")
    node_source.query(
        f"CREATE TABLE {name} (k UInt64, v String) "
        f"ENGINE = ReplicatedMergeTree('{_zk_path(name)}', '{{replica}}') "
        f"ORDER BY k SETTINGS {_table_settings(use_early_fetch_pool, fetch_bw)}"
    )
    # Keep the parts separate so the fresh replica must fetch N_PARTS of them.
    node_source.query(f"SYSTEM STOP MERGES {name}")
    for i in range(N_PARTS):
        # One row per INSERT => one part; a real high-entropy payload so the
        # bandwidth cap on the fetching side translates into wall-clock time.
        node_source.query(
            f"INSERT INTO {name} SELECT {i}, randomPrintableASCII({PAYLOAD_BYTES})"
        )
    assert int(node_source.query(f"SELECT count() FROM system.parts WHERE active AND table = '{name}'").strip()) >= N_PARTS


def _attach_fresh_replica(name, use_early_fetch_pool, fetch_bw=0):
    # No merges here either, so the replica's queue stays full of GET_PART entries.
    node_replica.query(f"DROP TABLE IF EXISTS {name} SYNC")
    node_replica.query(
        f"CREATE TABLE {name} (k UInt64, v String) "
        f"ENGINE = ReplicatedMergeTree('{_zk_path(name)}', '{{replica}}') "
        f"ORDER BY k SETTINGS {_table_settings(use_early_fetch_pool, fetch_bw)}"
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
    # t_early is throttled so its early fetches hold the 1-slot pool observably;
    # t_normal is un-throttled and routes to the 16-slot normal pool.
    _create_source_table("t_early", use_early_fetch_pool=1, fetch_bw=EARLY_FETCH_BW)
    _create_source_table("t_normal", use_early_fetch_pool=0)

    # Attach the fresh replica => initial sync => early/normal routing per setting.
    _attach_fresh_replica("t_early", use_early_fetch_pool=1, fetch_bw=EARLY_FETCH_BW)
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

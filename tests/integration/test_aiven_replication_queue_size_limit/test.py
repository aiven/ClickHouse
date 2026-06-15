"""
Aiven patch 008 — integration test for:

  "Fix unbounded replication queue growth"

ported from v25.8.18.1-lts-aiven (source SHA d6e78ab9938674342224e2e58408fadccaae9ee7),
augmented for the 26.3 uplift with a default-off server master guard
`aiven_enable_replication_queue_size_limit` (clause (v) blast-radius resolution;
see docs/aiven/proposals/2026-06-15-aiven-settings-naming-convention-and-queue-size-guard.md
and docs/aiven/patches/008-replication-queue-size-limit.md).

Why integration (not stateless): the limiter is genuinely cluster-level. A
per-table background thread (ReplicatedMergeTreeQueueSizeThread) polls every
replica's ZooKeeper `queue/` childcount once per second and publishes a
per-storage maximum; delayInsertOrThrowIfNeeded then gates inserts on that
maximum. Reproducing it needs a real multi-replica ReplicatedMergeTree over
Keeper with one replica's queue deliberately stalled — unreachable from the
single shared stateless server.

Differential axis = the new SERVER setting (not the per-table thresholds, which
are identical on both nodes). Both nodes are replicas of the SAME table with the
SAME small thresholds; the ONLY difference is the server config:

  * node_on  : <aiven_enable_replication_queue_size_limit>true</...>  -> "post"
               (feature reachable: monitor runs, inserts are gated)
  * node_off : setting absent -> default false                       -> "pre"
               (feature unreachable: upstream behavior, no monitor, no gating)

Mechanism of inflation (deterministic, bounded wait — no bare sleep as the sole
sync):
  1. `SYSTEM STOP FETCHES` on node_on makes node_on the STALLED replica: it
     enqueues a GET_PART entry per remote part but can never execute it, so its
     ZK `queue/` grows monotonically.
  2. node_off (guard OFF, so its own inserts are NEVER gated) inserts many parts.
     Each insert appends one log entry that node_on copies into its queue.
  3. We wait (bounded retry) until node_on's monitor has polled and published a
     queue maximum above the per-table `queue_size_to_throw_insert` (= 5), using
     the `ReplicatedQueuesTotalSize` metric as the synchronization signal.

Evidence-of-causation pair (the two-node config differential IS the pre/post
pair, per the dispatch's PC-6: the guard-OFF node reproduces the pre-feature
behavior, the guard-ON node the post-feature behavior):
  * "pre"  (node_off, guard OFF): the identical INSERT SUCCEEDS, and
           ReplicatedQueuesTotalSize stays 0 (the monitor never started).
  * "post" (node_on,  guard ON):  the INSERT is REJECTED with error code
           LIMIT_EXCEEDED and the Aiven-specific message substring
           "Too large replication queue" (asserting BOTH, per AGENTS.md §7 —
           a bare shared error code is insufficient).
"""

import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# "post" node: the server master guard is ON.
node_on = cluster.add_instance(
    "node_on",
    main_configs=["configs/enable_queue_limit.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"replica": "r_on"},
    keeper_randomize_feature_flags=False,
)

# "pre" / control node: the server master guard is absent => default false.
node_off = cluster.add_instance(
    "node_off",
    with_zookeeper=True,
    stay_alive=True,
    macros={"replica": "r_off"},
    keeper_randomize_feature_flags=False,
)

ZK_PATH = "/clickhouse/tables/aiven008/t"

# Deliberately tiny thresholds so a handful of stalled queue entries crosses the
# throw threshold. These are per-table MergeTreeSettings, replicated => identical
# on both nodes. The differential is purely the server guard, not the thresholds.
TABLE_SETTINGS = "queue_size_to_throw_insert = 5, queue_size_to_delay_insert = 1"

# Comfortably above the throw threshold (5): each INSERT makes one part => one
# GET_PART log entry => one entry in node_on's stalled queue.
N_INFLATE_INSERTS = 12


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _create_replica(node):
    node.query("DROP TABLE IF EXISTS t SYNC")
    node.query(
        "CREATE TABLE t (k UInt64, v String) "
        f"ENGINE = ReplicatedMergeTree('{ZK_PATH}', '{{replica}}') "
        "ORDER BY k "
        f"SETTINGS {TABLE_SETTINGS}"
    )


def _queues_total_size(node):
    return int(
        node.query(
            "SELECT value FROM system.metrics "
            "WHERE metric = 'ReplicatedQueuesTotalSize'"
        ).strip()
    )


def _replica_queue_size(node):
    return int(
        node.query(
            "SELECT queue_size FROM system.replicas "
            "WHERE database = currentDatabase() AND table = 't'"
        ).strip()
    )


def test_replication_queue_size_limit_gated_by_server_setting(start_cluster):
    # Baseline sanity.
    assert node_on.query("SELECT 1").strip() == "1"
    assert node_off.query("SELECT 1").strip() == "1"

    _create_replica(node_on)
    _create_replica(node_off)

    # Make node_on the stalled replica: it will enqueue but never drain GET_PART
    # entries, so its ZK queue/ childcount grows without bound.
    node_on.query("SYSTEM STOP FETCHES t")

    # Inflate via node_off (guard OFF => its own inserts are never gated, so we
    # can push as many parts as we need without tripping the limiter ourselves).
    for i in range(N_INFLATE_INSERTS):
        node_off.query(f"INSERT INTO t VALUES ({i}, 'x')")

    # Wait (bounded) until node_on's stalled physical queue has grown past the
    # throw threshold (5). Polled, not slept-on.
    for _ in range(60):
        if _replica_queue_size(node_on) > 5:
            break
        time.sleep(1)
    queue_on = _replica_queue_size(node_on)
    assert queue_on > 5, (
        f"setup failed: node_on stalled queue did not grow past 5 (got {queue_on})"
    )

    # Wait (bounded) until node_on's monitor thread has polled and published the
    # per-storage maximum as ReplicatedQueuesTotalSize. The monitor reschedules
    # every 1000 ms, so this is the synchronization point for the gating input.
    for _ in range(30):
        if _queues_total_size(node_on) > 5:
            break
        time.sleep(1)
    metric_on = _queues_total_size(node_on)
    metric_off = _queues_total_size(node_off)
    print(f"node_on  queue_size={queue_on} ReplicatedQueuesTotalSize={metric_on}")
    print(f"node_off ReplicatedQueuesTotalSize={metric_off}")

    assert metric_on > 5, (
        f"guard ON: monitor should publish a queue total > 5, got {metric_on}"
    )
    # Guard OFF => the monitor thread never started => metric stays 0. This is the
    # observable proof that the server guard (not the thresholds) gates the
    # feature.
    assert metric_off == 0, (
        f"guard OFF: monitor must not run, ReplicatedQueuesTotalSize should be 0, "
        f"got {metric_off}"
    )

    # "post" (guard ON): the next INSERT on node_on is rejected by the limiter.
    err = node_on.query_and_get_error("INSERT INTO t VALUES (999, 'y')")
    print("node_on INSERT error:", err)
    assert "Too large replication queue" in err, (
        f"guard ON: expected the Aiven-specific limiter message, got: {err}"
    )
    assert "LIMIT_EXCEEDED" in err, (
        f"guard ON: expected error code LIMIT_EXCEEDED, got: {err}"
    )

    # "pre" (guard OFF, control): the identical INSERT SUCCEEDS — the same cluster
    # has a huge replica queue, but the guard-off node reproduces upstream
    # behavior and accepts the write.
    node_off.query("INSERT INTO t VALUES (1000, 'z')")
    inserted = int(
        node_off.query(
            "SELECT count() FROM t WHERE k = 1000"
        ).strip()
    )
    assert inserted == 1, "guard OFF: control INSERT should have succeeded"

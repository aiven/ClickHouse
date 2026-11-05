"""
Aiven patch 008: inserts into a `ReplicatedMergeTree` table are delayed and then rejected
once a replica's replication queue in Keeper grows past a threshold, so that one slow
replica cannot grow its queue without bound.

Both nodes are replicas of the same tables with the same thresholds. Only `node_on`
enables the server setting `aiven_enable_replication_queue_size_limit`; `node_off` is
the upstream behaviour and the control for every case.

A queue is grown by stopping fetches on `node_on` and inserting through `node_off`:
every insert adds one `GET_PART` entry that `node_on` queues but cannot execute.

See docs/aiven/patches/008-replication-queue-size-limit.md.
"""

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node_on = cluster.add_instance(
    "node_on",
    main_configs=["configs/enable_queue_limit.xml"],
    with_zookeeper=True,
    macros={"replica": "r_on"},
)
node_off = cluster.add_instance(
    "node_off",
    with_zookeeper=True,
    macros={"replica": "r_off"},
)

THROW_AT = 5
INFLATING_INSERTS = 12


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_table(node, name, settings):
    node.query(
        f"CREATE TABLE {name} (k UInt64) "
        f"ENGINE = ReplicatedMergeTree('/clickhouse/tables/aiven_008/{name}', '{{replica}}') "
        f"ORDER BY k SETTINGS {settings}"
    )


def queues_total_size(node):
    return int(node.query("SELECT value FROM system.metrics WHERE metric = 'ReplicatedQueuesTotalSize'"))


def delayed_inserts(node):
    return int(node.query("SELECT sum(value) FROM system.events WHERE event = 'DelayedInserts'"))


def test_stalled_replica_delays_then_rejects_inserts(start_cluster):
    settings = f"aiven_queue_size_to_throw_insert = {THROW_AT}, aiven_queue_size_to_delay_insert = 1"
    for node in (node_on, node_off):
        create_table(node, "stalled", settings)

    node_on.query("SYSTEM STOP FETCHES stalled")
    for i in range(INFLATING_INSERTS):
        node_off.query(f"INSERT INTO stalled VALUES ({i})")

    # The monitor polls once per second; the metric is what the insert gate reads.
    assert_eq_with_retry(
        node_on,
        f"SELECT value > {THROW_AT} FROM system.metrics WHERE metric = 'ReplicatedQueuesTotalSize'",
        "1\n",
        retry_count=30,
        sleep_time=1,
    )
    assert queues_total_size(node_off) == 0

    error = node_on.query_and_get_error("INSERT INTO stalled VALUES (100)")
    assert "LIMIT_EXCEEDED" in error
    assert "Too large replication queue" in error

    # A query-level threshold overrides the table's: the insert is only delayed.
    delayed_before = delayed_inserts(node_on)
    node_on.query("INSERT INTO stalled SETTINGS aiven_queue_size_to_throw_insert = 1000000 VALUES (101)")
    assert delayed_inserts(node_on) > delayed_before

    # The same insert on the node without the server setting behaves as upstream.
    node_off.query("INSERT INTO stalled VALUES (102)")

    # Detaching the table removes its share of the server-wide total.
    node_on.query("DETACH TABLE stalled SYNC")
    assert queues_total_size(node_on) == 0


def test_threshold_zero(start_cluster):
    """With the server setting on, 0 is a threshold like any other; with it off, nothing is checked."""
    for node in (node_on, node_off):
        create_table(node, f"zero_{node.name}", "aiven_queue_size_to_throw_insert = 0")

    error = node_on.query_and_get_error("INSERT INTO zero_node_on VALUES (1)")
    assert "LIMIT_EXCEEDED" in error
    assert "Too large replication queue" in error

    node_off.query("INSERT INTO zero_node_off VALUES (1)")
    assert node_off.query("SELECT count() FROM zero_node_off") == "1\n"


def test_pre_rename_setting_name(start_cluster):
    """Tables created before the `aiven_` rename store the old name; its value must still apply."""
    create_table(node_on, "old_name", "queue_size_to_throw_insert = 0")

    error = node_on.query_and_get_error("INSERT INTO old_name VALUES (1)")
    assert "LIMIT_EXCEEDED" in error
    assert "Too large replication queue" in error

import threading
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)

# The harness lists three ZooKeeper nodes. A fresh server has no session, so the first query that needs
# ZooKeeper goes through `ZooKeeper::connect`; `CREATE TABLE ... ReplicatedMergeTree` connects in the
# storage constructor, outside the query-level Keeper retries.
node = cluster.add_instance(
    "node",
    main_configs=["configs/zk_connection_retries.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"replica": "r1"},
)

# A TCP reset is the fast failure of a restarting ZooKeeper: the attempt fails at once instead of
# waiting for the connection timeout.
FAST_FAILURE = "REJECT --reject-with tcp-reset"
BLACK_HOLE = "DROP"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_table(name):
    node.query(
        f"CREATE TABLE {name} (a UInt64) "
        f"ENGINE = ReplicatedMergeTree('/clickhouse/test/aiven_005/{name}', '{{replica}}') ORDER BY a"
    )


def restart_without_zookeeper(pm, action):
    pm.drop_instance_zk_connections(node, action=action)
    node.restart_clickhouse(kill=True)
    assert node.query("SELECT 1").strip() == "1"


def test_connect_survives_a_short_restart(start_cluster):
    node.query("DROP TABLE IF EXISTS fast_failure SYNC")

    with PartitionManager() as pm:
        restart_without_zookeeper(pm, FAST_FAILURE)

        # Unpatched, three rounds over three nodes fail within milliseconds. With a pause per
        # round the six rounds span about 6 s, so a ZooKeeper back after 3 s is reached.
        restore = threading.Timer(
            3,
            pm.restore_instance_zk_connections,
            args=(node,),
            kwargs={"action": FAST_FAILURE},
        )
        restore.start()
        try:
            started = time.monotonic()
            create_table("fast_failure")
            elapsed = time.monotonic() - started
        finally:
            restore.join()

    assert elapsed >= 2, f"CREATE returned after {elapsed:.1f} s, before ZooKeeper was restored"
    assert node.query("SELECT count() FROM system.tables WHERE name = 'fast_failure'").strip() == "1"
    node.query("DROP TABLE fast_failure SYNC")


def test_connect_gives_up_within_bound(start_cluster):
    with PartitionManager() as pm:
        restart_without_zookeeper(pm, BLACK_HOLE)

        # Each attempt waits the 1 s connection timeout: 6 rounds over 3 nodes plus 6.2 s of pauses is
        # about 24 s per connect. The DDL worker connects first at startup and holds the server-wide
        # ZooKeeper lock, so the CREATE waits for that connect and then makes its own: about 48 s.
        # A pause per node instead of per round takes about 140 s per connect.
        started = time.monotonic()
        error = node.query_and_get_error(
            "CREATE TABLE black_hole (a UInt64) "
            "ENGINE = ReplicatedMergeTree('/clickhouse/test/aiven_005/black_hole', '{replica}') ORDER BY a"
        )
        elapsed = time.monotonic() - started

    assert "All connection tries failed" in error
    assert elapsed < 120, f"connect gave up after {elapsed:.1f} s"

    create_table("after_outage")
    node.query("DROP TABLE after_outage SYNC")

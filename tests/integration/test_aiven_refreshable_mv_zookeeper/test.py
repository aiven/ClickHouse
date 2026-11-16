"""
Patch 050 (absorbs N02): coordinated refreshable materialized views work on Apache ZooKeeper.

Upstream 26.8 requires the ClickHouse-Keeper-only MULTI_READ and CREATE_IF_NOT_EXISTS feature
flags: a fresh CREATE fails with NOT_IMPLEMENTED, and a view loaded at startup (for example
after upgrading a server that runs on ZooKeeper) is stopped for good. On ZooKeeper the
`/running` create would also be sent as CreateIfNotExists, which ZooKeeper cannot parse.

`node_zk` embeds a Keeper with every Keeper-only extension turned off, so it behaves like
Apache ZooKeeper. `node_keeper` uses Keeper defaults and is the control: the patch must not
change behaviour there. Each node gets its own embedded Keeper, because the cluster helper can
only force flags on, not off.
"""

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node_zk = cluster.add_instance(
    "node_zk",
    main_configs=["configs/keeper_zookeeper_compatible.xml"],
    stay_alive=True,
)
node_keeper = cluster.add_instance(
    "node_keeper",
    main_configs=["configs/keeper_default.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_coordinated_view(node, db):
    # In a Replicated database a non-APPEND refreshable view is coordinated through Keeper.
    node.query(
        f"CREATE DATABASE {db} ENGINE = Replicated('/clickhouse/databases/{db}', '{{shard}}', '{{replica}}')"
    )
    node.query(f"CREATE TABLE {db}.src (a UInt64) ENGINE = ReplicatedMergeTree ORDER BY a")
    node.query(f"CREATE TABLE {db}.tgt (a UInt64) ENGINE = ReplicatedMergeTree ORDER BY a")
    node.query(
        f"CREATE MATERIALIZED VIEW {db}.mv REFRESH EVERY 1 HOUR TO {db}.tgt AS SELECT a FROM {db}.src"
    )


def refresh_and_check(node, db, expected_rows):
    node.query(f"SYSTEM REFRESH VIEW {db}.mv")
    node.query(f"SYSTEM WAIT VIEW {db}.mv")
    assert_eq_with_retry(node, f"SELECT count() FROM {db}.tgt", f"{expected_rows}\n")
    status = node.query(
        f"SELECT status, exception FROM system.view_refreshes WHERE database = '{db}' AND view = 'mv'"
    )
    assert "Disabled" not in status, status
    # A rejected Keeper request used to drop the shared session: replicated tables went read-only.
    assert node.query(f"SELECT max(is_readonly) FROM system.replicas WHERE database = '{db}'") == "0\n"


@pytest.mark.parametrize("node_name", ["node_zk", "node_keeper"])
def test_coordinated_view_refreshes_and_survives_restart(start_cluster, node_name):
    node = node_zk if node_name == "node_zk" else node_keeper
    db = f"aiven_050_{node_name}"

    # Without the patch, on node_zk this CREATE fails with NOT_IMPLEMENTED.
    create_coordinated_view(node, db)

    node.query(f"INSERT INTO {db}.src VALUES (1), (2), (3)")
    refresh_and_check(node, db, 3)

    # A second refresh creates and removes `/running` again.
    node.query(f"INSERT INTO {db}.src VALUES (4)")
    refresh_and_check(node, db, 4)

    # The upgrade case: the view is loaded at startup. Without the patch, on node_zk it is
    # stopped for good ("Keeper server doesn't have all feature flags ...").
    node.restart_clickhouse()
    node.query(f"INSERT INTO {db}.src VALUES (5)")
    refresh_and_check(node, db, 5)

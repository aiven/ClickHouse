# Smoke test for the Aiven integration lane: proves the lane starts a cluster
# and runs at least one test against a server that can write and read a
# MergeTree table.

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_merge_tree_round_trip(started_cluster):
    node.query(
        "CREATE TABLE aiven_smoke (id UInt64, s String) ENGINE = MergeTree ORDER BY id"
    )
    try:
        node.query("INSERT INTO aiven_smoke VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        result = node.query(
            "SELECT count(), sum(id), arraySort(groupArray(s)) FROM aiven_smoke"
        )
        assert result == "3\t6\t['a','b','c']\n"
    finally:
        node.query("DROP TABLE aiven_smoke SYNC")

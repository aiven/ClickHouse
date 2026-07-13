import logging

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

ZK_PATH = "/named_collections_path"


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node_with_keeper",
            main_configs=[
                "configs/config.d/named_collections_with_zookeeper.xml",
            ],
            user_configs=[
                "configs/users.d/users.xml",
            ],
            stay_alive=True,
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node_with_keeper_2",
            main_configs=[
                "configs/config.d/named_collections_with_zookeeper.xml",
            ],
            user_configs=[
                "configs/users.d/users.xml",
            ],
            stay_alive=True,
            with_zookeeper=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def test_keeper_storage_alter_propagates_to_replica(cluster):
    node1 = cluster.instances["node_with_keeper"]
    node2 = cluster.instances["node_with_keeper_2"]
    zk = cluster.get_kazoo_client("zoo1")

    collection = "alter_propagation_coll"
    node1.query(f"DROP NAMED COLLECTION IF EXISTS {collection}")

    try:
        node1.query(f"CREATE NAMED COLLECTION {collection} AS key1='initial'")

        # Both replicas must first see the created collection.
        for node in (node1, node2):
            assert_eq_with_retry(
                node,
                f"SELECT collection['key1'] FROM system.named_collections WHERE name = '{collection}'",
                "initial",
            )

        zk.sync(ZK_PATH)
        children_before = sorted(zk.get_children(ZK_PATH))

        node1.query(
            f"ALTER NAMED COLLECTION {collection} SET key1='updated', key2='added'"
        )

        assert_eq_with_retry(
            node2,
            f"SELECT collection['key1'], collection['key2'] FROM system.named_collections WHERE name = '{collection}'",
            "updated\tadded",
        )

        node2.query(
            f"ALTER NAMED COLLECTION {collection} SET key1='final' DELETE key2"
        )
        assert_eq_with_retry(
            node1,
            f"SELECT collection['key1'], mapContains(collection, 'key2') FROM system.named_collections WHERE name = '{collection}'",
            "final\t0",
        )

        zk.sync(ZK_PATH)
        assert children_before == sorted(zk.get_children(ZK_PATH))
    finally:
        node1.query(f"DROP NAMED COLLECTION IF EXISTS {collection}")
        for node in (node1, node2):
            assert_eq_with_retry(
                node,
                f"SELECT count() FROM system.named_collections WHERE name = '{collection}'",
                "0",
            )

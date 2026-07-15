"""Aiven regression test for patch-port(080): in-place ALTER NAMED COLLECTION
propagation across replicas on ZooKeeper.

Background: with the ZooKeeper-backed named-collections metadata storage, each
collection is a *child* znode under the storage root. Replicas detect changes by
watching the root's children and its `cversion` (bumped only on child add/remove).
An in-place `ALTER NAMED COLLECTION ... SET/DELETE` is a `set` on the child znode,
which on ZooKeeper fires neither the parent children-watch nor bumps `cversion`, so
other replicas never observed the edit. The fix (`05fe09e` on
`v25.8.26.11-lts-aiven`, re-homed here) additionally tracks and bumps the root
node's data `version` on every in-place write, and watches it, so the change
propagates in both directions.

This test lives in an Aiven-prefixed directory (rather than editing the upstream
`test_named_collections`) so it is picked up by the Aiven CI lane and does not add
churn to an upstream test file on future uplifts.

Evidence pair: pre-fix -> node2 never observes the SET/DELETE (assert_eq_with_retry
times out); post-fix -> both directions propagate, and the root children set is
unchanged (only the data version was bumped).
"""

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

ZK_PATH = "/aiven_named_collections_path"


@pytest.fixture(scope="module")
def cluster():
    cluster = ClickHouseCluster(__file__)
    try:
        # use_keeper=False forces a real Apache ZooKeeper ensemble (not
        # ClickHouse Keeper). This is load-bearing: the bug being fixed is a
        # real-ZooKeeper watch semantic (a `set` on a child znode does not fire
        # the parent children-watch nor bump its cversion). ClickHouse Keeper
        # does not reproduce it, so the test must run against real ZooKeeper —
        # which also matches the Aiven managed-service topology.
        cluster.add_instance(
            "node1",
            main_configs=["configs/config.d/named_collections_with_zookeeper.xml"],
            user_configs=["configs/users.d/users.xml"],
            stay_alive=True,
            with_zookeeper=True,
            use_keeper=False,
        )
        cluster.add_instance(
            "node2",
            main_configs=["configs/config.d/named_collections_with_zookeeper.xml"],
            user_configs=["configs/users.d/users.xml"],
            stay_alive=True,
            with_zookeeper=True,
            use_keeper=False,
        )
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_alter_named_collection_propagates_to_replica(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    zk = cluster.get_kazoo_client("zoo1")

    collection = "alter_propagation_coll"
    node1.query(f"DROP NAMED COLLECTION IF EXISTS {collection}")

    try:
        node1.query(f"CREATE NAMED COLLECTION {collection} AS key1='initial'")

        # Both replicas must first observe the created collection (create bumps
        # cversion, so this path works even pre-fix).
        for node in (node1, node2):
            assert_eq_with_retry(
                node,
                f"SELECT collection['key1'] FROM system.named_collections WHERE name = '{collection}'",
                "initial",
                retry_count=60,
                sleep_time=1,
            )

        zk.sync(ZK_PATH)
        children_before = sorted(zk.get_children(ZK_PATH))

        # In-place ALTER on node1 (a `set` on the child znode). Pre-fix this is
        # invisible to node2; post-fix the root data-version bump propagates it.
        node1.query(
            f"ALTER NAMED COLLECTION {collection} SET key1='updated', key2='added'"
        )
        assert_eq_with_retry(
            node2,
            f"SELECT collection['key1'], collection['key2'] FROM system.named_collections WHERE name = '{collection}'",
            "updated\tadded",
            retry_count=60,
            sleep_time=1,
        )

        # Reverse direction: in-place ALTER on node2 must reach node1.
        node2.query(
            f"ALTER NAMED COLLECTION {collection} SET key1='final' DELETE key2"
        )
        assert_eq_with_retry(
            node1,
            f"SELECT collection['key1'], mapContains(collection, 'key2') FROM system.named_collections WHERE name = '{collection}'",
            "final\t0",
            retry_count=60,
            sleep_time=1,
        )

        # In-place edits must not change the set of child znodes (only their data
        # and the root data version); this guards the propagation trigger against
        # accidentally recreating nodes.
        zk.sync(ZK_PATH)
        assert children_before == sorted(zk.get_children(ZK_PATH))
    finally:
        node1.query(f"DROP NAMED COLLECTION IF EXISTS {collection}")
        for node in (node1, node2):
            assert_eq_with_retry(
                node,
                f"SELECT count() FROM system.named_collections WHERE name = '{collection}'",
                "0",
                retry_count=60,
                sleep_time=1,
            )

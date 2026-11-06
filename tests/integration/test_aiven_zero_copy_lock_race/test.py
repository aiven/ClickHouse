"""
Aiven patch 048: two shards of a table in a `Replicated` database share the table shared id, so
their zero-copy locks for parts with the same name live under the same node
`/clickhouse/zero_copy/zero_copy_s3/<uuid>/<part_name>`. Committing the first part (`all_0_0_0`)
on both shards at once must not fail on creating that node.

The window between building the commit's Keeper requests and running them is widened with the
`rmt_delay_commit_part` failpoint on both shards, so both see the shared node missing before
either commits.

See docs/aiven/patches/048-zero-copy-lock-race.md.
"""

import threading

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)


def add_node(name, shard, replica):
    return cluster.add_instance(
        name,
        main_configs=["configs/storage_conf.xml"],
        macros={"shard": shard, "replica": replica},
        with_minio=True,
        with_zookeeper=True,
        stay_alive=True,
    )


node1 = add_node("node1", "s1", "r1")
node2 = add_node("node2", "s2", "r2")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_same_part_name_committed_on_two_shards(started_cluster):
    for node in (node1, node2):
        node.query("CREATE DATABASE zc_race ENGINE = Replicated('/test/zc_race_db', '{shard}', '{replica}')")
    node1.query(
        "CREATE TABLE zc_race.tbl (id UInt32) ENGINE = ReplicatedMergeTree ORDER BY id "
        "SETTINGS storage_policy = 's3', allow_remote_fs_zero_copy_replication = 1"
    )
    node2.query_with_retry(
        "SELECT count() FROM system.tables WHERE database = 'zc_race' AND name = 'tbl'",
        check_callback=lambda x: x.strip() == "1",
    )
    table_uuid = node1.query("SELECT uuid FROM system.tables WHERE database = 'zc_race' AND name = 'tbl'").strip()

    for node in (node1, node2):
        node.query("SYSTEM ENABLE FAILPOINT rmt_delay_commit_part")

    errors = {}

    def insert(node):
        try:
            node.query("INSERT INTO zc_race.tbl VALUES (1)")
        except Exception as e:
            errors[node.name] = str(e)

    threads = [threading.Thread(target=insert, args=(node,)) for node in (node1, node2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for node in (node1, node2):
        node.query("SYSTEM DISABLE FAILPOINT rmt_delay_commit_part")

    assert not errors, errors
    for node in (node1, node2):
        assert node.query("SELECT name FROM system.parts WHERE database = 'zc_race' AND table = 'tbl' AND active") == "all_0_0_0\n"

    # Both shards hold their own lock (one per blob id) under the shared part node.
    part_path = f"/clickhouse/zero_copy/zero_copy_s3/{table_uuid}/all_0_0_0"
    assert node1.query(f"SELECT count() FROM system.zookeeper WHERE path = '{part_path}'") == "2\n"

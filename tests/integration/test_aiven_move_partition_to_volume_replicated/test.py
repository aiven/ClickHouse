import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Two replicas of one shard of a Replicated database. Both nodes expand the same {shard}
# macro and a distinct {replica}; default_replica_path is {uuid}/{shard}-keyed, so every
# ReplicatedMergeTree table is also replicated between the two nodes.
COMMON = dict(
    main_configs=["configs/storage_policy.xml", "configs/default_replica_path.xml"],
    with_zookeeper=True,
    stay_alive=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
)
node1 = cluster.add_instance(
    "node1", macros={"shard": "shard1", "replica": "replica1"}, **COMMON
)
node2 = cluster.add_instance(
    "node2", macros={"shard": "shard1", "replica": "replica2"}, **COMMON
)
NODES = [node1, node2]


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_database(db):
    for node in NODES:
        node.query(
            f"CREATE DATABASE {db} ENGINE = Replicated('/clickhouse/databases/{db}', '{{shard}}', '{{replica}}')"
        )


def create_table(db, table):
    node1.query(
        f"CREATE TABLE {db}.{table} (a UInt64) ENGINE = ReplicatedMergeTree() "
        "PARTITION BY a ORDER BY a SETTINGS storage_policy = 'tiered'"
    )
    node2.query(f"SYSTEM SYNC DATABASE REPLICA {db}")


def insert_and_sync(db, table, values):
    node1.query(f"INSERT INTO {db}.{table} VALUES {values}")
    node2.query(f"SYSTEM SYNC REPLICA {db}.{table}")


def disk_of(node, db, table, partition):
    return node.query(
        f"SELECT disk_name FROM system.parts WHERE database = '{db}' AND table = '{table}' "
        f"AND partition = '{partition}' AND active"
    ).strip()


def db_log_entries(db, pattern):
    return int(
        node1.query(
            f"SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/databases/{db}/log' "
            f"AND value LIKE '%{pattern}%'"
        )
    )


def test_move_to_volume_runs_on_every_replica(start_cluster):
    # MOVE ... TO VOLUME is local to a replica and not in the table log. Without the patch it
    # runs only in node1's session and node2's copy stays on the default disk.
    create_database("mv_vol")
    create_table("mv_vol", "t")
    insert_and_sync("mv_vol", "t", "(1)")
    assert [disk_of(n, "mv_vol", "t", 1) for n in NODES] == ["default", "default"]

    node1.query("ALTER TABLE mv_vol.t MOVE PARTITION 1 TO VOLUME 'cold'")

    assert [disk_of(n, "mv_vol", "t", 1) for n in NODES] == ["ext_disk", "ext_disk"]
    assert db_log_entries("mv_vol", "TO VOLUME") == 1


def test_move_to_disk_runs_on_every_replica(start_cluster):
    create_database("mv_disk")
    create_table("mv_disk", "t")
    insert_and_sync("mv_disk", "t", "(1)")

    node1.query("ALTER TABLE mv_disk.t MOVE PARTITION 1 TO DISK 'ext_disk'")

    assert [disk_of(n, "mv_disk", "t", 1) for n in NODES] == ["ext_disk", "ext_disk"]
    assert db_log_entries("mv_disk", "TO DISK") == 1


def test_move_to_table_is_routed_and_runs_once(start_cluster):
    # MOVE ... TO TABLE goes through the database log too, but it is a leader-only task: one
    # replica runs it and the other gets the result through table replication.
    create_database("mv_tbl")
    create_table("mv_tbl", "src")
    create_table("mv_tbl", "dst")
    insert_and_sync("mv_tbl", "src", "(1)")

    node1.query("ALTER TABLE mv_tbl.src MOVE PARTITION 1 TO TABLE mv_tbl.dst")

    assert db_log_entries("mv_tbl", "TO TABLE") == 1
    for node in NODES:
        node.query("SYSTEM SYNC REPLICA mv_tbl.dst")
        node.query("SYSTEM SYNC REPLICA mv_tbl.src")
        assert node.query("SELECT count() FROM mv_tbl.src").strip() == "0"
        assert node.query("SELECT count() FROM mv_tbl.dst").strip() == "1"


def test_mixed_batch_moves_on_one_replica(start_cluster):
    # Known limitation: a batch that is not only MOVE ... TO DISK/VOLUME is leader-only, so its
    # TO VOLUME part moves data on one replica, the same as without the patch.
    create_database("mv_mix")
    create_table("mv_mix", "src")
    create_table("mv_mix", "dst")
    insert_and_sync("mv_mix", "src", "(1), (2)")

    node1.query(
        "ALTER TABLE mv_mix.src MOVE PARTITION 1 TO VOLUME 'cold', MOVE PARTITION 2 TO TABLE mv_mix.dst"
    )

    assert db_log_entries("mv_mix", "TO VOLUME") == 1
    assert sorted(disk_of(n, "mv_mix", "src", 1) for n in NODES) == ["default", "ext_disk"]


def test_replay_failure_on_other_replica(start_cluster):
    # Known cost: a replay that fails on another replica (here moves are stopped there; in
    # practice "already on volume" or "no such part") is a database entry that cannot be
    # skipped. That replica retries it until max_retries_before_automatic_recovery, then
    # recovers as a lost replica, which skips the move. The client gets a timeout.
    create_database("mv_fail")
    create_table("mv_fail", "t")
    insert_and_sync("mv_fail", "t", "(1)")
    node2.query("SYSTEM STOP MOVES mv_fail.t")

    error = node1.query_and_get_error(
        "ALTER TABLE mv_fail.t MOVE PARTITION 1 TO VOLUME 'cold' SETTINGS distributed_ddl_task_timeout = 10"
    )
    assert "TIMEOUT_EXCEEDED" in error and "is not finished on 1 of 2 hosts" in error

    # Later DDL waits behind the failing entry and reaches node2 only after the recovery.
    node1.query(
        "CREATE TABLE mv_fail.after (a UInt64) ENGINE = ReplicatedMergeTree() ORDER BY a "
        "SETTINGS distributed_ddl_task_timeout = 300"
    )
    assert node2.contains_in_log("Database got stuck at processing task")
    assert node2.contains_in_log("Replica seems to be lost")
    assert node2.query("EXISTS TABLE mv_fail.after").strip() == "1"
    assert [disk_of(n, "mv_fail", "t", 1) for n in NODES] == ["ext_disk", "default"]

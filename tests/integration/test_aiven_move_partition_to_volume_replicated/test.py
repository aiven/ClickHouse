import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

# Two REPLICAS of ONE shard of a Replicated (DatabaseReplicated) database. Both
# nodes expand the same {shard} macro (shard1) but a distinct {replica}, so they
# are replica1/replica2 of the same database and -- because default_replica_path
# is {uuid}/{shard}-keyed -- of the same ReplicatedMergeTree table.
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


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_move_partition_to_volume_replicates_to_all_replicas(start_cluster):
    """
    Patch 009 (110900c986 / aiven a2c312b): replicate ALTER TABLE ... MOVE
    PARTITION queries through DatabaseReplicated.

    Differential evidence pair on the *storage tier of the part on the
    non-initiating replica*, not on mere completion.

    Topology: a 2-replica (single-shard) Replicated database; one
    ReplicatedMergeTree table with a 'tiered' storage policy (volume 'hot' =
    built-in 'default' disk, volume 'cold' = ext_disk). A part is inserted (it
    lands on 'hot' and replicates to both nodes via table replication), then
    `ALTER TABLE ... MOVE PARTITION 1 TO VOLUME 'cold'` is issued on node1 only.

    MOVE ... TO VOLUME is NOT a leader-only DDL task
    (DDLWorker::taskShouldBeExecutedOnLeader returns false for
    isMovePartitionToDiskOrVolumeAlter), so once the query is in the database DDL
    log it runs on EVERY replica, each moving its own local copy of the part.

    Pre-009:  shouldReplicateQuery() returns false for MOVE PARTITION, so the move
              executes only on node1 (the receiving replica); node2's copy of the
              part stays on the 'hot' (default) disk -> tiering diverges.
    Post-009: the move is routed through the DDL log and replayed on node2, which
              moves its own copy to ext_disk too -> both replicas converge on
              'cold'.

    The observable that flips is node2's part disk_name ('ext_disk' post-009,
    still 'default' pre-009). This is why the test moves TO VOLUME and not TO
    TABLE: a TO-TABLE move is a leader-only DDL task, so it executes on a single
    replica with or without the patch and is blind to the routing change.
    """
    db_path = "/clickhouse/databases/move_vol"

    # Same DB ZK path, same {shard}, distinct {replica} -> two replicas of one DR
    # database.
    node1.query(
        f"CREATE DATABASE move_vol ENGINE = Replicated('{db_path}', '{{shard}}', '{{replica}}')"
    )
    node2.query(
        f"CREATE DATABASE move_vol ENGINE = Replicated('{db_path}', '{{shard}}', '{{replica}}')"
    )

    # One ReplicatedMergeTree table, created once (CREATE in a Replicated database
    # is itself a replicated DDL that propagates to the other replica).
    node1.query(
        "CREATE TABLE move_vol.t (a UInt64) ENGINE = ReplicatedMergeTree() "
        "PARTITION BY a ORDER BY a SETTINGS storage_policy = 'tiered'"
    )
    node2.query("SYSTEM SYNC DATABASE REPLICA move_vol")

    # Insert one partition; it lands on the first volume ('hot' = default disk) and
    # replicates to node2 via table replication.
    node1.query("INSERT INTO move_vol.t VALUES (1)")
    node2.query("SYSTEM SYNC REPLICA move_vol.t")

    part_disk = (
        "SELECT disk_name FROM system.parts "
        "WHERE database = 'move_vol' AND table = 't' AND partition = '1' AND active"
    )

    # Sanity: before the move the part is on the default disk on BOTH replicas.
    assert node1.query(part_disk).strip() == "default"
    assert node2.query(part_disk).strip() == "default"

    # Move the partition to the 'cold' volume (ext_disk) on node1 ONLY.
    node1.query("ALTER TABLE move_vol.t MOVE PARTITION 1 TO VOLUME 'cold'")

    # node1 (the initiator) moved its copy.
    assert_eq_with_retry(node1, part_disk, "ext_disk\n", retry_count=60, sleep_time=1)

    # Post-009 discriminator: the move was routed through the DB DDL log and
    # replayed on node2, so node2's copy is ALSO on ext_disk. Pre-009 node2 stays
    # on 'default' and this assertion fails.
    assert_eq_with_retry(node2, part_disk, "ext_disk\n", retry_count=60, sleep_time=1)

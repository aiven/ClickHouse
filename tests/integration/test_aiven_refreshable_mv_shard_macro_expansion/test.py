import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/no_shard_macro.xml"],
    with_zookeeper=True,
    # MULTI_READ is otherwise randomized per-run (see helpers/cluster.py
    # keeper_randomize_feature_flags=True default). Without it, the patched
    # RefreshTask code path throws NOT_IMPLEMENTED at line 119 before the
    # multi-write at line 142, masking the differential we want to exercise.
    # Pinning it matches the precedent in
    # tests/integration/test_refreshable_mv_skip_old_temp_table_ddls/.
    keeper_required_feature_flags=["multi_read"],
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_refreshable_mv_in_replicated_database_expands_shard_macro(start_cluster):
    """
    Patch 049 (cc745f53f9): Fix refreshable materialized views where there is a
    shard macro in the target table.

    Trigger reachability (parent (iv) preflight, recorded here so the test is
    self-documenting): in a DatabaseReplicated database, the upstream sanity
    check at StorageMaterializedView.cpp:222-225 rejects the combination
    "replicated database + non-replicated target + non-APPEND" with Code: 36
    BAD_ARGUMENTS BEFORE RefreshTask::create is invoked. Using a
    ReplicatedMergeTree target table (which DR rewrites internally to use
    default_replica_path + default_replica_name) passes both gates and reaches
    RefreshTask::RefreshTask, where the patched code path lives.

    Pre-patch behavior: RefreshTask::RefreshTask cannot expand {shard} in
      default_replica_path ('/clickhouse/tables/{uuid}/{shard}') because
      info.shard is unset and the server config has no global <shard>
      macro fallback. CREATE MATERIALIZED VIEW throws Code: 139
      (NO_ELEMENTS_IN_CONFIG).
    Post-patch behavior: info.shard = replicated_db->getShardName() so {shard}
      expands to "aiven_shard_x" and CREATE MATERIALIZED VIEW succeeds.

    Note: the target table itself uses DR's own macro-expansion code
    (DatabaseReplicated.cpp:1191-1199) which already sets info.shard from
    getShardName(). The target's creation therefore succeeds on BOTH the
    pre-patch and post-patch builds; the differential is purely in the RMV's
    coordination-path expansion done by the patched RefreshTask code.
    """
    node1.query(
        "CREATE DATABASE testdb ENGINE = Replicated("
        "'/clickhouse/databases/testdb', 'aiven_shard_x', 'aiven_replica_y')"
    )

    # Target is ReplicatedMergeTree() with no explicit args — DR synthesizes
    # the ZK path via default_replica_path (which contains {shard}). DR's own
    # macro expansion handles this independently of patch 049.
    node1.query(
        "CREATE TABLE testdb.target (a UInt64) ENGINE = ReplicatedMergeTree() ORDER BY a"
    )

    # The CREATE that fires the patched code path. Pre-patch:
    # NO_ELEMENTS_IN_CONFIG. Post-patch: succeeds.
    node1.query(
        "CREATE MATERIALIZED VIEW testdb.mv REFRESH EVERY 1 HOUR TO testdb.target "
        "AS SELECT 1 AS a"
    )

    # Sanity: the RMV is registered. (We avoid asserting on the resolved ZK
    # path directly to keep the test resilient to future system-table schema
    # changes; the differential is already covered by the pre-patch error.)
    result = node1.query(
        "SELECT count() FROM system.view_refreshes "
        "WHERE database = 'testdb' AND view = 'mv'"
    )
    assert result.strip() == "1", f"Expected one refresh entry, got: {result!r}"

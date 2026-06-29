import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

# Two SHARDS of ONE Replicated (DatabaseReplicated) database. Each shard is a
# single node; the two nodes share the same database ZK path but expand a
# distinct {shard} macro, so they form a 2-shard / 1-replica-per-shard cluster.
#
# This is the minimal topology that exercises patch 066: a refreshable MV in a
# sharded Replicated database. Pre-066 every shard ran the refresh fully
# independently and finished it with a replicated EXCHANGE/DROP; because DDL in
# a Replicated database propagates to ALL shards, one shard's EXCHANGE swapped
# in a temp table that held no data on the other shards, deleting their freshly
# written rows. Post-066 a single global leader coordinates: one shared temp
# table (keyed by UUID in Keeper), every shard writes its local rows into its
# copy of it, and the EXCHANGE happens only once, after all shards report
# finished -- so every shard keeps its own data.
#
# multi_read / create_if_not_exists are pinned because helpers/cluster.py
# randomizes Keeper feature flags per run; pinning matches the precedent in
# test_refreshable_mat_view_replicated and keeps the coordination path
# deterministic.
shard1 = cluster.add_instance(
    "shard1",
    main_configs=["configs/default_replica_path.xml"],
    with_zookeeper=True,
    stay_alive=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    macros={"shard": "shard1", "replica": "replica1"},
)
shard2 = cluster.add_instance(
    "shard2",
    main_configs=["configs/default_replica_path.xml"],
    with_zookeeper=True,
    stay_alive=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    macros={"shard": "shard2", "replica": "replica1"},
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_sharded_refresh_preserves_per_shard_data(start_cluster):
    """
    Patch 066 (3866708bab): Fix MV refresh in sharded environment.

    Differential evidence pair (assert the per-shard row counts, not "it ran").

    Setup: a 2-shard Replicated database with a refreshable MV
        CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 HOUR TO tgt AS SELECT a FROM src
    where each shard holds DISJOINT source data (INSERT in a Replicated database
    is local to the shard, so shard1.src = {1,2,3}, shard2.src = {10,20,30}).

    Pre-066 behavior: each shard refreshes independently; the replicated
      EXCHANGE from one shard's refresh swaps `tgt` for a temp table that is
      empty on the other shard, so at least one shard's `tgt` loses its rows
      (data loss) -> one of the assertions below FAILS.
    Post-066 behavior: the global leader exchanges only after every shard has
      written into the shared temp table, so each shard's `tgt` retains exactly
      its own source rows -> both assertions PASS.

    The observable that flips is per-shard target content
    (shard1.tgt == {1,2,3} AND shard2.tgt == {10,20,30}), a differential on the
    data itself rather than on an error code or mere completion.
    """
    db_path = "/clickhouse/databases/sharded_rmv"

    # Same DB ZK path, distinct {shard} macro -> two shards of one DR database.
    shard1.query(
        f"CREATE DATABASE sharded_rmv ENGINE = Replicated('{db_path}', '{{shard}}', '{{replica}}')"
    )
    shard2.query(
        f"CREATE DATABASE sharded_rmv ENGINE = Replicated('{db_path}', '{{shard}}', '{{replica}}')"
    )

    # Source + target are bare ReplicatedMergeTree(). The configs/ drop-in sets
    # default_replica_path = /clickhouse/tables/{uuid}/{shard}: {shard} gives each
    # shard INDEPENDENT data, and {uuid} keys the path by table identity so the
    # refresh's temp inner table (a fresh-UUID copy of the target) gets a distinct
    # path instead of colliding with the target (REPLICA_ALREADY_EXISTS). Replicated
    # databases keep {uuid}/{shard}/{replica} unexpanded in metadata and expand them
    # per member at runtime (see TableZnodeInfo::resolve). Explicit engine args are
    # rejected in a Replicated database (database_replicated_allow_replicated_engine_
    # arguments defaults to 0), so the shard/uuid keys are supplied via the config.
    #
    # CREATE TABLE in a Replicated database is itself a replicated DDL that
    # propagates to every shard, so each table is created ONCE (issuing the CREATE
    # per node double-creates -> TABLE_ALREADY_EXISTS on the second shard).
    shard1.query(
        "CREATE TABLE sharded_rmv.src (a UInt64) ENGINE = ReplicatedMergeTree() ORDER BY a"
    )
    shard1.query(
        "CREATE TABLE sharded_rmv.tgt (a UInt64) ENGINE = ReplicatedMergeTree() ORDER BY a"
    )
    # Ensure shard2 has applied the replicated CREATE TABLE DDL before it inserts.
    shard2.query("SYSTEM SYNC DATABASE REPLICA sharded_rmv")

    # Disjoint per-shard source data. INSERT into a Replicated-database table is
    # local to the issuing node's shard (it is not distributed across shards).
    shard1.query("INSERT INTO sharded_rmv.src VALUES (1), (2), (3)")
    shard2.query("INSERT INTO sharded_rmv.src VALUES (10), (20), (30)")

    # The refreshable MV. The CREATE replicates to both shards as one DDL.
    shard1.query(
        "CREATE MATERIALIZED VIEW sharded_rmv.mv REFRESH EVERY 1 HOUR "
        "TO sharded_rmv.tgt AS SELECT a FROM sharded_rmv.src"
    )

    # Drive one out-of-schedule coordinated refresh across all shards.
    shard1.query("SYSTEM REFRESH VIEW sharded_rmv.mv")

    # Post-066: every shard keeps exactly its own source rows in the target.
    assert_eq_with_retry(
        shard1,
        "SELECT a FROM sharded_rmv.tgt ORDER BY a",
        "1\n2\n3\n",
        retry_count=120,
        sleep_time=1,
    )
    assert_eq_with_retry(
        shard2,
        "SELECT a FROM sharded_rmv.tgt ORDER BY a",
        "10\n20\n30\n",
        retry_count=120,
        sleep_time=1,
    )

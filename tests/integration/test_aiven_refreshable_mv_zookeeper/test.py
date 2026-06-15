import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

# `node` embeds a Keeper with MULTI_READ DISABLED (simulating plain Apache
# ZooKeeper). It hosts the differential evidence pair for patch 050.
node = cluster.add_instance(
    "node",
    main_configs=["configs/keeper_no_multi_read.xml"],
    stay_alive=True,
)

# `node_mr` embeds a Keeper with MULTI_READ ENABLED (the ClickHouse Keeper
# default). It hosts the regression-guard control proving the
# atomic-multi -> async-create rewrite did not regress the happy path.
node_mr = cluster.add_instance(
    "node_mr",
    main_configs=["configs/keeper_with_multi_read.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_refreshable_mv_without_multi_read(start_cluster):
    """
    Patch 050 (63f05c4e12): Allow refreshable materialized views when using
    ZooKeeper (rather than ClickHouse Keeper).

    Differential evidence pair. `node`'s embedded Keeper has the MULTI_READ
    feature flag disabled, simulating plain Apache ZooKeeper.

    Trigger reachability: in a Replicated database a non-APPEND refreshable
    materialized view is forced into coordinated mode
    (StorageMaterializedView.cpp: refresh_coordinated = true), which makes
    RefreshTask::RefreshTask enter the `if (coordinated)` block and, on a
    fresh CREATE (attach = false, is_restore_from_backup = false) where the
    replica znode does not yet exist, reach the MULTI_READ guard. A Replicated
    target table is required, otherwise the upstream sanity check at
    StorageMaterializedView.cpp:224-225 rejects the combination with
    BAD_ARGUMENTS before RefreshTask runs.

    Pre-patch behavior: RefreshTask::RefreshTask throws
      Code: 48 NOT_IMPLEMENTED "Keeper server doesn't support multi-reads."
      so CREATE MATERIALIZED VIEW fails and node.query raises -> test FAILS.
    Post-patch behavior: the guard is gone, the coordination znodes are created
      via asyncTryCreateNoThrow, and CREATE MATERIALIZED VIEW succeeds -> the
      MV is registered -> test PASSES.

    The observable that flips is presence/absence of the CREATE error
    (a presence/absence-of-error differential, stronger than an error-code
    assert per docs/aiven/AGENTS.md §7), not an error-substring check.
    """
    node.query(
        "CREATE DATABASE rmv_db_no_mr ENGINE = Replicated("
        "'/clickhouse/databases/rmv_db_no_mr', '{shard}', '{replica}')"
    )

    # Source and target are ReplicatedMergeTree() with no explicit args; the
    # Replicated database synthesizes the ZK path via default_replica_path.
    node.query(
        "CREATE TABLE rmv_db_no_mr.src (a UInt64) ENGINE = ReplicatedMergeTree() ORDER BY a"
    )
    node.query(
        "CREATE TABLE rmv_db_no_mr.tgt (a UInt64) ENGINE = ReplicatedMergeTree() ORDER BY a"
    )

    # The CREATE that fires the patched code path. Pre-patch: NOT_IMPLEMENTED
    # "Keeper server doesn't support multi-reads.". Post-patch: succeeds.
    node.query(
        "CREATE MATERIALIZED VIEW rmv_db_no_mr.mv REFRESH EVERY 1 HOUR "
        "TO rmv_db_no_mr.tgt AS SELECT a FROM rmv_db_no_mr.src"
    )

    # Post-patch assertion: the refreshable MV is registered.
    result = node.query(
        "SELECT count() FROM system.tables "
        "WHERE database = 'rmv_db_no_mr' AND name = 'mv'"
    )
    assert result.strip() == "1", f"Expected the MV to exist, got: {result!r}"


def test_refreshable_mv_with_multi_read_still_works(start_cluster):
    """
    Regression-guard control for behavior change 2 of patch 050 (the atomic
    `multi(ops)` -> independent `asyncTryCreateNoThrow` rewrite). `node_mr`'s
    embedded Keeper has MULTI_READ enabled (the ClickHouse Keeper default).

    This case PASSES both pre- and post-patch: the MULTI_READ path worked
    before, and the async rewrite must not regress it. It is a control, not
    part of the FAIL->PASS pair. If it FAILS post-patch, the async rewrite
    regressed the Keeper happy path (a real finding to escalate).

    Besides asserting the CREATE succeeds, it drives one full refresh
    (SYSTEM REFRESH VIEW + wait for the data to land in the target). The
    refresh path exercises RefreshTask::readZnodesIfNeeded, whose MULTI_READ
    guard was also removed by the patch, so this confirms reads still work
    with MULTI_READ enabled.
    """
    node_mr.query(
        "CREATE DATABASE rmv_db_mr ENGINE = Replicated("
        "'/clickhouse/databases/rmv_db_mr', '{shard}', '{replica}')"
    )
    node_mr.query(
        "CREATE TABLE rmv_db_mr.src (a UInt64) ENGINE = ReplicatedMergeTree() ORDER BY a"
    )
    node_mr.query(
        "CREATE TABLE rmv_db_mr.tgt (a UInt64) ENGINE = ReplicatedMergeTree() ORDER BY a"
    )
    node_mr.query(
        "CREATE MATERIALIZED VIEW rmv_db_mr.mv REFRESH EVERY 1 HOUR "
        "TO rmv_db_mr.tgt AS SELECT a FROM rmv_db_mr.src"
    )

    result = node_mr.query(
        "SELECT count() FROM system.tables "
        "WHERE database = 'rmv_db_mr' AND name = 'mv'"
    )
    assert result.strip() == "1", f"Expected the MV to exist, got: {result!r}"

    # Drive one full refresh and verify the data lands in the target. This
    # exercises the (also-patched) read path readZnodesIfNeeded with MULTI_READ
    # enabled.
    node_mr.query("INSERT INTO rmv_db_mr.src VALUES (1), (2), (3)")
    node_mr.query("SYSTEM REFRESH VIEW rmv_db_mr.mv")
    assert_eq_with_retry(
        node_mr,
        "SELECT count() FROM rmv_db_mr.tgt",
        "3\n",
        retry_count=60,
        sleep_time=1,
    )

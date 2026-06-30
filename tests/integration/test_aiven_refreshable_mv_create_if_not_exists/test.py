import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

# `node` embeds a Keeper with the CREATE_IF_NOT_EXISTS feature flag DISABLED
# (simulating plain Apache ZooKeeper). It hosts the differential evidence pair
# for patch N02.
node = cluster.add_instance(
    "node",
    main_configs=["configs/keeper_no_create_if_not_exists.xml"],
    stay_alive=True,
)

# `node_cine` embeds a Keeper with CREATE_IF_NOT_EXISTS ENABLED (the ClickHouse
# Keeper default). It hosts the regression-guard control proving the patch does
# not change the ClickHouse-Keeper path (where the idempotent CreateIfNotExists
# op is still used).
node_cine = cluster.add_instance(
    "node_cine",
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


def _setup_coordinated_rmv(instance, db):
    """
    Create a coordinated refreshable MV. In a Replicated database a non-APPEND
    refreshable MV is forced into coordinated mode
    (StorageMaterializedView.cpp: refresh_coordinated = true), so RefreshTask
    runs the keeper-backed coordination path (the `/running` ephemeral znode
    create that patch N02 fixes). A Replicated target table is required,
    otherwise the upstream sanity check rejects the combination before
    RefreshTask runs.
    """
    instance.query(
        f"CREATE DATABASE {db} ENGINE = Replicated("
        f"'/clickhouse/databases/{db}', '{{shard}}', '{{replica}}')"
    )
    instance.query(
        f"CREATE TABLE {db}.src (a UInt64) ENGINE = ReplicatedMergeTree() ORDER BY a"
    )
    instance.query(
        f"CREATE TABLE {db}.tgt (a UInt64) ENGINE = ReplicatedMergeTree() ORDER BY a"
    )
    instance.query(
        f"CREATE MATERIALIZED VIEW {db}.mv REFRESH EVERY 1 HOUR "
        f"TO {db}.tgt AS SELECT a FROM {db}.src"
    )


def test_coordinated_refresh_without_create_if_not_exists(start_cluster):
    """
    Patch N02: a coordinated refreshable MV must complete its refresh against a
    keeper that does not support CreateIfNotExists (real Apache ZooKeeper).

    Differential evidence pair. `node`'s embedded Keeper has the
    CREATE_IF_NOT_EXISTS feature flag disabled.

    Trigger reachability: a forced refresh (SYSTEM REFRESH VIEW) makes the
    coordinated RefreshTask call updateCoordinationState(running=true), which
    builds a `multi` that creates the ephemeral `/running` znode. With
    `ignore_if_exists=true` that create serializes as the CreateIfNotExists op
    (OpNum 502).

    Pre-patch behavior: the multi carries CreateIfNotExists; the keeper rejects
      the unsupported op (server-side BAD_ARGUMENTS "Unsupported operation:
      CreateIfNotExists", the equivalent of Apache ZooKeeper's marshalling
      error). The coordinated refresh can never mark itself running, the
      replicated tables can't make progress, and the workload stalls -> the
      INSERT/refresh hits TIMEOUT_EXCEEDED and the test FAILS (observed: the
      INSERT into the replicated source times out after ~120s).
    Post-patch behavior: the client sees the flag is unadvertised, emulates
      CreateIfNotExists with a plain create gated on an existence pre-check, the
      multi succeeds, the refresh completes and the rows land in the target ->
      test PASSES (in seconds).

    The observable that flips is whether the coordinated refreshable-MV workload
    completes at all against a CreateIfNotExists-less keeper (a presence/absence
    differential, stronger than an error-substring assert per
    docs/aiven/AGENTS.md §7).
    """
    db = "rmv_no_cine"
    _setup_coordinated_rmv(node, db)

    node.query(f"INSERT INTO {db}.src VALUES (1), (2), (3)")
    node.query(f"SYSTEM REFRESH VIEW {db}.mv")

    assert_eq_with_retry(
        node,
        f"SELECT count() FROM {db}.tgt",
        "3\n",
        retry_count=60,
        sleep_time=1,
    )


def test_coordinated_refresh_with_create_if_not_exists_still_works(start_cluster):
    """
    Regression-guard control. `node_cine`'s embedded Keeper has
    CREATE_IF_NOT_EXISTS enabled (the ClickHouse Keeper default). The patch must
    not change this path: the client keeps using the idempotent
    CreateIfNotExists op and the refresh completes as before.

    This case PASSES both pre- and post-patch; it is a control, not part of the
    FAIL->PASS pair. If it FAILS post-patch, the feature-flag gating regressed
    the ClickHouse-Keeper happy path (a real finding to escalate).
    """
    db = "rmv_cine"
    _setup_coordinated_rmv(node_cine, db)

    node_cine.query(f"INSERT INTO {db}.src VALUES (1), (2), (3)")
    node_cine.query(f"SYSTEM REFRESH VIEW {db}.mv")

    assert_eq_with_retry(
        node_cine,
        f"SELECT count() FROM {db}.tgt",
        "3\n",
        retry_count=60,
        sleep_time=1,
    )

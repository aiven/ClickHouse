import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    stay_alive=True,
)

TS_SETTINGS = {"allow_experimental_time_series_table": 1}


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _create_external_time_series(node):
    node.query("DROP TABLE IF EXISTS prometheus SYNC")
    node.query("DROP TABLE IF EXISTS mydata SYNC")
    node.query("DROP TABLE IF EXISTS mytags SYNC")
    node.query("DROP TABLE IF EXISTS mymetrics SYNC")

    node.query(
        "CREATE TABLE mydata (id UUID, timestamp DateTime64(3), value Float64) "
        "ENGINE = MergeTree ORDER BY (id, timestamp)"
    )
    node.query(
        "CREATE TABLE mytags ("
        "  id UUID,"
        "  metric_name LowCardinality(String),"
        "  tags Map(LowCardinality(String), String),"
        "  min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))),"
        "  max_time SimpleAggregateFunction(max, Nullable(DateTime64(3)))"
        ") ENGINE = AggregatingMergeTree ORDER BY (metric_name, id)"
    )
    # The external metrics table must NOT use LowCardinality columns:
    # StorageTimeSeries explicitly rejects that (which is the very branch that
    # crashes when the table is absent), so use plain String types here.
    node.query(
        "CREATE TABLE mymetrics ("
        "  metric_family_name String, type String, unit String, help String"
        ") ENGINE = ReplacingMergeTree ORDER BY metric_family_name"
    )
    node.query(
        "CREATE TABLE prometheus ENGINE = TimeSeries "
        "DATA mydata TAGS mytags METRICS mymetrics",
        settings=TS_SETTINGS,
    )


def test_attach_time_series_with_missing_external_metrics_must_not_crash(start_cluster):
    """Regression for the SIGSEGV in ``StorageTimeSeries::StorageTimeSeries``.

    A ``TimeSeries`` table with an *external* ``METRICS`` target dereferences
    the result of ``DatabaseCatalog::tryGetTable(...)`` without a null check and
    without an ``ATTACH``-mode guard (``src/Storages/StorageTimeSeries.cpp``).
    When the metrics target is not present at ``ATTACH`` time -- as happens
    during ``DatabaseReplicated::recoverLostReplica``, where the external target
    is not registered as a loading dependency and may not be created yet -- the
    constructor reads through a null pointer and the server crashes with
    ``SIGSEGV``.

    We reproduce the identical faulting path deterministically on a single node:
    drop the external metrics target, then restart so the startup loader
    re-``ATTACH``es the ``TimeSeries`` table with the target absent. On the buggy
    binary the server crashes during startup and does not come back up.
    """
    assert node.query("SELECT 1").strip() == "1"

    _create_external_time_series(node)

    # Remove the external METRICS target. This drop is allowed precisely because
    # the TimeSeries -> external-target dependency is not tracked (the other half
    # of the bug); on a fixed server the subsequent reattach must not crash.
    node.query("DROP TABLE mymetrics SYNC")

    # Startup loader re-ATTACHes the TimeSeries table with its metrics target
    # absent. On the buggy binary this segfaults in the constructor and the
    # server fails to start (restart_clickhouse then raises).
    node.restart_clickhouse(kill=True)

    # If we reach here the server came back up. Assert it is healthy and that no
    # fatal signal was logged while the loader attached the TimeSeries table.
    assert node.query("SELECT 1").strip() == "1"
    assert not node.contains_in_log("Received signal")
    assert not node.contains_in_log("Segmentation fault")

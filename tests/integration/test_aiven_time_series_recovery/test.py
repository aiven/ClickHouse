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
    """Attaching a ``TimeSeries`` table whose external target is gone must not crash.

    Historically the ``StorageTimeSeries`` constructor validated the columns of an
    external ``METRICS`` target by dereferencing
    ``DatabaseCatalog::tryGetTable(...)`` with no null check and on every load
    mode, so an absent target at ``ATTACH`` time crashed the server with
    ``SIGSEGV``. That is reachable during
    ``DatabaseReplicated::recoverLostReplica``, where an external target is not a
    loading dependency and may not be created yet, and on any restart after the
    target was dropped (referential-dependency enforcement is off by default).

    Reading the external target columns now happens only for a new table and
    null-checks the lookup (``normalizeTimeSeriesDefinition``), so this test pins
    that behaviour rather than a specific guard: drop the external metrics target,
    then restart so the startup loader re-``ATTACH``es the ``TimeSeries`` table
    with the target absent. The server must come back up cleanly instead of
    crash-looping.
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

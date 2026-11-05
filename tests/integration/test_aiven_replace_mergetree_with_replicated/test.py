"""
Aiven patch 004: with the server setting `aiven_replace_mergetree_with_replicated`, a table
created in a `Replicated` database with a non-replicated `*MergeTree` engine gets its
`Replicated*` engine, so its data replicates like its definition.

`node_on1` and `node_on2` enable the setting and are replicas of `rdb_on`; `node_on3` joins
`rdb_on` later to prove that a recovering replica gets the same definitions. `node_off1` and
`node_off2` keep the default and are replicas of `rdb_off`, the upstream control.

See docs/aiven/patches/004-replace-mergetree-with-replicated.md.
"""

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)


def add_node(name, replica, enabled):
    return cluster.add_instance(
        name,
        main_configs=["configs/enable_replace.xml"] if enabled else [],
        with_zookeeper=True,
        stay_alive=True,
        macros={"shard": "s1", "replica": replica},
    )


node_on1 = add_node("node_on1", "r1", True)
node_on2 = add_node("node_on2", "r2", True)
node_on3 = add_node("node_on3", "r3", True)
node_off1 = add_node("node_off1", "r1", False)
node_off2 = add_node("node_off2", "r2", False)

CREATE_RDB_ON = "CREATE DATABASE rdb_on ENGINE = Replicated('/clickhouse/databases/rdb_on', '{shard}', '{replica}')"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        for node in (node_on1, node_on2):
            node.query(CREATE_RDB_ON)
        for node in (node_off1, node_off2):
            node.query(
                "CREATE DATABASE rdb_off ENGINE = Replicated('/clickhouse/databases/rdb_off', '{shard}', '{replica}')"
            )
        yield cluster
    finally:
        cluster.shutdown()


def engine_of(node, database, table):
    return node.query(f"SELECT engine FROM system.tables WHERE database = '{database}' AND name = '{table}'").strip()


def inner_table_of(node, database, view):
    return node.query(
        f"SELECT '.inner_id.' || toString(uuid) FROM system.tables WHERE database = '{database}' AND name = '{view}'"
    ).strip()


def assert_engine_on_both(database, table, expected):
    node_on2.query(f"SYSTEM SYNC DATABASE REPLICA {database}")
    for node in (node_on1, node_on2):
        assert engine_of(node, database, table) == expected, node.name
        assert expected in node.query(f"SHOW CREATE TABLE {database}.`{table}`"), node.name


@pytest.mark.parametrize(
    "engine, columns, row",
    [
        ("MergeTree", "a Int64", "(42)"),
        ("SummingMergeTree", "a Int64", "(42)"),
        ("ReplacingMergeTree(v)", "a Int64, v UInt64", "(42, 1)"),
        ("CoalescingMergeTree", "a Int64, b Nullable(Int64)", "(42, 1)"),
    ],
)
def test_engine_converts_and_data_replicates(start_cluster, engine, columns, row):
    name = engine.split("(")[0]
    table = f"t_{name.lower()}"
    node_on1.query(f"CREATE TABLE rdb_on.{table} ({columns}) ENGINE = {engine} ORDER BY a")
    assert_engine_on_both("rdb_on", table, f"Replicated{name}")

    node_on1.query(f"INSERT INTO rdb_on.{table} VALUES {row}")
    node_on2.query(f"SYSTEM SYNC REPLICA rdb_on.{table}")
    assert node_on2.query(f"SELECT a FROM rdb_on.{table}") == "42\n"

    node_on1.query(f"DROP TABLE rdb_on.{table} SYNC")


def test_switch_off_keeps_upstream_behaviour(start_cluster):
    node_off1.query("CREATE TABLE rdb_off.t (a Int64) ENGINE = MergeTree ORDER BY a")
    node_off2.query("SYSTEM SYNC DATABASE REPLICA rdb_off")
    for node in (node_off1, node_off2):
        assert engine_of(node, "rdb_off", "t") == "MergeTree", node.name

    node_off1.query("INSERT INTO rdb_off.t VALUES (99)")
    assert node_off2.query("SELECT count() FROM rdb_off.t") == "0\n"

    node_off1.query("DROP TABLE rdb_off.t SYNC")


def test_non_replicated_database_is_not_converted(start_cluster):
    node_on1.query("CREATE DATABASE atomic_db ENGINE = Atomic")
    node_on1.query("CREATE TABLE atomic_db.t (a Int64) ENGINE = MergeTree ORDER BY a")
    assert engine_of(node_on1, "atomic_db", "t") == "MergeTree"
    node_on1.query("DROP DATABASE atomic_db SYNC")


def test_engine_without_replicated_twin_is_not_renamed(start_cluster):
    error = node_on1.query_and_get_error("CREATE TABLE rdb_on.bogus (a Int64) ENGINE = FooMergeTree ORDER BY a")
    assert "UNKNOWN_STORAGE" in error
    assert "FooMergeTree" in error and "ReplicatedFooMergeTree" not in error


def test_materialized_view_inner_table_converts(start_cluster):
    node_on1.query("CREATE TABLE rdb_on.src (a Int64) ENGINE = MergeTree ORDER BY a")
    node_on1.query("CREATE MATERIALIZED VIEW rdb_on.mv ENGINE = MergeTree ORDER BY a AS SELECT a FROM rdb_on.src")
    node_on2.query("SYSTEM SYNC DATABASE REPLICA rdb_on")
    for node in (node_on1, node_on2):
        assert engine_of(node, "rdb_on", inner_table_of(node, "rdb_on", "mv")) == "ReplicatedMergeTree", node.name

    node_on1.query("INSERT INTO rdb_on.src VALUES (5)")
    node_on2.query(f"SYSTEM SYNC REPLICA rdb_on.`{inner_table_of(node_on2, 'rdb_on', 'mv')}`")
    assert node_on2.query("SELECT a FROM rdb_on.mv") == "5\n"

    node_on1.query("DROP TABLE rdb_on.mv SYNC")
    node_on1.query("DROP TABLE rdb_on.src SYNC")


def test_refreshable_append_view_inner_table_converts(start_cluster):
    """A coordinated APPEND refresh runs on one replica; converted, its result reaches both."""
    node_on1.query(
        "CREATE MATERIALIZED VIEW rdb_on.rmv REFRESH EVERY 1 HOUR APPEND ENGINE = MergeTree ORDER BY x AS SELECT 1 AS x"
    )
    node_on2.query("SYSTEM SYNC DATABASE REPLICA rdb_on")
    for node in (node_on1, node_on2):
        assert engine_of(node, "rdb_on", inner_table_of(node, "rdb_on", "rmv")) == "ReplicatedMergeTree", node.name
        assert_eq_with_retry(node, "SELECT count() FROM rdb_on.rmv", "1\n")

    node_on1.query("DROP TABLE rdb_on.rmv SYNC")


def test_refreshable_replacing_view_keeps_upstream_check(start_cluster):
    """Upstream checks the written engine before the inner table exists, so it still refuses."""
    error = node_on1.query_and_get_error(
        "CREATE MATERIALIZED VIEW rdb_on.rmv_replace REFRESH EVERY 1 HOUR ENGINE = MergeTree ORDER BY x AS SELECT 1 AS x"
    )
    assert "replicated database, non-replicated table" in error


def test_backstop_rejects_only_unconvertible_engines(start_cluster):
    """With the upstream `database_replicated_allow_only_replicated_engine`, converted tables pass."""
    settings = {"database_replicated_allow_only_replicated_engine": 1}
    node_on1.query("CREATE TABLE rdb_on.backstop (a Int64) ENGINE = MergeTree ORDER BY a", settings=settings)
    assert engine_of(node_on1, "rdb_on", "backstop") == "ReplicatedMergeTree"
    error = node_on1.query_and_get_error("CREATE TABLE rdb_on.log_t (a Int64) ENGINE = Log", settings=settings)
    assert "Only tables with a Replicated engine" in error
    node_on1.query("DROP TABLE rdb_on.backstop SYNC")


def test_recovering_replica_gets_converted_definitions(start_cluster):
    node_on1.query("CREATE TABLE rdb_on.rec (a Int64) ENGINE = MergeTree ORDER BY a")
    node_on1.query("CREATE MATERIALIZED VIEW rdb_on.rec_mv ENGINE = MergeTree ORDER BY a AS SELECT a FROM rdb_on.rec")
    node_on1.query("INSERT INTO rdb_on.rec VALUES (3)")

    node_on3.query(CREATE_RDB_ON)
    node_on3.query("SYSTEM SYNC DATABASE REPLICA rdb_on")
    assert engine_of(node_on3, "rdb_on", "rec") == "ReplicatedMergeTree"
    assert engine_of(node_on3, "rdb_on", inner_table_of(node_on3, "rdb_on", "rec_mv")) == "ReplicatedMergeTree"
    node_on3.query("SYSTEM SYNC REPLICA rdb_on.rec")
    assert node_on3.query("SELECT a FROM rdb_on.rec") == "3\n"

    node_on1.query("DROP TABLE rdb_on.rec_mv SYNC")
    node_on1.query("DROP TABLE rdb_on.rec SYNC")


def test_attach_is_not_converted(start_cluster):
    """Runs last: it turns the switch on for `node_off1`."""
    node_off1.query("CREATE DATABASE rdb_attach ENGINE = Replicated('/clickhouse/databases/rdb_attach', 's1', 'r1')")
    node_off1.query("CREATE TABLE rdb_attach.t (a Int64) ENGINE = MergeTree ORDER BY a")
    node_off1.query("INSERT INTO rdb_attach.t VALUES (7)")
    node_off1.query("DETACH TABLE rdb_attach.t PERMANENTLY")

    node_off1.replace_config(
        "/etc/clickhouse-server/config.d/zz_enable_replace.xml",
        "<clickhouse><aiven_replace_mergetree_with_replicated>true</aiven_replace_mergetree_with_replicated></clickhouse>",
    )
    node_off1.restart_clickhouse()

    node_off1.query("ATTACH TABLE rdb_attach.t")
    assert engine_of(node_off1, "rdb_attach", "t") == "MergeTree"
    assert node_off1.query("SELECT a FROM rdb_attach.t") == "7\n"
    node_off1.query("DROP DATABASE rdb_attach SYNC")

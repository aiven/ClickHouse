"""
Aiven patch 058: with the server setting `aiven_enforce_default_replication_path`, a new
`ReplicatedMergeTree` definition must use the server's `default_replica_path` and
`default_replica_name`, while tables that already exist keep loading.

All nodes allow explicit engine arguments in `Replicated` databases
(`database_replicated_allow_replicated_engine_arguments = 1`), so every rejection here comes
from the Aiven check, not from upstream's guard. `node_on` enables the check (and patch 004);
`node_off` keeps the default; `node_legacy` creates tables with the check off and then turns
it on, like a service whose tables predate the check.

See docs/aiven/patches/058-enforce-default-replication-path.md.
"""

import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)


def add_node(name, replica, main_configs):
    return cluster.add_instance(
        name,
        main_configs=main_configs + ["configs/backups.xml"],
        user_configs=["configs/users.d/allow_engine_arguments.xml"],
        with_zookeeper=True,
        stay_alive=True,
        macros={"shard": "s1", "replica": replica},
    )


node_on = add_node("node_on", "r1", ["configs/enable_enforce.xml"])
node_off = add_node("node_off", "r2", [])
node_legacy = add_node("node_legacy", "r3", [])

DEFAULT_ARGS = "'/clickhouse/tables/{uuid}/{shard}', '{replica}'"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        for node in (node_on, node_legacy):
            name = f"rdb_{node.name}"
            node.query(f"CREATE DATABASE {name} ENGINE = Replicated('/clickhouse/databases/{name}', '{{shard}}', '{{replica}}')")
        yield cluster
    finally:
        cluster.shutdown()


def assert_rejected(error, what):
    assert "BAD_ARGUMENTS" in error, error
    assert f"Setting {what} to" in error and "is not allowed" in error, error


def test_omitted_arguments_pass(start_cluster):
    node_on.query("CREATE TABLE rdb_node_on.t_omitted (a Int64) ENGINE = ReplicatedMergeTree ORDER BY a")
    engine_full = node_on.query(
        "SELECT engine_full FROM system.tables WHERE database = 'rdb_node_on' AND name = 't_omitted' FORMAT TSVRaw"
    )
    assert DEFAULT_ARGS in engine_full, engine_full
    node_on.query("DROP TABLE rdb_node_on.t_omitted SYNC")


def test_explicit_defaults_pass(start_cluster):
    node_on.query(f"CREATE TABLE rdb_node_on.t_explicit (a Int64) ENGINE = ReplicatedMergeTree({DEFAULT_ARGS}) ORDER BY a")
    node_on.query("DROP TABLE rdb_node_on.t_explicit SYNC")


def test_foreign_path_rejected(start_cluster):
    error = node_on.query_and_get_error(
        "CREATE TABLE rdb_node_on.t_foreign (a Int64) ENGINE = ReplicatedMergeTree('/somewhere/else/{shard}', '{replica}') ORDER BY a"
    )
    assert_rejected(error, "ZooKeeper path")


def test_foreign_replica_name_rejected(start_cluster):
    error = node_on.query_and_get_error(
        "CREATE TABLE rdb_node_on.t_foreign (a Int64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', 'other_{replica}') ORDER BY a"
    )
    assert_rejected(error, "replica name")


def test_foreign_path_rejected_outside_replicated_database(start_cluster):
    error = node_on.query_and_get_error(
        "CREATE TABLE default.t_foreign (a Int64) ENGINE = ReplicatedMergeTree('/somewhere/else/{shard}', '{replica}') ORDER BY a"
    )
    assert_rejected(error, "ZooKeeper path")


def test_full_definition_attach_with_foreign_path_rejected(start_cluster):
    error = node_on.query_and_get_error(
        f"ATTACH TABLE default.t_attach UUID '{uuid.uuid4()}' (a Int64) "
        "ENGINE = ReplicatedMergeTree('/somewhere/else/{shard}', '{replica}') ORDER BY a"
    )
    assert_rejected(error, "ZooKeeper path")


def test_full_definition_attach_with_defaults_passes(start_cluster):
    """The backup tool restores tables this way: the stored definition, `CREATE` turned into `ATTACH`."""
    node_on.query(
        f"ATTACH TABLE rdb_node_on.t_reattached UUID '{uuid.uuid4()}' (a Int64) "
        f"ENGINE = ReplicatedMergeTree({DEFAULT_ARGS}) ORDER BY a"
    )
    node_on.query("SYSTEM RESTORE REPLICA rdb_node_on.t_reattached")
    node_on.query("INSERT INTO rdb_node_on.t_reattached VALUES (3)")
    assert node_on.query("SELECT a FROM rdb_node_on.t_reattached") == "3\n"
    node_on.query("DROP TABLE rdb_node_on.t_reattached SYNC")


def test_converted_mergetree_passes(start_cluster):
    """Patch 004 creates the `Replicated*` table with omitted arguments, so it takes the defaults."""
    node_on.query("CREATE TABLE rdb_node_on.t_converted (a Int64) ENGINE = MergeTree ORDER BY a")
    assert "ReplicatedMergeTree" in node_on.query("SHOW CREATE TABLE rdb_node_on.t_converted")
    node_on.query("DROP TABLE rdb_node_on.t_converted SYNC")


def test_restore_of_default_table_passes(start_cluster):
    node_on.query("CREATE TABLE rdb_node_on.t_backed (a Int64) ENGINE = ReplicatedMergeTree ORDER BY a")
    node_on.query("INSERT INTO rdb_node_on.t_backed VALUES (1)")
    node_on.query("BACKUP TABLE rdb_node_on.t_backed TO Disk('backups', 'default_table')")
    node_on.query("RESTORE TABLE rdb_node_on.t_backed AS rdb_node_on.t_restored FROM Disk('backups', 'default_table')")
    assert node_on.query("SELECT a FROM rdb_node_on.t_restored") == "1\n"
    node_on.query("DROP TABLE rdb_node_on.t_restored SYNC")
    node_on.query("DROP TABLE rdb_node_on.t_backed SYNC")


def test_switch_off_keeps_upstream_behaviour(start_cluster):
    node_off.query(
        "CREATE TABLE default.t_off (a Int64) ENGINE = ReplicatedMergeTree('/somewhere/else/{shard}/t_off', '{replica}') ORDER BY a"
    )
    node_off.query("INSERT INTO default.t_off VALUES (5)")
    assert node_off.query("SELECT a FROM default.t_off") == "5\n"
    node_off.query("DROP TABLE default.t_off SYNC")


def test_existing_tables_keep_loading(start_cluster):
    """Runs last: it turns the check on for `node_legacy`."""
    for table in ("default.t_legacy", "rdb_node_legacy.t_legacy"):
        node_legacy.query(
            f"CREATE TABLE {table} (a Int64) "
            f"ENGINE = ReplicatedMergeTree('/legacy/{{shard}}/{table}', '{{replica}}') ORDER BY a"
        )
        node_legacy.query(f"INSERT INTO {table} VALUES (7)")
    node_legacy.query("BACKUP TABLE default.t_legacy TO Disk('backups', 'legacy_table')")

    node_legacy.replace_config(
        "/etc/clickhouse-server/config.d/zz_enable_enforce.xml",
        "<clickhouse><aiven_enforce_default_replication_path>true</aiven_enforce_default_replication_path></clickhouse>",
    )
    node_legacy.restart_clickhouse()

    for table in ("default.t_legacy", "rdb_node_legacy.t_legacy"):
        assert node_legacy.query(f"SELECT a FROM {table}") == "7\n", table
        node_legacy.query(f"INSERT INTO {table} VALUES (8)")

    node_legacy.query("DETACH TABLE default.t_legacy")
    node_legacy.query("ATTACH TABLE default.t_legacy")
    assert node_legacy.query("SELECT count() FROM default.t_legacy") == "2\n"

    node_legacy.query("BACKUP TABLE default.t_legacy TO Disk('backups', 'legacy_table_after')")

    error = node_legacy.query_and_get_error(
        "RESTORE TABLE default.t_legacy AS default.t_legacy_copy FROM Disk('backups', 'legacy_table')"
    )
    assert_rejected(error, "ZooKeeper path")

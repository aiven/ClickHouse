"""
With `aiven_prohibit_tmp_table_creation`, table names starting with `.tmp` are
reserved for the temporary tables of the engine. `node1` and `node2` enable the
setting, `node_off` keeps the default and shows that the same statements are
accepted without it.

Before the setting is turned on, `node1` creates tables with such names, as a
server that ran without the setting would have. They must keep loading,
replicating and being recoverable afterwards.
"""

import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

SETTING_CONFIG = "/etc/clickhouse-server/config.d/enable_prohibit_tmp.xml"
RESERVED = "names starting with '.tmp' are reserved for internal use"

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_prohibit_tmp.xml"],
    with_zookeeper=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    stay_alive=True,
    macros={"shard": "s1", "replica": "r1"},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/enable_prohibit_tmp.xml"],
    with_zookeeper=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    stay_alive=True,
    macros={"shard": "s1", "replica": "r2"},
)
node_off = cluster.add_instance(
    "node_off",
    with_zookeeper=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    macros={"shard": "s1", "replica": "r_off"},
)

REPLICATED_DB = (
    "ENGINE = Replicated('/clickhouse/databases/{}', '{{shard}}', '{{replica}}')"
)


def set_guard(node, enabled):
    node.replace_in_config(
        SETTING_CONFIG,
        f">{'false' if enabled else 'true'}<",
        f">{'true' if enabled else 'false'}<",
    )
    node.restart_clickhouse()


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()

        set_guard(node1, False)
        node1.query(f"CREATE DATABASE rdb {REPLICATED_DB.format('rdb')}")
        for table in ("t", ".tmplegacy", ".tmplegacy_move"):
            node1.query(
                f'CREATE TABLE rdb."{table}" (a UInt64) ENGINE = ReplicatedMergeTree ORDER BY a'
            )
        node1.query('INSERT INTO rdb.".tmplegacy" SELECT number FROM numbers(3)')
        node1.query(
            'CREATE TABLE ".tmplegacy_atomic" (a UInt64) ENGINE = MergeTree ORDER BY a'
        )
        set_guard(node1, True)

        node_off.query(f"CREATE DATABASE rdb {REPLICATED_DB.format('rdb_off')}")

        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def drop_accepted_tables():
    yield
    for database in ("default", "rdb"):
        node1.query(f'DROP TABLE IF EXISTS {database}.".tmp_new" SYNC')


def assert_reserved(node, query):
    error = node.query_and_get_error(query)
    assert "BAD_ARGUMENTS" in error and RESERVED in error, error


def tables_with_prefix(node, database, prefix):
    return node.query(
        f"SELECT name FROM system.tables WHERE database = '{database}' "
        f"AND startsWith(name, '{prefix}') ORDER BY name"
    ).split()


@pytest.mark.parametrize(
    "query",
    [
        'CREATE TABLE ".tmp_new" (a UInt64) ENGINE = MergeTree ORDER BY a',
        'CREATE TABLE ".tmp_new" ENGINE = MergeTree ORDER BY a AS SELECT 1 AS a',
        'CREATE OR REPLACE TABLE ".tmp_new" (a UInt64) ENGINE = MergeTree ORDER BY a',
        'CREATE VIEW ".tmp_new" AS SELECT 1 AS a',
        "ATTACH TABLE \".tmp_new\" UUID '{uuid}' (a UInt64) ENGINE = MergeTree ORDER BY a",
    ],
    ids=[
        "create",
        "create_as_select",
        "create_or_replace",
        "view",
        "attach_with_definition",
    ],
)
def test_new_definition_rejected(query):
    assert_reserved(node1, query.format(uuid=uuid.uuid4()))
    assert tables_with_prefix(node1, "default", ".tmp") == [".tmplegacy_atomic"]

    node_off.query(query.format(uuid=uuid.uuid4()))
    assert tables_with_prefix(node_off, "default", ".tmp") == [".tmp_new"]
    node_off.query('DROP TABLE ".tmp_new" SYNC')


def test_new_definition_in_replicated_database_rejected():
    query = (
        'CREATE TABLE rdb.".tmp_new" (a UInt64) ENGINE = ReplicatedMergeTree ORDER BY a'
    )
    assert_reserved(node1, query)
    assert ".tmp_new" not in tables_with_prefix(node1, "rdb", ".tmp")

    node_off.query(query)
    node_off.query('DROP TABLE rdb.".tmp_new" SYNC')


def test_rename_and_exchange_in_replicated_database_rejected():
    for query in (
        'RENAME TABLE rdb.t TO rdb.".tmp_new"',
        'EXCHANGE TABLES rdb.t AND rdb.".tmplegacy"',
        'EXCHANGE TABLES rdb.".tmplegacy" AND rdb.t',
    ):
        assert_reserved(node1, query)
    assert node1.query('SELECT count() FROM rdb.".tmplegacy"') == "3\n"
    assert node1.query("SELECT count() FROM rdb.t") == "0\n"

    node_off.query(
        "CREATE TABLE rdb.t (a UInt64) ENGINE = ReplicatedMergeTree ORDER BY a"
    )
    node_off.query('RENAME TABLE rdb.t TO rdb.".tmp_new"')
    node_off.query(
        "CREATE TABLE rdb.t (a UInt64) ENGINE = ReplicatedMergeTree ORDER BY a"
    )
    node_off.query('EXCHANGE TABLES rdb.t AND rdb.".tmp_new"')
    node_off.query('EXCHANGE TABLES rdb.".tmp_new" AND rdb.t')
    assert tables_with_prefix(node_off, "rdb", "") == [".tmp_new", "t"]
    node_off.query('DROP TABLE rdb.".tmp_new" SYNC')
    node_off.query("DROP TABLE rdb.t SYNC")


def test_existing_tables_keep_working():
    assert node1.query('SELECT count() FROM rdb.".tmplegacy"') == "3\n"

    node1.query(
        'CREATE TABLE IF NOT EXISTS ".tmplegacy_atomic" (a UInt64) ENGINE = MergeTree ORDER BY a'
    )
    node1.query('INSERT INTO ".tmplegacy_atomic" VALUES (1)')
    assert node1.query('SELECT count() FROM ".tmplegacy_atomic"') == "1\n"

    node1.query('RENAME TABLE rdb.".tmplegacy_move" TO rdb.legacy_moved')
    assert "legacy_moved" in tables_with_prefix(node1, "rdb", "legacy")


def test_new_replica_recovers_existing_tables():
    node2.query(f"CREATE DATABASE rdb {REPLICATED_DB.format('rdb')}")
    node2.query("SYSTEM SYNC DATABASE REPLICA rdb")
    assert tables_with_prefix(node2, "rdb", ".tmp") == [".tmplegacy"]
    node2.query('SYSTEM SYNC REPLICA rdb.".tmplegacy"')
    assert node2.query('SELECT count() FROM rdb.".tmplegacy"') == "3\n"


def test_engine_temporaries_exempt():
    for value in (1, 2):
        node1.query(
            "CREATE OR REPLACE TABLE cor (a UInt64) ENGINE = MergeTree ORDER BY a "
            f"AS SELECT {value} AS a"
        )
        assert node1.query("SELECT a FROM cor") == f"{value}\n"
    node1.query("CREATE TABLE ctas ENGINE = MergeTree ORDER BY a AS SELECT 3 AS a")
    assert node1.query("SELECT a FROM ctas") == "3\n"

    for _ in range(2):
        node1.query(
            "CREATE OR REPLACE TABLE rdb.cor (a UInt64) ENGINE = ReplicatedMergeTree ORDER BY a"
        )
    node2.query("SYSTEM SYNC DATABASE REPLICA rdb")
    assert "cor" in tables_with_prefix(node2, "rdb", "cor")

    for database in ("default", "rdb"):
        engine = "ReplicatedMergeTree" if database == "rdb" else "MergeTree"
        node1.query(
            f"CREATE MATERIALIZED VIEW {database}.mv REFRESH EVERY 1 HOUR "
            f"(a UInt64) ENGINE = {engine} ORDER BY a "
            "AS SELECT number AS a FROM numbers(3)"
        )
        node1.query(f"SYSTEM REFRESH VIEW {database}.mv")
        node1.query(f"SYSTEM WAIT VIEW {database}.mv")
        assert node1.query(f"SELECT count() FROM {database}.mv") == "3\n"

    node2.query("SYSTEM SYNC DATABASE REPLICA rdb")
    assert_eq_with_retry(node2, "SELECT count() FROM rdb.mv", "3\n")

    assert tables_with_prefix(node1, "default", ".tmp") == [".tmplegacy_atomic"]
    for node in (node1, node2):
        assert tables_with_prefix(node, "rdb", ".tmp") == [".tmplegacy"]

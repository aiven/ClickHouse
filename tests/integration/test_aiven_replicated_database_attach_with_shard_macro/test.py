"""
Aiven patch 006: the `{shard}` macro must resolve when the startup loader attaches
a `ReplicatedMergeTree` table that lives in a `Replicated` database.

`TableZnodeInfo::resolve` keeps `{shard}` and `{replica}` unexpanded in the table's
metadata file on purpose, so the file stays copyable between replicas. Every ATTACH
therefore resolves them again. The shard and replica names come from the containing
`Replicated` database only inside a DDL context, and the startup loader attaches
outside one, so on restart `{shard}` fell through to the server's configured macros.

Neither node defines a `shard` macro. That is the managed-service shape: the shard
name belongs to each `Replicated` database, so a server hosting several of them has no
single correct global value.

Both cases assert on the restart itself. With `async_load_databases` disabled, a failed
ATTACH fails the startup load job, and `restart_clickhouse` raises instead of returning
a queryable server.

See docs/aiven/patches/006-replicated-database-attach-with-shard-macro.md.
"""

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

# Deliberately no "shard" macro, see the module docstring. The remote database disk is
# disabled because enabling it makes the harness add a global `shard` macro, which hides
# the failure: the attach then falls back to that macro and succeeds on an unpatched build.
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/no_async_load.xml"],
    with_zookeeper=True,
    with_remote_database_disk=False,
    stay_alive=True,
    macros={"replica": "r1"},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/no_async_load.xml"],
    with_zookeeper=True,
    with_remote_database_disk=False,
    stay_alive=True,
    macros={"replica": "r2"},
)
# Separate from node1 and node2 so that a restart failing in one case cannot fail the other.
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/no_async_load.xml"],
    with_zookeeper=True,
    with_remote_database_disk=False,
    stay_alive=True,
    macros={"replica": "r3"},
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        for node in (node1, node2, node3):
            assert node.query("SELECT count() FROM system.macros WHERE macro = 'shard'") == "0\n", (
                f"{node.name} has a global 'shard' macro, so this test cannot reproduce the failure"
            )
        yield cluster
    finally:
        cluster.shutdown()


def test_restart_attaches_replicated_table_with_shard_macro(start_cluster):
    """The fixed path: the shard name comes from the `Replicated` database."""
    db_engine = "Replicated('/clickhouse/test/aiven_006_db/{database}', 'aiven_shard_a', '{replica}')"
    for node in (node1, node2):
        node.query(f"CREATE DATABASE testdb ENGINE = {db_engine}")

    # No engine arguments: the path then comes from `default_replica_path`,
    # '/clickhouse/tables/{uuid}/{shard}', which is what keeps `{shard}` in the metadata.
    node1.query("CREATE TABLE testdb.t (x UInt32) ENGINE = ReplicatedMergeTree ORDER BY x")
    node1.query("INSERT INTO testdb.t VALUES (42)")
    assert_eq_with_retry(node2, "SELECT count() FROM testdb.t", "1\n")

    node1.restart_clickhouse(kill=True)
    node2.restart_clickhouse(kill=True)

    for node in (node1, node2):
        assert_eq_with_retry(node, "SELECT * FROM testdb.t", "42\n", retry_count=20, sleep_time=1)


def test_restart_attaches_replicated_table_in_plain_database(start_cluster):
    """The skip path: a replicated *table* in a database that is not `Replicated`.

    Startup attaches this table too, so a guard on `query.attach` alone admits it.
    The 26.3 shape did exactly that and then downcast the database to
    `DatabaseReplicated` with `assert_cast`, which is undefined behaviour. That
    `assert_cast` is unchecked on every build type, so the bad cast does not fail
    the restart and this case cannot tell the 26.3 shape from the fix. It guards
    the skip path: the table must still attach, taking its macros from the server
    config, which is the right source for a database without a shard.
    """
    node3.query("CREATE DATABASE plaindb ENGINE = Atomic")
    node3.query(
        "CREATE TABLE plaindb.t (x UInt32) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/aiven_006_plain', '{replica}') "
        "ORDER BY x"
    )
    node3.query("INSERT INTO plaindb.t VALUES (7)")

    node3.restart_clickhouse(kill=True)

    assert_eq_with_retry(node3, "SELECT * FROM plaindb.t", "7\n", retry_count=20, sleep_time=1)

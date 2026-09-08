"""
Aiven patch 006 — the {shard} macro must resolve when the startup loader
attaches a ReplicatedMergeTree table.

`TableZnodeInfo::resolve` keeps `{shard}` and `{replica}` unexpanded in the
table's metadata file on purpose, so metadata stays copyable between replicas.
The cost is that every ATTACH has to resolve them again, and the startup loader
attaches outside any DDL context. Before the patch the shard name was taken from
the containing DatabaseReplicated only when `is_replicated_database` held, and
that flag requires `isDDLOrOnClusterInternal()` — true for DDL-log and ON
CLUSTER work, false for the loader's local ATTACH. So on restart `{shard}` fell
through to the configured macros, and a deployment that has no global <shard>
failed to start.

Neither node here defines a <shard> macro, which is the customer's shape: the
shard name is per-database, so one server hosting several Replicated databases
has no single correct global value, and the engine argument is authoritative.

Both cases below assert on the restart itself. With `async_load_databases`
false, a failed ATTACH fails the startup load job, and the helper's
`restart_clickhouse` raises `Exception: Cannot start ClickHouse` rather than
returning a queryable server.

See docs/aiven/patches/006-replicated-database-attach-with-shard-macro.md.
"""

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

# Deliberately no "shard" macro — see the module docstring.
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/no_async_load.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"replica": "r1"},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/no_async_load.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"replica": "r2"},
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_restart_attaches_replicated_table_with_shard_macro(start_cluster):
    """The patched path: the shard name comes from the Replicated database."""
    db_engine = (
        "Replicated('/clickhouse/test/aiven_006_db/{database}', "
        "'aiven_shard_a', '{replica}')"
    )

    for node in (node1, node2):
        node.query(f"CREATE DATABASE testdb ENGINE = {db_engine}")

    # No explicit zookeeper_path/replica_name: inside a Replicated database that
    # needs database_replicated_allow_replicated_engine_arguments and is the
    # discouraged shape. Letting the engine synthesize the path from
    # default_replica_path ('/clickhouse/tables/{uuid}/{shard}') is what keeps
    # {shard} in it, which is the macro this patch is about.
    node1.query("CREATE TABLE testdb.t (x UInt32) ENGINE = ReplicatedMergeTree ORDER BY x")
    node1.query("INSERT INTO testdb.t VALUES (42)")

    assert_eq_with_retry(node1, "SELECT count() FROM testdb.t", "1\n")
    assert_eq_with_retry(node2, "SELECT count() FROM testdb.t", "1\n")

    node1.restart_clickhouse(kill=True)
    node2.restart_clickhouse(kill=True)

    for node in (node1, node2):
        assert_eq_with_retry(node, "SELECT * FROM testdb.t", "42\n", retry_count=20, sleep_time=1)


def test_restart_attaches_replicated_table_in_plain_database(start_cluster):
    """The guard: a Replicated*table* in a non-Replicated *database*.

    This case is why the port does not simply widen the condition to
    `is_replicated_database || query.attach`, as the 26.3 patch did. On that
    shape `query.attach` alone admits this table, and the block then reaches
    `getReplicatedDatabaseShardName`, whose `assert_cast<const DatabaseReplicated *>`
    has no type check — undefined behaviour in a release build, and
    `throwBadAssertCast` here, which fails the startup load and so the restart.

    The path deliberately has no {shard}: there is no shard to resolve for a
    plain database, and the configured macros are the right source for it.
    """
    node1.query("CREATE DATABASE plaindb")
    node1.query(
        "CREATE TABLE plaindb.t (x UInt32) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/aiven_006_plain', '{replica}') "
        "ORDER BY x"
    )
    node1.query("INSERT INTO plaindb.t VALUES (7)")

    node1.restart_clickhouse(kill=True)

    assert_eq_with_retry(node1, "SELECT * FROM plaindb.t", "7\n", retry_count=20, sleep_time=1)

"""
Aiven patch 006 — regression test for:

  "DB::Exception: No macro 'shard'" thrown by the clickhouse-server startup
  loader when re-attaching ReplicatedMergeTree tables in a DatabaseReplicated
  database whose ZooKeeper path uses the {shard} macro, in a deployment where
  the server config does NOT provide a global <shard> macro (the per-database
  shard name from the DatabaseReplicated engine arguments is the authoritative
  source).

Trigger reproduced here:
  1. Two-node cluster, neither node has a config <shard> macro.
  2. DatabaseReplicated created with a literal shard name ('aiven_shard_a');
     DDL log propagation makes the CREATE TABLE execute as SECONDARY_QUERY
     on both nodes, so TableZnodeInfo::resolve enters the
     is_replicated_database branch at CREATE time and the table's znode
     lives at /clickhouse/tables/<uuid>/aiven_shard_a/...
  3. Server restart on either node => loader issues local ATTACH as
     INITIAL_QUERY (NOT from DDL log) with create.attach=true and
     is_replicated_database=false.

Pre-patch (TableZnodeInfo.cpp:58 reads `if (is_replicated_database)`):
  - info.shard is left empty.
  - Macros::expand("{shard}", info) finds no 'shard' macro
    anywhere => raises Code: 62 ("No macro 'shard'") and the loader
    leaves the table missing/broken. A subsequent SELECT fails with
    "Code: 60. Unknown table" or "Code: 242. Table is in readonly mode".

Post-patch (TableZnodeInfo.cpp:58 reads `if (is_replicated_database || query.attach)`):
  - The branch fires on the loader-issued ATTACH.
  - info.shard is populated from the DatabaseReplicated instance
    via getReplicatedDatabaseShardName(database), yielding "aiven_shard_a".
  - Macros::expand succeeds; ATTACH succeeds; table is queryable.

The assertion is therefore: after restart, SELECT from the table returns
the inserted row. Pre-patch this fails; post-patch this passes.

See docs/aiven/patches/006-replicated-database-attach-with-shard-macro.md
for the original source SHA (22e03c9d9d6cf9929aec824b724e09ea5c58653f),
upstream-drift analysis, and the stateless-test impasse that motivated
this integration-level test.
"""

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

# IMPORTANT: macros for each node deliberately OMITS "shard". This is the
# customer's deployment shape — they rely on the DatabaseReplicated engine
# arguments to supply the shard name, not on a global <shard> in the server
# config. Pre-patch this is the breaking shape on server restart.
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
    db_engine = (
        "Replicated('/clickhouse/test/aiven_006_db/{database}', "
        "'aiven_shard_a', '{replica}')"
    )

    for node in (node1, node2):
        node.query(f"CREATE DATABASE testdb ENGINE = {db_engine}")

    # IMPORTANT: do NOT pass explicit zookeeper_path / replica_name to
    # ReplicatedMergeTree here. Inside a Replicated database, that requires
    # the setting `database_replicated_allow_replicated_engine_arguments` and
    # is the discouraged shape. Letting the engine synthesize from
    # `default_replica_path` (default value `/clickhouse/tables/{uuid}/{shard}`)
    # and `default_replica_name` (default `{replica}`) keeps `{shard}` in the
    # synthesized path — which is the exact macro the patch addresses.
    node1.query(
        "CREATE TABLE testdb.t (x UInt32) "
        "ENGINE = ReplicatedMergeTree "
        "ORDER BY x"
    )

    node1.query("INSERT INTO testdb.t VALUES (42)")

    assert_eq_with_retry(node1, "SELECT count() FROM testdb.t", "1\n")
    assert_eq_with_retry(node2, "SELECT count() FROM testdb.t", "1\n")

    node1.restart_clickhouse(kill=True)
    node2.restart_clickhouse(kill=True)

    assert_eq_with_retry(
        node1,
        "SELECT * FROM testdb.t",
        "42\n",
        retry_count=20,
        sleep_time=1,
    )
    assert_eq_with_retry(
        node2,
        "SELECT * FROM testdb.t",
        "42\n",
        retry_count=20,
        sleep_time=1,
    )

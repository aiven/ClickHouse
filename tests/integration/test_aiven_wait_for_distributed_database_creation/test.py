"""
Aiven patch 072 — "Wait for distributed database creation".

createReplicatedDatabaseByClient previously fire-and-forgot the internal
CREATE DATABASE ... ON CLUSTER query: it never drained the result pipeline, so
errors raised while the DDL executed on cluster hosts were silently swallowed and
the caller saw success. Patch 072 sets distributed_ddl_output_mode='throw',
captures the BlockIO and drives the pipeline to completion, so the caller (1)
waits until creation finishes on all replicas and (2) receives forwarded errors.

Single node with Keeper, configured for the indirect-create feature (avnadmin +
cluster_db). Evidence-of-causation case: test_create_error_forwarded induces a
server-side CREATE failure (name collision detected WHILE the ON CLUSTER DDL runs,
not by a client-side pre-check) and asserts the error reaches the caller. Pre-patch
the pipeline is never drained, so the caller sees success and the test FAILS.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/wait_distributed_db.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "the_shard", "replica": "r1"},
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        # The reference Replicated database named by `cluster_database`, created by the
        # default (admin) user via the normal path. Its `{shard}` macro is what
        # createReplicatedDatabaseByClient reuses for the implicit ON CLUSTER target.
        node.query(
            "CREATE DATABASE cluster_db ENGINE = "
            "Replicated('/clickhouse/databases/cluster_db', '{shard}', '{replica}')"
        )
        node.query("CREATE USER avnadmin IDENTIFIED WITH no_password")

        yield cluster
    finally:
        cluster.shutdown()


def test_create_error_forwarded(start_cluster):
    # Pre-create a database whose name collides with the one avnadmin will create.
    # avnadmin's CREATE DATABASE goes through the indirect path, which builds
    # `CREATE DATABASE collide_db ON CLUSTER cluster_db ENGINE = Replicated(...)`
    # (no IF NOT EXISTS). The "already exists" failure is raised WHILE that DDL runs
    # on the cluster host, so it lands in the distributed-DDL result pipeline.
    #
    #   post-patch: pipeline drained with distributed_ddl_output_mode='throw'
    #               => the error is forwarded to avnadmin (this assertion holds).
    #   pre-patch:  pipeline never drained => avnadmin's CREATE returns success,
    #               the error is lost => query_and_get_error raises => test FAILS.
    node.query("CREATE DATABASE collide_db")

    err = node.query_and_get_error("CREATE DATABASE collide_db", user="avnadmin")
    assert "collide_db" in err and "already exists" in err.lower(), err

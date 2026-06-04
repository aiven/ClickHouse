#!/usr/bin/env python3

"""
Regression test for the Aiven "zero copy fixes" patch (dossier 048, Leg 2).

Background
----------
With zero-copy replication, the part-level lock znode lives under a
*table-shared-id*-keyed path:

    /clickhouse/zero_copy/zero_copy_<disk>/<table_shared_id>/<part_name>/...

`table_shared_id` is the table's UUID. In a `Replicated` database the same
table has the *same* UUID on every shard, so two different shards share this
zero-copy lock namespace. When two shards commit a part with the *same name*
(e.g. the first part `all_0_0_0` in a partition) at the same time, both commit
transactions try to create the shared ancestor
`.../zero_copy_<disk>/<uuid>/all_0_0_0`.

The commit path builds that create op via
`checkExistsAndGetCreateAncestorsOps`, which does a read-side `exists()` check
and then appends a *non-idempotent* hard `CREATE`. Two shards that both observe
the ancestor as absent both append a hard `CREATE`; whichever multi runs second
gets `ZNODEEXISTS`. The sink treats any failure inside the shared-lock op range
as `LOGICAL_ERROR` ("Creating shared lock for part ... has failed ... It's a
bug. No race is possible since it is a new part."), with no retry.

The patch pre-creates that ancestor idempotently
(`createAncestors` + `createIfNotExists`) before building the commit ops, so the
`exists()` check observes it as present on both shards and no racy hard `CREATE`
is appended.

Determinism
-----------
The race window between the `exists()` check and the commit multi is normally
sub-millisecond. We widen it deterministically with the EXISTING test failpoint
`rmt_delay_commit_part`, which sleeps 5s right before the commit multi (after
the ops -- including the `exists()` check -- are already built). With the
failpoint enabled on both shards and the two inserts launched concurrently,
both shards pass their `exists()` check before either multi runs, so the
collision is reproduced reliably pre-patch.
"""

import threading

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/storage_conf.xml"],
    macros={"shard": "s1", "replica": "r1"},
    with_minio=True,
    with_zookeeper=True,
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/storage_conf.xml"],
    macros={"shard": "s2", "replica": "r2"},
    with_minio=True,
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_concurrent_same_name_part_commit_across_shards(started_cluster):
    for node in [node1, node2]:
        node.query("DROP DATABASE IF EXISTS zc_race SYNC")

    # Two shards of the same Replicated database -> the table gets the SAME
    # UUID on both shards, hence the same zero-copy table_shared_id.
    for node in [node1, node2]:
        node.query(
            """
            CREATE DATABASE zc_race
            ENGINE = Replicated('/test/zc_race_db', '{shard}', '{replica}')
            """
        )

    # No PARTITION BY -> the single partition is "all"; the first commit on each
    # shard produces a part named all_0_0_0, so the shared zero-copy ancestor
    # .../zero_copy_s3/<uuid>/all_0_0_0 collides across shards.
    node1.query(
        """
        CREATE TABLE zc_race.tbl (id UInt32)
        ENGINE = ReplicatedMergeTree ORDER BY id
        SETTINGS storage_policy = 's3', allow_remote_fs_zero_copy_replication = 1
        """
    )

    for node in [node1, node2]:
        assert (
            node.query_with_retry(
                "SELECT count() FROM system.tables WHERE database = 'zc_race' AND name = 'tbl'",
                check_callback=lambda x: x.strip() == "1",
            ).strip()
            == "1"
        )

    table_uuid = node1.query(
        "SELECT uuid FROM system.tables WHERE database = 'zc_race' AND name = 'tbl'"
    ).strip()

    # Widen the commit window so both shards reliably pass their exists() check
    # before either commit multi runs.
    for node in [node1, node2]:
        node.query("SYSTEM ENABLE FAILPOINT rmt_delay_commit_part")

    errors = {}

    def insert(node):
        try:
            node.query("INSERT INTO zc_race.tbl VALUES (1)")
        except Exception as e:  # noqa: BLE001
            errors[node.name] = str(e)

    threads = [threading.Thread(target=insert, args=(node,)) for node in [node1, node2]]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for node in [node1, node2]:
        node.query("SYSTEM DISABLE FAILPOINT rmt_delay_commit_part")

    # Pre-patch: the shard whose commit multi runs second hits ZNODEEXISTS on the
    # shared ancestor and the sink raises LOGICAL_ERROR with this exact message.
    assert not errors, (
        "concurrent same-name part commit across shards failed: "
        + "; ".join(f"{k}: {v}" for k, v in errors.items())
    )

    for node in [node1, node2]:
        assert node.query("SELECT count() FROM zc_race.tbl").strip() == "1"

    # The shared part-level zero-copy lock znode must have materialized.
    assert (
        node1.query(
            f"""
            SELECT count() FROM system.zookeeper
            WHERE path = '/clickhouse/zero_copy/zero_copy_s3/{table_uuid}'
              AND name = 'all_0_0_0'
            """
        ).strip()
        == "1"
    )

    for node in [node1, node2]:
        node.query("DROP DATABASE IF EXISTS zc_race SYNC")

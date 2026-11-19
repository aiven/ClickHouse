"""
Patch 066: refreshable materialized views in a sharded Replicated database.

`shard1` and `shard2` are two shards of one Replicated database, one replica each. Every DDL in
the database runs on both shards, so a refresh that ends with its own EXCHANGE also replaces the
target table on the other shard, with a table that is empty there. With 066 one replica leads the
refresh, every shard inserts its own rows into the same new table, and the leader does one
EXCHANGE after all shards have finished.

Keeper feature flags are left to the harness's random draw: with 050 coordination works without
MULTI_READ and CREATE_IF_NOT_EXISTS.
"""

import re

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

shard1 = cluster.add_instance(
    "shard1",
    main_configs=[
        "configs/default_replica_path.xml",
        "configs/allow_zookeeper_write.xml",
    ],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "shard1", "replica": "replica1"},
)
shard2 = cluster.add_instance(
    "shard2",
    main_configs=["configs/default_replica_path.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "shard2", "replica": "replica1"},
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_sharded_database(name):
    for node in (shard1, shard2):
        node.query(
            f"CREATE DATABASE {name} ENGINE = Replicated('/clickhouse/databases/{name}', '{{shard}}', '{{replica}}')"
        )
    # CREATE TABLE runs on both shards through the database's DDL queue, so create each table once.
    for table in ("src", "tgt"):
        shard1.query(
            f"CREATE TABLE {name}.{table} (a UInt64) ENGINE = ReplicatedMergeTree() ORDER BY a"
        )
    shard2.query(f"SYSTEM SYNC DATABASE REPLICA {name}")
    # An INSERT stays in the shard it was sent to.
    shard1.query(f"INSERT INTO {name}.src VALUES (1), (2), (3)")
    shard2.query(f"INSERT INTO {name}.src VALUES (10), (20), (30)")


def refresh_and_wait(db, node):
    # One refresh, then make sure both shards have applied its DDL (EXCHANGE, DROP).
    node.query(f"SYSTEM REFRESH VIEW {db}.mv")
    node.query(f"SYSTEM WAIT VIEW {db}.mv")
    for n in (shard1, shard2):
        n.query(f"SYSTEM SYNC DATABASE REPLICA {db}")


def rows(node, db):
    return node.query(f"SELECT a FROM {db}.tgt ORDER BY a")


def test_sharded_refresh_keeps_each_shards_rows(start_cluster):
    # EMPTY: no refresh until we ask for one, so each check sees exactly one finished refresh.
    # Without 066 shard1's refresh EXCHANGEs `tgt` on shard2 too, leaving it empty there.
    create_sharded_database("sharded_rmv")
    shard1.query(
        "CREATE MATERIALIZED VIEW sharded_rmv.mv REFRESH EVERY 1 YEAR "
        "TO sharded_rmv.tgt EMPTY AS SELECT a FROM sharded_rmv.src"
    )
    shard2.query("SYSTEM SYNC DATABASE REPLICA sharded_rmv")

    refresh_and_wait("sharded_rmv", shard1)
    assert rows(shard1, "sharded_rmv") == "1\n2\n3\n"
    assert rows(shard2, "sharded_rmv") == "10\n20\n30\n"

    # A second refresh, started from the other shard, sees new rows on both.
    shard1.query("INSERT INTO sharded_rmv.src VALUES (4)")
    shard2.query("INSERT INTO sharded_rmv.src VALUES (40)")
    refresh_and_wait("sharded_rmv", shard2)
    assert rows(shard1, "sharded_rmv") == "1\n2\n3\n4\n"
    assert rows(shard2, "sharded_rmv") == "10\n20\n30\n40\n"

    for node in (shard1, shard2):
        assert (
            node.query(
                "SELECT exception FROM system.view_refreshes WHERE database = 'sharded_rmv' AND view = 'mv'"
            )
            == "\n"
        )

    shard1.query("DROP DATABASE sharded_rmv SYNC")
    shard2.query("DROP DATABASE sharded_rmv SYNC")


def test_sharded_append_refresh(start_cluster):
    # APPEND has no EXCHANGE; with 066 every shard must still append its own rows.
    create_sharded_database("sharded_append")
    shard1.query(
        "CREATE MATERIALIZED VIEW sharded_append.mv REFRESH EVERY 1 YEAR APPEND "
        "TO sharded_append.tgt EMPTY AS SELECT a FROM sharded_append.src"
    )
    shard2.query("SYSTEM SYNC DATABASE REPLICA sharded_append")

    refresh_and_wait("sharded_append", shard1)
    assert rows(shard1, "sharded_append") == "1\n2\n3\n"
    assert rows(shard2, "sharded_append") == "10\n20\n30\n"

    refresh_and_wait("sharded_append", shard2)
    assert rows(shard1, "sharded_append") == "1\n1\n2\n2\n3\n3\n"
    assert rows(shard2, "sharded_append") == "10\n10\n20\n20\n30\n30\n"

    shard1.query("DROP DATABASE sharded_append SYNC")
    shard2.query("DROP DATABASE sharded_append SYNC")


def _sql_string(value):
    return (
        "'"
        + value.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n")
        + "'"
    )


# The fields this version writes after `attempt_number`, rewritten into the order an older
# version wrote them in.
CURRENT_TAIL = (
    r"\nattempt_number: ([^\n]*)\nrandomness: ([^\n]*)\nrefresh_running: [^\n]*\n"
    r"refresh_dir: [^\n]*\nlast_success_end_time_ns: [^\n]*\nlast_success_dependencies: [^\n]*\n$"
)
OLDER_TAILS = {
    # 25.8: `refresh_dir` and `target_table_id` before `randomness`, no `refresh_running`.
    "25.8": r"\nattempt_number: \1\nrefresh_dir: _refresh_1700000000000\ntarget_table_id: \nrandomness: \2\n",
    # 26.3: `refresh_dir` and `target_table_id` at the end.
    "26.3": r"\nattempt_number: \1\nrandomness: \2\nrefresh_running: 0\nrefresh_dir: \ntarget_table_id: \n",
}


@pytest.mark.parametrize("version", ["25.8", "26.3"])
def test_znode_written_by_older_version(start_cluster, version):
    # After an upgrade the view must read the znode an older server wrote, and keep refreshing.
    db = "older_znode_" + version.replace(".", "_")
    shard1.query(
        f"CREATE DATABASE {db} ENGINE = Replicated('/clickhouse/databases/{db}', 's1', 'r1')"
    )
    shard1.query(
        f"CREATE TABLE {db}.src (a UInt64) ENGINE = ReplicatedMergeTree() ORDER BY a"
    )
    shard1.query(
        f"CREATE TABLE {db}.tgt (a UInt64) ENGINE = ReplicatedMergeTree() ORDER BY a"
    )
    shard1.query(f"INSERT INTO {db}.src VALUES (1), (2), (3)")
    shard1.query(
        f"CREATE MATERIALIZED VIEW {db}.mv REFRESH EVERY 1 YEAR "
        f"TO {db}.tgt AS SELECT a FROM {db}.src"
    )
    assert_eq_with_retry(shard1, f"SELECT count() FROM {db}.tgt", "3\n")
    assert_eq_with_retry(
        shard1,
        f"SELECT status FROM system.view_refreshes WHERE database = '{db}' AND view = 'mv'",
        "Scheduled\n",
    )

    uuid = shard1.query(
        f"SELECT uuid FROM system.tables WHERE database = '{db}' AND name = 'mv'"
    ).strip()
    parent = f"/clickhouse/tables/{uuid}"
    name = "mv_refresh_qrLUb5TgIJ"

    def read_znode():
        # TSVRaw adds a newline after the value.
        return shard1.query(
            f"SELECT value FROM system.zookeeper WHERE path = '{parent}' AND name = '{name}' FORMAT TSVRaw"
        )[:-1]

    value = read_znode()
    older, n = re.subn(CURRENT_TAIL, OLDER_TAILS[version], value)
    assert n == 1, value
    shard1.query(
        f"INSERT INTO system.zookeeper (name, path, value) VALUES ('{name}', '{parent}', {_sql_string(older)})"
    )
    assert read_znode() == older

    shard1.query(f"INSERT INTO {db}.src VALUES (4), (5)")
    shard1.query(f"SYSTEM REFRESH VIEW {db}.mv")

    assert_eq_with_retry(shard1, f"SELECT count() FROM {db}.tgt", "5\n")
    assert_eq_with_retry(
        shard1,
        f"SELECT exception FROM system.view_refreshes WHERE database = '{db}' AND view = 'mv'",
        "\n",
    )
    # The refresh wrote the znode back in the current order.
    assert re.search(CURRENT_TAIL, read_znode()), read_znode()
    assert "target_table_id" not in read_znode()

    shard1.query(f"DROP DATABASE {db} SYNC")

"""
Aiven patch 047: the server settings `aiven_max_bytes_to_merge_override` and
`aiven_max_bytes_to_mutate_override` cap the source size of the merges and mutations a server
assigns and executes, without touching table definitions.

`capped` starts with a 5 MiB merge cap and a 2 MiB mutation cap; `free` has none. Parts are
uncompressed (`CODEC(NONE)`) with a fixed payload, so their sizes are predictable: six ~2 MiB
parts can merge pairwise under the cap but never into one part. Merges are driven by the
background scheduler, not `OPTIMIZE`, which does not apply size limits.

See docs/aiven/patches/047-max-bytes-merge-mutate-override.md.
"""

import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node_capped = cluster.add_instance("capped", main_configs=["configs/caps.xml"], with_zookeeper=True, stay_alive=True, macros={"replica": "capped"})
node_free = cluster.add_instance("free", with_zookeeper=True, stay_alive=True, macros={"replica": "free"})

CAPS_CONFIG = "/etc/clickhouse-server/config.d/caps.xml"
TABLE_SETTINGS = "merge_selecting_sleep_ms = 1000, max_bytes_to_merge_at_max_space_in_pool = 1000000000, min_bytes_for_wide_part = 0"
ROWS_PER_PART = 2048
SIZE_REASON = "is greater than the current maximum"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def active_parts(node, table):
    return int(node.query(f"SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '{table}' AND active"))


def insert_parts(node, table, count):
    for i in range(count):
        node.query(f"INSERT INTO {table} SELECT number + {i * ROWS_PER_PART}, repeat('a', 1024) FROM numbers({ROWS_PER_PART})")


def wait_parts(node, table, expected):
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '{table}' AND active",
        f"{expected}\n",
        retry_count=60,
        sleep_time=1,
    )


def server_setting(node, name):
    return node.query(f"SELECT value, changeable_without_restart FROM system.server_settings WHERE name = '{name}'")


def test_server_settings(start_cluster):
    assert server_setting(node_capped, "aiven_max_bytes_to_merge_override") == "5242880\tYes\n"
    assert server_setting(node_capped, "aiven_max_bytes_to_mutate_override") == "2097152\tYes\n"
    assert server_setting(node_free, "aiven_max_bytes_to_merge_override") == "0\tYes\n"


def test_merge_cap(start_cluster):
    for node in (node_capped, node_free):
        node.query(f"CREATE TABLE t_merge (k UInt64, v String CODEC(NONE)) ENGINE = MergeTree ORDER BY k SETTINGS {TABLE_SETTINGS}")
        node.query("SYSTEM STOP MERGES t_merge")
        insert_parts(node, "t_merge", 6)
        node.query("SYSTEM START MERGES t_merge")

    wait_parts(node_free, "t_merge", 1)
    time.sleep(5)
    assert active_parts(node_capped, "t_merge") > 1


def test_mutation_cap(start_cluster):
    for node in (node_capped, node_free):
        node.query(f"CREATE TABLE t_mut (k UInt64, v String CODEC(NONE)) ENGINE = MergeTree ORDER BY k SETTINGS {TABLE_SETTINGS}")
        node.query("INSERT INTO t_mut SELECT number, repeat('a', 1024) FROM numbers(8192)")
        node.query("ALTER TABLE t_mut UPDATE v = '' WHERE 1")

    query = "SELECT is_done FROM system.mutations WHERE database = currentDatabase() AND table = 't_mut'"
    assert_eq_with_retry(node_free, query, "1\n", retry_count=60, sleep_time=1)
    time.sleep(5)
    assert node_capped.query(query) == "0\n"


def test_replicated_merge_and_runtime_change(start_cluster):
    """A merge assigned by `free` is postponed on `capped`, a lightweight sync does not wait for
    it, and removing the cap at runtime lets it run."""
    for node in (node_capped, node_free):
        node.query(
            "CREATE TABLE t_repl (k UInt64, v String CODEC(NONE)) "
            "ENGINE = ReplicatedMergeTree('/clickhouse/tables/aiven047/t_repl', '{replica}') "
            f"ORDER BY k SETTINGS {TABLE_SETTINGS}"
        )
        node.query("SYSTEM STOP MERGES t_repl")
    insert_parts(node_free, "t_repl", 6)
    node_capped.query("SYSTEM SYNC REPLICA t_repl")
    assert active_parts(node_capped, "t_repl") == 6

    node_free.query("SYSTEM START MERGES t_repl")
    wait_parts(node_free, "t_repl", 1)
    node_capped.query("SYSTEM START MERGES t_repl")
    assert_eq_with_retry(
        node_capped,
        f"SELECT count() > 0 FROM system.replication_queue WHERE table = 't_repl' AND postpone_reason LIKE '%{SIZE_REASON}%'",
        "1\n",
        retry_count=60,
        sleep_time=1,
    )
    node_capped.query("SYSTEM SYNC REPLICA t_repl LIGHTWEIGHT", settings={"receive_timeout": 30})
    assert active_parts(node_capped, "t_repl") > 1

    node_capped.replace_in_config(CAPS_CONFIG, "<aiven_max_bytes_to_merge_override>5242880<", "<aiven_max_bytes_to_merge_override>0<")
    node_capped.query("SYSTEM RELOAD CONFIG")
    assert server_setting(node_capped, "aiven_max_bytes_to_merge_override") == "0\tYes\n"
    wait_parts(node_capped, "t_repl", 1)

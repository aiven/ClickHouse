"""
Aiven patch chain 062/063/064 — integration test for:

  062 "Prohibit .tmp table creation"      (50a42b769e)
  063 "Use .tmp for all fake temporal tables" (cc718390aa)
  064 ".tmp create or replace" exemption   (c95ef0cc6b)

ported onto the 26.3 uplift as one squashed commit. See
docs/aiven/patches/062-prohibit-tmp-table-creation.md.

Why integration (not stateless): the prohibition is keyed on a SERVER setting
`aiven_prohibit_tmp_table_creation` (default false) that cannot be set
per-session, so the differential axis is the server config, exactly like the
patch 008 layout. Both nodes run identical SQL; the only difference is whether
the guard config drop-in is present:

  * node_on  : <aiven_prohibit_tmp_table_creation>true</...>  -> guard ON
  * node_off : setting absent -> default false                -> guard OFF (stock)

The guard ON node proves the three behaviors of the chain:
  1. A user `CREATE TABLE ".tmpfoo"` is rejected with BAD_ARGUMENTS (062, the
     create-side guard in InterpreterCreateQuery::doCreateTable).
  2. A user `RENAME TABLE ... TO ".tmpbar"` in a Replicated database is rejected
     with BAD_ARGUMENTS (062, the rename-side guard in
     InterpreterRenameQuery::executeToTables, keyed on isInternalQuery).
  3. `CREATE OR REPLACE TABLE` still works: it internally creates a
     `.tmp_replace_*` table (063 renamed the prefix from `_tmp_replace_`), which
     is exempt because 064 marks that inner create internal.
  4. A refreshable materialized view refresh still works: it internally creates a
     `.tmp.inner_id.*` table, exempt because StorageMaterializedView marks that
     create internal.

The guard OFF node proves neutrality: the identical `.tmp*` CREATE and RENAME
that are rejected with the guard ON are ACCEPTED with the guard OFF, i.e. stock
26.3 behavior. This is the pre/post evidence pair (presence/absence of the
error), per docs/aiven/AGENTS.md.
"""

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

# "post" node: the server guard is ON.
node_on = cluster.add_instance(
    "node_on",
    main_configs=["configs/enable_prohibit_tmp.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "s1", "replica": "r_on"},
)

# "pre" / control node: the server guard is absent => default false.
node_off = cluster.add_instance(
    "node_off",
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "s1", "replica": "r_off"},
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_create_tmp_table_rejected_when_guard_on(start_cluster):
    # "post" (guard ON): a non-internal CREATE of a `.tmp*` table is rejected.
    err = node_on.query_and_get_error(
        'CREATE TABLE ".tmpfoo" (a UInt64) ENGINE = MergeTree ORDER BY a'
    )
    assert "reserved for internal use" in err, (
        f"guard ON: expected the patch 062 message, got: {err}"
    )
    assert "BAD_ARGUMENTS" in err, (
        f"guard ON: expected error code BAD_ARGUMENTS, got: {err}"
    )

    # "pre" (guard OFF, control): the identical CREATE succeeds (stock 26.3).
    node_off.query('DROP TABLE IF EXISTS ".tmpfoo" SYNC')
    node_off.query(
        'CREATE TABLE ".tmpfoo" (a UInt64) ENGINE = MergeTree ORDER BY a'
    )
    exists = node_off.query(
        "SELECT count() FROM system.tables "
        "WHERE database = currentDatabase() AND name = '.tmpfoo'"
    ).strip()
    assert exists == "1", f"guard OFF: `.tmpfoo` should have been created, got: {exists}"
    node_off.query('DROP TABLE ".tmpfoo" SYNC')


def test_rename_to_tmp_rejected_when_guard_on(start_cluster):
    # The rename guard only applies in a Replicated database (it sits behind
    # `database->shouldReplicateQuery`). Each node gets its OWN independent
    # Replicated database (distinct ZooKeeper path) so the two servers do not
    # replicate to each other; otherwise the table created on the first node
    # would already exist on the second.
    for node, suffix in ((node_on, "on"), (node_off, "off")):
        node.query(f"DROP DATABASE IF EXISTS rdb062_{suffix} SYNC")
        node.query(
            f"CREATE DATABASE rdb062_{suffix} ENGINE = Replicated("
            f"'/clickhouse/databases/rdb062_{suffix}', '{{shard}}', '{{replica}}')"
        )
        node.query(
            f"CREATE TABLE rdb062_{suffix}.src (a UInt64) "
            "ENGINE = ReplicatedMergeTree() ORDER BY a"
        )

    # "post" (guard ON): renaming into `.tmp*` is rejected.
    err = node_on.query_and_get_error(
        'RENAME TABLE rdb062_on.src TO rdb062_on.".tmpbar"'
    )
    assert "reserved for internal use" in err, (
        f"guard ON: expected the patch 062 rename message, got: {err}"
    )
    assert "BAD_ARGUMENTS" in err, (
        f"guard ON: expected error code BAD_ARGUMENTS, got: {err}"
    )

    # "pre" (guard OFF, control): the identical rename succeeds (stock 26.3).
    node_off.query('RENAME TABLE rdb062_off.src TO rdb062_off.".tmpbar"')
    exists = node_off.query(
        "SELECT count() FROM system.tables "
        "WHERE database = 'rdb062_off' AND name = '.tmpbar'"
    ).strip()
    assert exists == "1", f"guard OFF: rename into `.tmp*` should have succeeded, got: {exists}"

    for node, suffix in ((node_on, "on"), (node_off, "off")):
        node.query(f"DROP DATABASE IF EXISTS rdb062_{suffix} SYNC")


def test_create_or_replace_still_works_when_guard_on(start_cluster):
    # 064 exemption: CREATE OR REPLACE TABLE internally creates a
    # `.tmp_replace_*` table (063 prefix), which must be exempt from the guard.
    node_on.query("DROP TABLE IF EXISTS cor062 SYNC")
    node_on.query(
        "CREATE OR REPLACE TABLE cor062 (a UInt64) ENGINE = MergeTree ORDER BY a "
        "AS SELECT 1 AS a"
    )
    assert node_on.query("SELECT a FROM cor062").strip() == "1"

    # Replace it again to exercise the temp-table-then-exchange path on an
    # existing table.
    node_on.query(
        "CREATE OR REPLACE TABLE cor062 (a UInt64) ENGINE = MergeTree ORDER BY a "
        "AS SELECT 2 AS a"
    )
    assert node_on.query("SELECT a FROM cor062").strip() == "2"
    node_on.query("DROP TABLE cor062 SYNC")


def test_refreshable_mv_refresh_still_works_when_guard_on(start_cluster):
    # The refresh path internally creates a `.tmp.inner_id.*` table
    # (StorageMaterializedView::prepareTableForInsert), which must be exempt
    # because that create is marked internal.
    node_on.query("DROP TABLE IF EXISTS mv062 SYNC")
    node_on.query(
        "CREATE MATERIALIZED VIEW mv062 REFRESH EVERY 1 HOUR "
        "(a UInt64) ENGINE = MergeTree ORDER BY a "
        "AS SELECT number AS a FROM numbers(3)"
    )
    node_on.query("SYSTEM REFRESH VIEW mv062")
    assert_eq_with_retry(
        node_on,
        "SELECT count() FROM mv062",
        "3\n",
        retry_count=60,
        sleep_time=1,
    )
    node_on.query("DROP TABLE mv062 SYNC")

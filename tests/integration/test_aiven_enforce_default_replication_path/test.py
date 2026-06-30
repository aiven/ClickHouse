"""
Aiven patch 058 — integration test for the gated enforcement that a
`ReplicatedMergeTree` table must use the managed `default_replica_path` /
`default_replica_name`. See
docs/aiven/patches/058-disallow-replication-parameter-customization.md.

Why integration (not stateless): the enforcement is keyed on the SERVER setting
`aiven_enforce_default_replication_path` (default false) that cannot be set
per-session, so the differential axis is the server config:

  * node_on : <aiven_enforce_default_replication_path>true</...>  -> gate ON
  * node_off: setting absent -> default false                    -> gate OFF (stock)

The tables here are STANDALONE `ReplicatedMergeTree` (not inside a `Replicated`
database). That is exactly the scope upstream's
`database_replicated_allow_replicated_engine_arguments` guard does NOT cover (it
requires `is_replicated_database`), so the ON/OFF difference isolates the Aiven
patch's added value rather than upstream behavior.

`expand_special_macros_only` expands only the special `{database}`/`{table}`
macros; `{uuid}`/`{shard}`/`{replica}` are left intact. So a non-special default
template is compared raw-vs-raw and the default-args creation path does not
self-reject. node_on overrides `default_replica_path` to a `{shard}`-only
template (no `{uuid}`): the stock `/clickhouse/tables/{uuid}/{shard}` default
would make a STANDALONE table fail later in `TableZnodeInfo::resolve` with the
unrelated upstream "Macro 'uuid' ... only supported ..." error (a standalone
table is not ON CLUSTER and not in a Replicated DB), which would mask the guard.

Gate ON proves enforcement:
  1. Default creation (no explicit args) succeeds.
  2. Passing the exact default template explicitly succeeds (equals the expanded
     default).
  3. A foreign ZooKeeper path is rejected with BAD_ARGUMENTS.
  4. A foreign replica name is rejected with BAD_ARGUMENTS.

Gate OFF proves neutrality (the differential leg / pre-state): the identical
foreign-path DDL is ACCEPTED, because upstream does not guard standalone
replicated tables and the Aiven gate is off. This is the fails-before/passes-after
evidence pair, per docs/aiven/AGENTS.md.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Gate ON: the enforcement is active.
node_on = cluster.add_instance(
    "node_on",
    main_configs=["configs/enable_enforce.xml"],
    with_zookeeper=True,
    macros={"shard": "s1", "replica": "r1"},
)

# Gate OFF: stock 26.3 (setting absent -> default false). Shares the same
# ZooKeeper so we know the ACCEPT on this node is the gate's doing, not isolation.
node_off = cluster.add_instance(
    "node_off",
    with_zookeeper=True,
    macros={"shard": "s1", "replica": "r2"},
)

# Must match <default_replica_path> in configs/enable_enforce.xml.
DEFAULT_PATH = "/clickhouse/tables/{shard}/s058"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_on_default_creation_passes(start_cluster):
    # Gate ON: omitting the engine args must succeed -- the default-args path
    # passes the raw `default_replica_path` to the guard, which equals its own
    # special-macro expansion (no {database}/{table}), so it does NOT self-reject.
    node_on.query(
        "CREATE TABLE t_default (a Int64) ENGINE = ReplicatedMergeTree ORDER BY a"
    )
    assert "ReplicatedMergeTree" in node_on.query("SHOW CREATE TABLE t_default")
    node_on.query("DROP TABLE t_default SYNC")


def test_on_explicit_default_template_passes(start_cluster):
    # Gate ON: passing the exact managed default template explicitly is allowed
    # (it equals the expanded default), so customers may be explicit as long as
    # they use the managed path/name.
    node_on.query(
        f"CREATE TABLE t_tmpl (a Int64) "
        f"ENGINE = ReplicatedMergeTree('{DEFAULT_PATH}', '{{replica}}') ORDER BY a"
    )
    node_on.query("DROP TABLE t_tmpl SYNC")


def test_on_foreign_zookeeper_path_rejected(start_cluster):
    # Gate ON: a ZooKeeper path that differs from the managed default is rejected.
    err = node_on.query_and_get_error(
        "CREATE TABLE t_foreign (a Int64) "
        "ENGINE = ReplicatedMergeTree('/foreign/path/{shard}', '{replica}') ORDER BY a"
    )
    assert "BAD_ARGUMENTS" in err, err
    assert "Setting ZooKeeper path" in err and "is not allowed" in err, err


def test_on_foreign_replica_name_rejected(start_cluster):
    # Gate ON: the managed path with a foreign replica name is rejected on the
    # replica-name check (path check passes first).
    err = node_on.query_and_get_error(
        f"CREATE TABLE t_foreign_r (a Int64) "
        f"ENGINE = ReplicatedMergeTree('{DEFAULT_PATH}', 'other_replica') ORDER BY a"
    )
    assert "BAD_ARGUMENTS" in err, err
    assert "Setting replica name" in err and "is not allowed" in err, err


def test_off_neutrality_foreign_path_accepted(start_cluster):
    # Gate OFF (the differential / pre-state): the identical foreign-path DDL is
    # ACCEPTED. Upstream does not guard standalone replicated tables and the Aiven
    # gate is off, so behavior is exactly upstream.
    node_off.query(
        "CREATE TABLE t_off (a Int64) "
        "ENGINE = ReplicatedMergeTree('/foreign/off/path/{shard}', '{replica}') ORDER BY a"
    )
    node_off.query("INSERT INTO t_off VALUES (5)")
    assert node_off.query("SELECT a FROM t_off").strip() == "5"
    node_off.query("DROP TABLE t_off SYNC")

"""
Aiven patch 004 — integration test for the gated
`MergeTree` -> `ReplicatedMergeTree` engine auto-substitution in `Replicated`
databases. See docs/aiven/patches/004-replace-mergetree-with-replicated.md.

Why integration (not stateless): the conversion is keyed on the SERVER setting
`aiven_replace_mergetree_with_replicated` (default false) that cannot be set
per-session, and the property being proven is genuinely cluster-level
(cross-replica data flow). The differential axis is therefore the server config:

  * node_on1 / node_on2 : <aiven_replace_mergetree_with_replicated>true</...>
                          -> setting ON  (Replicated DB `rdb_on`)
  * node_off1 / node_off2: setting absent -> default false
                          -> setting OFF (stock; Replicated DB `rdb_off`)

The setting-ON pair proves the patch behavior + REAL replication (evidence of
causation, not a cosmetic rename):
  1. `CREATE TABLE ... ENGINE = MergeTree` is rewritten to `ReplicatedMergeTree`
     on BOTH nodes (`system.tables.engine` + `SHOW CREATE TABLE` agree), and an
     INSERT on node_on1 becomes visible on node_on2.
  2. The `SummingMergeTree` / `ReplacingMergeTree` variants convert + replicate.
  3. A table in a NON-Replicated (Atomic) database keeps `MergeTree` even with the
     setting on (the rewrite is Replicated-DB-scoped).
  4. Missing-twin safety: `ENGINE = FooMergeTree` (no registered `Replicated*`
     twin) is NOT rewritten to a bogus `ReplicatedFooMergeTree`; resolution fails
     with the normal `UNKNOWN_STORAGE` for `FooMergeTree`, no `LOGICAL_ERROR`.

The setting-OFF pair proves neutrality (the differential leg): the identical
`ENGINE = MergeTree` DDL stays `MergeTree`, and a row inserted on node_off1 is
ABSENT on node_off2 (a plain MergeTree in a Replicated DB does not replicate
data). This is the pre/post evidence pair, per docs/aiven/AGENTS.md.

The ATTACH case exercises the §3.3 `!attach` guard via a real user `ATTACH`
DDL: a MergeTree table is created while the setting is OFF (stays MergeTree on
disk), then the setting is turned ON (config drop-in + restart) and the table is
re-ATTACHed. The ATTACH runs as a `SECONDARY_QUERY` with `attach=true` against a
stored `MergeTree` engine inside a `Replicated` database -- exactly the path the
guard protects -- and must NOT be converted (which would mis-adopt the on-disk
data).
"""

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

# "setting ON" pair: the rewrite is active. Both nodes are replicas of the same
# Replicated database (same ZooKeeper path) so we can prove cross-node replication.
node_on1 = cluster.add_instance(
    "node_on1",
    main_configs=["configs/enable_replace.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "s1", "replica": "r1"},
)
node_on2 = cluster.add_instance(
    "node_on2",
    main_configs=["configs/enable_replace.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "s1", "replica": "r2"},
)

# "setting OFF" pair: the rewrite is inactive (default false). Used for the
# neutrality differential leg, and node_off1 additionally drives the ATTACH case
# (it transitions OFF -> ON via a config drop-in + restart, last in the file).
node_off1 = cluster.add_instance(
    "node_off1",
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "s1", "replica": "r1"},
)
node_off2 = cluster.add_instance(
    "node_off2",
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "s1", "replica": "r2"},
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        for node, path in (
            (node_on1, "/clickhouse/databases/rdb_on"),
            (node_on2, "/clickhouse/databases/rdb_on"),
        ):
            node.query(
                f"CREATE DATABASE rdb_on ENGINE = Replicated('{path}', '{{shard}}', '{{replica}}')"
            )

        for node, path in (
            (node_off1, "/clickhouse/databases/rdb_off"),
            (node_off2, "/clickhouse/databases/rdb_off"),
        ):
            node.query(
                f"CREATE DATABASE rdb_off ENGINE = Replicated('{path}', '{{shard}}', '{{replica}}')"
            )

        yield cluster
    finally:
        cluster.shutdown()


def _engine_of(node, database, table):
    return node.query(
        "SELECT engine FROM system.tables "
        f"WHERE database = '{database}' AND name = '{table}'"
    ).strip()


def test_on_conversion_and_real_replication(start_cluster):
    # Setting ON: a plain MergeTree CREATE in a Replicated DB is rewritten to
    # ReplicatedMergeTree on the node that initiates it.
    node_on1.query(
        "CREATE TABLE rdb_on.t (a Int64) ENGINE = MergeTree ORDER BY a"
    )
    assert _engine_of(node_on1, "rdb_on", "t") == "ReplicatedMergeTree", (
        "setting ON: engine should have been rewritten to ReplicatedMergeTree on node_on1"
    )

    # Stored-DDL consistency (§3.5): SHOW CREATE shows the rewritten engine.
    assert "ReplicatedMergeTree" in node_on1.query("SHOW CREATE TABLE rdb_on.t")

    # The DDL replicates to node_on2 and the engine there is ReplicatedMergeTree too
    # (every replica re-derives the same engine -> identical stored metadata).
    node_on2.query("SYSTEM SYNC DATABASE REPLICA rdb_on")
    assert_eq_with_retry(
        node_on2,
        "SELECT engine FROM system.tables WHERE database = 'rdb_on' AND name = 't'",
        "ReplicatedMergeTree\n",
    )
    assert "ReplicatedMergeTree" in node_on2.query("SHOW CREATE TABLE rdb_on.t")

    # REAL replication (causation, not a cosmetic rename): a row inserted on
    # node_on1 becomes visible on node_on2.
    node_on1.query("INSERT INTO rdb_on.t VALUES (42)")
    node_on2.query("SYSTEM SYNC REPLICA rdb_on.t")
    assert_eq_with_retry(node_on2, "SELECT a FROM rdb_on.t", "42\n")

    node_on1.query("DROP TABLE rdb_on.t SYNC")


@pytest.mark.parametrize(
    "engine_name",
    ["SummingMergeTree", "ReplacingMergeTree"],
)
def test_on_variant_engines_convert_and_replicate(start_cluster, engine_name):
    table = f"v_{engine_name.lower()}"
    node_on1.query(
        f"CREATE TABLE rdb_on.{table} (a Int64) ENGINE = {engine_name} ORDER BY a"
    )
    assert _engine_of(node_on1, "rdb_on", table) == f"Replicated{engine_name}", (
        f"setting ON: {engine_name} should have been rewritten to Replicated{engine_name}"
    )

    node_on2.query("SYSTEM SYNC DATABASE REPLICA rdb_on")
    assert_eq_with_retry(
        node_on2,
        f"SELECT engine FROM system.tables WHERE database = 'rdb_on' AND name = '{table}'",
        f"Replicated{engine_name}\n",
    )

    node_on1.query(f"INSERT INTO rdb_on.{table} VALUES (7)")
    node_on2.query(f"SYSTEM SYNC REPLICA rdb_on.{table}")
    assert_eq_with_retry(node_on2, f"SELECT a FROM rdb_on.{table}", "7\n")

    node_on1.query(f"DROP TABLE rdb_on.{table} SYNC")


def test_on_non_replicated_database_never_converts(start_cluster):
    # Even with the setting ON, a table in a non-Replicated (Atomic) database keeps
    # its MergeTree engine: the rewrite is Replicated-DB-scoped (§6).
    node_on1.query("CREATE DATABASE IF NOT EXISTS atomic_db ENGINE = Atomic")
    node_on1.query(
        "CREATE TABLE atomic_db.t (a Int64) ENGINE = MergeTree ORDER BY a"
    )
    assert _engine_of(node_on1, "atomic_db", "t") == "MergeTree", (
        "setting ON: a MergeTree table in an Atomic database must stay MergeTree"
    )
    node_on1.query("DROP TABLE atomic_db.t SYNC")
    node_on1.query("DROP DATABASE atomic_db SYNC")


def test_on_missing_twin_not_rewritten(start_cluster):
    # Missing-twin safety (§3.4): `FooMergeTree` ends with "MergeTree" so it enters
    # the rewrite, but `ReplicatedFooMergeTree` is not a registered engine, so the
    # name is left UNCHANGED. Resolution then fails with the normal UNKNOWN_STORAGE
    # for the ORIGINAL name -- proving the rewrite neither fabricated a bogus
    # `ReplicatedFooMergeTree` nor raised a LOGICAL_ERROR of its own.
    err = node_on1.query_and_get_error(
        "CREATE TABLE rdb_on.bogus (a Int64) ENGINE = FooMergeTree ORDER BY a"
    )
    assert "UNKNOWN_STORAGE" in err, (
        f"missing-twin: expected UNKNOWN_STORAGE, got: {err}"
    )
    assert "FooMergeTree" in err and "ReplicatedFooMergeTree" not in err, (
        f"missing-twin: the error must name the original engine, not a fabricated twin: {err}"
    )
    assert "LOGICAL_ERROR" not in err, (
        f"missing-twin: the rewrite must not raise a LOGICAL_ERROR: {err}"
    )


def test_off_neutrality_no_conversion_no_replication(start_cluster):
    # Setting OFF: the identical DDL keeps MergeTree, and the row does NOT replicate
    # (a plain MergeTree in a Replicated DB only replicates its definition, not data).
    node_off1.query(
        "CREATE TABLE rdb_off.t (a Int64) ENGINE = MergeTree ORDER BY a"
    )
    assert _engine_of(node_off1, "rdb_off", "t") == "MergeTree", (
        "setting OFF: engine must stay MergeTree (stock 26.3 behavior)"
    )

    node_off2.query("SYSTEM SYNC DATABASE REPLICA rdb_off")
    assert_eq_with_retry(
        node_off2,
        "SELECT engine FROM system.tables WHERE database = 'rdb_off' AND name = 't'",
        "MergeTree\n",
    )

    node_off1.query("INSERT INTO rdb_off.t VALUES (99)")
    # Give any (erroneous) replication a chance, then assert the row is ABSENT.
    node_off2.query("SYSTEM SYNC DATABASE REPLICA rdb_off")
    assert node_off2.query("SELECT count() FROM rdb_off.t").strip() == "0", (
        "setting OFF: a plain MergeTree must NOT replicate data across nodes"
    )

    node_off1.query("DROP TABLE rdb_off.t SYNC")


def test_log_engine_backstop_when_only_replicated_allowed(start_cluster):
    # For the record (§5.3): the rewrite only covers the *MergeTree family. The
    # non-MergeTree disk-engine gap is closed by the upstream-owned
    # `database_replicated_allow_only_replicated_engine`, which REJECTS (does not
    # convert) a `Log` engine in a Replicated database. Asserted here to document the
    # combined production posture; this is upstream behavior, not the Aiven patch.
    err = node_on1.query_and_get_error(
        "CREATE TABLE rdb_on.log_t (a Int64) ENGINE = Log",
        settings={"database_replicated_allow_only_replicated_engine": 1},
    )
    assert "Log" in err and (
        "Replicated" in err or "replicated" in err
    ), f"expected the only-replicated-engine rejection for ENGINE = Log, got: {err}"


def test_attach_not_converted(start_cluster):
    # §3.3 guard: ATTACH must never re-engine an existing on-disk MergeTree table.
    # NOTE: this test transitions node_off1 from OFF -> ON via a config drop-in +
    # restart, so it must run AFTER the OFF-neutrality test. It uses its own
    # single-replica Replicated database so it does not disturb the rdb_off pair.
    node_off1.query(
        "CREATE DATABASE rdb_attach ENGINE = Replicated("
        "'/clickhouse/databases/rdb_attach', 's1', 'r1')"
    )
    # Setting is OFF here, so the table is stored as a plain MergeTree on disk.
    node_off1.query(
        "CREATE TABLE rdb_attach.t (a Int64) ENGINE = MergeTree ORDER BY a"
    )
    assert _engine_of(node_off1, "rdb_attach", "t") == "MergeTree"
    node_off1.query("INSERT INTO rdb_attach.t VALUES (7)")

    # Permanently detach so the table survives the restart in a detached state and
    # can be re-attached by an explicit user ATTACH DDL afterwards.
    node_off1.query("DETACH TABLE rdb_attach.t PERMANENTLY")

    # Turn the setting ON and restart, so the subsequent ATTACH runs with the
    # feature live -- without the §3.3 guard it WOULD convert the stored MergeTree.
    node_off1.replace_config(
        "/etc/clickhouse-server/config.d/zz_enable_replace.xml",
        "<clickhouse><aiven_replace_mergetree_with_replicated>true"
        "</aiven_replace_mergetree_with_replicated></clickhouse>",
    )
    node_off1.restart_clickhouse()

    # The ATTACH is a user DDL in a Replicated DB => SECONDARY_QUERY + attach=true
    # against a stored MergeTree engine: exactly the path the guard protects.
    node_off1.query("ATTACH TABLE rdb_attach.t")
    assert _engine_of(node_off1, "rdb_attach", "t") == "MergeTree", (
        "ATTACH must not convert an existing on-disk MergeTree table to Replicated*"
    )
    # The on-disk data was not mis-adopted / lost.
    assert node_off1.query("SELECT a FROM rdb_attach.t").strip() == "7"

    node_off1.query("DROP TABLE rdb_attach.t SYNC")
    node_off1.query("DROP DATABASE rdb_attach SYNC")

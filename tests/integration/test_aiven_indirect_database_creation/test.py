"""
Aiven patch 019 — "Allow avnadmin creating database using sql".

Exercises the indirect (privilege-elevated) Replicated-database creation path for a
configured admin user (`avnadmin`), the new
`GRANT DEFAULT REPLICATED DATABASE PRIVILEGES` statement, the reserved-prefix guard,
the ON-CLUSTER-for-non-admin-drop enforcement, and — most importantly — that the
`setGlobalContext` elevation used internally does NOT leak into the invoking user's
session (case F, the hard non-escalation gate).

Two single-node-with-Keeper instances:
  * `node_cfg`  — feature configured (configs/indirect_db.xml): the three server
                  settings are set and a `cluster_secret` named collection exists.
  * `node_plain`— feature unconfigured (server settings default to ""): proves the
                  feature is inert by default and that the DROP-enforcement change is a
                  no-op when `cluster_database` is empty (the clause-(v) fix, finding D).

Case map (see docs/aiven/patches/019-avnadmin-indirect-database-creation.md):
  A   test_a_indirect_create_happy_path
  B   test_b_normal_user_denied
  C/G test_c_grant_default_privileges
  D   test_d_reserved_prefix_rejected
  E   test_e_on_cluster_enforced_for_non_admin_drop
  F   test_f_non_escalation                (HARD GATE)
  H   test_h_grantee_injection             (finding A)
  D2  test_d2_reserved_prefix_scope        (finding C)
  E2  test_e2_drop_privilege_not_bypassed  (finding E)
  R   test_r_default_off_create
  R2  test_r2_default_off_drop             (finding D / clause-(v) proof)
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_cfg = cluster.add_instance(
    "node_cfg",
    main_configs=["configs/indirect_db.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "the_shard", "replica": "r1"},
)

node_plain = cluster.add_instance(
    "node_plain",
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "the_shard", "replica": "r2"},
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        # The reference Replicated database named by `cluster_database`. Created by the
        # admin (default) user via the normal path (default != avnadmin). Its pre-expansion
        # shard literal `{shard}` is what createReplicatedDatabaseByClient reuses.
        node_cfg.query(
            "CREATE DATABASE cluster_db ENGINE = "
            "Replicated('/clickhouse/databases/cluster_db', '{shard}', '{replica}')"
        )
        node_cfg.query("CREATE USER avnadmin IDENTIFIED WITH no_password")

        yield cluster
    finally:
        cluster.shutdown()


def _denied(err):
    return "ACCESS_DENIED" in err or "Not enough privileges" in err


def test_a_indirect_create_happy_path(start_cluster):
    # avnadmin issues the simplified CREATE DATABASE; the indirect path auto-fills the
    # ZK path, reuses cluster_db's shard macro and sets {replica}, all ON CLUSTER cluster_db.
    node_cfg.query("CREATE DATABASE db_a", user="avnadmin")

    assert (
        node_cfg.query("SELECT engine FROM system.databases WHERE name = 'db_a'").strip()
        == "Replicated"
    )
    show = node_cfg.query("SHOW CREATE DATABASE db_a")
    assert "/clickhouse/databases/db_a" in show
    assert "Replicated" in show
    # The shard macro was reused (pre-expansion form), not a hardcoded shard name.
    assert "{shard}" in show
    assert "{replica}" in show

    # avnadmin received the default replicated-database privileges on db_a (with grant option).
    grants = node_cfg.query("SHOW GRANTS FOR avnadmin")
    assert "db_a" in grants
    assert "WITH GRANT OPTION" in grants


def test_b_normal_user_denied(start_cluster):
    node_cfg.query("CREATE USER normal_user IDENTIFIED WITH no_password")
    # normal_user is not avnadmin -> the indirect branch does NOT fire -> normal access
    # rules apply -> CREATE DATABASE is refused (no CREATE DATABASE grant).
    err = node_cfg.query_and_get_error(
        "CREATE DATABASE db_b ENGINE = Replicated('/x', 's', '{replica}')",
        user="normal_user",
    )
    assert _denied(err), err
    assert (
        node_cfg.query("SELECT count() FROM system.databases WHERE name = 'db_b'").strip()
        == "0"
    )


def test_c_grant_default_privileges(start_cluster):
    node_cfg.query("CREATE USER grantee_c IDENTIFIED WITH no_password")
    # Parses + executes (case G round-trip is folded in here: the statement is accepted and
    # produces the correct grant set; note the AST formatter does not re-emit the keyword —
    # documented limitation).
    node_cfg.query("GRANT DEFAULT REPLICATED DATABASE PRIVILEGES ON c_db.* TO grantee_c")

    grants = node_cfg.query("SHOW GRANTS FOR grantee_c")
    assert "ON c_db.*" in grants
    assert "WITH GRANT OPTION" in grants

    # The full expected privilege set (c_db != cluster_db, so DROP DATABASE is included).
    # This is the set that patch 020 will later extend with CHECK.
    expected = {
        "DROP DATABASE",
        "ALTER UPDATE",
        "ALTER DELETE",
        "ALTER COLUMN",
        "ALTER MODIFY COMMENT",
        "ALTER INDEX",
        "ALTER PROJECTION",
        "ALTER CONSTRAINT",
        "ALTER TTL",
        "ALTER MATERIALIZE TTL",
        "ALTER SETTINGS",
        "ALTER MOVE PARTITION",
        "ALTER FETCH PARTITION",
        "ALTER VIEW",
        "CREATE TABLE",
        "DROP TABLE",
        "CREATE DICTIONARY",
        "DROP DICTIONARY",
        "dictGet",
        "INSERT",
        "OPTIMIZE",
        "SELECT",
        "SHOW",
        "SYSTEM SYNC REPLICA",
        "TRUNCATE",
    }
    # Representative subset must be present (robust against SHOW-grant coalescing).
    for priv in ("SELECT", "INSERT", "DROP DATABASE", "CREATE TABLE",
                 "TRUNCATE", "OPTIMIZE", "dictGet", "SYSTEM SYNC REPLICA"):
        assert priv in grants, f"{priv} missing from: {grants}"

    # Dangerous privileges MUST NOT be in the set.
    assert "ACCESS MANAGEMENT" not in grants
    assert "CREATE DATABASE" not in grants  # only DROP DATABASE is granted, never CREATE
    assert "SYSTEM SHUTDOWN" not in grants

    # Enumerate via system.grants for the record (logged for the dossier evidence).
    rows = node_cfg.query(
        "SELECT access_type FROM system.grants "
        "WHERE user_name = 'grantee_c' AND database = 'c_db' ORDER BY access_type"
    )
    print("grantee_c system.grants access types:\n" + rows)


def test_d_reserved_prefix_rejected(start_cluster):
    err = node_cfg.query_and_get_error("CREATE DATABASE reserved_x", user="avnadmin")
    assert "cannot start with" in err, err
    err2 = node_cfg.query_and_get_error("CREATE DATABASE system_x", user="avnadmin")
    assert "cannot start with" in err2, err2

    # A non-reserved name is accepted.
    node_cfg.query("CREATE DATABASE allowed_db", user="avnadmin")
    assert (
        node_cfg.query("SELECT count() FROM system.databases WHERE name = 'allowed_db'").strip()
        == "1"
    )


def test_e_on_cluster_enforced_for_non_admin_drop(start_cluster):
    node_cfg.query("CREATE DATABASE db_e", user="avnadmin")
    node_cfg.query("CREATE USER drop_user IDENTIFIED WITH no_password")
    node_cfg.query("GRANT DROP DATABASE ON db_e.* TO drop_user")

    # A non-admin DETACH DATABASE is refused outright by the enforcement block.
    err = node_cfg.query_and_get_error("DETACH DATABASE db_e", user="drop_user")
    assert "Database detach is not allowed" in err, err

    # A non-admin DROP DATABASE without ON CLUSTER succeeds: the enforcement upgrades it to
    # ON CLUSTER cluster_db implicitly so the removal is cluster-wide.
    node_cfg.query("DROP DATABASE db_e", user="drop_user")
    assert (
        node_cfg.query("SELECT count() FROM system.databases WHERE name = 'db_e'").strip()
        == "0"
    )


def test_f_non_escalation(start_cluster):
    # HARD GATE. After avnadmin performs the indirect (internally elevated) CREATE, its own
    # session must still NOT be a superuser. If this fails the elevation leaked -> STOP.
    node_cfg.query("CREATE DATABASE db_f", user="avnadmin")

    # Both inner elevated statements must have run: the CREATE and the GRANT each reuse the
    # cloned context's fresh query_id. A second query_id collision would surface only on the
    # GRANT, so assert avnadmin actually received the default privileges on db_f.
    grants_f = node_cfg.query("SHOW GRANTS FOR avnadmin")
    assert "db_f" in grants_f, f"inner GRANT did not land (possible second query_id collision): {grants_f}"

    # avnadmin cannot perform an ACCESS MANAGEMENT operation.
    err = node_cfg.query_and_get_error("CREATE USER leaked_user IDENTIFIED WITH no_password",
                                       user="avnadmin")
    assert _denied(err), f"NON-ESCALATION VIOLATED (CREATE USER): {err}"

    # avnadmin cannot read a table it was never granted.
    node_cfg.query("CREATE DATABASE f_secret_db")  # admin, Atomic
    node_cfg.query("CREATE TABLE f_secret_db.t (x UInt32) ENGINE = Memory")
    node_cfg.query("INSERT INTO f_secret_db.t VALUES (7)")
    err2 = node_cfg.query_and_get_error("SELECT * FROM f_secret_db.t", user="avnadmin")
    assert _denied(err2), f"NON-ESCALATION VIOLATED (SELECT): {err2}"


def test_h_grantee_injection(start_cluster):
    # finding A: the grantee is an identifier and must be back-quoted, not string-escaped.
    # A grantee name containing a comma would, under the buggy escapeString form, parse as
    # two grantees (or a syntax error). With backQuote it lands on exactly one grantee.
    node_cfg.query("CREATE USER `gr,antee` IDENTIFIED WITH no_password")
    node_cfg.query("GRANT DEFAULT REPLICATED DATABASE PRIVILEGES ON h_db.* TO `gr,antee`")

    grants = node_cfg.query("SHOW GRANTS FOR `gr,antee`")
    assert "ON h_db.*" in grants, grants
    assert "SELECT" in grants

    # No spurious grantees were created/affected by splitting the name on the comma.
    assert (
        node_cfg.query("SELECT count() FROM system.users WHERE name IN ('gr', 'antee')").strip()
        == "0"
    )
    # Exactly one user holds grants on h_db.
    assert (
        node_cfg.query(
            "SELECT count(DISTINCT user_name) FROM system.grants WHERE database = 'h_db'"
        ).strip()
        == "1"
    )


def test_d2_reserved_prefix_scope(start_cluster):
    # finding C (intended behavior, carried as-is): checkDatabaseNameAllowed skips an
    # explicit non-Replicated engine, so a reserved-prefixed Atomic database is NOT guarded.
    node_cfg.query("CREATE DATABASE reserved_atomic ENGINE = Atomic")  # admin -> allowed
    assert (
        node_cfg.query("SELECT count() FROM system.databases WHERE name = 'reserved_atomic'").strip()
        == "1"
    )

    # The default-engine path (no explicit engine) IS guarded, even for the admin.
    err = node_cfg.query_and_get_error("CREATE DATABASE reserved_default_engine")
    assert "cannot start with" in err, err


def test_e2_drop_privilege_not_bypassed(start_cluster):
    # finding E (intended): skip_distributed_checks bypasses the CLUSTER grant and
    # allow_distributed_ddl, but the DROP DATABASE privilege itself is still required.
    node_cfg.query("CREATE DATABASE db_e2", user="avnadmin")
    node_cfg.query("CREATE USER nodrop_user IDENTIFIED WITH no_password")

    err = node_cfg.query_and_get_error("DROP DATABASE db_e2", user="nodrop_user")
    assert _denied(err), err
    assert (
        node_cfg.query("SELECT count() FROM system.databases WHERE name = 'db_e2'").strip()
        == "1"
    )


def test_r_default_off_create(start_cluster):
    # With the feature unconfigured, normal CREATE DATABASE behaves exactly as upstream.
    node_plain.query("CREATE DATABASE r_db")
    assert (
        node_plain.query("SELECT count() FROM system.databases WHERE name = 'r_db'").strip()
        == "1"
    )

    # The indirect path is inert: even a user literally named avnadmin gets no special
    # treatment because user_with_indirect_database_creation is empty here.
    node_plain.query("CREATE USER avnadmin IDENTIFIED WITH no_password")
    err = node_plain.query_and_get_error(
        "CREATE DATABASE r_db2 ENGINE = Replicated('/x', 's', '{replica}')",
        user="avnadmin",
    )
    assert _denied(err), err


def test_r2_default_off_drop(start_cluster):
    # finding D / clause-(v) proof: with cluster_database EMPTY, a restricted non-admin user
    # can DROP DATABASE WITHOUT ON CLUSTER exactly as upstream — NO SETTING_CONSTRAINT_VIOLATION.
    # (Source-as-written would have thrown here; the no-op-when-empty fix restores upstream.)
    node_plain.query("CREATE DATABASE r2_db")
    node_plain.query("CREATE USER r2_user IDENTIFIED WITH no_password")
    node_plain.query("GRANT DROP DATABASE ON r2_db.* TO r2_user")

    node_plain.query("DROP DATABASE r2_db", user="r2_user")
    assert (
        node_plain.query("SELECT count() FROM system.databases WHERE name = 'r2_db'").strip()
        == "0"
    )

"""
Aiven patch 019 — "Allow avnadmin creating database using sql".

Exercises the indirect (privilege-elevated) Replicated-database creation path for a
configured admin user (`avnadmin`), the new
`GRANT DEFAULT REPLICATED DATABASE PRIVILEGES` statement, the reserved-prefix guard,
the ON-CLUSTER-for-non-admin-drop enforcement, and — most importantly — that the
elevation used internally does NOT leak into the invoking user's session (case F, the
hard non-escalation gate).

Two single-node-with-Keeper instances:
  * `node_cfg`  — feature configured (configs/indirect_db.xml): the three server
                  settings are set and a `cluster_secret` named collection exists.
  * `node_plain`— feature unconfigured (server settings default to ""): proves the
                  feature is inert by default, that the DROP-enforcement change is a
                  no-op when `aiven_cluster_database` is empty, and that an explicitly
                  written `ON CLUSTER` still requires the `CLUSTER` privilege.

Case map (see docs/aiven/patches/019-avnadmin-indirect-database-creation.md):
  A   test_a_indirect_create_happy_path
  B   test_b_normal_user_denied
  C/G test_c_grant_default_privileges
  D   test_d_reserved_prefix_rejected
  D2  test_d2_reserved_prefix_scope
  D3  test_d3_reserved_prefix_quoted_and_bare_forms
  E   test_e_on_cluster_enforced_for_non_admin_drop
  E2  test_e2_drop_privilege_not_bypassed
  E3  test_e3_explicit_on_cluster_still_requires_cluster_grant
  E4  test_e4_explicit_on_cluster_requires_cluster_grant_when_configured
  E5  test_e5_forced_drop_as_the_initial_user
  F   test_f_non_escalation                (HARD GATE)
  H   test_h_grantee_injection
  I   test_i_database_name_injection
  J   test_j_if_not_exists_on_existing_database
  J2  test_j2_predefined_database_refused
  J3  test_j3_attach_of_detached_database
  K   test_k_grant_shortcut_refuses_on_cluster
  K2  test_k2_grant_shortcut_formats_as_itself
  K3  test_k3_grant_shortcut_refuses_replace_option
  K4  test_k4_grant_shortcut_in_users_config_is_rejected
  T   test_t_truncate_database_requires_drop_database
  W   test_w_host_failure_is_forwarded
  R   test_r_default_off_create
  R2  test_r2_default_off_drop
  P   test_p_protected_caller            (last: it marks avnadmin PROTECTED)
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
    main_configs=["configs/plain_cluster.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "the_shard", "replica": "r2"},
)

node_initial = cluster.add_instance(
    "node_initial",
    main_configs=["configs/initial_user.xml"],
    with_zookeeper=True,
    macros={"shard": "the_shard", "replica": "r3"},
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        # The reference Replicated database named by `aiven_cluster_database`. Created by
        # the admin (default) user via the normal path (default != avnadmin). Its
        # pre-expansion shard literal `{shard}` and its named collection are what
        # createReplicatedDatabaseByClient copies into the statement it composes.
        node_cfg.query(
            "CREATE DATABASE cluster_db ENGINE = "
            "Replicated('/clickhouse/databases/cluster_db', '{shard}', '{replica}') "
            "SETTINGS collection_name='cluster_secret'"
        )
        node_cfg.query("CREATE USER avnadmin IDENTIFIED WITH no_password")
        node_initial.query(
            "CREATE DATABASE cluster_db ENGINE = "
            "Replicated('/clickhouse/databases/cluster_db_initial', '{shard}', '{replica}')"
        )

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
    # The collection was copied from the reference database rather than hardcoded.
    assert "cluster_secret" in show, show

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

    # Representative subset must be present (robust against SHOW-grant coalescing).
    # c_db != cluster_db, so DROP DATABASE is included. CHECK comes from patch 020.
    for priv in ("SELECT", "INSERT", "DROP DATABASE", "CREATE TABLE",
                 "TRUNCATE", "OPTIMIZE", "dictGet", "SYSTEM SYNC REPLICA", "CHECK"):
        assert priv in grants, f"{priv} missing from: {grants}"

    # Dangerous privileges MUST NOT be in the set.
    assert "ACCESS MANAGEMENT" not in grants
    assert "CREATE DATABASE" not in grants  # only DROP DATABASE is granted, never CREATE
    assert "SYSTEM SHUTDOWN" not in grants


def test_c2_reference_database_is_not_droppable(start_cluster):
    # The curated set omits DROP DATABASE when the target is the reference database, which
    # backs the cluster itself. This is the branch production actually depends on.
    node_cfg.query("CREATE USER grantee_c2 IDENTIFIED WITH no_password")
    node_cfg.query("GRANT DEFAULT REPLICATED DATABASE PRIVILEGES ON cluster_db.* TO grantee_c2")

    grants = node_cfg.query("SHOW GRANTS FOR grantee_c2")
    assert "SELECT" in grants, grants
    assert "DROP DATABASE" not in grants, grants


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


def test_d2_reserved_prefix_scope(start_cluster):
    # Intended behaviour, carried as-is: checkDatabaseNameAllowed skips an explicit
    # non-Replicated engine, so a reserved-prefixed Atomic database is NOT guarded.
    node_cfg.query("CREATE DATABASE reserved_atomic ENGINE = Atomic")  # admin -> allowed
    assert (
        node_cfg.query("SELECT count() FROM system.databases WHERE name = 'reserved_atomic'").strip()
        == "1"
    )

    # The default-engine path (no explicit engine) IS guarded, even for the admin.
    err = node_cfg.query_and_get_error("CREATE DATABASE reserved_default_engine")
    assert "cannot start with" in err, err


def test_d3_reserved_prefix_quoted_and_bare_forms(start_cluster):
    # The setting is parsed with parseIdentifierOrStringLiteral, so a quoted prefix must be
    # honoured identically to a bare one — managed deployments supply the quoted form.
    err = node_cfg.query_and_get_error("CREATE DATABASE system_quoted_form", user="avnadmin")
    assert "cannot start with" in err, err

    # A bare word with no trailing separator reserves every name beginning with that word,
    # not just a delimited namespace.
    err2 = node_cfg.query_and_get_error("CREATE DATABASE aivenfoo", user="avnadmin")
    assert "cannot start with" in err2, err2

    # A name that merely contains the reserved word is unaffected: only prefixes are.
    node_cfg.query("CREATE DATABASE not_aiven_db", user="avnadmin")
    assert (
        node_cfg.query("SELECT count() FROM system.databases WHERE name = 'not_aiven_db'").strip()
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
    # ON CLUSTER cluster_db implicitly so the removal is cluster-wide. Note drop_user holds
    # no CLUSTER privilege — the forced rewrite must not charge them for our decision.
    node_cfg.query("DROP DATABASE db_e", user="drop_user")
    assert (
        node_cfg.query("SELECT count() FROM system.databases WHERE name = 'db_e'").strip()
        == "0"
    )


def test_e2_drop_privilege_not_bypassed(start_cluster):
    # skip_distributed_checks bypasses the CLUSTER grant and allow_distributed_ddl, but the
    # DROP DATABASE privilege itself is still required: it is in access_to_check.
    node_cfg.query("CREATE DATABASE db_e2", user="avnadmin")
    node_cfg.query("CREATE USER nodrop_user IDENTIFIED WITH no_password")

    err = node_cfg.query_and_get_error("DROP DATABASE db_e2", user="nodrop_user")
    assert _denied(err), err
    assert (
        node_cfg.query("SELECT count() FROM system.databases WHERE name = 'db_e2'").strip()
        == "1"
    )


def test_e3_explicit_on_cluster_still_requires_cluster_grant(start_cluster):
    # The skip exists to pay for a clause *we* added. A clause the user wrote must still
    # cost them the CLUSTER privilege, exactly as upstream requires. Run with the feature
    # off, which is where the relaxation would otherwise reach users it was never about.
    node_plain.query("CREATE DATABASE e3_db")
    node_plain.query("CREATE USER e3_user IDENTIFIED WITH no_password")
    node_plain.query("GRANT DROP DATABASE ON e3_db.* TO e3_user")

    # Holds DROP DATABASE, does not hold CLUSTER -> refused.
    err = node_plain.query_and_get_error(
        "DROP DATABASE e3_db ON CLUSTER plain_cluster", user="e3_user"
    )
    assert _denied(err), err
    assert (
        node_plain.query("SELECT count() FROM system.databases WHERE name = 'e3_db'").strip()
        == "1"
    )

    # Granting CLUSTER is what unblocks it, proving CLUSTER was the missing privilege and
    # not something incidental.
    node_plain.query("GRANT CLUSTER ON *.* TO e3_user")
    node_plain.query("DROP DATABASE e3_db ON CLUSTER plain_cluster", user="e3_user")
    assert (
        node_plain.query("SELECT count() FROM system.databases WHERE name = 'e3_db'").strip()
        == "0"
    )


def test_e4_explicit_on_cluster_requires_cluster_grant_when_configured(start_cluster):
    # The same on the configured server: only the clause added by the rewrite skips the checks.
    node_cfg.query("CREATE DATABASE e4_db", user="avnadmin")
    node_cfg.query("CREATE USER e4_user IDENTIFIED WITH no_password")
    node_cfg.query("GRANT DROP DATABASE ON e4_db.* TO e4_user")

    err = node_cfg.query_and_get_error(
        "DROP DATABASE e4_db ON CLUSTER cluster_db", user="e4_user"
    )
    assert _denied(err), err
    assert (
        node_cfg.query("SELECT count() FROM system.databases WHERE name = 'e4_db'").strip()
        == "1"
    )

    node_cfg.query("DROP DATABASE e4_db", user="e4_user")
    assert (
        node_cfg.query("SELECT count() FROM system.databases WHERE name = 'e4_db'").strip()
        == "0"
    )


def test_e5_forced_drop_as_the_initial_user(start_cluster):
    # The hosts execute the forced statement as its user, so they must drop locally rather
    # than force the clause again.
    node_initial.query("CREATE DATABASE e5_db")
    node_initial.query("CREATE USER e5_user IDENTIFIED WITH no_password")
    node_initial.query("GRANT DROP DATABASE ON e5_db.* TO e5_user")

    node_initial.query(
        "DROP DATABASE e5_db",
        user="e5_user",
        # The queued statement names its user from entry format version 8.
        settings={"distributed_ddl_task_timeout": 30, "distributed_ddl_entry_format_version": 8},
    )
    assert (
        node_initial.query(
            "SELECT count() FROM system.databases WHERE name = 'e5_db'"
        ).strip()
        == "0"
    )
    assert (
        node_initial.query(
            "SELECT uniqExact(entry) FROM system.distributed_ddl_queue WHERE query LIKE '%DROP DATABASE%e5_db%'"
        ).strip()
        == "1"
    )


def test_f_non_escalation(start_cluster):
    # HARD GATE. After avnadmin performs the indirect (internally elevated) CREATE, its own
    # session must still NOT be a superuser. If this fails the elevation leaked -> STOP.
    node_cfg.query("CREATE DATABASE db_f", user="avnadmin")

    # Both inner elevated statements must have run: the CREATE and the GRANT. A failure of
    # the second would surface only here, so assert avnadmin actually received the set.
    grants_f = node_cfg.query("SHOW GRANTS FOR avnadmin")
    assert "db_f" in grants_f, f"inner GRANT did not land: {grants_f}"

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
    # The grantee is an identifier and must be back-quoted, not string-escaped. A grantee
    # name containing a comma would, under a string-escaped form, parse as two grantees
    # (or a syntax error). With backQuote it lands on exactly one grantee.
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


# The database name reaches three sinks in the composed statements: the ZooKeeper path via
# escapeForFileName, and two independent backQuote interpolations in the CREATE and the
# GRANT. A name that escapes any one of them would corrupt a statement the server wrote on
# the user's behalf, so the hostile-name surface is wider than the grantee name above.
@pytest.mark.parametrize(
    "db_name",
    [
        "`inj-db`",
        "`inj db`",
        "`inj.db`",
        "`inj,db`",
        "`inj'db`",
        "`inj\"db`",
        "`inj\\`db`",
        "`inj\\\\db`",
        "`inj\\ndb`",
        "`inj\\tdb`",
        "`inj ?.,2[}<>+=-!@#$%^&*()::~|_db`",
    ],
)
def test_i_database_name_injection(start_cluster, db_name):
    try:
        node_cfg.query(f"CREATE DATABASE {db_name}", user="avnadmin")
        show = node_cfg.query(f"SHOW CREATE DATABASE {db_name}")
        assert "Replicated" in show, show
    finally:
        node_cfg.query(f"DROP DATABASE IF EXISTS {db_name}")


def _grants_on(user, database):
    return node_cfg.query(
        f"SELECT count() FROM system.grants WHERE user_name = '{user}' AND database = '{database}'"
    ).strip()


def test_j_if_not_exists_on_existing_database(start_cluster):
    # The default set is granted only on a database the statement itself created. On an
    # existing database `IF NOT EXISTS` is a no-op, also when it comes from the session
    # setting, and the plain form fails.
    node_cfg.query("CREATE DATABASE j_existing")  # admin, Atomic
    node_cfg.query("CREATE DATABASE IF NOT EXISTS j_existing", user="avnadmin")
    node_cfg.query(
        "CREATE DATABASE j_existing", user="avnadmin", settings={"create_if_not_exists": 1}
    )
    err = node_cfg.query_and_get_error("CREATE DATABASE j_existing", user="avnadmin")
    assert "already exists" in err, err
    assert _grants_on("avnadmin", "j_existing") == "0"
    assert (
        node_cfg.query("SELECT engine FROM system.databases WHERE name = 'j_existing'").strip()
        == "Atomic"
    )


@pytest.mark.parametrize("db_name", ["system", "information_schema", "INFORMATION_SCHEMA"])
def test_j2_predefined_database_refused(start_cluster, db_name):
    for query in (f"CREATE DATABASE {db_name}", f"CREATE DATABASE IF NOT EXISTS {db_name}"):
        err = node_cfg.query_and_get_error(query, user="avnadmin")
        assert "reserved" in err, err
    assert _grants_on("avnadmin", db_name) == "0"

    node_cfg.query("CREATE USER IF NOT EXISTS j2_grantee IDENTIFIED WITH no_password")
    err = node_cfg.query_and_get_error(
        f"GRANT DEFAULT REPLICATED DATABASE PRIVILEGES ON {db_name}.* TO j2_grantee"
    )
    assert "cannot be granted on database" in err, err
    assert _grants_on("j2_grantee", db_name) == "0"


def test_j3_attach_of_detached_database(start_cluster):
    # ATTACH DATABASE takes the same interception as CREATE. A database detached by an
    # admin is not in the catalog, so the host fails on the leftover metadata instead, and
    # that failure must reach the caller without a grant.
    node_cfg.query("CREATE DATABASE j3_db")
    node_cfg.query("DETACH DATABASE j3_db")
    try:
        err = node_cfg.query_and_get_error("ATTACH DATABASE j3_db", user="avnadmin")
        assert "already exists" in err or "DATABASE_ALREADY_EXISTS" in err, err
        assert _grants_on("avnadmin", "j3_db") == "0"
    finally:
        node_cfg.query("ATTACH DATABASE IF NOT EXISTS j3_db")
    assert (
        node_cfg.query("SELECT engine FROM system.databases WHERE name = 'j3_db'").strip()
        == "Atomic"
    )


def test_k_grant_shortcut_refuses_on_cluster(start_cluster):
    # The shortcut runs locally only; an ON CLUSTER clause is refused rather than dropped.
    node_cfg.query("CREATE USER k_grantee IDENTIFIED WITH no_password")
    err = node_cfg.query_and_get_error(
        "GRANT ON CLUSTER cluster_db DEFAULT REPLICATED DATABASE PRIVILEGES ON k_db.* TO k_grantee"
    )
    assert "cannot be executed on cluster" in err, err
    assert _grants_on("k_grantee", "k_db") == "0"


def test_k2_grant_shortcut_formats_as_itself(start_cluster):
    # Statements which are formatted and parsed again, like `PARALLEL WITH`, keep the shortcut
    # and grant its privileges, not `ALL`.
    for statement in [
        "GRANT DEFAULT REPLICATED DATABASE PRIVILEGES ON k2_db.* TO k2_grantee",
        "GRANT DEFAULT REPLICATED DATABASE PRIVILEGES ON k2_db.* TO k2_grantee WITH GRANT OPTION",
    ]:
        assert node_cfg.query(f"SELECT formatQuery('{statement}')").strip() == statement

    node_cfg.query("CREATE USER k2_grantee, k2_other IDENTIFIED WITH no_password")
    node_cfg.query(
        "GRANT DEFAULT REPLICATED DATABASE PRIVILEGES ON k2_db.* TO k2_grantee "
        "PARALLEL WITH GRANT SELECT ON k2_db.* TO k2_other"
    )
    grants = node_cfg.query("SHOW GRANTS FOR k2_grantee")
    assert "GRANT ALL" not in grants, grants
    assert "DROP DATABASE" in grants and "SELECT" in grants, grants


def test_k3_grant_shortcut_refuses_replace_option(start_cluster):
    # The shortcut adds to the grants of its grantee; it cannot replace them.
    node_cfg.query("CREATE USER k3_grantee IDENTIFIED WITH no_password")
    node_cfg.query("GRANT SELECT ON k3_other.* TO k3_grantee")
    err = node_cfg.query_and_get_error(
        "GRANT DEFAULT REPLICATED DATABASE PRIVILEGES ON k3_db.* TO k3_grantee WITH REPLACE OPTION"
    )
    assert "WITH REPLACE OPTION cannot be used" in err, err
    err = node_cfg.query_and_get_error(
        "GRANT DEFAULT REPLICATED DATABASE PRIVILEGES ON k3_db.* TO k3_grantee WITH ADMIN OPTION"
    )
    assert "ADMIN OPTION should be specified for roles" in err, err
    assert _grants_on("k3_grantee", "k3_db") == "0"
    assert _grants_on("k3_grantee", "k3_other") == "1"


K4_USERS_CONFIG = "/etc/clickhouse-server/users.d/k4_grant_shortcut.xml"


def test_k4_grant_shortcut_in_users_config_is_rejected(start_cluster):
    # A users config grant has no grantee, and the shortcut needs one. The config is refused
    # and the server keeps running.
    node_plain.exec_in_container(
        [
            "bash",
            "-c",
            f"""cat > {K4_USERS_CONFIG} <<'EOF'
<clickhouse>
    <users>
        <k4_user>
            <no_password/>
            <grants>
                <query>GRANT DEFAULT REPLICATED DATABASE PRIVILEGES ON k4_db.*</query>
            </grants>
        </k4_user>
    </users>
</clickhouse>
EOF""",
        ]
    )
    try:
        node_plain.query_and_get_answer_with_error("SYSTEM RELOAD CONFIG")
        assert node_plain.query("SELECT 1").strip() == "1"
        assert (
            node_plain.query(
                "SELECT count() FROM system.grants WHERE user_name = 'k4_user'"
            ).strip()
            == "0"
        )
    finally:
        node_plain.exec_in_container(["rm", "-f", K4_USERS_CONFIG])
        node_plain.query("SYSTEM RELOAD CONFIG")
    assert node_plain.query("SELECT 1").strip() == "1"


def test_t_truncate_database_requires_drop_database(start_cluster):
    # TRUNCATE DATABASE needs DROP DATABASE. Replicated databases do not implement it, so
    # use an Atomic one; cluster_db spans node_cfg only.
    node_cfg.query("CREATE DATABASE t_db")
    node_cfg.query("CREATE TABLE t_db.t (x UInt32) ENGINE = MergeTree ORDER BY x")
    node_cfg.query("INSERT INTO t_db.t VALUES (1)")
    node_cfg.query("CREATE USER t_user IDENTIFIED WITH no_password")

    err = node_cfg.query_and_get_error("TRUNCATE DATABASE t_db", user="t_user")
    assert _denied(err), err
    assert node_cfg.query("SELECT count() FROM t_db.t").strip() == "1"

    node_cfg.query("GRANT DROP DATABASE ON t_db.* TO t_user")
    node_cfg.query("TRUNCATE DATABASE t_db", user="t_user")
    assert node_cfg.query("EXISTS TABLE t_db.t").strip() == "0"


def test_w_host_failure_is_forwarded(start_cluster):
    # The composed CREATE waits for every host and forwards the first failure. Make the host
    # fail where the initiator's own check cannot see it: another database already holds
    # this replica under the ZooKeeper path the composed statement derives from the name.
    node_cfg.query(
        "CREATE DATABASE w_holder ENGINE = "
        "Replicated('/clickhouse/databases/w_taken', '{shard}', '{replica}')"
    )
    err = node_cfg.query_and_get_error("CREATE DATABASE w_taken", user="avnadmin")
    assert "REPLICA_ALREADY_EXISTS" in err or "already exists" in err, err
    assert (
        node_cfg.query("SELECT count() FROM system.databases WHERE name = 'w_taken'").strip()
        == "0"
    )
    assert _grants_on("avnadmin", "w_taken") == "0"


def test_r_default_off_create(start_cluster):
    # With the feature unconfigured, normal CREATE DATABASE behaves exactly as upstream.
    node_plain.query("CREATE DATABASE r_db")
    assert (
        node_plain.query("SELECT count() FROM system.databases WHERE name = 'r_db'").strip()
        == "1"
    )

    # The indirect path is inert: even a user literally named avnadmin gets no special
    # treatment because aiven_user_with_indirect_database_creation is empty here.
    node_plain.query("CREATE USER avnadmin IDENTIFIED WITH no_password")
    err = node_plain.query_and_get_error(
        "CREATE DATABASE r_db2 ENGINE = Replicated('/x', 's', '{replica}')",
        user="avnadmin",
    )
    assert _denied(err), err


def test_r2_default_off_drop(start_cluster):
    # With aiven_cluster_database EMPTY, a restricted non-admin user can DROP DATABASE
    # without ON CLUSTER exactly as upstream — the enforcement block is a no-op.
    node_plain.query("CREATE DATABASE r2_db")
    node_plain.query("CREATE USER r2_user IDENTIFIED WITH no_password")
    node_plain.query("GRANT DROP DATABASE ON r2_db.* TO r2_user")

    node_plain.query("DROP DATABASE r2_db", user="r2_user")
    assert (
        node_plain.query("SELECT count() FROM system.databases WHERE name = 'r2_db'").strip()
        == "0"
    )


def test_p_protected_caller(start_cluster):
    # In production the configured user is a protected user. The composed GRANT targets it,
    # so it must run with an access that may change a protected user.
    node_cfg.query("ALTER USER avnadmin PROTECTED")
    try:
        node_cfg.query("CREATE DATABASE p_db", user="avnadmin")
        assert _grants_on("avnadmin", "p_db") != "0"
        # The caller itself still may not change protected users.
        err = node_cfg.query_and_get_error(
            "GRANT SELECT ON p_db.* TO avnadmin", user="avnadmin"
        )
        assert _denied(err), err
    finally:
        node_cfg.query("ALTER USER avnadmin NOT PROTECTED")

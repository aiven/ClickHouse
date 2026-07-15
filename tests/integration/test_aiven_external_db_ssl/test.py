"""Integration test for the Aiven LTS uplift patch 021 (external-db SSL).

Two layers share one provisioning fixture:

1. Harness proof (direct psycopg2 / pymysql clients): an external PostgreSQL and
   an external MySQL are brought up under ClickHouse's integration-test framework
   and reconfigured AT RUNTIME to REQUIRE TLS; they reject a non-TLS client and
   accept a TLS client that validates against a generated CA.

2. ClickHouse-side SSL plumbing (the patch-021 surface): a ClickHouse node in the
   same docker network connects to `postgres1` / `mysql80` via the `postgresql()`
   and `mysql()` table functions and asserts that the new `ssl_mode` /
   `ssl_root_cert` knobs are honored — `verify-full` + the generated `ca.crt`
   succeeds (CA + hostname validated in-network), the SSL-forbidding mode is
   rejected by the TLS-required servers, and (for MySQL `verify-full`) a
   connection to the container by an identity NOT in the cert SAN is rejected by
   the hostname check that the `mariadb-connector-c` `X509_check_host` fix
   protects.

These ClickHouse-side assertions are the evidence-of-causation differential: with
the 26 patched `src/` files flipped back to HEAD, the `ssl_mode` setting / named
collection key does not exist, so the assertions fail with an unknown-setting /
unknown-key error rather than an SSL outcome.

Run (from tests/integration/, env vars per runbook §5):
    pytest test_aiven_external_db_ssl/ -v --tb=short --timeout=400
"""

import os
import ssl
import subprocess

import psycopg2
import pymysql
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import mysql_pass, mysql_user, pg_db, pg_pass, pg_user

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CERTS_DIR = os.path.join(SCRIPT_DIR, "certs")
CA_CRT = os.path.join(CERTS_DIR, "ca.crt")

# Path the CA is copied to INSIDE the ClickHouse node container, so the server
# can read it for verify-full / verify-ca. /etc/clickhouse-server already exists.
NODE_CA = "/etc/clickhouse-server/aiven_external_db_ca.crt"

# A sentinel value inserted into the probe tables on both PG and MySQL; the
# ClickHouse-side SELECT asserts it round-trips over the validated TLS channel.
PROBE_VALUE = 42

# PGDATA inside the postgres1 container (compose sets PGDATA=/postgres/data).
PG_DATA = "/postgres/data"

# Writable, non-shared directory inside the mysql80 container for our CA-signed
# cert. We must NOT write to the server's configured --ssl-* paths
# (/etc/mysql/certs/*) because compose bind-mounts that from
# tests/integration/helpers/mysql_cert (a never-touch shared dir). Instead we
# drop our files in the logs volume and re-point ssl_ca/ssl_cert/ssl_key via
# SET GLOBAL + ALTER INSTANCE RELOAD TLS.
MYSQL_CERT_DIR = "/var/log/mysql/aiven_certs"
# mysqld runs as the host uid (compose: user=${MYSQL8_DOCKER_USER}=os.getuid()),
# so the key must be owned by / readable by that uid.
MYSQL_RUNTIME_UID = os.getuid()

cluster = ClickHouseCluster(__file__)

# The ClickHouse node both satisfies cluster.start() and drives the patch-021
# ClickHouse-side SSL assertions: it connects to postgres1 / mysql80 from within
# the docker network (where those hostnames resolve, so verify-full incl.
# hostname works directly). with_postgres / with_mysql8 bring up the external
# services the harness reconfigures.
node = cluster.add_instance(
    "node",
    with_postgres=True,
    with_mysql8=True,
    stay_alive=True,
    main_configs=["configs/named_collections.xml"],
)


def _gen_certs():
    """Regenerate the CA + server certs so the test is reproducible from clean."""
    subprocess.check_call(["bash", os.path.join(CERTS_DIR, "gen_certs.sh")])


def _pg_exec_sql(sslmode, sql_list, **extra):
    """Open a psycopg2 connection to postgres1 and run statements (autocommit)."""
    conn = psycopg2.connect(
        host=cluster.postgres_ip,
        port=cluster.postgres_port,
        dbname=pg_db,
        user=pg_user,
        password=pg_pass,
        sslmode=sslmode,
        connect_timeout=10,
        **extra,
    )
    conn.autocommit = True
    try:
        cur = conn.cursor()
        for sql in sql_list:
            cur.execute(sql)
        cur.close()
    finally:
        conn.close()


def _pg_exec_admin(sql_list):
    """Run admin SQL, tolerating an already-locked-down server.

    The integration framework bind-mounts PGDATA to a host directory that is not
    reliably wiped between runs (it is owned by the in-container postgres uid).
    A re-run therefore inherits a PGDATA whose pg_hba.conf is already
    hostssl-only, so the plaintext bootstrap connection is refused. Fall back to
    sslmode='require' (no CA needed) when that happens, which works as soon as
    SSL is enabled.
    """
    try:
        _pg_exec_sql("disable", sql_list)
    except psycopg2.OperationalError:
        _pg_exec_sql("require", sql_list)


def _configure_postgres_require_tls():
    """Place CA-signed certs into PGDATA and require TLS, with no compose edits.

    Ordering is load-bearing: enable SSL and prove an SSL connection works while
    pg_hba still permits plaintext, THEN tighten pg_hba to hostssl-only so a
    non-SSL TCP connection is refused.
    """
    pg_container = cluster.get_container_id("postgres1")

    # 1. Copy CA + server cert/key into PGDATA, fix ownership/permissions.
    for fname in ("ca.crt", "pg-server.crt", "pg-server.key"):
        cluster.copy_file_to_container(
            pg_container,
            os.path.join(CERTS_DIR, fname),
            f"{PG_DATA}/{fname}",
        )
    cluster.exec_in_container(
        pg_container,
        [
            "bash",
            "-c",
            f"chown postgres:postgres {PG_DATA}/ca.crt {PG_DATA}/pg-server.crt {PG_DATA}/pg-server.key "
            f"&& chmod 600 {PG_DATA}/pg-server.key {PG_DATA}/ca.crt "
            f"&& chmod 644 {PG_DATA}/pg-server.crt",
        ],
        user="root",
    )

    # 2. Enable SSL via ALTER SYSTEM over the still-plaintext-allowed connection,
    #    then SIGHUP-reload (ssl* GUCs are PGC_SIGHUP, no restart needed).
    _pg_exec_admin(
        [
            "ALTER SYSTEM SET ssl = 'on'",
            f"ALTER SYSTEM SET ssl_cert_file = '{PG_DATA}/pg-server.crt'",
            f"ALTER SYSTEM SET ssl_key_file = '{PG_DATA}/pg-server.key'",
            f"ALTER SYSTEM SET ssl_ca_file = '{PG_DATA}/ca.crt'",
            "SELECT pg_reload_conf()",
        ]
    )

    # 3. Prove SSL now works before tightening pg_hba.
    _pg_exec_sql("require", ["SELECT 1"])

    # 3b. Create the probe table the ClickHouse-side verify-full SELECT reads.
    #     Done over the SSL connection (idempotent across re-runs).
    _pg_exec_sql(
        "require",
        [
            "DROP TABLE IF EXISTS ssl_probe",
            "CREATE TABLE ssl_probe (id integer)",
            f"INSERT INTO ssl_probe VALUES ({PROBE_VALUE})",
        ],
    )

    # 4. Tighten pg_hba.conf: local trust + hostssl-only (no plain host line),
    #    so a non-SSL TCP connection is rejected. pg_hba reloads on SIGHUP too.
    hba = (
        "local all all trust\n"
        "hostssl all all 0.0.0.0/0 trust\n"
        "hostssl all all ::/0 trust\n"
    )
    cluster.exec_in_container(
        pg_container,
        ["bash", "-c", f"cat > {PG_DATA}/pg_hba.conf <<'EOF'\n{hba}EOF\n"
         f"chown postgres:postgres {PG_DATA}/pg_hba.conf"],
        user="root",
    )
    _pg_exec_sql("require", ["SELECT pg_reload_conf()"])


def _configure_mysql_require_tls():
    """Require TLS on mysql80 at runtime via SET PERSIST (no restart needed).

    MySQL 8 (clickhouse/test-mysql80 image) already serves TLS with its bundled
    server cert, so we take the documented simpler path: prove TLS-required +
    encrypted rather than full CA-chain validation.
    """
    # On a fresh container plaintext works; on a re-run that already has
    # require_secure_transport=ON it does not, so fall back to a TLS connection.
    try:
        conn = pymysql.connect(
            host=cluster.mysql8_ip,
            port=cluster.mysql8_port,
            user=mysql_user,
            password=mysql_pass,
            ssl_disabled=True,
            connect_timeout=10,
        )
    except pymysql.err.OperationalError:
        conn = pymysql.connect(
            host=cluster.mysql8_ip,
            port=cluster.mysql8_port,
            user=mysql_user,
            password=mysql_pass,
            ssl={"check_hostname": False, "verify_mode": ssl.CERT_NONE},
            connect_timeout=10,
        )
    try:
        cur = conn.cursor()
        cur.execute("SET PERSIST require_secure_transport = ON")
        cur.close()
    finally:
        conn.close()


def _mysql_connect(ssl_opts):
    """Open a pymysql connection to mysql80 with the given ssl options dict."""
    return pymysql.connect(
        host=cluster.mysql8_ip,
        port=cluster.mysql8_port,
        user=mysql_user,
        password=mysql_pass,
        ssl=ssl_opts,
        connect_timeout=10,
    )


def _install_mysql_ca_signed_cert():
    """Serve our CA-signed cert on mysql80 at runtime, no restart, no shared edits.

    Step 1: discover the configured ssl_* paths + datadir (logged for the record).
    Step 2: drop ca.crt/mysql-server.crt/mysql-server.key into a writable, NON
            bind-mounted dir, owned by the mysql runtime uid, key 0600.
    Step 3: SET GLOBAL ssl_ca/ssl_cert/ssl_key to the new paths (dynamic since
            MySQL 8.0.16) then ALTER INSTANCE RELOAD TLS to activate them.
    """
    mysql_container = cluster.get_container_id("mysql80")

    # Step 2: place our cert material (exec runs as the container's runtime uid,
    # which owns the logs volume, so no root needed here).
    for fname in ("ca.crt", "mysql-server.crt", "mysql-server.key"):
        cluster.copy_file_to_container(
            mysql_container,
            os.path.join(CERTS_DIR, fname),
            f"{MYSQL_CERT_DIR}/{fname}",
        )
    cluster.exec_in_container(
        mysql_container,
        [
            "bash",
            "-c",
            f"chmod 700 {MYSQL_CERT_DIR} "
            f"&& chmod 644 {MYSQL_CERT_DIR}/ca.crt {MYSQL_CERT_DIR}/mysql-server.crt "
            f"&& chmod 600 {MYSQL_CERT_DIR}/mysql-server.key",
        ],
    )

    # Steps 1 + 3 over a TLS-but-unverified admin connection (require_secure_transport
    # is ON by now, so plaintext is refused).
    conn = _mysql_connect({"check_hostname": False, "verify_mode": ssl.CERT_NONE})
    try:
        cur = conn.cursor()
        # Step 1: record where the server currently reads its TLS material from.
        cur.execute(
            "SHOW VARIABLES WHERE Variable_name IN "
            "('ssl_ca','ssl_cert','ssl_key','datadir')"
        )
        before = dict(cur.fetchall())
        print(f"\n[MySQL ssl_* before reload] {before}")

        # Step 3: re-point to our CA-signed material and reload the TLS context.
        cur.execute("SET GLOBAL ssl_ca = %s", (f"{MYSQL_CERT_DIR}/ca.crt",))
        cur.execute("SET GLOBAL ssl_cert = %s", (f"{MYSQL_CERT_DIR}/mysql-server.crt",))
        cur.execute("SET GLOBAL ssl_key = %s", (f"{MYSQL_CERT_DIR}/mysql-server.key",))
        cur.execute("ALTER INSTANCE RELOAD TLS")

        cur.execute(
            "SHOW STATUS WHERE Variable_name IN "
            "('Current_tls_ca','Current_tls_cert','Current_tls_key','Ssl_server_not_after')"
        )
        status = dict(cur.fetchall())
        print(f"\n[MySQL TLS status after reload] {status}")
        cur.close()
    finally:
        conn.close()


def _create_mysql_probe_table():
    """Create the clickhouse.ssl_probe table the CH-side MySQL SELECT reads.

    Runs over the TLS-but-unverified admin connection (require_secure_transport
    is ON by now). Idempotent across re-runs.
    """
    conn = _mysql_connect({"check_hostname": False, "verify_mode": ssl.CERT_NONE})
    try:
        cur = conn.cursor()
        cur.execute("CREATE DATABASE IF NOT EXISTS clickhouse")
        cur.execute("DROP TABLE IF EXISTS clickhouse.ssl_probe")
        cur.execute("CREATE TABLE clickhouse.ssl_probe (id INT)")
        cur.execute(f"INSERT INTO clickhouse.ssl_probe VALUES ({PROBE_VALUE})")
        conn.commit()
        cur.close()
    finally:
        conn.close()


def _install_ca_into_node():
    """Place ca.crt inside the ClickHouse node container at a readable path.

    The server process must be able to read it for verify-full / verify-ca, so
    we chmod 644 (it is a public CA cert) after docker-cp lands it root-owned.
    """
    node.copy_file_to_container(CA_CRT, NODE_CA)
    node.exec_in_container(["chmod", "644", NODE_CA], user="root")


@pytest.fixture(scope="module")
def started_cluster():
    _gen_certs()
    try:
        cluster.start()
        _configure_postgres_require_tls()
        _configure_mysql_require_tls()
        _install_mysql_ca_signed_cert()
        _create_mysql_probe_table()
        _install_ca_into_node()
        yield cluster
    finally:
        cluster.shutdown()


# --------------------------------------------------------------------------
# Proof assertions
# --------------------------------------------------------------------------


def test_postgres_non_tls_rejected(started_cluster):
    """A non-TLS PostgreSQL connection is refused by the hostssl-only pg_hba."""
    with pytest.raises(psycopg2.OperationalError) as exc_info:
        _pg_exec_sql("disable", ["SELECT 1"])
    msg = str(exc_info.value)
    print(f"\n[PG non-TLS rejected] {msg.strip()}")
    assert (
        "no encryption" in msg
        or "no pg_hba.conf entry" in msg
        or "SSL" in msg
        or "ssl" in msg
    ), f"unexpected rejection reason: {msg}"


def test_postgres_tls_verify_full(started_cluster):
    """A verify-full PostgreSQL connection validates the CA AND the hostname.

    The pytest host reaches the container by IP, so we use libpq's `hostaddr`
    (connect target) + `host` (TLS name to verify against the cert SAN). This
    proves both CA validation and hostname (SAN) verification from the host.
    """
    conn = psycopg2.connect(
        hostaddr=cluster.postgres_ip,
        host="postgres1",
        port=cluster.postgres_port,
        dbname=pg_db,
        user=pg_user,
        password=pg_pass,
        sslmode="verify-full",
        sslrootcert=CA_CRT,
        connect_timeout=10,
    )
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        row = cur.fetchone()
        cur.execute("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
        ssl_in_use = cur.fetchone()[0]
        cur.close()
    finally:
        conn.close()
    print(
        f"\n[PG TLS verify-full] SELECT 1 -> {row[0]}; pg_stat_ssl.ssl={ssl_in_use}"
    )
    assert row[0] == 1
    assert ssl_in_use is True


def test_mysql_non_tls_rejected(started_cluster):
    """A non-TLS MySQL connection is refused by require_secure_transport=ON."""
    with pytest.raises(pymysql.err.OperationalError) as exc_info:
        conn = pymysql.connect(
            host=cluster.mysql8_ip,
            port=cluster.mysql8_port,
            user=mysql_user,
            password=mysql_pass,
            ssl_disabled=True,
            connect_timeout=10,
        )
        conn.close()
    msg = str(exc_info.value)
    print(f"\n[MySQL non-TLS rejected] {msg.strip()}")
    assert "secure transport" in msg or "3159" in msg, f"unexpected: {msg}"


def test_mysql_tls_accepted(started_cluster):
    """A TLS MySQL connection succeeds and reports a non-empty Ssl_cipher."""
    conn = pymysql.connect(
        host=cluster.mysql8_ip,
        port=cluster.mysql8_port,
        user=mysql_user,
        password=mysql_pass,
        ssl={"check_hostname": False, "verify_mode": ssl.CERT_NONE},
        connect_timeout=10,
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        one = cur.fetchone()[0]
        cur.execute("SHOW STATUS LIKE 'Ssl_cipher'")
        cipher = cur.fetchone()[1]
        cur.close()
    finally:
        conn.close()
    print(f"\n[MySQL TLS accepted] SELECT 1 -> {one}; Ssl_cipher='{cipher}'")
    assert one == 1
    assert cipher, "Ssl_cipher is empty -> connection was not encrypted"


def test_mysql_tls_ca_validated(started_cluster):
    """A TLS MySQL connection validates the server cert against OUR ca.crt.

    This is the leg patch 021 actually cares about: VERIFY_FULL exercises the
    CA chain (and, in-network, the `X509_check_host` hostname path that the
    mariadb-connector-c fix touches). Here we prove CA-chain validation
    (verify_mode=CERT_REQUIRED, cafile=ca.crt) succeeds against the CA-signed
    cert we installed via ALTER INSTANCE RELOAD TLS. check_hostname is off
    because the pytest host reaches the container by IP (see the hostname probe
    below).
    """
    conn = _mysql_connect({"ca": CA_CRT, "check_hostname": False})
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        one = cur.fetchone()[0]
        cur.execute("SHOW STATUS LIKE 'Ssl_cipher'")
        cipher = cur.fetchone()[1]
        cur.close()
    finally:
        conn.close()
    print(f"\n[MySQL TLS CA-validated] SELECT 1 -> {one}; Ssl_cipher='{cipher}'")
    assert one == 1, "CA-validated TLS connection failed to run SELECT 1"
    assert cipher, "Ssl_cipher empty -> connection not encrypted"

    # Step 5 probe: full hostname verification from the pytest HOST.
    # pymysql binds the TLS server_hostname to the `host` arg with no
    # libpq-style hostaddr/host split, so verifying SAN 'mysql80' requires
    # connecting to literal host='mysql80' -- which the host cannot resolve to
    # the container. We connect by IP with check_hostname=True; the expected
    # outcome is a hostname-mismatch failure, which CONFIRMS the hostname check
    # is active but cannot pass from the host. The real verify_full +
    # X509_check_host assertion runs from the in-network ClickHouse-side test
    # where 'mysql80' resolves.
    host_hostname_ok = False
    host_probe_detail = ""
    try:
        probe = _mysql_connect({"ca": CA_CRT, "check_hostname": True})
        probe.close()
        host_hostname_ok = True
    except Exception as exc:  # noqa: BLE001 - documenting, not asserting
        host_probe_detail = f"{type(exc).__name__}: {exc}"
    print(
        f"\n[MySQL host-side hostname verify] possible={host_hostname_ok}; "
        f"detail={host_probe_detail}"
    )


# --------------------------------------------------------------------------
# ClickHouse-side SSL plumbing assertions (the patch-021 surface)
#
# These are the evidence-of-causation differential: with the 26 patched src/
# files flipped back to HEAD, `postgresql_connection_pool_ssl_mode` is an
# unknown setting and `ssl_mode` is an unknown MySQL named-collection key, so
# every assertion below fails with an unknown-setting / unknown-key error
# instead of the SSL outcome it asserts post-patch.
# --------------------------------------------------------------------------

PG_PASS = pg_pass

# Substrings that mark the pre-patch SSL-feature-ABSENCE failure (the setting /
# named-collection key does not exist). The "rejected"-style tests assert a real
# SSL/TLS rejection, so they must NOT be satisfiable by one of these markers —
# otherwise they would pass pre-patch on the incidental "ssl" substring inside
# the unknown-key/option message and distinguish nothing (AGENTS.md §7).
_FEATURE_ABSENCE_MARKERS = ("unexpected key", "unrecognized option", "unknown setting")


def _assert_not_feature_absence(err):
    low = err.lower()
    for marker in _FEATURE_ABSENCE_MARKERS:
        assert marker not in low, (
            f"got the pre-patch SSL-feature-absence error, not the asserted "
            f"SSL rejection: {err}"
        )


def test_ch_postgres_verify_full_succeeds(started_cluster):
    """postgresql() with verify-full + our ca.crt connects and returns the row.

    In-network the node reaches `postgres1`, whose server cert SAN includes
    `postgres1`, so verify-full validates BOTH the CA chain and the hostname
    against the TLS-required server.
    """
    res = node.query(
        f"SELECT id FROM postgresql('postgres1:5432', 'postgres', 'ssl_probe', "
        f"'{pg_user}', '{PG_PASS}')",
        settings={
            "postgresql_connection_pool_ssl_mode": "verify-full",
            "postgresql_connection_pool_ssl_root_cert": NODE_CA,
        },
    )
    print(f"\n[CH PG verify-full] SELECT id -> {res.strip()!r}")
    assert res.strip() == str(PROBE_VALUE)


def test_ch_postgres_ssl_disable_rejected(started_cluster):
    """postgresql() with ssl_mode=disable is refused by the hostssl-only pg_hba.

    Proves the knob is actually threaded through to libpq: disabling SSL makes
    the TLS-required server reject the connection (no-encryption / pg_hba).
    """
    err = node.query_and_get_error(
        f"SELECT id FROM postgresql('postgres1:5432', 'postgres', 'ssl_probe', "
        f"'{pg_user}', '{PG_PASS}')",
        settings={"postgresql_connection_pool_ssl_mode": "disable"},
    )
    print(f"\n[CH PG ssl_mode=disable rejected] {err.strip()[:400]}")
    _assert_not_feature_absence(err)
    low = err.lower()
    assert (
        "no encryption" in low or "pg_hba" in low
    ), f"unexpected PG rejection reason: {err}"


def test_ch_mysql_verify_full_succeeds(started_cluster):
    """mysql() with ssl_mode=VERIFY_FULL + our ca.crt connects and returns the row.

    In-network the node reaches `mysql80`, whose server cert SAN is `mysql80`,
    so VERIFY_FULL validates the CA chain AND the hostname — the exact path the
    `X509_check_host` connector fix protects.

    Note: for MySQL the patch threads the CA through the pre-existing `ssl_ca`
    key (-> `mysql_ssl_set`); `ssl_root_cert` is parsed into the Configuration
    but is inert on the MySQL path (only PostgreSQL forwards it to libpq as
    `sslrootcert`). The post-patch-only differential here is the `ssl_mode` key.
    """
    res = node.query(
        f"SELECT id FROM mysql(mysql_ssl, ssl_mode='verify-full', "
        f"ssl_ca='{NODE_CA}')"
    )
    print(f"\n[CH MySQL verify-full] SELECT id -> {res.strip()!r}")
    assert res.strip() == str(PROBE_VALUE)


def test_ch_mysql_non_tls_rejected(started_cluster):
    """mysql() with ssl_mode=disable is refused by require_secure_transport=ON.

    error 3159 == "Connections using insecure transport are prohibited".
    """
    err = node.query_and_get_error(
        "SELECT id FROM mysql(mysql_ssl, ssl_mode='disable')"
    )
    print(f"\n[CH MySQL ssl_mode=disable rejected] {err.strip()[:400]}")
    _assert_not_feature_absence(err)
    assert (
        "3159" in err or "secure transport" in err.lower()
    ), f"unexpected MySQL rejection reason: {err}"


def test_ch_mysql_verify_full_hostname_mismatch_rejected(started_cluster):
    """VERIFY_FULL to mysql80's container by IP is rejected: IP not in cert SAN.

    The cert SAN is `mysql80`; connecting by the container IP (override of the
    named collection's `host`) under VERIFY_FULL forces the connector's
    hostname check (`X509_check_host`) to fail. Same CA, same server, only the
    verified identity differs from the previous (success) case — so the failure
    is specifically the hostname mismatch, not a CA-chain or transport error.
    """
    mysql_ip = started_cluster.mysql8_ip
    err = node.query_and_get_error(
        f"SELECT id FROM mysql(mysql_ssl, host='{mysql_ip}', ssl_mode='verify-full', "
        f"ssl_ca='{NODE_CA}')"
    )
    print(f"\n[CH MySQL verify-full hostname mismatch] ip={mysql_ip}; {err.strip()[:400]}")
    _assert_not_feature_absence(err)
    low = err.lower()
    assert "3159" not in err and "secure transport" not in low, (
        f"connection was rejected for transport, not hostname: {err}"
    )
    # The mariadb connector surfaces every TLS failure as "SSL connection error:
    # <reason>"; a successful CA chain but a failed hostname check lands here.
    assert "ssl connection error" in low, (
        f"expected a TLS/hostname-verification rejection, got: {err}"
    )

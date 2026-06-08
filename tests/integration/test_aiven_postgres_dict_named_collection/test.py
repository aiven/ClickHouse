import logging

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import pg_pass
from helpers.postgres_utility import get_postgres_conn

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    dictionaries=[
        "configs/dictionaries/postgres_xml_dict.xml",
    ],
    with_postgres=True,
)


def get_conn(started_cluster):
    # Default ("postgres") maintenance database; no SSL.
    return get_postgres_conn(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_xml_config_dictionary_is_rejected(started_cluster):
    """Enforcement leg.

    A PostgreSQL dictionary configured from a server config file (no named
    collection, ``created_from_ddl == false``) must be rejected by the Aiven
    patch with ``UNSUPPORTED_METHOD`` and the "must use a named collection"
    message. The backing PostgreSQL table is created and filled so that the
    PRE-patch binary would otherwise load it successfully against the real
    server — this is what makes the pre/post differential causal rather than
    an artifact of a missing table.
    """
    conn = get_conn(started_cluster)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS xml_enforced_table")
    cursor.execute("CREATE TABLE xml_enforced_table (id integer, value integer)")
    cursor.execute(
        "INSERT INTO xml_enforced_table SELECT i, i FROM generate_series(0, 9) as t(i)"
    )

    err = node1.query_and_get_error("SYSTEM RELOAD DICTIONARY xml_pg_dict")
    logging.debug("enforcement-leg error: %s", err)

    # Assert BOTH the error code and the Aiven-specific message (AGENTS §7):
    # UNSUPPORTED_METHOD on its own is a generic upstream code, so the message
    # substring is what distinguishes the Aiven enforcement gate.
    assert "UNSUPPORTED_METHOD" in err
    assert "PostgreSQL dictionary source configuration must use a named collection" in err

    cursor.execute("DROP TABLE IF EXISTS xml_enforced_table")


def test_ddl_named_collection_with_tls_key_loads(started_cluster):
    """DDL named-collection + TLS leg.

    A DDL-created named collection carrying ``ssl_root_cert`` — a key the
    shared ``StoragePostgreSQL::processNamedCollectionResult`` parser accepts
    but the dictionary's pre-patch ``dictionary_allowed_keys`` set rejects —
    must validate and load rows after the patch. PRE-patch the same dictionary
    throws ``BAD_ARGUMENTS`` ("Unexpected key `ssl_root_cert`") at load time,
    before any connection is attempted.

    ``ssl_root_cert`` is set without ``ssl_mode`` on purpose: the dictionary
    pool's SSL context falls back to the server-level settings (non-SSL by
    default) when ``configuration.ssl_mode`` is unset, so the load still
    succeeds against the plain test PostgreSQL while still exercising the
    TLS-key acceptance the patch enables.
    """
    conn = get_conn(started_cluster)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS tls_dict_table")
    cursor.execute("CREATE TABLE tls_dict_table (id integer, value integer)")
    cursor.execute(
        "INSERT INTO tls_dict_table SELECT i, i FROM generate_series(0, 9) as t(i)"
    )

    node1.query("DROP NAMED COLLECTION IF EXISTS pg_tls_coll")
    node1.query(
        f"""
        CREATE NAMED COLLECTION pg_tls_coll AS
            user = 'postgres',
            password = '{pg_pass}',
            host = 'postgres1',
            port = 5432,
            database = 'postgres',
            table = 'tls_dict_table',
            ssl_root_cert = '/etc/clickhouse-server/ssl/pg-root.crt'
        """
    )

    node1.query("DROP DICTIONARY IF EXISTS pg_tls_dict")
    node1.query(
        """
        CREATE DICTIONARY pg_tls_dict (id UInt32, value UInt32)
        PRIMARY KEY id
        SOURCE(POSTGRESQL(NAME pg_tls_coll))
        LIFETIME(MIN 1 MAX 2)
        LAYOUT(HASHED())
        """
    )

    result = node1.query("SELECT dictGetUInt32('pg_tls_dict', 'value', toUInt64(9))")
    assert int(result.strip()) == 9

    node1.query("DROP DICTIONARY IF EXISTS pg_tls_dict")
    node1.query("DROP NAMED COLLECTION IF EXISTS pg_tls_coll")
    cursor.execute("DROP TABLE IF EXISTS tls_dict_table")

"""
Aiven patch 021 (MySQL portion) — integration test for the enforcement that a
MySQL dictionary source must be configured through a named collection. This
mirrors the PostgreSQL test (`test_aiven_postgres_dict_named_collection`) and the
PostgreSQL guard in `PostgreSQLDictionarySource.cpp`; the MySQL counterpart was
dropped during the 26.3 port of patch 021 (the inline-config `else` branch was
kept instead of the Aiven `else { throw }`) and is restored here.

See docs/aiven/patches/021-external-db-ssl.md.

The invariant: a tenant must not embed a raw host/user/password inline in
`CREATE DICTIONARY ... SOURCE(MYSQL(...))` DDL (which would let them point the
server at an arbitrary host and would leak credentials via `SHOW CREATE
DICTIONARY`). Credentials must come from an operator-provisioned named
collection — the only credential surface tenants may reference.
"""

import logging

import pymysql.cursors
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import mysql_pass

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    dictionaries=[
        "configs/dictionaries/mysql_xml_dict.xml",
    ],
    with_mysql8=True,
)


def get_mysql_conn(started_cluster):
    return pymysql.connect(
        user="root",
        password=mysql_pass,
        host=started_cluster.mysql8_ip,
        port=started_cluster.mysql8_port,
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _prepare_backing_table(started_cluster):
    conn = get_mysql_conn(started_cluster)
    with conn.cursor() as cursor:
        cursor.execute("CREATE DATABASE IF NOT EXISTS test_dict")
        cursor.execute("DROP TABLE IF EXISTS test_dict.xml_enforced_table")
        cursor.execute(
            "CREATE TABLE test_dict.xml_enforced_table (id INT, value INT)"
        )
        cursor.execute(
            "INSERT INTO test_dict.xml_enforced_table (id, value) VALUES (9, 9)"
        )
    conn.commit()
    conn.close()


def test_xml_config_dictionary_is_rejected(started_cluster):
    """Enforcement leg.

    A MySQL dictionary configured from a server config file (no named collection,
    ``created_from_ddl == false``) must be rejected with ``UNSUPPORTED_METHOD`` and
    the Aiven "must use a named collection" message. The backing MySQL table is
    created and filled so that a PRE-patch binary (with the inline ``else`` branch)
    would otherwise load it successfully against the real server — this is what
    makes the pre/post differential causal rather than a missing-table artifact.
    """
    _prepare_backing_table(started_cluster)

    err = node1.query_and_get_error("SYSTEM RELOAD DICTIONARY xml_mysql_dict")
    logging.debug("enforcement-leg error: %s", err)

    # Assert BOTH the error code and the Aiven-specific message (AGENTS §7):
    # UNSUPPORTED_METHOD on its own is a generic upstream code, so the message
    # substring is what distinguishes the Aiven enforcement gate.
    assert "UNSUPPORTED_METHOD" in err
    assert "MySQL dictionary source configuration must use a named collection" in err


def test_ddl_named_collection_loads(started_cluster):
    """Positive leg.

    The allowed credential surface — a named collection — still works end to end:
    the guard rejects ONLY the inline path, not named collections.
    """
    _prepare_backing_table(started_cluster)

    node1.query("DROP NAMED COLLECTION IF EXISTS mysql_dict_coll")
    node1.query(
        f"""
        CREATE NAMED COLLECTION mysql_dict_coll AS
            user = 'root',
            password = '{mysql_pass}',
            host = 'mysql80',
            port = 3306,
            database = 'test_dict',
            table = 'xml_enforced_table'
        """
    )

    node1.query("DROP DICTIONARY IF EXISTS mysql_ddl_dict")
    node1.query(
        """
        CREATE DICTIONARY mysql_ddl_dict (id UInt32, value UInt32)
        PRIMARY KEY id
        SOURCE(MYSQL(NAME mysql_dict_coll))
        LIFETIME(MIN 1 MAX 2)
        LAYOUT(HASHED())
        """
    )

    result = node1.query("SELECT dictGetUInt32('mysql_ddl_dict', 'value', toUInt64(9))")
    assert int(result.strip()) == 9

    node1.query("DROP DICTIONARY IF EXISTS mysql_ddl_dict")
    node1.query("DROP NAMED COLLECTION IF EXISTS mysql_dict_coll")

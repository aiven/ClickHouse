import logging

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# The server setting `dictionary_user` is set to `some_user` via main_configs.
# A LOCAL ClickHouse dictionary source must run as that user; a source whose
# configured `user` differs is rejected at registration (Aiven patch 061 leg C).
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/dictionary_user.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        # Backing table that the local dictionary source reads. Created and
        # filled so that the PRE-patch binary would otherwise load the
        # dictionary successfully — this is what makes the pre/post
        # differential causal rather than an artifact of a missing table.
        node1.query("DROP TABLE IF EXISTS default.dict_src")
        node1.query(
            "CREATE TABLE default.dict_src (id UInt64, val UInt64) ENGINE = Memory"
        )
        node1.query(
            "INSERT INTO default.dict_src SELECT number, number FROM numbers(10)"
        )

        # `dictionary_user` (some_user) must exist and be able to read the
        # source table for the matching-user positive control to load.
        node1.query("DROP USER IF EXISTS some_user")
        node1.query("CREATE USER some_user IDENTIFIED WITH no_password")
        node1.query("GRANT SELECT ON default.dict_src TO some_user")

        yield cluster
    finally:
        cluster.shutdown()


def test_local_dict_user_mismatch_is_rejected(started_cluster):
    """Primary causal pair (evidence-of-causation).

    A LOCAL ClickHouse dictionary source (host = this node) whose configured
    ``user`` (``default``) differs from the server setting ``dictionary_user``
    (``some_user``) must be rejected at dictionary registration. The throw
    fires inside ``registerDictionarySourceClickHouse`` before any query, so it
    is surfaced by ``SYSTEM RELOAD DICTIONARY``.

    POST-patch: ``BAD_ARGUMENTS`` with the Aiven-specific message
    "does not match the user specified by the setting dictionary_user".
    PRE-patch: the setting does not exist and the local path uses
    ``Session::authenticate`` as ``default`` — the dictionary loads, so this
    assertion fails (no error is raised). The error code alone is insufficient
    (``BAD_ARGUMENTS`` is generic); the message substring distinguishes the
    Aiven gate (AGENTS §7).
    """
    node1.query("DROP DICTIONARY IF EXISTS default.dict_user_mismatch")
    node1.query(
        """
        CREATE DICTIONARY default.dict_user_mismatch (id UInt64, val UInt64)
        PRIMARY KEY id
        SOURCE(CLICKHOUSE(host 'localhost' port 9000 user 'default' password '' db 'default' table 'dict_src'))
        LIFETIME(MIN 1 MAX 2)
        LAYOUT(FLAT())
        """
    )

    err = node1.query_and_get_error("SYSTEM RELOAD DICTIONARY default.dict_user_mismatch")
    logging.debug("dictionary_user mismatch error: %s", err)

    assert "BAD_ARGUMENTS" in err
    assert "does not match the user specified by the setting dictionary_user" in err

    node1.query("DROP DICTIONARY IF EXISTS default.dict_user_mismatch")


def test_local_dict_matching_user_loads(started_cluster):
    """Positive control.

    A LOCAL ClickHouse dictionary source whose configured ``user`` equals the
    ``dictionary_user`` setting (``some_user``) must load and serve values.
    This confirms the patch's gate accepts the matching user (it does not
    reject every local source). It passes both pre- and post-patch, so it is a
    sanity assertion rather than part of the causation pair.
    """
    node1.query("DROP DICTIONARY IF EXISTS default.dict_user_match")
    node1.query(
        """
        CREATE DICTIONARY default.dict_user_match (id UInt64, val UInt64)
        PRIMARY KEY id
        SOURCE(CLICKHOUSE(host 'localhost' port 9000 user 'some_user' password '' db 'default' table 'dict_src'))
        LIFETIME(MIN 1 MAX 2)
        LAYOUT(FLAT())
        """
    )

    node1.query("SYSTEM RELOAD DICTIONARY default.dict_user_match")
    result = node1.query(
        "SELECT dictGetUInt64('default.dict_user_match', 'val', toUInt64(7))"
    )
    assert result.strip() == "7"

    node1.query("DROP DICTIONARY IF EXISTS default.dict_user_match")

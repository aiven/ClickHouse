import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance")


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()

        instance.query("CREATE DATABASE test_db")
        instance.query("CREATE TABLE test_db.t1 (x UInt32) ENGINE = MergeTree ORDER BY x")
        instance.query("CREATE TABLE test_db.t2 (x UInt32) ENGINE = MergeTree ORDER BY x")
        instance.query("CREATE TABLE test_db.t3 (x UInt32) ENGINE = MergeTree ORDER BY x")
        instance.query("INSERT INTO test_db.t1 VALUES (1)")
        instance.query("INSERT INTO test_db.t2 VALUES (2)")
        instance.query("INSERT INTO test_db.t3 VALUES (3)")

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        instance.query("DROP USER IF EXISTS user1, user2")
        instance.query("DROP ROLE IF EXISTS role1")


def test_parser_round_trip():
    assert (
        instance.query(
            "SELECT formatQuerySingleLine('GRANT SELECT ON *.* EXCEPT SELECT ON system.* TO u')"
        )
        == "GRANT SELECT ON *.* EXCEPT SELECT ON system.* TO u\n"
    )
    assert (
        instance.query(
            "SELECT formatQuerySingleLine('GRANT ALL ON *.* EXCEPT ALL ON system.*, ALL ON information_schema.* TO u')"
        )
        == "GRANT ALL ON *.* EXCEPT ALL ON system.*, ALL ON information_schema.* TO u\n"
    )


def test_except_not_allowed_with_roles():
    assert "EXCEPT clause should be specified for access types" in instance.query_and_get_error(
        "SELECT formatQuerySingleLine('GRANT some_role EXCEPT SELECT ON test_db.* TO u')"
    )


def test_table_exclusion():
    instance.query("CREATE USER user1")
    instance.query("GRANT SELECT ON test_db.* EXCEPT SELECT ON test_db.t2 TO user1")

    assert instance.query("SELECT * FROM test_db.t1", user="user1") == "1\n"
    assert instance.query("SELECT * FROM test_db.t3", user="user1") == "3\n"
    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT * FROM test_db.t2", user="user1"
    )

    assert instance.query("SHOW GRANTS FOR user1") == (
        "GRANT SELECT ON test_db.* TO user1\n"
        "REVOKE SELECT ON test_db.t2 FROM user1\n"
    )


def test_database_exclusion():
    instance.query("CREATE USER user1")
    instance.query("GRANT SELECT ON *.* EXCEPT SELECT ON test_db.* TO user1")

    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT * FROM test_db.t1", user="user1"
    )

    assert instance.query("SHOW GRANTS FOR user1") == (
        "GRANT SELECT ON *.* TO user1\n"
        "REVOKE SELECT ON test_db.* FROM user1\n"
    )


def test_grant_option():
    instance.query("CREATE USER user1, user2")
    instance.query(
        "GRANT SELECT ON test_db.* EXCEPT SELECT ON test_db.t2 TO user2 WITH GRANT OPTION"
    )

    # user2 can re-grant what they have (t1, t3)
    instance.query("GRANT SELECT ON test_db.t1 TO user1", user="user2")
    assert instance.query("SELECT * FROM test_db.t1", user="user1") == "1\n"

    # user2 cannot grant the revoked table (t2)
    assert "Not enough privileges" in instance.query_and_get_error(
        "GRANT SELECT ON test_db.t2 TO user1", user="user2"
    )


def test_equivalent_to_separate_grant_revoke():
    instance.query("CREATE USER user1, user2")

    # Combined syntax
    instance.query("GRANT SELECT ON test_db.* EXCEPT SELECT ON test_db.t2 TO user1")

    # Separate statements
    instance.query("GRANT SELECT ON test_db.* TO user2")
    instance.query("REVOKE SELECT ON test_db.t2 FROM user2")

    grants1 = instance.query("SHOW GRANTS FOR user1").replace("user1", "userX")
    grants2 = instance.query("SHOW GRANTS FOR user2").replace("user2", "userX")
    assert grants1 == grants2


def test_role_grantee():
    instance.query("CREATE USER user1")
    instance.query("CREATE ROLE role1")
    instance.query("GRANT SELECT ON test_db.* EXCEPT SELECT ON test_db.t2 TO role1")
    instance.query("GRANT role1 TO user1")

    assert instance.query("SELECT * FROM test_db.t1", user="user1") == "1\n"
    assert instance.query("SELECT * FROM test_db.t3", user="user1") == "3\n"
    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT * FROM test_db.t2", user="user1"
    )

    assert instance.query("SHOW GRANTS FOR role1") == (
        "GRANT SELECT ON test_db.* TO role1\n"
        "REVOKE SELECT ON test_db.t2 FROM role1\n"
    )

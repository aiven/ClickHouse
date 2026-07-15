import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__, zookeeper_config_path="configs/zookeeper.xml")

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

# A user holding ACCESS MANAGEMENT but NOT PROTECTED_ACCESS_MANAGEMENT (the
# avnadmin-like cluster user). It must not be able to touch protected users.
CLUSTER_USER = "clusteruser"
CLUSTER_PW = "clusterpw"
PROTECTED_USER = "protuser"
# Bootstrap user holding ALL (incl. PROTECTED_ACCESS_MANAGEMENT), see configs/users.xml.
ADMIN_USER = "clickadmin"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        # The bootstrap user holds GRANT ALL (see configs/users.xml), so it can
        # create the protected service user and grant the restricted cluster user.
        node1.query(
            f"CREATE USER {PROTECTED_USER} IDENTIFIED WITH sha256_password BY 'pp' PROTECTED",
            user=ADMIN_USER,
        )
        node1.query(f"GRANT SELECT ON *.* TO {PROTECTED_USER}", user=ADMIN_USER)
        node1.query(
            f"CREATE USER {CLUSTER_USER} IDENTIFIED WITH sha256_password BY '{CLUSTER_PW}'",
            user=ADMIN_USER,
        )
        node1.query(
            f"GRANT ACCESS MANAGEMENT ON *.* TO {CLUSTER_USER} WITH GRANT OPTION",
            user=ADMIN_USER,
        )
        node1.query(
            f"GRANT SELECT ON *.* TO {CLUSTER_USER} WITH GRANT OPTION",
            user=ADMIN_USER,
        )

        # Wait until both entities are visible on node2 via the ZooKeeper-replicated storage.
        for node in (node1, node2):
            assert_eq_with_retry(
                node,
                f"SELECT count() FROM system.users WHERE name IN ('{PROTECTED_USER}', '{CLUSTER_USER}')",
                "2\n",
            )

        yield cluster
    finally:
        cluster.shutdown()


def _as_cluster_user(node, query):
    return node.query_and_get_error(query, user=CLUSTER_USER, password=CLUSTER_PW)


@pytest.mark.parametrize(
    "query",
    [
        pytest.param(
            f"DROP USER {PROTECTED_USER} ON CLUSTER default", id="drop_on_cluster"
        ),
        pytest.param(
            f"ALTER USER {PROTECTED_USER} ON CLUSTER default NOT PROTECTED",
            id="alter_not_protected_on_cluster",
        ),
        pytest.param(
            f"CREATE USER OR REPLACE {PROTECTED_USER} ON CLUSTER default IDENTIFIED WITH sha256_password BY 'x'",
            id="create_or_replace_on_cluster",
        ),
        pytest.param(
            f"REVOKE ON CLUSTER default ALL ON *.* FROM {PROTECTED_USER}",
            id="revoke_all_on_cluster",
        ),
    ],
)
def test_on_cluster_bypass_denied_on_initiator(started_cluster, query):
    # The protected-user checks must run on the initiator BEFORE the query is
    # dispatched via `executeDDLQueryOnCluster`, otherwise an ON CLUSTER statement
    # would skip them. The restricted cluster user must be denied here.
    error = _as_cluster_user(node1, query)
    assert "ACCESS_DENIED" in error, error

    # The protected user must remain intact and still protected after the attempts.
    assert "PROTECTED" in node1.query(f"SHOW CREATE USER {PROTECTED_USER}")


def test_protected_flag_replicates_via_zookeeper(started_cluster):
    # The `Protected` flag created on node1 must replicate to node2 through ZooKeeper.
    assert_eq_with_retry(
        node2,
        f"SELECT count() FROM system.users WHERE name = '{PROTECTED_USER}'",
        "1\n",
    )
    assert "PROTECTED" in node2.query_with_retry(f"SHOW CREATE USER {PROTECTED_USER}")

    # Enforcement is replicated too: the restricted cluster user is denied on node2.
    error = _as_cluster_user(node2, f"DROP USER {PROTECTED_USER}")
    assert "ACCESS_DENIED" in error, error

    error = _as_cluster_user(node2, f"ALTER USER {PROTECTED_USER} NOT PROTECTED")
    assert "ACCESS_DENIED" in error, error

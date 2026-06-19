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

# A user holding role-management privileges (CREATE/ALTER/DROP ROLE + ROLE ADMIN)
# but NOT PROTECTED_ACCESS_MANAGEMENT (the avnadmin-like cluster user). It must
# not be able to touch protected roles, including via the MOVE / ON CLUSTER paths.
CLUSTER_USER = "clusteruser"
CLUSTER_PW = "clusterpw"
# Protected service role (PROTECTED), created in the ZooKeeper-replicated storage.
PROTECTED_ROLE = "protrole"
# A non-protected control role, also in the replicated storage.
CONTROL_ROLE = "ctrlrole"
# Bootstrap user holding ALL (incl. PROTECTED_ACCESS_MANAGEMENT), see configs/users.xml.
ADMIN_USER = "clickadmin"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        # The bootstrap user holds GRANT ALL (see configs/users.xml), so it can
        # create the protected role and grant the restricted cluster user.
        node1.query(
            f"CREATE ROLE {PROTECTED_ROLE} PROTECTED IN replicated",
            user=ADMIN_USER,
        )
        node1.query(f"CREATE ROLE {CONTROL_ROLE} IN replicated", user=ADMIN_USER)
        node1.query(
            f"CREATE USER {CLUSTER_USER} IDENTIFIED WITH sha256_password BY '{CLUSTER_PW}'",
            user=ADMIN_USER,
        )
        # Genuine role-management privileges (so a denial proves the protected gate,
        # not a generic missing-privilege error). Deliberately NOT granting ALL /
        # PROTECTED_ACCESS_MANAGEMENT.
        node1.query(
            f"GRANT CREATE ROLE, ALTER ROLE, DROP ROLE, ROLE ADMIN ON *.* TO {CLUSTER_USER}",
            user=ADMIN_USER,
        )

        # Wait until the replicated entities are visible on both nodes.
        for node in (node1, node2):
            assert_eq_with_retry(
                node,
                f"SELECT count() FROM system.roles WHERE name IN ('{PROTECTED_ROLE}', '{CONTROL_ROLE}')",
                "2\n",
            )

        yield cluster
    finally:
        cluster.shutdown()


def _as_cluster_user(node, query):
    return node.query_and_get_error(query, user=CLUSTER_USER, password=CLUSTER_PW)


def test_protected_flag_replicates_via_zookeeper(started_cluster):
    # The `Protected` flag created on node1 must replicate to node2 through ZooKeeper.
    assert_eq_with_retry(
        node2,
        f"SELECT count() FROM system.roles WHERE name = '{PROTECTED_ROLE}'",
        "1\n",
    )
    assert "PROTECTED" in node2.query_with_retry(
        f"SHOW CREATE ROLE {PROTECTED_ROLE}"
    )

    # Enforcement is replicated too: the restricted cluster user is denied on node2,
    # even though it holds DROP ROLE / ALTER ROLE.
    error = _as_cluster_user(node2, f"DROP ROLE {PROTECTED_ROLE}")
    assert "ACCESS_DENIED" in error, error

    error = _as_cluster_user(node2, f"ALTER ROLE {PROTECTED_ROLE} NOT PROTECTED")
    assert "ACCESS_DENIED" in error, error

    # The protected role survived and is still protected.
    assert "PROTECTED" in node2.query(f"SHOW CREATE ROLE {PROTECTED_ROLE}")


@pytest.mark.parametrize(
    "query",
    [
        pytest.param(f"MOVE ROLE {PROTECTED_ROLE} TO memory", id="move_local"),
        pytest.param(
            f"MOVE ROLE {PROTECTED_ROLE} TO memory ON CLUSTER default",
            id="move_on_cluster",
        ),
    ],
)
def test_move_protected_role_denied_on_initiator(started_cluster, query):
    # The hoisted protected-role MOVE check must run on the initiator BEFORE any
    # ON CLUSTER dispatch, so neither the local nor the ON CLUSTER form can launder
    # the move through DDLWorker. The restricted user is denied here, and the error
    # names the Aiven privilege specifically (it DOES hold DROP/CREATE ROLE, so a
    # generic privilege check would not fire — only the protected gate blocks it).
    error = _as_cluster_user(node1, query)
    assert "ACCESS_DENIED" in error, error
    assert "PROTECTED ACCESS MANAGEMENT" in error, error

    # The protected role was not moved: it remains in the replicated storage.
    assert_eq_with_retry(
        node1,
        f"SELECT storage FROM system.roles WHERE name = '{PROTECTED_ROLE}'",
        "replicated\n",
    )


def test_move_nonprotected_role_allowed(started_cluster):
    # Control: the SAME restricted user CAN move a NON-protected role between the
    # same two storages (replicated -> memory), proving the move capability is
    # genuinely held and only the protected gate blocks the protected role above.
    node1.query(
        f"MOVE ROLE {CONTROL_ROLE} TO memory", user=CLUSTER_USER, password=CLUSTER_PW
    )
    assert_eq_with_retry(
        node1,
        f"SELECT storage FROM system.roles WHERE name = '{CONTROL_ROLE}'",
        "memory\n",
    )

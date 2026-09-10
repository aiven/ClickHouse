"""
Aiven patch 022 — protected roles across a cluster and a replicated access store.

The mirror of `test_aiven_protected_users` for roles, with the same per-replica
assertion discipline and for the same reason: the protected-entity check has to
run on the initiator, above the `ON CLUSTER` dispatch, or `DDLWorker` re-executes
the statement under an internal identity and `ON CLUSTER` becomes a bypass. An
assertion on the initiator's error alone cannot distinguish "refused" from
"refused after queueing".

Roles have no self-protection case — a role is never the acting principal — so
the matrix here is the privilege matrix only.

See docs/aiven/patches/022-protected-users-and-roles.md.
"""

import concurrent.futures
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

MAIN_CONFIGS = ["configs/replicated_access_storage.xml", "configs/cluster.xml"]
USER_CONFIGS = ["users.d/bootstrap.xml"]

node1 = cluster.add_instance(
    "node1",
    main_configs=MAIN_CONFIGS,
    user_configs=USER_CONFIGS,
    with_zookeeper=True,
    macros={"replica": "r1"},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=MAIN_CONFIGS,
    user_configs=USER_CONFIGS,
    with_zookeeper=True,
    macros={"replica": "r2"},
)

NODES = [node1, node2]

BOOTSTRAP = "bootstrap"
BOOTSTRAP_PW = "aiven_022_bootstrap"
NOPERM = "noperm"
NOPERM_PW = "aiven_022_noperm"
CLUSTER = "cluster"


def admin(node, query):
    return node.query(query, user=BOOTSTRAP, password=BOOTSTRAP_PW)


def as_noperm_error(node, query):
    return node.query_and_get_error(query, user=NOPERM, password=NOPERM_PW)


def assert_denied_for_protection(error, query):
    """ACCESS_DENIED, and denied by *this* patch rather than by something else."""
    assert "ACCESS_DENIED" in error, f"not denied at all: {query} -> {error}"
    assert (
        "PROTECTED ACCESS MANAGEMENT" in error
        or "PROTECTED_ACCESS_MANAGEMENT" in error
    ), f"denied for the wrong reason: {query} -> {error}"


def wait_for(node, query, expected, attempts=40, delay=0.5):
    """Poll `query` on `node` as the bootstrap user until it returns `expected`.

    `assert_eq_with_retry` cannot be used here: it takes a user but no password,
    and every observation in this suite is made as the bootstrap principal.
    """
    value = None
    for _ in range(attempts):
        value = admin(node, query).strip()
        if value == expected:
            return
        time.sleep(delay)
    raise AssertionError(
        f"{node.name}: `{query}` never reached {expected!r}, last was {value!r}"
    )


def ddl_barrier():
    """Flush the distributed DDL queue, without sleeping — see the users suite."""
    admin(node1, f"CREATE ROLE IF NOT EXISTS ddl_canary ON CLUSTER {CLUSTER}")
    admin(node1, f"DROP ROLE IF EXISTS ddl_canary ON CLUSTER {CLUSTER}")
    for node in NODES:
        wait_for(
            node,
            "SELECT count() FROM system.roles WHERE name = 'ddl_canary'",
            "0",
        )


def role_count(node, name):
    return admin(node, f"SELECT count() FROM system.roles WHERE name = '{name}'").strip()


def is_protected(node, name):
    definition = admin(node, f"SHOW CREATE ROLE {name}")
    if "NOT PROTECTED" in definition:
        return False
    return "PROTECTED" in definition


def grant_count(node, name):
    return admin(
        node, f"SELECT count() FROM system.grants WHERE role_name = '{name}'"
    ).strip()


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        admin(
            node1,
            f"""
            CREATE USER {NOPERM} IDENTIFIED WITH plaintext_password BY '{NOPERM_PW}';
            GRANT ACCESS MANAGEMENT ON *.* TO {NOPERM};
            GRANT CLUSTER ON *.* TO {NOPERM};
            GRANT SELECT, SHOW ON *.* TO {NOPERM} WITH GRANT OPTION;
            """,
        )
        for node in NODES:
            wait_for(
            node,
            f"SELECT count() FROM system.users WHERE name = '{NOPERM}'",
            "1",
        )
        yield cluster
    finally:
        cluster.shutdown()


def make_protected_role(name):
    """Create a protected role on node1 and wait for it to reach both replicas."""
    admin(node1, f"CREATE ROLE OR REPLACE {name} PROTECTED")
    admin(node1, f"GRANT SHOW ON *.* TO {name}")
    for node in NODES:
        wait_for(
            node,
            f"SELECT count() FROM system.roles WHERE name = '{name}'",
            "1",
        )
    return name


def drop_role(name):
    admin(node1, f"DROP ROLE IF EXISTS {name} ON CLUSTER {CLUSTER}")


def test_i6_on_cluster_is_not_a_bypass(start_cluster):
    """I6 — every ON CLUSTER form is refused on the initiator and never queued."""
    prot = make_protected_role("i6_prot")
    renamed = "i6_prot_renamed"

    arms = [
        f"DROP ROLE {prot} ON CLUSTER {CLUSTER}",
        f"ALTER ROLE {prot} ON CLUSTER {CLUSTER} RENAME TO {renamed}",
        f"CREATE ROLE OR REPLACE {prot} ON CLUSTER {CLUSTER}",
        f"ALTER ROLE {prot} ON CLUSTER {CLUSTER} NOT PROTECTED",
    ]

    for query in arms:
        assert_denied_for_protection(as_noperm_error(node1, query), query)
        ddl_barrier()
        for node in NODES:
            where = f"{node.name} after `{query}`"
            assert role_count(node, prot) == "1", where
            assert role_count(node, renamed) == "0", where
            assert is_protected(node, prot), where

    drop_role(prot)


def test_i7_revoke_from_protected_role_under_on_cluster(start_cluster):
    """I7 — a revoke rewrites the role, so it obeys the same policy.

    Grants are stored on the entity itself, so `REVOKE … FROM <role>` is an edit
    of a protected entity by another name. Asserting the grant survives on every
    replica is what distinguishes a real refusal from a queued one.
    """
    prot = make_protected_role("i7_prot")
    for node in NODES:
        wait_for(
            node,
            f"SELECT count() FROM system.grants WHERE role_name = '{prot}'",
            "1",
        )
    baseline = grant_count(node1, prot)
    assert baseline != "0"

    query = f"REVOKE ON CLUSTER {CLUSTER} SHOW ON *.* FROM {prot}"
    assert_denied_for_protection(as_noperm_error(node1, query), query)
    ddl_barrier()

    for node in NODES:
        assert grant_count(node, prot) == baseline, node.name
        assert is_protected(node, prot), node.name

    drop_role(prot)


def test_i8_if_not_exists_is_idempotent_from_both_nodes(start_cluster):
    """I8 — the powerup statement for roles, issued from both nodes at once."""
    name = "i8_prot"
    statement = f"CREATE ROLE IF NOT EXISTS {name} PROTECTED"

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(admin, node, statement) for node in NODES]
        for future in futures:
            future.result()

    for node in NODES:
        wait_for(
            node,
            f"SELECT count() FROM system.roles WHERE name = '{name}'",
            "1",
        )
        assert is_protected(node, name)

    for node in NODES:
        admin(node, statement)
        assert role_count(node, name) == "1"
        assert is_protected(node, name)

    drop_role(name)


def test_i9_flag_replicates_and_is_enforced_on_every_replica(start_cluster):
    """I9 — the flag travels through ZooKeeper, and so does the enforcement."""
    prot = make_protected_role("i9_prot")

    for node in NODES:
        assert role_count(node, prot) == "1"
        assert is_protected(node, prot)

    for query in (
        f"DROP ROLE {prot}",
        f"ALTER ROLE {prot} NOT PROTECTED",
    ):
        assert_denied_for_protection(as_noperm_error(node2, query), query)

    for node in NODES:
        assert role_count(node, prot) == "1"
        assert is_protected(node, prot)

    drop_role(prot)

"""
Aiven patch 022 — protected users across a cluster and a replicated access store.

The stateless tests already pin the single-node semantics. What only a real
cluster can prove is the part of the patch that is easy to get wrong and
invisible locally: every protected-entity check must run *on the initiator,
above the `ON CLUSTER` dispatch*.

`executeDDLQueryOnCluster` pushes the statement into the ZooKeeper DDL queue and
`DDLWorker` re-executes it on each replica under an internal identity that is
not the submitting user. A check placed after the dispatch therefore never sees
the real principal, and `ON CLUSTER` degenerates into a bypass: the initiator
answers "denied" while the replicas quietly apply the change. That failure mode
is why every case below asserts on *both* nodes rather than on the initiator's
error alone — an initiator-only assertion cannot tell "refused" from "refused
after queueing".

Access entities live in ZooKeeper here (`<replicated>` user directory), so the
`CheckFunc` threading through `ReplicatedAccessStorage` / `ZooKeeperReplicator`
is on the path too.

Actors:
  bootstrap - users.xml user holding ALL, hence PROTECTED. Sets things up and is
              the read-only observer for the per-replica assertions.
  noperm    - SQL user with ACCESS MANAGEMENT and CLUSTER but *not* PROTECTED.
              PROTECTED is a child of ALL, not of ACCESS MANAGEMENT, so handing a
              tenant access management must not hand it this privilege.

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
    """ACCESS_DENIED, and denied by *this* patch rather than by something else.

    Both spellings are accepted: the privilege check renders the SQL name
    `PROTECTED ACCESS MANAGEMENT`, while the self-protection messages name the
    C++ identifier `PROTECTED_ACCESS_MANAGEMENT`.
    """
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
    """Flush the distributed DDL queue, without sleeping.

    The queue is FIFO per node, so once a statement submitted *after* the one
    under test has been applied everywhere, anything the denied statement might
    have queued would already have been applied too. That turns "nothing
    happened" into a decidable assertion instead of a race against a timer.
    """
    admin(node1, f"CREATE USER IF NOT EXISTS ddl_canary ON CLUSTER {CLUSTER}")
    admin(node1, f"DROP USER IF EXISTS ddl_canary ON CLUSTER {CLUSTER}")
    for node in NODES:
        wait_for(
            node,
            "SELECT count() FROM system.users WHERE name = 'ddl_canary'",
            "0",
        )


def user_count(node, name):
    return admin(node, f"SELECT count() FROM system.users WHERE name = '{name}'").strip()


def is_protected(node, name):
    definition = admin(node, f"SHOW CREATE USER {name}")
    if "NOT PROTECTED" in definition:
        return False
    return "PROTECTED" in definition


def grant_count(node, name):
    return admin(
        node, f"SELECT count() FROM system.grants WHERE user_name = '{name}'"
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
            -- REVOKE only works for a privilege held WITH GRANT OPTION; without
            -- this the revoke arms would be refused for an unrelated reason.
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


def make_protected_user(name, password=None, extra_grants=""):
    """Create a protected user on node1 and wait for it to reach both replicas."""
    identified = (
        f"IDENTIFIED WITH plaintext_password BY '{password}'"
        if password
        else "IDENTIFIED WITH no_password"
    )
    admin(node1, f"CREATE USER OR REPLACE {name} {identified} PROTECTED")
    admin(node1, f"GRANT SELECT ON *.* TO {name}")
    if extra_grants:
        admin(node1, f"GRANT {extra_grants} ON *.* TO {name}")
    for node in NODES:
        wait_for(
            node,
            f"SELECT count() FROM system.users WHERE name = '{name}'",
            "1",
        )
    return name


def drop_user(name):
    admin(node1, f"DROP USER IF EXISTS {name} ON CLUSTER {CLUSTER}")


def test_i1_on_cluster_is_not_a_bypass(start_cluster):
    """I1 — every ON CLUSTER form is refused on the initiator and never queued.

    Each arm is checked on both replicas after a DDL barrier, so a statement that
    was refused to the caller but still pushed to the queue would be caught.
    """
    # A real authentication method, so the OR REPLACE arm's assertion below has
    # something to detect: the replacement would swap it for `no_password`.
    prot = make_protected_user("i1_prot", password="aiven_022_i1")
    renamed = "i1_prot_renamed"
    baseline_grants = grant_count(node1, prot)

    arms = [
        f"DROP USER {prot} ON CLUSTER {CLUSTER}",
        f"ALTER USER {prot} ON CLUSTER {CLUSTER} RENAME TO {renamed}",
        f"ALTER USER {prot} ON CLUSTER {CLUSTER} NOT PROTECTED",
        f"CREATE USER OR REPLACE {prot} ON CLUSTER {CLUSTER} IDENTIFIED WITH no_password",
        f"REVOKE ON CLUSTER {CLUSTER} ALL ON *.* FROM {prot}",
        f"GRANT ON CLUSTER {CLUSTER} SELECT ON *.* TO {prot}",
    ]

    for query in arms:
        assert_denied_for_protection(as_noperm_error(node1, query), query)
        ddl_barrier()
        for node in NODES:
            where = f"{node.name} after `{query}`"
            assert user_count(node, prot) == "1", where
            assert user_count(node, renamed) == "0", where
            assert is_protected(node, prot), where
            assert grant_count(node, prot) == baseline_grants, where
            # The OR REPLACE arm is the one that could silently swap the
            # authentication method out from under the user.
            assert (
                admin(
                    node,
                    "SELECT count() FROM system.users "
                    f"WHERE name = '{prot}' AND has(auth_type, 'no_password')",
                ).strip()
                == "0"
            ), where

    drop_user(prot)


def test_i2_flag_replicates_and_is_enforced_on_every_replica(start_cluster):
    """I2 — the flag travels through ZooKeeper, and so does the enforcement.

    Enforcement lives in the interpreter, so it can only work on node2 if node2
    actually reads the flag back out of the replicated store. Denying on node2
    proves the round-trip through `ZooKeeperReplicator`'s serialization.
    """
    prot = make_protected_user("i2_prot")

    for node in NODES:
        assert user_count(node, prot) == "1"
        assert is_protected(node, prot)

    for query in (
        f"DROP USER {prot}",
        f"ALTER USER {prot} NOT PROTECTED",
    ):
        assert_denied_for_protection(as_noperm_error(node2, query), query)

    for node in NODES:
        assert user_count(node, prot) == "1"
        assert is_protected(node, prot)

    drop_user(prot)


def test_i3_if_not_exists_is_idempotent_from_both_nodes(start_cluster):
    """I3 — the control plane's powerup statement, issued from both nodes at once.

    It runs on every service powerup from a deliberately non-exclusive action, so
    it must converge on exactly one protected user rather than racing into a
    duplicate or an error.
    """
    name = "i3_prot"
    statement = f"CREATE USER IF NOT EXISTS {name} IDENTIFIED WITH no_password PROTECTED"

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(admin, node, statement) for node in NODES]
        for future in futures:
            future.result()

    for node in NODES:
        wait_for(
            node,
            f"SELECT count() FROM system.users WHERE name = '{name}'",
            "1",
        )
        assert is_protected(node, name)

    # And a second, sequential run is still a silent no-op.
    for node in NODES:
        admin(node, statement)
        assert user_count(node, name) == "1"
        assert is_protected(node, name)

    drop_user(name)


def test_i4_dependency_cascade_guard_under_on_cluster(start_cluster):
    """I4 — the cascade guard also has to sit above the ON CLUSTER dispatch.

    Dropping a role that a protected user references would make
    `removeReferencesToRemovedIDs` rewrite that protected user through
    `updateImpl`, a path that carries no privilege check at all. The guard
    therefore refuses the *triggering* drop. Under ON CLUSTER the refusal has to
    happen before the statement is queued, or each replica would perform the
    cascade locally.
    """
    prot = make_protected_user("i4_prot")
    dep = "i4_dep"
    admin(node1, f"CREATE ROLE OR REPLACE {dep} ON CLUSTER {CLUSTER}")
    admin(node1, f"GRANT {dep} TO {prot}")
    for node in NODES:
        wait_for(
            node,
            "SELECT count() FROM system.role_grants "
            f"WHERE user_name = '{prot}' AND granted_role_name = '{dep}'",
            "1",
        )

    query = f"DROP ROLE {dep} ON CLUSTER {CLUSTER}"
    assert_denied_for_protection(as_noperm_error(node1, query), query)
    ddl_barrier()

    for node in NODES:
        assert (
            admin(
                node,
                "SELECT count() FROM system.role_grants "
                f"WHERE user_name = '{prot}' AND granted_role_name = '{dep}'",
            ).strip()
            == "1"
        ), node.name
        assert (
            admin(node, f"SELECT count() FROM system.roles WHERE name = '{dep}'").strip()
            == "1"
        ), node.name

    # Positive control: the holder of the privilege is unaffected and the
    # upstream cascade still runs, on every replica. `IF EXISTS` because the
    # access store is shared by both nodes: the first replica to run the queued
    # statement removes the role for everyone, and the second would otherwise
    # report ACCESS_ENTITY_NOT_FOUND. That is upstream behaviour of ON CLUSTER
    # over a replicated user directory, unrelated to this patch.
    admin(node1, f"DROP ROLE IF EXISTS {dep} ON CLUSTER {CLUSTER}")
    for node in NODES:
        wait_for(
            node,
            f"SELECT count() FROM system.roles WHERE name = '{dep}'",
            "0",
        )
        wait_for(
            node,
            "SELECT count() FROM system.role_grants "
            f"WHERE user_name = '{prot}' AND granted_role_name = '{dep}'",
            "0",
        )

    drop_user(prot)


def test_i5_self_protection_under_on_cluster(start_cluster):
    """I5 — self-protection is unconditional, and ON CLUSTER does not launder it.

    The actor here *holds* PROTECTED, so this is not the privilege check firing:
    dropping the identity you are acting as would orphan the session, and no
    privilege buys that.
    """
    password = "aiven_022_i5"
    prot = make_protected_user(
        "i5_prot", password=password, extra_grants="ACCESS MANAGEMENT, CLUSTER, PROTECTED"
    )

    query = f"DROP USER {prot} ON CLUSTER {CLUSTER}"
    error = node1.query_and_get_error(query, user=prot, password=password)
    assert_denied_for_protection(error, query)
    ddl_barrier()

    for node in NODES:
        assert user_count(node, prot) == "1", node.name
        assert is_protected(node, prot), node.name

    drop_user(prot)

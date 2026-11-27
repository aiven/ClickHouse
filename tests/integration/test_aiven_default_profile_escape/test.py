"""
The default profile disables `allow_non_default_profile` and locks it with a readonly
constraint in users.xml. A user on the default profile who manages access can then only
assign the default profile or the profiles inheriting from it, while the `default` user of
this test, on the `admin` profile, can assign any profile.

`cycle_a` and `cycle_b` inherit from each other. Users.xml can define such a cycle, so
checking whether a profile inherits from the default profile must terminate on it.

`mixed` inherits from the default profile and from `admin`, so it carries `admin`'s settings.
"""

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    user_configs=["configs/users.d/profiles.xml"],
)

ESCAPE = "must be the default profile or inherit from it"
LOCKED = "should not be changed"
CUSTOMER = "customer"
PASSWORD = "aiven_014_synthetic_password"


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        node.query(
            f"CREATE USER {CUSTOMER} IDENTIFIED WITH plaintext_password BY '{PASSWORD}' SETTINGS PROFILE 'default'"
        )
        node.query(f"GRANT ACCESS MANAGEMENT ON *.* TO {CUSTOMER} WITH GRANT OPTION")
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def drop_created_users():
    yield
    node.query("DROP USER IF EXISTS u_admin, u_cycle, u_child, u_default, u_by_admin")
    node.query("DROP SETTINGS PROFILE IF EXISTS p_mixed")


def as_customer(query):
    return node.query(query, user=CUSTOMER, password=PASSWORD, timeout=60)


def assert_rejected(query, message):
    with pytest.raises(QueryRuntimeException) as error:
        as_customer(query)
    assert "SETTING_CONSTRAINT_VIOLATION" in str(error.value)
    assert message in str(error.value)


def test_profile_outside_the_default_tree_is_rejected():
    assert_rejected("CREATE USER u_admin SETTINGS PROFILE 'admin'", ESCAPE)
    assert (
        node.query("SELECT count() FROM system.users WHERE name = 'u_admin'") == "0\n"
    )


def test_profile_with_a_parent_outside_the_default_tree_is_rejected():
    assert_rejected("CREATE USER u_admin SETTINGS PROFILE 'mixed'", ESCAPE)
    assert_rejected("CREATE SETTINGS PROFILE p_mixed SETTINGS INHERIT 'mixed'", ESCAPE)
    assert (
        node.query(
            "SELECT count() FROM system.settings_profile_elements WHERE inherit_profile = 'mixed'"
        )
        == "0\n"
    )


def test_switching_the_session_to_the_admin_profile_is_rejected():
    assert_rejected("SET profile = 'admin'", LOCKED)
    assert as_customer("SELECT getSetting('allow_non_default_profile')") == "false\n"


def test_cyclic_profiles_are_rejected_and_the_server_stays_up():
    assert_rejected("CREATE USER u_cycle SETTINGS PROFILE 'cycle_a'", ESCAPE)
    assert node.query("SELECT 1") == "1\n"


def test_default_and_its_descendants_are_allowed():
    as_customer("CREATE USER u_default SETTINGS PROFILE 'default'")
    as_customer("CREATE USER u_child SETTINGS PROFILE 'child'")
    assert (
        node.query(
            "SELECT count() FROM system.users WHERE name IN ('u_default', 'u_child')"
        )
        == "2\n"
    )


def test_the_guard_is_locked_for_the_customer():
    assert_rejected("SET allow_non_default_profile = 1", LOCKED)
    assert_rejected(
        "CREATE USER u_admin SETTINGS allow_non_default_profile WRITABLE", LOCKED
    )


def test_admin_profile_can_assign_any_profile():
    node.query("CREATE USER u_by_admin SETTINGS PROFILE 'admin'")
    assert (
        node.query(
            "SELECT inherit_profile FROM system.settings_profile_elements WHERE user_name = 'u_by_admin'"
        )
        == "admin\n"
    )

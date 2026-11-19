"""
Aiven patch 046: with the MergeTree setting `aiven_use_early_fetch_pool`, the fetches of a
replica catching up (queue entries without a source replica) run in their own pool, sized by
the server setting `aiven_background_early_fetches_pool_size`, instead of the pool of
`background_fetches_pool_size`.

`node_source` holds the data. `node_replica` creates the new replicas; its early pool has a
single slot, so while it catches up, the surplus entries are postponed with the reason
"early fetches already executing". That reason only exists in the early pool, which makes it
the observable for the routing. `node_replica` also turns the table setting off by default
through the old name `use_early_fetch_pool` in its `<replicated_merge_tree>` config section.

See docs/aiven/patches/046-early-fetch-pool.md.
"""

import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_source = cluster.add_instance(
    "node_source",
    with_zookeeper=True,
    stay_alive=True,
    macros={"replica": "src"},
)
node_replica = cluster.add_instance(
    "node_replica",
    main_configs=["configs/early_fetch_pool.xml", "configs/merge_tree_old_name.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"replica": "rep"},
)

N_PARTS = 40
EARLY_REASON = "early fetches already executing"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_table(node, name, settings):
    node.query(
        f"CREATE TABLE {name} (k UInt64, v String) "
        f"ENGINE = ReplicatedMergeTree('/clickhouse/tables/aiven046/{name}', '{{replica}}') "
        f"ORDER BY k {settings}"
    )


def fill_source(name):
    create_table(node_source, name, "")
    node_source.query(f"SYSTEM STOP MERGES {name}")
    for i in range(N_PARTS):
        node_source.query(f"INSERT INTO {name} VALUES ({i}, 'payload-{i}')")


def postpone_reasons(table):
    return node_replica.query(
        "SELECT DISTINCT postpone_reason FROM system.replication_queue "
        f"WHERE database = currentDatabase() AND table = '{table}' AND postpone_reason != ''"
    )


def sync_new_replica(name, settings):
    """Create the replica of `name` on `node_replica`, wait until it caught up, and return
    whether any of its entries was postponed because the early pool was full."""
    fill_source(name)
    create_table(node_replica, name, settings)
    seen_early = False
    for _ in range(600):
        seen_early = seen_early or EARLY_REASON in postpone_reasons(name)
        if int(node_replica.query(f"SELECT count() FROM {name}")) == N_PARTS:
            return seen_early
        time.sleep(0.1)
    raise AssertionError(f"{name} did not catch up: {node_replica.query(f'SELECT count() FROM {name}')}")


def early_pool_metric(name):
    return int(node_replica.query(f"SELECT value FROM system.metrics WHERE metric = '{name}'"))


def test_pool_size_from_server_setting(start_cluster):
    assert early_pool_metric("BackgroundEarlyFetchesPoolSize") == 1
    assert node_source.query("SELECT value FROM system.metrics WHERE metric = 'BackgroundEarlyFetchesPoolSize'").strip() == "8"


def test_routing_follows_table_setting(start_cluster):
    assert sync_new_replica("t_early", "SETTINGS aiven_use_early_fetch_pool = 1")
    assert not sync_new_replica("t_normal", "SETTINGS aiven_use_early_fetch_pool = 0")


def test_old_name_in_server_config(start_cluster):
    """The built-in default is on; `node_replica` turned it off with the old name."""
    query = "SELECT value, changed FROM system.replicated_merge_tree_settings WHERE name = 'aiven_use_early_fetch_pool'"
    assert node_source.query(query) == "1\t0\n"
    assert node_replica.query(query) == "0\t1\n"
    assert not sync_new_replica("t_config_default", "")


def test_old_name_in_table_settings(start_cluster):
    """Tables created before the rename have the old name in their stored definition."""
    assert sync_new_replica("t_old_name", "SETTINGS use_early_fetch_pool = 1")
    node_replica.query("DETACH TABLE t_old_name")
    node_replica.query("ATTACH TABLE t_old_name")
    assert int(node_replica.query("SELECT count() FROM t_old_name")) == N_PARTS


def test_drop_while_catching_up(start_cluster):
    """Dropping a table must remove its tasks from the early pool, like from the other pools."""
    name = "t_drop"
    fill_source(name)
    create_table(node_replica, name, "SETTINGS aiven_use_early_fetch_pool = 1")
    for _ in range(600):
        if EARLY_REASON in postpone_reasons(name):
            break
        time.sleep(0.05)
    node_replica.query(f"DROP TABLE {name} SYNC")

    assert node_replica.query("SELECT 1") == "1\n"
    for _ in range(100):
        if early_pool_metric("BackgroundEarlyFetchesPoolTask") == 0:
            break
        time.sleep(0.1)
    assert early_pool_metric("BackgroundEarlyFetchesPoolTask") == 0
    assert node_replica.query(f"SELECT count() FROM system.tables WHERE name = '{name}'") == "0\n"


def test_pool_size_increase_on_reload(start_cluster):
    assert node_replica.query(
        "SELECT value, changeable_without_restart FROM system.server_settings "
        "WHERE name = 'aiven_background_early_fetches_pool_size'"
    ) == "1\tIncreaseOnly\n"

    config = "/etc/clickhouse-server/config.d/early_fetch_pool.xml"
    node_replica.replace_in_config(config, "<aiven_background_early_fetches_pool_size>1<", "<aiven_background_early_fetches_pool_size>3<")
    node_replica.query("SYSTEM RELOAD CONFIG")

    assert early_pool_metric("BackgroundEarlyFetchesPoolSize") == 3
    assert node_replica.query(
        "SELECT value FROM system.server_settings WHERE name = 'aiven_background_early_fetches_pool_size'"
    ) == "3\n"

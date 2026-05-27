"""
Aiven patch 005 — regression test for:

  ZooKeeper unavailable for several seconds (typical during a ZK rolling
  restart in production). Pre-patch, the connection-establishment loop
  in `src/Common/ZooKeeper/ZooKeeperImpl.cpp` `ZooKeeper::connect` tries
  `num_connection_retries + 1` times with no inter-attempt sleep and
  thus gives up in single-digit milliseconds. The first ZK-using operation
  on a freshly-started server fails fast, propagating the error to the
  user.

Trigger reproduced here (Shape A from the T3.8 → T3.9 policy_call
discussion, plus the empirical refinement from T3.9 iteration that the
defended path is `Context::getZooKeeper` -> `ZooKeeper::create` ->
`ZooKeeper::connect` on the first ZK use, not the table-attach path
which gracefully degrades to read-only mode):

  1. Single-node cluster + 3 Keeper nodes; node1's config pins
       <zookeeper><num_connection_retries>2</num_connection_retries></zookeeper>
     (the pre-patch default; with it, the post-patch path silently floors
     to `std::max(min_num_tries=6, 2+1=3) = 6` retries, while the
     pre-patch path uses literally 3 retries with no inter-attempt sleep.)
  2. In a background thread, stop all 3 Keepers, hold them down for ~10
     seconds, then restart them.
  3. Once the Keepers are observed stopped, `restart_clickhouse(kill=True)`
     on node1. The fresh server has no cached `shared->zookeeper`, and
     `SELECT 20` (the helper's readiness probe) does not need ZK, so the
     restart helper returns quickly even while the Keepers are down.
  4. Now issue `CREATE TABLE r ... ENGINE = ReplicatedMergeTree(...)` —
     this is the FIRST operation that calls `Context::getZooKeeper()`,
     which in turn calls `ZooKeeper::create` -> `ZooKeeper::connect`.
     This is exactly the patched path. The Keepers are still down at
     this point and will not come back for several more seconds.

Why a fresh `restart_clickhouse` is load-bearing for the test signal:
  If we issue the DDL against the already-running server (with a healthy
  cached ZK session), the patch is not exercised — `Context::getZooKeeper`
  returns the cached session and never touches `connect`. If we use an
  INSERT against an already-existing replicated table, the table goes
  read-only on session loss and the INSERT short-circuits to
  `TABLE_IS_READ_ONLY` before any ZK call — also missing the patched
  path. The combination here (fresh server + new replicated table) puts
  the patched code on the only path between the test query and a result.

Pre-patch (no `min_num_tries`, no inter-attempt sleep):
  - `ZooKeeper::connect` exhausts ~3 tight attempts in milliseconds.
  - `ZooKeeper::create` throws `Coordination::Exception` with
    `ZCONNECTIONLOSS`.
  - `CREATE TABLE` fails immediately with a `KEEPER_EXCEPTION` /
    `Cannot use any of provided ZooKeeper nodes` error.

Post-patch (`min_num_tries=6` floor + exponential backoff + 10 s cap):
  - The retry loop sleeps 100, 200, 400, 800, 1600, 3200, ... ms between
    failed attempts (per node, per attempt; capped at 10 s).
  - By the time the Keepers come back at ~10 s, the loop is still well
    inside its retry budget; the next attempt connects; `CREATE TABLE`
    succeeds.

See `docs/aiven/patches/005-tolerate-zk-restart-with-exponential-backoff.md`
for the source SHA, upstream-drift analysis, and the parent-preflight
discipline that surfaced this test design (Shape A from the policy_call
discussion at T3.8's halt-and-escalate).
"""

import threading
import time
from multiprocessing.dummy import Pool

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/zk_no_retry.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"replica": "r1"},
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_clickhouse_tolerates_zk_briefly_unavailable_at_first_use(start_cluster):
    # Baseline: CH and Keepers are up; simple non-ZK query works.
    assert node1.query("SELECT 1").strip() == "1"

    # Make sure no left-over replicated table from a previous run forces
    # `Context::getZooKeeper` during startup (which would eagerly use a
    # cached, then-broken session and mask the test signal).
    node1.query("DROP TABLE IF EXISTS r SYNC")

    # Background thread: stop all 3 Keepers, hold them down for 10 s,
    # then restart them.  10 s is comfortably past the pre-patch retry
    # window (~ms, three tight retries with no inter-attempt sleep) and
    # inside the post-patch retry window (the patched loop sleeps
    # 100, 200, 400, 800, 1600, 3200 ms after each failed node attempt
    # before reaching `min_num_tries=6` of attempts; with three Keeper
    # nodes per attempt the effective sleep budget far exceeds 10 s).
    pool = Pool(1)
    zk_stopped_event = threading.Event()
    zk_back_event = threading.Event()

    def zk_restart_cycle():
        cluster.stop_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
        zk_stopped_event.set()
        time.sleep(10)
        cluster.start_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
        zk_back_event.set()

    job = pool.apply_async(zk_restart_cycle)
    assert zk_stopped_event.wait(120), "background thread did not stop all 3 Keepers within 120s"

    # Kill + restart CH. The new process has `shared->zookeeper == nullptr`
    # and the readiness probe (`SELECT 20`) does not touch ZK, so the
    # helper returns even though the Keepers are down.
    node1.restart_clickhouse(kill=True)
    assert node1.query("SELECT 1").strip() == "1"

    # First ZK-using DDL on the fresh server. This is the call that goes
    # through the patched `ZooKeeper::connect`.
    create_started_at = time.monotonic()
    create_exc = None
    try:
        node1.query(
            "CREATE TABLE r (a UInt64) "
            "ENGINE = ReplicatedMergeTree('/clickhouse/test/aiven_005/r', '{replica}') "
            "ORDER BY tuple()"
        )
    except Exception as exc:
        create_exc = exc
    create_finished_at = time.monotonic()

    # Drain the background job before asserting.
    job.wait()
    pool.close()
    pool.join()

    if create_exc is not None:
        # Pre-patch outcome — `ZooKeeper::connect` gave up before the
        # Keepers came back; re-raise so pytest reports a clean FAIL
        # with the underlying ZK-connect-loss exception.
        raise AssertionError(
            "CREATE TABLE failed while ZK was briefly unavailable "
            "(pre-patch behaviour, the connect-retry loop gave up "
            "before the Keepers came back): {}; "
            "create-elapsed={:.2f}s, keepers-back-event-set={}"
            .format(
                create_exc,
                create_finished_at - create_started_at,
                zk_back_event.is_set(),
            )
        ) from create_exc

    # Confirm the table was actually registered (not just that CREATE
    # returned without throwing).
    assert_eq_with_retry(
        node1,
        "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'r'",
        "1\n",
        retry_count=10,
        sleep_time=1,
    )

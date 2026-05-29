"""
Aiven patch 005 — regression test for:

  ZooKeeper unavailable for several seconds (typical during a ZK rolling
  restart in production). Pre-patch, the connection-establishment loop
  in `src/Common/ZooKeeper/ZooKeeperImpl.cpp` `ZooKeeper::connect` tries
  `num_connection_retries + 1` times with no inter-attempt sleep and
  thus gives up in single-digit milliseconds. The first ZK-using operation
  on a freshly-started server fails fast, propagating the error to the
  user.

Trigger reproduced here (Shape A from the T3.8 -> T3.9 policy_call
discussion, plus the empirical refinement from T3.9 iteration that the
defended path is `Context::getZooKeeper` -> `ZooKeeper::create` ->
`ZooKeeper::connect` on the first ZK use, not the table-attach path
which gracefully degrades to read-only mode):

  1. Single-node cluster + 3 Keeper nodes; node1's config pins
       <zookeeper><num_connection_retries>2</num_connection_retries></zookeeper>
     (the pre-patch default; with it, the post-patch path silently floors
     to `std::max(min_num_tries=6, 2+1=3) = 6` retries, while the
     pre-patch path uses literally 3 retries with no inter-attempt sleep.)
  2. Make ZK unreachable *at the TCP layer* (iptables DROP on the Keeper
     client port 2181, via `PartitionManager.drop_instance_zk_connections`)
     while leaving the Keeper containers running and their hostnames
     resolvable. Hold it down for a fixed, budget-safe window, then restore.
  3. With ZK blocked, `restart_clickhouse(kill=True)` on node1. The fresh
     server has no cached `shared->zookeeper`, and `SELECT 20` (the helper's
     readiness probe) does not need ZK, so the restart helper returns
     quickly even while ZK is unreachable.
  4. Immediately issue `CREATE TABLE r ... ENGINE = ReplicatedMergeTree(...)`
     — the FIRST operation that calls `Context::getZooKeeper()`, which in
     turn calls `ZooKeeper::create` -> `ZooKeeper::connect`. This is exactly
     the patched path. ZK is still unreachable at this point and comes back
     a few seconds later, inside the patched retry budget.

Why a network partition and NOT stopping the Keeper containers
--------------------------------------------------------------
`ZooKeeper::connect` first *resolves* every configured host and, if NONE
resolve, throws `Cannot use any of provided ZooKeeper nodes` with
`ZCONNECTIONLOSS` BEFORE the retry/backoff loop is ever entered
(`ZooKeeperImpl.cpp`, the `resolved_count == 0` branch). Stopping the Keeper
*containers* removes their names from Docker's embedded DNS, so a stopped-
container outage hits exactly that pre-loop DNS fast-throw (observed on
Buildkite build 40: the CREATE failed in 0.11s — far below any backoff — with
that message). That tests a DNS-failure path the patch does not cover and is
NOT what a production ZK rolling restart looks like. An iptables DROP on port
2181 keeps the containers running (DNS resolves, `resolved_count > 0`), so
`connect` enters the retry loop that patch 005 actually hardens, and TCP
SYNs simply go unanswered until connectivity is restored.

Why the "make ZK unreachable" window is opened BEFORE the restart, but the
"restore" countdown starts only just before the CREATE
------------------------------------------------------------------------
`restart_clickhouse(kill=True)` can take many seconds on a loaded runner. In
the previous version the fixed ZK-downtime clock started before the restart,
so a slow restart ate the whole window and the CREATE landed *after* ZK was
already coming back — in a transient where it fast-failed (also observed on
build 40). Here the iptables rule is installed once (it survives the process
restart because the container is not recreated) and the timed *restore* is
scheduled from right before the CREATE, so the outage the CREATE experiences
is a controlled `ZK_DOWNTIME_SECONDS`, independent of restart duration.
Restoring is an instant iptables delete — no container boot, no leader
election — so "ZK is back" is deterministic.

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
    failed attempts (per node, per attempt; capped at 10 s), a budget that
    far exceeds `ZK_DOWNTIME_SECONDS`.
  - By the time connectivity is restored, the loop is still well inside its
    retry budget; the next attempt connects; `CREATE TABLE` succeeds.

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
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/zk_no_retry.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"replica": "r1"},
)

# How long ZK stays unreachable, measured from just before the CREATE. Chosen to
# be comfortably beyond the pre-patch window (~ms of tight retries) yet far inside
# the post-patch retry budget (the exponential backoff alone sums to tens of
# seconds), so the test is robust to runner load in both directions.
ZK_DOWNTIME_SECONDS = 8


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
    # cached, then-broken session and mask the test signal). Done while ZK is
    # still reachable so the DROP itself completes cleanly.
    node1.query("DROP TABLE IF EXISTS r SYNC")

    pool = Pool(1)
    zk_back_event = threading.Event()
    try:
        with PartitionManager() as pm:
            # Block CH -> ZK at the TCP layer (port 2181). The Keeper containers keep
            # running, so their hostnames still resolve and `ZooKeeper::connect`
            # enters the retry/backoff loop (rather than the DNS fast-throw that
            # stopping the containers would trigger). The rule survives the process
            # restart below because the container is not recreated.
            pm.drop_instance_zk_connections(node1)

            # Fresh server with ZK unreachable: no cached session, so the first ZK
            # use (the CREATE below) goes through the patched connect. The readiness
            # probe does not need ZK, so the restart returns while ZK is blocked.
            node1.restart_clickhouse(kill=True)
            assert node1.query("SELECT 1").strip() == "1"

            # Restore ZK connectivity after a fixed, budget-safe delay, measured from
            # right before the CREATE so restart duration can't eat the window.
            # Restore is an instant iptables delete (no boot / leader election), so
            # the "ZK is back" moment is deterministic.
            def restore_after_delay():
                time.sleep(ZK_DOWNTIME_SECONDS)
                pm.restore_instance_zk_connections(node1)
                zk_back_event.set()

            job = pool.apply_async(restore_after_delay)

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

            if create_exc is not None:
                # Pre-patch outcome — `ZooKeeper::connect` gave up before the
                # connectivity was restored; re-raise so pytest reports a clean FAIL
                # with the underlying ZK-connect-loss exception.
                raise AssertionError(
                    "CREATE TABLE failed while ZK was briefly unavailable "
                    "(pre-patch behaviour, the connect-retry loop gave up "
                    "before connectivity was restored): {}; "
                    "create-elapsed={:.2f}s, zk-restored-event-set={}"
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
    finally:
        pool.terminate()
        pool.join()

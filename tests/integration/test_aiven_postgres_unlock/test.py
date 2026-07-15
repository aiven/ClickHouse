#!/usr/bin/env python3

"""
Regression test for the Aiven "Unlock PostgreSQL database" patch (dossier 034).

Background
----------
A database using the ``PostgreSQL`` engine keeps a single per-database mutex
(the base ``IDatabase::mutex``). Before the patch this mutex was held across all
PostgreSQL network I/O: ``pool->get()`` (the TCP connect) plus the
``fetchPostgreSQLTablesList`` / ``checkPostgresTable`` / ``fetchPostgreSQLTableStructure``
queries, in ``getTablesIterator``, ``isTableExist``, ``tryGetTable``,
``attachTable``, ``detachTable``, ``dropTable`` and the background
``removeOutdatedTables`` cleaner.

The very same mutex guards the database name and its comment, which
``system.databases`` reads via ``IDatabase::getDatabaseComment`` /
``getCreateDatabaseQuery``. So a single failing PostgreSQL integration (an
unreachable host where the TCP connect hangs until the connection timeout)
makes a ``SELECT ... FROM system.databases`` block for the full timeout window,
because it cannot acquire the per-database mutex that is pinned by an in-flight
PostgreSQL operation. The same coupling makes ``DROP DATABASE`` wedge the
metadata table while it runs.

The patch reduces the lock scope: every method copies the few plain values it
needs while briefly holding the mutex, releases it, performs the PostgreSQL I/O
without the lock, and only re-acquires the mutex (re-validating) to mutate
shared state. After the patch no PostgreSQL network call runs while the mutex is
held, so concurrent metadata reads stay responsive.

Determinism
-----------
The database points at a blackhole address (``10.255.255.1:5432``) where the
TCP connect is silently dropped (NOT refused), so ``pool->get()`` blocks for the
whole connection-attempt window. With ``postgresql_connection_attempt_timeout``
seconds per attempt and ``postgresql_connection_pool_retries`` attempts, that
window is ~ ``ATTEMPT_TIMEOUT * RETRIES`` seconds -- long and deterministic.

We drive an in-flight PostgreSQL operation in a background thread (a ``SHOW
TABLES`` -> ``getTablesIterator``, or the ``DROP DATABASE`` itself ->
``getTablesIterator``), wait until it is actually running (event-based, via
``system.processes``), and then measure how long a foreground ``SELECT name,
comment FROM system.databases`` takes.

  * post-patch: the foreground read completes in milliseconds (it only contends
    on the briefly-held mutex), well under ``LATENCY_BOUND``.
  * pre-patch: the foreground read blocks until the in-flight PostgreSQL
    operation releases the mutex, i.e. ~ the full connection window, far above
    ``LATENCY_BOUND``.

The flip is robust to variability in the connection window: a longer window only
pushes the pre-patch latency further above the bound, while the post-patch
latency stays in the millisecond range.
"""

import threading
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node", stay_alive=True)

# Blackhole endpoint: a non-routable RFC1918 address where the SYN is dropped, so
# the TCP connect hangs until the attempt timeout (NOT a refused port, which would
# return instantly and would not exercise the hang).
BLACKHOLE = "10.255.255.1:5432"

# Bounded, deterministic connection window: ~ ATTEMPT_TIMEOUT * RETRIES seconds.
ATTEMPT_TIMEOUT = 2  # seconds, libpq connect_timeout per attempt
RETRIES = 3  # number of connection attempts

# Foreground metadata reads must complete well under the connection window.
# Window ~= 6s; post-patch reads take ~milliseconds, pre-patch ~6s.
LATENCY_BOUND = 3.0  # seconds


def _pg_settings():
    return {
        "postgresql_connection_attempt_timeout": ATTEMPT_TIMEOUT,
        "postgresql_connection_pool_retries": RETRIES,
        "postgresql_connection_pool_wait_timeout": 2000,
        "postgresql_connection_pool_size": 2,
    }


def _create_pg_db(db_name):
    node.query(f"DROP DATABASE IF EXISTS {db_name} SYNC")
    node.query(
        f"""
        CREATE DATABASE {db_name}
        ENGINE = PostgreSQL('{BLACKHOLE}', 'pgdb', 'pguser', 'pgpass')
        COMMENT 'aiven-034-probe'
        """,
        settings=_pg_settings(),
    )


def _wait_query_running(query_id, timeout=20.0):
    """Block (event-based) until the query with `query_id` shows up in
    system.processes, i.e. it has actually started executing on the server."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        running = node.query(
            f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'"
        ).strip()
        if running != "0":
            return True
        time.sleep(0.05)
    return False


def _measure_metadata_read():
    """Return the wall-clock seconds taken by a `system.databases` read that
    includes the comment column (which is guarded by the per-database mutex)."""
    start = time.monotonic()
    node.query("SELECT name, comment FROM system.databases")
    return time.monotonic() - start


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_system_databases_stays_responsive_during_pg_io(started_cluster):
    """A `system.databases` read must stay responsive while a PostgreSQL
    operation against an unreachable backend is in flight."""
    db = "pg_unlock_select"
    _create_pg_db(db)

    bg_query_id = "aiven034_bg_show_tables"
    bg_error = {}

    def background_pg_op():
        try:
            # getTablesIterator -> pool->get() (connect hangs ~ window).
            # It swallows PostgreSQL errors and returns an empty list.
            node.query(f"SHOW TABLES FROM {db}", query_id=bg_query_id)
        except Exception as e:  # noqa: BLE001
            bg_error["show_tables"] = str(e)

    bg = threading.Thread(target=background_pg_op)
    bg.start()
    try:
        assert _wait_query_running(
            bg_query_id
        ), "background SHOW TABLES never started running"

        latency = _measure_metadata_read()
        assert latency < LATENCY_BOUND, (
            f"system.databases read took {latency:.2f}s (bound {LATENCY_BOUND}s) "
            f"while a PostgreSQL operation was in flight -- the per-database mutex "
            f"is still held across PostgreSQL I/O"
        )
    finally:
        node.query(f"KILL QUERY WHERE query_id = '{bg_query_id}' SYNC", ignore_error=True)
        bg.join(timeout=30)

    node.query(f"DROP DATABASE IF EXISTS {db} SYNC")


def test_drop_database_with_unreachable_backend_is_prompt(started_cluster):
    """DROP DATABASE of an unreachable-backed PostgreSQL database must complete
    promptly and remove the database.

    On 26.3 ``DROP DATABASE`` for a ``PostgreSQL`` engine skips the per-table
    iteration (``DatabasePostgreSQL::shouldBeEmptyOnDetach`` returns ``false``),
    so the drop path itself does no PostgreSQL I/O. What can still couple it to a
    dead backend pre-patch is ``shutdown`` -> ``cleaner_task->deactivate``: the
    background ``removeOutdatedTables`` cleaner, scheduled immediately when the
    database loads, holds the per-database mutex across its connection attempt
    pre-patch, and ``deactivate`` waits for that in-flight run to finish (~ the
    full connection window). Post-patch the cleaner early-exits (nothing to
    reconcile) and never connects, so the drop is immediate.

    Post-patch this bound is met deterministically. Pre-patch it may additionally
    flip (drop blocked ~ the connection window via the cleaner), but that leg is
    timing-dependent -- the deterministic flip lives in
    ``test_system_databases_stays_responsive_during_pg_io``.
    """
    db = "pg_unlock_drop"
    _create_pg_db(db)

    start = time.monotonic()
    node.query(f"DROP DATABASE {db} SYNC")
    elapsed = time.monotonic() - start

    assert (
        node.query(
            f"SELECT count() FROM system.databases WHERE name = '{db}'"
        ).strip()
        == "0"
    ), "database still present after DROP"

    assert elapsed < LATENCY_BOUND, (
        f"DROP DATABASE of an unreachable-backed PostgreSQL database took "
        f"{elapsed:.2f}s (bound {LATENCY_BOUND}s) -- the drop is still coupled to "
        f"PostgreSQL I/O while the per-database mutex is held"
    )

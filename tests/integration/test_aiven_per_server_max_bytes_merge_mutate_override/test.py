"""
Aiven patch 047 — integration test for:

  "Add per-server override for max bytes to merge/mutate"

Two server settings, both default 0 (= unlimited):
  * max_bytes_to_merge_override  — caps the total source-parts size the
    BACKGROUND merge selector is allowed to combine
    (CompactionStatistics::getMaxSourcePartsBytesForMerge).
  * max_bytes_to_mutate_override — caps the part size the BACKGROUND mutation
    selector is allowed to mutate
    (CompactionStatistics::getMaxSourcePartBytesForMutation).

Why integration (not stateless): these are *server* settings, supplied via a
config file at startup (the shared stateless server cannot be reconfigured
safely). So we stand up two single-node servers — one configured with small
caps ("capped"), one with the defaults ("uncapped") — and compare behavior on
identical data.

PITFALL (deliberately avoided): `OPTIMIZE TABLE` selects parts via
`selectAllPartsToMergeWithinPartition`, which BYPASSES the merge-size cap. If we
triggered merges with OPTIMIZE the test would pass vacuously. We therefore drive
the *background* merge/mutation scheduler (insert parts, leave merges enabled,
bounded-wait, then assert) — the only path that calls the capped functions.

Determinism: the value column uses CODEC(NONE) and a fixed-length payload, so
each part's on-disk size is predictable (no compression guesswork). Merges are
held with `SYSTEM STOP MERGES` while parts are inserted, so the initial
6-parts state is stable before the scheduler is released.

Sizing (merge case): 6 parts x ~2 MiB = ~12 MiB total; cap = 5 MiB. Each part is
below the cap, a pair (~4 MiB) may merge, but no sequence of capped merges can
ever yield the single ~12 MiB part: any merge whose source parts sum to > 5 MiB
is rejected, so the capped node is mathematically stuck at >= 3 active parts
while the uncapped control collapses to exactly 1.

Sizing (mutate case): one ~8 MiB part vs a 2 MiB mutate cap, so the capped node
must postpone the mutation (system.mutations.is_done = 0) while the uncapped
control applies it.

Evidence of causation (byte-equivalence is impossible — the patch was rewritten
onto upstream-renamed functions): on the PRE-patch binary the two settings are
unknown and silently ignored, so the "capped" node behaves like the "uncapped"
one (it merges to a single part / completes the mutation) and the assertions
below FAIL. On the POST-patch binary the caps take effect and the test PASSES.

See docs/aiven/patches/047-per-server-max-bytes-merge-mutate-override.md.
"""

import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node_capped = cluster.add_instance(
    "capped",
    main_configs=["configs/overrides.xml"],
    stay_alive=True,
)
# No overrides config => both settings default to 0 (= unlimited): the control.
node_uncapped = cluster.add_instance(
    "uncapped",
    stay_alive=True,
)

# Keep the background merge-selecting task brisk and the pool-space ceiling well
# above our data so OUR override is the only binding constraint.
# min_age_to_force_merge_seconds stays 0 (force-merge would bypass the cap).
TABLE_SETTINGS = (
    "merge_selecting_sleep_ms = 1000, "
    "max_bytes_to_merge_at_max_space_in_pool = 1000000000, "
    "min_bytes_for_wide_part = 0"
)

ROW_BYTES = 1024  # fixed payload per row, stored uncompressed (CODEC(NONE))


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _active_parts(node, table):
    return int(
        node.query(
            f"SELECT count() FROM system.parts "
            f"WHERE database = currentDatabase() AND table = '{table}' AND active"
        ).strip()
    )


def _part_bytes(node, table):
    return node.query(
        f"SELECT count(), sum(bytes_on_disk), max(bytes_on_disk) FROM system.parts "
        f"WHERE database = currentDatabase() AND table = '{table}' AND active"
    ).strip()


def _create(node, table):
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"CREATE TABLE {table} (k UInt64, v String CODEC(NONE)) "
        f"ENGINE = MergeTree ORDER BY k "
        f"SETTINGS {TABLE_SETTINGS}"
    )


def test_merge_override_caps_background_merge(start_cluster):
    # Baseline sanity.
    assert node_capped.query("SELECT 1").strip() == "1"
    assert node_uncapped.query("SELECT 1").strip() == "1"

    rows_per_part = 2048  # ~2 MiB of uncompressed payload per part

    for node in (node_capped, node_uncapped):
        _create(node, "t_merge")
        # Hold merges so the initial 6-part state is observable and stable.
        node.query("SYSTEM STOP MERGES t_merge")
        for i in range(6):
            node.query(
                f"INSERT INTO t_merge SELECT number + {i} * {rows_per_part}, "
                f"repeat('a', {ROW_BYTES}) FROM numbers({rows_per_part})"
            )
        assert _active_parts(node, "t_merge") == 6

    # Log the on-disk sizes so the evidence shows the cap (5 MiB) sits between a
    # single part and the full set.
    print("capped   parts(count,sum,max) =", _part_bytes(node_capped, "t_merge"))
    print("uncapped parts(count,sum,max) =", _part_bytes(node_uncapped, "t_merge"))

    # Release the background merge scheduler on both nodes.
    for node in (node_capped, node_uncapped):
        node.query("SYSTEM START MERGES t_merge")

    # The uncapped control collapses all six parts into one (cap 0 = unlimited):
    # proves the data IS mergeable and background merges ARE enabled.
    assert_eq_with_retry(
        node_uncapped,
        "SELECT count() FROM system.parts "
        "WHERE database = currentDatabase() AND table = 't_merge' AND active",
        "1\n",
        retry_count=60,
        sleep_time=1,
    )

    # Give the capped node at least as long to merge whatever it is ALLOWED to.
    # With a 5 MiB cap and ~12 MiB total it can never reach a single part.
    time.sleep(5)
    capped_parts = _active_parts(node_capped, "t_merge")
    uncapped_parts = _active_parts(node_uncapped, "t_merge")
    print("after merge window: capped =", capped_parts, "uncapped =", uncapped_parts)
    assert uncapped_parts == 1, f"control should fully merge, got {uncapped_parts}"
    assert capped_parts > 1, (
        f"max_bytes_to_merge_override should prevent a full merge, "
        f"but capped node collapsed to {capped_parts} active part(s)"
    )


def test_mutate_override_caps_background_mutation(start_cluster):
    rows = 8192  # ~8 MiB of uncompressed payload => one ~8 MiB part

    for node in (node_capped, node_uncapped):
        _create(node, "t_mut")
        node.query(
            f"INSERT INTO t_mut SELECT number, repeat('a', {ROW_BYTES}) "
            f"FROM numbers({rows})"
        )
        assert _active_parts(node, "t_mut") == 1

    print("capped   t_mut(count,sum,max) =", _part_bytes(node_capped, "t_mut"))
    print("uncapped t_mut(count,sum,max) =", _part_bytes(node_uncapped, "t_mut"))

    # Async mutation (mutations_sync defaults to 0) => driven by the background
    # mutation scheduler, which consults getMaxSourcePartBytesForMutation.
    for node in (node_capped, node_uncapped):
        node.query("ALTER TABLE t_mut UPDATE v = '' WHERE k >= 0")

    def is_done(node):
        return node.query(
            "SELECT is_done FROM system.mutations "
            "WHERE database = currentDatabase() AND table = 't_mut' "
            "ORDER BY mutation_id DESC LIMIT 1"
        ).strip()

    # The uncapped control applies the mutation (cap 0 = unlimited).
    assert_eq_with_retry(
        node_uncapped,
        "SELECT is_done FROM system.mutations "
        "WHERE database = currentDatabase() AND table = 't_mut' "
        "ORDER BY mutation_id DESC LIMIT 1",
        "1\n",
        retry_count=60,
        sleep_time=1,
    )

    # Give the capped node the same wall-clock budget; its mutation must stay
    # pending because the ~8 MiB part exceeds the 2 MiB mutate cap.
    time.sleep(5)
    assert is_done(node_uncapped) == "1", "control mutation should complete"
    assert is_done(node_capped) == "0", (
        "max_bytes_to_mutate_override should keep the mutation pending on a part "
        "larger than the cap, but it completed"
    )

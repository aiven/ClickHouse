---
description: 'Spread the ZooKeeper connection attempts over a few seconds, so a restarting ZooKeeper does not fail the first query that needs it'
sidebarTitle: '005: Tolerate a ZooKeeper restart'
slug: '/aiven/patches/005-tolerate-zk-restart-with-exponential-backoff'
title: 'Patch 005: Tolerate a ZooKeeper restart'
doc_type: 'reference'
aiven_identity: '005'
aiven_absorbs: []
aiven_changelog_category: 'Improvement'
aiven_settings: []
---

# Patch 005 — Tolerate a ZooKeeper restart {#patch-005-tolerate-zk-restart-with-exponential-backoff}

## Summary {#summary}

When ZooKeeper restarts or elects a leader, it can accept a client connection and close it at once.
Upstream's `ZooKeeper::connect` then spends all `num_connection_retries + 1` attempts (3 by default)
within milliseconds, and the query or background task that needed ZooKeeper fails. The patch makes at
least 6 rounds over the configured nodes and pauses between rounds, 200 ms doubling, so the attempts
cover about 6 seconds. The pause is per round, so the total does not grow with the number of nodes.

## Lineage {#lineage}

| LTS | Commits | Outcome |
|---|---|---|
| before 25.3 | package patch, 2022-10-04 and 2022-10-19 | 6 tries instead of 3, a 2 s connection timeout, then a pause after each failed node, 100 ms doubling, no cap; the timeout change was dropped without a note in the 23.3 rebase |
| 25.3 | `886f5855edb` | moved to the fork; `max(6, num_connection_retries + 1)` tries |
| 25.8 | `20ba7d08e64` | 10 s cap on one pause |
| 26.3 | `18580b7e668` | cherry-pick; integration test |
| 26.8 | `patch-port(005)` | `still-needed-but-rewrite`: pause per round instead of per node, new test |

## Background (ClickHouse) {#background}

`Context::getZooKeeper` creates the first session, and a new one after expiry, while holding the
server-wide `zookeeper_mutex`. Every caller that needs ZooKeeper waits on that mutex: queries, the DDL
worker, and the restarting threads of replicated tables in the shared background schedule pool.

`ZooKeeper::connect` resolves the configured nodes, then makes `num_tries` rounds; each round tries
every node once and stops at the first handshake that succeeds. An attempt against a reachable port
that refuses or closes the connection fails at once; an attempt against an unreachable host waits
`connection_timeout_ms` (1 s by default).

Query-level Keeper retries (`keeper_max_retries`, `insert_keeper_max_retries`) wrap operations on an
existing session. The first `setZooKeeper` in the `ReplicatedMergeTree` constructor is outside them, so
`CREATE TABLE ... ReplicatedMergeTree` on a server without a session depends on `connect` alone.

## Component tour {#component-tour}

- **`ZooKeeper::connect`** (upstream, `src/Common/ZooKeeper/ZooKeeperImpl.cpp`). The only change: the
  floor on `num_tries` and the pause at the start of every round after the first.

## Problem {#problem}

ZooKeeper restarts a few times while a service node is replaced, and each restart takes about
6 seconds. During a leadership transfer it keeps the client port open, accepts the handshake and closes
the connection, so the connection timeout adds no delay between attempts. With upstream's three tight
rounds, the first query or background task that needs a new session fails, for example a
`CREATE TABLE ... ReplicatedMergeTree` issued while the node is being set up.

## Decisions {#decisions}

### The patch stays {#decision-the-patch-stays}

- **Chosen:** carry a paced connect on 26.8.
- **Rejected:** dropping it. 26.8 and upstream `master` (as of 2026-11-04) still make
  `num_connection_retries + 1` rounds with no pause.
- **Rejected:** the managed configuration raising `num_connection_retries` alone. More tight rounds
  still fail within milliseconds.
- **Rung:** 5, an edit to upstream's connection loop. Rung 2 is open: a `<zookeeper>` key for the pause,
  default off, is a candidate upstream contribution.

### Pause per round, not per node {#decision-per-round}

- **Chosen:** pause once before each round after the first: 200, 400, 800, 1600 and 3200 ms over the
  six rounds, 6.2 s in total, whatever the number of nodes. No pause after the last round.
- **Rejected:** the 26.3 shape, a pause after every failed node, 100 ms doubling with a 10 s cap. Its
  total grows with the node count: with three nodes that all refuse, 18 pauses sum to about 123 s, and
  with unreachable nodes the 1 s timeouts add 18 s more. The connect runs under `zookeeper_mutex`, so
  every caller waits that long, and the next caller repeats the wait. Measured with three unreachable
  nodes: the first `CREATE` on a fresh server gave up after 280.7 s (the DDL worker's connect, then its
  own), against 47.5 s with this patch (about 18 s is expected without it). The 26.3 shape also paused before trying
  the next node, which may be healthy, and after the last attempt.
- **Supersedes:** the per-node pause of every version since 2022.

### Keep the floor of 6 rounds {#decision-floor}

- **Chosen:** `max(6, num_connection_retries + 1)`, as on 26.3. A higher configured value still wins.
- **Why:** the managed configuration does not set `num_connection_retries`; without the floor the
  pause would cover only two gaps (0.6 s).

## Approach {#approach}

The pause sits at the top of the round loop, guarded by `try_no > 0`, so a successful round never
pauses and the loop ends without a final pause. The cap of 10 s per pause matters only when
`num_connection_retries` is raised above 5.

## Concept {#concept}

A retry delay inside a loop over replicas multiplies by the number of replicas. When the loop runs
under a lock, every waiter pays it too. Pace the outer loop, the one that means "try again later".

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-but-rewrite`

- `connect` is unchanged in shape; the 26.3 hunks would apply after an include-list conflict.
- The 26.3 test black-holed the ZooKeeper port for 8 s. With the harness's three nodes the unpatched loop
  already waits 3 × 3 × 1 s = 9 s, so it does not reliably fail without the patch; its recorded failure
  came from an earlier version that stopped the containers. The new test models the fast failure with a
  TCP reset.

## Customer impact {#customer-impact}

- **Who:** every service; the patch acts only while ZooKeeper cannot be reached.
- **What they see:** during a short ZooKeeper restart, a query that needs a new session waits up to
  about 6 seconds instead of failing with `All connection tries failed while connecting to ZooKeeper`.
  When ZooKeeper stays down, the error comes after about 6 s with refusing nodes, or about 24 s with
  three unreachable nodes, instead of at once or after 9 s.
- **Versus 26.3:** a full outage no longer blocks every ZooKeeper user for minutes.
- **Upstream tests edited:** none.

## Security review {#security-review}

No new input, setting or privilege. The pause is bounded by compile-time constants and by
`num_connection_retries`, which only the server configuration sets.

## Tests {#tests}

- **Paths:** `tests/integration/test_aiven_zk_connect_retry/`
- **Proves:** with every ZooKeeper connection reset for 3 s, the first `CREATE TABLE ... ReplicatedMergeTree`
  on a fresh server waits and succeeds; with ZooKeeper unreachable, it fails with
  `All connection tries failed` in under 120 s, and the server connects once ZooKeeper is back.
- **Run:** `python -m ci.praktika run "integration" --test test_aiven_zk_connect_retry`, or `pytest` from
  `tests/integration` with `CLICKHOUSE_TESTS_SERVER_BIN_PATH` set.
- **Evidence (2026-11-17):** on the parent commit the reset case fails at once with
  `All connection tries failed` for all three nodes, and the bound case passes. With the patch both pass,
  four runs. With the 26.3 shape the bound case fails after 280.7 s.

## Rollback {#rollback}

- **Downgrade:** not supported; services only move forward.
- **Revert in a later release:** no state; the connect loop goes back to tight rounds.
- **Disable without rebuild:** not possible; `num_connection_retries` can only raise the floor.

## Retirement {#retirement}

Upstream pacing of `ZooKeeper::connect`, or a configurable pause there. Re-check upstream on every
uplift.

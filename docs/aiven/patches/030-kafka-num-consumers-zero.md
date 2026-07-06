# Patch 030 — kafka-num-consumers-zero

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `37b61f443b` | Tilman Moeller (author + committer joelynch112@gmail.com), 2025-12-22 | (the version we are porting FROM) |
| 26.3-aiven | `patch-port(030)` (STAGED — HEAD unmoved; see inventory) | parent agent, 2026-06-04 | **`still-needed`** — clean cherry-pick, all landmarks unchanged on 26.3 |

## 1. Purpose

Decrease the lower bound on `kafka_num_consumers` from 1 to 0. Setting it to `0`
disables Kafka consumption globally — a fleet-level ops lever that does not rely
on per-table SQL (`DETACH` / `ALTER`), which users could otherwise use to
interfere adversely with management operations.

The change is three small hunks:

1. `src/Storages/Kafka/StorageKafkaUtils.cpp` — remove the DDL validation block
   that threw `BAD_ARGUMENTS` `"Number of consumers can not be lower than 1"`
   when `num_consumers < 1`.
2. `src/Storages/Kafka/StorageKafka.cpp::getMaxBlockSize` — guard the
   division by introducing `size_t nonzero_num_consumers = num_consumers > 0 ? num_consumers : 1;`
   and dividing `max_insert_block_size` by that instead of `num_consumers`.
3. `src/Storages/Kafka/StorageKafka2.cpp::getMaxBlockSize` — the identical
   division-by-zero guard for the second (Keeper-backed) Kafka engine.

When `num_consumers` is 0: consumer loops (`for (i = 0; i < num_consumers; ++i)`)
do not execute, container `resize`/`reserve` handle 0, and `getMaxBlockSize` no
longer divides by zero. No consumer threads are created, so a `Kafka` table with
`kafka_num_consumers = 0` performs no broker I/O.

## 2. Upstream-drift / validity findings

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`.

- **Still needed.** 26.3 still enforces the lower bound: `StorageKafkaUtils.cpp`
  carries the `if (num_consumers < 1) throw Exception(... "Number of consumers
  can not be lower than 1")` block (verified at lines 209–212 on HEAD), and both
  `getMaxBlockSize` implementations still divide `max_insert_block_size` by the
  raw `num_consumers` (verified at `StorageKafka.cpp:552` and
  `StorageKafka2.cpp:505`). Nothing upstream provides the "0 disables
  consumption" behavior.
- **Clean cherry-pick.** All three landmarks are present and unchanged on 26.3;
  `git format-patch -1 37b61f443b --stdout | git apply --check` exits 0
  (`cherry_pick_clean=yes` in the inventory). Allman braces already match (the
  source used them).
- **No third-engine gap.** The `StorageKafka2::getMaxBlockSize` guard was already
  part of the 25.8 source commit `37b61f443b` (its message notes "this fix was
  missing from the original patch"). So both shipped Kafka engines are covered;
  there is no additional un-guarded `getMaxBlockSize` to port.
- **Clause (v) screen: opt-in, no default change.** `kafka_num_consumers`
  defaults to 1; the patch only removes a lower-bound rejection and adds a
  defensive guard. Existing tables and the default keep their exact prior
  behavior. Blast radius is narrow — the only newly-reachable state is the
  operator explicitly setting `0`.

## 3. C++ / security review

- **Reach.** The removed validation lives in `registerStorageKafka`'s factory
  lambda — it runs at `CREATE TABLE ... ENGINE = Kafka` (DDL) time, driven by
  operator/DDL input, not by untrusted query data. Removing it relaxes a
  numeric-range check, not a security gate.
- **Division-by-zero guards reviewed by inspection (not by a test).** Both
  `getMaxBlockSize` guards are defensive runtime paths: `getMaxBlockSize` feeds
  `getPollMaxBatchSize` / streaming, which are only reached while a consumer is
  actually polling a broker. With `num_consumers = 0` no consumer exists, so the
  divide is unreachable in practice; the guard exists purely so that a future
  caller (or a code path that computes block size before checking consumer count)
  cannot trigger UB `% 0`. The guard substitutes 1 for 0, which yields the same
  block size as a single-consumer table — a safe, conservative value. These
  paths cannot be exercised deterministically from a stateless test (they need a
  live broker and an active stream), so they are covered here by inspection
  rather than by the test in §4. This matches the dispatch instruction.
- **No new attack surface.** No parsing of untrusted input; one check removed,
  two `size_t` guards added. `num_consumers` is `size_t`, so `> 0` is the correct
  predicate (no signedness pitfall).

## 4. Test design

Stateless `.sql` test `tests/queries/0_stateless/9030_kafka_num_consumers_zero.sql`
(Aiven `9<NNN>_<slug>` convention from `testing-suites.md` §4.1; `NNN = 030`).

The testable behavior change is the **DDL validation removal**: post-patch,
`CREATE TABLE ... ENGINE = Kafka SETTINGS ..., kafka_num_consumers = 0` must
succeed; pre-patch it is rejected with `BAD_ARGUMENTS`. The div-by-zero guards
are covered by inspection in §3, not by this test.

```sql
-- Tags: no-fasttest
DROP TABLE IF EXISTS kafka_num_consumers_zero;
CREATE TABLE kafka_num_consumers_zero (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092',
         kafka_topic_list = 't',
         kafka_group_name = 'g',
         kafka_format = 'CSV',
         kafka_num_consumers = 0;
SELECT 'kafka_num_consumers=0 accepted';
DROP TABLE kafka_num_consumers_zero;
```

- `no-fasttest` tag: the Kafka storage engine is absent from the fast-test build.
- **Feasibility confirmed (the dispatch's hard gate).** A `Kafka` table with
  `kafka_num_consumers = 0` is created without a live broker, deterministically
  and without hanging: with zero consumers no consumer threads start, so the
  CREATE performs no broker connection (mirroring upstream `03523`/`03370`,
  where a `Kafka` CREATE against a dead broker succeeds and only `SELECT` fails).
  Manually verified: CREATE + DROP completed in 112 ms, RC=0, no Kafka lines in
  the server log. So a stateless test is appropriate (no integration test
  needed).

### Evidence (worktree-flip pair, 2026-06-04)

Single-axis worktree flip per `testing-suites.md` §6 (HEAD never moved; index
kept the staged patch; only the three Kafka `.cpp` worktree files flipped to
`HEAD` and back).

- **post-patch:** PASS —
  ```
  9030_kafka_num_consumers_zero:                                          [ OK ] 0.08 sec.
  1 tests passed. 0 tests skipped.
  ```
- **pre-patch** (three Kafka sources flipped to `HEAD`, binary rebuilt, server
  restarted to load the pre-patch binary): FAIL —
  ```
  9030_kafka_num_consumers_zero:                                          [ FAIL ] 0.13 sec.
  Reason: return code:  36
  ... DB::Exception: Number of consumers can not be lower than 1. (BAD_ARGUMENTS) ...
  ... StorageKafkaUtils.cpp:211:19 ... registerStorageKafka ...
  Having 1 errors! 0 tests passed.
  ```
  The pre-patch stack points at exactly the removed validation block
  (`StorageKafkaUtils.cpp:211`), pinning the test to the patch's contribution.
  Worktree was then restored from the index and the post-patch binary rebuilt;
  postcondition verified (`git diff` on the three files empty, staged patch
  intact).

## 5. Rollback considerations

Self-contained 10-line, 3-file revert with no submodule, schema, or cross-patch
coupling. Re-instating the `num_consumers < 1` throw and reverting the two
`getMaxBlockSize` divisors to `num_consumers` restores the upstream lower bound.
The guards are inert when `num_consumers >= 1`, so reverting only them (keeping
the validation removed) would re-introduce the `% 0` UB risk — revert all three
hunks together.

## 6. Per-uplift notes

### 25.8-aiven (historical)

Source `37b61f443b` (author Tilman Moeller, 2025-12-22): the same three hunks.
Its message explicitly records that the `StorageKafka2` guard was added in this
commit because it was missing from the original patch — so the 25.8 carry is the
first version with both engines guarded.

### 26.3-aiven (this uplift)

- `still-needed`; ported as a clean cherry-pick. `git cherry-pick -n` was blocked
  by a repo hook, so the patch was applied via
  `git format-patch -1 37b61f443b --stdout | git apply` and the three files were
  `git add`-ed (functionally identical to `cherry-pick -n`: worktree + index get
  the patch, HEAD does not move).
- **HEAD note.** The dispatch prompt expected HEAD at `fd8bdaac976
  patch-port(026)`, but by dispatch time the maintainer had committed
  `patch-port(048)` (`72a22292ef5`, a Kafka-unrelated zero-copy lock-race fix) on
  top of 026. HEAD was therefore at `72a22292ef5`; it was kept unmoved. Patch 048
  does not touch `src/Storages/Kafka/`, so there is no interaction with this
  patch.
- Incremental build: 3 TUs (the three Kafka `.cpp`) + relink, exit 0.
- Verified with stateless test `9030_kafka_num_consumers_zero` + worktree-flip
  evidence pair (§4). Div-by-zero guards covered by inspection (§3).

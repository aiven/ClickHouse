# Patch N02 (refreshable-mv-create-if-not-exists) — keep coordinated refreshable MVs working on Apache ZooKeeper

> **NET-NEW Aiven patch (`N02`) — NOT a port / NOT a cherry-pick.** Authored fresh
> against `v26.3.15.4-lts`; there is no prior-LTS source commit, so `source_sha: none`
> and `byte_equivalent: n/a` throughout. `N02` is a per-cycle net-new handle
> (category D in `commit-hygiene.md` §1); commit subject `patch-new(N02):`.
>
> This is the **second** ZooKeeper-compatibility fix for refreshable MVs, after the
> ported patch [`050`](050-refreshable-mv-zookeeper.md). 050 removed the `MULTI_READ`
> requirement at MV-construction time; `N02` removes a *different* Keeper-only op
> (`CreateIfNotExists`) on the refresh-coordination hot path that was reintroduced by
> the upstream backport pulled in during the `.10.62 → .15.4` intra-LTS rebase.

## 0. Lineage

| LTS uplift | First-carry handle on aiven branch | Authored by | Outcome |
|---|---|---|---|
| 26.3-aiven | `N02` (staged) | Aiven RMV-ZK worker, 2026-06-30 | net-new authoring (no prior-LTS source) |

The current uplift's row stays "(staged)" until the human commits. There is no
25.3-aiven / 25.8-aiven row: in 25.8 the `/running` znode in
`RefreshTask::updateCoordinationState` was created with a **plain `Create`** (the
incompatible `ignore_if_exists=true` form did not exist), so there was nothing to fix
and nothing to inherit. The incompatibility is new in our 26.3 branch.

## 1. Purpose

Keep **coordinated refreshable materialized views** (a refreshable MV in a
`Replicated`/`Shared` database, which upstream forces into coordinated mode) working
when the cluster is backed by **real Apache ZooKeeper** instead of ClickHouse Keeper.
Aiven runs production clusters on plain ZooKeeper; this is the same motivation as
patch `050`.

The refresh-coordination path in `RefreshTask::updateCoordinationState` issues a
`multi` (atomic transaction) that creates the ephemeral `…/running` znode with
`ignore_if_exists=true`. That flag serializes the create as the ClickHouse-Keeper
protocol-extension op **`CreateIfNotExists` (OpNum 502)**. Apache ZooKeeper does not
implement that op and cannot even deserialize it inside a `multi`
(`org.apache.zookeeper.MultiOperationRecord.deserialize` → `Invalid type of op`). The
failure is not contained:

- ZooKeeper rejects the whole transaction.
- ClickHouse classifies the response as a transport-level **`Marshalling error`**
  (`Hardware error`), which **finalizes the shared ZooKeeper session**.
- Every `ReplicatedMergeTree` table on that node shares that session, so they all
  observe `Connection loss` / `Session expired` and drop into **readonly**
  (`ReplicatedMergeTreeRestartingThread: Couldn't start replication (table will be in
  readonly mode)`). The session reconnects, the refresh retries, and the cycle
  repeats — a self-sustaining outage triggered by a single refreshable MV.

This patch makes the `/running` create ZooKeeper-compatible while leaving the
ClickHouse-Keeper behavior byte-identical.

Source SHA on a previous LTS: **none — net-new Aiven patch authored against
`v26.3.15.4-lts`.**

### Root cause (how it was reintroduced)

The `multi`+`CreateIfNotExists` form was introduced upstream by
**#104051 / backport #105589** ("Refreshable MV: avoid duplicate refresh on brief
keeper connection loss", committed to our branch as `1387b697ba9`). It changed:

```diff
- zkutil::makeCreateRequest(coordination.path + "/running", coordination.replica_name, zkutil::CreateMode::Ephemeral);
+ zkutil::makeCreateRequest(coordination.path + "/running", coordination.replica_name, zkutil::CreateMode::Ephemeral, /*ignore_if_exists=*/ true);
```

The intent (idempotency: a retried create after a brief reconnect must not fail with
`ZNODEEXISTS`) is correct **against Keeper**, and upstream CI only runs Keeper, so the
regression is invisible upstream. It was pulled into our branch by the intra-LTS
rebase. Patch `050`'s §6 "ZooKeeper-compatibility verdict — COMPATIBLE" (which audited
the refresh ops as "set/create/check only") was correct when written and was
*invalidated* by this newer backport — `N02` restores that invariant.

## 2. Upstream-drift findings

> For a net-new patch there is no prior-LTS source to drift from. The relevant check
> is whether the chosen mechanism (the `CREATE_IF_NOT_EXISTS` feature flag) and the
> failing call site exist and behave as assumed on `v26.3.15.4-lts`.

### Commands run

```bash
# The incompatible op and its source flag:
grep -n "ignore_if_exists\|/running" src/Storages/MaterializedView/RefreshTask.cpp
grep -n "not_exists ? OpNum::CreateIfNotExists" src/Common/ZooKeeper/ZooKeeperCommon.h   # :248
# Client never downgrades CreateIfNotExists inside a multi (no feature-flag gate there):
grep -n "isFeatureEnabled\|CREATE_IF_NOT_EXISTS" src/Common/ZooKeeper/ZooKeeperImpl.cpp
# Keeper enforces the flag server-side per multi sub-op (so the test reproduces faithfully):
grep -n "isOperationSupported" src/Server/KeeperTCPHandler.cpp src/Coordination/KeeperContext.cpp
# Who reintroduced the op:
git log -1 -L 1459,1463:src/Storages/MaterializedView/RefreshTask.cpp
```

### Findings

- `ZooKeeperCreateRequest::getOpNum()` returns
  `not_exists ? OpNum::CreateIfNotExists : OpNum::Create` **unconditionally**
  (`ZooKeeperCommon.h:248`); the `multi` serializer writes each sub-op's raw
  `getOpNum()` with **no feature-flag downgrade** (`ZooKeeperCommon.cpp:1109`). Only
  the *standalone* helpers (`createIfNotExists`, `createAncestors`) honor
  `isFeatureEnabled(CREATE_IF_NOT_EXISTS)` (`ZooKeeper.cpp:512`). The hand-built
  `RefreshTask` `multi` is the gap.
- `DB::KeeperFeatureFlag::CREATE_IF_NOT_EXISTS` exists; `zkutil::ZooKeeper::isFeatureEnabled`
  is the public accessor (`ZooKeeper.h:236`). Against Apache ZooKeeper none of the
  CH-specific flags are advertised, so it returns `false` — the exact discriminator we
  need.
- Keeper enforces the flag **server-side**: `KeeperTCPHandler` validates every `multi`
  sub-op via `KeeperContext::isOperationSupported`, which maps
  `OpNum::CreateIfNotExists → CREATE_IF_NOT_EXISTS` (`KeeperContext.cpp:637-638`) and
  throws `BAD_ARGUMENTS "Unsupported operation: CreateIfNotExists"` when disabled. This
  makes an embedded Keeper with the flag off a faithful, deterministic stand-in for
  Apache ZooKeeper in the integration test (same technique as patch 050's
  `multi_read=0`).
- Conclusion: **net-new fix needed**; upstream has no equivalent on this LTS line.

## 3. C++ review

Apply `docs/aiven/skills/cpp-review-checklist.md`. One bullet per section.

- **1 Lifetime + ownership:** ✓ — no new heap state; `running_path` /
  `running_replica_name` are plain locals copied from the set-once-immutable
  `coordination.path` / `coordination.replica_name` before the `lock.unlock()`, so they
  are safe to read while the lock is released. The `zookeeper` `shared_ptr` outlives the
  call.
- **2 Exception safety:** ✓ — the added `zookeeper->exists()` and `tryMulti` run
  outside the lock (consistent with the pre-existing discipline). `tryMulti`'s result
  still flows through the unchanged `ZBADVERSION` short-circuit and
  `KeeperMultiException::check`, so the throw/rollback contract is unchanged. The
  in-memory `coordination.*` mutation happens only after a successful multi, as before.
- **3 Thread-safety + concurrency:** ✓ — the only writer of `…/running` is this
  replica's single-threaded coordination loop (under the task mutex); the real
  concurrency guard is the version-checked `Set`/`Check` on the root znode (the
  `ZBADVERSION` retry), not the create. The existence pre-check therefore has no
  meaningful TOCTOU: a second replica with the same `replica_name` is already treated as
  a misconfiguration elsewhere in this function. No sleeps.
- **4 Performance + memory:** n/a (not a hot path) — one extra `exists()` round-trip
  per refresh *start*, and **only on keepers without `CREATE_IF_NOT_EXISTS`** (real
  ZooKeeper). On ClickHouse Keeper there is zero added work (the `||` short-circuits on
  `create_if_not_exists == true`). Refresh-coordination updates are per-refresh, not
  per-row.
- **5 Settings as public API:** n/a — no setting. The behavior is selected
  automatically by the connected keeper's advertised feature flags, mirroring how the
  rest of the ZooKeeper client adapts (`createAncestors`, `getChildren`, etc.).
- **6 Error handling:** ✓ — reuses the existing `KeeperMultiException::check` path; no
  new error code. The emulation removes a *cause* of `KEEPER_EXCEPTION`, it does not add
  a new throw.
- **7 Upstream / vendored code:** ✓ — single edit to
  `src/Storages/MaterializedView/RefreshTask.cpp` plus one `#include`; no `contrib/**`,
  no `.github/**`, no build-system change.
- **8 Behavior under settings:** n/a-with-note — the change is inert on ClickHouse
  Keeper (feature flag advertised ⇒ identical to upstream) and only alters the
  ZooKeeper path, where the previous behavior was a hard failure. There is no gate to
  re-litigate; "off" is structurally impossible to need because Keeper users never enter
  the emulated branch.

## 4. Test design

(a) **New integration test that fails on the parent commit and passes after the patch.**

- Test path: `tests/integration/test_aiven_refreshable_mv_create_if_not_exists/`
  (`test.py`, `configs/keeper_no_create_if_not_exists.xml`,
  `configs/keeper_default.xml`).
- Topology: two single-node instances, each embedding its **own** Keeper. `node`
  embeds a Keeper with `<feature_flags><create_if_not_exists>0` (Apache ZooKeeper
  simulation) and hosts the differential; `node_cine` embeds a default Keeper
  (`CREATE_IF_NOT_EXISTS` enabled) and hosts the control. Per-node embedded Keeper is
  required because the cluster helper cannot force a feature flag **off** (only
  enable/randomize) — identical rationale to patch 050.
- Reproduction faithfulness: with the flag off, Keeper's `KeeperTCPHandler` rejects the
  `CreateIfNotExists` sub-op of the refresh `multi` with `BAD_ARGUMENTS "Unsupported
  operation: CreateIfNotExists"` — the server-side analogue of Apache ZooKeeper's
  `Invalid type of op` marshalling failure. (The embedded-Keeper sim reproduces the
  *op rejection*; it does not reproduce the Apache-ZooKeeper-specific session-kill
  cascade, which is downstream amplification of the same root cause and not needed to
  validate the fix.)
- Trigger: `SYSTEM REFRESH VIEW` forces the coordinated `RefreshTask` to call
  `updateCoordinationState(running=true)`, which builds the offending `multi`.
- Differential observable (per AGENTS.md §7): **whether a coordinated refresh
  completes** (rows land in the target) against a `CreateIfNotExists`-less keeper — a
  presence/absence-of-result differential, stronger than an error-substring assert.
  Pre-patch the refresh can never mark itself running, so the target stays empty and
  `assert_eq_with_retry` times out; post-patch the emulated plain create succeeds and
  the rows land.
- The `with_create_if_not_exists` control proves the feature-flag gating did not
  regress the ClickHouse-Keeper path (passes both pre- and post-patch).

Observed pre-patch failure surface: with the pre-patch binary the differential
case fails because the coordinated refresh's rejected op stalls the replicated
workload — the `INSERT` into the replicated source hits
`Code: 159 ... Wait for async insert timeout (120000 ms) exceeded ...
(TIMEOUT_EXCEEDED)` (the server-side `BAD_ARGUMENTS "Unsupported operation:
CreateIfNotExists"` is in the ClickHouse log, not pytest stdout). This is the
embedded-Keeper analogue of the production cascade (replicated tables unable to
make progress). Post-patch the same workload completes in seconds.

### Evidence (FAIL → PASS)

- Pre-patch (deliberate repro: unconditional `CreateIfNotExists`),
  `build/test_n02_prepatch_fail.log`:

  ```text
  test_aiven_refreshable_mv_create_if_not_exists/test.py::test_coordinated_refresh_without_create_if_not_exists FAILED
  E   ... Code: 159. DB::Exception: ... Wait for async insert timeout (120000 ms) exceeded:
        While executing WaitForAsyncInsert. ... (TIMEOUT_EXCEEDED)
        (query: INSERT INTO rmv_no_cine.src VALUES (1), (2), (3))
  1 failed in 136.34s (0:02:16)
  ```

- Post-patch (the shipped fix), `build/test_n02_refreshable_mv_cine.log`:

  ```text
  test_coordinated_refresh_without_create_if_not_exists PASSED [ 50%]
  test_coordinated_refresh_with_create_if_not_exists_still_works PASSED [100%]
  2 passed in 15.79s   (Success | Failures: 0/2)
  ```

## 5. Rollback considerations

- Revert safety: safe. Single-TU code change, no schema/on-disk/format change.
  Reverting re-introduces the unconditional `CreateIfNotExists`, which only re-breaks
  ZooKeeper-backed clusters; ClickHouse-Keeper deployments are unaffected either way.
- State surviving restart: none new. The `…/running` znode is the same ephemeral znode
  with the same content; only the op used to create it differs (plain `Create` vs
  `CreateIfNotExists`) and only on non-Keeper backends.
- Disable without rebuild: none — behavior is auto-selected by the keeper's advertised
  feature flags, by design.

## 6. Per-uplift notes

### 26.3-aiven (this uplift)

- Authoring: net-new against `v26.3.15.4-lts`; no cherry-pick.
- Mechanism: gate the `ignore_if_exists` create on
  `zookeeper->isFeatureEnabled(CREATE_IF_NOT_EXISTS)`; when unsupported, emulate the op
  with a plain create guarded by an `exists()` pre-check (the pre-check, not post-hoc
  `ZNODEEXISTS` tolerance, because `multi` is atomic — a failed create would roll back
  the sibling set/check).
- Build: warm-cache incremental `ninja clickhouse`, exit 0 (~55 s).
- Test added at: `tests/integration/test_aiven_refreshable_mv_create_if_not_exists/`.
  Verified FAIL → PASS: pre-patch differential `1 failed in 136.34s`
  (`INSERT` → `TIMEOUT_EXCEEDED`); post-patch `2 passed in 15.79s` (differential +
  Keeper control). See §4 Evidence.
- Relationship to 050: same goal (refreshable MVs on Apache ZooKeeper), different
  Keeper-only op, different call site (construction-time `MULTI_READ` vs refresh-time
  `CreateIfNotExists`). 050 and N02 are independent and both required.

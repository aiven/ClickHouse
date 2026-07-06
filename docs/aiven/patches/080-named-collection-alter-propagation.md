# Patch 080 — named-collection ALTER propagation on ZooKeeper

## 0. Lineage

| LTS uplift | Source SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `05fe09e9b199a6754035186a87493884a7f416f3` (+ test `01add3b46fd`, merge `a4b377111545`, PR #34) | Joe Lynch | original carry on `v25.8.26.11-lts-aiven` (post-fork-point addition) |
| 26.3-aiven | (staged, uncommitted) | screening 2026-07-15 | code applied verbatim; test re-homed under an Aiven-prefixed dir |

This is a **post-fork-point addition**: it is not in the frozen `v25.8.18.1-lts`
classification range. It appeared on `v25.8.26.11-lts-aiven` above the re-dated
protected-roles tip (`dacf0e75460` = row 079); see the 2026-07-15 source-branch
re-check note in `inventory.md`. Captured as inventory **row 080**, not an `N0x`
number (which is reserved for patches authored against 26.3 — this was authored on
25.8-aiven).

## 1. Purpose

Make in-place `ALTER NAMED COLLECTION ... SET/DELETE` propagate to other replicas
when the named-collections metadata is stored in **ZooKeeper**.

Original purpose (quoted from `git log` `05fe09e`):

> Propagate ALTER NAMED COLLECTION to replicas on ZooKeeper. A set on a child node
> does not fire the parent children watch nor bump its cversion on ZooKeeper, so an
> in-place ALTER was not observed by other replicas. Bump the root node data version
> on replace and watch it, so the change propagates.

## 2. The bug (invariant it protects)

In `NamedCollectionsMetadataStorage.cpp`, `ZooKeeperStorage` stores each collection
as a **child** znode under `root_path`. Replicas discover changes by:

- `list()` — `getChildren(root_path, &stat, wait_event)`, recording
  `collections_node_cversion = stat.cversion` and arming a **children-watch**;
- `shouldUpdate()` — re-reads the root and returns
  `stat.cversion != collections_node_cversion`.

`cversion` is bumped only when children are **added or removed** (CREATE / DROP of a
collection). An in-place `ALTER` calls `createOrUpdate(getPath(file_name), …)` — a
`set` on the *child* znode — which on ZooKeeper fires neither the parent
children-watch nor bumps the parent `cversion`. Consequence: other replicas never
observe `SET`/`DELETE`-key edits (CREATE/DROP worked because they change children).

The invariant restored: *a committed `ALTER NAMED COLLECTION` is eventually visible
on every replica.*

> Note: ClickHouse Keeper may coalesce/behave differently, but the contract must hold
> on real Apache ZooKeeper (cf. patch N02, same "ZooKeeper-parity" theme).

## 3. Fix

Five insertions in `src/Common/NamedCollections/NamedCollectionsMetadataStorage.cpp`
(`ZooKeeperStorage`), verbatim from `05fe09e`:

1. New member `mutable Int32 collections_node_version = 0;` beside
   `collections_node_cversion`.
2. `shouldUpdate` return also compares the root data version:
   `stat.cversion != collections_node_cversion || stat.version != collections_node_version`
   (`stat` already comes from the existing `tryGet(root_path, res, &stat)`).
3. `list()` additionally reads + records the root data version and arms a **data-watch**:
   `tryGet(root_path, root_data, &root_stat, wait_event); collections_node_version = root_stat.version;`
   (shares the same `wait_event`, so either a child change or an in-place edit wakes
   the refresh).
4. In `write()`'s `replace` branch, after `createOrUpdate`, call `bumpRootVersion()`.
5. New private `void bumpRootVersion() { getClient()->set(root_path, ""); }`.

### Why `set(root_path, "")` is safe

The root node is created empty in the constructor
(`client->createIfNotExists(root_path, "")`) and its **data is never read for
meaning** — `shouldUpdate` reads it into `res` but uses only `stat`. So bumping it is
a pure watch/version trigger, not a data mutation, and cannot clobber anything.

### 26.3 adaptation

None functionally. The only textual difference vs 25.8 is the `wait_event` member
type (`Coordination::EventPtr` in 26.3 vs `zkutil::EventPtr` in 25.8 — aliases of the
same type); `ZooKeeper::tryGet`'s 4th parameter is `const Coordination::EventPtr &`,
so the added `tryGet(..., wait_event)` compiles unchanged.

## 4. Gating decision — ungated (no setting)

This is a **correctness bug fix in an upstream code path**, not an Aiven policy
divergence. Gating it behind a default-off setting would ship the bug by default; a
default-on setting would only add a dead knob and would have to be stripped before
any eventual upstream contribution. Per the project rule ("gate policy, not fixes"),
it ships ungated. The real regression guard is running the upstream
`test_named_collections` suite before/after, not a runtime flag.

## 5. Test design

Test: `tests/integration/test_aiven_named_collection_alter_propagation/` (Aiven-
prefixed dir so the Aiven CI lane discovers it and no upstream test file is churned).
Two nodes (`node1`, `node2`), ZK-backed named-collections storage at
`/aiven_named_collections_path/`.

**Load-bearing harness detail — `use_keeper=False` (REAL Apache ZooKeeper).** The
default `with_zookeeper=True` in the integration harness starts *ClickHouse Keeper* in
ZK-compatibility mode, and **Keeper does NOT reproduce this bug** — an in-place child
`set` propagated to node2 even on the unpatched binary, so a Keeper-backed test is
*vacuous* (it passes pre- AND post-fix). This was observed empirically:
`build/test_nc_alter_propagation_2.log` (Keeper, unpatched) → PASS, distinguishing
nothing. The bug is a *real-ZooKeeper* watch semantic (see §2), so both instances pass
`use_keeper=False` to bring up a real Apache ZooKeeper ensemble (`zoo1/zoo2/zoo3`).
This also matches the Aiven managed-service topology (real ZooKeeper), and is the same
"mechanism-isolation" lesson as `docs/aiven/runbooks/integration-tests.md §7.2`: assert
on the ONE path with no masking layer.

Server config also sets `display_secrets_in_show_and_select=1` (server-level) *and*
the `format_display_secrets_in_show_and_select=1` profile setting — both are required
or `SELECT collection['key1'] FROM system.named_collections` returns `[HIDDEN]` and the
value assertion is meaningless.

Flow: `CREATE NAMED COLLECTION` on node1 → both replicas observe it (create bumps
`cversion`, works pre-fix) → in-place `ALTER ... SET` on node1 → assert node2 sees it
(propagation, the fixed path) → reverse-direction `ALTER ... SET ... DELETE` on node2
→ assert node1 sees it → assert the root child-znode set is unchanged (only data +
data-version changed). All waits are `assert_eq_with_retry` polling (no server-side
sleeps).

**Evidence pair (verified 2026-07-15, real ZooKeeper):**
- pre-fix (`git show HEAD:` of the storage file, rebuilt): **FAIL** —
  `build/test_nc_prefix_realzk.log`, `AssertionError: 'updated\tadded' != 'initial'`
  (node2 stuck on `initial`; the in-place SET never propagated).
- post-fix: **PASS** — `build/test_nc_postfix_realzk.log`, `1 passed … 29.35s` (both
  directions propagate; root children unchanged).

## 6. Regression risk

Touches the ZK named-collections path used by *all* ZK-backed named collections. The
extra per-in-place-ALTER `set` bumps the root `version`; no upstream test asserts on
the root's exact version, and the existing `test_named_collections::test_keeper_storage`
asserts only *children* equality (preserved). Guard = run `test_named_collections`
before/after.

## 7. Upstreaming

Candidate for upstreaming (the fix is upstream-shaped and ungated). If proposed
upstream, align with any existing ClickHouse fix for the "child `set` doesn't bump
parent `cversion`/fire children-watch" propagation gap first.

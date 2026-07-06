# Patch 035 — named-collection-integration-metadata

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | (unknown — to be researched at 25.X dossier merge) | (unknown) | (original carry, or "ported from 24.x") |
| 25.8-aiven | `65248bdd3464d6c5b279028b0084df295c725fb4` | Tilman Moeller `<tilman.moeller@aiven.io>` (author) / joelynch112@gmail.com (committer on source branch) | the version we're porting FROM |
| 26.3-aiven | `patch-port(035)` | T3.16 worker (this dispatch) | byte-equivalent (patch-ids match) |

The 26.3-aiven carry is its `patch-port(035)` commit (find it with `git log --grep '^patch-port(035)'`); per `commit-hygiene.md` §4 the `-aiven-dev` branch is resliced at each LTS transition, so we pin the stable subject handle, not a SHA that would re-hash.

## 1. Purpose

Aiven needs to track which named collections are owned or managed by an external
integration. To do that, two metadata keys — `integration_id` and
`integration_hash` — are stored directly inside a named collection alongside its
functional configuration. Upstream's `validateNamedCollection` rejects any key
that is neither a declared required nor optional key, so without this patch any
collection carrying the metadata fails validation with `BAD_ARGUMENTS` the moment
a storage or table function consumes it. The patch whitelists the two metadata
keys in `validateNamedCollection` so they are silently accepted, letting
integrations annotate collections without breaking the existing validation logic.

The durable "why": the metadata-tracking design treats named collections as
integration-owned objects; the marker keys must survive validation by every
consumer (`s3`, `url`, `mysql`, …) without each consumer having to list them as
optional. Centralizing the whitelist in the shared validator is the minimal,
single-site way to achieve that.

Source SHA on `v25.8.18.1-lts-aiven`: `65248bdd3464d6c5b279028b0084df295c725fb4`
(from the 26.3 uplift inventory, row 035).

Original author: `Tilman Moeller <tilman.moeller@aiven.io>` (per
`git log --format=%ae`); committed on the source branch by
`joelynch112@gmail.com`; co-authored by Aris Tritas `<aris.tritas@aiven.io>`.

Original purpose (verbatim from the source commit body):

> Add support for integration metadata to named collections validation
>
> We need to keep track of existing named collections. In order to do that,
> we wish to add some metadata to each collection. The metadata keys are
> added as optional collection parameters for the storages and functions
> that validate the keys.
>
> This change allows `integration_id` and `integration_hash` keys to be
> present in named collections without triggering validation errors. These
> metadata keys are whitelisted in the validation function, allowing
> integrations to track which collections they own or manage without
> breaking existing validation logic.
>
> Changes:
> - Added whitelist check for `integration_id` and `integration_hash` keys
>   in validateNamedCollection() function
> - These keys are now silently ignored during validation, allowing them
>   to be stored in named collections without being listed as required or
>   optional keys
>
> Co-authored-by: Aris Tritas `<aris.tritas@aiven.io>`

## 2. Upstream-drift findings

> Mandatory section. The point of this section is to verify the patch is still
> SEMANTICALLY correct against `v26.3.10.62-lts`, not just textually
> applicable.

### Commands run

```bash
for id in validateNamedCollection required_keys BAD_ARGUMENTS integration_id integration_hash; do
  printf '%s: ' "$id"; git grep -c "$id" -- src/ | awk -F: '{s += $NF} END {print s+0}'
done
git log --oneline v25.8.18.1-lts..v26.3.10.62-lts -- src/Storages/NamedCollectionsHelpers.h
git log --oneline --grep 'integration_id' v25.8.18.1-lts..v26.3.10.62-lts
```

### Findings

- **Identifier inventory** (counts on HEAD, `src/`):
  - `validateNamedCollection` — 24 occurrences (template + ~20 callers present).
  - `required_keys` — 12; `BAD_ARGUMENTS` — 2696. Both present.
  - `integration_id` — **0**; `integration_hash` — **0**. The whitelist does
    NOT yet exist on 26.3 → the patch is needed and is not obsoleted-by-upstream.
- **Upstream changes to the touched file between prior and current LTS**:
  - `src/Storages/NamedCollectionsHelpers.h`: three upstream commits in range —
    `5b6922d3337` "Clarify comment", `3a23ef4b4d3` "Track named collection
    dependencies for dictionary sources", `2311e59a6a4` "Add check to ensure no
    named collection is dropped if still in use". None touched the
    `validateNamedCollection` key-iteration loop's patched region; the ±5
    context around the insertion point is byte-identical to the source diff's
    pre-image (verified by reading HEAD lines 132–146).
- **Upstream changes that touched the patch's behavior** (the whitelist, the
  `Unexpected key` throw, the `BAD_ARGUMENTS` code): commit-message grep for
  `integration_id` over the LTS-to-LTS range returned EMPTY — no upstream
  commit introduced an equivalent whitelist.
- **Conclusion**: **`still-needed-and-applies`** — proceed with cherry-pick.

## 3. C++ review

Per `docs/aiven/skills/cpp-review-checklist.md`. One bullet per section.

- 1 Lifetime + ownership: `n/a — adds two literal string comparisons against the loop-local key (a std::string_view into the collection's key storage); no new owners, allocations, or pointer chains.`
- 2 Exception safety: `✓ — the inserted branch is a plain continue that BYPASSES a throw for two specific keys. It introduces no new throw site and acquires no resource, so the function's exception-neutral contract is preserved.`
- 3 Thread-safety + concurrency: `✓ — validateNamedCollection is a pure function over its arguments; the new branch reads only the immutable local key. No shared mutable state is touched, so the (read-only, called-at-configuration-time) concurrency model is unchanged.`
- 4 Performance + memory: `✓ — at most two extra string_view equality comparisons per key, only on the cold collection-validation path (executed when a storage/table-function consumes a named collection). Not measurable; zero allocation.`
- 5 Settings as public API: `n/a — no setting introduced or removed.`
- 6 Error handling: `✓ — the change suppresses the BAD_ARGUMENTS "Unexpected key" throw for integration_id/integration_hash only; every other unexpected key still throws unchanged. No error code added or removed.`
- 7 Upstream / vendored code: `✓ — src/Storages/NamedCollectionsHelpers.h is upstream-owned; this is a documented Aiven deviation (metadata whitelist). The commit body records the intent for the next LTS rebaser.`
- 8 Behavior under settings: `✓ — blast radius is limited to two reserved metadata keys that PREVIOUSLY caused a hard validation failure; no pre-existing legitimate collection could have used them (it would have errored). No existing object's behavior changes, so no setting gate is required (parent preflight clause (v): N/A).`

## 4. Test design

**(a) New test that fails on the parent commit and passes after the patch.**

- Test paths:
  - `tests/queries/0_stateless/9035_named_collection_integration_metadata.sh`
  - `tests/queries/0_stateless/9035_named_collection_integration_metadata.reference` (contains exactly `1\n`).

The test drives `validateNamedCollection` through a real fixed-key-set caller —
`StorageURL`, whose key set is `required = {url}`, `optional = {format,
compression, …}` and does NOT include the metadata keys. A `CREATE TABLE …
ENGINE = URL(<collection>)` with explicit columns validates the collection but
performs no network I/O (the URL engine connects only on read/write), so the
result is deterministic and offline. The test asserts the error code AND the
unexpected-key message substring on the pre-patch path, and a successful create
(`EXISTS TABLE → 1`) on the post-patch path.

Test body:

```sh
#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Named collections are server-global; scope the name to this test's database to avoid collisions.
NC_NAME="nc_integration_meta_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP TABLE IF EXISTS tbl_9035;
DROP NAMED COLLECTION IF EXISTS ${NC_NAME};
"

$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION ${NC_NAME} AS url = 'http://localhost:${CLICKHOUSE_PORT_HTTP}/', format = 'CSV', integration_id = 'abc123', integration_hash = 'def456';"

$CLICKHOUSE_CLIENT -q "CREATE TABLE tbl_9035 (c String) ENGINE = URL(${NC_NAME})" 2>&1 \
  | grep -oE 'BAD_ARGUMENTS|Unexpected key `integration_(id|hash)`' | sort -u

$CLICKHOUSE_CLIENT -q "EXISTS TABLE tbl_9035"

$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP TABLE IF EXISTS tbl_9035;
DROP NAMED COLLECTION IF EXISTS ${NC_NAME};
"
```

- Pre-patch run output (the FAIL), captured from `tmp/patch-035/test-prepatch.log`:

  ```text
  9035_named_collection_integration_metadata:                             [ FAIL ] 0.53 sec.
  Reason: result differs with reference:
  --- .../9035_named_collection_integration_metadata.reference
  +++ .../9035_named_collection_integration_metadata.stdout
  @@ -1 +1,3 @@
  -1
  +BAD_ARGUMENTS
  +Unexpected key `integration_hash`
  +0

  Having 1 errors! 0 tests passed.
  ```

- Post-patch run output (the PASS), captured from `tmp/patch-035/test-postpatch.log`:

  ```text
  9035_named_collection_integration_metadata:                             [ OK ] 0.66 sec.
  1 tests passed. 0 tests skipped. 0.68 s elapsed (Process-3).
  ```

- Why this test distinguishes the Aiven gate from upstream behavior (per AGENTS.md §7):
  the test asserts the message substring `` Unexpected key `integration_(id|hash)` ``
  — the text produced ONLY by `validateNamedCollection`'s unexpected-key branch —
  in addition to the `BAD_ARGUMENTS` code. A bare `BAD_ARGUMENTS` assertion would
  be insufficient (the URL engine throws `BAD_ARGUMENTS` for other reasons too,
  e.g. a bad `http_method`); pinning the message substring ties the failure to the
  exact code path the patch loosens. Pre-patch the metadata key is rejected
  (validation iterates keys alphabetically: `format` < `integration_hash` <
  `integration_id` < `url`, so `integration_hash` is the first unexpected key hit);
  post-patch both metadata keys are whitelisted and the table is created.

- **Known limitation (documented for future work)**: the test exercises one
  consumer (`StorageURL`). The patch loosens the SHARED validator, so every
  other consumer (`s3`, `mysql`, `postgresql`, dictionaries, …) benefits
  identically; covering each would be redundant for an evidence-of-causation
  pair. The URL path is sufficient because it reaches `validateNamedCollection`
  with the metadata key as genuinely unexpected and produces a clean offline
  signal.

## 5. Rollback considerations

- **Is the revert safe?** Yes. The patch is a 5-line additive whitelist in a
  pure validation function; reverting restores upstream's stricter validation.
  No schema migration, no on-disk format change.
- **Does the patch introduce any state that survives a `clickhouse-server`
  restart?** No. It only changes a runtime validation decision. Note: a
  collection created while the patch is active CAN persist `integration_id` /
  `integration_hash` keys to the named-collection store on disk; after a revert
  such a collection would again fail validation when consumed. This is the only
  durable interaction and is inherent to the feature, not to the code path.
- **Disable without rebuilding**: n/a — there is no setting gate (the change is
  unconditional, by design, with a two-key blast radius). To disable, revert.

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — to be filled in if and when the 25.3-aiven dossier merges into this file.

### 25.8-aiven (historical, may be empty)

Carried on `v25.8.18.1-lts-aiven` as commit
`65248bdd3464d6c5b279028b0084df295c725fb4`. Authored by Tilman Moeller,
co-authored by Aris Tritas; this is the source from which the 26.3 port was
performed.

### 26.3-aiven (this uplift)

- Cherry-pick was: **clean** (`git cherry-pick --no-commit -x` succeeded with
  only an "Auto-merging src/Storages/NamedCollectionsHelpers.h" notice — no
  conflict markers; the line-number shift from source line 128 to HEAD line 142
  was absorbed by git's 3-way merge).
- Upstream-drift conclusion: **still-needed-and-applies** (see §2).
- Patch-id verification: source and staged `git patch-id --stable` both equal
  `2dde2601cafbb1e6815bfe5e7d2d3a4a523f45b8` — `byte_equivalent: true`;
  decomposition diff empty.
- Test added at:
  `tests/queries/0_stateless/9035_named_collection_integration_metadata.{sh,reference}`.
  Pre-patch FAIL (`BAD_ARGUMENTS` + `` Unexpected key `integration_hash` `` +
  table-not-created) and post-patch PASS verified via the single-axis
  worktree-flip technique (`docs/aiven/runbooks/testing-suites.md` §6).
- Anything surprising: this build directory's `.ninja_deps` did NOT track the
  `NamedCollectionsHelpers.h` → consumer dependency, so the initial post-patch
  `ninja` recompiled **zero** of the header's ~38 consumers and the binary
  retained pre-patch validation behaviour. Because `validateNamedCollection` is
  a template ODR-folded across translation units, a partial rebuild risks
  linking a stale instantiation. Resolved by force-touching all `.cpp` includers
  (`git grep -l 'NamedCollectionsHelpers.h' -- 'src/**/*.cpp' | xargs touch`)
  before each `ninja`, which also repopulated `.ninja_deps` so the worktree-flip
  rebuilds tracked the header correctly thereafter. Tags `no-fasttest,
  no-replicated-database` mirror the sibling `03822_named_collection_drop_dependency_check.sh`
  (URL engine + global named collection).

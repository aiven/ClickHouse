# Patch 040 — disable-replicas-status-endpoint

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | (unknown — to be researched at 25.X dossier merge) | (unknown) | (original carry, or "ported from 24.x") |
| 25.8-aiven | `1151af44bb858b26d352958a4d8b4a9743c1ecc6` | Tilman Moeller `<tilman.moeller@aiven.io>` (author) / joelynch112@gmail.com (committer on source branch) | the version we're porting FROM |
| 26.3-aiven | `patch-port(040)` | T3.2 worker (this dispatch) | byte-equivalent (patch-ids match) |

The 26.3-aiven carry is its `patch-port(040)` commit (find it with `git log --grep '^patch-port(040)'`); per `commit-hygiene.md` §4 the `-aiven-dev` branch is resliced at each LTS transition, so we pin the stable subject handle, not a SHA that would re-hash.
## 1. Purpose

Remove the **default** registration of the `/replicas_status` HTTP endpoint to
reduce Aiven's default attack surface and prevent unintended exposure of
replication-state information for tenants who did not opt in. The endpoint
reports replication lag and per-replica state for `Replicated*MergeTree`
tables — useful for self-managed operators, but not appropriate to publish by
default on a multi-tenant managed service. Users who do need it can still opt
in via the `<http_handlers>` XML config (see §5 below).

Source SHA on `v25.8.18.1-lts-aiven`: `1151af44bb858b26d352958a4d8b4a9743c1ecc6`
(from the 26.3 uplift inventory).

Original author: `Tilman Moeller <tilman.moeller@aiven.io>` (per
`git log --format=%ae`); committed on the source branch by
`joelynch112@gmail.com`.

Original purpose (verbatim from the source commit body):

> Disable replicas_status endpoint
>
> This patch removes the default registration of the /replicas_status HTTP
> endpoint to reduce the default attack surface and prevent exposure of
> replication state information by default.
>
> The /replicas_status endpoint provides information about the status of
> replicated MergeTree tables, including replication lag and detailed state
> information. While useful for monitoring, this information should not be
> exposed by default for security and privacy reasons.
>
> Co-authored-by: Kevin Michel `<kevin.michel@aiven.io>`

## 2. Upstream-drift findings

> Mandatory section. The point of this section is to verify the patch is still
> SEMANTICALLY correct against `v26.3.10.62-lts`, not just textually
> applicable. Run the commands; record the findings; don't ship without them.

### Commands run

```bash
for id in replicas_status ReplicasStatusHandler addCommonDefaultHandlersFactory \
          HTTPRequestHandlerFactoryMain HandlingRuleHTTPHandlerFactory \
          attachNonStrictPath allowGetAndHeadRequest addPathToHints addHandler; do
  echo "=== $id ==="; git grep -c -- "$id" -- 'src/Server/' 'src/Core/' | head -10
done

git log v25.8.18.1-lts..v26.3.10.62-lts -- src/Server/HTTPHandlerFactory.cpp --oneline
git log v25.8.18.1-lts..v26.3.10.62-lts --grep 'replicas_status' --oneline
sed -n '310,345p' src/Server/HTTPHandlerFactory.cpp
```

Full output captured in `tmp/patch-040/drift-identifiers.log`,
`tmp/patch-040/drift-file-history.log`,
`tmp/patch-040/drift-grep-history.log`, and
`tmp/patch-040/drift-hunk-context.log`.

### Findings

- **Identifier inventory**: every identifier from the source diff is PRESENT
  on HEAD with count > 0 in `src/Server/`. In particular:
  - `replicas_status` — 6 occurrences in
    `src/Server/HTTPHandlerFactory.cpp` (5 in the default-registration block
    targeted by the patch, plus 1 in the user-configured `<http_handlers>`
    dispatch at lines 181-184 which the patch deliberately leaves alone).
  - `ReplicasStatusHandler` — class definition + header still present in
    `src/Server/ReplicasStatusHandler.{cpp,h}`; 3 mentions in
    `src/Server/HTTPHandlerFactory.cpp`.
  - `addCommonDefaultHandlersFactory` — definition + callers present (4
    occurrences in `src/Server/HTTPHandlerFactory.cpp`).
  - Factory-API methods (`attachNonStrictPath`, `allowGetAndHeadRequest`,
    `addPathToHints`, `addHandler`, `HandlingRuleHTTPHandlerFactory`,
    `HTTPRequestHandlerFactoryMain`) all present in the expected files.
- **Upstream changes to touched files between prior and current LTS**:
  - `src/Server/HTTPHandlerFactory.cpp`: several upstream commits touched
    this file in the `v25.8.18.1-lts..v26.3.10.62-lts` range — most notably
    `a4cb6c38` "Allow custom http_handlers per protocol endpoint",
    `d6d0a18e` "Put ACME request handler to its own file",
    `77173af5` "Add visualization", `a5692bf1` "feat: add clickstack to
    clickhouse http server". None of these touched the
    `/replicas_status` default registration; they live above and below the
    hunk.
- **Upstream changes that touched the patch's behavior** (`replicas_status`
  default registration, `ReplicasStatusHandler` class, the
  `addCommonDefaultHandlersFactory` function semantics):
  - Commit-message grep on the LTS-to-LTS range for `replicas_status`
    returned EMPTY — no upstream commit removed, disabled, or gated the
    endpoint default-registration.
  - The five `replicas_status_handler` lines exist verbatim in HEAD,
    shifted from line 314 (source diff) to line 335 (HEAD) due to
    unrelated upstream additions above them. The local hunk context —
    the `ping_handler` block above and the `play_handler` block below —
    is identical to the source diff's context, so git's 3-way merge
    applies the patch cleanly.
- **Conclusion**: **`still-needed-and-applies`** — proceed with cherry-pick.

## 3. C++ review

Per `docs/aiven/skills/cpp-review-checklist.md`. One bullet per checklist
section.

- 1 Lifetime + ownership: `n/a — patch is deletion-only; no new lifetimes, owners, or pointer chains introduced.`
- 2 Exception safety: `n/a — patch is deletion-only; removes a sequence of straight-line factory-registration calls. No new throw site, no rollback concern.`
- 3 Thread-safety + concurrency: `n/a — addCommonDefaultHandlersFactory runs once at server startup on the main thread, building an immutable handler chain consumed by the HTTP server's per-connection threads. Removing one handler from the build phase does not change the concurrency model.`
- 4 Performance + memory: `✓ — one fewer handler in the linear handler-resolution chain. Trivial micro-improvement on every HTTP request; not measurable. No new allocation.`
- 5 Settings as public API: `n/a — no setting introduced or removed. Behavior is config-driven via the existing <http_handlers> XML schema (see §5).`
- 6 Error handling: `n/a — patch introduces no new error code; the resulting 404 is produced by upstream's NotFoundHandler.`
- 7 Upstream / vendored code: `✓ — file src/Server/HTTPHandlerFactory.cpp is upstream-owned; patch is a documented default-policy deviation (Aiven hardening). The commit body explains the deviation so a future LTS rebaser sees the intent.`
- 8 Behavior under settings: `✓ — default behavior changes (endpoint no longer registered by default); users who need the endpoint can re-enable via <http_handlers> XML config — the handler_type "replicas_status" dispatch at lines 181-184 of HTTPHandlerFactory.cpp is the escape hatch and is intentionally left intact by this patch.`

## 4. Test design

**(a) New test that fails on the parent commit and passes after the patch.**

- Test paths:
  - `tests/queries/0_stateless/9040_disable_replicas_status_default.sh`
  - `tests/queries/0_stateless/9040_disable_replicas_status_default.reference` (contains exactly `404\n`).

Test body (a stateless `.sh` test that hits the default HTTP port with no
custom `<http_handlers>` config and asserts a `404` status):

```sh
#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS -o /dev/null -w "%{http_code}\n" \
    "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/replicas_status"
```

- Pre-patch run output (the FAIL), captured from `tmp/patch-040/test-prepatch.log`:

  ```text
  9040_disable_replicas_status_default:                                  [ FAIL ] 0.28 sec.
  Reason: result differs with reference:
  --- .../9040_disable_replicas_status_default.reference
  +++ .../9040_disable_replicas_status_default.stdout
  @@ -1 +1 @@
  -404
  +200

  Having 1 errors! 0 tests passed.
  ```

- Post-patch run output (the PASS), captured from `tmp/patch-040/test-postpatch.log`:

  ```text
  9040_disable_replicas_status_default:                                  [ OK ] 0.28 sec.
  1 tests passed. 0 tests skipped. 0.31 s elapsed (Process-3).
  ```

- Why this test distinguishes the Aiven gate from upstream behavior (per AGENTS.md §7):
  upstream registers `/replicas_status` by default in
  `addCommonDefaultHandlersFactory` — without this patch the endpoint responds
  HTTP 200 with the replication-state body. The test asserts HTTP 404, which
  is the response shape produced by upstream's `NotFoundHandler` when no
  handler claims the path. Only the absence of the default registration
  produces a 404 under default config; an upstream server (or this branch
  without the patch) would produce 200. The pre-patch FAIL (`expected 404 /
  actual 200`) and post-patch PASS form the evidence-of-causation pair.

- **Known limitation (documented for future work)**: the test exercises the
  default-config path only. Users (or Aiven operators) who re-enable the
  endpoint via `<http_handlers><rule><handler><type>replicas_status</type>`
  in server config are out of scope here — that opt-in path is still wired
  through `handler_type == "replicas_status"` at lines 181-184 of
  `src/Server/HTTPHandlerFactory.cpp` and would respond 200. Testing that
  path requires multi-config setup, which is integration-suite territory.

## 5. Rollback considerations

- **Is the revert safe?** Yes. The patch is deletion-only; revert restores
  the upstream default. No schema migration, no on-disk format change.
- **Does the patch introduce any state that survives a `clickhouse-server`
  restart?** No. The change is purely in startup-time HTTP routing setup.
  Nothing is persisted to ZooKeeper, disk, or in-memory caches that would
  outlive the process.
- **Escape hatch (re-enable without rebuilding)**: add an `<http_handlers>`
  block to the server config (or to a `config.d/` drop-in) such as:

  ```xml
  <http_handlers>
    <rule>
      <url>/replicas_status</url>
      <handler><type>replicas_status</type></handler>
    </rule>
  </http_handlers>
  ```

  This routes through the existing `handler_type == "replicas_status"`
  dispatch at lines 181-184 of `src/Server/HTTPHandlerFactory.cpp`, which the
  patch deliberately does not touch.

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — to be filled in if and when the 25.3-aiven dossier merges into this
file. Patch may have been carried on prior LTSes; the original-carry SHA is
recorded in the lineage table once researched.

### 25.8-aiven (historical, may be empty)

Carried on `v25.8.18.1-lts-aiven` as commit
`1151af44bb858b26d352958a4d8b4a9743c1ecc6`. Authored by Tilman Moeller; this
is the source from which the 26.3 port was performed.

### 26.3-aiven (this uplift)

- Cherry-pick was: **clean** (`git cherry-pick --no-commit -x` succeeded
  with only an "Auto-merging src/Server/HTTPHandlerFactory.cpp" notice — no
  conflict markers; the upstream-drift-induced line-number shift was
  absorbed by git's 3-way merge).
- Upstream-drift conclusion: **still-needed-and-applies** (see §2).
- Patch-id verification: source and staged `git patch-id --stable` both equal
  `ed8af313a50360e5952b907b01d03a8eab6a5e56` — `byte_equivalent: true`.
- Test added at:
  `tests/queries/0_stateless/9040_disable_replicas_status_default.{sh,reference}`.
  Pre-patch FAIL (expected 404, actual 200) and post-patch PASS verified
  via the single-axis worktree-flip technique
  (`docs/aiven/runbooks/testing-suites.md` §6).
- Time-to-port (subagent wall-clock): ~8.5 minutes
  (`2026-05-23T21:08:21Z` start of Step 0 → `2026-05-23T21:16:49Z`
  end of Step 8). Faster than budgeted because the build directory
  already had warm object/sccache state, so all three `ninja` runs
  finished in under a minute (66s "full" graph that hit the cache,
  22s pre-patch incremental, 14s post-patch incremental restore).
- Anything surprising: this dispatch was T3.2 — the first 26.3 dispatch
  exercising `tests.added: yes`, including the first real exercise of the
  worktree-flip technique. The runbook described it accurately and the
  postcondition checks (`git diff $PATCHED_FILES` empty,
  `git diff --cached --stat` unchanged) both held on flip-back. The
  technique is now validated for future workers.

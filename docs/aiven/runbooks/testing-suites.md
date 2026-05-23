# Runbook — testing suites for the Aiven LTS uplift

**Scope.** This runbook is the durable, version-agnostic reference for choosing, running, and authoring tests during the Aiven LTS uplift work. T3+ patch workers cite this runbook from their dispatch prompts; humans use it as a refresher when designing a test or reviewing a worker's test choice.

**Status convention.** Each section is labeled:

- **VERIFIED <ISO date>** — the content has been validated end-to-end against this repository.
- **PROVISIONAL — verify before use** — content designed from first principles but not yet exercised by a real patch dispatch in this lifecycle. The first worker who needs it MUST verify and promote.

**Update discipline.** When a section is promoted, the date is updated and a "What was actually run" or "What was actually observed" subsection records the evidence verbatim. PROVISIONAL caveats are not deleted — they document what was assumed at first.

**Companion runbook.** This runbook decides _which_ test to write and _why_. For _how_ to run a stateless test (env vars, runner flags, common failure modes), see `docs/aiven/runbooks/build-and-test.md` §5 — this runbook does not duplicate it.

---

## 1. The five test suites in this repository

**Status: VERIFIED 2026-05-23** (counts checked on the 26.3 branch at runbook authoring time; counts are indicative, the rest is normative)

| suite | location | what it tests | scaffolding | when to use |
|---|---|---|---|---|
| **Unit** | `src/**/tests/gtest_*.cpp` | Pure C++ correctness in isolation. No server. | `ninja unit_tests_dbms` then run `./build/src/unit_tests_dbms --gtest_filter=...` | Patches to classes/functions with a testable seam and no cross-process state. |
| **Stateless** | `tests/queries/0_stateless/` (`.sql`, `.sh`, `.j2`) | Server-level behavior expressible against a running default-configured server. | `tests/clickhouse-test <name>` — runner spins up DB per test. | Default. The vast majority of patches. |
| **Stateful** | `tests/queries/1_stateful/` | Same as stateless but assumes the `hits`/`visits` benchmark dataset is loaded. | `tests/clickhouse-test --no-stateless` | Only when the patch's effect requires a real dataset. |
| **Integration** | `tests/integration/<suite>/` (738 suite dirs at writing) | Multi-process / multi-container scenarios: ZK, MinIO, MySQL/PostgreSQL, restart cycles, cluster topology. | Python pytest + Docker. Per `AGENTS.md`: `python -m ci.praktika run "integration" --test <selector>`. | Patches whose effect requires multiple processes, restart, or external services. |
| **Fuzz** | `tests/fuzz/` and `src/**/tests/fuzz/` | Crashes / UB under random inputs (libFuzzer). | `ninja <fuzzer-target>` then run the target. | Patches that change input parsing or other untrusted-input surface. |

(Out-of-scope here but documented for completeness: `tests/performance/*.xml` for query-latency regression detection; `tests/sqllogic/` for compatibility with the sqllogic test corpus; `tests/jepsen.clickhouse/` for consistency / partition-tolerance; `tests/casa_del_dolor/` for chaos-style scenarios. These are CI-managed and not part of normal patch verification.)

**Indicative counts at 2026-05-23.**

```
tests/queries/0_stateless: 7 757 .sql, 2 232 .sh, 4 .py, 194 .j2 (~10 187 tests)
tests/queries/1_stateful:  small (dataset-dependent)
tests/integration:         738 suite dirs
tests/performance:         419 .xml
tests/fuzz:                23 dirs
```

These counts drift with each LTS uplift. Treat the _shape_ (stateless dominates by far) as normative; treat the _numbers_ as a snapshot.

## 2. The transport-follows-format decision rule

**Status: VERIFIED 2026-05-23**

ClickHouse exposes three primary transports. The test format follows the transport — this is not a preference, it is the only choice that produces robust tests.

| transport | test format | why |
|---|---|---|
| Native TCP (the `clickhouse-client` protocol) | `.sql` (with `.reference`) | The runner connects via TCP; the test body _is_ the SQL. |
| HTTP (port 8123 by default) | `.sh` using `$CLICKHOUSE_CURL` | `.sql` tests cannot reach the HTTP port. The `url()` table function is a workaround that loses status codes and requires allowlisting — do not use it for endpoint testing. |
| MySQL / PostgreSQL wire protocols | `.sh` invoking the respective CLI client, or integration test | Same reason as HTTP — `.sql` runs over native TCP only. |

**If the patch's user-observable effect is reachable only over HTTP / MySQL / PG protocol, the test MUST be `.sh` (or integration).** Trying to fit it into `.sql` produces a brittle test that proves the wrong thing.

**Decision tree for "which format do I write?"**

1. Does the patch change behavior that is observable purely via SQL over native TCP? → `.sql`.
2. Does the patch change behavior on an HTTP endpoint, MySQL handler, or anything outside the native TCP query path? → `.sh`.
3. Does the patch require a second process, ZooKeeper, an object store, or a restart cycle? → integration test.
4. Does the patch change an internal C++ class that exposes a clean testable seam (constructor + a few methods, no implicit Context dependency)? → consider a unit test in addition to a stateless one.

The default is stateless `.sql`. Deviate only when the rule above forces it.

## 3. Anatomy of a stateless `.sh` test

**Status: VERIFIED 2026-05-23**

Every `.sh` test follows the same skeleton (cf. `tests/queries/0_stateless/01528_play.sh`, which is 8 lines total):

```sh
#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# ... test body using $CLICKHOUSE_CLIENT, $CLICKHOUSE_CURL,
# $CLICKHOUSE_HOST, $CLICKHOUSE_PORT_HTTP, $CLICKHOUSE_PORT_HTTP_PROTO,
# $CLICKHOUSE_DATABASE, etc. ...
```

Pairing file: `<test-name>.reference` holds the expected stdout of the test body. The runner diffs actual stdout against `.reference`; identical → PASS, different → FAIL with a unified diff.

**Env vars provided by `shell_config.sh`** (most-used subset):

- `$CLICKHOUSE_CLIENT` — pre-configured `clickhouse client` invocation (correct port, no-progress, etc.).
- `$CLICKHOUSE_CURL` — `curl` invocation with sane defaults for testing.
- `$CLICKHOUSE_HOST`, `$CLICKHOUSE_PORT_TCP`, `$CLICKHOUSE_PORT_HTTP`, `$CLICKHOUSE_PORT_HTTP_PROTO` — connection coordinates injected by the runner; do not hardcode them.
- `$CLICKHOUSE_DATABASE` — the per-test database name the runner allocated; use it instead of `default`.

`.sql` tests are simpler — they are just SQL statements, one per line; the runner submits each via `clickhouse client` and concatenates output for the diff against `.reference`.

## 4. Adding a new stateless test

**Status: VERIFIED 2026-05-23**

ClickHouse stateless tests have a strict 5-digit numeric prefix on the filename. The next available prefix is allocated by a helper in the same directory:

```bash
cd tests/queries/0_stateless
./add-test <base_name>          # creates <NNNNN>_<base_name>.sql + .reference
./add-test <base_name>.sh       # creates <NNNNN>_<base_name>.sh + .reference (executable)
```

`add-test` reads the directory listing, finds the highest existing `^[0-9]+` prefix, increments by 1, zero-pads to 5 digits, and creates the files. For `.sh` it also `chmod +x`s and pre-fills the shebang + `shell_config.sh` source.

Per workspace `AGENTS.md`: always consult `add-test` to determine the prefix; do not invent a number, do not extend an existing test.

**Tags.** A test can declare tags on a `# Tags:` comment near the top (shell) or `-- Tags:` (SQL). Common tags: `no-random-settings`, `no-random-merge-tree-settings`, `no-fasttest`, `no-parallel`. Per workspace `AGENTS.md`: do not add `no-*` tags unless strictly necessary — they signal the test is fragile.

## 5. The pre/post evidence-pair requirement

**Status: PROVISIONAL — verify on first real `tests.added: yes` dispatch**

Per the halt-and-escalate schema, when a port records `tests.added: yes` the worker MUST produce **two** test-run logs:

- **`test-postpatch.log`** — the test, the patch applied, the test PASSES.
- **`test-prepatch.log`** — the same test, the patch _not_ applied, the test FAILS.

Both logs are evidence; both are referenced from the dossier. The pair proves two distinct claims:

1. _The patch achieves the intended behavior_ (PASS post-patch).
2. _The test actually exercises the patch's contribution_ (FAIL pre-patch).

**Why both are load-bearing.** A test that only passes post-patch could be passing for any number of incidental reasons; it does not prove the patch is what made it pass. A test that only fails pre-patch could be detecting any number of incidental absences. Only the pair pins the test to the patch.

**Anti-pattern: "swap the reference".** Writing the test, running it post-patch (PASS), then editing `.reference` to expect pre-patch output and re-running (FAIL) does not produce honest pre-patch evidence. It only proves that inverted assertions fail — a tautology. Do not accept this in review.

The recommended technique for producing honest pre-patch evidence is the single-axis worktree flip — see §6.

## 6. The single-axis worktree-flip technique

**Status: PROVISIONAL — verify on first real `tests.added: yes` dispatch (T3.2)**

To produce honest pre-patch evidence while keeping the user's review state (the staged cherry-pick) intact, the worker uses `git restore --worktree --source=HEAD <files>` — git's rare single-axis operation that modifies only the worktree, never HEAD, never the index.

**Mental model.** Git tracks three independent axes:

- **HEAD** — what branch/commit you are "on".
- **Index** (staging area) — what is staged for the next commit.
- **Worktree** — the actual files on disk that the compiler sees.

Most git commands (`commit`, `checkout`, `reset`) move two or three axes at once. `git restore --worktree --source=<tree> <files>` moves only the worktree. The compiler sees pre-patch source; HEAD and the index are unchanged.

**The flip procedure.** Inputs: a successful cherry-pick `--no-commit` of the patch's SHA, a post-patch incremental build, and the closed set of files in the patch's diff (`PATCHED_FILES`).

```bash
# Precondition: worktree + index = post-patch, HEAD = pre-patch (cherry-pick staged).
#               post-patch binary built in build/.

# 1) Post-patch evidence.
./tests/clickhouse-test ... <test_name> > tmp/patch-<NNN>/test-postpatch.log  # expect PASS

# 2) Flip worktree to pre-patch (index untouched — your staged cherry-pick survives).
git restore --worktree --source=HEAD $PATCHED_FILES

# 3) Incremental rebuild — only the patched .o(s) + final link change.
ninja -C build clickhouse

# 4) Pre-patch evidence.
./tests/clickhouse-test ... <test_name> > tmp/patch-<NNN>/test-prepatch.log   # expect FAIL

# 5) Flip worktree back to post-patch (from the index).
git restore --worktree $PATCHED_FILES

# 6) Incremental rebuild back to post-patch.
ninja -C build clickhouse

# Postcondition: worktree + index = post-patch, HEAD = pre-patch, post-patch binary.
#                Identical to the precondition. The user's review state is preserved byte-for-byte.
```

**Hard constraints.**

1. **HEAD never moves.** No `git checkout <sha|branch>`, no `git switch`. The deny hooks already block irreversible HEAD moves; the worker MUST NOT attempt to bypass them.
2. **Index never changes during the flip.** Use only `git restore --worktree ...` and `git restore --worktree --source=HEAD ...`. Never `git restore --staged`, never `git reset`, never `git checkout HEAD -- <file>` (that one DOES touch the index — easy mistake).
3. **`$PATCHED_FILES` is a closed set.** Derive it once from `git diff --cached --name-only` and use that variable for both flips. Never glob across the tree; never iterate the diff again between the two flips.
4. **Restore is unconditional.** If the pre-patch test run hangs, errors, or is interrupted, the worker MUST still execute step 5 (`git restore --worktree $PATCHED_FILES`) before halting, even on a failure path. The dossier logs the restore.
5. **Verify postcondition.** Before declaring the procedure done, the worker runs `git diff $PATCHED_FILES` (must be empty) and `git diff --cached --name-only` (must equal `$PATCHED_FILES`) to prove the state is restored.

**When the flip is not enough.**

- _Patch deletes a file._ `git restore --worktree --source=HEAD <deleted>` resurrects it (HEAD still has it). Flip-back removes it again (index says "deleted"). Works.
- _Patch adds a new file._ `git restore --worktree --source=HEAD <new-file>` removes it (HEAD does not have it). Flip-back recreates it from the index. Works.
- _Patch renames a file._ Treat as delete + add; restore both sides. Verify carefully.
- _Patch is in `contrib/` or generated code._ The flip still works mechanically, but the test design is usually different (most contrib/ patches do not have stateless tests). Use case-by-case judgment.

**Cost.** Two incremental rebuilds of ClickHouse. For a single-file patch the relink is the dominant cost (~3–5 minutes); the recompile of the changed .o is small. Empirical numbers will be added under "What was actually observed" on the first real run.

## 7. Worked example — testing HTTP endpoint registration

**Status: PROVISIONAL — referenced by T3.2 dispatch**

Patch 040 (Aiven 25.8 → 26.3) disables the default registration of the `/replicas_status` HTTP endpoint by deleting six lines in `src/Server/HTTPHandlerFactory.cpp`. The behavior change is:

- Pre-patch: `GET /replicas_status` → HTTP 200 with body listing replica state.
- Post-patch: `GET /replicas_status` → HTTP 404 (handled by `NotFoundHandler`).

**Format choice.** HTTP behavior → `.sh` (per §2). The reference analog is `tests/queries/0_stateless/01528_play.sh` (8 lines).

**Test body draft.**

```sh
#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS -o /dev/null -w "%{http_code}\n" \
  "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/replicas_status"
```

`.reference` content: `404` followed by a newline.

**Subtlety.** The patch removes only the _default_ registration in `addCommonDefaultHandlersFactory`. It does NOT remove the user-configured handler path (a server with `<http_handlers><rule><handler><type>replicas_status</type>...</type>` in config can still expose the endpoint). The stateless test runs against the default config and therefore exercises only the default path. If we ever want to test "user config can re-enable", that is an integration test, not a stateless one — and a separate, optional dossier section.

**Evidence pair.** Per §5 + §6, the worker produces `tmp/patch-040/test-postpatch.log` (PASS) and `tmp/patch-040/test-prepatch.log` (FAIL: expected `404`, got `200`). The dossier references both.

## 8. Common pitfalls

| Symptom | Likely cause | Fix |
|---|---|---|
| `.sh` test passes locally but fails in CI with "port refused" | Hardcoded `8123` instead of `$CLICKHOUSE_PORT_HTTP` | Use the env var |
| `.sql` test passes alone but fails in parallel runs | Used `default` database instead of `$CLICKHOUSE_DATABASE` | Use the env var; runner allocates per-test DB |
| Test depends on system time, file system state, random IDs | Non-determinism | Pin time with `now64(3, 'UTC')` etc.; use `randomString()` with a seed; or use a `.sh` test that captures a fixed substring with `grep` |
| Pre-patch test PASSES (not FAILS) | Test does not actually exercise the patched code path | Re-derive test from the diff; the patch must change observable output, not internals |
| Both test runs produce the same output | One of the two flips silently no-op'd (build did not pick up the change) | Verify `git diff $PATCHED_FILES` after each flip; rebuild fully if in doubt |
| `git restore --worktree --source=HEAD` deletes uncommitted edits | The worker had unstaged edits in `$PATCHED_FILES` before the flip | Hard rule: the cherry-pick must be staged cleanly with no extra unstaged edits before §6 begins |

## 9. What is deliberately not in this runbook

- **CI-specific harness.** How Praktika orchestrates the suites, retry logic, sanitizer matrix — out of scope; documented separately in the CI workstream.
- **Performance regression detection.** Performance tests have their own dispatch flow; see the `.claude/tools/fetch_perf_report.py` workspace rule for CI-side analysis.
- **Fuzz dispatching.** Fuzz harnesses are not part of the patch-port verification tier. A patch that warrants fuzz coverage gets a separate optional dossier section.
- **Sanitizer builds.** Same as `build-and-test.md` §3 — added when first needed.
- **Multi-LTS test backporting.** Whether a test we add for the current LTS also lands in older lines is a release-management decision, not a worker concern.

Each omission is intentional and is named here so a future reader knows the gap is known, not forgotten.

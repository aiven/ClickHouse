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

**Status: VERIFIED 2026-05-25** (T3.3 patch 011; Aiven convention adopted and verified end-to-end through the worktree-flip evidence pair on both `9040_*` and `9011_*`)

Aiven LTS uplift work uses a deliberately different test-naming convention from upstream's, to avoid future-rebase collisions. Read §4.1 (the convention you actually use) and skim §4.2 (the upstream convention you do NOT touch, but should recognize when reading the test directory).

### 4.1 Aiven convention — `9<NNN>_<slug>.{sh,sql}` (use this for all Aiven uplift work)

**Filename shape**:

```
tests/queries/0_stateless/9<NNN>_<slug>.{sh,sql,reference}
```

where `<NNN>` is the 3-digit Aiven patch dossier number (the same number as in `docs/aiven/patches/<NNN>-<slug>.md`) and `<slug>` is a short snake_case description.

**Examples** (from the 26.3 uplift at runbook authoring time):

| Patch dossier | Test path |
|---|---|
| `docs/aiven/patches/040-disable-replicas-status-endpoint.md` | `tests/queries/0_stateless/9040_disable_replicas_status_default.{sh,reference}` |
| `docs/aiven/patches/011-restrict-show-create-database-access.md` | `tests/queries/0_stateless/9011_restrict_show_create_access.{sh,reference}` |

The prefix is derived directly from the patch number — there is no allocator to consult, no sequence counter to maintain, and no risk of two patches racing for the same test number.

**If one patch needs multiple tests**, append a disambiguator to the slug while keeping `9<NNN>_` stable: `9<NNN>_<slug>_a.sh`, `9<NNN>_<slug>_b.sh`. Do not allocate sibling numbers like `9NNN1`, `9NNN2`; that breaks the patch ↔ prefix bijection.

**Why this shape (load-bearing detail).** Upstream's `tests/queries/0_stateless/add-test` is a naive allocator: it scans for the highest existing `^[0-9]+` prefix and increments. Currently it sits in the `04XXX` range. If we placed Aiven tests in the upstream growth zone (`04XXX`, `05XXX`, ...), two problems would emerge:

1. **Allocator pollution.** The moment any Aiven test exists at a higher prefix than the upstream max, `add-test` would jump there for any subsequent call — breaking upstream-style allocation for anything else in the fork.
2. **Rebase collisions.** On the next LTS rebase upstream's allocator will eventually claim numbers we used downstream, forcing a per-rebase rename burden.

The `9XXXX` partition is virgin territory upstream-wide (0 of ~20,500 tests on this branch use a non-`0` leading digit). It gives a permanent reservation with no need to modify upstream's `add-test`. The runner accepts the non-monotonic prefix because `tests/clickhouse-test` falls back via try/except at `tests/clickhouse-test:3121` and `:3925`: numeric prefix → sort key `int(prefix)`; non-numeric → sort key `99997`. The Aiven `9NNN` tests sort cleanly after all `0XXXX` upstream tests, in numeric order among themselves.

**Hard constraints for workers.**

1. **DO NOT run `./add-test`** for Aiven test creation. Name the files directly with the `9<NNN>_<slug>` shape.
2. **DO NOT pick a different range** (`1XXXX`, `5XXXX`, etc.). The `9` prefix is the convention; arbitrary deviation breaks the partition rationale and makes the test list harder to grep.
3. **DO NOT reuse `9<NNN>` across patches.** The number is derived from the dossier; conflicts mean either a dossier-numbering bug or two patches sharing a slot, both of which need escalation.
4. **If a patch ports a test from upstream** (i.e., the test was authored upstream and the cherry-pick adds it under its original `0XXXX` prefix), leave the upstream prefix alone — that test is upstream-owned, not Aiven-introduced. Only NEWLY-AUTHORED Aiven tests use `9<NNN>_`.

**Authoring a `9<NNN>_*.sh` test by hand** (since `add-test` is not used):

```bash
NNN=011   # the patch dossier number
SLUG=restrict_show_create_access
PREFIX=9${NNN}
cat > tests/queries/0_stateless/${PREFIX}_${SLUG}.sh <<'EOF'
#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# ... test body ...
EOF
chmod +x tests/queries/0_stateless/${PREFIX}_${SLUG}.sh
touch tests/queries/0_stateless/${PREFIX}_${SLUG}.reference
```

(The `.sh` body skeleton — `CURDIR=`, `shell_config.sh` source, etc. — is the same as upstream's, just authored manually instead of `add-test`-generated.)

### 4.2 Upstream convention — `add-test` allocator (for context only)

Upstream-owned stateless tests use a strict 5-digit numeric prefix on the filename, allocated by:

```bash
cd tests/queries/0_stateless
./add-test <base_name>          # creates <NNNNN>_<base_name>.sql + .reference
./add-test <base_name>.sh       # creates <NNNNN>_<base_name>.sh + .reference (executable)
```

`add-test` reads the directory listing, finds the highest existing `^[0-9]+` prefix, increments by 1, zero-pads to 5 digits, and creates the files. For `.sh` it also `chmod +x`s and pre-fills the shebang + `shell_config.sh` source.

**Aiven LTS uplift workers do NOT touch `add-test`.** This section exists so workers can recognize what upstream-style tests look like in the directory listing (and not be confused when they encounter a stateless test in the `04XXX` range that was cherry-picked from upstream as part of a patch).

If upstream's max prefix ever crosses `09999` (decades away at current growth rate), the `9XXXX` Aiven partition will need rethinking. The reservation is intentional but not eternal.

### 4.3 Tags

A test can declare tags on a `# Tags:` comment near the top (shell) or `-- Tags:` (SQL). Common tags: `no-random-settings`, `no-random-merge-tree-settings`, `no-fasttest`, `no-parallel`. Per workspace `AGENTS.md`: do not add `no-*` tags unless strictly necessary — they signal the test is fragile.

### 4.4 Aiven convention for integration tests — `test_aiven_<slug>/`

**Status: VERIFIED-with-discipline 2026-05-28** (rule-of-three reached: T3.6 patch 006 + T3.9 patch 005 + T3.14 patch 049; promoted from VERIFIED-with-precedent at the third independent application).

**Directory shape:**

```
tests/integration/test_aiven_<slug>/
                  ├── test.py                                # the pytest module
                  └── configs/                               # any per-test XML overrides
                      └── <name>.xml
```

The `test_aiven_` prefix is the integration-test analogue of the `9<NNN>_` numeric prefix used for stateless tests (§4.1). Same motivation: avoid future-upstream-merge collisions. Different mechanism: directory names are alphabetic, so we reserve a string prefix instead of a numeric range. Upstream has zero `test_aiven_*` directories at the time of writing — the prefix is exclusively ours.

**Example** (from the 26.3 uplift at runbook authoring time):

| Patch dossier | Test directory |
|---|---|
| `docs/aiven/patches/006-replicated-database-attach-with-shard-macro.md` | `tests/integration/test_aiven_replicated_database_attach_with_shard_macro/` |
| `docs/aiven/patches/005-tolerate-zk-restart-with-exponential-backoff.md` | `tests/integration/test_aiven_zk_connect_retry/` |
| `docs/aiven/patches/049-refreshable-mv-shard-macro-expansion.md` | `tests/integration/test_aiven_refreshable_mv_shard_macro_expansion/` |

The slug after `test_aiven_` SHOULD match the patch dossier slug (without the leading `NNN-`). When a single patch needs multiple integration tests, append a disambiguator inside the slug while keeping the prefix stable: `test_aiven_<slug>_a/`, `test_aiven_<slug>_b/`.

**Hard constraints for workers.**

1. **DO NOT use upstream-style names** (e.g. `test_replicated_database_<something>/`) for newly-authored Aiven integration tests. The prefix is the marker that lets a future maintainer grep `tests/integration/test_aiven_*` and find every Aiven-only test in O(1).
2. **DO NOT use the `9<NNN>_` numeric scheme for integration tests.** That scheme is reserved for stateless. Numeric prefixes inside a directory tree sort poorly against the existing `test_*` shape and would be visually jarring.
3. **DO NOT rename an upstream-cherry-picked integration test** to `test_aiven_*`. If the test was authored upstream and lands here via cherry-pick, it remains upstream-owned (under its original name). The prefix marks Aiven origin, not Aiven custody.

**Why a directory prefix and not a numeric one.** Integration tests live in per-test directories (not individual files), so the unit being named is a directory. The runner discovers tests via `Path("./tests/integration/").glob("test_*/test*.py")` (`ci/jobs/integration_test_job.py:257`); a directory matching this glob with our reserved prefix is unambiguous. A numeric prefix on the directory name (e.g. `test_9006_<slug>/`) would technically work but would (a) look like a typo to a casual reader, and (b) sort awkwardly between `test_*` directories. The `test_aiven_*` prefix sorts neatly into the alphabetical listing alongside `test_a...` and is unmistakably ours.

**Running an Aiven integration test locally.** See `docs/aiven/runbooks/integration-tests.md` — that runbook covers the pip-install set, env-var exports, and the pytest invocation. This section only fixes the naming.

### What was actually observed (2026-05-25)

T3.2 (patch 040) originally landed at `04206_disable_replicas_status_default` via `add-test`'s allocator before the convention was decided. T3.3 (patch 011) was about to repeat the pattern at `04207_restrict_show_create_access` when the convention was adopted. Both tests were renamed in the same per-uplift cleanup pass (bundled into the T3.3 patch-port amend at `bdfd3c7327b`): `04206_disable_replicas_status_default → 9040_disable_replicas_status_default`, `04207_restrict_show_create_access → 9011_restrict_show_create_access`. The renames were detected as `R100` by git (pure renames, identical content); the runner accepted the new prefixes without modification; the patches' pre/post evidence pairs are valid under the new names. Patch dossiers, retrospectives, and the worked example in §7 were updated to match.

## 5. The pre/post evidence-pair requirement

**Status: VERIFIED 2026-05-24** (T3.2 patch 040; subagent id `083abbef-ded9-4be2-8481-1f12ff4ad588`)

Per the halt-and-escalate schema, when a port records `tests.added: yes` the worker MUST produce **two** test-run logs:

- **`test-postpatch.log`** — the test, the patch applied, the test PASSES.
- **`test-prepatch.log`** — the same test, the patch _not_ applied, the test FAILS.

Both logs are evidence; both are referenced from the dossier. The pair proves two distinct claims:

1. _The patch achieves the intended behavior_ (PASS post-patch).
2. _The test actually exercises the patch's contribution_ (FAIL pre-patch).

**Why both are load-bearing.** A test that only passes post-patch could be passing for any number of incidental reasons; it does not prove the patch is what made it pass. A test that only fails pre-patch could be detecting any number of incidental absences. Only the pair pins the test to the patch.

**Anti-pattern: "swap the reference".** Writing the test, running it post-patch (PASS), then editing `.reference` to expect pre-patch output and re-running (FAIL) does not produce honest pre-patch evidence. It only proves that inverted assertions fail — a tautology. Do not accept this in review.

The recommended technique for producing honest pre-patch evidence is the single-axis worktree flip — see §6.

### What was actually observed (2026-05-24)

T3.2 (patch 040, `Disable replicas_status endpoint`) was the first dispatch to ship `tests.added: yes`. The schema requirement held without amendment: the worker produced `tmp/patch-040/test-postpatch.log` (PASS, `OK 0.28 sec`) and `tmp/patch-040/test-prepatch.log` (FAIL with unified diff `expected 404 / actual 200`) against the same test (currently `9040_disable_replicas_status_default`; the test was named `04206_disable_replicas_status_default` at dispatch time, before the Aiven test-naming convention in §4.1 was adopted) built from two different worktree states (see §6 for the technique that produced the two binaries). Both logs are referenced from the dossier's §4 and from the halt-and-escalate report's Evidence section. No anti-pattern (swap-the-reference) was attempted; the worker followed the procedure literally.

## 6. The single-axis worktree-flip technique

**Status: VERIFIED 2026-05-24** (T3.2 patch 040)

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

**Cost.** Two incremental rebuilds of ClickHouse. For a single-file patch the relink is the dominant cost (~3–5 minutes); the recompile of the changed .o is small.

### What was actually observed (2026-05-24)

T3.2 (patch 040) executed the procedure literally for a single-file 6-line deletion patch in `src/Server/HTTPHandlerFactory.cpp`. Empirical numbers (note: warm cache — build directory had hot sccache state from a prior session; cold-cache numbers will be larger):

- 6c (flip to pre-patch + incremental rebuild): `ninja -C build clickhouse` 22 seconds, exit 0.
- 6f (flip back to post-patch + incremental rebuild): `ninja -C build clickhouse` 14 seconds, exit 0.

Postconditions verified by the parent agent after the worker halted: `git diff src/Server/HTTPHandlerFactory.cpp` empty (worktree matches index); `git diff --cached --stat` shows the original 6 deletions still staged; HEAD unchanged at `v26.3.10.62-lts-aiven-dev`. The technique is now empirical, not theoretical.

## 7. Worked example — testing HTTP endpoint registration

**Status: VERIFIED 2026-05-24** (T3.2 patch 040)

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

### What was actually observed (2026-05-24)

T3.2 implemented this example verbatim. The test file is `tests/queries/0_stateless/9040_disable_replicas_status_default.sh`, 9 lines. (The dispatch predated the Aiven convention in §4.1, so the worker originally used `add-test` and got prefix `04206`; the extra line vs. the `01528_play.sh` analog is `add-test`'s `CUR_DIR` / blank-line idiom and is now historical baggage — handwritten `9<NNN>_*.sh` tests should follow `01528_play.sh`'s 8-line shape exactly. The rename to `9040_*` happened in a later per-uplift commit; see the §4 "What was actually observed" note.) `.reference` is exactly `404\n` (4 bytes, verified by `od -c`). The Aiven-vs-upstream gate distinction (404 from `NotFoundHandler` vs. 200 from default registration) was recorded in the dossier's §4 along with the documented known limitation (default-config-only — opt-in via `<http_handlers>` not covered). The worker noted but did NOT spawn an integration test for the opt-in path; that is now an explicit future-work item tracked in the dossier and the T3.2 retrospective rather than in scope.

## 8. Recurring stateless test recipes

**Scope.** This section codifies recurring shapes for writing stateless tests that exercise specific kinds of ClickHouse-internal state. Each subsection is independently labeled VERIFIED or PROVISIONAL based on rule-of-three counters; promote PROVISIONAL → VERIFIED on the third independent application.

### 8.1 `system.zookeeper`-as-observable-assertion

**Status: PROVISIONAL 2026-05-28** (rule-of-three counter: **2 of 3**; T3.7 patch 010 + T3.15 patch 042; promote to VERIFIED on third use).

When a patch's defended behavior manifests as a change in ZooKeeper metadata (a znode getting created/deleted, a znode's value being updated, an empty parent being garbage-collected), the **canonical stateless test recipe** is a `SELECT … FROM system.zookeeper WHERE path = '…'` assertion. ClickHouse's own ZK introspection makes this trivial to author and ~100× cheaper to run than an integration test.

**The recipe.** Two shape variants:

| Variant | Assertion | When to use |
|---|---|---|
| **Value-assertion** | `SELECT value FROM system.zookeeper WHERE path = '<znode-path>'` | Patch changes the *content* of a znode (a setting default, a serialized config blob). |
| **Count-assertion** | `SELECT count() FROM system.zookeeper WHERE path = '<parent-path>' AND name = '<child>'` | Patch changes whether a znode *exists* (created/deleted/leaked). |

Both variants are pure SQL — no helper functions, no per-test config overrides, no integration-test infrastructure.

**Worked examples.**

| Patch | Variant | Test | Pre/post differential |
|---|---|---|---|
| `010-default-logs-to-keep` (T3.7) | Value | `9010_default_logs_to_keep.sql` | Pre: `value = '1000'`; post: `value = '300'`. |
| `042-zk-node-leak-after-create-delete-table` (T3.15) | Count | `9042_zk_node_leak_after_create_delete_table.sql` | Pre: `count() = 1` (parent znode survives DROP); post: `count() = 0` (cleaned up). |

**Hard constraint for either variant.** The ZK path the test inspects MUST be deterministic across test runs. Use `currentDatabase()` substitution in the path (`'/test/<slug>/' || currentDatabase() || '/...'`) so parallel test runs don't collide. **Avoid `{uuid}` macros in the path** unless the test explicitly captures the UUID — the parent runner randomizes per-test database UUIDs and a path containing `{uuid}` becomes unpredictable from the test body.

**When the recipe does NOT apply.** Patches whose ZK effect requires multi-node coordination (DDL log entry propagation, replica election, replica catch-up) are unreachable from a single-node stateless test. Those go integration per `integration-tests.md §7`.

**Counterexample / what learning from patch 060 looks like.** Patch 060 (`alter-order-by-sorting-key-zk-metadata`) attempted a `system.zookeeper` test three times (T3.10/T3.11/T3.12) and could not produce evidence-of-causation on 26.3 — but that is NOT a counterexample to the recipe. The recipe failed because the test trigger could not reach the patched code on either LTS (upstream sanity-check gates upstream of `StorageReplicatedMergeTree::alter`); the `system.zookeeper` assertion would have worked if a reachable trigger existed. See retro 11 for the full saga and the `(iv) reachability check` discipline that emerged from it.

**Promotion criterion.** Stays PROVISIONAL until a third independent application produces a clean evidence pair. The most likely candidate is a future patch in the **DDL-coordination / quorum / metadata** family (e.g., `quorum_status`, `pending_mutations`, `replication_queue` — all live in ZK and inspectable via `system.zookeeper` paths). On the third use, this section graduates to VERIFIED-with-discipline.

## 9. Common pitfalls

| Symptom | Likely cause | Fix |
|---|---|---|
| `.sh` test passes locally but fails in CI with "port refused" | Hardcoded `8123` instead of `$CLICKHOUSE_PORT_HTTP` | Use the env var |
| `.sql` test passes alone but fails in parallel runs | Used `default` database instead of `$CLICKHOUSE_DATABASE` | Use the env var; runner allocates per-test DB |
| Test depends on system time, file system state, random IDs | Non-determinism | Pin time with `now64(3, 'UTC')` etc.; use `randomString()` with a seed; or use a `.sh` test that captures a fixed substring with `grep` |
| Pre-patch test PASSES (not FAILS) | Test does not actually exercise the patched code path | Re-derive test from the diff; the patch must change observable output, not internals |
| Both test runs produce the same output | One of the two flips silently no-op'd (build did not pick up the change) | Verify `git diff $PATCHED_FILES` after each flip; rebuild fully if in doubt |
| `git restore --worktree --source=HEAD` deletes uncommitted edits | The worker had unstaged edits in `$PATCHED_FILES` before the flip | Hard rule: the cherry-pick must be staged cleanly with no extra unstaged edits before §6 begins |

## 10. What is deliberately not in this runbook

- **CI-specific harness.** How Praktika orchestrates the suites, retry logic, sanitizer matrix — out of scope; documented separately in the CI workstream.
- **Performance regression detection.** Performance tests have their own dispatch flow; see the `.claude/tools/fetch_perf_report.py` workspace rule for CI-side analysis.
- **Fuzz dispatching.** Fuzz harnesses are not part of the patch-port verification tier. A patch that warrants fuzz coverage gets a separate optional dossier section.
- **Sanitizer builds.** Same as `build-and-test.md` §3 — added when first needed.
- **Multi-LTS test backporting.** Whether a test we add for the current LTS also lands in older lines is a release-management decision, not a worker concern.

Each omission is intentional and is named here so a future reader knows the gap is known, not forgotten.

<!-- subagentStop archive | source=transcript | status=completed | type=general-purpose | ts=2026-06-11T15:07:57Z -->

Here is the exit report.

---
outcome: success
patch_slug: disable-table-engines-and-functions
source_sha: 4641e51fdc021ba901a46c65f67fff8515db0c00
proposed_commit:
  staged_files:
    - src/Common/config.h.in
    - src/Storages/registerStorages.cpp
    - src/Storages/ObjectStorage/registerStorageObjectStorage.cpp
    - src/TableFunctions/TableFunctionRemote.cpp
    - src/TableFunctions/registerTableFunctions.cpp
    - src/configure_config.cmake
    - docs/aiven/patches/051-disable-table-engines-and-functions.md
  commit_message: |
    patch-port(051): Added support for disabling various table engines and table functions

    Original author: Tilman Moeller <tilman.moeller@aiven.io>, 2026-01-08.

    (cherry picked from commit 4641e51fdc021ba901a46c65f67fff8515db0c00)

    26.3 adaptation: added the matching option(REGISTER_<NAME> ... ON) block to
    src/configure_config.cmake (absent from the 25.8 source, which relied on the
    Aiven build passing explicit -D flags). Without it, #cmakedefine01 resolves the
    undefined symbols to 0 and silently disables the engines/functions. All flags
    default ON, mirroring patch-port(045) — no default-behavior change. byte_equivalent: false.
  byte_equivalent: false
tests:
  added: no_justified
  kind: upstream-existing
  paths: []
  upstream_reference:
    - tests/queries/0_stateless/00832_storage_file_lock.sql
    - tests/queries/0_stateless/00288_empty_stripelog.sql
  pre_patch_fail_verified: false
  post_patch_pass_verified: false
  justification: |
    Build-system-only patch: 29 compile-time REGISTER_* flags + #if guards, all
    default ON via option(... ON), so the default build is behavior-identical to
    upstream and is exercised by the entire existing stateless/stateful suite. A
    default-ON flag has no differential-observability pair (gated registrations
    behave identically pre/post-patch), so no new stateless test is feasible;
    the disable path is Aiven-managed-build-only (-DREGISTER_<NAME>=0), out of
    repo scope. Mirrors committed sibling 045 (which shipped no test). Two
    representative existing tests that exercise gated surfaces were run against
    the staged post-patch binary and PASSED:
    00832_storage_file_lock.sql (File engine -> REGISTER_FILE_TABLE_ENGINE) and
    00288_empty_stripelog.sql (StripeLog -> REGISTER_LOG_TABLE_ENGINE). Correctness
    additionally verified by Step 5 (generated config.h: all 29 flags = 1; smoke
    counts system.table_functions=6, system.table_engines=6).
escalation_reason: none
---

## Tier results

- Tier 1 (textual cherry-pick): **pass** — cherry-pick hit the predicted trivial one-line-shift conflicts in `config.h.in` + `TableFunctionRemote.cpp` (045's `YTSAURUS` line), resolved by hand keeping the 16 dictionary flags intact; other 3 files merged clean; PC-1 `option(... ON)` block added → 7 files staged, flag↔option coverage 29 == 29 (empty diff).
- Tier 2 (semantic patch-id): **pass** (intentional MISMATCH per PC-3) — source patch-id `c62fc08b…`, staged `5ca2755b…`; decomposition shows the only deltas are the entire `configure_config.cmake` `option()` block (`81a81,115`, the deliberate PC-1 divergence) and one structural blank-line shift in `config.h.in` (upstream removed the old `.incbin`/`SOURCE_DIR` trailer). No C++ guard differs from source.
- Tier 3 (build + verification): **pass** — `ninja -C build clickhouse` exit 0 (CMake reconfigure confirmed); Step 5a config.h all 29 = 1; Step 5b smoke 6/6; 2 reference tests PASS.

## Evidence

Note: this was a **resumed dispatch**. A prior run had completed Steps 0–4 (preflight, drift, cherry-pick, PC-1 option block, patch-id) and left a near-full build running. I independently re-verified its staged state, waited for the build to finish, then completed Steps 5–9.

Cherry-pick (`tmp/patch-051/cherrypick.log`):
```
CONFLICT (content): Merge conflict in src/Common/config.h.in
CONFLICT (content): Merge conflict in src/TableFunctions/TableFunctionRemote.cpp
... (resolved by hand; 3 other files auto-merged)
```

Authoritative flag↔option coverage (re-run by me, added-lines only both sides):
```
COVERAGE-DIFF-EMPTY (PASS)   # config.h.in new flags = 29 ; cmake new options = 29
```
No conflict markers remain in any of the 6 source files. 16 `REGISTER_DICTIONARY_*` flags still present (045 block intact).

Decomposition (`tmp/patch-051/decomposition.log`, re-verified): only `81a81,115` (the `configure_config.cmake` option block) + `31d30 < +` (config.h.in structural blank line). No semantic C++ divergence. `TableFunctionRemote.cpp` adds exactly `#if REGISTER_REMOTE_FUNCTION` / `#endif` around the `"remote"` registration only (matches source).

Build (`tmp/patch-051/build-postpatch.log`):
```
[0/2] Re-checking globbed directories...
[1/2] Re-running CMake...          # reconfigure confirmed (configure_config.cmake edited)
...
[15540/15543] Linking CXX executable programs/clickhouse
ninja exit: 0
```

Step 5a — generated `build/includes/configs/config.h` (`tmp/patch-051/configh-generated.log`):
```
REGISTER_AZURE_BLOB_QUEUE_TABLE_ENGINE = 1 ... REGISTER_URL_FUNCTION = 1
ALL-29-ARE-1 (PASS)
```

Step 5b — runtime smoke (`smoke-functions.log` / `smoke-engines.log`):
```
SELECT count() FROM system.table_functions WHERE name IN ('s3','url','file','remote','executable','redis')  -> 6
SELECT count() FROM system.table_engines   WHERE name IN ('Log','StripeLog','S3','URL','File','KeeperMap')   -> 6
```

Reference tests post-patch (`tmp/patch-051/upstream-ref-tests-pass.log`):
```
00832_storage_file_lock:   [ OK ] 0.13 sec.
00288_empty_stripelog:     [ OK ] 0.13 sec.
2 tests passed.
```
(`00004_shard_format_ast_and_remote_table` was also tried; it reached `TableFunctionRemote::executeImpl` — proving `remote()` is registered — and failed only on the single-node smoke env not listening on the `127.0.0.2/3` loopback aliases, i.e. a network/env artifact, not the patch.)

Final staged set (`git diff --cached --name-only`, 7 entries, no `tests/queries`):
```
docs/aiven/patches/051-disable-table-engines-and-functions.md
src/Common/config.h.in
src/Storages/ObjectStorage/registerStorageObjectStorage.cpp
src/Storages/registerStorages.cpp
src/TableFunctions/TableFunctionRemote.cpp
src/TableFunctions/registerTableFunctions.cpp
src/configure_config.cmake
```

## What I did

- Read the dispatch prompt, `docs/aiven/AGENTS.md`, the halt-and-escalate schema, the build-and-test runbook, the C++ review checklist, the dossier template, and the committed 045 pattern (`git show 0d6ef43e590`) + dossier.
- Step 0 preflight: confirmed HEAD = `v26.3.10.62-lts-aiven-dev`, `4641e51fdc` is a commit. Discovered the source files were **already staged** by a prior run of this dispatch (cherry-pick + PC-1 option block done), with a near-full build in progress (started 16:21).
- Did NOT re-cherry-pick (would clobber resolved conflicts). Instead independently re-verified the staged state: flag↔option coverage (29==29 empty), Step 3 decomposition (only the cmake addition + structural drift), 16 dictionary flags preserved, no conflict markers, and the `#if REGISTER_REMOTE_FUNCTION` guard matches source.
- Waited for the in-flight build to complete green (`ninja exit: 0`); confirmed the CMake reconfigure happened.
- Step 5a: verified all 29 new `REGISTER_*` flags = 1 in the generated `config.h`. Step 5b: started the existing smoke server (granted-user + Keeper config from prior setup), ran the two catalog smoke queries (6/6), then ran two representative existing tests (both PASS), and stopped the server.
- Wrote and staged the dossier `docs/aiven/patches/051-disable-table-engines-and-functions.md` (all sections filled). Verified the final staged set is exactly 7 files.
- No `git commit`/`push`/`rebase`/`amend`; no branch ops; only the prior `git cherry-pick --no-commit` + `git add` (dossier). Touched only the cherry-pick's 5 files + `configure_config.cmake` + the dossier (PC-4).

## Proposed next step

Ready for human commit. Suggested:

```
git commit -F tmp/patch-051/commit-message.txt
```

The commit message body (above) includes the required `Original author: Tilman Moeller <tilman.moeller@aiven.io>, 2026-01-08.` line — do not use `git commit --author=` or `-c CHERRY_PICK_HEAD`. The PC-1 `option(... ON)` divergence is intentional and load-bearing (`byte_equivalent: false`); the flag↔option coverage diff is empty and the generated `config.h` shows all 29 flags = 1, so no engine/function is silently disabled. (You may want to write the message body to `tmp/patch-051/commit-message.txt` before committing, since I do not commit.)

---

**Today you learned**
- A `#cmakedefine01 FOO` of an *undefined* CMake symbol expands to `#define FOO 0` — so a flag without a backing `option(FOO ... ON)` silently compiles the feature *out*; the `option()` (not `set(...1)`) form is precisely what lets a cached `-DFOO=…` win, preserving the RPM harness's selection.
- A "FAIL" can still be confirming evidence: `00004`'s stack trace reaching `TableFunctionRemote::executeImpl` proved `remote()` is registered even though the test failed on a network/env detail.

**Rule of thumb**
- On a resumed dispatch, re-verify the prior staged state from ground truth (coverage diff, decomposition, conflict-marker scan) before trusting it — never blindly re-run the cherry-pick over already-staged work.

**Next rabbit hole**
- The family-wide consumer-contract risk: a `-DREGISTER_<NAME>=…` whose flag isn't defined yet (052/071/075 not ported) is silently ignored by CMake — worth a spec/CI assertion that no `REGISTER_*` shows up in the "unused variables" warning.

# Patch 061 — clickhouse-dictionary-source-changes

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | (unknown — to be researched at 25.X dossier merge) | (unknown) | (unknown) |
| 25.8-aiven | `285e6fde0fec610af38798561b0c852f4d2fc873` | Tilman Moeller (author), Joe Lynch (committer) | (the version we're porting FROM) |
| 26.3-aiven | `patch-port(061)` | T3 worker | `byte-equivalent: false` — `still-needed-and-applies` (clean apply of 3 files + 1-line manual insert; only Allman normalization reshapes the diff) |

The 26.3-aiven carry is its `patch-port(061)` commit (find it with `git log --grep '^patch-port(061)'`); per `commit-hygiene.md` §4 the `-aiven-dev` branch is resliced at each LTS transition, so we pin the stable subject handle, not a SHA that would re-hash.

## 1. Purpose

Quoted source-commit body (verbatim):

```
Changes for ClickHouse dictionary source, both remote and local.

Remote:
* Allow use of addresses_expr in named collection configuration.
* Enforce `secure` parameter for ClickHouse dictionary source when not
  using named collections.

Local:
* New server setting `dictionary_user` to specify which user
  to run the dictionary query, rather than just the default user
  (since we don't have a default user)

This commit was applied from the patch file 0089-Dictionary-source-ClickHouse.patch

Co-authored-by: Joe Lynch <joe.lynch@aiven.io>
```

The "why" (durable): Aiven's managed fleet does not have a `default` user (the control plane removes it), and ClickHouse dictionaries with a `CLICKHOUSE` source historically ran as `default`. This patch makes the `CLICKHOUSE` dictionary source fit the Aiven model along three axes:

- **Remote `addresses_expr` (leg A):** lets a named collection describe its target(s) with an `addresses_expr` (the same multi-address expression syntax the `remote`/external-database integrations use), parsed via `parseRemoteDescriptionForExternalDatabase` and capped by `glob_expansion_max_elements`. The source then builds a `ConnectionPoolWithFailover` over all resulting addresses. It also rejects an address set that mixes the local instance with remote instances. `TableFunctionRemote` gains a no-op `secure` allowed key so a single named collection is shareable between the `remote` table function and the dictionary source.
- **Enforce `secure` for remote (leg B):** a non-local `CLICKHOUSE` dictionary source must use a secure connection; the source throws `BAD_ARGUMENTS` ("supports only secure connections") at registration if `secure` is not set. This protects credentials that would otherwise traverse the network in clear text.
- **Local `dictionary_user` (leg C):** a new server setting `dictionary_user` (default `default`) names which user a *local* dictionary query runs as. The local path now requires the configured `user` to equal `dictionary_user`, looks the user up via `getAccessControl().find<User>`, and runs through `Context::createCopy` + `setUser` instead of `Session::authenticate`. Motivated by Aiven removing the `default` user.

Source SHA on `v25.8.18.1-lts-aiven`: `285e6fde0fec610af38798561b0c852f4d2fc873` (inventory row 061).
Original author: Tilman Moeller <tilman.moeller@aiven.io> (2026-01-21).
Source committer (on the v25.8 branch): Joe Lynch <joelynch112@gmail.com>.

## 2. Upstream-drift findings

### Commands run

```bash
git show 285e6fde0fec610af38798561b0c852f4d2fc873 -- <4 files>     # source diff
git diff --cached -- <4 files>                                      # staged carry
git log v25.8.18.1-lts..v26.3.10.62-lts -- \
    src/Dictionaries/ClickHouseDictionarySource.cpp \
    src/Dictionaries/ClickHouseDictionarySource.h \
    src/Core/ServerSettings.cpp src/TableFunctions/TableFunctionRemote.cpp
git grep -n "addresses_expr" -- src/Storages/NamedCollectionsHelpers.h
git grep -n "parseRemoteDescriptionForExternalDatabase" -- src/Common/parseRemoteDescription.h
git grep -n "dictionary_user" -- src/Core/ServerSettings.cpp        # absent on HEAD
```

### Findings

- **Apply-check** (from the parent preflight, re-confirmed): `ClickHouseDictionarySource.cpp`,
  `ClickHouseDictionarySource.h`, and `TableFunctionRemote.cpp` apply **cleanly**; only
  `ServerSettings.cpp` conflicts on pure context drift. The carry hand-inserted the single
  `DECLARE(String, dictionary_user, "default", "Which user to use for dictionary queries.", 0)`
  line immediately before the `storage_metadata_write_full_object_key` `DECLARE` (which upstream
  changed to default `true` + `SettingsTierType::OBSOLETE`; that neighbor is left exactly as on HEAD).
- **Upstream changes to touched files** between prior and current LTS:
  - `src/Dictionaries/ClickHouseDictionarySource.cpp`/`.h`: HEAD has **no** `addresses`/
    `addresses_expr`/`dictionary_user`; the registration fn, the local `Session::authenticate`
    path, and the remote `checkHostAndPort` call all match the patch pre-image (hence the clean apply).
  - `src/Core/ServerSettings.cpp`: context-only drift; `dictionary_user` is **not** a server setting on HEAD.
  - `src/TableFunctions/TableFunctionRemote.cpp`: the allowed-keys list matches the pre-image.
- **Behavior-relevant drift (important — corrects a parent-preflight assumption):** upstream's
  `ExternalDatabaseEqualKeysSet` in `src/Storages/NamedCollectionsHelpers.h` **already lists**
  `addresses_expr` as a key equivalent to `host`/`hostname`
  (`std::pair{"addresses_expr", "host"}`, `std::pair{"addresses_expr", "hostname"}`). Consequently,
  `validateNamedCollection` **accepts** `addresses_expr` even pre-patch — the parent preflight's
  expected pre-patch failure (`BAD_ARGUMENTS` "unexpected key `addresses_expr`") does **not** occur
  on this base. Pre-patch the key is accepted but **never read**: the `CLICKHOUSE` dictionary source
  still resolves `host`/`port` (defaulting to `localhost:<port>`) and silently ignores
  `addresses_expr`. This drove the leg A test redesign (see §4).
- **Dependencies present on HEAD** (all resolve): `parseRemoteDescriptionForExternalDatabase`
  (`src/Common/parseRemoteDescription.h`), `getAccessControl().find<User>`,
  `ConnectionPoolWithFailover`, `isLocalAddress`, `Context::createCopy`/`setUser`.
- **Conclusion**: `still-needed-and-applies`. Aiven-specific behavior not present upstream; the
  three clean files apply and the one `ServerSettings.cpp` line inserts cleanly. The functional
  content equals the source; the only diff reshaping is Allman-brace normalization of the source's
  few Egyptian braces + one trailing-whitespace cleanup (`byte_equivalent: false`, but the
  added/removed lines are identical modulo brace placement — see the tier-2 note in the report).

## 3. C++ review

Per `docs/aiven/skills/cpp-review-checklist.md`. One bullet per section.

- **1 Lifetime + ownership**: `✓ — Configuration::addresses is a const std::vector<std::pair<String,UInt16>> held by value on the struct; the failover pool holds shared_ptr<ConnectionPool>. The local path takes user_id by value (std::optional<UUID>) and runs through Context::createCopy + setUser; named_collection stays alive across the factory lambda. The structured-binding loops bind references into the live configuration->addresses. No raw-owner or dangling reference introduced.`
- **2 Exception safety**: `✓ — every new throw (empty-addresses, mixed local/remote, secure-enforcement, user-mismatch, user-not-found) fires before any ConnectionPool or query Context is created/used; the optional<Configuration> is emplaced on the success path only. No partial state leaks.`
- **3 Thread-safety + concurrency**: `✓ — the factory lambda runs at dictionary load; it reads global_context server/settings snapshots and creates a per-dictionary Context copy. No new shared mutable state; the dictGet hot path is untouched.`
- **4 Performance + memory**: `✓ — addresses_expr is parsed once per dictionary load (rare); the failover pool replaces a single-pool emplace with a loop over addresses. dictGet and steady-state querying are unchanged.`
- **5 Settings as public API**: `New server setting dictionary_user (String, default "default"). The name is public API; default preserves the upstream-equivalent shape (run as the configured user) while letting Aiven point it at a non-default user. No setting is removed or renamed.`
- **6 Error handling**: `✓ — all new failures use BAD_ARGUMENTS with specific, user-facing messages ("supports only secure connections...", "Either all addresses should be the local ClickHouse instance or all should be remote ClickHouse instances", "...does not match the user specified by the setting dictionary_user", "...specified by setting dictionary_user not found"). The messages distinguish the Aiven gates from generic BAD_ARGUMENTS (AGENTS §7).`
- **7 Upstream / vendored code**: `✓ — all four files are upstream-owned src/ files carried as a documented Aiven patch. TableFunctionRemote only adds a no-op allowed key (secure), leaving its parsing behavior otherwise unchanged.`
- **8 Behavior under settings**: see §blast-radius — legs B and C are parity-preserving behavior carries; no new default-disabled gate is added (matches the 25.8 production shape).

### Blast-radius — parity-preserving behavior carries (clause (v))

Legs B and C are behavior changes versus stock upstream 26.3:

- **Leg B** rejects a previously-valid *non-secure remote* `CLICKHOUSE` dictionary source
  (`BAD_ARGUMENTS` at registration).
- **Leg C** changes how a *local* dictionary authenticates (`setUser` of `dictionary_user`
  instead of `Session::authenticate`) and can throw when the configured user does not match
  `dictionary_user` or when `dictionary_user` is not found.

These are **not** new default-behavior gate decisions for the Aiven fleet: both already ship in
Aiven 25.8 production and align with the control-plane / no-`default`-user model, so the carry
**preserves parity** with the previous LTS rather than introducing a new behavior change. The user
ratified these as parity-preserving carries, so **no new default-disabled server setting is added**
(this is not a clause-(v) `policy_call`). This mirrors the precedent recorded for **patch 036**
(`docs/aiven/patches/036-postgresql-dictionary-named-collection.md` §blast-radius), where the
analogous PostgreSQL-dictionary enforcement was carried as a parity-preserving breaking change with
no setting gate. Recorded here for the audit trail.

## 4. Test design

Option (a) — new tests that fail on the parent commit and pass after the patch.

Legs A and B are covered by **stateless** tests; leg C is covered by an **integration** test
(`tests/integration/test_aiven_dictionary_user/`). All three are staged in this checkout
(legs A/B authored in the main checkout; leg C built/validated in an isolated worktree to avoid
a build race, then delivered into the main checkout).

- **Test paths (legs A, B):**
  - `tests/queries/0_stateless/9061_clickhouse_dict_addresses_expr.{sh,reference}` (leg A)
  - `tests/queries/0_stateless/9061_clickhouse_dict_enforce_secure.{sh,reference}` (leg B)

### Leg A — `9061_clickhouse_dict_addresses_expr`

The test exercises the `addresses_expr` **parsing** path via the mixed local/remote rejection,
which exists only post-patch. A named collection sets
`addresses_expr = '127.0.0.1:<tcp>|192.0.2.1:9000'` (the `|` replica separator that
`parseRemoteDescriptionForExternalDatabase` understands; `192.0.2.1` is RFC 5737 TEST-NET-1, a
non-local, non-routable address). The dictionary names that collection.

- **Pre-patch run output (the FAIL)** — `addresses_expr` is accepted by `validateNamedCollection`
  (upstream already lists it in `ExternalDatabaseEqualKeysSet`) but is never read; the source falls
  back to `host=localhost` and loads, so the rejection message never appears:

  ```text
  9061_clickhouse_dict_addresses_expr:                                    [ FAIL ]
  Reason: result differs with reference:
  @@ -1 +1 @@
  -addresses_expr_mixed_rejected
  +addresses_expr_mixed_not_rejected
  ```

- **Post-patch run output (the PASS)** — `addresses_expr` is parsed into two addresses; the local
  (`127.0.0.1`) + remote (`192.0.2.1`) mix is rejected at registration with
  `BAD_ARGUMENTS` "Either all addresses should be the local ClickHouse instance or all should be
  remote ClickHouse instances", before any connection:

  ```text
  9061_clickhouse_dict_addresses_expr:                                    [ OK ]
  ```

- **Causation/redesign note:** the original leg A framing (assert `addresses_expr` is *accepted* and
  the dictionary loads) does **not** distinguish pre/post on this base, because upstream already
  accepts the key and a single-address expression pointing at the local instance loads identically
  pre- and post-patch (the value is ignored pre-patch). The mixed-rejection assertion is the minimal
  observable that proves `addresses_expr` is actually consumed and parsed — and its message is
  Aiven-specific (upstream has no such check), satisfying AGENTS §7.

### Leg B — `9061_clickhouse_dict_enforce_secure`

A dictionary with a clearly-remote, non-routable host and `secure 0`
(`SOURCE(CLICKHOUSE(host '192.0.2.1' port 9000 secure 0 db 'system' table 'one'))`).

- **Pre-patch run output (the FAIL)** — no enforcement; the loader proceeds to connect to the
  non-routable host and fails with a connection error that does **not** contain the secure message:

  ```text
  9061_clickhouse_dict_enforce_secure:                                    [ FAIL ]
  Reason: result differs with reference:
  @@ -1 +1 @@
  -secure_enforced
  +secure_not_enforced
  ```

- **Post-patch run output (the PASS)** — registration throws `BAD_ARGUMENTS` "supports only secure
  connections" **before** any connection attempt (deterministic, no network round-trip):

  ```text
  9061_clickhouse_dict_enforce_secure:                                    [ OK ]
  ```

- **Why this distinguishes the Aiven gate** (AGENTS §7): the test asserts the message substring
  "supports only secure connections", emitted only by this Aiven check; upstream has no such gate
  and would attempt to connect.

### Leg C — `dictionary_user` (integration)

- **Test paths:** `tests/integration/test_aiven_dictionary_user/{__init__.py,test.py,configs/dictionary_user.xml}` (staged).
- **Evidence-of-causation pair (verified):** the server config sets `<dictionary_user>some_user</dictionary_user>`.
  - `test_local_dict_user_mismatch_is_rejected`: a *local* CH dict source naming `user 'default'` →
    **pre-patch** loads (no `dictionary_user` gate; `Session::authenticate` path) → test FAILS;
    **post-patch** `SYSTEM RELOAD DICTIONARY` throws `Code: 36 BAD_ARGUMENTS` "does not match the
    user specified by the setting dictionary_user (some_user)" → test PASSES. Asserts code + the
    Aiven-specific message substring (§7).
  - `test_local_dict_matching_user_loads` (positive control): the matching user loads in both
    binaries, confirming the gate accepts the matching user rather than rejecting all local sources.
- **Note:** the `not found` branch (a non-existent `dictionary_user`) also errors pre-patch but via
  `Session::authenticate` (`AUTHENTICATION_FAILED`), so its causal signal is message-substring-only;
  not separately covered here.

- **Why integration, not stateless:** leg C requires a server-level `<dictionary_user>` config and
  a created user, and the mismatch/not-found throws fire at dictionary registration for a *local*
  source — a scenario most naturally exercised in the integration harness.

## 5. Rollback considerations

- **Revert safety**: yes. The patch changes how a `CLICKHOUSE` dictionary source is configured,
  validated, and (for local sources) authenticated. No schema migration, no on-disk format change,
  no new persisted metadata.
- **State surviving restart**: none. No new files, ZK nodes, or caches are introduced. The
  `dictionary_user` server setting is read at dictionary load only.
- **Re-enabling pre-patch behavior on a running server**: leg B (secure enforcement) and leg A
  (mixed-address rejection) have no disabling setting — they are intentional and match the 25.8
  production shape. For leg C, `dictionary_user` defaults to `default`; on a deployment that still
  has a `default` user this preserves the prior local-dictionary behavior shape.

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — to be researched at the 25.X dossier merge. Predates the dossier system.

### 25.8-aiven (historical, may be empty)

The version we are porting FROM. Source SHA `285e6fde0fec610af38798561b0c852f4d2fc873`.
Author Tilman Moeller; committer Joe Lynch. Co-author: Joe Lynch <joe.lynch@aiven.io>.
Author date 2026-01-21.

### 26.3-aiven (this uplift)

- **Cherry-pick was**: `still-needed-and-applies` (clean apply of the three dict/table-function
  files + a one-line manual insert in `ServerSettings.cpp`). The only diff reshaping is Allman-brace
  normalization of the source's few Egyptian braces plus one trailing-whitespace cleanup.
- **Upstream-drift conclusion**: see §2 — `still-needed-and-applies`; notable nuance: upstream
  already lists `addresses_expr` in `ExternalDatabaseEqualKeysSet`, which changed the leg A test
  design (see §4).
- **Test added at**: `tests/queries/0_stateless/9061_clickhouse_dict_addresses_expr.{sh,reference}`
  (leg A) and `tests/queries/0_stateless/9061_clickhouse_dict_enforce_secure.{sh,reference}`
  (leg B). Leg C integration test at `tests/integration/test_aiven_dictionary_user/` (see §4).
- **Time-to-port**: this is the FINISH tail (legs A+B evidence + dossier) of an already-applied
  port. Build-cache state: **warm-cache** incremental rebuilds via the in-place worktree-flip —
  post-patch verify ~54 s; pre-patch flip ~96 s; post-patch restore ~100 s and ~338 s (each a
  recompile of `ClickHouseDictionarySource.cpp` + `ServerSettings.cpp` and a relink). The `build/`
  dir needed a `cmake --fresh` recovery at the start (missing `CMakeFiles/rules.ninja`,
  runbook §6 row 1).
- **Anything surprising**: the parent-preflight expectation that pre-patch rejects `addresses_expr`
  was inaccurate (upstream already accepts the key), so leg A was redesigned to assert the
  mixed local/remote rejection — the minimal observable that proves the new parsing path runs.
```

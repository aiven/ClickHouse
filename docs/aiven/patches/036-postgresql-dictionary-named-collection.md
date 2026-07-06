# Patch 036 — postgresql-dictionary-named-collection

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | (unknown — to be researched at 25.X dossier merge) | (unknown) | (unknown) |
| 25.8-aiven | `d293fbe109d676fd5312dd879d2c486962b9b8e6` | Tilman Moeller (author), Joe Lynch (committer) | (the version we're porting FROM) |
| 26.3-aiven | `patch-port(036)` | T3 worker | `byte-equivalent: false` — `still-needed-but-rewrite` (dict file hand-ported around upstream drift) |

The 26.3-aiven carry is its `patch-port(036)` commit (find it with `git log --grep '^patch-port(036)'`); per `commit-hygiene.md` §4 the `-aiven-dev` branch is resliced at each LTS transition, so we pin the stable subject handle, not a SHA that would re-hash.

## 1. Purpose

Quoted source-commit body (verbatim):

```
Multiple changes in PostgreSQL dictionary

* Allow using DDL created named collection for PostgreSQL dictionary.
  Currently this fails upstream.
* Ensure that TLS works.
* Enforce using named collection.

This patch simplifies the PostgreSQL dictionary source implementation by
removing support for config file-based configuration and enforcing the use
of named collections. This improves consistency, security, and enables
proper TLS/SSL support for dictionary sources.

Changes:
- Removed validateConfigKeys() function and all config file parsing logic
- Enforced named collection requirement (throws UNSUPPORTED_METHOD if not provided)
- Simplified dictionary registration to use StoragePostgreSQL::processNamedCollectionResult()
- Added overloaded processNamedCollectionResult() method that accepts additional_allowed_args
- Changed pool creation from replicas_by_priority to single common_configuration
- Removed replica support from config files (only works with named collections now)

Breaking change: Users using config file-based PostgreSQL dictionary configuration
must migrate to named collections. The old configuration method is no longer supported.

Co-authored-by: Joe Lynch <joe.lynch@aiven.io>
```

The "why" (durable): Aiven's managed fleet configures PostgreSQL dictionary sources exclusively
through **named collections** provisioned by the control plane. Two problems with the upstream
shape motivated this patch: (1) a DDL-created named collection (`CREATE NAMED COLLECTION ... ;
CREATE DICTIONARY ... SOURCE(POSTGRESQL(NAME <coll>))`) fails upstream because the dictionary's
own narrow key-validation (`dictionary_allowed_keys`) does not accept the keys the rest of the
PostgreSQL integration accepts (in particular the TLS keys `ssl_mode` / `ssl_root_cert` and the
`addresses_expr` form); and (2) TLS could not be expressed for dictionary sources through the
named collection at all. The patch routes the dictionary through the **shared** parser
`StoragePostgreSQL::processNamedCollectionResult`, so the dictionary inherits the same allowed-key
set, address parsing, and TLS handling as the table engine. It also **enforces** named collections
by deleting the config-file/XML branch and throwing `UNSUPPORTED_METHOD` when no named collection
is present — matching what Aiven's 25.8 production already ships.

Source SHA on `v25.8.18.1-lts-aiven`: `d293fbe109d676fd5312dd879d2c486962b9b8e6` (inventory row 036).
Original author: Tilman Moeller <tilman.moeller@aiven.io> (2026-01-02).
Source committer (on the v25.8 branch): Joe Lynch <joelynch112@gmail.com>.

## 2. Upstream-drift findings

### Commands run

```bash
git apply --check --3way tmp/036-source.diff           # which files apply cleanly
git grep -c <id> -- src/                                # identifier inventory on HEAD
git merge-base --is-ancestor a485331144e HEAD           # is the sslmode band-aid present
git grep -n "M(1, UNSUPPORTED_METHOD)" -- src/Common/ErrorCodes.cpp
sed -n '38,60p' src/Core/PostgreSQL/PoolWithFailover.h  # single-Configuration ctor present
```

### Findings

- **Apply-check**: `src/Storages/StoragePostgreSQL.cpp` and `src/Storages/StoragePostgreSQL.h`
  apply **cleanly** from `tmp/036-source.diff`. `src/Dictionaries/PostgreSQLDictionarySource.cpp`
  applies **WITH CONFLICTS**.
- **Cause of the dict-file drift** (two unrelated upstream commits, both ancestors of HEAD):
  - `340be5a4968` ("implicit-int-conversion"): the register branch now casts the port via
    `static_cast<UInt16>(...)`. Those lines are inside the block the patch deletes wholesale, so
    the drift is purely textual — once the block is replaced by the shared-parser path the casts
    disappear with it.
  - `a485331144e` ("Add 'sslmode' to allowed keys in PostgreSQL dictionary"): adds `sslmode`
    (no underscore) to the dictionary's `dictionary_allowed_keys`. This is a partial band-aid that
    does NOT enforce named collections and does NOT route DDL/TLS through the shared parser, so it
    does NOT obsolete this patch. `git merge-base --is-ancestor a485331144e HEAD` → present.
- **Identifier inventory on HEAD** (all PRESENT): `processNamedCollectionResult` (old 3-arg form
  at `StoragePostgreSQL.cpp:559`), `dictionary_allowed_keys`, `tryGetNamedCollectionWithOverrides`,
  `ExternalDatabaseEqualKeysSet`, `getRemoteHostFilter().checkHostAndPort`, `background_reconnect`.
  `UNSUPPORTED_METHOD` is a global error code (`M(1, UNSUPPORTED_METHOD)` in `ErrorCodes.cpp`), so
  the `extern const int UNSUPPORTED_METHOD;` declaration resolves. The single-`Configuration`
  `PoolWithFailover` ctor exists at `src/Core/PostgreSQL/PoolWithFailover.h:43`. `Configuration`'s
  `addresses` member is `std::vector<std::pair<String, UInt16>>`, so the
  `for (auto & [host, port] : addresses) checkHostAndPort(host, toString(port))` loop compiles.
- **Conclusion**: `still-needed-but-rewrite`. The two storage files were applied from the source
  diff; the dictionary file was hand-ported around the two drift commits while preserving HEAD
  style (the `#if USE_LIBPQXX` guards and the lambda's `-> DictionarySourcePtr` signature). The
  functional content equals the source.

## 3. C++ review

Per `docs/aiven/skills/cpp-review-checklist.md`. One bullet per section.

- **1 Lifetime + ownership**: `✓ — common_configuration is a value built by the shared parser; the pool is a shared_ptr; named_collection is a shared_ptr kept alive across the lambda. No raw-owner or dangling-reference introduced. The structured-binding loop over addresses binds to references into the live common_configuration.`
- **2 Exception safety**: `✓ — UNSUPPORTED_METHOD and BAD_ARGUMENTS (from validateNamedCollection) are thrown before any pool/connection is created; the optional<Configuration> is emplaced only on the success path. No partial state leaks.`
- **3 Thread-safety + concurrency**: `✓ — the factory lambda runs at dictionary load; no new shared mutable state. background_reconnect routes to the existing ReplicasReconnector path unchanged.`
- **4 Performance + memory**: `✓ — the new overload copies the optional-keys set once per dictionary load to union additional_allowed_args. Dictionary load is rare; negligible. The hot dictGet path is unchanged.`
- **5 Settings as public API**: `n/a — no new setting. The dictionary's TLS continues to read postgresql_connection_pool_ssl_mode / postgresql_connection_pool_ssl_root_cert settings for the pool; the named-collection ssl keys are accepted (and stored on the Configuration) so DDL configuration validates.`
- **6 Error handling**: `✓ — missing named collection throws UNSUPPORTED_METHOD with a specific message; unexpected keys throw BAD_ARGUMENTS via validateNamedCollection. Both are user-facing and specific.`
- **7 Upstream / vendored code**: `✓ — all three files are upstream-owned src/ files; the patch is a documented Aiven carry. processNamedCollectionResult's 3-arg form is preserved (delegates to the new 4-arg overload), so other callers (the table engine) are unaffected.`
- **8 Behavior under settings**: see §blast-radius below — removing the config-file path is a breaking change in absolute terms but a parity-preserving carry for the Aiven fleet; no setting gate is added (matches the 25.8 production shape).

### Blast-radius — parity-preserving breaking change (clause (v))

Removing config-file/XML-configured PostgreSQL dictionaries is a breaking change in absolute terms:
an XML-configured PG dictionary that loaded on stock 26.3 now throws `UNSUPPORTED_METHOD`. This is
**not** a new default-behavior gate decision for the Aiven fleet, however: Aiven's 25.8 production
**already** ships this enforcement, and the control plane configures PostgreSQL exclusively via
named collections, so the config-file path is **unused in production**. The carry preserves parity
with the previous LTS rather than introducing a new behavior change. No default-disabled server
setting is required (this is not a clause-(v) `policy_call`); the change is recorded here for the
audit trail.

## 4. Test design

Option (a) — new integration test that fails on the parent commit and passes after the patch.

- **Test paths** (Aiven integration convention `test_aiven_<slug>/`, per
  `docs/aiven/runbooks/integration-tests.md` §6):
  - `tests/integration/test_aiven_postgres_dict_named_collection/__init__.py`
  - `tests/integration/test_aiven_postgres_dict_named_collection/configs/dictionaries/postgres_xml_dict.xml`
  - `tests/integration/test_aiven_postgres_dict_named_collection/test.py`
  - The XML dictionary is mounted via the harness `dictionaries=` parameter (it lands in the
    instance's `/etc/clickhouse-server/dictionaries/` dir, loaded by the
    `<dictionaries_config>.../dictionaries/*.xml</dictionaries_config>` glob the harness injects).
    Mounting via `main_configs=` (`config.d/`) does NOT register the dictionary, so
    `SYSTEM RELOAD DICTIONARY` would return `Dictionary not found` and never reach the source factory.
- **Why integration, not stateless**: the behaviour needs a real PostgreSQL server (the dictionary
  must actually load against PG to make the pre/post differential causal rather than a missing-table
  artifact), and the enforcement leg needs a server config-file dictionary (`created_from_ddl == false`),
  which a stateless `.sql`/`.sh` runner cannot mount.
- **Two legs**, each with its own evidence-of-causation pair (worktree-flip; see §6):

  1. **Enforcement leg — `test_xml_config_dictionary_is_rejected`.** A PostgreSQL dictionary defined
     in a server config file (no named collection) is reloaded with `SYSTEM RELOAD DICTIONARY`. The
     backing PG table `xml_enforced_table` is created and filled, so the PRE-patch binary loads it
     successfully against the real PG. POST-patch the reload throws `UNSUPPORTED_METHOD` with the
     message `PostgreSQL dictionary source configuration must use a named collection`. The test
     asserts BOTH the error code (`UNSUPPORTED_METHOD`) AND the Aiven-specific message substring
     (AGENTS §7): `UNSUPPORTED_METHOD` alone is a generic upstream code, so the message is what
     distinguishes the Aiven gate.

  2. **DDL named-collection + TLS leg — `test_ddl_named_collection_with_tls_key_loads`.** A
     DDL-created named collection carries `ssl_root_cert` — a key the shared
     `processNamedCollectionResult` parser accepts (it is in the parser's optional-keys set) but the
     dictionary's pre-patch `dictionary_allowed_keys` set rejects (HEAD's set has `sslmode`, NOT
     `ssl_root_cert`). A `CREATE DICTIONARY ... SOURCE(POSTGRESQL(NAME <coll>))` over that collection
     is loaded. POST-patch it validates and loads rows from the real PG. PRE-patch it throws
     `BAD_ARGUMENTS` ("Unexpected key `ssl_root_cert`") at load time, before any connection.
     `ssl_root_cert` is set **without** `ssl_mode` on purpose: with `configuration.ssl_mode` unset,
     `PoolWithFailover`'s `get_ssl_context` falls back to the server-level SSL settings (non-SSL by
     default), so the load succeeds against the plain test PostgreSQL while still exercising
     TLS-key acceptance.

- **Clause (iv) reachability**: named-collection KEY VALIDATION (`validateNamedCollection`) and the
  enforcement check both run at dictionary load, BEFORE the PG connect — so the pre/post differential
  is observable independent of connectivity.

### Pre-patch run output (the FAIL)

Built via the worktree-flip (`git restore --worktree --source=HEAD` on the 3 patched files;
index kept staged) + incremental rebuild. Both legs FAIL for the expected reasons
(`tmp/patch-036/test-prepatch.log`):

```text
======================== 2 failed, 4 warnings in 16.35s ========================

# Enforcement leg — the XML config-file dictionary LOADS successfully pre-patch,
# so query_and_get_error raises because the reload did not fail:
test_xml_config_dictionary_is_rejected:
  helpers.client.QueryRuntimeException: Client expected to be failed but succeeded!
  (query: SYSTEM RELOAD DICTIONARY xml_pg_dict)

# DDL named-collection + TLS leg — ssl_root_cert is rejected by the narrow
# pre-patch dictionary_allowed_keys set:
test_ddl_named_collection_with_tls_key_loads:
  Code: 36. DB::Exception: Unexpected key `ssl_root_cert` in named collection.
  Required keys: , optional keys: background_reconnect, db, database, host,
  invalidate_query, name, password, port, priority, query, schema, sslmode, table,
  update_field, update_lag, user, where ... (BAD_ARGUMENTS)
  (in scope SELECT dictGetUInt32('pg_tls_dict', 'value', toUInt64(9)))
```

### Post-patch run output (the PASS)

From `tmp/patch-036/test-postpatch.log` (verified against the freshly-rebuilt post-patch binary):

```text
======================== 2 passed, 3 warnings in 5.36s =========================

# Enforcement leg — reload now throws the Aiven gate, asserting BOTH code and message:
enforcement-leg error: Code: 1. DB::Exception: PostgreSQL dictionary source
  configuration must use a named collection. ... (UNSUPPORTED_METHOD)
  (query: SYSTEM RELOAD DICTIONARY xml_pg_dict)

# DDL named-collection + TLS leg — ssl_root_cert is accepted (routed through the
# shared parser) and the dictionary loads rows from the real PostgreSQL:
Executing query SELECT dictGetUInt32('pg_tls_dict', 'value', toUInt64(9)) on node1  -> 9
```

**Why this distinguishes the Aiven gate from upstream behavior** (AGENTS §7): the enforcement leg
asserts the message `PostgreSQL dictionary source configuration must use a named collection`, which
is emitted only by this Aiven patch; upstream throws `UNSUPPORTED_METHOD` (a generic code) for
unrelated reasons but never with this message. The TLS leg keys on `ssl_root_cert`, which upstream's
dictionary rejects (`BAD_ARGUMENTS`) and the Aiven patch accepts — a key-acceptance differential
specific to routing through the shared parser.

## 5. Rollback considerations

- **Revert safety**: yes. The patch only changes how a PostgreSQL dictionary source is configured
  and validated; no schema migration, no on-disk format change, no new persisted metadata.
- **State surviving restart**: none. No new files, ZK nodes, or caches are introduced.
- **Re-enabling pre-patch behavior on a running server**: there is no setting to restore the
  config-file path (the enforcement is intentional and matches the 25.8 production shape). To run a
  PostgreSQL dictionary, define a named collection and reference it via
  `SOURCE(POSTGRESQL(NAME <coll>))`.

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — to be researched at the 25.X dossier merge. Predates the dossier system.

### 25.8-aiven (historical, may be empty)

The version we are porting FROM. Source SHA `d293fbe109d676fd5312dd879d2c486962b9b8e6`.
Author Tilman Moeller; committer Joe Lynch. Co-author: Joe Lynch. Author date 2026-01-02.

### 26.3-aiven (this uplift)

- **Cherry-pick was**: `still-needed-but-rewrite`. The two `StoragePostgreSQL.{cpp,h}` files were
  applied from `tmp/036-source.diff` (clean); `PostgreSQLDictionarySource.cpp` was hand-ported
  around two upstream drift commits (`340be5a4968` port `static_cast<UInt16>` casts inside the
  deleted block; `a485331144e` `sslmode` allowed-key addition, left untouched on HEAD). Minor style
  normalization was applied to the source diff's 3-arg `processNamedCollectionResult` body
  (brace-on-same-line + stray blank line → Allman) and the new `.h` overload declaration.
- **Upstream-drift conclusion**: see §2 — `still-needed-but-rewrite`; the `sslmode` band-aid does
  NOT obsolete the patch.
- **Test added at**: `tests/integration/test_aiven_postgres_dict_named_collection/`.
- **Time-to-port (subagent wall-clock)**: ~1 h 45 m (dispatch start `2026-06-08T09:03:19Z`),
  dominated by the cold full build. Build-cache state: **cold-cache** (the `build/` directory was
  half-broken — missing `CMakeFiles/rules.ninja` — so a `cmake --fresh` reconfigure was required,
  which combined with a cold sccache forced a full build: ~98 min). The two subsequent incremental
  rebuilds were warm-cache: pre-patch flip ~47 s (1540 targets), post-patch restore ~25 s
  (34 targets). Each of the three test runs (post-patch, pre-patch, post-patch) completed in
  ~5–16 s wall-clock with cached Docker images.
- **Anything surprising**: the `build/` dir needed `cmake --fresh` recovery (runbook §6 row 1); the
  cold sccache turned the expected incremental build into a full build.
```

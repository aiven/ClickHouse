# Patch 021 — external-db-ssl

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `934b35cc7d` | Tilman Moeller (author) / Aliaksei Khatskevich (committer), 2025-12-15; co-authored by Joe Lynch | (original carry — the version we are porting FROM) |
| 26.3-aiven (`.10.62`) | `patch-port(021)` (`58b8a259e4e`) | T3 worker, Dispatch 1 of 2 (decoupled code port), 2026-06-03 | `still-needed-but-rewrite` — conflict-resolved cherry-pick + submodule fork-redirect; see §2, §6 |
| 26.3-aiven (`.15.4` rebase) | replayed onto `v26.3.15.4-lts` | intra-LTS rebase, 2026-06-26 | **source patch only — MariaDB fork DROPPED (obsoleted-by-upstream).** Upstream `.15` bumped `contrib/mariadb-connector-c` to `95d6264bb43`, which already carries 021's sole connector delta (the `X509_check_host` strlen fix), so the Aiven fork is no longer needed: `.gitmodules` reverts to `url=ClickHouse/mariadb-connector-c` and the gitlink is pinned to `95d6264bb43`. A follow-up `fix(aiven/26.3): pin contrib/mariadb-connector-c to upstream .15` corrects a transient conflict-resolution mis-pin that briefly recorded the fork SHA `2914d3f`. The §2/§6/§7 fork-redirect notes below describe the `.62` state and are superseded for `.15`. |

The 26.3 row is committed as `58b8a259e4e`. This uplift is **decoupled**
(runbook §7.3): Dispatch 1 (this dossier) delivers the staged, building code port +
submodule redirect; Dispatch 2 layers the integration test on the already-proven
harness and the human commits once.

## 1. Purpose

Adds SSL/TLS configuration support for ClickHouse's **outbound** connections to a
user's own external PostgreSQL and MySQL servers, plus a security fix in the
MariaDB Connector/C fork. Without it, ClickHouse cannot require/verify TLS to those
external databases, leaving cross-network credentials and data exposed to
interception and man-in-the-middle attacks. The durable motivation: Aiven runs
managed ClickHouse that integrates with managed PG/MySQL over untrusted networks,
where encrypted + certificate-validated connections are mandatory.

Concretely the patch:

- Adds an `SSLMode` enum (`disable`/`allow`/`prefer`/`require`/`verify-ca`/`verify-full`)
  for PostgreSQL and a `MySQLSSLMode` subset (`disable`/`prefer`/`verify-full`) for MySQL.
- Adds two user-overridable settings: `postgresql_connection_pool_ssl_mode` (default
  `PREFER`) and `postgresql_connection_pool_ssl_root_cert` (default empty).
- Threads SSL mode + root cert through `postgres::PoolWithFailover` /
  `postgres::formatConnectionString` and the `mysqlxx` `Connection`/`Pool`/`PoolWithFailover`
  classes, and through every PG/MySQL integration point (Database/Storage/TableFunction/Dictionary).
- Repoints `contrib/mariadb-connector-c` to the Aiven fork that fixes the
  `X509_check_host` call to pass the hostname length (prevents a certificate
  hostname-validation bypass).

Source SHA on `v25.8.18.1-lts-aiven`: `934b35cc7d5397dad3965d118248a09ba5fafc2e`
(from `docs/aiven/uplifts/26.3/inventory.md` row 021).
Original author: Tilman Moeller <tilman.moeller@aiven.io>, 2025-12-15 (co-authored by Joe Lynch <joe.lynch@aiven.io>).

Default `PREFER` is preserved → backward-compatible (attempt SSL, fall back gracefully if unavailable).

## 2. Upstream-drift findings

> Mandatory section. Verify the patch is still SEMANTICALLY correct against `v26.3.10.62-lts`.

### Commands run

```bash
# new identifiers absent on HEAD (expect 0)
git grep -l 'SSLMode|MySQLSSLMode|postgresql_connection_pool_ssl_mode|postgresql_connection_pool_ssl_root_cert' -- 'src/*'
# existing anchors present (expect >0)
git grep -l 'formatConnectionString'         -- 'src/*'   # 5
git grep -l 'createMySQLPoolWithFailover'     -- 'src/*'   # 6
# connector macro lives in the submodule, not src/
git -C contrib/mariadb-connector-c grep -n MYSQL_OPT_SSL_VERIFY_SERVER_CERT 2914d3f -- include/
```

### Findings

- New identifiers (`SSLMode`, `MySQLSSLMode`, `postgresql_connection_pool_ssl_mode`,
  `postgresql_connection_pool_ssl_root_cert`): **0** on 26.3 HEAD — no upstream
  equivalent for PG/MySQL client SSL. The feature is genuinely missing upstream.
- Existing anchors present: `formatConnectionString` (5 files), `createMySQLPoolWithFailover`
  (6 files); the signatures the patch extends all exist on HEAD.
- `git apply --check` fails → cherry-pick is **apply-then-fix**. The 3-way merge
  auto-resolved 4 of the 5 predicted-conflict files (by line offset); only
  `src/Dictionaries/MySQLDictionarySource.cpp` produced a real conflict.
- `MYSQL_OPT_SSL_VERIFY_SERVER_CERT` (used in `mysqlxx/Connection.cpp` for
  `verify-full`) is **0 in `src/`** — it is a `contrib/mariadb-connector-c` macro.
  Verified present at the pinned fork: `2914d3f:include/mysql.h:188`. The build's
  `verify-full` path is therefore satisfied by the pin.
- **Conclusion: `still-needed-but-rewrite`** — semantics unchanged; the cherry-pick
  required the manual `MySQLDictionarySource` resolution + the submodule value
  adaptation described in §6.

## 3. C++ review

Per `docs/aiven/skills/cpp-review-checklist.md`. One bullet per section.

- **1 Lifetime + ownership**: ✓ — SSL params are passed by value (`MySQLSSLMode`,
  `SSLMode` enums) or `const std::string &` and copied into `Pool`/`PoolWithFailover`
  members; `StoragePostgreSQL::Configuration::ssl_mode` is `std::optional<SSLMode>`
  (value-owned). No new raw pointers; `mysqlxx::Pool`'s copy ctor was extended to
  copy `ssl_mode`.
- **2 Exception safety**: ✓ — the new `verify-full` branch in `Connection::connect`
  throws `ConnectionFailed` on `mysql_options` failure, identical to the pre-existing
  `mysql_ssl_set` failure path; no partial mutation escapes (connection not yet
  established).
- **3 Thread-safety + concurrency**: ✓ — SSL fields are immutable per-pool config set
  at construction; no new shared mutable state, no new lock, no sleep.
- **4 Performance + memory**: n/a — all changes are in connection setup / settings /
  named-collection parsing paths, not a per-row hot loop.
- **5 Settings as public API**: ✓ (policy P1) — `postgresql_connection_pool_ssl_mode`
  and `postgresql_connection_pool_ssl_root_cert` are `DECLARE`d in `src/Core/Settings.cpp`;
  `SSLMode` added to `COMMON_SETTINGS_SUPPORTED_TYPES` in `src/Core/Settings.h`
  (alphabetically, between `SQLSecurityType` and `StreamingHandleErrorMode`). Default
  `PREFER` preserves backward compat.
- **6 Error handling**: ✓ — enum parsing uses `BAD_ARGUMENTS` (real code) via
  `IMPLEMENT_SETTING_ENUM`; SSL setup failures reuse `ConnectionFailed`. No invented codes.
- **7 Upstream / vendored code**: ✓ (sanctioned exception, clause vi) — no file content
  inside `contrib/` was edited; only the `contrib/mariadb-connector-c` **gitlink pointer**
  + `.gitmodules` were repointed to the prepared Aiven fork (the explicit purpose of 021).
- **8 Behavior under settings**: ✓ — default `PREFER` attempts SSL and falls back;
  `disable` reproduces prior no-SSL behavior; `verify-full` additionally enables
  `MYSQL_OPT_SSL_VERIFY_SERVER_CERT` / `sslmode=verify-full`. Nothing observable changes
  for users who do not set the new keys.

## 4. Test design

**Completed in Dispatch 2 (decoupled per runbook §7.3).** The integration test is
`tests/integration/test_aiven_external_db_ssl/` (the proven TLS-required PG+MySQL
harness, extended with a ClickHouse node that drives the patch-021 SSL surface).
Files: `test.py`, `configs/named_collections.xml` (base `mysql_ssl` collection),
`certs/gen_certs.sh` (CA + leaf certs; PG SAN `postgres1`, MySQL SAN `mysql80`),
`__init__.py`.

### What it provisions

`cluster.start()` brings up PostgreSQL `postgres1` + MySQL `mysql80` + a ClickHouse
`node` (`main_configs=["configs/named_collections.xml"]`). The harness then
reconfigures both DBs at runtime to REQUIRE TLS with our CA-signed certs (PG:
`ssl=on` + hostssl-only `pg_hba`; MySQL: `require_secure_transport=ON` +
`ALTER INSTANCE RELOAD TLS` onto our cert). Probe tables `postgres.ssl_probe` and
`clickhouse.ssl_probe` (each one row, value `42`) are created over TLS, and
`certs/ca.crt` is copied into the node container at
`/etc/clickhouse-server/aiven_external_db_ca.crt`. The node reaches `postgres1` /
`mysql80` from inside the docker network, where those hostnames resolve, so
`verify-full` incl. hostname works directly.

### ClickHouse-side assertions (the patch surface)

| Test | Assertion |
|---|---|
| `test_ch_postgres_verify_full_succeeds` | `postgresql('postgres1:5432',…)` with `postgresql_connection_pool_ssl_mode='verify-full'` + `…_ssl_root_cert=<ca.crt>` returns `42` (CA + hostname `postgres1` validated). |
| `test_ch_postgres_ssl_disable_rejected` | same table function with `…_ssl_mode='disable'` is rejected by the hostssl-only `pg_hba` (`no pg_hba.conf entry … no encryption`). Proves the knob reaches libpq. |
| `test_ch_mysql_verify_full_succeeds` | `mysql(mysql_ssl, ssl_mode='verify-full', ssl_ca=<ca.crt>)` returns `42` (CA + hostname `mysql80` validated). |
| `test_ch_mysql_non_tls_rejected` | `mysql(mysql_ssl, ssl_mode='disable')` is rejected by `require_secure_transport=ON` (`ERROR 3159 … insecure transport are prohibited`). |
| `test_ch_mysql_verify_full_hostname_mismatch_rejected` | `mysql(mysql_ssl, host='<container-IP>', ssl_mode='verify-full', ssl_ca=<ca.crt>)` is rejected: the IP is NOT in the cert SAN (`mysql80`), so the connector's `X509_check_host` hostname check fails (`ERROR 2026 … SSL connection error: Validation of SSL server certificate failed`). Same CA/server as the success case — only the verified identity differs. |

The four direct-client harness self-checks (`test_postgres_*`, `test_mysql_*`) are
retained as cheap provisioning self-checks.

### The exact differential (evidence-of-causation pair, worktree-flip of the 26 `src/` files)

- **Post-patch (src == index): 10 passed.** `d2-test-postpatch.log`.
- **Pre-patch (src restored to HEAD, rebuilt): 5 failed / 5 passed.** `d2-test-prepatch.log`.
  All five ClickHouse-side tests fail on the SSL-feature-ABSENCE, not an incidental
  error:
  - PostgreSQL: `Code: 552 … Unrecognized option '--postgresql_connection_pool_ssl_mode' (UNRECOGNIZED_ARGUMENTS)` — the setting does not exist on HEAD.
  - MySQL: `Code: 36 … Unexpected key 'ssl_mode' in named collection … optional keys: … ssl_ca, ssl_cert, ssl_key (BAD_ARGUMENTS)` — no `ssl_mode`/`ssl_root_cert` key on HEAD.
  The "rejected"-style tests carry a `_assert_not_feature_absence` guard so they
  cannot pass pre-patch on the incidental `ssl` substring inside the unknown-key /
  unrecognized-option message (AGENTS.md §7: the test must distinguish the gate).
  Flip-back was unconditional; `git diff` over the 26 `src/` files is empty
  (worktree == index).

### Faithful-behavior note (MySQL `ssl_root_cert`)

For MySQL the CA is threaded through the pre-existing `ssl_ca` named-collection key
(→ `mysql_ssl_set`). The patch parses `ssl_root_cert` into
`StorageMySQL::Configuration::ssl_root_cert` but does NOT forward it to the
connector — it is inert on the MySQL path (only PostgreSQL forwards
`ssl_root_cert` to libpq as `sslrootcert` via `formatConnectionString`). The test
therefore supplies the MySQL CA via `ssl_ca`; the post-patch-only differential is
the `ssl_mode` key. This is faithful to the source patch, not a port defect.

### Known-coverage note (connector pre/post flip out of scope)

The evidence pair flips the **26 `src/` files only**; the `contrib/mariadb-connector-c`
`X509_check_host` fix (gitlink `2914d3f`) is **exercised** post-patch
(`verify-full` + a container-IP identity not in the cert SAN is rejected with
`SSL connection error: Validation of SSL server certificate failed`), but NOT via a
connector pre/post flip — a full connector flip to the unfixed commit is out of
scope per parent Q1. The submodule pin stays `2914d3f` in both halves of the pair.

## 5. Rollback considerations

- Revert safety: reverting drops the settings, the enums, and the gitlink repoint;
  no on-disk format change, no schema migration, no ZK state. Existing tables/dictionaries
  keep working (default `PREFER` = prior behavior).
- Persistent state: none survives a restart (settings are per-query/session; pool config
  is in-memory).
- Disable without rebuild: set `postgresql_connection_pool_ssl_mode = 'disable'` (PG) or
  `ssl_mode = 'disable'` in the MySQL named collection to turn off the new behavior.
- The submodule pin (`2914d3f`) carries the `X509_check_host` security fix; a revert would
  also drop that fix — note this in any rollback decision.

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — patch first carried on 25.8.

### 25.8-aiven (historical)

Source `934b35cc7d` (author Tilman Moeller, committer Aliaksei Khatskevich, 2025-12-15;
co-authored by Joe Lynch). Introduced the SSL settings/enums and the
`contrib/mariadb-connector-c` fork redirect with **25.8** values
(`branch = aiven/clickhouse-v25.8.12.129`, gitlink `107011a3e98…`).

### 26.3-aiven (this uplift)

- Cherry-pick was **conflict-resolved**. Exactly one real conflict
  (`src/Dictionaries/MySQLDictionarySource.cpp`); the other 4 predicted-conflict files
  auto-merged on line offset.
- **Manual resolutions:**
  1. `src/Core/Settings.h` — `M(CLASS_NAME, SSLMode)` moved from the source's position
     (between `UInt64Auto` and `URI`) to **alphabetical** order (between `SQLSecurityType`
     and `StreamingHandleErrorMode`). `MySQLSSLMode` deliberately **not** added here
     (it is not a `Settings`-typed enum).
  2. `src/Dictionaries/MySQLDictionarySource.cpp` — **kept the 26.3 non-named-collection
     `else` branch** (replica/host-filter + `PoolFactory` path); the source's deletion of
     that branch (replacing it with a `throw`) was **NOT** replayed. Only the
     named-collection `createMySQLPoolWithFailover(...)` call was threaded with `ssl_mode`,
     and `"ssl_mode"` added to `dictionary_allowed_keys` (both auto-merged cleanly).
     **⚠️ This was a security regression — see "MySQL named-collection enforcement
     restored" below.**

### MySQL named-collection enforcement restored (2026-06-30, `v26.3.15.4`)

The "kept the 26.3 non-named-collection `else` branch" resolution in §6.2(2) above
was wrong: that `else` branch is the **inline-credential path**, and deleting it
(replacing it with `throw Exception(UNSUPPORTED_METHOD, "MySQL dictionary source
configuration must use a named collection")`) is a **security guard**, not a
styling choice. It is the MySQL sibling of the PostgreSQL guard carried by patch
036 (`PostgreSQLDictionarySource.cpp` early throw). The 25.8 source the 021
cherry-pick was taken from (`934b35cc7d`, also `v25.8.18.1`/`aiven/v26.3`-line and
`PRODSEC-1797` / `aiven/v25.8.24.21-lts-aiven:150`) carried the `else { throw }`
form; the 26.3 port silently reverted to upstream's inline path, leaving a live
inline-credential code path for MySQL dictionaries (a tenant could embed a raw
host/user/password in `SOURCE(MYSQL(...))` DDL and leak it via `SHOW CREATE
DICTIONARY`, pointing the server at an arbitrary host).

- **Fix:** the inline `else` branch in `extractZooKeeperPathAndReplicaNameFrom…`'s
  MySQL counterpart (`registerDictionarySourceMysql`'s factory lambda) is replaced
  by the bare `else { throw … "must use a named collection"; }`, **verbatim from
  the 25.8 form** (no inline `Configuration`/pool is built before the throw). The
  named-collection branch (and the trailing `table or query` check, which applies
  to it) is unchanged.
- **Why it slipped through originally (subtle):** with the inline `else` present, a
  `SOURCE(MYSQL(host … user … password … db …))` with no `table`/`query` does **not**
  fail on "named collection" — it builds an inline config and trips the *later*
  `"must contain table or query field"` check. So the symptom was a misleading
  error message; the real defect was the live inline-credential path.
- **Test added:** `tests/integration/test_aiven_mysql_dict_named_collection/`
  (mirrors `test_aiven_postgres_dict_named_collection`): an XML/config-file MySQL
  dictionary is rejected with `UNSUPPORTED_METHOD` + the Aiven message (asserting
  both, per AGENTS §7), and a DDL named-collection dictionary still loads rows from
  a real MySQL.
  3–5. `DatabaseMaterializedPostgreSQL.cpp`, `StorageMaterializedPostgreSQL.cpp`,
     `StorageMySQL.h` — `#include <Core/SettingsEnums.h>` + the new fields / extended
     `formatConnectionString` calls (auto-merged; verified by hand).
- **Policy P1 (faithful Settings carry):** SSL knobs kept as user-overridable `Settings`
  (`DECLARE`), not converted to `ServerSettings` — they govern outbound connections to the
  user's own external DBs, matching sibling `postgresql_connection_pool_*` knobs.
- **Policy P2 (submodule fork-redirect, 26.3-adapted):** the cherry-pick staged the wrong
  25.8 values; overridden to 26.3: `.gitmodules` `url = https://github.com/aiven/mariadb-connector-c`,
  `branch = aiven/clickhouse-v26.3.10.62`, gitlink → `2914d3fbce4f82b0f0d66034eb7afd1dd3dc5c70`
  ("fix call for X509_check_host"). Object present locally; checked out in the submodule
  worktree and re-staged.
- **Connector-macro verification:** `MYSQL_OPT_SSL_VERIFY_SERVER_CERT` present at
  `2914d3f:include/mysql.h:188` — `verify-full` build path satisfied.
- `byte_equivalent: false` — only the documented divergences: the two submodule value
  adaptations (P2) and the kept-26.3 `MySQLDictionarySource` `else` branch (**later
  reverted — the `else { throw }` enforcement was restored 2026-06-30, see "MySQL
  named-collection enforcement restored" above**). (`Settings.h`'s alphabetical move is
  byte-identical at the +/- line level.)
- Build: `ninja -C build clickhouse` → exit 0 (full recompile, 1162 steps,
  `Settings.cpp`/`SettingsEnums.h` widely included + `mariadb-connector-c` rebuilt from the
  new pin). Cold-ish cache; ~17.5 min wall-clock.
- Test: authored in Dispatch 2 on the proven harness; evidence-of-causation pair
  green (post-patch 10/10) / red (pre-patch 5 CH tests fail on SSL-feature-absence).
  See §4.
- Anything surprising: the parent predicted 5 conflict files but the 3-way merge
  auto-resolved 4 — only `MySQLDictionarySource.cpp` needed hand resolution.

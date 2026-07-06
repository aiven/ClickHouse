# Patch 059 — skip-unused-server-certificates

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `2f477f8b6d` (author `tilman.moeller@aiven.io`, committer `joelynch112@gmail.com`, 2026-01-13; co-authored by Kevin Michel, Joe Lynch, Aris Tritas) | (the version we are porting FROM) |
| 26.3-aiven | `patch-port(059)` (`850d9b502b9`) | parent agent + subagent, 2026-06-09 | `still-needed-but-rewrite` — guard manually re-applied and **scoped to the server prefix**; source Hunk 1 (`Server.cpp`) **dropped**; ships an integration test |

`byte_equivalent: false` — the cherry-pick was reshaped on purpose (see §2/§6). The
source commit had two hunks; only one is carried, and that one is scoped
differently than the source.

## 1. Purpose

When the `openSSL.server` certificate/key paths are present in the config but the
server does not actually serve a secure port (neither `tcp_port_secure` nor
`https_port` is configured), ClickHouse should not try to read or parse those
files. Operators legitimately leave stale or placeholder cert paths in config; an
unused, missing, or unparseable file must not produce a spurious error. The
durable motivation is robustness of startup/config-reload against
configured-but-unused server certificates.

Source SHA on `v25.8.18.1-lts-aiven`: `2f477f8b6d` (from
`docs/aiven/uplifts/26.3/inventory.md` row 059).
Original author: `tilman.moeller@aiven.io` (committer `joelynch112@gmail.com`).
Original purpose (quoted from `git log`):

> Fix ClickHouse trying to read non-existent certificate files
> If both the certificate file and key file are defined, even when they are
> not used, ClickHouse will try to read and parse them. We can't even
> work around the issue by creating empty files.
>
> The latest approach to the problem is to make sure there's one
> successful call for `tryLoad` before calling `tryReloadAll`. During the
> research it was noticed that `tryLoad` loads exactly one prefix
> `"openSSL.server."` and there's no place in the code that adds another
> prefix (at least in our usecase). This means that `tryReloadAll` acts
> effectively the same as `tryLoad`.

## 2. Upstream-drift findings

### Commands run

```bash
git log v25.8.18.1-lts..v26.3.10.62-lts -- src/Server/CertificateReloader.cpp
git show v26.3.10.62-lts:src/Server/CertificateReloader.cpp | grep -n \
  'tcp_port_secure\|https_port\|CFG_SERVER_PREFIX\|CFG_CLIENT_PREFIX\|tryLoadClient'
```

### Findings

- Upstream changes to touched files between prior and current LTS:
  - `src/Server/CertificateReloader.cpp`: **heavily refactored** between 25.8 and
    26.3. The subsystem now supports per-prefix multi-context loading
    (`data_index` keyed by prefix), client certificates (`tryLoadClient` →
    `CFG_CLIENT_PREFIX`), and ACME / Let's Encrypt (`tryLoadACMECertificate`,
    gated on `config.has("acme")`). All paths funnel through the single private
    `tryLoadImpl(config, ctx, prefix)`.
  - `programs/server/Server.cpp`: the config-reload handler now calls only
    `CertificateReloader::instance().tryReloadAll(config())` (Server.cpp:2447);
    `tryReloadAll` loops over **every** registered prefix in `data_index`
    (server *and* client) and calls `tryLoadImpl` for each. Startup additionally
    calls `tryLoad(config())` then `tryLoadClient(config())` (Server.cpp:2640-2641).
- Upstream changes that touched the patch's behavior:
  - **No existing secure-port gate.** Base 26.3 `tryLoadImpl` has no
    `tcp_port_secure`/`https_port` check — the patch's premise still holds.
  - The only upstream "do nothing" guard in `tryLoadImpl` is the *empty-path*
    check (`if (new_cert_path.empty() || new_key_path.empty()) … return;`). It
    does **not** cover the patch's case: paths that are **defined but unused**
    (and possibly non-existent). Those non-empty paths sail past the empty-path
    check and get read, which is exactly the bug.
- Conclusion: **`still-needed-but-rewrite`** — semantics unchanged but the
  cherry-pick cannot apply as-is; manual port required (see §6).

## 3. C++ review

- 1 Lifetime + ownership: ✓ — the guard is a pure early `return` before any
  `findOrInsert`/context creation; no objects are constructed, so no ownership
  changes.
- 2 Exception safety: ✓ — returning before the load path means the
  `defaultServerContext()` initialization (which throws on a missing/unparseable
  cert) is never reached for the unused-server-cert case; nothing to roll back.
- 3 Thread-safety + concurrency: ✓ — `tryLoadImpl` is called under
  `data_mutex` (`TSA_REQUIRES(data_mutex)`); the guard adds only local reads of
  the config and a log line, holding the same lock.
- 4 Performance + memory: ✓ — strictly *removes* work (two `getString` lookups
  + early return) on the no-secure-port path; no allocations.
- 5 Settings as public API: n/a — no new setting; reads existing
  `tcp_port_secure` / `https_port` config keys.
- 6 Error handling: ✓ — converts a spurious `<Error>` (missing-cert read) into
  an informational `<Information>` log on a path where certificates are genuinely
  not needed.
- 7 Upstream / vendored code: n/a — change is confined to Aiven-relevant
  `src/Server/CertificateReloader.cpp`.
- 8 Behavior under settings: ✓ — narrow, functional no-op (see clause-(v) note
  below); only affects the case where no secure server port exists.

**Clause-(v) note.** This is a default-behavior change but it is a *narrow,
functional no-op*: it only suppresses loading of **server** certificates that the
server would never use anyway (no secure port to present them on). It does not
change behavior for any deployment that actually serves `tcp_port_secure` /
`https_port`, and it does not affect client certificates. It is therefore **not
gated behind a setting** — there is no broad blast radius to gate.

## 4. Test design

(a) **New integration test that fails on the pre-patch binary and passes after the patch.**

- Test path: `tests/integration/test_aiven_lazy_certificates/`
  (per the `test_aiven_<slug>` convention). Two legs:
  - **Leg A (the fix)** — `node_no_secure`: `openSSL.server.certificateFile` /
    `privateKeyFile` point at non-existent paths and **no** secure port is
    configured. Asserts the server starts, `CertificateReloader` logs the guard
    message, and `CertificateReloader` does **not** reference the unused paths.
  - **Leg B (guard does not over-skip)** — `node_secure`: `https_port` + a valid,
    freshly-generated server cert/key. Asserts `CertificateReloader` loads the
    certificate, the guard does **not** fire, and the HTTPS port actually serves
    TLS (an unverified-context `SELECT 1` over HTTPS returns `1`).

  Assertions are **scoped to the `CertificateReloader` logger** on purpose: other
  SSL-context consumers (notably `MySQLHandlerFactory`, which lazily builds its
  own context from `openSSL.server` and logs `Failed to create SSL context. SSL
  will be disabled.` for the same bogus paths) are a separate, pre-existing code
  path **not** addressed by this patch — exactly as upstream's source commit also
  touched only `CertificateReloader`. Scoping keeps the test signal on the code
  the patch actually changes.

- **Pre-patch run output (the FAIL)** — `build/test_059_prepatch.log`
  (binary built with the guard stashed out):

  ```text
  [leg A guard log]
  [leg A CertificateReloader missing-path refs] 2026.06.09 10:50:07.410194 [ 8 ] {} <Error> CertificateReloader: Poco::Exception. Code: 1000, e.code() = 0, SSL context exception: Error loading private key from file /etc/clickhouse-server/does_not_exist.key: error:80000002:system library::No such file or directory (version 26.3.10.1)
  FAILED
  E   AssertionError: expected the patched guard message 'No server certificates needed' from CertificateReloader in the server log, but it was absent (pre-patch behavior).
  =================== 1 failed, 1 passed, 4 warnings in 7.33s ====================
  ```

  i.e. the unpatched `CertificateReloader` **does** try to read the unused key
  (`Error loading private key from file …/does_not_exist.key`), and leg A fails
  on the missing guard message. Leg B passes pre-patch (cert loading works
  regardless), confirming the divergence is the *read attempt*, not a
  feature-absence artifact.

- **Post-patch run output (the PASS)** — `build/test_059_postpatch.log`:

  ```text
  [leg A guard log] 2026.06.09 10:48:33.825002 [ 8 ] {} <Information> CertificateReloader: No server certificates needed as tcp_port_secure and https_port are not provided
  [leg A CertificateReloader missing-path refs]
  PASSED
  [leg B reload log] 2026.06.09 10:48:33.832036 [ 8 ] {} <Information> CertificateReloader: Reloaded certificate (/etc/clickhouse-server/config.d/server-cert.pem) and key (/etc/clickhouse-server/config.d/server-key.pem).
  PASSED
  ======================== 2 passed, 3 warnings in 5.26s =========================
  ```

- Why this test distinguishes the Aiven gate from upstream behavior: base 26.3
  has no secure-port gate; the guard message `No server certificates needed …`
  is emitted only by the Aiven patch, and its presence/absence is the exact
  pre/post differentiator (causation pair).

- **No private key material is committed.** Leg B's cert/key are generated at
  fixture time by `certs/generate_certs.sh` and are git-ignored
  (`certs/.gitignore`). Leg A uses deliberately non-existent paths (no files).

## 5. Rollback considerations

- Revert safety: removing the guard restores the prior
  read-the-configured-cert behavior; no schema/migration/on-disk-format impact.
- No state survives restart (no ZK nodes, no files, no caches introduced).
- No setting to toggle (clause-(v): narrow functional no-op, not gated). An
  operator who wanted the old behavior would have to patch it out, but the only
  observable difference is suppression of a spurious error on a path where the
  certificate is never used.

## 6. Per-uplift notes

### 25.3-aiven (historical, may be empty)

n/a — patch first carried later.

### 25.8-aiven (historical)

Source `2f477f8b6d` (author `tilman.moeller@aiven.io`, committer
`joelynch112@gmail.com`, 2026-01-13; co-authored by Kevin Michel, Joe Lynch,
Aris Tritas). Two hunks:
1. `programs/server/Server.cpp` — swap
   `CertificateReloader::instance().tryReloadAll(*config)` →
   `tryLoad(*config)` in the config-reload handler.
2. `src/Server/CertificateReloader.cpp` `tryLoadImpl` — early-return guard
   (`if tcp_port_secure empty && https_port empty → return`), **unscoped**
   (applied to every prefix).

### 26.3-aiven (this uplift)

- Cherry-pick was: **rewritten** (two key deviations from source).
- Upstream-drift conclusion: `still-needed-but-rewrite` (from §2).

**Deviation (i) — source Hunk 1 (`Server.cpp`) is DROPPED.** On 26.3 the
config-reload handler calls `tryReloadAll`, which loops over *all* registered
prefixes in `data_index` (server **and** client) and routes each through
`tryLoadImpl`. The guard in `tryLoadImpl` (Hunk 2) already fixes the bug for
every entry point (startup `tryLoad`/`tryLoadClient` and reload `tryReloadAll`),
so Hunk 1 is unnecessary. Worse, swapping `tryReloadAll` → `tryLoad` on 26.3
would reload only the **server** prefix and stop **client-cert hot-reload on
config reload** — a regression that did not exist when the source patch was
written (the source's own reasoning, "`tryReloadAll` acts effectively the same
as `tryLoad`", was true on the older base with a single prefix, but is false on
26.3). `programs/server/Server.cpp` is therefore left untouched.

**Deviation (ii) — the guard is SCOPED to `CFG_SERVER_PREFIX`.** On 26.3,
client certificates (outgoing mutual TLS) route through the **same**
`tryLoadImpl` with `Poco::Net::SSLManager::CFG_CLIENT_PREFIX`, and they are
independent of `tcp_port_secure` / `https_port`. An unscoped guard (as in the
source) would skip client-cert loading whenever no secure *server* port is
configured — a regression on 26.3. The ported guard adds
`prefix == Poco::Net::SSLManager::CFG_SERVER_PREFIX &&` so it fires only for the
server prefix; the log message is correspondingly worded "No **server**
certificates needed …".

- Test added at: `tests/integration/test_aiven_lazy_certificates/` (legs A + B;
  evidence pair in §4). Optional leg C (client-cert-without-secure-port) was
  **not** added: it would require non-trivial client-mTLS plumbing
  (an outgoing TLS consumer to exercise the client context). The server-prefix
  scoping is instead covered by code review (the `CFG_SERVER_PREFIX` guard is
  visibly inert for the client prefix) plus leg B (which proves the guard does
  not over-skip when a secure port *is* present).
- Build: incremental, `CertificateReloader.cpp` force-recompiled
  (`touch` + `ninja -C build clickhouse`), clean (`build/build_059.log`).
- Anything surprising: the unpatched read attempt surfaces as a Poco
  `SSL context exception: Error loading private key from file …` (from
  `defaultServerContext()` initialization inside `tryLoadImpl`'s `try` block),
  not as a `std::filesystem` modification-time error — the missing **key** is
  hit during context construction before the modification-time check.

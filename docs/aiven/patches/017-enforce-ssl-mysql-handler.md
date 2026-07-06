# Patch 017 — enforce-ssl-mysql-handler

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `0a8c8c86bd` | Tilman Moeller (author) / Aliaksei Khatskevich (committer), 2025-12-13 | (the version we are porting FROM) |
| 26.3-aiven | `patch-drop(017)` | parent agent, 2026-06-02 | `obsoleted-by-upstream` — drop; see §2 |

The drop was committed as `patch-drop(017)` (no code carried; reason in §2).
Find it with `git log --grep '^patch-drop(017)'`. Co-authored on the source by
Joe Lynch <joe.lynch@aiven.io>.

## 1. Purpose

Per the source commit body, the intent was to **enforce SSL/TLS for all inbound
MySQL protocol connections** — reject any client that connects without SSL:

> This commit enforces SSL/TLS encryption for all MySQL protocol connections to
> ClickHouse. Previously, clients could connect without SSL and communicate in
> plaintext. Now, any connection attempt without SSL is rejected with an error
> message. [...] An error packet (MySQL error code 3159) is sent [...]
> Error message: "SSL support for MySQL TCP protocol is required. If using the
> MySQL CLI client, please connect with --ssl-mode=REQUIRED."

The change (`0a8c8c86bd`, 1 file, `src/Server/MySQLHandler.cpp`) rewrites the
`else` branch of `MySQLHandler::finishHandshake` — the path taken when the
client did **not** send an `SSLRequest`. Instead of reading the rest of the
plaintext `HandshakeResponse`, it ignores the remaining bytes and:

```cpp
static constexpr const char * error_msg = "SSL support for MySQL TCP protocol is required. ...";
packet_endpoint->sequence_id++;
packet_endpoint->sendPacket(ERRPacket(3159, "HY000", error_msg), true);   // true = flush
throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, error_msg);
```

Source SHA on `v25.8.18.1-lts-aiven`: `0a8c8c86bd`. Original author:
`tilman.moeller@aiven.io`.

## 2. Upstream-drift / validity findings

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`.

### Commands run

```bash
# Does upstream 26.3 ship a setting that enforces SSL on the MySQL port?
rg -n 'mysql_require_secure_transport' src/Core/ServerSettings.cpp
# → 1516: DECLARE(Bool, mysql_require_secure_transport, false,
#         "If set to true, secure communication is required with clients over
#          mysql_port. Connection with option <--ssl-mode=none> will be refused. ...")

# Is the setting upstream (in the base tag), not Aiven-added?
git show v26.3.10.62-lts:src/Core/ServerSettings.cpp | rg -n mysql_require_secure_transport
# → present (line 1514) — UPSTREAM base, not aiven.
git show v25.8.18.1-lts:src/Core/ServerSettings.cpp  | rg -n mysql_require_secure_transport
# → (no output) — the setting did NOT exist on the 25.8 base.

# Does the handler enforce it, and with what error?
rg -n 'secure_required|SSL connection required' src/Server/MySQLHandler.cpp
# → 282: if (secure_required && !(client_capabilities & CLIENT_SSL))
#   283:     throw Exception(ErrorCodes::OPENSSL_ERROR, "SSL connection required.");

# Did the per-connection check already exist on the 25.8 handler?
git show v25.8.18.1-lts:src/Server/MySQLHandler.cpp | rg -n 'secure_required|SSL connection required'
# → 281-282: the check existed in 25.8 too (but with no setting to turn it on).
```

Full logs under `tmp/patch-017/`.

### Findings

- **Upstream 26.3 already enforces SSL on the MySQL port — via a setting.**
  `mysql_require_secure_transport` (default `false`) is declared in
  `ServerSettings.cpp` and is present in the base tag `v26.3.10.62-lts`
  (upstream-shipped, not Aiven). When enabled, `MySQLHandler` rejects any
  non-SSL connection at `MySQLHandler.cpp:282-283`:
  `if (secure_required && !(client_capabilities & CLIENT_SSL)) throw OPENSSL_ERROR "SSL connection required."`.
  The sibling `postgresql_require_secure_transport` does the same for the
  PostgreSQL port.
- **The capability is already exercised by an upstream test.**
  `tests/integration/test_mysql_protocol/test.py::test_mysql_client_secure`
  connects to a node with the setting on using `--ssl-mode=disabled`, asserts
  the connection is dropped, and asserts the server log contains
  `SSL connection required.`. This is green on 26.3.
- **Provenance — why the Aiven patch existed, and why it's now obsolete.** On
  the **25.8** base the handler already had the `secure_required` check, but
  `ServerSettings.cpp` had **no `mysql_require_secure_transport` setting** — i.e.
  there was no supported way to turn the check on. That is why Aiven hardcoded
  the rejection in the `else` branch (unconditional enforcement). On **26.3**
  upstream **added the setting** (and the wiring that sets `secure_required` from
  it), so the security goal is now reachable the idiomatic way — the hardcode is
  redundant.
- **Equivalence of outcome.** Both approaches reject a non-SSL MySQL session
  **before authentication** (upstream parses the plaintext `HandshakeResponse`
  then refuses at line 282; the patch refuses in the `else` branch without
  parsing it — the plaintext bytes are on the wire either way, so not parsing
  them locally changes no exposure). The security invariant — *no plaintext
  MySQL session reaches auth* — holds with `mysql_require_secure_transport=true`.

| Aspect | Patch 017 (Aiven, from 25.8) | Upstream 26.3 |
|---|---|---|
| Gating | Unconditional (hardcoded) | Setting `mysql_require_secure_transport` (default `false`) |
| Enforcement point | `finishHandshake` `else` branch | `MySQLHandler.cpp:282` via `secure_required` |
| Error | `SUPPORT_IS_DISABLED`, MySQL code 3159, CLI hint | `OPENSSL_ERROR`, "SSL connection required." |
| Configurable / testable | No | Yes (and already tested) |

- Conclusion: **`obsoleted-by-upstream`** — drop. The replacement is **not** more
  code; it is **configuration**: set `mysql_require_secure_transport=true` (and,
  for symmetry, `postgresql_require_secure_transport=true`) in Aiven's managed
  server config. This differs from 007 (`irrelevant-by-removal`) and 044
  (`ineffective-no-op`); here the patch worked, but upstream now provides the
  same guarantee as a first-class, configurable, already-tested setting.

### The only reasons to NOT drop (considered, rejected)

- **Different error code/message** (3159 + `SUPPORT_IS_DISABLED` + a `--ssl-mode`
  hint vs `OPENSSL_ERROR` + "SSL connection required."). Would matter only if
  Aiven client tooling parses the 3159 code or that exact hint string. No
  evidence it does; the security outcome is identical. Not worth carrying a
  custom code path against upstream.
- **Non-disableable behavior.** The hardcode cannot be turned off; the setting
  can. For a managed fleet this is not a loss — Aiven sets the config and does
  not expose it, which is operationally equivalent to "always on".

## 3. C++ review

`n/a — no code carried by the drop.` The relevant observation is the
equivalence argument in §2: upstream's `secure_required` path preserves the same
pre-auth-rejection invariant the patch enforced, gated by a setting rather than
hardcoded.

## 4. Test design

(b)/(c) **No new test — the patch is DROPPED, not ported.**

- **Existing upstream test that covers the behavior:**
  `tests/integration/test_mysql_protocol/test.py::test_mysql_client_secure` —
  connects with `--ssl-mode=disabled` to a node with
  `mysql_require_secure_transport`/`secure_required` on, asserts the non-SSL
  connection is refused and the log shows `SSL connection required.`. Green on
  26.3.
- **Why no Aiven test is warranted:** there is no Aiven *code* change to test —
  we carry nothing, so a fails-before/passes-after evidence pair (AGENTS §7(a))
  is impossible by construction. The behavior we would assert is upstream's, and
  it is already exercised by the test above (AGENTS §7(b) justification).
- **What a repo test cannot cover (deliberately):** the production guarantee now
  lives in the *config* (`mysql_require_secure_transport=true`), in Aiven's
  managed deployment layer outside this checkout. A stateless/integration test
  in this repo cannot assert the production config value; that guarantee belongs
  to the deployment layer and the §5 handover note, not to a test here.

## 5. Rollback considerations

- **Critical handover — the security posture moves from code → config.** With
  the patch dropped, nothing in the binary forces SSL. Aiven's managed server
  config MUST set `<mysql_require_secure_transport>true</mysql_require_secure_transport>`
  (and `<postgresql_require_secure_transport>true</postgresql_require_secure_transport>`
  for the PostgreSQL port) to keep the guarantee. If that config is not set, the
  MySQL handler reverts to allowing plaintext sessions — a silent security
  regression. **This drop is only safe paired with that config change.**
- Revert safety: `n/a` — no code carried by the drop.
- Future-uplift watch: if a later upstream ever removes
  `mysql_require_secure_transport`, the guarantee disappears and this analysis
  must be revisited (the next uplift's drift check — the `ServerSettings.cpp`
  grep in §2 — will surface that).

## 6. Per-uplift notes

### 25.8-aiven (historical)

Original carry: `0a8c8c86bd` (author Tilman Moeller, committer Aliaksei
Khatskevich, 2025-12-13; co-authored by Joe Lynch). The handler's
`secure_required` check existed, but `ServerSettings.cpp` had no
`mysql_require_secure_transport` setting to enable it — so the patch hardcoded
unconditional rejection in the `finishHandshake` `else` branch.

### 26.3-aiven (this uplift)

- Cherry-pick: NOT performed. Parent stopped at Step 1 (drift/validity) with
  conclusion `obsoleted-by-upstream`.
- Test added: `n/a — no source change; see §4.` Behavior covered by the existing
  upstream `test_mysql_client_secure`.
- Surprising bit: the patch applies cleanly (the `else`-branch pre-image is
  byte-identical on 26.3 HEAD — clauses (i)/(ii) both PASS), so a naive port
  would have succeeded and silently shipped an unconditional, non-disableable
  default-on change (a clause (v) blast-radius hazard) that *duplicates* an
  upstream setting. The tell was the existing `test_mysql_client_secure` already
  asserting an `SSL connection required.` log line with a *different* message
  than the patch's — which led to the `mysql_require_secure_transport` setting.
  "Applies cleanly" said nothing about "still needed".

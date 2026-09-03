---
description: 'Dossier for the Aiven patch that reduces the default ClickHouse HTTP endpoint surface'
sidebarTitle: '040: Harden HTTP defaults'
slug: '/aiven/patches/040-harden-default-http-endpoints'
title: 'Patch 040: Harden default HTTP endpoints'
doc_type: 'reference'
---

# Patch 040 — Harden default HTTP endpoints {#patch-040-harden-default-http-endpoints}

Policy matrix:
[`../uplifts/26.8/http-endpoint-inventory.md`](../uplifts/26.8/http-endpoint-inventory.md).

## Lineage {#lineage}

| LTS | How to find | Outcome |
|---|---|---|
| 26.3 | `patch-port(040): Disable replicas_status endpoint` at `68991b17cc331709ca3cdd35ec670ccc78be0690` | ported |
| 26.3 | `patch-new(N05): disable optional debug web UI HTTP endpoints` at `a85de28354aee48ad5193e5321a05c67a0622a3b` | absorbed into `040` on 26.8 |
| 26.8 | `patch-port(040): harden default HTTP endpoint surface` | `still-needed-but-rewrite` |

The 26.3 `040` commit ultimately descends from
`1151af44bb858b26d352958a4d8b4a9743c1ecc6`. The 26.3 `N05` commit descends
from `7cee0c660e9e8b55e54deb2a1706d85f57a13e61`.

## Background {#background}

`HTTPHandlerFactory` installs a set of handlers whenever the default HTTP
configuration is used. Those handlers become reachable without an operator
writing an explicit `http_handlers` rule.

Some handlers are service endpoints, such as `/ping` and the query API. Others
serve optional browser tools. The HTML for a browser tool may be static, but
the tool can issue queries against the same ClickHouse origin, so automatic
registration still expands the managed service's unauthenticated surface.

The composable `http_handlers` configuration is the opt-in mechanism. Removing
a handler from the default set does not require removing its implementation.

## Problem {#problem}

The upstream 26.8 default exposes more HTTP paths than the Aiven service needs:

- `/replicas_status` walks replicated tables and reports health and lag without
  authentication;
- `/binary`, `/merges`, `/jemalloc`, and `/clickstack` are optional diagnostic
  or observability UIs already removed from the 26.3 Aiven default; and
- `/schema` and `/processors-profile` were added after the earlier endpoint
  review and are also optional.

Porting only the historical `040` commit would close one path while leaving the
same policy problem on the others. Porting `N05` literally would also miss the
26.8 additions.

## Approach {#approach}

Use one reviewed allow/deny matrix rather than replaying endpoint removals as
unrelated patches.

Keep `/`, `/ping`, the query API, `/play`, `/dashboard`, `/docs`, and `/js/` in the
default set. Stop automatically registering `/replicas_status`, `/binary`,
`/merges`, `/jemalloc`, `/clickstack`, `/schema`, and `/processors-profile`.
No other endpoint registration changes.

`/docs` remains an Aiven-approved reference UI. Its upstream stateless tests
cover content sanitization and same-origin credential boundaries, so retaining
the default also retains meaningful security coverage rather than moving that
suite to a fork-only configuration.

Keep every implementation available for explicit configuration. Most disabled
handlers already have a matching `http_handlers` type. Add types for
`clickstack` and `processors_profile`, which are the two reversibility gaps on
the 26.8 base.

Do not remove `/webterminal`. Aiven sets `enable_webterminal=false`, and the
handler enforces that setting by returning `403 Forbidden`. Do not alter ACME:
its challenge handler is already registered only when `<acme>` exists, while
Aiven provisions certificates outside ClickHouse.

Do not add another server setting. Per-endpoint `http_handlers` rules provide
the required granular opt-in once the two missing types exist. A global switch
would overlap that mechanism and make the effective policy harder to reason
about.

## Drift on this uplift {#drift-on-this-uplift}

Conclusion: `still-needed-but-rewrite`.

- `HTTPHandlerFactory.cpp` still registers the historical `040` and `N05`
  endpoints by default.
- 26.8 additionally registers `/schema`, `/processors-profile`, `/docs`, and
  `/webterminal`; `/docs` is retained after policy review.
- `/webterminal` now has an existing `enable_webterminal` guard; Aiven uses it.
- `schema` and `docs` can already be restored with explicit handler rules.
- `clickstack` and `processors_profile` need new explicit handler types.
- The landing page and not-found response contain links or advice that must
  match the resulting default surface.

## Tests {#tests}

- Stateless path: `tests/queries/0_stateless/aiven_040_default_http_endpoints.sh`
  and its reference file.
- Integration path: `tests/integration/test_aiven_default_http_endpoints/`.
- The stateless test proves the default allow/deny status matrix. The
  integration test uses dedicated server configuration to prove
  `enable_webterminal=false` returns `403 Forbidden` and explicit
  `http_handlers` rules restore `clickstack` and `processors_profile`.
- Evidence: FAIL on parent / PASS with patch, recorded in the halt report.
- Existing upstream tests that require disabled UIs by default are deleted or
  adapted; they must not silently redefine the Aiven default.

## Rollback {#rollback}

A code revert restores upstream automatic registration and requires no data or
metadata migration. Before a rebuild, an operator can restore a disabled
endpoint with an explicit `http_handlers` rule. No on-disk or ZooKeeper state
is created by the patch.

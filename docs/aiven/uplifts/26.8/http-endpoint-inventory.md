---
description: 'Decision record for the default HTTP surface exposed by Aiven ClickHouse 26.8'
sidebarTitle: 'HTTP endpoint inventory'
slug: '/aiven/uplifts/26.8/http-endpoint-inventory'
title: 'Aiven 26.8 default HTTP endpoint inventory'
doc_type: 'reference'
---

# Aiven 26.8 default HTTP endpoint inventory {#aiven-26-8-default-http-endpoint-inventory}

This inventory defines which built-in HTTP handlers Aiven ClickHouse exposes
without an explicit `http_handlers` rule. It is the policy input for patch
`040-harden-default-http-endpoints`.

The inventory is deliberately scoped to the HTTP handler surface. It is not a
global inventory of the 26.8 uplift.

## Decision rule {#decision-rule}

Keep a handler in the default set only when at least one of these is true:

1. It is required for managed-service health or query traffic.
2. It is an Aiven-approved product UI.
3. It is already inert unless the operator enables a separate server feature.

Otherwise, require an explicit `http_handlers` rule. In particular, an
unauthenticated diagnostics page is not safe merely because it serves a static
HTML shell: the page can initiate queries against the same ClickHouse HTTP
origin and it expands the fleet's externally reachable behavior.

Disabling a default must remain reversible. The handler implementation stays
compiled, and a matching `http_handlers` type must exist so an operator can
restore the endpoint deliberately. A new server setting is justified only when
the composable handler mechanism cannot express the required control.

## Endpoint decisions {#endpoint-decisions}

| Endpoint | Default decision | Control after patch | Reason |
|---|---|---|---|
| `/` | allow | built-in default; configurable response remains supported | Landing page and HTTP entry point. Its links must list only enabled defaults. |
| `/ping` | allow | built-in default and `ping` rule | Managed health check. |
| HTTP query API | allow | existing query handler and custom rules | Core ClickHouse service traffic. |
| `/play` | allow | built-in default and `play` rule | Aiven-approved SQL UI. |
| `/dashboard` | allow | built-in default and `dashboard` rule | Aiven-approved operational UI. |
| `/docs` | allow | built-in default and `docs` rule | Aiven-approved reference UI. Keeping it also preserves upstream coverage of its same-origin and content-sanitization boundaries. |
| `/js/` | allow | built-in default and `js` rule | Serves the embedded assets required by retained UIs, including `/dashboard`. |
| `/replicas_status` | deny | existing `replicas_status` rule | Walks replicated tables and exposes replication health and lag without authentication. |
| `/binary` | deny | existing `binary` rule | Optional binary-inspection UI; not required for service operation. |
| `/merges` | deny | existing `merges` rule | Optional merge-observability UI; not required for service operation. |
| `/jemalloc` | deny | existing `jemalloc` rule | Optional allocator UI. This does not disable the jemalloc profiler, settings, or system tables. |
| `/clickstack` | deny | add a `clickstack` rule type | Optional observability UI. The 26.3 patch removed its default but did not provide an explicit opt-in type; 26.8 must close that reversibility gap. |
| `/schema` | deny | existing `schema` rule | Optional schema UI added after the earlier Aiven review. |
| `/processors-profile` | deny | add a `processors_profile` rule type | Optional processor-profile UI added after the earlier Aiven review. No explicit rule type exists on the 26.8 base. |
| `/webterminal` | registered but disabled by Aiven configuration | `enable_webterminal=false`; existing `webterminal` rule remains available | The handler already enforces the server setting and returns `403 Forbidden` when disabled. Removing registration would duplicate an existing guard. |
| `/.well-known/acme-challenge/…` | configuration-gated | existing `<acme>` configuration | The handler is registered only when `<acme>` exists. Aiven provisions TLS externally, so no fork change is needed. |
| Prometheus paths | configuration-gated | existing Prometheus configuration and rules | Paths are deployment-defined rather than unconditional defaults. |

The deny decisions concern automatic registration only. They do not remove
handler classes, embedded resources, SQL functionality, profiling facilities,
or explicit operator configuration.

## Port lineage {#port-lineage}

The unified 26.8 port uses identity `040` and absorbs two earlier decisions:

- `patch-port(040): Disable replicas_status endpoint`
  (`68991b17cc331709ca3cdd35ec670ccc78be0690`).
- `patch-new(N05): disable optional debug web UI HTTP endpoints`
  (`a85de28354aee48ad5193e5321a05c67a0622a3b`).

`N05` must not be ported again after `040` lands. The unified port also reviews
the `/schema`, `/processors-profile`, `/docs`, and `/webterminal` handlers that
arrived or changed after the earlier review.

## Implementation boundaries {#implementation-boundaries}

The patch should:

- remove deny-listed handlers from the built-in default registration;
- retain `/play`, `/dashboard`, `/docs`, and the `/js/` assets they require;
- add explicit `clickstack` and `processors_profile` handler types;
- leave the `enable_webterminal` check and ACME registration logic intact;
- remove links to disabled defaults from `programs/server/index.html`;
- stop advertising `/replicas_status` in the not-found response; and
- delete or adapt upstream tests whose only contract is that a disabled UI is
  available by default.

The patch should not:

- add a global “all optional endpoints” switch;
- change authentication, query routing, Prometheus, Keeper HTTP handlers, or
  `clickhouse-proxy`;
- remove embedded UI resources or handler implementations; or
- change jemalloc profiling behavior.

## Verification contract {#verification-contract}

Use two focused tests:

- `aiven_040_default_http_endpoints` is a stateless status matrix for the
  default deny list and the retained `/`, `/ping`, `/play`, `/dashboard`,
  `/docs`, and `/js/` paths.
- `test_aiven_default_http_endpoints` is an integration test with explicit
  server configuration. It verifies `enable_webterminal=false` returns
  `403 Forbidden` and `http_handlers` rules restore `clickstack` and
  `processors_profile`. A dedicated instance is justified because these are
  startup-time server configuration decisions.

The halt report must contain FAIL-on-parent and PASS-with-patch evidence. A
failure caused only by an upstream test that assumes a removed default is not
sufficient; the Aiven tests must exercise the policy and reversibility
contracts directly.

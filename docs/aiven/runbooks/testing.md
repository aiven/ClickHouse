# Runbook — which tests to write

| Need | Suite | Location |
|---|---|---|
| Default server behavior | **Stateless** | `tests/queries/0_stateless/aiven_<NNN>_<slug>.{sql,sh}` (+ `.reference`) |
| Cluster / restart / external deps | **Integration** | `tests/integration/test_aiven_<slug>/` |
| Pure C++ seam | **Unit** | `src/**/tests/gtest_*.cpp` |

## Naming

| Suite | Pattern | Example |
|---|---|---|
| Stateless | `aiven_<NNN>_<slug>` | `aiven_022_protected_users.sql` |
| Integration | `test_aiven_<slug>/` | `test_aiven_protected_users/` |

- `<NNN>` = dossier number (zero-padded). `<slug>` = dossier slug without `NNN-`.
- Glob for the Aiven CI lane: `aiven_*` / `test_aiven_*`.
- Do **not** use upstream `add-test`. When porting 26.3 `9<NNN>_*`, rename to
  `aiven_<NNN>_<slug>` on this line.

## Rules

- Prefer stateless. Integration only when unreachable on one default server
  (see `integration.md`). **TLS fixtures must mint certs via a generator** —
  never commit PEMs (`integration.md`).
- Decide suite at **parent preflight** (clause (iv) reachability) — do not
  discover mid-worker that you needed DinD.
- Error-code-only asserts are insufficient; include an Aiven-specific message
  substring.
- Causation: FAIL on parent, PASS with patch (`build-and-test.md`).

## Recurring stateless recipe: `system.zookeeper`

When the bug is a leftover ZK path, assert **counts or values** on an explicit
path under a unique prefix (include patch slug / database name) — do not rely
on ambient leftover znodes. Prefer a path **without** `{uuid}` if the leak is
on a parent node the patch must delete.

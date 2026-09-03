# Skill — C++ review checklist (Aiven ports)

One pass over the staged patch. Record hits in the halt report; `n/a` is fine.

1. **Lifetime / ownership** — who owns memory; no use-after-free across waits.
2. **Exception safety** — locks/resources released; no partial durable state.
3. **Concurrency** — shared state guarded; no new races on Context/caches.
4. **Performance** — prefer batch/column paths; avoid per-row alloc churn.
5. **Settings as API** — new knobs documented; defaults safe; `aiven_` for new
   Aiven settings only.
6. **Errors** — precise exceptions; user-visible messages stable enough to test.
7. **Vendored / contrib** — no silent forks; submodule redirects escalate.
8. **Behavior under settings** — gated features default off when blast radius is
   wide (`policy_call`).

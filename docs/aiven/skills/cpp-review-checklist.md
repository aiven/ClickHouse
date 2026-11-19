---
description: 'One-pass C++ review checklist for staged Aiven patch ports'
sidebarTitle: 'C++ review checklist'
slug: '/aiven/skills/cpp-review-checklist'
title: 'Skill — C++ review checklist (Aiven ports)'
doc_type: 'guide'
---

# Skill — C++ review checklist (Aiven ports) {#skill-c-review-checklist-aiven-ports}

One pass over the staged patch. Record hits in the halt report; `n/a` is fine.

1. **Lifetime / ownership** — who owns memory; no use-after-free across waits.
2. **Exception safety** — locks/resources released; no partial durable state.
3. **Concurrency** — shared state guarded; no new races on Context/caches.
4. **Performance** — prefer batch/column paths; avoid per-row alloc churn.
5. **Settings as API** — new knobs documented; defaults safe; `aiven_` for new
   Aiven settings only.
6. **Errors** — precise exceptions; user-visible messages stable enough to test.
7. **Vendored / contrib** — no silent forks; submodule redirects escalate. A
   contrib library that decodes client-supplied bytes (compression, formats)
   is attack surface: check its upstream security advisories against the
   pinned version.
8. **Behavior under settings** — gated features default off when blast radius is
   wide (`policy_call`).
9. **Identity and paths** — authorization uses the authenticated identity, not
   one the client sent; a path is checked in the form it is used, and never
   built with `fs::path::operator/` from an untrusted absolute or `..` part.

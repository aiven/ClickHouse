---
description: 'How to order Aiven patch ports within an LTS uplift'
sidebarTitle: 'Execution sequencing'
slug: '/aiven/runbooks/execution-sequencing'
title: 'Runbook — execution sequencing'
doc_type: 'guide'
---

# Runbook — execution sequencing {#runbook-execution-sequencing}

Order ports by **subsystem**, not by historical commit number.

1. **Group** — Kafka, object storage, Access, Refreshable MV, replication, …
2. **Dependencies inside a group** — consecutive source commits / shared files
   first.
3. **Warm-up** — small, cherry-pick-clean, low blast-radius first.
4. **Hold** — default-behavior / fleet-wide policy for explicit human call.
5. **Last** — high-churn cores (`RefreshTask`, `MergeTreeData`, replication).

Groups become **tickets** in the tracker, and tickets are grouped into
**lanes**. A lane works its tickets in ascending number order; lanes do not wait
for each other. There is deliberately no single global order — a top-to-bottom
list would invent precedence that does not exist.

A note inside a ticket about which unit to take next is scoped to that ticket
and never establishes what the next port is overall. Hard dependencies between
units go in the tracker's ordering-constraint field, with the reason.

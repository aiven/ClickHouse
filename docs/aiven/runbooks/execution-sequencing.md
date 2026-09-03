# Runbook — execution sequencing

Order ports by **subsystem**, not by historical commit number.

1. **Group** — Kafka, object storage, Access, Refreshable MV, replication, …
2. **Dependencies inside a group** — consecutive source commits / shared files
   first.
3. **Warm-up** — small, cherry-pick-clean, low blast-radius first.
4. **Hold** — default-behavior / fleet-wide policy for explicit human call.
5. **Last** — high-churn cores (`RefreshTask`, `MergeTreeData`, replication).

Record the chosen order in `uplifts/<ver>/execution-plan.md`. Update as you
learn; do not maintain a parallel inventory table unless needed.

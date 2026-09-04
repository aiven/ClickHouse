# 26.8 — execution plan

Fill when ports start. Method: `../../runbooks/execution-sequencing.md`.

## Groups (draft — edit freely)

1. Warm-up / low-risk (config, small gates)
2. Access / security
3. Object storage / backups
4. Kafka / integrations
5. Refreshable MV family (high churn — later)
6. Replication / MergeTree core (last)

## Queue

| Order | Slug | Notes |
|---|---|---|
| 1 | `040-harden-default-http-endpoints` | First security warm-up; absorbs `N05`; policy in `http-endpoint-inventory.md` — landed |
| 2 | `011-restrict-show-create-access` | Access group; needs rewrite for `StorageSystemTables`; adds `system.databases` as a third door |

There is no global patch inventory table. Add focused subsystem inventories
only when a port requires a policy review, as `http-endpoint-inventory.md`
does. Discover source commits with:

```bash
git log --oneline --no-merges v26.3.26.3-lts..v26.3.26.3-lts-aiven
```

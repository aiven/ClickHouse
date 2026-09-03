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
| | | |

No inventory table. Discover source commits with:

```bash
git log --oneline --no-merges v26.3.26.3-lts..v26.3.26.3-lts-aiven
```

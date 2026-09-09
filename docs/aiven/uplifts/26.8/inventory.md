---
description: 'Ledger of every Aiven patch identity carried onto ClickHouse 26.8, with its group, ticket and status'
sidebarTitle: 'Patch inventory'
slug: '/aiven/uplifts/26.8/inventory'
title: 'Aiven 26.8 uplift patch inventory'
doc_type: 'reference'
---

# Aiven 26.8 uplift patch inventory {#aiven-26-8-uplift-patch-inventory}

One row per Aiven patch identity on the 26.3 source line. This is the ledger of
**what exists**; [`execution-plan.md`](execution-plan.md) owns **what order and
who**, and each patch's dossier under [`../../patches/`](../../patches/) owns
**why and how**.

Jira mirrors this: one issue per ticket, holding a title, a status and a link to
the plan. Where a Jira description disagrees with these files, these files win.

## How to read a row {#how-to-read-a-row}

`26.3 size` and `26.3 outcome` describe how the patch landed on the **previous**
line. They are context for scoping, not a verdict on 26.8.

The `Basis` column is the one to read first:

- **`26.8`** — this uplift screened or decided the row. Trustworthy.
- **`26.3`** — the row is inherited. The group and ticket are a *provisional*
  placement derived from what the patch did at 26.3.

Only eight identities carry a `26.8` basis today — the five landed ones plus
`026`, `N06` and `N07`, whose decisions this uplift took. Every `26.3` row is a
hypothesis, and screening may change a patch's purpose, its shape, its ticket,
or drop it outright.

Note that a `26.8` basis does not mean "settled work": `N06`'s *shape* is
decided by the [defense-in-depth policy](execution-plan.md#defense-in-depth) —
`RESTORE` removed outright, executable UDFs patched out rather than gated —
while its port is still ahead.

## Screening changes purpose, not just applicability {#screening-changes-purpose}

A row is not a work order. When a patch is picked up, screening asks **what is
this for on 26.8** before asking whether it still applies — and the answer can
invalidate the row.

The 26.3 uplift is the evidence. `002` and `017` were not obsolete in the sense
of "upstream fixed the bug"; upstream grew a *setting*, so the patch's purpose
moved from code into Aiven's managed config. `044` turned out never to have
done anything on either line. `026`'s purpose held but its whole design was
retracted. `057` was deep-screened without reaching a conclusion at all, and is
carried here with its purpose explicitly still open.

So a screening outcome is legitimately any of: port as-is, port rewritten,
replace with config, drop as obsolete, drop as never-effective, or re-scope the
requirement. Update the row and the plan when that happens; do not force the
finding to fit the inherited placement.

## Status vocabulary {#status-vocabulary}

| Status | Meaning |
|---|---|
| `landed` | ported and committed on the 26.8 work branch |
| `planned` | assigned to a ticket, not yet screened |
| `dropped` | never carried past 26.3, with the recorded reason — but see [conditional drops](#conditional-drops) where that reason is "upstream grew a setting" |
| `folded` | not an independent item; squashed into a sibling |

**Most** dropped rows stay dropped without re-screening. Where upstream
absorbed the behavior in code or removed the feature outright, 26.8 is strictly
newer than 26.3, so the drop holds by construction. They are listed so a future
uplift does not rediscover them.

### Conditional drops must be verified once {#conditional-drops}

One class of drop does **not** hold by construction. Where upstream replaced
the patch with a *setting*, the patch's purpose did not disappear — it moved out
of the binary and into Aiven's managed config. The drop is then valid only if
that config actually sets the key. Nothing in the build enforces it, and both
known cases default to **off**:

| Patch | Upstream replacement | Default | Verified |
|---|---|---|---|
| `002` enable internal replication | `internal_replication`, a `DatabaseReplicated` setting | `false` | no — coordinate with `004` and its [ordering constraint](execution-plan.md#ordering-constraints) |
| `017` enforce SSL MySQL handler | `mysql_require_secure_transport`, a server setting | `false` | no |

`017` is the one to take seriously: a TLS enforcement control that used to be
compiled into the binary is now one config key away from being absent, in a
repository nobody reading this file is looking at. Upstream also grew a sibling,
`postgresql_require_secure_transport`, which the original MySQL-only patch never
covered — a second door, the same shape as the executable-UDF drivers.

This is the [defense-in-depth](execution-plan.md#defense-in-depth) argument
pointed at the drops rather than the ports: *"dropped because upstream grew a
setting"* and *"inert because a setting defaults to false"* are the same accident
of configuration. Both rows keep a `26.3` basis until checked, which is exactly
what that column is for.

Verifying them is Ticket 0 work. The remaining `obsoleted-by-upstream` rows
(`001`, `027`, `074`) need the same one-time triage — absorbed-in-code versus
replaced-by-config — before any of them can be trusted as unconditional.

## Inventory {#inventory}

| Patch | Title | 26.3 size | 26.3 outcome | Group | Ticket | 26.8 status | Basis |
|---|---|---|---|---|---|---|---|
| `001` | advertise host from config | 1f/3 | dropped | — | — | dropped — obsoleted-by-upstream | 26.3 |
| `002` | enable internal replication for databasereplicated clusters | 3f/27 | dropped | — | — | dropped — replaced-by-config, unverified | 26.3 |
| `003` | enable alter database modify setting | 4f/29 | ported | G1 | 6 | planned | 26.3 |
| `004` | replace mergetree with replicated | 2f/30 | rewrite | G1 | 5 | planned | 26.3 |
| `005` | tolerate zk restart with exponential backoff | 1f/10 | ported | G1 | 6 | planned | 26.3 |
| `006` | replicated database attach with shard macro | 1f/2 | ported | G1 | — | landed | 26.8 |
| `007` | recover lost replica deflate qpl setting | 1f/1 | dropped | — | — | dropped — irrelevant-by-removal | 26.3 |
| `008` | replication queue size limit | 13f/287 | rewrite | G1 | 5 | planned | 26.3 |
| `009` | replicate move partition through database replicated | 1f/14 | rewrite | G1 | 5 | planned | 26.3 |
| `010` | default logs to keep | 1f/2 | ported | G1 | 6 | planned | 26.3 |
| `011` | restrict show create database access | 2f/14 | ported | G3 | — | landed | 26.8 |
| `012` | s3 custom ca path | 24f/158 | rewrite | G5 | 4 | planned | 26.3 |
| `013` | azure custom ca path | 2f/8 | rewrite | G5 | 4 | planned | 26.3 |
| `014` | default profile escape | 7f/166 | ported | G3 | 1 | planned | 26.3 |
| `015` | s3 signature delegation | 17f/297 | ported | G6 | 3 | planned | 26.3 |
| `016` | azure signature delegation | 10f/148 | ported | G6 | 3 | planned | 26.3 |
| `017` | enforce ssl mysql handler | 1f/12 | dropped | — | — | dropped — replaced-by-config, unverified | 26.3 |
| `018` | enforce https url storage | 3f/32 | rewrite | G5 | 4 | planned | 26.3 |
| `019` | avnadmin indirect database creation | 14f/287 | rewrite | G3 | 1 | planned | 26.3 |
| `020` | check table default privileges | 1f/1 | ported | G3 | 1 | planned | 26.3 |
| `021` | external db ssl | 28f/236 | rewrite | G5 | 4 | planned | 26.3 |
| `022` | protected users | 38f/847 | rewrite | G3 | 1 | planned | 26.3 |
| `023` | fix ipv6 s3 object storage host | 1f/2 | folded | — | — | folded into `015` | 26.3 |
| `024` | fix ipv6 azure object storage host | 2f/4 | ported | G6 | 3 | planned | 26.3 |
| `025` | azure storage prefix | 1f/3 | ported | G6 | 3 | planned | 26.3 |
| `026` | soft delete on object storage (supersedes backup disk) | — | concept replaced | G6 | 3 | planned | 26.8 |
| `027` | fix uncaught exception s3 storage | 1f/11 | dropped | — | — | dropped — obsoleted-by-upstream | 26.3 |
| `028` | skip create azure container | 2f/15 | staged | G6 | 3 | planned | 26.3 |
| `029` | kafka sasl ssl settings | 8f/263 | rewrite | G7 | 7 | planned | 26.3 |
| `030` | kafka num consumers zero | 3f/10 | ported | G7 | 7 | planned | 26.3 |
| `031` | kafka schema registry auth | 5f/30 | ported | G7 | 7 | planned | 26.3 |
| `032` | kafka offset reset datetime | 9f/38 | ported | G7 | 7 | planned | 26.3 |
| `033` | kafka extra settings | 8f/95 | ported | G7 | 7 | planned | 26.3 |
| `034` | unlock postgresql database | 2f/322 | rewrite | G8 | 9 | planned | 26.3 |
| `035` | named collection integration metadata | 1f/5 | ported | G8 | 9 | planned | 26.3 |
| `036` | postgresql dictionary named collection | 3f/157 | ported | G8 | 9 | planned | 26.3 |
| `037` | ignore unreadable sensors | 1f/2 | ported | G11 | 10 | planned | 26.3 |
| `038` | fix tcp port secure from zk | 2f/25 | ported | G11 | 10 | planned | 26.3 |
| `039` | disable thread fuzzer | 1f/8 | ported | G11 | 10 | planned | 26.3 |
| `040` | disable replicas status endpoint | 1f/6 | ported | G3 | — | landed | 26.8 |
| `041` | swap drift | 7f/138 | rewrite | G11 | 10 | planned | 26.3 |
| `042` | zk node leak after create delete table | 2f/18 | ported | G1 | 6 | planned | 26.3 |
| `043` | remove ssbs check from no armv81 or higher | 1f/2 | dropped | — | — | dropped — superseded-by-upstream-equivalent | 26.3 |
| `044` | enable curl ipv6 | 1f/2 | dropped | — | — | dropped — ineffective-no-op | 26.3 |
| `045` | disable individual dictionary sources | 3f/45 | ported | G4 | 2 | planned | 26.3 |
| `046` | early fetch pool | 9f/93 | ported | G2 | 6 | planned | 26.3 |
| `047` | per server max bytes merge mutate override | 5f/62 | ported | G2 | 6 | planned | 26.3 |
| `048` | zero copy fixes | 2f/28 | ported | G2 | 6 | planned | 26.3 |
| `049` | refreshable mv shard macro expansion | 1f/3 | ported | G9 | 8 | planned | 26.3 |
| `050` | refreshable mv zookeeper | 1f/27 | ported | G9 | 8 | planned | 26.3 |
| `051` | disable table engines and functions | 5f/81 | ported | G4 | 2 | planned | 26.3 |
| `052` | register flags newer engines | 3f/15 | ported | G4 | 2 | planned | 26.3 |
| `053` | check mergetree settings constraints before ddl | 1f/16 | ported | G2 | 6 | planned | 26.3 |
| `054` | keeper map read only setting | 4f/222 | rewrite | G12 | 6 | planned | 26.3 |
| `055` | named collection system tables | 25f/163 | ported | G8 | 9 | planned | 26.3 |
| `056` | freeze include metadata version | 1f/1 | ported | G6 | 3 | planned | 26.3 |
| `057` | remove cloud specific settings | 16f/610 | ported | G12 | 6 | planned | 26.3 |
| `058` | disallow replication parameter customization | 1f/29 | ported | G12 | 6 | planned | 26.3 |
| `059` | skip unused server certificates | 2f/9 | rewrite | G5 | 4 | planned | 26.3 |
| `060` | alter order by sorting key zk metadata | 1f/3 | ported | G1 | 6 | planned | 26.3 |
| `061` | clickhouse dictionary source changes | 4f/131 | ported | G8 | 9 | planned | 26.3 |
| `062` | prohibit tmp table creation | 5f/21 | rewrite | G3 | 2 | planned | 26.3 |
| `063` | — | 1f/4 | ported | — | — | folded into `062` | 26.3 |
| `064` | — | 1f/4 | ported | — | — | folded into `062` | 26.3 |
| `065` | prohibit https to http redirect | 2f/24 | ported | G5 | 4 | planned | 26.3 |
| `066` | mv refresh sharded | 4f/1011 | rewrite | G9 | 8 | planned | 26.3 |
| `067` | self signed certs local endpoints | 1f/7 | ported | G5 | 4 | planned | 26.3 |
| `068` | systemd logging | 3f/255 | ported | G11 | 10 | planned | 26.3 |
| `069` | zk uptime keeper mntr | 5f/22 | ported | G11 | 10 | planned | 26.3 |
| `070` | disable ytsaurus engine | 2f/4 | ported | G4 | 2 | planned | 26.3 |
| `071` | register ytsaurus directives | 4f/9 | rewrite | G4 | 2 | planned | 26.3 |
| `072` | wait for distributed database creation | 1f/13 | ported | G1 | 6 | planned | 26.3 |
| `073` | fix compatibility setting crash on removed setting | 1f/5 | ported | G12 | — | landed | 26.8 |
| `074` | fix arrowflight ipv6 listen host | 2f/50 | dropped | — | — | dropped — obsoleted-by-upstream | 26.3 |
| `075` | register arrowflight flags | 3f/8 | staged | G4 | 2 | planned | 26.3 |
| `076` | kafka offset reset by duration | 9f/294 | rewrite | G7 | 7 | planned | 26.3 |
| `077` | hide secrets system mutations command | 4f/36 | ported | G3 | 2 | planned | 26.3 |
| `078` | — | 2f/69 | ported | — | — | folded into `066` | 26.3 |
| `079` | protected roles | 8f/98 | rewrite | G3 | 1 | planned | 26.3 |
| `080` | named collection alter propagation | 1 (+test)f/~16 | ported | G8 | 9 | planned | 26.3 |
| `N01` | REGISTER_WEBASSEMBLY_UDF build-time gate | — | new | G4 | 2 | planned | 26.3 |
| `N02` | keep coordinated refreshable MVs on Apache ZooKeeper | — | new | G9 | 8 | planned | 26.3 |
| `N03` | register TimeSeries external targets as dependencies | — | new | G10 | 10 | planned | 26.3 |
| `N04` | no deref of missing TimeSeries metrics target on ATTACH | — | new | G10 | 10 | planned | 26.3 |
| `N05` | disable optional debug web UI HTTP endpoints | — | new | G3 | — | landed — absorbed into `040` | 26.8 |
| `N06` | remove RESTORE and executable UDFs; gate custom disks | — | new, shape decided by 26.8 policy | G4 | 2 | planned | 26.8 |
| `N07` | GRANT ... EXCEPT in one statement | — | new | G3 | 2 | planned | 26.8 |

87 identities: 70 planned, 5 landed, 8 dropped, 4 folded.

## Groups {#groups}

Groups are subsystem buckets used to keep a ticket's bullets reviewable by one
person. They carry no status of their own.

| Group | Subsystem |
|---|---|
| G1 | Replicated database and replication core |
| G2 | Merges, fetches, storage policy |
| G3 | Access control and tenant isolation |
| G4 | Feature and engine expose-gate (`REGISTER_*`) |
| G5 | Transport security and certificates |
| G6 | Object storage: S3, Azure, soft delete |
| G7 | Kafka |
| G8 | External DBs, dictionaries, named collections |
| G9 | Refreshable materialized views |
| G10 | TimeSeries engine |
| G11 | Ops, packaging, observability |
| G12 | Settings surface |

## Work outside the identities {#outside-identities}

Three `patch-fix` commits on the 26.3 line carry no identity and must be ported
**with their parent**, or the parent ships with a known defect: `patch-fix(046)`
(early-fetch executor not drained on shutdown), `patch-fix(022,079)`
(`PROTECTED` round-trip via `optional<bool>`), and `patch-fix(050)`
(refreshable-MV shard refresh race). A fourth, `patch-fix(026)`, is retired
along with the wrapped-disk design.

Aiven also pulled three upstream backports forward onto the 26.3 line
(`#106946` cluster function parallelism, `#107077` wildcard-free `like`,
`#107027` MV insert squashing). These are upstream commits, not Aiven patches;
confirm 26.8 contains them, but they need no port.

Because the inventory is keyed by number, none of the above is visible to a
number-driven sweep. They were recovered by reconciling every commit subject on
the source branch against these rows — worth repeating at the end of the uplift.

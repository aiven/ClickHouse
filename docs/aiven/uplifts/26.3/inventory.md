# 26.3 uplift — patch inventory

Mechanical inventory produced by the T2 classifier subagent. One row per
commit in `v25.8.18.1-lts..origin/v25.8.18.1-lts-aiven` at classification
time. No judgment of testability, complexity, or porting priority — those
decisions are made fresh per dispatch in T3+.

## How to read this inventory

- **`NNN`** is a stable 1-based index, sorted by committer-date ascending
  (oldest commit on the branch = `001`). For most patches this matches the
  `date` column closely; for cherry-picks from upstream (e.g. row 074), the
  `date` (= author date) can be much older than the position implies.
- **`date`** is the author date (`git log --format=%ad --date=short`),
  NOT the committer date. For native Aiven patches the two are close; for
  upstream-authored cherry-picks they may diverge by months.
- **`author`** is the **author** email (`git log --format=%ae`) — i.e., the
  person who originally wrote the patch. This is the right column to consult
  when asking "who designed this?". Author attribution is preserved across
  the 25.3 → 25.8 → 26.3 porting chain; if it shows tilman, tilman wrote it.
  T2.2 (post-rebase) data shows: 63 tilman, 10 khatskevich, 4 joelynch,
  1 vitlibar (the vitlibar row is upstream-authored — see note on patch 074
  in the retrospective).
- **`committer`** is the **committer** email (`git log --format=%ce`) — i.e.,
  the person who landed this SHA on the `v25.8.18.1-lts-aiven` branch. Useful
  for "who to ask about the integration / merge" as opposed to "who designed
  it". After the May 21 force-push of `v25.8.18.1-lts-aiven`, the committer
  is almost entirely `joelynch112@gmail.com` (57) or
  `alex.khatskevich@aiven.io` (21). Pre-rebuild SHAs had committer mostly
  `tilman.moeller@aiven.io` (the 25.3 → 25.8 porting committer); the rebuild
  re-attributed committer to whoever landed each patch on the release branch.
- **`cherry_pick_clean`** is `yes` if `git format-patch -1 <sha> --stdout |
  git apply --check` exits 0 against the current HEAD on
  `v26.3.10.62-lts-aiven-dev` (the readonly equivalent of a cherry-pick
  dry-run). `no` means the patch would conflict at apply time. `error`
  means metadata extraction failed for that commit (none on this run).
- **`files`** and **`loc`** are from `git diff-tree --numstat`. `loc` is
  added + removed lines (i.e., total line churn, not net delta).
- **Linked `NNN`** — when an `NNN` cell is a markdown link (e.g.
  `[007](../patches/007-...)`), the linked file is the patch's dossier and
  is the source of truth for the per-uplift port outcome. The dossier's §6
  records whether the patch was shipped byte-equivalent, conflict-resolved,
  drift-superseded, or dropped — and why. The inventory itself stays
  mechanical (it does not duplicate outcomes); follow the link.

## Range justification

Source range: `v25.8.18.1-lts..origin/v25.8.18.1-lts-aiven`. This is the
"patches Aiven added to the prior LTS" range (78 commits), NOT
`v26.3.10.62-lts..origin/v25.8.18.1-lts-aiven` (which would be 944 commits
because it sweeps in ~866 upstream stable-branch backports that are not
Aiven patches). Per release-management decision 2026-05-22:
`v25.8.18.1-lts-aiven` is the current production aiven LTS line.

## Classification history

- **T2.1 (2026-05-22T12:24:05Z)** — first classifier dispatch against
  `v25.8.18.1-lts-aiven` at tip `3f401037cfb` ("Hide secrets in
  system.mutations.command column"). Count: 77 patches. Captured 8 columns
  (NNN, sha, date, author, files, loc, subject, cherry_pick_clean).
- **2026-05-21 (after T2.1 data collection but before commit)** —
  `v25.8.18.1-lts-aiven` was **force-pushed** by release management, rebuilt
  with proper `Author / Committer` distinction (committer is now the person
  who landed the patch on the release line, not the porter). One additional
  patch landed ("Fix MV refresh task race condition" = the previously
  pending `khatskevich/mv_race_258` work). New tip: `ea9c5d6420f`. All 77
  prior SHAs changed.
- **T2.2 (2026-05-22, later)** — re-classifier dispatch against the new tip.
  Count: 78. Same 25 `cherry_pick_clean=yes` / 53 `no` shape. Added a
  `committer` column (9 columns total) to surface the
  author-vs-committer distinction that the rebuild made meaningful.

The table below reflects T2.2 (current state). All SHAs are stable against
the current `origin/v25.8.18.1-lts-aiven` tip until the next force-push.

## Pending merges (out of scope for this inventory)

After the May 21 rebuild:

- `khatskevich/mv_race_258` — **merged** into `v25.8.18.1-lts-aiven` as row
  078 ("Fix MV refresh task race condition", `ea9c5d6420`). No longer pending.
- `khatskevich/peerdb_258` — still unmerged. After the force-push of
  `v25.8.18.1-lts-aiven`, this branch's tip is no longer a descendant of
  the production line, so `git rev-list --count
  origin/v25.8.18.1-lts-aiven..origin/khatskevich/peerdb_258` reports 58
  commits unique to it — but those are mostly the *old* (pre-rebuild) line
  re-surfacing; only the genuinely-new patches will need to be re-classified
  once `peerdb_258` is rebased onto the new tip.

When `peerdb_258` lands, re-dispatch the classifier; the inventory
regenerates idempotently and the only diff will be the new row(s).

## Classification metadata

- T2.1 classifier dispatch: 2026-05-22T12:24:05Z (subagent id `toolu_019bjhJRVovPjShrqdjWCitE`)
- T2.2 classifier dispatch: 2026-05-22 (re-run after force-push)
- Source range count: 78 (`git rev-list --count v25.8.18.1-lts..origin/v25.8.18.1-lts-aiven`)
- Classifier exit-toolchain: `git format-patch -1 <sha> --stdout | git apply --check`
- See also: T2 retrospective at `docs/aiven/uplifts/26.3/01-t2-classifier-retrospective.md`

## Status annotations (per-uplift overlay)

The inventory's columns are immutable T2-classifier output. Per-uplift outcomes (ported, dropped) are NOT a separate column — adding one would require migrating all 77 rows on every uplift. Instead, per-uplift status is recorded **inline in the `subject` column** using markdown conventions:

- **Ported (default):** subject text unchanged. The patch's dossier (`docs/aiven/patches/<NNN>-<slug>.md`) is the source of truth for the porting outcome.
- **Dropped (`obsoleted-by-upstream` or `irrelevant-by-removal`):** subject text is wrapped in `~~strikethrough~~`, followed by **bold annotation** stating the reason, the upstream SHA (for `obsoleted-by-upstream`) or the removed feature (for `irrelevant-by-removal`), and a relative-path link to the dossier.

Pattern (used for row 001 in this uplift):

> ~~Advertise host from config for replicated databases~~ **DROPPED — `obsoleted-by-upstream`. Upstream `9dd658aea06` landed equivalent fix. See dossier [`001-advertise-host-from-config.md`](../../patches/001-advertise-host-from-config.md).**

Rationale: dropped rows are the load-bearing case (the patch will never carry through any future uplift); ported rows are routine progress. Annotating only the load-bearing case keeps the inventory readable. The dossier carries the durable record either way.

## Inventory

| NNN | sha | date | author | committer | files | loc | subject | cherry_pick_clean |
|-----|-----|------|--------|-----------|-------|-----|---------|-------------------|
| 001 | ac84fa6f7c | 2025-12-02 | tilman.moeller@aiven.io | alex.khatskevich@aiven.io | 1 | 3 | ~~Advertise host from config for replicated databases~~ **DROPPED — `obsoleted-by-upstream`. Upstream `9dd658aea06` landed equivalent fix. See dossier [`001-advertise-host-from-config.md`](../../patches/001-advertise-host-from-config.md).** | no |
| 002 | fdc262dc9d | 2025-12-04 | tilman.moeller@aiven.io | alex.khatskevich@aiven.io | 3 | 27 | Enable internal replication for DatabaseReplicated clusters | no |
| 003 | 1055b0defe | 2025-12-04 | tilman.moeller@aiven.io | alex.khatskevich@aiven.io | 4 | 29 | Enable ALTER DATABASE MODIFY SETTING for Replicated databases | no |
| 004 | 226ed6cc31 | 2025-12-05 | tilman.moeller@aiven.io | alex.khatskevich@aiven.io | 2 | 30 | Replace MergeTree with ReplicatedMergeTree in Replicated databases | yes |
| 005 | 6a37150173 | 2025-12-06 | tilman.moeller@aiven.io | alex.khatskevich@aiven.io | 1 | 10 | Tolerate ZooKeeper restart with increased retries and exponential backoff | no |
| 006 | 22e03c9d9d | 2025-12-07 | tilman.moeller@aiven.io | alex.khatskevich@aiven.io | 1 | 2 | Fix ClickHouse restart with replicated tables containing {shard} macro | yes |
| [007](../../patches/007-recover-lost-replica-deflate-qpl-setting.md) | 5228bf2cd4 | 2025-12-07 | tilman.moeller@aiven.io | alex.khatskevich@aiven.io | 1 | 1 | Add missing settings to recoverLostReplica | yes |
| 008 | d6e78ab993 | 2025-12-07 | tilman.moeller@aiven.io | alex.khatskevich@aiven.io | 13 | 287 | Fix unbounded replication queue growth | no |
| 009 | 110900c986 | 2025-12-07 | tilman.moeller@aiven.io | alex.khatskevich@aiven.io | 1 | 14 | Replicate ALTER TABLE MOVE PARTITION queries through DatabaseReplicated | yes |
| 010 | 199db08799 | 2025-12-08 | tilman.moeller@aiven.io | alex.khatskevich@aiven.io | 1 | 2 | Change default logs_to_keep from 1000 to 300 for DatabaseReplicated | no |
| 011 | 654f61e864 | 2025-12-09 | tilman.moeller@aiven.io | alex.khatskevich@aiven.io | 2 | 14 | Restrict SHOW CREATE DATABASE access | yes |
| 012 | 8fc1c96ae0 | 2025-12-09 | tilman.moeller@aiven.io | alex.khatskevich@aiven.io | 24 | 158 | Allow custom CA certificate path for S3 connections | no |
| 013 | 2f70d49490 | 2025-12-09 | tilman.moeller@aiven.io | alex.khatskevich@aiven.io | 2 | 8 | Allow custom CA certificate path for Azure Blob Storage connections | no |
| 014 | e65f68836b | 2025-12-10 | tilman.moeller@aiven.io | alex.khatskevich@aiven.io | 7 | 166 | Fix default profile escape vulnerability | yes |
| 015 | d3b5e9016f | 2025-12-12 | tilman.moeller@aiven.io | alex.khatskevich@aiven.io | 17 | 297 | Allow delegating S3 signature to a separate process | no |
| 016 | 38e54d3589 | 2025-12-13 | tilman.moeller@aiven.io | alex.khatskevich@aiven.io | 10 | 148 | Allow delegating Azure signature to a separate process | no |
| 017 | 0a8c8c86bd | 2025-12-13 | tilman.moeller@aiven.io | alex.khatskevich@aiven.io | 1 | 12 | Enforce SSL in the MySQL handler | yes |
| 018 | ae35b0cc72 | 2025-12-13 | tilman.moeller@aiven.io | alex.khatskevich@aiven.io | 3 | 32 | Enforce HTTPS for URL storage and HTTPDictionarySource | no |
| 019 | 05d8148a57 | 2025-12-13 | tilman.moeller@aiven.io | alex.khatskevich@aiven.io | 14 | 287 | Allow avnadmin creating database using sql | no |
| 020 | c5b03b2c0e | 2025-12-13 | tilman.moeller@aiven.io | alex.khatskevich@aiven.io | 1 | 1 | Add CHECK TABLE to default privileges | no |
| 021 | 934b35cc7d | 2025-12-15 | tilman.moeller@aiven.io | joelynch112@gmail.com | 28 | 236 | Add SSL/TLS configuration support for PostgreSQL and MySQL connections | no |
| 022 | ce74bc008d | 2025-12-15 | tilman.moeller@aiven.io | joelynch112@gmail.com | 38 | 847 | Added support for protected users | no |
| 023 | abd678347b | 2025-12-15 | tilman.moeller@aiven.io | joelynch112@gmail.com | 1 | 2 | Fix IPv6 S3 object storage host The AWS SDK does not fully support IPv6 in hostnames, mainly because the escaping brackets are not parsed and removed at the right time. | yes |
| 024 | 1abdc712c0 | 2025-12-16 | tilman.moeller@aiven.io | joelynch112@gmail.com | 2 | 4 | Fix IPv6 Azure object storage host | no |
| 025 | fdb7a142d1 | 2025-12-16 | tilman.moeller@aiven.io | joelynch112@gmail.com | 1 | 3 | Add support for Azure object storage path prefix | no |
| 026 | 3139011de0 | 2025-12-16 | tilman.moeller@aiven.io | joelynch112@gmail.com | 14 | 343 | Add Backup disk type | no |
| 027 | 2f052691db | 2025-12-18 | tilman.moeller@aiven.io | joelynch112@gmail.com | 1 | 11 | Fix uncaught exception if S3 storage fails | yes |
| 028 | 8ed6167986 | 2025-12-18 | tilman.moeller@aiven.io | joelynch112@gmail.com | 1 | 1 | Skip attempt to create a container in azure blob storage | yes |
| 029 | f45b8fb6fb | 2025-12-22 | tilman.moeller@aiven.io | joelynch112@gmail.com | 8 | 263 | Add Kafka configuration support for SASL and SSL settings | no |
| 030 | 37b61f443b | 2025-12-22 | tilman.moeller@aiven.io | joelynch112@gmail.com | 3 | 10 | Allow decreasing number of Kafka consumers to zero | yes |
| 031 | 7d48793fc5 | 2025-12-22 | tilman.moeller@aiven.io | joelynch112@gmail.com | 5 | 30 | Support per-table schema registry with authentication | no |
| 032 | 9f769ed901 | 2025-12-23 | tilman.moeller@aiven.io | joelynch112@gmail.com | 9 | 38 | Add kafka_auto_offset_reset and kafka_date_time_input_format settings | no |
| 033 | 6384eccfac | 2025-12-23 | tilman.moeller@aiven.io | joelynch112@gmail.com | 8 | 95 | Add extra settings to Kafka Table Engine | no |
| 034 | b1a99ca92b | 2026-01-02 | tilman.moeller@aiven.io | joelynch112@gmail.com | 2 | 322 | Unlock PostgreSQL database | no |
| 035 | 65248bdd34 | 2026-01-02 | tilman.moeller@aiven.io | joelynch112@gmail.com | 1 | 5 | Add support for integration metadata to named collections validation | yes |
| 036 | d293fbe109 | 2026-01-02 | tilman.moeller@aiven.io | joelynch112@gmail.com | 3 | 157 | Multiple changes in PostgreSQL dictionary | no |
| 037 | 7a7058eba8 | 2026-01-05 | tilman.moeller@aiven.io | joelynch112@gmail.com | 1 | 2 | Ignore unreadable sensors | yes |
| 038 | a498627944 | 2026-01-05 | tilman.moeller@aiven.io | joelynch112@gmail.com | 2 | 25 | Fix tcp_port_secure from ZK | no |
| 039 | 32e9abc159 | 2026-01-05 | tilman.moeller@aiven.io | joelynch112@gmail.com | 1 | 8 | Disable thread fuzzer | yes |
| 040 | 1151af44bb | 2026-01-05 | tilman.moeller@aiven.io | joelynch112@gmail.com | 1 | 6 | Disable replicas_status endpoint | yes |
| 041 | a05665ce57 | 2026-01-05 | tilman.moeller@aiven.io | joelynch112@gmail.com | 7 | 138 | Fix swap drift | no |
| 042 | 0d6eb5bece | 2026-01-05 | tilman.moeller@aiven.io | joelynch112@gmail.com | 2 | 18 | Fix ZK node leak after create delete table | yes |
| 043 | 6b54a78936 | 2026-01-05 | tilman.moeller@aiven.io | joelynch112@gmail.com | 1 | 2 | Remove SSBS check from `NO_ARMV81_OR_HIGHER` | no |
| 044 | b8e3cdd8ea | 2026-01-05 | tilman.moeller@aiven.io | joelynch112@gmail.com | 1 | 2 | Enable curl ipv6 | yes |
| 045 | d4973afdd7 | 2026-01-06 | tilman.moeller@aiven.io | joelynch112@gmail.com | 3 | 45 | Allow disabling of individual dictionary sources | no |
| 046 | 7e08e44e51 | 2026-01-06 | tilman.moeller@aiven.io | joelynch112@gmail.com | 9 | 93 | Add early fetch pool When adding a replica to an existing cluster, the replica will add many GET_PART tasks to its replication queue. These tasks are in charge of downloading the data that existed before the creation of the replica. | no |
| 047 | 4a05c78da7 | 2026-01-06 | tilman.moeller@aiven.io | joelynch112@gmail.com | 5 | 62 | Add per-server override for max bytes to merge/mutate | no |
| 048 | c6d9323ba2 | 2026-01-07 | tilman.moeller@aiven.io | joelynch112@gmail.com | 2 | 28 | Zero copy fixes | no |
| 049 | cc745f53f9 | 2026-01-07 | tilman.moeller@aiven.io | joelynch112@gmail.com | 1 | 3 | Fix refreshable materialized views where there is a shard macro in the target table | yes |
| 050 | 63f05c4e12 | 2026-01-08 | tilman.moeller@aiven.io | joelynch112@gmail.com | 1 | 27 | Allow refreshable materialized views when using ZooKeeper (rather than ClickHouse Keeper) | yes |
| 051 | 4641e51fdc | 2026-01-08 | tilman.moeller@aiven.io | joelynch112@gmail.com | 5 | 81 | Added support for disabling various table engines and table functions | no |
| 052 | f987c06d9e | 2026-01-08 | tilman.moeller@aiven.io | joelynch112@gmail.com | 3 | 15 | additional compiler flags for newer engines/function | no |
| 053 | bb04848360 | 2026-01-09 | tilman.moeller@aiven.io | joelynch112@gmail.com | 1 | 16 | Check MergeTree settings constraints before enqueuing DDL queries | no |
| 054 | 4c16ca65e9 | 2026-01-09 | tilman.moeller@aiven.io | joelynch112@gmail.com | 4 | 222 | Add read-only setting to KeeperMap storage | no |
| 055 | d6d957fd41 | 2026-01-09 | tilman.moeller@aiven.io | joelynch112@gmail.com | 25 | 163 | Add named_collection column to system.tables | no |
| 056 | b18cff803a | 2026-01-09 | tilman.moeller@aiven.io | joelynch112@gmail.com | 1 | 1 | Include metadata_version.txt when freezing | no |
| 057 | ac227df851 | 2026-01-12 | tilman.moeller@aiven.io | joelynch112@gmail.com | 16 | 610 | remove all cloud-specific settings | no |
| 058 | 9a2883592c | 2026-01-13 | tilman.moeller@aiven.io | joelynch112@gmail.com | 1 | 29 | Disallow replication parameters customization | no |
| 059 | 2f477f8b6d | 2026-01-13 | tilman.moeller@aiven.io | joelynch112@gmail.com | 2 | 9 | Fix ClickHouse trying to read non-existent certificate files If both the certificate file and key file are defined, even when they are not used, ClickHouse will try to read and parse them. We can't even work around the issue by creating empty files. | no |
| 060 | 93c2be960f | 2026-01-13 | tilman.moeller@aiven.io | joelynch112@gmail.com | 1 | 3 | Fix alter order by `alter code` is detached from `create table` code, which makes it necessary to copy field initializaiton logic. This commit makes `alter table` to produce the same `sorting key` ZooKeeper metadata as `create table`. | yes |
| 061 | 285e6fde0f | 2026-01-21 | tilman.moeller@aiven.io | joelynch112@gmail.com | 4 | 131 | Changes for ClickHouse dictionary source, both remote and local. | no |
| 062 | 50a42b769e | 2026-01-29 | tilman.moeller@aiven.io | joelynch112@gmail.com | 5 | 21 | Prohibit .tmp table creation | no |
| 063 | cc718390aa | 2026-02-02 | alex.khatskevich@aiven.io | joelynch112@gmail.com | 1 | 4 | Use .tmp for all fake temporal tables | yes |
| 064 | c95ef0cc6b | 2026-02-05 | alex.khatskevich@aiven.io | joelynch112@gmail.com | 1 | 4 | asd .tmp create or replace | yes |
| 065 | 6fe07b9acd | 2026-01-29 | alex.khatskevich@aiven.io | joelynch112@gmail.com | 2 | 24 | Prohibit redirects from https to http | no |
| 066 | 3866708bab | 2025-12-22 | alex.khatskevich@aiven.io | joelynch112@gmail.com | 4 | 1011 | Fix MV refresh in sharded environment | no |
| 067 | d0a99ce495 | 2026-02-11 | tilman.moeller@aiven.io | joelynch112@gmail.com | 1 | 7 | Allow self signed certificates for local endpoints | yes |
| 068 | fc8f70cca6 | 2026-01-27 | joelynch112@gmail.com | joelynch112@gmail.com | 3 | 255 | Systemd logging | yes |
| 069 | 1502b77d88 | 2026-02-18 | joelynch112@gmail.com | joelynch112@gmail.com | 5 | 22 | Add zk_uptime to Keeper mntr four-letter command | no |
| 070 | d297c19ff8 | 2026-03-16 | alex.khatskevich@aiven.io | joelynch112@gmail.com | 2 | 4 | Disable YTsaurus engine | no |
| 071 | 1b350e6b65 | 2026-03-18 | alex.khatskevich@aiven.io | joelynch112@gmail.com | 4 | 9 | Add REGISTER_YTSAURUS directives | no |
| 072 | 92ab446d49 | 2026-03-26 | alex.khatskevich@aiven.io | joelynch112@gmail.com | 1 | 13 | Wait for distributed database creation | no |
| 073 | aec2378a0e | 2026-04-07 | alex.khatskevich@aiven.io | joelynch112@gmail.com | 1 | 5 | Fix compatibility setting crash on removed setting | yes |
| 074 | ab1fb1df41 | 2025-09-04 | vitlibar@clickhouse.com | joelynch112@gmail.com | 2 | 50 | Fix ArrowFlight support for IPv6 in listen_host. | no |
| 075 | 9650c1a6f5 | 2026-04-23 | alex.khatskevich@aiven.io | joelynch112@gmail.com | 3 | 8 | Add register arrowflight flags | no |
| 076 | 7e09c63352 | 2026-04-29 | joelynch112@gmail.com | joelynch112@gmail.com | 9 | 294 | Add kafka_auto_offset_reset_ms setting for time-based consumer offset reset | no |
| 077 | a25b337024 | 2026-04-30 | joelynch112@gmail.com | joelynch112@gmail.com | 4 | 36 | Hide secrets in system.mutations.command column | no |
| 078 | ea9c5d6420 | 2026-05-07 | alex.khatskevich@aiven.io | joelynch112@gmail.com | 2 | 69 | Fix MV refresh task race condition | no |

## Summary

- Total: 78 patches
- `cherry_pick_clean=yes`: 25
- `cherry_pick_clean=no`: 53
- `cherry_pick_clean=error`: 0
- Authors (the person who originally wrote the patch):
  - `tilman.moeller@aiven.io`: 63
  - `alex.khatskevich@aiven.io`: 10
  - `joelynch112@gmail.com`: 4
  - `vitlibar@clickhouse.com`: 1 (upstream ClickHouse maintainer — patch 074, likely an upstream cherry-pick that may be in 26.3 already)
- Committers (the person who landed the patch on `v25.8.18.1-lts-aiven`,
  post-rebuild):
  - `joelynch112@gmail.com`: 57
  - `alex.khatskevich@aiven.io`: 21
- Date range (author date): 2025-12-02 to 2026-05-07 (~5 months of aiven-side work)

## Pointers

- T2 retrospective: `docs/aiven/uplifts/26.3/01-t2-classifier-retrospective.md`
- Classifier dispatch report (verbatim, scratch): `tmp/classifier/report.md`
- Dispatch prompt (verbatim, scratch): `tmp/classifier/dispatch-prompt.md`
- Plan: `docs/aiven/plans/2026-05-22-classifier-subagent.md`
- Design spec: `docs/aiven/proposals/2026-05-19-uplift-orchestration-design.md` §10 step 6, §14 Q6
- Subagent log: `docs/aiven/uplifts/26.3/log.md`

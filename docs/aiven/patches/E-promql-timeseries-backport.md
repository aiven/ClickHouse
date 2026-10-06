# Effort E (promql-timeseries-backport) — upstream PromQL + `TimeSeries` parity port

> **CATEGORY (E) UPSTREAM FEATURE PORT — not an uplift of an Aiven patch.** This is a
> master-only upstream feature pulled back into our LTS, governed by
> `commit-hygiene.md` §1(E). It has **no** source-index `NNN` and **no** `N<nn>` handle,
> and it takes **one dossier for the whole effort** rather than one per upstream PR.
> Commit subjects: `upstream-port(promql-timeseries/phase<N>): <slug>`.
>
> **Status: implemented and staged, not committed.** The snapshot compiles, links, and evaluates
> PromQL correctly (41/41 probed expressions, up from 8; 49/49 unit tests; 36/49 stateless tests).
> Both blocking decisions in §5 were resolved by a product decision: `TimeSeries` is test-only here,
> so backward compatibility is NOT preserved and upstream's schema is taken verbatim.
> See `docs/aiven/plans/2026-09-17-promql-timeseries-port-record.md` for what was done.

## 0. Lineage

| LTS | Handle | Driver | Outcome |
|---|---|---|---|
| 26.3-aiven | `E-promql-timeseries-backport` | 2026-09-17 | implemented and staged; integration tests and the compliance gate outstanding |

There is no prior-LTS row: this is the first category-(E) effort on this fork. The only
precedent for any upstream-feature backport is three informal commits by Aliaksei
Khatskevich on 2026-08-27 (`ff1bbfc4132`, `4acab30ff60`, `3a6e641596e`), which used
`Backport #<PR> to 26.3: <slug>` and carried no dossier. That form is grandfathered, not
extended — see `commit-hygiene.md` §1(E).

## 1. Purpose

Aiven already runs the `TimeSeries` engine and PromQL in production on this fork. Our
branch point (`aa5df024249`, 2026-03-19) predates upstream's aggregation and
binary-operator work, so PromQL here **cannot evaluate `sum(...)`, `rate(a)/rate(b)`,
comparisons, or set operators at all** — measured, not inferred: 28 of 36 probed
expressions returned `NOT_IMPLEMENTED` on our own binary before the port; all 41 probed
expressions evaluate after it (`docs/aiven/plans/evidence-2026-09-17/promql-capability.txt`).
No real Grafana dashboard is serviceable against that. Upstream closed its PromQL umbrella
issue ([#57545](https://github.com/ClickHouse/ClickHouse/issues/57545)) on 2026-09-15.

Full plan, evidence and phase breakdown:
`docs/aiven/plans/2026-09-17-promql-timeseries-backport.md`.

## 2. Pinned upstream commit

**`290adb52c4ee238c9f3f0b557d6f2ca9bb3b29ff`** — `upstream/master`, 2026-09-17 08:23:47 +0000.

Branch point: `aa5df024249641558bd2e553166246ab017c1a43`, 2026-03-19. 68,106 upstream
commits in between.

Re-pinning invalidates every file-set and line-count figure in the plan and in the ledger.
If you re-pin, redo the snapshot per the port record's §5 and regenerate both.

## 3. Ledger

`docs/aiven/plans/2026-09-17-promql-timeseries-ledger.md` — **184 PRs to port**, from 204
with real net subsystem change (212 candidates), minus 8 incidental and 12 net-cancelled.

This satisfies `commit-hygiene.md` §1(E) batching condition 2: the snapshot loses
per-commit history, and the ledger is what preserves the changelog, review and audit trail.

The originating brief's figure of 171 is **not reachable by any counting method** and is
superseded. Two traps the ledger documents:

- **`#117693` survives at PIN and must be ported** — the `#117693 → #118118 → #118119`
  trio is land → revert → re-revert, so only two of the three net-cancel.
- **`#120336` reverted `#112842`** (under 10 hours later), so `present_over_time`,
  `absent_over_time`, `quantile_over_time` and `predict_linear` are unimplemented **even at
  PIN**. Do not promise them.

## 4. Aiven-local divergence inside the snapshot path set

`commit-hygiene.md` §1(E) batching condition 3. Exactly **two** commits touch the path set,
established by diffing against the pure-upstream mirror `origin/v26.3.33.24-lts-upstream`
(3 files, +70/−6 in-subsystem):

| Commit | Patch | Disposition |
|---|---|---|
| `776a227e3cc` | `patch-new(N03)` — register TimeSeries external target tables as referential dependencies | **DROP — fully subsumed** by upstream `#108388` (+`#115944`). PIN's `DDLDependencyVisitor.cpp:133-149` is byte-identical in mechanism and additionally covers `RecentSamples`. Do not resolve forward: it conflicts textually and would not compile (`Kind::Data`/`Kind::Metrics` are gone). Its functional change lives in `src/Databases/DDLDependencyVisitor.cpp`, **outside** the snapshot path set, so the snapshot does not overwrite it — it must be removed deliberately. Also drop `tests/queries/0_stateless/04410_time_series_referential_dependencies.{sql,reference}`: PIN carries the same test as `04409_…`, and porting both duplicates it. |
| `fc54a00f752` | `patch-new(N04)` — do not dereference a missing TimeSeries metrics target on ATTACH | **RE-APPLY, re-derived.** Its hunk is in `StorageTimeSeries.cpp`, inside the snapshot, so it *will* be overwritten. PIN restructured the area (`getTargetTableID`/`tryGetTargetTableID` at `:339/:344` vs our single `getTargetTableId` at `:234`), so a cherry-pick will not apply — re-derive the guard against PIN's shape. Its integration test lives at `tests/integration/test_aiven_time_series_recovery/`, outside the brief's path set; keep it. |

Neither N03 nor N04 has ever had a dossier (only `N01`/`N02` do). Pre-existing gap, noted
here, not in scope for this effort.

Three further commits in these paths carry `Backport #NNN to 26.3:` subjects (`d264f6c5580`
`#114953`, `a7b3209fad4` `#112614`, `d3a3455bfd5` `#100500`) but are authored by
`robot-clickhouse` and present on the pure-upstream mirrors — **upstream's own LTS
backport-bot commits, not Aiven work.** The snapshot supersedes them.

## 5. Decisions — all resolved

Resolved 2026-09-17: the `contrib/antlr4-grammars*` scoped exception (`AGENTS.md` §3) and
this commit category (`commit-hygiene.md` §1(E), one dossier, tests via `no_justified`).

Phase 0 surfaced two more, and a product decision resolved both: **`TimeSeries` is used only in
test ClickHouse, so backward compatibility is not preserved and upstream's schema is taken
verbatim.** Recorded here because the reasoning matters if that ever stops being true:

1. 🔴 **A straight port silently splits every existing Aiven series.** Upstream `#114300`
   changed series-identifier computation with **no backward-compatibility path**, while
   `MIN_WRITABLE = 0` keeps the server writing to old tables. Post-upgrade, a live series
   gets a second tags row under a new id while its history stays under the old one; a
   PromQL query spanning the boundary sees two series with an identical labelset. No
   in-tree setting reproduces the old identifiers. Upstream's own migration test cannot
   catch it. Plan §4.1 has the three-fact proof and four options.
2. **`ViewTarget::Kind` rename** (`Data`→`Samples`, `Metrics`→`MetricFamilies`, new
   `RecentSamples`) — 14 of 49 spike TUs, and not mechanical, because 26.3 already ships
   `DATA`/`METRICS` DDL. Plan §4.2.

Consequences of the decision: no version-0 compatibility shim, no Aiven variant of
`test_upgrade_from_prealpha.py`, and **no production `SHOW CREATE TABLE` needed**. If Aiven ever
puts `TimeSeries` in front of customer data, §5.1 becomes live again and must be re-opened before
any upgrade.

## 6. Phase 0 evidence

Reproduce per the port record's §5.

- Overlay: **220 files, +25,015/−5,832** (261-file subsystem from PIN, 17-file prerequisite
  closure, 8 deletions).
- `cmake -B build` clean; `ninja -k 0 clickhouse` → **482 steps, 49 failed targets**.
- **Zero missing headers. Zero missing error codes. Zero missing settings vocabulary.**
  The include graph closes at depth 2, and no new contrib submodule is needed. This is what
  makes the snapshot viable.
- Top blocker is a **~4-line ODR fix** (`VectorWithMemoryTracking` alias) clearing 17 TUs.
- The two "highest-risk repo-wide prerequisites" (`#102505`, `#102644`) are **confirmed but
  small** — ~10 of 49 TUs. **Do not front-load them**, contra the originating brief;
  `#102505` reduces to 7 call-site edits and `#102644` is hard-required by one file.
- Caveat: the spike **never reached linking**, so link-time gaps are unmeasured.

Full classification: `docs/aiven/plans/2026-09-17-promql-timeseries-spike-error-surface.md`.

## 7. Verification plan

Per `commit-hygiene.md` §1(E), the per-PR evidence pair is discharged by the ported
upstream tests (`tests.added: no_justified`). That relaxation explicitly does **not** cover
Aiven-specific behavior, so this effort still owes three tests of its own:

1. **Migration acceptance gate** — an Aiven variant of `test_upgrade_from_prealpha.py`
   pinned to a real production DDL, asserting a version-0 table attaches, keeps the
   `time_series` column name and `METRICS` target, stays writable, and answers PromQL. Plus
   the assertion upstream lacks: **re-insert the same series post-upgrade and assert
   `uniq(id) = 1`.** Against a straight port this **will fail** — that is §5.1, and it is
   the gate. If it fails, the port does not ship regardless of compliance score.
2. **A real compliance gate.** `test_promql_compliance` has **no assertion** — 0% exits
   green, failing queries are printed then discarded, and the baseline is S3-only with
   `should_publish_master_baseline` hard-coded to `ClickHouse/ClickHouse` + `master`, so a
   fork can never publish one. Convert it to an in-tree per-query baseline. Assert on the
   per-query set, never the aggregate percentage.
3. **N04 coverage** — the existing `test_aiven_time_series_recovery` re-derived against
   PIN's restructured `StorageTimeSeries`.

`tests.added` for the effort as a whole: **`no_justified`** for the ported upstream
surface, **`yes`** for the three above.

## 8. Known next-LTS rebase conflict

Per the `AGENTS.md` §3 exception's terms: `contrib/antlr4-grammars/promql/**` and the
PromQL files under `contrib/antlr4-grammars-cmake/generated/**` will carry a **10-file,
+948/−748** delta taken verbatim from the pinned commit. Whoever runs the next LTS rebase
should expect it and resolve in favour of the newer upstream grammar.

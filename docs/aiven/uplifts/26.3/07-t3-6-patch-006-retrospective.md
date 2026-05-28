# T3.6 — patch 006 dispatch retrospective

> **What this is:** The reflection-after-cost step (spec §10 step 12) applied to the sixth real patch dispatch — the first to (a) escalate `test_design_blocked` AND get resolved by **building the missing infrastructure** rather than by schema expansion (T3.5's path) or test redesign, (b) introduce the `test_aiven_<slug>/` integration-test naming convention and VERIFY it at first use, (c) produce a runbook for local integration testing (`docs/aiven/runbooks/integration-tests.md`) authored from the in-session bring-up evidence, (d) honor T3.5's "command literals first" mitigation on the very next dispatch, and (e) close two open Bootstrap-class concerns from T3.5 (Findings B/C/G on commit-message body preservation) with empirical "did not recur" evidence.
> **Date:** 2026-05-26.
> **Dispatch reference:** `docs/aiven/uplifts/26.3/log.md` row at `2026-05-26T12:08:46Z`; subagent id `toolu_01Hr5eGvi4VS8h2bRy6ahWu4`; worker transcript at `agent-transcripts/f7573554-163e-4a8a-af35-1ff9ee1f4cf1/subagents/<id>.jsonl` (not separately archived per T3.5 Finding D's new convention).
> **Outcome of T3.6:** `escalate` (worker) → human + parent decision to build integration-test infrastructure → integration test authored + verified pre/post → patch landed. Two commits in dependency order: `3298e97ae39` (Bootstrap; integration-tests runbook + `test_aiven_<slug>/` naming convention) precedes `fde4e568a51` (patch-port; src + dossier + integration test). Bootstrap-before-patch-port ordering achieved this time — see Finding D for the contrast with T3.5 Finding G.

## Headline

T3.6 was the first dispatch where a worker's `test_design_blocked` escalation got resolved NOT by changing the schema (T3.5's path) and NOT by re-dispatching with a different test design — but by **the parent + human building the missing capability in-session** (a local integration-test pipeline that had not previously existed in this uplift system) and then authoring the test under that new capability. The worker's analysis was correct; the missing piece was infrastructure, not judgment.

The dispatch took 9 minutes worker-side; the in-session integration-test follow-up took ~85 minutes (~40 min infrastructure investigation including reading docs, identifying the four landmines, and producing `tmp/integration-smoke/findings.md`; ~45 min authoring the test, pre/post verification with two incremental rebuilds, and the runbook + dossier revision). Net per-patch effort: ~95 minutes — comparable to T3.4's ~90 minutes but with a permanent capability gain (the runbook unlocks future integration tests in ~10 minutes each).

The first VERIFIED use of the `test_aiven_<slug>/` integration-test naming convention landed in the same commit as the test itself. T3.5's "command literals first" mitigation (Findings B/C/G) was honored on the very next dispatch — both commits' bodies carry the prepared messages verbatim; the `Original author:` line is present.

## What worked in T3.6

1. **Worker's escalation was honest and well-documented.** Worker tried three SQL-trigger candidates (`DETACH/ATTACH`, `DETACH PERMANENTLY/ATTACH`, `SYSTEM RESTART REPLICA`), recorded WHY each failed to produce a pre/post evidence pair (DDL log propagation makes both nodes execute as `SECONDARY_QUERY`, the global `<shard>` macro in `tests/config/config.d/macros.xml` masks the trigger, single-node setup can't generate the `SECONDARY_QUERY` execution from a DDL log), and escalated with `escalation_reason: test_design_blocked`. Without this engineering rigor in the escalation, the right follow-up (integration test) wouldn't have been obvious. The dossier §4's pre-revision text (now in commit history) is a model of "honest failure mode" — three failed approaches, explicit reasoning, recommended human follow-up.
2. **Stop-point dialog (α/β/γ) worked as scope-control discipline.** The human's "Find the optimal stopping point if exist" instruction gave the parent license to spend ~40 minutes on infrastructure investigation without scope creep, then offer escalating commitment levels (Stop α: stop now; Stop β: small reversible step; Stop γ: full session investment). Three stop-points are enough granularity without choice overload. The user picked Stop β; the smoke ran; the user then escalated to "see the patch integration test first" which mapped cleanly to Stop γ-without-system-changes. The α/β/γ vocabulary will recur in future dispatches (the format is reusable).
3. **The integration-test pipeline came up much faster than the praktika docs suggest.** `tests/integration/README.md` recommends the full `clickhouse/integration-tests-runner:<hash>` image (~7 GB, DinD, sudoers entry for `iptables`, sudo for `dmesg`). Bypassing praktika and calling pytest directly required exactly 8 user-local pip packages (~80 MB), one `ci/tmp/*` cleanup snippet, and four `CLICKHOUSE_TESTS_*` env-vars. Total bring-up: ~3 minutes. The runbook now captures this as the VERIFIED path; the praktika docs document the *CI-canonical* path; both are correct for their audience.
4. **The first pre/post evidence pair was reproducible in ~3 minutes wall-clock.** One post-patch run (~42 s) + one incremental rebuild (~39 s) for pre-patch + one pre-patch run (~99 s; longer because it has to wait for `restart_clickhouse` to time out) + one incremental rebuild (~32 s) + one sanity post-patch recheck (~44 s) = ~256 s of pytest time + ~71 s of rebuild = ~327 s total = 5.5 minutes including build. This is now the VERIFIED upper bound for restart-class integration test pre/post verification.
5. **Two-commit dependency-order worked perfectly.** Bootstrap commit `3298e97ae39` precedes patch-port commit `fde4e568a51`. The patch-port commit's dossier §4 references the runbook (`docs/aiven/runbooks/integration-tests.md`) introduced in the Bootstrap. No mixed-category debt. This is what T3.5 Finding G said *should* happen; T3.6 is the proof. Contributing factors: the parent's hand-off message numbered the commits as `1. git commit -F ...A.txt` then `2. git add ... && git commit -F ...B.txt` (per T3.5 Finding G's mitigation), which prevented ordering confusion.
6. **"Command literals first" mitigation held.** Hand-off message put `git commit -F tmp/patch-006/commit-A-bootstrap-message.txt` on line 1 of an explicit two-command sequence. Both commits' bodies carry the prepared messages verbatim. The patch-port commit's `Original author:`, `Co-author:`, and `Committer (25.8 carry):` lines are all present. T3.5 Findings B/C are BLOCKED from recurrence (until the parent breaks the habit) — empirical evidence on the very next dispatch.
7. **The infrastructure investigation produced a durable artifact.** `tmp/integration-smoke/findings.md` (240 lines) was authored as the investigation progressed — not after the fact. The three Stop options (α / β / γ) were drafted *in the artifact* before being presented to the human. The runbook `docs/aiven/runbooks/integration-tests.md` (192 lines) is a derivative of `findings.md` trimmed to steady-state. The investigation methodology — "read docs first, probe specifically, identify EXACTLY where the wall is, document for future" — converted ~40 minutes of exploratory work into a permanent capability with three artifacts (findings.md, runbook, the smoke test working).

## What surfaced

### A. The "build-the-missing-infrastructure" workflow primitive

**Symptom:** worker escalated `test_design_blocked` with the recommendation: "(preferred) Author an **integration test** under `tests/integration/test_replicated_database_attach_with_shard_macro/` that starts a server with a custom config omitting `<shard>`, creates the `DatabaseReplicated` ... and asserts the tables come back up cleanly. This contradicts parent policy call #2 (`.sql` only) and so requires a parent decision." The escalation was correct; the integration-test infrastructure to act on the recommendation did not exist in the uplift system at that moment.

**Diagnosis:** the LTS uplift system at T3.6 had documented integration tests in `docs/aiven/runbooks/testing-suites.md` §1 as "the suite for multi-process / restart scenarios" but had never RUN one. The praktika `--no-docker` path, the user-local pip set, the `ci/tmp/` cleanup discipline, and the `test_aiven_<slug>/` naming convention all needed to come into existence before the test could be authored. The worker, by spec, cannot build cross-dispatch infrastructure (out of scope). A separate worker dispatch for "build the integration-test infrastructure" would have lost the just-loaded context of patch 006's specific trigger and would have produced a less-targeted runbook. The **parent + human, in-session, working as peers**, was the right vehicle: the human approved each escalating commitment level (`Stop α/β/γ`), the parent investigated and produced the artifacts, and both jointly committed.

**Decision:** name this primitive in the runbook. The workflow shape is now: worker escalation → parent + human decide between (i) accept the escalation as-is (T3.5: schema expansion), (ii) re-dispatch with a different test design (no precedent yet), (iii) **build the missing capability in-session** (T3.6: this dispatch). Each path has different cost profiles and produces different durable artifacts. Add a "Workflow primitives" section to `docs/aiven/runbooks/commit-hygiene.md` documenting the three options.

**Rule-of-three count: 1 of 3** for the "build-the-missing-capability" path. Single occurrence. Defer formalizing the workflow primitive in a Bootstrap commit until a second occurrence validates the pattern isn't a one-off.

### B. Error-code prediction wrong (Code: 62 → Code: 139)

**Symptom:** dossier §4's pre-revision text (worker authored, parent reviewed pre-dispatch) predicted the pre-patch failure would be `Code: 62. DB::Exception: No macro 'shard'`. The empirically-observed error from the pre-patch integration test is `Code: 139. DB::Exception: No macro 'shard' in config while processing substitutions in '/clickhouse/tables/{uuid}/{shard}' at '27' or macro is not supported here ... (NO_ELEMENTS_IN_CONFIG)`. Same root cause (`Macros::expand` cannot find a `shard` macro entry); different code path throws.

**Diagnosis:** `Code: 62` is `SYNTAX_ERROR`; `Code: 139` is `NO_ELEMENTS_IN_CONFIG`. The worker and parent both projected the failure mode from `Common/Macros.cpp`'s "missing macro" path without grepping for the *specific* throw site that would fire for the `{shard}` substitution on the ATTACH-from-loader code path. The actual throw is at a different line in `Macros.cpp` (the one that explicitly says "or macro is not supported here") and uses a different error code. The predictions were anchored on the *symbol* ("No macro 'shard'") rather than on the *code value*.

**Decision:** add a parent + worker discipline: when predicting a failure mode in the dispatch-prompt or dossier draft, assert on ROOT CAUSE (a substring of the message, like `No macro 'shard'`) rather than on a specific `Code: <N>` value. The error-code is brittle (multiple throw sites can share the cause); the message substring is durable (it's the failure semantics, not the implementation). Add to `docs/aiven/skills/cpp-review-checklist.md` as a §9c "Prediction vs ground truth": predictions are evidence-quality only when they survive the actual run.

**Rule-of-three count: 1 of 3.** First explicit prediction-mismatch in the T3.x series. Earlier dispatches predicted failure modes (e.g., T3.4's "post-patch SELECT pair returns `1\n0` instead of `0\n1`") but those predictions held. Defer the cpp-review-checklist §9c addition to a Bootstrap commit unless a second prediction-mismatch occurs.

### C. The integration-test infrastructure investigation produced more value than the patch port itself

**Symptom:** the patch port (commit `fde4e568a51`) is a 1-line source change. The Bootstrap commit (`3298e97ae39`) is 226 lines of runbook + naming convention. The retrospective for T3.6 emphasizes the infrastructure more than the patch.

**Diagnosis:** for ~90% of the 71 remaining patches in the inventory, the relevant test mode IS stateless or no-test-justified. For the other ~10% (restart-class, multi-node DDL, ZK-restart, server-startup loader bugs — patches 005, 006, 008, 009, 010, possibly others), the integration-test capability is load-bearing. T3.6 paid the bring-up cost (~85 minutes including the integration test itself) so future patches in this class pay only ~10-15 minutes (the runbook makes the setup trivial).

**Decision:** treat the runbook + naming convention as a **leverage multiplier**, not a one-off. The next integration-test patch should be ~10 minutes of setup + N minutes of test design. If the next integration-test patch *also* takes ~85 minutes, the runbook is incomplete (deficiency) and needs amendment. The runbook's `Status: VERIFIED 2026-05-26` claim will be re-verified on the second use.

**Rule-of-three count: n/a.** This is an observation about cost profile, not a recurring failure mode. Recorded for future reference only.

### D. Bootstrap-before-patch-port ordering achieved (T3.5 Finding G resolved at next occurrence)

**Symptom:** the Bootstrap commit `3298e97ae39` (committer time 2026-05-26T15:21Z) precedes the patch-port commit `fde4e568a51` (committer time 2026-05-26T15:22Z). The patch-port's dossier §4 references `docs/aiven/runbooks/testing-suites.md` §4.4 (introduced in the Bootstrap) and `docs/aiven/runbooks/integration-tests.md` (introduced in the Bootstrap). Anyone bisecting through the patch-port checks out a state where all references resolve.

**Diagnosis:** the parent's hand-off message presented the two commits as a numbered sequence with command literals on line 1 of each step:

```
git commit -F tmp/patch-006/commit-A-bootstrap-message.txt

git add src/Storages/TableZnodeInfo.cpp \
        docs/aiven/patches/006-...md \
        docs/aiven/uplifts/26.3/log.md \
        tests/integration/test_aiven_replicated_database_attach_with_shard_macro/

git commit -F tmp/patch-006/commit-B-patch-port-message.txt
```

The first command stages only the Bootstrap files (which were pre-staged). The second command stages the patch-port files. The human executed both verbatim; the resulting history is correct.

**Decision:** confirm T3.5 Finding G's mitigation works. The "command literals first, prose second" pattern combined with explicit numbered command sequences (1. ... 2. ...) prevents the prose-skim failure mode. No further action.

**Rule-of-three count: 1 of 3** for the *successful* application (T3.5 Finding G was the 1 of 3 for the *failure*; this is the 1 of 3 for the working mitigation). Track recurrence: if T3.7 also produces a Bootstrap+patch-port pair in correct order, the mitigation is empirically proven (3 of 3 needed). If T3.7 reverts to the wrong order, the mitigation is brittle and needs strengthening.

### E. `Original author:` line preserved (T3.5 Finding B's mitigation works on first try)

**Symptom:** patch-port commit `fde4e568a51`'s body contains:

```
Original author: Tilman Moeller <tilman.moeller@aiven.io>, 2025-12-07.
Co-author: Kevin Michel <kevin.michel@aiven.io>.
Committer (25.8 carry): Aliaksei Khatskevich, 2026-03-12.
```

All three lines are present. The committed body matches the prepared `tmp/patch-006/commit-B-patch-port-message.txt` verbatim (modulo trailing newlines).

**Diagnosis:** the hand-off message put `git commit -F tmp/patch-006/commit-B-patch-port-message.txt` on line 1 (per T3.5 Finding G's mitigation). The human ran the exact command. `git commit -F <file>` always uses the file as the entire message body; no other code path is possible.

**Decision:** the "command literals first" mitigation closes the body-line-preservation failure for this case. No further action; track the rule-of-three counter (Finding D above) to confirm durability.

**Rule-of-three count: 1 of 3** for the successful preservation. T3.4's Finding C (lost Joe Lynch) was the 1 of 3 for the *failure* of `--author=` flag; T3.5's Finding B was the 2 of 3 for the failure of `Original author:` body line. T3.6's Finding E is the 1 of 3 for the *working* mitigation (the prepared-file pattern with command-literal-first hand-off). Same tracking semantics as Finding D.

### F. `test_aiven_<slug>/` integration-test naming convention introduced AND VERIFIED in same dispatch

**Symptom:** `docs/aiven/runbooks/testing-suites.md` §4.4 (the integration-test naming convention) landed in the Bootstrap commit; `tests/integration/test_aiven_replicated_database_attach_with_shard_macro/` (the first directory exercising the convention) landed in the same series in the patch-port commit. The convention's `Status: VERIFIED 2026-05-26` claim cites the patch-port commit as the verification evidence.

**Diagnosis:** this is the runbook lifecycle working as designed. Bootstrap-class additions land with a `PROVISIONAL` or `VERIFIED <date>` status; the first real use is the verification event; the runbook is the recording. Compare to §4.1 (the stateless `9<NNN>_<slug>` convention), which was introduced provisionally at T3.4 and verified at T3.4 with `9077_*`. T3.6 does the same for integration tests in one dispatch (vs. T3.4's split between proposal and verification).

**Decision:** no action. Note that single-dispatch introduce-and-verify is cleaner than split-dispatch — when the convention proposal AND the first use happen in the same session, the convention can incorporate the first-use evidence verbatim. Future conventions should follow this pattern if possible.

**Rule-of-three count: n/a.** Observation, not failure mode.

### G. Hook regression — third occurrence (rule-of-three exhausted; investigated and resolved 2026-05-26)

**Symptom:** `log.md` row for T3.6 (`2026-05-26T12:08:46Z`, subagent `toolu_01Hr5eGvi4VS8h2bRy6ahWu4`) was auto-written by the `subagentStop` hook with `slug=unknown / outcome=unknown / report=n/a`. Per T3.5 Finding D, this is the THIRD consecutive occurrence (T3.4 = 1, T3.5 = 2, T3.6 = 3). The originally-recorded `unknown / unknown` row was manually backfilled by the parent agent earlier in this session (see Finding E above), which masked the regression until the investigation re-exposed the on-disk state.

**Diagnosis (investigated 2026-05-26 with seven synthetic probes against an instrumented hook):**

The actual root cause was **not** any of the three hypotheses originally listed in T3.5 Finding D. Cursor's hook JSON input shape silently changed under us: as of 2026-05-26 for both `explore` and `general-purpose` subagents, the JSON contains:

- `.agent_transcript_path = null` — used to point to the subagent's JSONL.
- `.transcript_path` = the PARENT's JSONL (wrong for our needs).
- `.summary = null` — prose-only fallback no longer populated either.

The subagent's own JSONL still exists at `dirname(.transcript_path)/subagents/<uuid>.jsonl` where `<uuid>` is Cursor-internal and bears no relation to `.subagent_id` (so the old direct-lookup-by-id approach is also unavailable).

A secondary issue surfaced during verification: workers don't always put the YAML report in the LAST assistant turn. A second-to-last text turn carrying the YAML, followed by a wrap-up prose turn, would have caused the previous `[…] | last | …` jq filter to miss the YAML even with the path resolution fixed.

**Decision (landed in commit `<bootstrap-hook-fix-2026-05-26>`):**

Two-part fix in `.cursor/hooks/log-subagent-completion.sh`:

1. **Path resolution fallback.** When `.agent_transcript_path` is null, derive the subagents directory from `dirname(.transcript_path)` and pick the most-recently-modified `*.jsonl`. The just-finished subagent's transcript is the youngest by sub-second margin; race window with parallel subagents is tight (worst case: one wrong row body, never an exception). The fix is forward-compatible — if Cursor restores `.agent_transcript_path`, the preferred branch wins.
2. **All-turn concatenation.** The jq filter now concatenates text content from EVERY assistant turn, joined by blank lines, rather than only the last turn. The column-0 awk anchors (`^outcome:`, `^patch_slug:`, `^escalation_reason:`) are specific enough that joining doesn't introduce false matches from unrelated prose.

The fix was verified end-to-end with two synthetic dispatches (`hook-fix-verify-2026-05-26` with YAML in the middle + trailing prose; `hook-fix-verify-shape2` with YAML at the end). Both populated all eight columns of `log.md` correctly and archived the verbatim transcript body to `docs/aiven/uplifts/26.3/reports/<subagent_id>.md`. The synthetic rows and archives were deleted before the commit; the investigation history lives in this finding and the commit message.

**Forward signpost left in the hook:** a comment at the end of the script documents the three preferred sources of the transcript body and how to re-instrument if Cursor's JSON shape changes again. A probe block is reachable via this file's git history (the commit that introduced the signpost).

**Rule-of-three count: 3 of 3 → RESOLVED.** No further deferral. Next observation: confirm the next real patch dispatch (T3.7) populates its row correctly without manual backfill.

### H. The integration-test infrastructure is local-only; CI runs against the praktika path

**Symptom:** the runbook `docs/aiven/runbooks/integration-tests.md` §1 explicitly says "This runbook is the _local_ override". CI (when patch 006 reaches it) will run via `python -m ci.praktika run integration --test test_aiven_replicated_database_attach_with_shard_macro` with the full DinD wrapper, all 109 pip requirements, and the `sudo iptables` cleanup. We haven't verified that the CI path also passes for this test.

**Diagnosis:** the local and CI paths are equivalent in INTENT (run the same pytest module) but different in MECHANISM (local: pytest direct, 8 packages, no DinD; CI: praktika wrapper, 109 packages, DinD, sudoers entries). The local path being green is necessary but not sufficient evidence that CI will pass. Specifically, the local path skips: parallel batching (CI: `-n 2 --dist=loadfile`), the dmesg OOM check, the iptables cleanup, and runs from `pytest` (CI's `Result.from_pytest_run` wrapper does additional book-keeping).

**Decision:** when the patch 006 commit reaches CI, the integration-test job is the canonical check. If it fails there but passes locally, the runbook is incomplete (deficiency) and needs a "local-vs-CI gap" section. Defer this concern until the CI job actually runs.

**Rule-of-three count: n/a.** Observation, not failure mode.

## Concrete decisions for T3.7+

| Decision | Driver | Action |
|---|---|---|
| **Hook investigation MANDATORY before T3.7 dispatch.** ✓ DONE in commit `<bootstrap-hook-fix-2026-05-26>` (this session). Root cause: Cursor changed the JSON shape so `.agent_transcript_path` is null. Fix: derive `subagents/<youngest>.jsonl` from `dirname(.transcript_path)`; concatenate all assistant turns (not just last). Verified end-to-end with synthetic probes; next observation is whether T3.7's row populates without manual backfill. | Finding G (3 of 3 → resolved) | Done. Watch T3.7 for confirmation; if T3.7 row still comes in `unknown / unknown`, escalate with another probe round. |
| **"Build the missing capability in-session" is a workflow primitive.** Document it in `docs/aiven/runbooks/commit-hygiene.md` as one of three possible responses to `test_design_blocked` escalation (alongside schema expansion and re-dispatch). Include the α/β/γ stop-point format as the discipline for in-session investigations. | Finding A | Defer to T3.7 Bootstrap commit unless a second "build in-session" occurrence happens before then. |
| **Prediction discipline: assert on root cause, not error code.** Add `docs/aiven/skills/cpp-review-checklist.md` §9c "Prediction vs ground truth": predictions about failure modes in dispatch prompts and dossier drafts MUST assert on message substring (durable) rather than `Code: <N>` value (brittle). | Finding B | Defer to first occurrence-2 of error-code prediction mismatch. Single occurrence today. |
| **The runbook needs second-use verification.** Track whether the next integration-test patch takes ~10-15 minutes of setup (proves the runbook works) or ~80+ minutes (proves the runbook is incomplete). | Finding C | No code change; track on T3.7 if T3.7 involves integration tests. |
| **The "command literals first" mitigation works** — keep using it. Numbered command sequences (1. ... 2. ...) prevent ordering confusion. Track rule-of-three for the *working* mitigation across T3.7 and T3.8. | Findings D + E | Mental discipline; no code change. |
| **First VERIFIED use of a runbook section is the verification event.** Future Bootstrap commits should aim to land the proposal AND the first use together, the way T3.6 did for `test_aiven_<slug>/`. | Finding F | Pattern; no code change. |
| **CI verification of patch 006 is the canonical confirmation.** Local pass is necessary but not sufficient. | Finding H | When patch 006 reaches CI: confirm the integration-test job passes. If not, address the local-vs-CI gap in a follow-up Bootstrap commit. |

## What this retrospective is NOT

- Not a re-litigation of patch 006's design — that's in `docs/aiven/patches/006-replicated-database-attach-with-shard-macro.md`.
- Not the hook fix. Finding G mandates the fix; it lands as a separate Bootstrap commit before T3.7 (not inside this retrospective).
- Not the runbook itself. The runbook (`docs/aiven/runbooks/integration-tests.md`) shipped in commit `3298e97ae39`. This retrospective references it but does not duplicate it.
- Not a fix for the `Code: 62` vs `Code: 139` discrepancy in past commits — the patch-006 commit's body and the patch-006 dossier §4 both record the calibration. No amend per AGENTS.md.
- Not the next patch's planning. T3.7 patch selection happens in a separate session opener.

## Pointers

- Dossier: `docs/aiven/patches/006-replicated-database-attach-with-shard-macro.md` (298 lines; §4 documents the integration test and the Code: 139 calibration; §6 documents the in-session integration-test follow-up time-cost).
- T3.6 patch-port commit: `fde4e568a51` "Port patch 006: replicated-database-attach-with-shard-macro".
- T3.6 Bootstrap commit: `3298e97ae39` "docs(aiven): runbook for local integration tests + test_aiven_<slug>/ naming".
- Integration-tests runbook: `docs/aiven/runbooks/integration-tests.md` (192 lines; all sections `Status: VERIFIED 2026-05-26`).
- Aiven integration-test naming convention: `docs/aiven/runbooks/testing-suites.md` §4.4 (33 lines; introduced in Bootstrap commit, VERIFIED at first use in the patch-port commit).
- Integration-test infrastructure findings (in-session investigation artifact): `tmp/integration-smoke/findings.md` (240 lines; the durable record of the four landmines hit during bring-up, the three Stop options, and the eight-package minimal pip set).
- Pre/post evidence pair logs: `tmp/integration-smoke/patch006-{postpatch,prepatch,postpatch-3}.log` (~270 KB total; `prepatch.log` contains the verbatim `Code: 139` pre-patch failure mode).
- T3.6 worker scratch logs: `tmp/patch-006/*.log` (build-pre/post and dispatch prompt; ~110 KB).
- Parent-prepared commit messages (USED at commit time — Finding E confirms): `tmp/patch-006/commit-A-bootstrap-message.txt`, `tmp/patch-006/commit-B-patch-port-message.txt`.
- T3.5 retrospective (predecessor; contains the "command literals first" / "Original author preserved" mitigations T3.6 honored): `docs/aiven/uplifts/26.3/06-t3-5-patch-073-retrospective.md`.
- T2.2 inventory (source row): `docs/aiven/uplifts/26.3/inventory.md` row 006 (`22e03c9d9d6cf9929aec824b724e09ea5c58653f`).

## Mentor lesson (per the C++ Architect rule)

**Intuition.** A retrospective is a Bloom filter against future regret. You spend ~15-20 minutes capturing what you'd otherwise re-learn the hard way next time. The cost is fixed; the savings compound across all future dispatches that would have hit the same wall.

**Mechanism.** The 7-section structure (preamble, headline, what-worked, what-surfaced, decisions, what-it-is-not, pointers) plus the 4-element finding nucleus (symptom, diagnosis, decision, rule-of-three) forces every finding to be *actionable*. A finding without a decision is a feeling. A decision without a driver is taste. A driver without a symptom is speculation. The structure cuts down on each.

**System consequence.** The schema, skills, runbooks, dispatch-prompt template, and hooks all evolve through retrospectives. Without retrospectives, the system stays at its initial design quality forever. With them, the system improves at every dispatch — at the cost of ~15 minutes per dispatch.

**Today you learned.** Two new workflow primitives: (a) `test_design_blocked` escalations can be resolved by **building the missing capability in-session**, not just by schema expansion or re-design (Finding A); (b) introducing a runbook section + the first use of that section in the same dispatch is cleaner than splitting them (Finding F).

**Rule of thumb.** When a worker escalates with engineering rigor (recommended follow-up, explicit failure modes for each rejected alternative), ask "is the missing piece judgment or capability?" — and if capability, the parent + human can build it in-session under the α/β/γ stop-point discipline.

**Next rabbit hole.** The hook regression (Finding G) is now mandatory. The three hypotheses are testable in ~15 minutes total with a sleep + debug-log instrumentation. Worth doing in the very next session before T3.7 dispatches.

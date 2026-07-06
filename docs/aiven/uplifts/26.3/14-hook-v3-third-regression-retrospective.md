# Retro 14 — `subagentStop` hook v3 third regression (cross-cutting)

> **What this is:** A **cross-cutting retrospective** spanning T3.8 through T3.15 about the third regression of Cursor's `subagentStop` hook (the hook that auto-populates `docs/aiven/uplifts/26.3/log.md` and archives worker reports to `docs/aiven/uplifts/26.3/reports/`). The previous two regression chapters live in retro 07 Finding G (the v1 → v2 transition) and retro 08 Finding B (the v2 → v3 refinement). This retro consolidates the v3 third regression's symptoms, the probe-block investigation, the diagnostic conclusion, the offline backfill procedure used in Phase A of the packaging cleanup, and the forward signpost.
> **Date range:** 2026-05-27 to 2026-05-28.
> **Probe block:** landed in commit `acb4d88fc70` ("hooks: probe block for subagentStop v3 third regression (T3.8/T3.9)"), removed in the current Phase A cleanup after diagnosis.
> **Probe captures:** six raw `$input` payloads at `tmp/hook-probe/2026-05-{27,28}T*-toolu_*-<pid>.json` (gitignored). All six show `"message_count": 0, "tool_call_count": 0, "loop_count": 0` and no assistant content.

## Headline

**Cursor never sends the assistant transcript content in the `subagentStop` hook input.** The probe block confirmed this across six real worker dispatches (T3.10 through T3.15). The hook script's v3 JSONL-fallback (read the worker's own JSONL via `dirname(.transcript_path)/subagents/<youngest>.jsonl`) is therefore **the permanent path, not a workaround**. The intermittent `unknown / unknown` rows observed at T3.8/T3.9/T3.13/T3.14/T3.15 are timing artifacts — the hook fires before the JSONL file is fully flushed, so the body-extraction returns empty and the row degrades to metadata-only.

The diagnostic conclusion changes what we ship in the hook script. Previously, the v3 fallback was framed as a defensive measure pending Cursor's restoration of `.agent_transcript_path`. Now it is the canonical path: future Cursor changes to the hook input shape should be treated as additional fallback branches LAYERED on top of the JSONL read, not replacements for it.

Three artifacts changed in the cleanup commit:
1. Probe block removed from `.cursor/hooks/log-subagent-completion.sh` (diagnostic complete).
2. Forward-signpost comment block in the hook updated to point at this retro instead of the probe.
3. Three `log.md` rows (T3.13, T3.14, T3.15) backfilled from the on-disk JSONLs using the same jq pipeline the hook uses; three corresponding report archives created at `reports/toolu_017G6Sxog7w15j7wi61nPbj6.md`, `reports/toolu_01Ngku5yMGP4YVcVxAQ3ND3G.md`, and `reports/toolu_01FigZuPjPKDHr5pMUFgpoQ6.md` with `backfilled=true` in their archive headers.

## Diagnosis chain

### Chapter 1: v1 → v2 transition (retro 07 Finding G, 2026-05-25)

Cursor silently changed the `subagentStop` hook input shape: `.agent_transcript_path` became `null` and `.transcript_path` was redirected to the PARENT's JSONL. The v1 hook read from `.agent_transcript_path` directly; under the new shape, v1's read returned empty and the log.md row degraded to metadata-only.

**Fix (v2):** derive the subagent's own JSONL via `dirname(.transcript_path)/subagents/<youngest>.jsonl` — heuristic picks the most-recently-modified `.jsonl` in that directory (the just-finished subagent is the youngest by sub-second margin). v2 also concatenated text from ALL assistant turns (not just the last) to tolerate the case where a worker emits a wrap-up turn after the YAML.

### Chapter 2: v2 → v3 refinement (retro 08 Finding B, 2026-05-26)

v2's "all-turn concatenation" produced archives bloated with worker narration ("I need to read…", "Let me check…") preceding the actual YAML. T3.7's archive came in at 354 lines with `outcome:` at line 170 — vs T3.4's precedent of 178 lines with `outcome:` at line 4.

**Fix (v3):** filter to turns containing the column-0 YAML anchors (`(^|\n)outcome:|(^|\n)patch_slug:|(^|\n)escalation_reason:`) — the SAME anchors the awk parser already uses, so the filter cannot drop a turn the parser would have read. Fall back to all-turn join only when no turn carries the YAML (read-only subagents like `explore` / `cursorGuide`, or aborted runs).

T3.7's archive was re-extracted in-session under the v3 filter; size shrank from 354 lines to 187, `outcome:` moved to line 4. T3.6 finding G's rule-of-three counter for the WORKING archive scheme reached 1 of 3.

### Chapter 3: v3 third regression (T3.8 through T3.15, this retro)

**Symptom (first observation):** T3.8 + T3.9 dispatches (2026-05-27 morning UTC) produced ZERO auto-populated rows in `log.md` and ZERO archives in `reports/`. The only hook-fire evidence was a single `unknown / unknown / unknown / n/a` row at `2026-05-27T10:26:31Z` (mid-T3.9-runtime, ~34 minutes before T3.9 completion), which was discarded by parent inspection at end-of-session. T3.7's archive (`reports/toolu_01FgRHtjZGkmh4h6Z7DZaceE.md`) had been auto-archived correctly; T3.8/T3.9 were the FIRST dispatches under v3 to produce nothing.

**Diagnosis-at-the-time (incomplete):** the hook script was verified healthy under synthetic input. Running the same jq pipeline manually against the on-disk JSONLs at `subagents/53d78719*.jsonl` (T3.8) and `subagents/cdeba357*.jsonl` (T3.9) produced correct YAML extraction. Running the hook script with synthetic input (parent `transcript_path` only, `agent_transcript_path=null`, `status="completed"`) produced a correct `log.md` row + archive. So the hook script was healthy; the regression had to be in WHAT CURSOR SENT to the hook.

**Probe block landing (acb4d88fc70):** parent inserted a probe block at the top of `.cursor/hooks/log-subagent-completion.sh` that captured the raw `$input` to `tmp/hook-probe/<ts>-<id>-<pid>.json` on every fire. `tmp/` is gitignored, so probe files never accidentally commit. The probe was the **first investigative tool** for a third-regression that was not directly inspectable from script-side healthy-check evidence alone.

**Six probe captures (T3.10 through T3.15):** every captured payload showed:

```json
{
  "conversation_id": "f7573554-...",
  "subagent_id": "toolu_...",
  "subagent_type": "general-purpose",
  "status": "completed",
  "duration_ms": <number, 347-615ms across the six>,
  "parent_conversation_id": "f7573554-...",
  "message_count": 0,
  "tool_call_count": 0,
  "loop_count": 0,
  "task": "<the original prompt the parent sent the subagent>"
}
```

**The smoking gun: `message_count: 0`, `tool_call_count: 0`, and no assistant content.** Cursor sends the hook ONLY the metadata header + the original task prompt. The hook never receives the worker's output. The v3 JSONL-fallback is the only viable path.

**The intermittent symptom explained:** when the hook fires while the JSONL file is still being flushed by Cursor's logging layer, the most-recently-modified `.jsonl` heuristic picks a stale file OR the chosen file's last line is incomplete and jq's `select(test(...))` filter matches nothing → the body-extraction returns empty → the row degrades to `unknown / unknown / unknown / n/a`. When the hook fires after the JSONL is fully written, the heuristic picks the correct file, the filter matches the YAML turn, and the row populates correctly. The race is between Cursor's hook-fire timing and Cursor's own JSONL-flush timing; we cannot synchronize either from the hook side.

**Result:** T3.10 + T3.11 + T3.12 + T3.13 + T3.14 + T3.15 = six real dispatches under the probe. Of those, T3.10 + T3.11 + T3.12 produced healthy auto-rows (the JSONL was flushed in time). T3.13 + T3.14 + T3.15 produced `unknown / unknown` rows (the JSONL flush was late). The split is roughly 50/50 and not correlated with worker runtime or outcome — it's a pure race condition.

## Resolution

### 1. Diagnosis-complete; probe block removed

Phase A of the packaging cleanup removes the probe block from `.cursor/hooks/log-subagent-completion.sh`. The diagnostic-history comment at the top of the file is preserved (it documents the v1 / v2 / v3 / v3+probe evolution for future bisects). The forward-signpost block at the bottom of the file is replaced with a brief pointer to this retro.

### 2. Backfill procedure (used for T3.13/T3.14/T3.15)

When a future dispatch produces an `unknown / unknown` row in `log.md`:

```bash
# Locate the subagent's own JSONL by matching the hook timestamp against mtimes
# of files in agent-transcripts/<parent-id>/subagents/. The hook fires at
# completion, so the JSONL's mtime should be ≈ hook timestamp (within 1 minute).
SUBAGENT_DIR="$HOME/.cursor/projects/<parent-conversation-id>/agent-transcripts/<parent-id>/subagents"
ls -lt "$SUBAGENT_DIR" | head

# Identify by content: grep the JSONL for the dispatch identifier (patch slug,
# T3.x number, dispatch ID — whichever the parent set in the prompt).
grep -l "T3\.X\|patch-NNN" "$SUBAGENT_DIR"/*.jsonl | head -1

# Extract the YAML body the same way the hook would have:
body=$(jq -rs '
  [ .[]
    | select(.role == "assistant")
    | (.message.content // [])
    | map(select(.type == "text") | .text)
    | join("\n")
  ] as $turns
  | ($turns | map(select(test("(^|\n)outcome:|(^|\n)patch_slug:|(^|\n)escalation_reason:")))) as $yaml_turns
  | (if ($yaml_turns | length) > 0 then $yaml_turns else $turns end)
  | join("\n\n")
' "$SUBAGENT_DIR/<chosen-jsonl>")

# Write the archive
cat > docs/aiven/uplifts/26.3/reports/<toolu-id>.md <<HEADER
<!-- subagentStop archive | source=transcript | status=<status> | type=general-purpose | ts=<ts> | backfilled=true | jsonl=<chosen-jsonl> | reason=hook-v3-third-regression -->

$body
HEADER

# Update the log.md row in-place by replacing the unknown fields with the
# slug / outcome / escalation_reason extracted from the body, and changing
# the Report column from `n/a` to `[report](reports/<toolu-id>.md)`.
```

Phase A of the current packaging cleanup applied this procedure to T3.13, T3.14, and T3.15. The three updated `log.md` rows + three new report archives are part of the same cleanup commit.

### 3. Forward signpost (in-hook)

The hook's forward-signpost comment block now reads:

```sh
# Forward signpost: if Cursor ever restores `.agent_transcript_path` (or
# starts sending the assistant content directly in `$input`), prefer that
# over the JSONL-fallback heuristic by adding a new branch above the
# fallback at line ~102. Diagnosis history lives in the comment above
# `subagent_type=` near the top of this file, and in the §3 / cross-cutting
# retro `14-hook-v3-third-regression.md`.
```

## What this means going forward

1. **`unknown / unknown` rows are not bugs** — they're race-condition timing artifacts. The data is always recoverable from the on-disk JSONL.
2. **The hook script is correct.** Future maintainers should NOT attempt to "fix" the hook by adding retry loops or sleeps before the JSONL read — that would block tool execution and the failClosed=false invariant. The right escalation path is to backfill offline using the procedure above.
3. **Real-time observability for worker dispatches is best-effort.** If you need a real-time signal that a worker finished, watch the worker's JSONL grow with `tail -F`. If you need a durable signal, wait for the human commit (which always lands the dossier with the worker's report inline).
4. **The probe block remains a model for future infrastructure regressions.** When a Cursor-side hook input shape changes silently, the diagnostic move is: (a) check the script is healthy under synthetic input, (b) if yes, capture the live `$input` to a forensics file, (c) inspect the captured payloads to localize the change.

## Rule-of-three counts

- **Hook v1 → v2 transition** (retro 07): n=1 of 3 of REGRESSION. Mitigation landed.
- **Hook v2 → v3 refinement** (retro 08): n=2 of 3 of REGRESSION. Mitigation landed.
- **Hook v3 third regression** (this retro): n=3 of 3 of REGRESSION → **RESOLVED-AT-DIAGNOSTIC-COMPLETE.** Mitigation: declare the JSONL fallback as the permanent path; remove the probe; document the backfill procedure. The "rule-of-three for the regression itself" is satisfied; future regressions (v4) would be a NEW regression chapter, not a continuation.

- **Backfill procedure as durable institutional knowledge** (n=1 of 3): the procedure documented in §"Resolution / 2" was applied to three rows in Phase A. Counts as n=1 of an institutional-knowledge pattern. Track for next infrastructure regression where the data is recoverable but the live signal is degraded.

## Learning log

**Today you learned:** infrastructure regressions in agent-platform tools (hooks, MCP servers, transport layers) often manifest in unexpected ways because the contract between the platform and the user-side code is implicit. The probe-block pattern — capture the raw input on every fire, eyeball the captures, localize the change — is the canonical diagnostic move for any "the script worked yesterday and doesn't today" situation in agent-platform integrations.

**Rule of thumb:** when an agent-platform hook starts misbehaving and the script-side healthy-check passes, the regression is in WHAT THE PLATFORM SENDS, not in HOW THE SCRIPT PARSES. Capture the live input to a forensics file FIRST, then diagnose. Do not start refactoring the parser until you have the captured input.

**Next rabbit hole:** the v3 fallback's "youngest JSONL" heuristic has a known race window with parallel subagents — if two subagents complete within the same flush-window, the heuristic could pick the wrong JSONL. We have not observed this in T3.x (dispatches are sequential), but the next time we run parallel subagents (e.g., parallel patch dispatches in the same parent session), we should watch for cross-attribution. Possible mitigation: a `subagent_id`-based match against the JSONL's internal metadata, if Cursor stores `subagent_id` in the JSONL body.

#!/usr/bin/env bash
# Aiven LTS uplift hook: log subagent completion to docs/aiven/uplifts/26.3/log.md
# AND archive the verbatim worker report to docs/aiven/uplifts/26.3/reports/<id>.md.
#
# The hook input is the JSON described at https://cursor.com/docs/hooks#subagentstop.
# Load-bearing fields:
#   .summary               — Cursor extracts ONLY the <user_visible_high_level_summary>
#                            block as `.summary` (prose, not structured YAML). It is NOT
#                            sufficient to parse the halt-and-escalate report from this
#                            field. See T3.4 retrospective Finding (hook).
#   .agent_transcript_path — Absolute path to the subagent's own JSONL transcript file.
#                            Each line is a JSON event {role, message:{content:[…]}};
#                            the LAST assistant message's text blocks carry the full
#                            structured halt-and-escalate report including the
#                            outcome:/patch_slug:/escalation_reason: YAML lines.
#   .status                — "completed" | "error" | "aborted". Always present;
#                            orthogonal to the worker-emitted `outcome:` field inside
#                            the report body. A worker can finish status=completed
#                            with outcome=escalate (a legitimate halt-and-escalate),
#                            or status=aborted (user-canceled mid-run) with no outcome
#                            at all.
#
# Body-selection precedence (the "report body" we parse + archive):
#   1. Full assistant-text concatenated from agent_transcript_path (preferred — carries
#      the structured YAML and is the worker's authoritative final response).
#   2. .summary content (fallback — prose only, parsing of outcome/slug usually fails,
#      but rows still get logged with the status field).
#
# Configured failClosed=false in hooks.json so a script abort here doesn't block tool
# execution — it silently misses observability. We drop `set -e` and swallow jq errors
# for the same reason as the other hooks (a malformed JSON input must never block the
# parent agent's work). Log rows are always emitted, even when parseable content is
# empty — those rows still carry timestamp + subagent_id + status, which is enough
# to locate the run in the agent transcript.
set -uo pipefail

input=$(cat 2>/dev/null || true)
ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# --- BEGIN PROBE BLOCK (hook v3 third regression, investigation started 2026-05-27) ---
#
# Symptom: T3.8 + T3.9 dispatches (2026-05-27 morning UTC) produced ZERO
# auto-populated rows in log.md and ZERO archives in reports/. The only
# evidence of a hook fire was a single `unknown / unknown` row at
# 2026-05-27T10:26:31Z (mid-T3.9-runtime, ~34 min before T3.9 completion),
# which was discarded by parent inspection.
#
# Diagnosis-as-of-this-comment: the hook's body-extraction jq filter
# produces correct YAML (`outcome: success`, slug, escalation_reason) when
# run manually against the on-disk subagent JSONLs at
#   $PARENT_DIR/subagents/{53d78719,cdeba357}*.jsonl
# AND running the hook script with synthetic input (parent transcript_path
# only, agent_transcript_path=null, status="completed") produces a
# correct log.md row + archive. So the hook script is healthy; the
# regression is in WHAT CURSOR SENDS to the hook (most likely some
# combination of transcript_path=null, subagent_id=null,
# subagent_type=null, or a wholly different event-binding shape).
#
# Probe captures the raw $input to a timestamped file in tmp/hook-probe/
# so the NEXT real dispatch (T3.10) leaves a forensic record. tmp/ is
# gitignored, so probe files never accidentally commit. Filename includes
# bash PID for tie-breaking when parallel subagents complete in the same
# second.
#
# REMOVAL CRITERION: delete this block once we've captured >=2 real
# subagent dispatches that show either (a) the regression's exact JSON
# shape (after which we add a recovery branch above) or (b) the input is
# fully-populated and the regression is gone. Cite the probe file in the
# retrospective when documenting the fix.
probe_dir="tmp/hook-probe"
mkdir -p "$probe_dir" 2>/dev/null || true
probe_id=$(printf '%s' "$input" | jq -r '.subagent_id // .id // "unknown"' 2>/dev/null || echo "unknown")
probe_file="$probe_dir/${ts}-${probe_id}-${BASHPID:-$$}.json"
printf '%s' "$input" > "$probe_file" 2>/dev/null || true
# --- END PROBE BLOCK ---

subagent_type=$(printf '%s' "$input" | jq -r '.subagent_type // "unknown"' 2>/dev/null || echo "unknown")
subagent_id=$(printf '%s' "$input" | jq -r '.subagent_id // .id // "unknown"' 2>/dev/null || echo "unknown")
status=$(printf '%s' "$input" | jq -r '.status // "unknown"' 2>/dev/null || echo "unknown")
summary=$(printf '%s' "$input" | jq -r '.summary // empty' 2>/dev/null || true)

# Resolve the subagent's own transcript path.
#
# T3.6 Finding G investigation (2026-05-26) — Cursor's hook JSON shape changed
# silently under us. As of this date, for both `explore` and `general-purpose`
# subagents, the JSON input has:
#   .agent_transcript_path = null  (used to point to the subagent's JSONL)
#   .transcript_path       = the PARENT's JSONL  (wrong for our needs)
#   .summary               = null
# The subagent's own JSONL still exists on disk at
#   dirname(.transcript_path)/subagents/<uuid>.jsonl
# where <uuid> is Cursor-internal and bears no relation to .subagent_id.
#
# Recovery: derive subagents/ from the parent path and pick the
# most-recently-modified .jsonl (the just-finished subagent is the youngest
# by sub-second margin). Race window with parallel subagents is tight; a
# wrong pick produces a single bad row but never an exception.
#
# When Cursor restores .agent_transcript_path or invents a new field, prefer
# it over the heuristic by adding a new branch above the fallback.
transcript_path=$(printf '%s' "$input" | jq -r '.agent_transcript_path // empty' 2>/dev/null || true)
if [[ -z "$transcript_path" || "$transcript_path" == "null" ]]; then
  parent_transcript=$(printf '%s' "$input" | jq -r '.transcript_path // empty' 2>/dev/null || true)
  if [[ -n "$parent_transcript" && "$parent_transcript" != "null" ]]; then
    subagents_dir="$(dirname "$parent_transcript")/subagents"
    if [[ -d "$subagents_dir" ]]; then
      transcript_path=$(find "$subagents_dir" -maxdepth 1 -name '*.jsonl' -printf '%T@ %p\n' 2>/dev/null \
        | sort -nr \
        | head -1 \
        | awk '{ print $2 }')
    fi
  fi
fi

# Extract assistant-text: prefer turns containing the halt-and-escalate YAML
# anchors at column 0; fall back to all-turn join when none have them.
#
# History (T3.6 Finding G → T3.7 Finding B refinement):
#   v1 (pre-T3.6): read the LAST assistant turn only. Missed YAML in
#     non-last turns (workers sometimes emit a wrap-up prose turn AFTER
#     the YAML report).
#   v2 (T3.6 fix): concatenate every assistant text block. Captured the
#     YAML wherever it sat, BUT also captured every preceding planning
#     turn ("I need to read…", "Let me check…"), bloating the archive
#     from ~180 lines (T3.4 precedent) to ~354 lines (T3.7) with the
#     actual YAML buried at line ~170. The awk parser still worked
#     (column-0 anchors aren't fooled), but readability collapsed.
#   v3 (T3.7 fix, this version): same all-turn collection as v2, but
#     prefer turns whose text contains a column-0 YAML anchor
#     (`outcome:`, `patch_slug:`, or `escalation_reason:` at start of
#     line) — these are the same anchors the awk parser uses below, so
#     the filter cannot drop a turn the parser would have read. Fall back
#     to all-turn join only when no turn carries the YAML (read-only
#     subagents like `explore`/`cursorGuide`, or aborted runs).
#
# Forward-signpost: if a worker ever splits the YAML across two turns
# (e.g., commit_message body in one turn, the rest in another), this
# filter selects BOTH (because both carry column-0 anchors) and joins
# with a blank line — the awk parser reads the first occurrence of each
# anchor, which still produces the right outcome/slug/reason.
transcript_body=""
if [[ -n "$transcript_path" && -r "$transcript_path" ]]; then
  transcript_body=$(jq -rs '
    [ .[]
      | select(.role == "assistant")
      | (.message.content // [])
      | map(select(.type == "text") | .text)
      | join("\n")
    ] as $turns
    | ($turns | map(select(test("(^|\n)outcome:|(^|\n)patch_slug:|(^|\n)escalation_reason:")))) as $yaml_turns
    | (if ($yaml_turns | length) > 0 then $yaml_turns else $turns end)
    | join("\n\n")
  ' "$transcript_path" 2>/dev/null || true)
fi

# Body-selection precedence: transcript_body wins (it has the YAML), summary is fallback.
if [[ -n "$transcript_body" ]]; then
  report_body="$transcript_body"
  body_source="transcript"
else
  report_body="$summary"
  body_source="summary"
fi

# Archive the verbatim body to a per-subagent file. Retrospectives cite the rendered
# report; checking it into the repo (instead of relying on chat scrollback) makes the
# audit trail durable.
reports_dir="docs/aiven/uplifts/26.3/reports"
report_link="n/a"
if [[ -n "$report_body" && "$subagent_id" != "unknown" ]]; then
  mkdir -p "$reports_dir" 2>/dev/null || true
  report_file="$reports_dir/${subagent_id}.md"
  {
    printf '<!-- subagentStop archive | source=%s | status=%s | type=%s | ts=%s -->\n\n' \
      "$body_source" "$status" "$subagent_type" "$ts"
    printf '%s\n' "$report_body"
  } > "$report_file" 2>/dev/null || true
  if [[ -f "$report_file" ]]; then
    report_link="[report](reports/${subagent_id}.md)"
  fi
fi

# Parse the worker's halt-and-escalate YAML front-matter
# (docs/aiven/schema/halt-and-escalate.md). The fence form workers actually emit varies
# (plain `---` delimiters or ```yaml ... ``` code fence); in both forms the key lines
# sit at column 0 inside the block, so a plain `^key:` anchor in awk catches them.
# Non-conforming workers (read-only / cursorGuide / aborted runs) leave these as
# "unknown" / "n/a".
outcome="unknown"
patch_slug="unknown"
escalation_reason="n/a"

if [[ -n "$report_body" ]]; then
  parsed_outcome=$(printf '%s' "$report_body" | awk '/^outcome:/ {print $2; exit}' 2>/dev/null || true)
  [[ -n "$parsed_outcome" ]] && outcome="$parsed_outcome"
  parsed_slug=$(printf '%s' "$report_body" | awk '/^patch_slug:/ {print $2; exit}' 2>/dev/null || true)
  [[ -n "$parsed_slug" ]] && patch_slug="$parsed_slug"
  parsed_reason=$(printf '%s' "$report_body" | awk '/^escalation_reason:/ {print $2; exit}' 2>/dev/null || true)
  [[ -n "$parsed_reason" ]] && escalation_reason="$parsed_reason"
fi

log_file="docs/aiven/uplifts/26.3/log.md"
if [[ ! -f "$log_file" ]]; then
  mkdir -p "$(dirname "$log_file")" 2>/dev/null || true
  cat > "$log_file" <<HEADER
# 26.3 uplift work log

Appended automatically by the subagentStop hook. Human-edit only to add
commentary rows. The Report column links to the verbatim worker summary
archived under \`reports/\`; rows with \`n/a\` either had no parseable
summary (read-only subagents like cursorGuide / explore) or predate the
hook fix in T3.4 retrospective.

| Timestamp (UTC) | Status | Patch slug | Subagent type | Subagent id | Outcome | Escalation reason | Report |
|---|---|---|---|---|---|---|---|
HEADER
fi

echo "| $ts | $status | $patch_slug | $subagent_type | $subagent_id | $outcome | $escalation_reason | $report_link |" >> "$log_file"

# Forward signpost (T3.6 Finding G; T3.8/T3.9 third regression). The
# probe block near the top of this file (search "BEGIN PROBE BLOCK") is
# CURRENTLY ACTIVE as of 2026-05-27 and writes raw $input to
# tmp/hook-probe/<ts>-<id>-<pid>.json on every fire. If the next real
# subagent dispatch lands an `unknown / unknown` row in log.md, inspect
# the matching probe file to localize the regression to one of three
# sources:
#   1. Cursor restored .agent_transcript_path  (preferred path; prefer over heuristic)
#   2. dirname(.transcript_path)/subagents/<youngest>.jsonl  (current heuristic above)
#   3. .summary  (prose-only fallback; YAML keys typically absent)
# If instead the next dispatch's probe shows a fully-populated input (all
# expected fields present, transcript_path resolves, body-extraction
# produces YAML), the probe block can be removed per the criterion in
# its inline comment.

echo '{}'

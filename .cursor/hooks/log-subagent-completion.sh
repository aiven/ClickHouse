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

subagent_type=$(printf '%s' "$input" | jq -r '.subagent_type // "unknown"' 2>/dev/null || echo "unknown")
subagent_id=$(printf '%s' "$input" | jq -r '.subagent_id // .id // "unknown"' 2>/dev/null || echo "unknown")
status=$(printf '%s' "$input" | jq -r '.status // "unknown"' 2>/dev/null || echo "unknown")
summary=$(printf '%s' "$input" | jq -r '.summary // empty' 2>/dev/null || true)
transcript_path=$(printf '%s' "$input" | jq -r '.agent_transcript_path // empty' 2>/dev/null || true)

# Extract the LAST assistant turn's concatenated text content from the transcript.
# This carries the worker's full halt-and-escalate report (YAML body + prose). The
# .summary field is prose-only (user-visible high-level summary); it lacks the
# structured YAML keys we need for outcome/slug/escalation_reason parsing.
transcript_body=""
if [[ -n "$transcript_path" && -r "$transcript_path" ]]; then
  transcript_body=$(jq -rs '
    [.[] | select(.role == "assistant")] | last
    | .message.content // []
    | map(select(.type == "text") | .text)
    | join("\n")
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

echo '{}'

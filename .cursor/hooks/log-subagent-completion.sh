#!/usr/bin/env bash
# Aiven LTS uplift hook: log subagent completion to docs/aiven/uplifts/26.3/log.md.
# Observability minimum-viable. Captures timestamp, subagent type, and outcome from the report (if parsable).
#
# Configured failClosed=false, so a script abort here doesn't block — it
# silently misses observability. We still drop -e and swallow jq errors for
# consistency with the other hooks and so log rows are always emitted, even
# if some fields end up "unknown". A separate todo tracks improving the
# outcome-parsing logic that produces "outcome: unknown" rows (see
# subagentStop log gap in §14 of the design spec).
set -uo pipefail

input=$(cat 2>/dev/null || true)
ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
subagent_type=$(printf '%s' "$input" | jq -r '.subagent_type // "unknown"' 2>/dev/null || echo "unknown")
subagent_id=$(printf '%s' "$input" | jq -r '.subagent_id // .id // "unknown"' 2>/dev/null || echo "unknown")

final_response=$(printf '%s' "$input" | jq -r '.final_response // .response // empty' 2>/dev/null || true)

outcome="unknown"
patch_slug="unknown"
escalation_reason="n/a"

if [[ -n "$final_response" ]]; then
  outcome=$(echo "$final_response" | awk '/^outcome:/ {print $2; exit}' || echo "unknown")
  patch_slug=$(echo "$final_response" | awk '/^patch_slug:/ {print $2; exit}' || echo "unknown")
  escalation_reason=$(echo "$final_response" | awk '/^escalation_reason:/ {print $2; exit}' || echo "n/a")
fi

log_file="docs/aiven/uplifts/26.3/log.md"
if [[ ! -f "$log_file" ]]; then
  mkdir -p "$(dirname "$log_file")"
  cat > "$log_file" <<HEADER
# 26.3 uplift work log

Appended automatically by the subagentStop hook. Human-edit only to add commentary rows.

| Timestamp (UTC) | Patch slug | Subagent type | Subagent id | Outcome | Escalation reason |
|---|---|---|---|---|---|
HEADER
fi

echo "| $ts | $patch_slug | $subagent_type | $subagent_id | $outcome | $escalation_reason |" >> "$log_file"

echo '{}'

#!/usr/bin/env bash
# Aiven LTS uplift hook: log subagent completion to docs/aiven/uplifts/26.3/log.md.
# Observability minimum-viable. Captures timestamp, subagent type, and outcome from the report (if parsable).
set -euo pipefail

input=$(cat)
ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
subagent_type=$(echo "$input" | jq -r '.subagent_type // "unknown"')
subagent_id=$(echo "$input" | jq -r '.subagent_id // .id // "unknown"')

final_response=$(echo "$input" | jq -r '.final_response // .response // empty')

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

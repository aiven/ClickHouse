#!/usr/bin/env bash
# Aiven LTS uplift hook: deny writes to upstream-owned paths.
# Covers G4. Runs on preToolUse for Write|Edit (matcher in hooks.json).
#
# See note in deny-agent-commits.sh about why we drop -e and swallow jq errors.
# Same reasoning here: failClosed=true means a script abort would block every
# Write/Edit. We fail open on environmental hiccups; the path-prefix tests
# below still fire correctly whenever jq returns a parseable path.
set -uo pipefail

input=$(cat 2>/dev/null || true)
path=$(printf '%s' "$input" | jq -r '.input.path // .input.target_file // .input.file_path // empty' 2>/dev/null || true)

if [[ -z "$path" ]]; then
  echo '{"permission":"allow"}'
  exit 0
fi

path="${path#./}"

if [[ "$path" =~ ^\.claude/ ]] \
   || [[ "$path" == "AGENTS.md" ]] \
   || [[ "$path" == "CONTRIBUTING.md" ]] \
   || [[ "$path" =~ ^\.github/workflows/ ]] \
   || [[ "$path" =~ ^contrib/ ]]; then
  echo "{\"permission\":\"deny\",\"agent_message\":\"Write to upstream-owned path denied (G4): $path. Aiven content goes under docs/aiven/, .cursor/, or other prefixed locations.\"}"
  exit 0
fi

echo '{"permission":"allow"}'

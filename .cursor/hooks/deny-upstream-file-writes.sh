#!/usr/bin/env bash
# Aiven LTS uplift hook: deny writes to upstream-owned paths.
# Covers G4. Runs on preToolUse for Write|Edit (matcher in hooks.json).
set -euo pipefail

input=$(cat)
path=$(echo "$input" | jq -r '.input.path // .input.target_file // .input.file_path // empty')

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

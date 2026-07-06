#!/usr/bin/env bash
# Aiven LTS uplift hook: deny writes to upstream-owned paths.
# Covers G4. Runs on preToolUse for Write|Edit (matcher in hooks.json).
#
# Cursor's preToolUse JSON schema (verified 2026-05-24 by capturing the
# actual input from a live invocation):
#   .tool_name                — "Write" | "Edit" | etc.
#   .tool_input.file_path     — absolute path the tool is about to touch
#   .workspace_roots[0]       — absolute workspace root
# Prior versions of this hook looked for `.input.path` / `.input.target_file`
# / `.input.file_path`, which Cursor does not send — the hook was a silent
# no-op for every real Write/Edit since authoring.
#
# See note in deny-agent-commits.sh about why we drop -e and swallow jq
# errors. Same reasoning here: failClosed=true means a script abort would
# block every Write/Edit. We fail open on environmental hiccups; the
# path-prefix tests below still fire correctly whenever jq returns a
# parseable path.
set -uo pipefail

input=$(cat 2>/dev/null || true)

path=$(printf '%s' "$input" | jq -r '.tool_input.file_path // .tool_input.target_file // .tool_input.path // empty' 2>/dev/null || true)
workspace_root=$(printf '%s' "$input" | jq -r '.workspace_roots[0] // empty' 2>/dev/null || true)

if [[ -z "$path" ]]; then
  echo '{"permission":"allow"}'
  exit 0
fi

# Normalize: strip workspace root (so absolute paths become repo-relative)
# AND strip leading "./" (in case a future input is repo-relative already).
if [[ -n "$workspace_root" && "$path" == "$workspace_root"/* ]]; then
  path="${path#"$workspace_root"/}"
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

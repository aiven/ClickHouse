#!/usr/bin/env bash
# Deny writes to upstream-owned paths (G4). Always emit JSON.
set -uo pipefail

allow() { printf '%s\n' '{"permission":"allow"}'; exit 0; }
deny()  { printf '%s\n' "$1"; exit 0; }

input=$(cat 2>/dev/null || true)
path=""
workspace_root=""
if command -v jq >/dev/null 2>&1; then
  path=$(printf '%s' "$input" | jq -r '.tool_input.file_path // .tool_input.target_file // .tool_input.path // empty' 2>/dev/null || true)
  workspace_root=$(printf '%s' "$input" | jq -r '.workspace_roots[0] // empty' 2>/dev/null || true)
fi

if [[ -z "$path" ]]; then
  allow
fi

if [[ -n "$workspace_root" && "$path" == "$workspace_root"/* ]]; then
  path="${path#"$workspace_root"/}"
fi
path="${path#./}"

case "$path" in
  .claude/*|AGENTS.md|CONTRIBUTING.md|.github/workflows/*|contrib/*)
    deny "{\"permission\":\"deny\",\"agent_message\":\"Write to upstream-owned path denied (G4): $path.\"}"
    ;;
esac

allow

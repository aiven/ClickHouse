#!/usr/bin/env bash
# Ask before destructive ops (G3). Always emit JSON. failClosed should be false
# so a hook bug never blocks the agent; denies for commit/push live in sibling hooks.
set -uo pipefail

allow() { printf '%s\n' '{"permission":"allow"}'; exit 0; }
ask()   { printf '%s\n' "$1"; exit 0; }

input=$(cat 2>/dev/null || true)
command=""
if command -v jq >/dev/null 2>&1; then
  command=$(printf '%s' "$input" | jq -r '.command // empty' 2>/dev/null || true)
fi

if printf '%s' "$command" | grep -Eq '(^|[[:space:];|&])git[[:space:]]+reset[[:space:]].*--hard'; then
  ask '{"permission":"ask","user_message":"Destructive: git reset --hard. Confirm.","agent_message":"G3: waiting for approval."}'
fi

if printf '%s' "$command" | grep -Eq '(^|[[:space:];|&])git[[:space:]]+clean[[:space:]].*-[fd]'; then
  ask '{"permission":"ask","user_message":"Destructive: git clean -f. Confirm.","agent_message":"G3: waiting for approval."}'
fi

allow

#!/usr/bin/env bash
# Deny best-of-n-runner (G5). Always emit JSON.
set -uo pipefail

allow() { printf '%s\n' '{"permission":"allow"}'; exit 0; }
deny()  { printf '%s\n' "$1"; exit 0; }

input=$(cat 2>/dev/null || true)
subagent_type=""
if command -v jq >/dev/null 2>&1; then
  subagent_type=$(printf '%s' "$input" | jq -r '.subagent_type // empty' 2>/dev/null || true)
fi

if [[ "$subagent_type" == "best-of-n-runner" ]]; then
  deny '{"permission":"deny","user_message":"best-of-n-runner denied (G5)."}'
fi

allow

#!/usr/bin/env bash
# Deny push / force-push / rebase (G1/G2). Always emit JSON.
set -uo pipefail

allow() { printf '%s\n' '{"permission":"allow"}'; exit 0; }
deny()  { printf '%s\n' "$1"; exit 0; }

input=$(cat 2>/dev/null || true)
command=""
if command -v jq >/dev/null 2>&1; then
  command=$(printf '%s' "$input" | jq -r '.command // empty' 2>/dev/null || true)
fi

if printf '%s' "$command" | grep -Eq '(^|[[:space:];|&])git[[:space:]]+push([[:space:]]|$)'; then
  if printf '%s' "$command" | grep -Eq -- '(--force|[[:space:]]-f([[:space:]]|$))'; then
    deny '{"permission":"deny","agent_message":"Force-push denied (G2)."}'
  fi
  deny '{"permission":"deny","agent_message":"git push denied (G1). Humans push."}'
fi

if printf '%s' "$command" | grep -Eq '(^|[[:space:];|&])git[[:space:]]+rebase([[:space:]]|$)'; then
  deny '{"permission":"deny","agent_message":"git rebase denied (G2)."}'
fi

allow

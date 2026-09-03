#!/usr/bin/env bash
# Deny agent git commit / cherry-pick without --no-commit (G7).
# Contract: always print exactly one JSON permission object on stdout.
set -uo pipefail

allow() { printf '%s\n' '{"permission":"allow"}'; exit 0; }
deny()  { printf '%s\n' "$1"; exit 0; }

input=$(cat 2>/dev/null || true)
command=""
if command -v jq >/dev/null 2>&1; then
  command=$(printf '%s' "$input" | jq -r '.command // empty' 2>/dev/null || true)
fi

# Match `git commit` / `git cherry-pick` at start or after ;|& — not random "commit" words.
if printf '%s' "$command" | grep -Eq '(^|[[:space:];|&])git[[:space:]]+commit([[:space:]]|$)'; then
  deny '{"permission":"deny","agent_message":"git commit denied (G7). Stage only; human commits."}'
fi

if printf '%s' "$command" | grep -Eq '(^|[[:space:];|&])git[[:space:]]+cherry-pick([[:space:]]|$)'; then
  if ! printf '%s' "$command" | grep -Eq -- '--no-commit'; then
    deny '{"permission":"deny","agent_message":"git cherry-pick without --no-commit denied (G7)."}'
  fi
fi

allow

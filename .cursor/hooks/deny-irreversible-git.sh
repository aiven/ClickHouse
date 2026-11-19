#!/usr/bin/env bash
# Deny git push / force-push / rebase. Agents commit; humans push.
# Contract: always print exactly one JSON permission object on stdout.
set -uo pipefail
allow() { printf '%s\n' '{"permission":"allow"}'; exit 0; }
deny()  { printf '%s\n' "$1"; exit 0; }

if ! command -v jq >/dev/null 2>&1; then
  deny '{"permission":"deny","agent_message":"jq is missing, so the command cannot be inspected; refusing. Install jq."}'
fi

command=$(cat 2>/dev/null | jq -r '.command // empty' 2>/dev/null || true)

# `git`, then any global options (`-C <dir>`, `-c <k=v>`, `--git-dir=…`), then the subcommand.
git_prefix='(^|[[:space:];|&(])git(([[:space:]]+-[Cc][[:space:]]+[^[:space:]]+)|([[:space:]]+--[a-z-]+(=[^[:space:]]+)?))*[[:space:]]+'

if printf '%s' "$command" | grep -Eq -- "${git_prefix}push([[:space:]]|$)"; then
  if printf '%s' "$command" | grep -Eq -- '(--force|--force-with-lease|[[:space:]]-f([[:space:]]|$)|[[:space:]]\+[^[:space:]]+)'; then
    deny '{"permission":"deny","agent_message":"Force-push denied. Humans push."}'
  fi
  deny '{"permission":"deny","agent_message":"git push denied. Humans push."}'
fi
if printf '%s' "$command" | grep -Eq -- "${git_prefix}rebase([[:space:]]|$)"; then
  deny '{"permission":"deny","agent_message":"git rebase denied. Add new commits instead."}'
fi
allow

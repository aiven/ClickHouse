#!/usr/bin/env bash
# Aiven LTS uplift hook: deny agent commits (G7).
# Agent stages with `git cherry-pick --no-commit` and `git add`; human runs git commit.
set -euo pipefail

input=$(cat)
command=$(echo "$input" | jq -r '.command // empty')

# Chain-aware: see note in deny-irreversible-git.sh.
GIT_AT_START='(^[[:space:]]*|[&;|][[:space:]]*)git[[:space:]]+'

if [[ "$command" =~ ${GIT_AT_START}commit ]]; then
  echo '{"permission":"deny","agent_message":"git commit denied (G7). Agent does not commit. Stage your changes and propose the commit in your halt-and-escalate report; the human runs git commit themselves."}'
  exit 0
fi

if [[ "$command" =~ ${GIT_AT_START}cherry-pick ]] && ! [[ "$command" =~ --no-commit ]]; then
  echo '{"permission":"deny","agent_message":"git cherry-pick without --no-commit denied (G7). Use: git cherry-pick --no-commit <SHA>. The human commits the staged result."}'
  exit 0
fi

echo '{"permission":"allow"}'

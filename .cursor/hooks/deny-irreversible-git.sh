#!/usr/bin/env bash
# Aiven LTS uplift hook: deny irreversible git operations.
# Covers G1 (push), G2 (rebase, force-push).
# Amend is denied separately by deny-agent-commits.sh (G7).
set -euo pipefail

input=$(cat)
command=$(echo "$input" | jq -r '.command // empty')

# Chain-aware: matches `git X` at the start of the string OR after any chain
# separator (&&, ;, |). Necessary because the Shell tool passes the full chain
# (e.g., `cd /repo && git push ...`) as a single command string.
GIT_AT_START='(^[[:space:]]*|[&;|][[:space:]]*)git[[:space:]]+'

if [[ "$command" =~ ${GIT_AT_START}push ]] && [[ "$command" =~ (--force|-f([[:space:]]|$)) ]]; then
  echo '{"permission":"deny","agent_message":"Force-push denied (G2). AGENTS.md forbids rewrite-history; add new commits instead."}'
  exit 0
fi

if [[ "$command" =~ ${GIT_AT_START}push ]]; then
  echo '{"permission":"deny","agent_message":"git push denied (G1). Humans push, agents never. Stage your changes and propose them in the halt-and-escalate report."}'
  exit 0
fi

if [[ "$command" =~ ${GIT_AT_START}rebase ]]; then
  echo '{"permission":"deny","agent_message":"git rebase denied (G2). Add new commits instead of rewriting history."}'
  exit 0
fi

echo '{"permission":"allow"}'

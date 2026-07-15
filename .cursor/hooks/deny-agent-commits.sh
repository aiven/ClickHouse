#!/usr/bin/env bash
# Aiven LTS uplift hook: deny agent commits (G7).
# Agent stages with `git cherry-pick --no-commit` and `git add`; human runs git commit.
#
# Failure mode: this hook is configured failClosed=true. If the jq pipeline below
# fails (jq missing, stdin unexpected, etc.), `set -e` would abort before the
# permit-by-default echo at the bottom, blocking EVERY shell command. We
# deliberately drop -e and swallow jq errors so an environmental hiccup on a
# benign command (e.g. `echo`) falls through to allow. The deny rules below
# still fire as expected when the input is well-formed JSON.
set -uo pipefail

input=$(cat 2>/dev/null || true)
command=$(printf '%s' "$input" | jq -r '.command // empty' 2>/dev/null || true)

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

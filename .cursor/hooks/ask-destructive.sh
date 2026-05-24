#!/usr/bin/env bash
# Aiven LTS uplift hook: ask before destructive operations.
# Covers G3: git reset --hard, git clean -fd, rm -rf <path>.
#
# Configured failClosed=false, so a script abort here would NOT block the
# command — it would silently miss the destructive check. We still drop -e
# and swallow jq errors to stay consistent with the other two hooks; the
# regex tests still fire when input is well-formed JSON.
set -uo pipefail

input=$(cat 2>/dev/null || true)
command=$(printf '%s' "$input" | jq -r '.command // empty' 2>/dev/null || true)

# Chain-aware: see note in deny-irreversible-git.sh.
GIT_AT_START='(^[[:space:]]*|[&;|][[:space:]]*)git[[:space:]]+'
RM_AT_START='(^[[:space:]]*|[&;|][[:space:]]*)rm[[:space:]]+'

if [[ "$command" =~ ${GIT_AT_START}reset[[:space:]].*--hard ]]; then
  echo '{"permission":"ask","user_message":"Destructive: git reset --hard will discard tracked changes. Confirm before proceeding.","agent_message":"Hook G3 flagged this as destructive; waiting for human approval."}'
  exit 0
fi

if [[ "$command" =~ ${GIT_AT_START}clean[[:space:]].*-[fd] ]]; then
  echo '{"permission":"ask","user_message":"Destructive: git clean -f removes untracked files. Confirm before proceeding.","agent_message":"Hook G3 flagged this as destructive; waiting for human approval."}'
  exit 0
fi

if [[ "$command" =~ ${RM_AT_START}.*-[a-z]*r[a-z]*f[a-z]*[[:space:]]+/[^[:space:]]+ ]]; then
  echo '{"permission":"ask","user_message":"Destructive: rm -rf with an absolute path. Confirm.","agent_message":"Hook G3 flagged this as destructive; waiting for human approval."}'
  exit 0
fi

echo '{"permission":"allow"}'

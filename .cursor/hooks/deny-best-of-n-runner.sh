#!/usr/bin/env bash
# Aiven LTS uplift hook: deny best-of-n-runner subagents (G5).
# best-of-n-runner creates worktrees/branches; our policy is single-branch.
set -euo pipefail

input=$(cat)
subagent_type=$(echo "$input" | jq -r '.subagent_type // empty')

if [[ "$subagent_type" == "best-of-n-runner" ]]; then
  echo '{"permission":"deny","user_message":"best-of-n-runner denied (G5): our LTS-uplift policy is single-branch, single-worker. Use generalPurpose or explore instead."}'
  exit 0
fi

echo '{"permission":"allow"}'

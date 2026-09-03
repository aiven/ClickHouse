#!/usr/bin/env bash
# Reclaim Buildkite checkout ownership and wipe root-owned DinD leftovers.
#
# Integration tests run as root inside DinD and create
# tests/integration/**/_instances* that a later git clean cannot delete.
# When a job is hard-killed (Buildkite exit_status -1), the step EXIT trap
# never runs, so the next job on a persistent agent fails during checkout.
#
# Used by:
#   - .buildkite/agent-hooks/pre-checkout  (must be installed on the AMI)
#   - .buildkite/hooks/pre-exit           (repository hook, happy-path cleanup)
#   - .buildkite/provision.sh             (start + EXIT trap, defense in depth)
#
# Usage: bash .buildkite/cleanup_checkout_pollution.sh [checkout_path]
set -euo pipefail

checkout_path="${1:-${BUILDKITE_BUILD_CHECKOUT_PATH:-${PWD:-.}}}"

if [ ! -d "$checkout_path" ]; then
    exit 0
fi

echo "--- Cleaning checkout pollution under ${checkout_path}"

# Prefer chown so a subsequent git clean can remove everything; wipe _instances*
# explicitly to free disk even when chown is slow or incomplete.
sudo chown -R "$(id -u):$(id -g)" "$checkout_path" || true

if [ -d "${checkout_path}/tests/integration" ]; then
    # -prune + -exec rm: delete matching dirs without descending into them first.
    sudo find "${checkout_path}/tests/integration" \
        \( -type d -name '_instances' -o -type d -name '_instances-*' \) \
        -prune -exec rm -rf {} + 2>/dev/null || true
fi

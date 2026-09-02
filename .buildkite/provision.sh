#!/usr/bin/env bash
# Phase-1 in-pipeline provisioning (no AMI changes yet): make Docker and Python
# available on the ephemeral agent so praktika can run its containerized jobs.
# Shared by every step to keep the setup in one place.
set -euo pipefail

cleanup_checkout_ownership()
{
    # Praktika runs Docker containers with the repository bind-mounted. Some
    # tools create root-owned files in the checkout (__pycache__, generated
    # configs, disk test data, etc.). If those are left behind, the next
    # Buildkite job on the same persistent worker can fail before its command
    # starts, during git clean/checkout. Restore ownership on step exit.
    local checkout_path="${BUILDKITE_BUILD_CHECKOUT_PATH:-$PWD}"

    if [ -d "$checkout_path" ]; then
        sudo chown -R "$(id -u):$(id -g)" "$checkout_path" || true
    fi
}

trap cleanup_checkout_ownership EXIT

# Skip the dnf install when the tools we need are already present (persistent
# agents reuse the same image across steps/builds). Still start docker and
# relax SELinux every time — those are cheap and must be correct for the step.
need_install=0
if ! command -v docker >/dev/null 2>&1; then
    need_install=1
fi
if ! command -v python3 >/dev/null 2>&1; then
    need_install=1
fi
if ! python3 -c 'import requests' >/dev/null 2>&1; then
    need_install=1
fi

if [ "$need_install" -eq 1 ]; then
    sudo dnf install -y moby-engine python3 python3-requests
fi

sudo systemctl start docker
# Grant socket access directly instead of the interactive `newgrp docker`,
# which previously opened a shell and stalled the step.
sudo chmod 666 /var/run/docker.sock
# Fedora runs SELinux enforcing; praktika bind-mounts the repo into its
# containers, which SELinux can deny. Relax it on this throwaway agent.
sudo setenforce 0 || true
docker info >/dev/null

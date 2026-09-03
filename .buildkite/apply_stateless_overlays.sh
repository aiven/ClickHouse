#!/usr/bin/env bash
# Apply Buildkite-only tweaks to functional-test server configs *before*
# praktika runs `tests/config/install.sh`.
#
# Important: install.sh does NOT glob `tests/config/config.d/*.xml`. It
# selectively `ln -sf`s an allowlist into `/etc/clickhouse-server/config.d/`.
# Dropping a new zz_*.xml into config.d would never reach the server — patch
# the files install.sh actually links instead. Mutations are local to the
# agent checkout; the next Buildkite git clean/checkout restores upstream.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
azure_conf="${repo_root}/tests/config/config.d/azure_storage_conf.xml"

# Upstream default is 100000000000 (~100 GiB). That exceeds smaller Aiven agent
# roots (and still races free space on a grown disk). 20 GiB is enough for the
# suite and leaves headroom for Docker / logs / binaries.
azure_cache_max_size=20000000000

if [ ! -f "$azure_conf" ]; then
    echo "ERROR: missing ${azure_conf}" >&2
    exit 1
fi

# Scope to <cached_azure> so we never rewrite unrelated disks in this file.
sed -i \
    "/<cached_azure>/,/<\/cached_azure>/s|<max_size>[^<]*</max_size>|<max_size>${azure_cache_max_size}</max_size>|" \
    "$azure_conf"

if ! grep -q "<max_size>${azure_cache_max_size}</max_size>" "$azure_conf"; then
    echo "ERROR: failed to patch cached_azure max_size in ${azure_conf}" >&2
    exit 1
fi

echo "Patched cached_azure max_size -> ${azure_cache_max_size} in ${azure_conf}"

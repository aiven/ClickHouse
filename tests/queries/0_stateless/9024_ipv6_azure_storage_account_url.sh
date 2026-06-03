#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: requires Azure Blob Storage support (USE_AZURE_BLOB_STORAGE)

# Patch 024 broadens validateStorageAccountUrl so an Azure `storage_account_url` with a
# bracketed IPv6 host (and an optional port) passes URL validation instead of being
# rejected. We exercise the validation through a dynamic Azure disk: validation runs
# (in processEndpoint) before any connection is attempted, so a bogus [::1]:1 endpoint
# is enough to reach it. We assert only whether the *validation* error fires; the
# subsequent connection failure (if any) is irrelevant and not checked.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_9024_ipv6"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_9024_bad"

# Reports whether the URL was rejected by validateStorageAccountUrl, robust to the
# validation phrase appearing on more than one line of the error.
check() {
    if ${CLICKHOUSE_CLIENT} --query "$1" 2>&1 | grep -qF "Blob Storage URL is not valid"; then
        echo "REJECTED"
    else
        echo "ACCEPTED"
    fi
}

# Bracketed IPv6 host with an explicit port must NOT be rejected as an invalid URL.
# Post-patch: ACCEPTED (validation passes). Pre-patch: REJECTED.
check "
CREATE TABLE test_9024_ipv6 (a Int) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    type = azure_blob_storage,
    storage_account_url = 'http://[::1]:1/devstoreaccount1',
    container_name = 'cont',
    account_name = 'devstoreaccount1',
    account_key = 'dGVzdGtleQ==')"

# Negative control: a non-URL string must still be rejected by validation (both pre/post).
check "
CREATE TABLE test_9024_bad (a Int) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    type = azure_blob_storage,
    storage_account_url = 'not a url',
    container_name = 'cont',
    account_name = 'devstoreaccount1',
    account_key = 'dGVzdGtleQ==')"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_9024_ipv6"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_9024_bad"

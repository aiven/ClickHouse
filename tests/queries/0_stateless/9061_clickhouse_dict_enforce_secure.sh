#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Leg B of patch 061: a remote (non-local) ClickHouse dictionary source must use a
# secure connection. The host 192.0.2.1 (RFC 5737 TEST-NET-1) is non-local and
# non-routable; `secure 0` makes it a non-secure remote source.
#
# Post-patch: registration throws BAD_ARGUMENTS "supports only secure connections"
#             BEFORE any connection attempt (deterministic, no network round-trip).
# Pre-patch:  no such check; the loader proceeds to connect to the non-routable host
#             and fails with a connection error that does NOT contain that message.

${CLICKHOUSE_CLIENT} -q "DROP DICTIONARY IF EXISTS dict_enforce_secure"

${CLICKHOUSE_CLIENT} -q "CREATE DICTIONARY dict_enforce_secure (id UInt64, val String) PRIMARY KEY id SOURCE(CLICKHOUSE(host '192.0.2.1' port 9000 secure 0 db 'system' table 'one')) LIFETIME(MIN 0 MAX 0) LAYOUT(FLAT())"

if ${CLICKHOUSE_CLIENT} -q "SYSTEM RELOAD DICTIONARY dict_enforce_secure" 2>&1 | grep -q "supports only secure connections"; then
    echo "secure_enforced"
else
    echo "secure_not_enforced"
fi

${CLICKHOUSE_CLIENT} -q "DROP DICTIONARY IF EXISTS dict_enforce_secure"

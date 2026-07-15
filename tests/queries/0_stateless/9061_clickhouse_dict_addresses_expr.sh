#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Leg A of patch 061: a ClickHouse dictionary source may name a named collection
# carrying `addresses_expr`, which is parsed via
# `parseRemoteDescriptionForExternalDatabase` into a list of <host, port> pairs.
# The Aiven carry additionally rejects an `addresses_expr` that mixes the local
# instance with a remote one.
#
# Causation note: upstream 26.3 already lists `addresses_expr` as a key
# equivalent to `host`/`hostname` in `ExternalDatabaseEqualKeysSet`, so
# `validateNamedCollection` ACCEPTS the key even pre-patch. Pre-patch, however,
# the value is never parsed: the source falls back to `host=localhost:port`, so a
# single-address expression is silently ignored and cannot distinguish the patch.
# We therefore exercise the `addresses_expr` parsing path directly via the mixed
# local/remote rejection, which exists only post-patch.
#
# The addresses are separated by `|` (the replica separator used by
# `parseRemoteDescriptionForExternalDatabase`).
#
# Post-patch: `addresses_expr` is parsed into [127.0.0.1:<tcp> (local),
#             192.0.2.1:9000 (remote)]; the mixed set is rejected at registration
#             with BAD_ARGUMENTS "Either all addresses should be the local
#             ClickHouse instance or all should be remote ClickHouse instances"
#             BEFORE any connection attempt (deterministic, no network).
# Pre-patch:  `addresses_expr` is ignored; host defaults to localhost and the
#             dictionary loads, so the rejection message never appears.

COLL="coll_dict_addr_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP DICTIONARY IF EXISTS dict_addr_expr"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS src_addr_expr"
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS ${COLL}"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE src_addr_expr (id UInt64, val String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "INSERT INTO src_addr_expr SELECT 1, 'leg_a_ok'"

# 192.0.2.1 is RFC 5737 TEST-NET-1 (non-local, non-routable). Mixing it with the
# local instance address must be rejected post-patch before any connection.
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION ${COLL} AS addresses_expr = '127.0.0.1:${CLICKHOUSE_PORT_TCP}|192.0.2.1:9000', database = '${CLICKHOUSE_DATABASE}', table = 'src_addr_expr'"

${CLICKHOUSE_CLIENT} -q "CREATE DICTIONARY dict_addr_expr (id UInt64, val String) PRIMARY KEY id SOURCE(CLICKHOUSE(name '${COLL}')) LIFETIME(MIN 0 MAX 0) LAYOUT(FLAT())"

if ${CLICKHOUSE_CLIENT} -q "SYSTEM RELOAD DICTIONARY dict_addr_expr" 2>&1 | grep -q "Either all addresses should be the local"; then
    echo "addresses_expr_mixed_rejected"
else
    echo "addresses_expr_mixed_not_rejected"
fi

${CLICKHOUSE_CLIENT} -q "DROP DICTIONARY IF EXISTS dict_addr_expr"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS src_addr_expr"
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS ${COLL}"

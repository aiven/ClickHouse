#!/usr/bin/env bash
# Aiven: the patch removes the optional debug / observability web UIs (/binary, /merges,
# /jemalloc, /clickstack) from the default HTTP handler set. Assert those paths are no
# longer served (HTTP 404) while the retained UIs (/play, /dashboard) and the /ping health
# check still respond (HTTP 200).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Removed endpoints: expect 404 (no handler attached)
for path in "/binary" "/merges" "/jemalloc" "/clickstack"; do
    echo -n "${path} "
    ${CLICKHOUSE_CURL} -s -o /dev/null -w '%{http_code}\n' "${BASE}${path}"
done

# Retained endpoints: expect 200
for path in "/ping" "/play" "/dashboard"; do
    echo -n "${path} "
    ${CLICKHOUSE_CURL} -s -o /dev/null -w '%{http_code}\n' "${BASE}${path}"
done

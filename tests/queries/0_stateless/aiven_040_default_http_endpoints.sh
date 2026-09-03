#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

for path in /replicas_status /binary /merges /jemalloc /clickstack /schema /processors-profile; do
    printf '%s ' "${path}"
    ${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' "${BASE}${path}"
done

for path in / /ping /play /dashboard /docs /js/uplot.js; do
    printf '%s ' "${path}"
    ${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' "${BASE}${path}"
done

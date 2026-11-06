#!/usr/bin/env bash
# Tags: no-ordinary-database, zookeeper, no-fasttest, no-parallel
# no-parallel: enables a server-wide failpoint

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

REPL_DB="repl_054_${CLICKHOUSE_TEST_UNIQUE_NAME}"
KEEPER_PATH="/${CLICKHOUSE_DATABASE}_054"

# Prints the result of a query that is expected to fail, by error code name.
expect_error() {
    local label="$1" out
    if out=$($CLICKHOUSE_CLIENT --query "$2" 2>&1); then
        echo "${label}: allowed"
    else
        echo "${label}: $(echo "$out" | grep -m1 -oE '\(([A-Z_]+)\)' | tr -d '()')"
    fi
}

cleanup() {
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT keeper_map_sink_pause_before_multi" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS km SYNC"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS km_created_ro SYNC"
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${REPL_DB} SYNC" > /dev/null
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -q "CREATE TABLE km (key UInt64, value String) ENGINE = KeeperMap('${KEEPER_PATH}/km') PRIMARY KEY key"
$CLICKHOUSE_CLIENT -q "INSERT INTO km VALUES (1, 'a'), (2, 'b')"

echo "-- not supported, as without the setting"
expect_error "add column" "ALTER TABLE km ADD COLUMN extra UInt8"
expect_error "unknown setting" "ALTER TABLE km MODIFY SETTING no_such_setting = 1"
expect_error "invalid value" "ALTER TABLE km MODIFY SETTING read_only = 'abc'"

echo "-- read-only"
$CLICKHOUSE_CLIENT -q "ALTER TABLE km MODIFY SETTING read_only = 1"
expect_error "insert" "INSERT INTO km VALUES (3, 'c')"
expect_error "truncate" "TRUNCATE TABLE km"
expect_error "delete" "DELETE FROM km WHERE key = 1"
expect_error "alter delete" "ALTER TABLE km DELETE WHERE key = 1"
expect_error "alter update" "ALTER TABLE km UPDATE value = 'z' WHERE key = 1"
expect_error "add column" "ALTER TABLE km ADD COLUMN extra UInt8"
$CLICKHOUSE_CLIENT -q "ALTER TABLE km MODIFY COMMENT 'still allowed'"
$CLICKHOUSE_CLIENT -q "SELECT key, value FROM km ORDER BY key"

echo "-- kept on attach"
$CLICKHOUSE_CLIENT -q "DETACH TABLE km"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE km"
expect_error "insert" "INSERT INTO km VALUES (3, 'c')"

echo "-- reset"
$CLICKHOUSE_CLIENT -q "ALTER TABLE km RESET SETTING read_only"
$CLICKHOUSE_CLIENT -q "INSERT INTO km VALUES (3, 'c')"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM km"
$CLICKHOUSE_CLIENT -q "SELECT extract(create_table_query, 'read_only = \\\\w+') FROM system.tables WHERE database = currentDatabase() AND name = 'km'"
$CLICKHOUSE_CLIENT -q "DETACH TABLE km"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE km"
$CLICKHOUSE_CLIENT -q "INSERT INTO km VALUES (5, 'e')"

echo "-- set on create"
$CLICKHOUSE_CLIENT -q "CREATE TABLE km_created_ro (key UInt64, value String) ENGINE = KeeperMap('${KEEPER_PATH}/km_created_ro') PRIMARY KEY key SETTINGS read_only = 1"
expect_error "insert" "INSERT INTO km_created_ro VALUES (1, 'a')"

echo "-- write that started before the setting was turned on"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT keeper_map_sink_pause_before_multi"
# One sink: the failpoint pauses only the first sink, and with parallel sinks that may be an empty one.
$CLICKHOUSE_CLIENT --max_insert_threads 1 -q "INSERT INTO km VALUES (4, 'd')" > "${CLICKHOUSE_TMP}/054_insert.out" 2>&1 &
insert_pid=$!
# Bounded, so that an insert that fails before the failpoint shows up as a diff instead of a hang.
timeout 60 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT keeper_map_sink_pause_before_multi PAUSE" || echo "insert did not reach the failpoint"
$CLICKHOUSE_CLIENT -q "ALTER TABLE km MODIFY SETTING read_only = 1"
$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT keeper_map_sink_pause_before_multi"
if wait $insert_pid; then
    echo "insert: allowed"
else
    echo "insert: $(grep -m1 -oE '\(([A-Z_]+)\)' "${CLICKHOUSE_TMP}/054_insert.out" | tr -d '()')"
fi
$CLICKHOUSE_CLIENT -q "SELECT count() FROM km"

echo "-- Replicated database"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${REPL_DB} ENGINE = Replicated('/test/aiven_054/${CLICKHOUSE_DATABASE}/${REPL_DB}', 'shard_1', 'replica_1')"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${REPL_DB}.km (key UInt64, value String) ENGINE = KeeperMap('${KEEPER_PATH}/repl_km') PRIMARY KEY key" > /dev/null
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${REPL_DB}.km MODIFY SETTING read_only = 1" > /dev/null
expect_error "insert" "INSERT INTO ${REPL_DB}.km VALUES (1, 'a')"

#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

if [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.build_options WHERE name = 'ENABLE_AIVEN_HARDENING' AND value IN ('ON', '1')") != 1 ]]; then
    echo "skipped: requires ENABLE_AIVEN_HARDENING"
    exit 0
fi

expect_error()
{
    local expected=$1
    local query=$2
    local output
    if output=$($CLICKHOUSE_CLIENT --query "$query" 2>&1); then
        echo "Unexpected success: $query"
        exit 1
    fi
    if [[ $output != *"($expected)"* ]]; then
        echo "$output"
        echo "Expected $expected: $query"
        exit 1
    fi
}

test_user="hardening_${CLICKHOUSE_DATABASE}"
test_function="hardening_increment_${CLICKHOUSE_DATABASE}"
snapshot="hardening_${CLICKHOUSE_DATABASE}"
snapshot_created=0

cleanup()
{
    if [[ $snapshot_created == 1 ]]; then
        $CLICKHOUSE_CLIENT --query "ALTER TABLE hardening_rows UNFREEZE WITH NAME '$snapshot'" > /dev/null || true
    fi
    $CLICKHOUSE_CLIENT --multiquery --query "
        DROP VIEW IF EXISTS hardening_invoker;
        DROP VIEW IF EXISTS hardening_definer;
        DROP VIEW IF EXISTS hardening_none;
        DROP TABLE IF EXISTS hardening_disk;
        DROP TABLE IF EXISTS hardening_rows;
        DROP FUNCTION IF EXISTS $test_function;
        DROP USER IF EXISTS $test_user;
    "
}
trap cleanup EXIT

expect_error UNKNOWN_FUNCTION "SELECT file('hardening_absent.txt')"
expect_error UNKNOWN_FUNCTION "SELECT catboostEvaluate('hardening_absent_model', 1)"
expect_error UNKNOWN_FUNCTION "SELECT getClientHTTPHeader('Authorization')"
expect_error SUPPORT_IS_DISABLED "EXECUTE AS $test_user SELECT 1"
expect_error SUPPORT_IS_DISABLED "EXECUTE AS $test_user"
expect_error SUPPORT_IS_DISABLED "CREATE VIEW hardening_none SQL SECURITY NONE AS SELECT 1"
expect_error BAD_ARGUMENTS "CREATE USER $test_user IDENTIFIED WITH no_password"
expect_error BAD_ARGUMENTS "CREATE USER $test_user IDENTIFIED WITH plaintext_password BY 'test-only-password'"

$CLICKHOUSE_CLIENT --multiquery --query "
    CREATE USER $test_user IDENTIFIED WITH sha256_password BY 'test-only-password';
    CREATE TABLE hardening_rows (value UInt64) ENGINE = MergeTree ORDER BY value;
    INSERT INTO hardening_rows VALUES (1), (2);
    CREATE VIEW hardening_invoker SQL SECURITY INVOKER AS SELECT sum(value) AS total FROM hardening_rows;
    CREATE VIEW hardening_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT sum(value) AS total FROM hardening_rows;
    CREATE FUNCTION $test_function AS value -> value + 1;
"

expect_error BAD_ARGUMENTS "ALTER USER $test_user IDENTIFIED WITH no_password"
expect_error BAD_ARGUMENTS "ALTER USER $test_user IDENTIFIED WITH plaintext_password BY 'test-only-password'"
expect_error SUPPORT_IS_DISABLED "ALTER TABLE hardening_invoker MODIFY SQL SECURITY NONE"
expect_error SUPPORT_IS_DISABLED "BACKUP TABLE hardening_rows TO File('hardening_absent.zip')"
expect_error SUPPORT_IS_DISABLED "INSERT INTO hardening_rows SETTINGS input_format_record_errors_file_path = 'hardening_errors.tsv' FORMAT CSV invalid"
expect_error SUPPORT_IS_DISABLED "CREATE TABLE hardening_disk (value UInt64) ENGINE = MergeTree ORDER BY value SETTINGS disk = disk(type = 'local', path = 'hardening_disk/')"

[[ $($CLICKHOUSE_CLIENT --query "SELECT total FROM hardening_invoker") == 3 ]]
[[ $($CLICKHOUSE_CLIENT --query "SELECT total FROM hardening_definer") == 3 ]]
[[ $($CLICKHOUSE_CLIENT --query "SELECT $test_function(2)") == 3 ]]
[[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.table_functions WHERE name IN ('remoteSecure', 'url', 'urlCluster', 's3', 'azureBlobStorage', 'input')") == 6 ]]
[[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.table_functions WHERE name IN ('remote', 'file', 'fileCluster', 'executable', 'arrowFlight')") == 0 ]]

snapshot_created=1
$CLICKHOUSE_CLIENT --query "ALTER TABLE hardening_rows FREEZE WITH NAME '$snapshot' SETTINGS alter_partition_verbose_result = 0" > /dev/null
$CLICKHOUSE_CLIENT --query "SYSTEM UNFREEZE WITH NAME '$snapshot'" > /dev/null
snapshot_created=0

echo "Aiven hardening smoke test passed"
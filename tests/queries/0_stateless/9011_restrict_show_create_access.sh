#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

PRIV=priv_user_$CLICKHOUSE_TEST_UNIQUE_NAME
NONPRIV=nonpriv_user_$CLICKHOUSE_TEST_UNIQUE_NAME

# Cleanup any prior state (idempotent).
$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS $PRIV"
$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS $NONPRIV"

# Setup: create test table + two users with distinct privileges.
$CLICKHOUSE_CLIENT --query "CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DATABASE}.tab (id Int32) ENGINE = Memory"
$CLICKHOUSE_CLIENT --query "CREATE USER $PRIV IDENTIFIED WITH PLAINTEXT_PASSWORD BY 'hello'"
$CLICKHOUSE_CLIENT --query "CREATE USER $NONPRIV IDENTIFIED WITH PLAINTEXT_PASSWORD BY 'hello'"
$CLICKHOUSE_CLIENT --query "GRANT SHOW DATABASES, SHOW TABLES, SHOW COLUMNS, CREATE DATABASE, CREATE TABLE ON *.* TO $PRIV"
$CLICKHOUSE_CLIENT --query "GRANT SHOW DATABASES, SHOW TABLES, SHOW COLUMNS ON *.* TO $NONPRIV"

# Privileged user: all three queries succeed.
$CLICKHOUSE_CLIENT --user $PRIV --password hello --query "SHOW CREATE TABLE ${CLICKHOUSE_DATABASE}.tab FORMAT Null"
echo "priv_table_ok"

$CLICKHOUSE_CLIENT --user $PRIV --password hello --query "SHOW CREATE DATABASE ${CLICKHOUSE_DATABASE} FORMAT Null"
echo "priv_db_ok"

# Priv user sees the actual create query in system.tables.
$CLICKHOUSE_CLIENT --user $PRIV --password hello --query \
  "SELECT create_table_query != '' FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'tab'"

# Non-privileged user: post-patch sees ACCESS_DENIED for SHOW CREATE TABLE.
# Grep fragment "necessary to have the grant CREATE TABLE" appears ONLY in the
# access-denied error message — never in a successful CREATE TABLE statement —
# so this distinguishes pre-patch (query succeeds, fragment absent → set -e kills
# the test) from post-patch (query fails, fragment present → echo runs).
$CLICKHOUSE_CLIENT --user $NONPRIV --password hello --query "SHOW CREATE TABLE ${CLICKHOUSE_DATABASE}.tab FORMAT Null" 2>&1 \
  | grep -F "necessary to have the grant CREATE TABLE" > /dev/null && echo "nonpriv_table_denied"

# Non-privileged user: post-patch sees ACCESS_DENIED for SHOW CREATE DATABASE.
$CLICKHOUSE_CLIENT --user $NONPRIV --password hello --query "SHOW CREATE DATABASE ${CLICKHOUSE_DATABASE} FORMAT Null" 2>&1 \
  | grep -F "necessary to have the grant CREATE DATABASE" > /dev/null && echo "nonpriv_db_denied"

# Non-privileged user: post-patch system.tables.create_table_query is empty
# (gated by isGranted CREATE_TABLE).
$CLICKHOUSE_CLIENT --user $NONPRIV --password hello --query \
  "SELECT create_table_query = '' FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'tab'"

# Cleanup.
$CLICKHOUSE_CLIENT --query "DROP USER $PRIV"
$CLICKHOUSE_CLIENT --query "DROP USER $NONPRIV"
$CLICKHOUSE_CLIENT --query "DROP TABLE ${CLICKHOUSE_DATABASE}.tab"

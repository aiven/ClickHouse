#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Aiven patch 011: reading an object's definition requires a create privilege on
# that object, on top of the implied SHOW privilege. There are four doors to the
# same information and each is checked here in both directions:
#   - SHOW CREATE TABLE          (throws ACCESS_DENIED)
#   - SHOW CREATE DATABASE       (throws ACCESS_DENIED)
#   - system.tables.{create_table_query,engine_full,as_select}  (empty string)
#   - system.databases.engine_full                              (empty string)
#
# The fixtures are picked so that every asserted column is non-empty for a
# privileged user: an explicit database engine keeps databases.engine_full
# non-empty, a MergeTree table keeps tables.engine_full non-empty, and a view
# keeps tables.as_select non-empty. Without that, an "is empty" assertion would
# pass even if the patch did nothing.

# A database of our own, rather than the per-test one, so that its engine clause
# is known here instead of depending on how the runner created it.
DB="db_${CLICKHOUSE_TEST_UNIQUE_NAME}"
USER_PRIV="priv_${CLICKHOUSE_TEST_UNIQUE_NAME}"
USER_NONPRIV="nonpriv_${CLICKHOUSE_TEST_UNIQUE_NAME}"
PASSWORD="aiven_011_synthetic_password"

${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${USER_PRIV}, ${USER_NONPRIV};
    DROP DATABASE IF EXISTS ${DB};

    CREATE DATABASE ${DB} ENGINE = Atomic;
    CREATE TABLE ${DB}.tab (id Int32) ENGINE = MergeTree ORDER BY id;
    CREATE VIEW ${DB}.v AS SELECT id FROM ${DB}.tab;

    CREATE USER ${USER_PRIV} IDENTIFIED WITH plaintext_password BY '${PASSWORD}';
    CREATE USER ${USER_NONPRIV} IDENTIFIED WITH plaintext_password BY '${PASSWORD}';

    GRANT SHOW DATABASES, SHOW TABLES, SHOW COLUMNS ON ${DB}.* TO ${USER_PRIV}, ${USER_NONPRIV};
    GRANT CREATE DATABASE, CREATE TABLE ON ${DB}.* TO ${USER_PRIV};
"

run_as()
{
    ${CLICKHOUSE_CLIENT} --user "$1" --password "${PASSWORD}" --query "$2"
}

# Asserts that the statement fails with ACCESS_DENIED *and* that the message
# names the grant the patch now requires - the error code alone would also match
# a denial for one of the implied SHOW privileges.
expect_denied()
{
    local label="$1"
    local grant="$2"
    local query="$3"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${USER_NONPRIV}" --password "${PASSWORD}" --query "${query}" 2>&1 || true)
    if [[ "${out}" == *ACCESS_DENIED* && "${out}" == *"it's necessary to have the grant ${grant}"* ]]; then
        echo "${label} denied (missing ${grant} grant)"
    else
        echo "${label} NOT DENIED: ${out}"
    fi
}

echo '--- privileged user (holds CREATE TABLE / CREATE DATABASE) ---'

run_as "${USER_PRIV}" "SHOW CREATE TABLE ${DB}.tab" > /dev/null && echo 'SHOW CREATE TABLE ok'
run_as "${USER_PRIV}" "SHOW CREATE DATABASE ${DB}" > /dev/null && echo 'SHOW CREATE DATABASE ok'

printf 'tables.create_table_query non-empty '
run_as "${USER_PRIV}" "SELECT create_table_query != '' FROM system.tables WHERE database = '${DB}' AND name = 'tab'"
printf 'tables.engine_full non-empty '
run_as "${USER_PRIV}" "SELECT engine_full != '' FROM system.tables WHERE database = '${DB}' AND name = 'tab'"
printf 'tables.as_select non-empty '
run_as "${USER_PRIV}" "SELECT as_select != '' FROM system.tables WHERE database = '${DB}' AND name = 'v'"
printf 'databases.engine_full non-empty '
run_as "${USER_PRIV}" "SELECT engine_full != '' FROM system.databases WHERE name = '${DB}'"

echo '--- non-privileged user (only the implied SHOW privileges) ---'

expect_denied 'SHOW CREATE TABLE' 'CREATE TABLE' "SHOW CREATE TABLE ${DB}.tab"
expect_denied 'SHOW CREATE DATABASE' 'CREATE DATABASE' "SHOW CREATE DATABASE ${DB}"

printf 'tables.create_table_query empty '
run_as "${USER_NONPRIV}" "SELECT create_table_query = '' FROM system.tables WHERE database = '${DB}' AND name = 'tab'"
printf 'tables.engine_full empty '
run_as "${USER_NONPRIV}" "SELECT engine_full = '' FROM system.tables WHERE database = '${DB}' AND name = 'tab'"
printf 'tables.as_select empty '
run_as "${USER_NONPRIV}" "SELECT as_select = '' FROM system.tables WHERE database = '${DB}' AND name = 'v'"
printf 'databases.engine_full empty '
run_as "${USER_NONPRIV}" "SELECT engine_full = '' FROM system.databases WHERE name = '${DB}'"

${CLICKHOUSE_CLIENT} --query "
    DROP USER ${USER_PRIV}, ${USER_NONPRIV};
    DROP DATABASE ${DB};
"

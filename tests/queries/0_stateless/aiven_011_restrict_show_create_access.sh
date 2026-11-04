#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Aiven patch 011: reading an object's definition requires a create privilege on
# that object, on top of the implied SHOW privilege. Every door to the same
# information is checked here in both directions:
#   - SHOW CREATE TABLE / VIEW / DATABASE           (throw ACCESS_DENIED)
#   - SHOW CREATE on an Alias, whose target also needs CREATE TABLE
#   - system.tables.{create_table_query,engine_full,as_select}  (empty string)
#   - information_schema.views.view_definition                 (empty string)
#   - system.databases.engine_full                             (empty string)
# Definition fragments such as the sorting key stay visible on purpose.
#
# The fixtures are picked so that every asserted column is non-empty for a
# privileged user: an explicit database engine keeps databases.engine_full
# non-empty, a MergeTree table keeps tables.engine_full non-empty, and a view
# keeps tables.as_select non-empty. Without that, an "is empty" assertion would
# pass even if the patch did nothing.
#
# The patch relies on upstream masking for credentials, so the privileged user,
# who holds no secrets grant, must never see the password of the URL table.

DB="db_${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_TARGET="db_target_${CLICKHOUSE_TEST_UNIQUE_NAME}"
USER_PRIV="priv_${CLICKHOUSE_TEST_UNIQUE_NAME}"
USER_NONPRIV="nonpriv_${CLICKHOUSE_TEST_UNIQUE_NAME}"
PASSWORD="aiven_011_synthetic_password"
URL_SECRET="aiven011urlsecret"

${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${USER_PRIV}, ${USER_NONPRIV};
    DROP DATABASE IF EXISTS ${DB};
    DROP DATABASE IF EXISTS ${DB_TARGET};

    CREATE DATABASE ${DB} ENGINE = Atomic;
    CREATE DATABASE ${DB_TARGET} ENGINE = Atomic;
    CREATE TABLE ${DB}.tab (id Int32) ENGINE = MergeTree ORDER BY id;
    CREATE VIEW ${DB}.v AS SELECT id FROM ${DB}.tab;
    CREATE TABLE ${DB}.url_tab (id Int32) ENGINE = URL('http://svc:${URL_SECRET}@localhost:1/data.csv', 'CSV');
    CREATE TABLE ${DB_TARGET}.target (id Int32) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE ${DB}.alias_tab ENGINE = Alias('${DB_TARGET}', 'target');

    CREATE USER ${USER_PRIV} IDENTIFIED WITH plaintext_password BY '${PASSWORD}';
    CREATE USER ${USER_NONPRIV} IDENTIFIED WITH plaintext_password BY '${PASSWORD}';

    GRANT SHOW DATABASES, SHOW TABLES, SHOW COLUMNS ON ${DB}.* TO ${USER_PRIV}, ${USER_NONPRIV};
    GRANT SHOW TABLES, SHOW COLUMNS ON ${DB_TARGET}.target TO ${USER_PRIV}, ${USER_NONPRIV};
    GRANT SELECT ON information_schema.views TO ${USER_PRIV}, ${USER_NONPRIV};
    GRANT CREATE DATABASE, CREATE TABLE ON ${DB}.* TO ${USER_PRIV};
"

run_as()
{
    ${CLICKHOUSE_CLIENT} --user "$1" --password "${PASSWORD}" --query "$2"
}

# Asserts that the statement fails with ACCESS_DENIED *and* that the message
# names what the patch now requires - the error code alone would also match a
# denial for one of the implied SHOW privileges.
expect_denied()
{
    local label="$1"
    local user="$2"
    local needle="$3"
    local query="$4"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${user}" --password "${PASSWORD}" --query "${query}" 2>&1 || true)
    if [[ "${out}" == *ACCESS_DENIED* && "${out}" == *"${needle}"* ]]; then
        echo "${label} denied"
    else
        echo "${label} NOT DENIED: ${out}"
    fi
}

echo '--- privileged user (holds CREATE TABLE / CREATE DATABASE) ---'

run_as "${USER_PRIV}" "SHOW CREATE TABLE ${DB}.tab" > /dev/null && echo 'SHOW CREATE TABLE ok'
run_as "${USER_PRIV}" "SHOW CREATE VIEW ${DB}.v" > /dev/null && echo 'SHOW CREATE VIEW ok'
run_as "${USER_PRIV}" "SHOW CREATE DATABASE ${DB}" > /dev/null && echo 'SHOW CREATE DATABASE ok'

printf 'tables.create_table_query non-empty '
run_as "${USER_PRIV}" "SELECT create_table_query != '' FROM system.tables WHERE database = '${DB}' AND name = 'tab'"
printf 'tables.engine_full non-empty '
run_as "${USER_PRIV}" "SELECT engine_full != '' FROM system.tables WHERE database = '${DB}' AND name = 'tab'"
printf 'tables.as_select non-empty '
run_as "${USER_PRIV}" "SELECT as_select != '' FROM system.tables WHERE database = '${DB}' AND name = 'v'"
printf 'information_schema.views.view_definition non-empty '
run_as "${USER_PRIV}" "SELECT view_definition != '' FROM information_schema.views WHERE table_schema = '${DB}' AND table_name = 'v'"
printf 'databases.engine_full non-empty '
run_as "${USER_PRIV}" "SELECT engine_full != '' FROM system.databases WHERE name = '${DB}'"

echo '--- privileged user: credentials stay masked without the secrets grant ---'

printf 'SHOW CREATE TABLE hides the URL password '
run_as "${USER_PRIV}" "SHOW CREATE TABLE ${DB}.url_tab" | grep -c -F "${URL_SECRET}"
printf 'tables.create_table_query hides the URL password '
run_as "${USER_PRIV}" "SELECT create_table_query != '' AND position(create_table_query, '${URL_SECRET}') = 0 FROM system.tables WHERE database = '${DB}' AND name = 'url_tab'"

echo '--- privileged user: an Alias also needs CREATE TABLE on its target ---'

expect_denied 'SHOW CREATE TABLE on the alias' "${USER_PRIV}" 'Not enough privileges to show metadata exposed by' "SHOW CREATE TABLE ${DB}.alias_tab"
printf 'tables.create_table_query of the alias empty '
run_as "${USER_PRIV}" "SELECT create_table_query = '' AND engine_full = '' FROM system.tables WHERE database = '${DB}' AND name = 'alias_tab'"

${CLICKHOUSE_CLIENT} --query "GRANT CREATE TABLE ON ${DB_TARGET}.target TO ${USER_PRIV}"

run_as "${USER_PRIV}" "SHOW CREATE TABLE ${DB}.alias_tab" > /dev/null && echo 'SHOW CREATE TABLE on the alias ok with the target grant'
printf 'tables.create_table_query of the alias non-empty with the target grant '
run_as "${USER_PRIV}" "SELECT create_table_query != '' FROM system.tables WHERE database = '${DB}' AND name = 'alias_tab'"

echo '--- non-privileged user (only the implied SHOW privileges) ---'

expect_denied 'SHOW CREATE TABLE' "${USER_NONPRIV}" "it's necessary to have the grant CREATE TABLE" "SHOW CREATE TABLE ${DB}.tab"
expect_denied 'SHOW CREATE VIEW' "${USER_NONPRIV}" "it's necessary to have the grant CREATE TABLE" "SHOW CREATE VIEW ${DB}.v"
expect_denied 'SHOW CREATE DATABASE' "${USER_NONPRIV}" "it's necessary to have the grant CREATE DATABASE" "SHOW CREATE DATABASE ${DB}"

printf 'tables.create_table_query empty '
run_as "${USER_NONPRIV}" "SELECT create_table_query = '' FROM system.tables WHERE database = '${DB}' AND name = 'tab'"
printf 'tables.engine_full empty '
run_as "${USER_NONPRIV}" "SELECT engine_full = '' FROM system.tables WHERE database = '${DB}' AND name = 'tab'"
printf 'tables.as_select empty '
run_as "${USER_NONPRIV}" "SELECT as_select = '' FROM system.tables WHERE database = '${DB}' AND name = 'v'"
printf 'information_schema.views.view_definition empty '
run_as "${USER_NONPRIV}" "SELECT view_definition = '' FROM information_schema.views WHERE table_schema = '${DB}' AND table_name = 'v'"
printf 'databases.engine_full empty '
run_as "${USER_NONPRIV}" "SELECT engine_full = '' FROM system.databases WHERE name = '${DB}'"

echo '--- non-privileged user: definition fragments stay visible ---'

printf 'tables.sorting_key visible '
run_as "${USER_NONPRIV}" "SELECT sorting_key FROM system.tables WHERE database = '${DB}' AND name = 'tab'"

${CLICKHOUSE_CLIENT} --query "
    DROP USER ${USER_PRIV}, ${USER_NONPRIV};
    DROP DATABASE ${DB};
    DROP DATABASE ${DB_TARGET};
"

#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

PROFILE="profile_053_${CLICKHOUSE_TEST_UNIQUE_NAME}"
USER="user_053_${CLICKHOUSE_TEST_UNIQUE_NAME}"
REPL_DB="repl_053_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ATOMIC_DB="atomic_053_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ZK_PATH="/test/aiven_053/${CLICKHOUSE_DATABASE}/${REPL_DB}"

# `Replicated` database DDL prints a status row per replica; setup and cleanup stay silent.
admin() { $CLICKHOUSE_CLIENT --query "$1" >/dev/null; }

# Any error other than the profile constraint is printed, so it cannot pass for "allowed".
run_as_user() {
    local label="$1" out
    if out=$($CLICKHOUSE_CLIENT --user "${USER}" --password hello --query "$2" 2>&1); then
        echo "${label}: allowed"
    elif echo "$out" | grep -qF "SETTING_CONSTRAINT_VIOLATION"; then
        echo "${label}: rejected by constraint"
    else
        echo "${label}: other error: $(echo "$out" | grep -m1 -o 'Code: [0-9]*.*')"
    fi
}

admin "DROP DATABASE IF EXISTS ${REPL_DB}"
admin "DROP DATABASE IF EXISTS ${ATOMIC_DB}"
admin "DROP USER IF EXISTS ${USER}"
admin "DROP SETTINGS PROFILE IF EXISTS ${PROFILE}"

# The server default of `max_suspicious_broken_parts` is 100, above the cap.
admin "CREATE SETTINGS PROFILE ${PROFILE} SETTINGS merge_tree_max_suspicious_broken_parts MAX 5"
admin "CREATE USER ${USER} IDENTIFIED WITH plaintext_password BY 'hello' SETTINGS PROFILE ${PROFILE}"
admin "CREATE DATABASE ${REPL_DB} ENGINE = Replicated('${ZK_PATH}', 'shard_1', 'replica_1')"
admin "CREATE DATABASE ${ATOMIC_DB} ENGINE = Atomic"
admin "GRANT CREATE TABLE, ALTER, DROP TABLE ON ${REPL_DB}.* TO ${USER}"
admin "GRANT CREATE TABLE, ALTER, DROP TABLE ON ${ATOMIC_DB}.* TO ${USER}"
admin "GRANT TABLE ENGINE ON MergeTree, TABLE ENGINE ON ReplicatedMergeTree TO ${USER}"

# The `Replicated` database runs the query from its DDL queue in the context of the DDL worker, not of
# the user, so the user's profile constraints must be checked before the query is enqueued.
run_as_user "create MergeTree over the cap" \
    "CREATE TABLE ${REPL_DB}.t_create (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS max_suspicious_broken_parts = 999"
run_as_user "create ReplicatedMergeTree over the cap" \
    "CREATE TABLE ${REPL_DB}.t_create_replicated (x UInt64) ENGINE = ReplicatedMergeTree ORDER BY x SETTINGS max_suspicious_broken_parts = 999"
run_as_user "create within the cap" \
    "CREATE TABLE ${REPL_DB}.t_alter (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS max_suspicious_broken_parts = 3"
run_as_user "alter over the cap" \
    "ALTER TABLE ${REPL_DB}.t_alter MODIFY SETTING max_suspicious_broken_parts = 999"
run_as_user "alter within the cap" \
    "ALTER TABLE ${REPL_DB}.t_alter MODIFY SETTING max_suspicious_broken_parts = 4"
run_as_user "alter back to the default over the cap" \
    "ALTER TABLE ${REPL_DB}.t_alter MODIFY SETTING max_suspicious_broken_parts = 100"

admin "CREATE TABLE ${REPL_DB}.t_reset (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS max_suspicious_broken_parts = 2, parts_to_throw_insert = 1000"
run_as_user "reset to the default over the cap" \
    "ALTER TABLE ${REPL_DB}.t_reset RESET SETTING max_suspicious_broken_parts"
run_as_user "reset an unconstrained setting" \
    "ALTER TABLE ${REPL_DB}.t_reset RESET SETTING parts_to_throw_insert"

# Upstream behaviour without the DDL queue, for comparison.
admin "CREATE TABLE ${ATOMIC_DB}.t_reset (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS max_suspicious_broken_parts = 2"
run_as_user "Atomic: reset to the default over the cap" \
    "ALTER TABLE ${ATOMIC_DB}.t_reset RESET SETTING max_suspicious_broken_parts"

$CLICKHOUSE_CLIENT --query "
    SELECT name, extract(create_table_query, 'max_suspicious_broken_parts = \\\\d+') FROM system.tables
    WHERE database = '${REPL_DB}' ORDER BY name"

admin "DROP DATABASE ${REPL_DB}"
admin "DROP DATABASE ${ATOMIC_DB}"
admin "DROP USER ${USER}"
admin "DROP SETTINGS PROFILE ${PROFILE}"

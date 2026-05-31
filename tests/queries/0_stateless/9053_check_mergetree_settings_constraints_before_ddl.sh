#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: creates server-global access entities (user + settings profile)
#              and a named Replicated database.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Unique, collision-free names derived from the runner's per-test unique token.
PROFILE="profile_9053_${CLICKHOUSE_TEST_UNIQUE_NAME}"
USER="user_9053_${CLICKHOUSE_TEST_UNIQUE_NAME}"
REPL_DB="repl_9053_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ZK_PATH="/test/aiven_053/${CLICKHOUSE_DATABASE}/${REPL_DB}"

# Run an admin (default-user) statement, discarding its stdout. Replicated-DB
# DDL returns a per-replica status row; we silence setup/cleanup so only the
# two assertion tokens reach the test's stdout.
admin() { $CLICKHOUSE_CLIENT --query "$1" >/dev/null; }

# Asserts BOTH the error-code name AND a message substring specific to the
# settings-constraint path, so an unrelated DDL error cannot masquerade as a pass.
classify() {
    local label="$1" out="$2"
    if echo "$out" | grep -qF "SETTING_CONSTRAINT_VIOLATION" \
       && echo "$out" | grep -qF "shouldn't be greater than"; then
        echo "${label}_rejected_by_constraint"
    else
        echo "${label}_unexpectedly_allowed"
    fi
}

run_as_user() {
    $CLICKHOUSE_CLIENT --user "${USER}" --password hello --query "$1" 2>&1
}

# Idempotent cleanup of any leftover state from a prior aborted run.
admin "DROP DATABASE IF EXISTS ${REPL_DB}"
admin "DROP USER IF EXISTS ${USER}"
admin "DROP SETTINGS PROFILE IF EXISTS ${PROFILE}"

# --- Setup (as admin / default user) ---------------------------------------

# A profile that caps the MergeTree setting `max_suspicious_broken_parts` at 5.
# MergeTree settings are referenced in profile constraints with the
# `merge_tree_` prefix (see src/Access/resolveSetting.h MERGE_TREE_SETTINGS_PREFIX).
admin "CREATE SETTINGS PROFILE ${PROFILE} SETTINGS merge_tree_max_suspicious_broken_parts MAX 5"

# A non-admin user bound to that profile.
admin "CREATE USER ${USER} IDENTIFIED WITH plaintext_password BY 'hello' SETTINGS PROFILE ${PROFILE}"

# The Replicated database. The new check lives in the DatabaseReplicated DDL
# path; an Atomic database would NOT reach it.
admin "CREATE DATABASE ${REPL_DB} ENGINE = Replicated('${ZK_PATH}', 'shard_1', 'replica_1')"

# Give the user enough rights to drive CREATE/ALTER DDL on the Replicated DB.
admin "GRANT CREATE TABLE, ALTER, DROP TABLE ON ${REPL_DB}.* TO ${USER}"

# --- CREATE path (checkTableEngine hunk) -----------------------------------
# The user submits a CREATE TABLE whose SETTINGS violate the profile cap.
# Pre-patch: enqueued and executed under the system profile -> constraint
#            bypassed -> table created (no error) -> "unexpectedly_allowed".
# Post-patch: rejected at enqueue in the user's context -> "rejected_by_constraint".
out_create=$(run_as_user \
  "CREATE TABLE ${REPL_DB}.t_create (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS max_suspicious_broken_parts = 999")
classify "create" "$out_create"

# --- ALTER path (checkQueryValid hunk) -------------------------------------
# A compliant table first (no constrained setting), then a violating
# ALTER ... MODIFY SETTING. Same pre/post differential as above.
admin "CREATE TABLE ${REPL_DB}.t_alter (x UInt64) ENGINE = MergeTree ORDER BY x"
out_alter=$(run_as_user \
  "ALTER TABLE ${REPL_DB}.t_alter MODIFY SETTING max_suspicious_broken_parts = 999")
classify "alter" "$out_alter"

# --- Cleanup ----------------------------------------------------------------
admin "DROP DATABASE ${REPL_DB}"
admin "DROP USER ${USER}"
admin "DROP SETTINGS PROFILE ${PROFILE}"

#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: this test creates PROTECTED users and roles, and `TO ALL` resolves to every
# entity on the server. A concurrent test writing `TO ALL` would pick ours up and be denied
# for want of PROTECTED ACCESS MANAGEMENT.

# Keep the server log stream out of the captured output: these tests assert on
# error text, and interleaved trace lines would swamp it.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Aiven patch 022. Statements that change a protected entity, or what applies to it, without
# naming it as their target:
#
# G: `GRANT ... TO <self> WITH REPLACE OPTION` revokes everything from the caller first, so it is
#    refused like `REVOKE ... FROM <self>`, whatever the caller's privileges.
# O: `CREATE ROLE/USER ... OR REPLACE` gives the entity a new ID, which strips the old one from
#    every entity that referenced it, like `DROP`.
# M: `MOVE` removes the entity from its source storage, with the same effect.
# D: `DROP ROW POLICY/QUOTA/SETTINGS PROFILE` changes what applies to the targets in its `TO` set.
#    A role in that set reaches a protected target through the protected users that hold it.
# P: the parser rejects a repeated or contradictory `PROTECTED` / `NOT PROTECTED`.
#
# Every group has a control that the holder of the privilege, or an unrelated entity, is unaffected.

U="${CLICKHOUSE_TEST_UNIQUE_NAME}"
ADMIN="admin_${U}"
NOPERM="noperm_${U}"
PROT_USER="prot_user_${U}"
PROT_ROLE="prot_role_${U}"
REG_USER="reg_user_${U}"
CLUSTER="test_shard_localhost"
PW="aiven_022_synthetic_password"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "
        DROP ROW POLICY IF EXISTS dp2_${U} ON ${CLICKHOUSE_DATABASE}.t;
        DROP QUOTA IF EXISTS dq1_${U}, dq4_${U}, dq5_${U};
        DROP SETTINGS PROFILE IF EXISTS dprof3_${U};
        DROP ROLE IF EXISTS g3_${U}, o1_${U}, o5_${U}, m1_${U}, m3_${U}, p3_${U}, ${PROT_ROLE};
        DROP USER IF EXISTS ${ADMIN}, ${NOPERM}, ${PROT_USER}, ${REG_USER}, o3_${U}, p1_${U};
    " 2>/dev/null
}
cleanup

${CLICKHOUSE_CLIENT} --query "
    CREATE USER ${ADMIN}  IDENTIFIED WITH plaintext_password BY '${PW}';
    CREATE USER ${NOPERM} IDENTIFIED WITH plaintext_password BY '${PW}';
    CREATE USER ${PROT_USER} PROTECTED;
    CREATE USER ${REG_USER};
    CREATE ROLE ${PROT_ROLE} PROTECTED;
    GRANT ${PROT_ROLE} TO ${PROT_USER};

    GRANT ACCESS MANAGEMENT ON *.* TO ${ADMIN}, ${NOPERM};
    GRANT ROLE ADMIN ON *.* TO ${ADMIN}, ${NOPERM};
    GRANT CLUSTER ON *.* TO ${ADMIN}, ${NOPERM};
    GRANT SELECT ON system.one TO ${ADMIN}, ${NOPERM} WITH GRANT OPTION;
    GRANT PROTECTED ON *.* TO ${ADMIN};
"

as_user() {
    local user="$1"; shift
    ${CLICKHOUSE_CLIENT} --user "${user}" --password "${PW}" --query "$1" 2>&1
}

admin_q() {
    ${CLICKHOUSE_CLIENT} --query "$1" 2>&1
}

denied() {
    local id="$1" user="$2" query="$3"
    local out
    out=$(as_user "${user}" "${query}")
    if [[ "${out}" != *ACCESS_DENIED* ]]; then
        echo "${id} NOT DENIED: ${out}"
    elif ! grep -qE "PROTECTED[ _]ACCESS[ _]MANAGEMENT" <<< "${out}"; then
        echo "${id} denied for the wrong reason: ${out}"
    else
        echo "${id} denied"
    fi
}

self_denied() {
    local id="$1" user="$2" query="$3"
    local out
    out=$(as_user "${user}" "${query}")
    if [[ "${out}" == *ACCESS_DENIED* && "${out}" == *"cannot revoke rights from themselves"* ]]; then
        echo "${id} self denied"
    else
        echo "${id} NOT SELF DENIED: ${out}"
    fi
}

allowed() {
    local id="$1" user="$2" query="$3"
    local out
    out=$(as_user "${user}" "${query}")
    if [[ -z "${out}" ]]; then
        echo "${id} allowed"
    else
        echo "${id} FAILED: ${out}"
    fi
}

syntax_error() {
    local id="$1" query="$2"
    local out
    out=$(admin_q "${query}")
    if [[ "${out}" == *SYNTAX_ERROR* ]]; then
        echo "${id} syntax error"
    else
        echo "${id} NO SYNTAX ERROR: ${out}"
    fi
}

observe() {
    echo "$1 $(admin_q "$2")"
}

grants_of() {
    echo "SELECT count() FROM system.grants WHERE user_name = '$1'"
}

role_grant() {
    echo "SELECT count() FROM system.role_grants WHERE user_name = '$1' AND granted_role_name = '$2'"
}

echo '=== G: GRANT ... TO <self> WITH REPLACE OPTION ==='
self_denied G1 "${NOPERM}" "GRANT SELECT ON system.one TO ${NOPERM} WITH REPLACE OPTION"
self_denied G1c "${NOPERM}" "GRANT ON CLUSTER ${CLUSTER} SELECT ON system.one TO ${NOPERM} WITH REPLACE OPTION"
self_denied G2 "${ADMIN}" "GRANT SELECT ON system.one TO CURRENT_USER WITH REPLACE OPTION"
admin_q "CREATE ROLE g3_${U}"
self_denied G3 "${NOPERM}" "GRANT g3_${U} TO ${NOPERM} WITH REPLACE OPTION"
observe G3ref "SELECT count() > 1 FROM system.grants WHERE user_name = '${NOPERM}'"
allowed G4 "${NOPERM}" "GRANT SELECT ON system.one TO ${REG_USER} WITH REPLACE OPTION"
observe G4ref "$(grants_of "${REG_USER}")"

echo '=== O: CREATE ... OR REPLACE of an entity a protected entity references ==='
admin_q "CREATE ROLE o1_${U}"
admin_q "GRANT o1_${U} TO ${PROT_USER}"
denied  O1 "${NOPERM}" "CREATE ROLE OR REPLACE o1_${U}"
denied  O1c "${NOPERM}" "CREATE ROLE OR REPLACE o1_${U} ON CLUSTER ${CLUSTER}"
observe O1ref "$(role_grant "${PROT_USER}" "o1_${U}")"
admin_q "CREATE USER o3_${U}"
admin_q "ALTER USER ${PROT_USER} GRANTEES o3_${U}"
denied  O3 "${NOPERM}" "CREATE USER OR REPLACE o3_${U}"
echo "O3ref $(admin_q "SHOW CREATE USER ${PROT_USER}" | grep -c "o3_${U}")"
allowed O4 "${ADMIN}" "CREATE ROLE OR REPLACE o1_${U}"
observe O4ref "$(role_grant "${PROT_USER}" "o1_${U}")"
admin_q "CREATE ROLE o5_${U}"
admin_q "GRANT o5_${U} TO ${REG_USER}"
allowed O5 "${NOPERM}" "CREATE ROLE OR REPLACE o5_${U}"

echo '=== M: MOVE of an entity a protected entity references ==='
admin_q "CREATE ROLE m1_${U}"
admin_q "GRANT m1_${U} TO ${PROT_USER}"
denied  M1 "${NOPERM}" "MOVE ROLE m1_${U} TO memory"
denied  M1c "${NOPERM}" "MOVE ROLE m1_${U} TO memory ON CLUSTER ${CLUSTER}"
observe M1ref "SELECT storage FROM system.roles WHERE name = 'm1_${U}'"
observe M1ref2 "$(role_grant "${PROT_USER}" "m1_${U}")"
allowed M2 "${ADMIN}" "MOVE ROLE m1_${U} TO memory"
observe M2ref "SELECT storage FROM system.roles WHERE name = 'm1_${U}'"
admin_q "CREATE ROLE m3_${U}"
admin_q "GRANT m3_${U} TO ${REG_USER}"
allowed M3 "${NOPERM}" "MOVE ROLE m3_${U} TO memory"

echo '=== D: DROP of an entity whose TO set reaches a protected target ==='
admin_q "CREATE QUOTA dq1_${U} FOR INTERVAL 1 hour MAX queries = 100 TO ${PROT_USER}"
denied  D1 "${NOPERM}" "DROP QUOTA dq1_${U}"
denied  D1c "${NOPERM}" "DROP QUOTA dq1_${U} ON CLUSTER ${CLUSTER}"
denied  D1e "${NOPERM}" "DROP QUOTA IF EXISTS dq1_${U}"
observe D1ref "SELECT count() FROM system.quotas WHERE name = 'dq1_${U}'"
admin_q "CREATE ROW POLICY dp2_${U} ON ${CLICKHOUSE_DATABASE}.t USING 1 TO ${PROT_ROLE}"
denied  D2 "${NOPERM}" "DROP ROW POLICY dp2_${U} ON ${CLICKHOUSE_DATABASE}.t"
observe D2ref "SELECT count() FROM system.row_policies WHERE short_name = 'dp2_${U}'"
admin_q "CREATE SETTINGS PROFILE dprof3_${U} TO default"
denied  D3 "${NOPERM}" "DROP SETTINGS PROFILE dprof3_${U}"
observe D3ref "SELECT count() FROM system.settings_profiles WHERE name = 'dprof3_${U}'"
admin_q "CREATE QUOTA dq4_${U} FOR INTERVAL 1 hour MAX queries = 100 TO ALL"
denied  D4 "${NOPERM}" "DROP QUOTA dq4_${U}"
admin_q "CREATE QUOTA dq5_${U} FOR INTERVAL 1 hour MAX queries = 100 TO ${REG_USER}"
allowed D5 "${NOPERM}" "DROP QUOTA dq5_${U}"
allowed D6 "${ADMIN}" "DROP QUOTA dq1_${U}, dq4_${U}"
allowed D7 "${ADMIN}" "DROP ROW POLICY dp2_${U} ON ${CLICKHOUSE_DATABASE}.t"

echo '=== P: repeated or contradictory PROTECTED ==='
syntax_error P1 "CREATE USER p1_${U} PROTECTED PROTECTED"
syntax_error P2 "ALTER USER ${REG_USER} PROTECTED NOT PROTECTED"
syntax_error P3 "CREATE ROLE p3_${U} PROTECTED PROTECTED"
syntax_error P4 "ALTER ROLE ${PROT_ROLE} NOT PROTECTED NOT PROTECTED"
allowed P5 "${ADMIN}" "ALTER USER ${REG_USER} PROTECTED"
echo "P5ref $(admin_q "SHOW CREATE USER ${REG_USER}" | grep -c "PROTECTED")"
observe P6ref "SELECT count() FROM system.users WHERE name = 'p1_${U}'"

cleanup

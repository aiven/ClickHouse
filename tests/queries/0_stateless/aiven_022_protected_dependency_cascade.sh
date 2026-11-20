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

# Aiven patch 022, the dependency-cascade guard. New on the 26.8 line.
#
# 26.8 added IAccessStorage::removeReferencesToRemovedIDs: after any successful
# top-level remove(), it walks every access entity and calls update() on anything
# that referenced the deleted id, so no dangling references are left behind. That
# write goes through updateImpl, which carries no CheckFunc, and it runs as a side
# effect with no user identity in scope - so it reaches a protected entity behind
# both the interpreter checks and the storage CheckFunc.
#
# What it can strip from a protected entity is default_roles, granted_roles,
# grantees and settings. The guard refuses the *triggering* DROP on the initiator
# when a protected entity depends on the target and the actor does not hold
# PROTECTED_ACCESS_MANAGEMENT.
#
# C5 and C6 are not optional. Without them the guard could be a blanket break of
# the upstream cascade and this suite would still be green.

U="${CLICKHOUSE_TEST_UNIQUE_NAME}"
ADMIN="admin_${U}"
NOPERM="noperm_${U}"
PROT_USER="prot_user_${U}"
PROT_ROLE="prot_role_${U}"
REG_USER="reg_user_${U}"
PW="aiven_022_synthetic_password"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "
        DROP ROLE IF EXISTS dep1_${U}, dep2_${U}, dep3_${U}, dep4_${U}, dep5_${U}, dep6_${U}, ${PROT_ROLE};
        DROP SETTINGS PROFILE IF EXISTS prof3_${U};
        DROP USER IF EXISTS ${ADMIN}, ${NOPERM}, ${PROT_USER}, ${REG_USER}, dep7_${U};
    " 2>/dev/null
}
cleanup

${CLICKHOUSE_CLIENT} --query "
    CREATE USER ${ADMIN}  IDENTIFIED WITH plaintext_password BY '${PW}';
    CREATE USER ${NOPERM} IDENTIFIED WITH plaintext_password BY '${PW}';
    CREATE USER ${PROT_USER} PROTECTED;
    CREATE USER ${REG_USER};
    CREATE ROLE ${PROT_ROLE} PROTECTED;

    GRANT ACCESS MANAGEMENT ON *.* TO ${ADMIN}, ${NOPERM};
    GRANT ROLE ADMIN ON *.* TO ${ADMIN}, ${NOPERM};
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

observe() {
    echo "$1 $(admin_q "$2")"
}

echo '=== C1: a plain role granted to a protected user ==='
admin_q "CREATE ROLE dep1_${U}"
admin_q "GRANT dep1_${U} TO ${PROT_USER}"
denied  C1 "${NOPERM}" "DROP ROLE dep1_${U}"
observe C1ref "SELECT count() FROM system.role_grants WHERE user_name = '${PROT_USER}' AND granted_role_name = 'dep1_${U}'"

echo '=== C2: the same, reached through default_roles ==='
admin_q "CREATE ROLE dep2_${U}"
admin_q "GRANT dep2_${U} TO ${PROT_USER}"
admin_q "ALTER USER ${PROT_USER} DEFAULT ROLE dep2_${U}"
observe C2before "SELECT has(default_roles_list, 'dep2_${U}') FROM system.users WHERE name = '${PROT_USER}'"
denied  C2 "${NOPERM}" "DROP ROLE dep2_${U}"
observe C2after "SELECT has(default_roles_list, 'dep2_${U}') FROM system.users WHERE name = '${PROT_USER}'"

echo '=== C3: the same, reached through a settings profile ==='
admin_q "CREATE SETTINGS PROFILE prof3_${U}"
admin_q "ALTER USER ${PROT_USER} SETTINGS PROFILE prof3_${U}"
denied  C3 "${NOPERM}" "DROP SETTINGS PROFILE prof3_${U}"
echo "C3ref $(${CLICKHOUSE_CLIENT} --query "SHOW CREATE USER ${PROT_USER}" 2>&1 | grep -c "prof3_${U}")"

echo '=== C4: a plain role granted to a protected ROLE ==='
admin_q "CREATE ROLE dep4_${U}"
admin_q "GRANT dep4_${U} TO ${PROT_ROLE}"
denied  C4 "${NOPERM}" "DROP ROLE dep4_${U}"
observe C4ref "SELECT count() FROM system.role_grants WHERE role_name = '${PROT_ROLE}' AND granted_role_name = 'dep4_${U}'"

echo '=== C5: positive control - the holder of the privilege may still drop it ==='
admin_q "CREATE ROLE dep5_${U}"
admin_q "GRANT dep5_${U} TO ${PROT_USER}"
allowed C5 "${ADMIN}" "DROP ROLE dep5_${U}"
# The upstream cascade must still have run: the protected user's reference is gone.
observe C5ref "SELECT count() FROM system.role_grants WHERE user_name = '${PROT_USER}' AND granted_role_name = 'dep5_${U}'"

echo '=== C6: negative control - the guard is narrow ==='
admin_q "CREATE ROLE dep6_${U}"
admin_q "GRANT dep6_${U} TO ${REG_USER}"
allowed C6 "${NOPERM}" "DROP ROLE dep6_${U}"
observe C6ref "SELECT count() FROM system.role_grants WHERE user_name = '${REG_USER}' AND granted_role_name = 'dep6_${U}'"

echo '=== C7: reached through grantees ==='
admin_q "CREATE USER dep7_${U}"
admin_q "ALTER USER ${PROT_USER} GRANTEES dep7_${U}"
out=$(as_user "${NOPERM}" "DROP USER dep7_${U}")
if [[ "${out}" == *ACCESS_DENIED* && "${out}" == *"PROTECTED ACCESS MANAGEMENT"* ]]; then
    echo "C7 denied"
elif [[ -z "${out}" ]]; then
    echo "C7 allowed"
else
    echo "C7 other: ${out}"
fi
echo "C7ref $(${CLICKHOUSE_CLIENT} --query "SHOW CREATE USER ${PROT_USER}" 2>&1 | grep -c "dep7_${U}")"

cleanup

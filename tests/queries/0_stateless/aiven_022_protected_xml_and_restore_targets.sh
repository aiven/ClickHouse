#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: RESTORE of `system.quotas`, `system.settings_profiles` and `system.row_policies`
# restores every such entity in the backup, and this test creates PROTECTED users.

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Aiven patch 022: the `TO` clause of a row policy, quota or settings profile may name a protected
# entity, or a user of a read-only storage such as `users.xml`, only with PROTECTED_ACCESS_MANAGEMENT.
# SQL cannot manage the users of `users.xml`, so it must not change what applies to them either.
# RESTORE writes these entities past the interpreters and needs the privilege for the same targets.

U="${CLICKHOUSE_TEST_UNIQUE_NAME}"
ADMIN="admin_${U}"
NOPERM="noperm_${U}"
PROT="prot_${U}"
REG="reg_${U}"
TBL="${CLICKHOUSE_DATABASE}.tbl_${U}"
QUOTA="quota_${U}"
PROFILE="profile_${U}"
POLICY="policy_${U}"
PW="aiven_022_synthetic_password"
# Backups outlive the test database, so a rerun in the same database needs fresh backup names.
B="${U}_${RANDOM}${RANDOM}"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "
        DROP QUOTA IF EXISTS ${QUOTA};
        DROP SETTINGS PROFILE IF EXISTS ${PROFILE};
        DROP ROW POLICY IF EXISTS ${POLICY} ON ${TBL};
        DROP USER IF EXISTS ${ADMIN}, ${NOPERM}, ${PROT}, ${REG};
        DROP TABLE IF EXISTS ${TBL};
    " 2>/dev/null
}
cleanup

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TBL} (x UInt8) ENGINE = Memory;
    CREATE USER ${ADMIN}  IDENTIFIED WITH plaintext_password BY '${PW}';
    CREATE USER ${NOPERM} IDENTIFIED WITH plaintext_password BY '${PW}';
    CREATE USER ${PROT} PROTECTED;
    CREATE USER ${REG};
    GRANT ACCESS MANAGEMENT ON *.* TO ${ADMIN}, ${NOPERM};
    GRANT READ ON DISK TO ${ADMIN}, ${NOPERM};
    GRANT PROTECTED ON *.* TO ${ADMIN};
"

as_user() {
    local user="$1"; shift
    ${CLICKHOUSE_CLIENT} --user "${user}" --password "${PW}" --query "$1" 2>&1
}

exists() {
    local column=name
    [[ "$1" == system.row_policies ]] && column=short_name
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM $1 WHERE ${column} = '$2'"
}

check_denied() {
    local id="$1" table="$2" name="$3" out="$4"
    if [[ "${out}" != *ACCESS_DENIED* ]]; then
        echo "${id} NOT DENIED: ${out}"
    elif ! grep -qE "PROTECTED[ _]ACCESS[ _]MANAGEMENT" <<< "${out}"; then
        echo "${id} denied for the wrong reason: ${out}"
    else
        echo "${id} denied, exists: $(exists "${table}" "${name}")"
    fi
}

check_allowed() {
    local id="$1" out="$2"
    echo "${id} ${out:-ok}"
}

# X: SQL targeting the `default` user, which the test server keeps in `users.xml`.
${CLICKHOUSE_CLIENT} --query "SELECT storage FROM system.users WHERE name = 'default'"
check_denied "X1 quota TO users.xml user" system.quotas "${QUOTA}" \
    "$(as_user "${NOPERM}" "CREATE QUOTA ${QUOTA} FOR INTERVAL 1 hour MAX queries = 1000 TO default")"
check_denied "X2 profile TO users.xml user" system.settings_profiles "${PROFILE}" \
    "$(as_user "${NOPERM}" "CREATE SETTINGS PROFILE ${PROFILE} SETTINGS max_threads = 1 TO default")"
check_denied "X3 row policy TO users.xml user" system.row_policies "${POLICY}" \
    "$(as_user "${NOPERM}" "CREATE ROW POLICY ${POLICY} ON ${TBL} USING 0 TO default")"
check_allowed "X4 quota TO regular user:" \
    "$(as_user "${NOPERM}" "CREATE QUOTA ${QUOTA} FOR INTERVAL 1 hour MAX queries = 1000 TO ${REG}")"
check_allowed "X5 privileged quota TO users.xml user:" \
    "$(as_user "${ADMIN}" "ALTER QUOTA ${QUOTA} TO default")"
${CLICKHOUSE_CLIENT} --query "DROP QUOTA ${QUOTA}"

restore_as() {
    local user="$1" table="$2" backup="$3" mode="$4"
    as_user "${user}" "RESTORE TABLE ${table} FROM ${backup} SETTINGS create_access = '${mode}' FORMAT Null"
}

# backup_quota <suffix> <targets>: back up a quota with the given `TO` set, then drop it.
backup_quota() {
    ${CLICKHOUSE_CLIENT} --query "CREATE QUOTA ${QUOTA} FOR INTERVAL 1 hour MAX queries = 1000 TO $2"
    ${CLICKHOUSE_CLIENT} --query "BACKUP TABLE system.quotas TO Disk('backups', '${B}_quota_$1') FORMAT Null"
    ${CLICKHOUSE_CLIENT} --query "DROP QUOTA ${QUOTA}"
}

# R: RESTORE of a quota whose `TO` set reaches a protected entity, a `users.xml` user or `ALL`.
backup_quota prot "${PROT}"
backup_quota xml default
backup_quota all ALL
backup_quota reg "${REG}"
check_denied "R1 restore quota TO protected" system.quotas "${QUOTA}" \
    "$(restore_as "${NOPERM}" system.quotas "Disk('backups', '${B}_quota_prot')" if-not-exists)"
check_denied "R2 restore quota TO users.xml user" system.quotas "${QUOTA}" \
    "$(restore_as "${NOPERM}" system.quotas "Disk('backups', '${B}_quota_xml')" if-not-exists)"
check_denied "R3 restore quota TO ALL" system.quotas "${QUOTA}" \
    "$(restore_as "${NOPERM}" system.quotas "Disk('backups', '${B}_quota_all')" if-not-exists)"
check_allowed "R4 restore quota TO regular user:" \
    "$(restore_as "${NOPERM}" system.quotas "Disk('backups', '${B}_quota_reg')" if-not-exists)"

# R5: replacing an existing quota whose stored `TO` set reaches a protected entity.
${CLICKHOUSE_CLIENT} --query "ALTER QUOTA ${QUOTA} TO ${PROT}"
check_denied "R5 replace quota stored TO protected" system.quotas "${QUOTA}" \
    "$(restore_as "${NOPERM}" system.quotas "Disk('backups', '${B}_quota_reg')" replace)"
echo "R5 still applies to the protected user: $(${CLICKHOUSE_CLIENT} --query "SELECT apply_to_list = ['${PROT}'] FROM system.quotas WHERE name = '${QUOTA}'")"
check_allowed "R6 privileged replace:" \
    "$(restore_as "${ADMIN}" system.quotas "Disk('backups', '${B}_quota_reg')" replace)"
echo "R6 now applies to the regular user: $(${CLICKHOUSE_CLIENT} --query "SELECT apply_to_list = ['${REG}'] FROM system.quotas WHERE name = '${QUOTA}'")"
${CLICKHOUSE_CLIENT} --query "DROP QUOTA ${QUOTA}"

# R7, R8: the same check for settings profiles and row policies.
${CLICKHOUSE_CLIENT} --query "CREATE SETTINGS PROFILE ${PROFILE} SETTINGS max_threads = 1 TO ${PROT}"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE system.settings_profiles TO Disk('backups', '${B}_profile') FORMAT Null"
${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE ${PROFILE}"
check_denied "R7 restore profile TO protected" system.settings_profiles "${PROFILE}" \
    "$(restore_as "${NOPERM}" system.settings_profiles "Disk('backups', '${B}_profile')" if-not-exists)"

${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY ${POLICY} ON ${TBL} USING 0 TO ${PROT}"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE system.row_policies TO Disk('backups', '${B}_policy') FORMAT Null"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY ${POLICY} ON ${TBL}"
check_denied "R8 restore row policy TO protected" system.row_policies "${POLICY}" \
    "$(restore_as "${NOPERM}" system.row_policies "Disk('backups', '${B}_policy')" if-not-exists)"

cleanup

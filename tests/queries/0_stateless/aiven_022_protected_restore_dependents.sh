#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: RESTORE of `system.users`, `system.roles` and `system.settings_profiles` restores
# every entity in the backup, not only the ones this test created, and this test creates PROTECTED users.

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Aiven patch 022: besides the entities it inserts, RESTORE rewrites existing entities to reference
# them (`update_access_entities_dependents`), and with `create_access = 'replace'` it gives the
# replaced entities new IDs, which removes the old ones from every entity that held them. Both change
# a protected user without naming it, so both require PROTECTED_ACCESS_MANAGEMENT. So does a restored
# quota whose `TO` set reaches a protected user through a role the user holds.

U="${CLICKHOUSE_TEST_UNIQUE_NAME}"
NOPERM="noperm_${U}"
PROT="prot_${U}"
P_LOCK="p_lock_${U}"
R_HELD="r_held_${U}"
R_FREE="r_free_${U}"
QUOTA="quota_${U}"
PW="aiven_022_synthetic_password"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "
        DROP QUOTA IF EXISTS ${QUOTA};
        DROP USER IF EXISTS ${NOPERM}, ${PROT};
        DROP ROLE IF EXISTS ${R_HELD}, ${R_FREE};
        DROP SETTINGS PROFILE IF EXISTS ${P_LOCK};
    " 2>/dev/null
}
cleanup

${CLICKHOUSE_CLIENT} --query "
    CREATE USER ${NOPERM} IDENTIFIED WITH plaintext_password BY '${PW}';
    GRANT ACCESS MANAGEMENT ON *.* TO ${NOPERM};
    GRANT READ ON DISK TO ${NOPERM};
    CREATE SETTINGS PROFILE ${P_LOCK} SETTINGS max_result_rows = 1000;
    CREATE USER ${PROT} PROTECTED SETTINGS PROFILE ${P_LOCK};
"

restore_as_noperm() {
    local tables="$1" backup="$2" settings="$3"
    ${CLICKHOUSE_CLIENT} --user "${NOPERM}" --password "${PW}" --query "
        RESTORE ${tables} FROM Disk('backups', '${backup}')
        SETTINGS restore_access_entities_with_current_grants = 1${settings:+, ${settings}}
        FORMAT Null" 2>&1
}

prot_state() {
    ${CLICKHOUSE_CLIENT} --query "
        SELECT inherit_profile FROM system.settings_profile_elements WHERE user_name = '${PROT}' ORDER BY ALL FORMAT TSV;
        SELECT granted_role_name FROM system.role_grants WHERE user_name = '${PROT}' ORDER BY ALL FORMAT TSV;
    "
}

check_denied() {
    local id="$1" before="$2" out="$3"
    if [[ "${out}" != *ACCESS_DENIED* ]]; then
        echo "${id} NOT DENIED: ${out}"
    elif ! grep -qE "PROTECTED[ _]ACCESS[ _]MANAGEMENT" <<< "${out}"; then
        echo "${id} denied for the wrong reason: ${out}"
    elif [[ "${before}" != "$(prot_state)" ]]; then
        echo "${id} denied but the protected user CHANGED"
    else
        echo "${id} denied, protected user unchanged"
    fi
}

# D1: the backup of the profile carries the protected user as a dependent, which the restore
# re-attaches to the restored profile.
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE system.settings_profiles TO Disk('backups', '${U}_profiles') FORMAT Null"
${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE ${P_LOCK}"
before=$(prot_state)
check_denied "D1 dependent" "${before}" "$(restore_as_noperm "TABLE system.settings_profiles" "${U}_profiles")"

# D2: without rewriting dependents the same restore is an ordinary one and stays allowed.
out=$(restore_as_noperm "TABLE system.settings_profiles" "${U}_profiles" "update_access_entities_dependents = 0")
echo "D2 without dependents: ${out:-ok}, protected user unchanged: $([[ "${before}" == "$(prot_state)" ]] && echo yes || echo no)"
${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE ${P_LOCK}"

# D3: the backup holds the user unprotected with the profile; the restore binds it to the existing,
# now protected user of the same name and rewrites that one.
${CLICKHOUSE_CLIENT} --query "
    CREATE SETTINGS PROFILE ${P_LOCK} SETTINGS max_result_rows = 1000;
    ALTER USER ${PROT} NOT PROTECTED SETTINGS PROFILE ${P_LOCK};
"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE system.users, TABLE system.settings_profiles TO Disk('backups', '${U}_users') FORMAT Null"
${CLICKHOUSE_CLIENT} --query "
    ALTER USER ${PROT} PROTECTED;
    DROP SETTINGS PROFILE ${P_LOCK};
"
before=$(prot_state)
check_denied "D3 bound to existing" "${before}" "$(restore_as_noperm "TABLE system.users, TABLE system.settings_profiles" "${U}_users")"

# D4: replacing a role that the protected user holds gives the role a new ID, so the user loses it.
${CLICKHOUSE_CLIENT} --query "
    CREATE ROLE ${R_HELD};
    GRANT ${R_HELD} TO ${PROT};
"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE system.roles TO Disk('backups', '${U}_roles') FORMAT Null"
before=$(prot_state)
check_denied "D4 replace held role" "${before}" "$(restore_as_noperm "TABLE system.roles" "${U}_roles" "create_access = 'replace'")"

# D5: a quota aimed at a role the protected user holds applies to that user, as `CREATE QUOTA` would.
${CLICKHOUSE_CLIENT} --query "CREATE QUOTA ${QUOTA} FOR INTERVAL 1 hour MAX queries = 1 TO ${R_HELD}"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE system.quotas TO Disk('backups', '${U}_quota_held') FORMAT Null"
${CLICKHOUSE_CLIENT} --query "DROP QUOTA ${QUOTA}"
before=$(prot_state)
check_denied "D5 quota TO held role" "${before}" "$(restore_as_noperm "TABLE system.quotas" "${U}_quota_held")"
echo "D5 quota restored: $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.quotas WHERE name = '${QUOTA}'")"

# D6: the same quota aimed at a role no protected entity holds stays allowed.
${CLICKHOUSE_CLIENT} --query "
    CREATE ROLE ${R_FREE};
    CREATE QUOTA ${QUOTA} FOR INTERVAL 1 hour MAX queries = 1 TO ${R_FREE};
"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE system.quotas TO Disk('backups', '${U}_quota_free') FORMAT Null"
${CLICKHOUSE_CLIENT} --query "DROP QUOTA ${QUOTA}"
out=$(restore_as_noperm "TABLE system.quotas" "${U}_quota_free")
echo "D6 quota TO free role: ${out:-ok}"

cleanup

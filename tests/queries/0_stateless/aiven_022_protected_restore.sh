#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: RESTORE of `system.users` restores every user in the backup, not only the
# ones this test created, and this test creates PROTECTED users.

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Aiven patch 022: RESTORE writes access entities straight into the storage, past the
# interpreters. It therefore requires PROTECTED_ACCESS_MANAGEMENT itself when it would create
# a protected entity, or, with `create_access = 'replace'`, overwrite an existing one.

U="${CLICKHOUSE_TEST_UNIQUE_NAME}"
NOPERM="noperm_${U}"
PROT="prot_${U}"
PW="aiven_022_synthetic_password"
BACKUP_PLAIN="Disk('backups', '${U}_plain')"
BACKUP_PROTECTED="Disk('backups', '${U}_protected')"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${NOPERM}, ${PROT}" 2>/dev/null
}
cleanup

# NOPERM can restore users in general: it may create and drop users, grant roles, and read
# the backup disk. `restore_access_entities_with_current_grants` below keeps the grants of
# other users in the backup from being the reason for a denial.
${CLICKHOUSE_CLIENT} --query "
    CREATE USER ${NOPERM} IDENTIFIED WITH plaintext_password BY '${PW}';
    GRANT ACCESS MANAGEMENT ON *.* TO ${NOPERM};
    GRANT READ ON DISK TO ${NOPERM};
    CREATE USER ${PROT} IDENTIFIED WITH plaintext_password BY '${PW}';
"

${CLICKHOUSE_CLIENT} --query "BACKUP TABLE system.users TO ${BACKUP_PLAIN} FORMAT Null"
${CLICKHOUSE_CLIENT} --query "ALTER USER ${PROT} PROTECTED"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE system.users TO ${BACKUP_PROTECTED} FORMAT Null"

restore_as() {
    local user="$1" backup="$2" mode="$3"
    local auth=()
    [[ "${user}" != default ]] && auth=(--user "${user}" --password "${PW}")
    ${CLICKHOUSE_CLIENT} "${auth[@]}" --query "
        RESTORE TABLE system.users FROM ${backup}
        SETTINGS create_access = '${mode}', restore_access_entities_with_current_grants = 1
        FORMAT Null" 2>&1
}

prot_state() {
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.users WHERE name = '${PROT}'"
    ${CLICKHOUSE_CLIENT} --query "SHOW CREATE USER ${PROT}" 2>/dev/null | grep -c ' PROTECTED'
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

# R1: replace an existing protected user with its unprotected definition from the backup.
before=$(prot_state)
check_denied "R1 replace existing protected" "${before}" "$(restore_as "${NOPERM}" "${BACKUP_PLAIN}" replace)"

# R2: the same restore by a principal holding the privilege goes through and unprotects it.
out=$(restore_as default "${BACKUP_PLAIN}" replace)
echo "R2 privileged replace: ${out:-ok}, protected flag now $(prot_state | tail -n1)"

# R3: recreate a dropped protected user from a backup that carries the flag.
${CLICKHOUSE_CLIENT} --query "DROP USER ${PROT}"
before=$(prot_state)
check_denied "R3 create protected" "${before}" "$(restore_as "${NOPERM}" "${BACKUP_PROTECTED}" if-not-exists)"

# R4: the same user without the flag is an ordinary restore and stays allowed.
out=$(restore_as "${NOPERM}" "${BACKUP_PLAIN}" if-not-exists)
echo "R4 unprivileged create unprotected: ${out:-ok}, user exists $(prot_state | head -n1)"

cleanup

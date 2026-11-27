#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: RESTORE of `system.users` and `system.settings_profiles` restores every entity in
# the backup, not only the ones this test created.

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Aiven patch 014: with `allow_non_default_profile = 0` locked, RESTORE checks the settings of what it restores
# against the constraints of the user running it, and every parent of a restored profile, like the SQL.

U="${CLICKHOUSE_TEST_UNIQUE_NAME}"
CR="creator_${U}"
PW="aiven_014_synthetic_password"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${CR}, u_writable_${U}, u_mixed_${U}, u_big_${U}, u_fits_${U}, u_mask_${U}" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE IF EXISTS mask_${U}, mixed_${U}, guard_${U}, elevated_${U}" 2>/dev/null
}
cleanup

${CLICKHOUSE_CLIENT} --query "
    CREATE SETTINGS PROFILE elevated_${U} SETTINGS max_threads = 3;
    CREATE SETTINGS PROFILE guard_${U} SETTINGS INHERIT default, allow_non_default_profile = 0 CONST, max_threads = 4 MAX 8;
    CREATE USER ${CR} IDENTIFIED WITH plaintext_password BY '${PW}' SETTINGS PROFILE guard_${U};
    GRANT ACCESS MANAGEMENT ON *.* TO ${CR} WITH GRANT OPTION;
    GRANT READ ON DISK TO ${CR};
"

backup() {
    ${CLICKHOUSE_CLIENT} --query "BACKUP $1 TO Disk('backups', '${U}_$2') FORMAT Null"
}

restore_as_creator() {
    local what="$1" name="$2"
    ${CLICKHOUSE_CLIENT} --user "${CR}" --password "${PW}" --query "
        RESTORE ${what} FROM Disk('backups', '${U}_${name}')
        SETTINGS create_access = 'create-if-not-exists', restore_access_entities_with_current_grants = 1
        FORMAT Null" 2>&1
}

check_rejected() {
    local id="$1" out="$2" message="$3"
    if [[ "${out}" != *SETTING_CONSTRAINT_VIOLATION* ]]; then
        echo "${id} NOT REJECTED: ${out}"
    elif [[ "${out}" != *"${message}"* ]]; then
        echo "${id} rejected for the wrong reason: ${out}"
    else
        echo "${id} rejected"
    fi
}

exists() {
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.$1 WHERE name = '$2'"
}

echo "-- a user whose own settings make the guard writable"
${CLICKHOUSE_CLIENT} --query "CREATE USER u_writable_${U} SETTINGS PROFILE default, allow_non_default_profile = 1 WRITABLE"
backup "TABLE system.users" writable
${CLICKHOUSE_CLIENT} --query "DROP USER u_writable_${U}"
check_rejected "restore writable guard" "$(restore_as_creator "TABLE system.users" writable)" "should not be changed"
echo "nothing restored: $(exists users "u_writable_${U}")"

echo "-- a user whose own setting is above the creator's bound"
${CLICKHOUSE_CLIENT} --query "CREATE USER u_big_${U} SETTINGS PROFILE default, max_threads = 100"
backup "TABLE system.users" big
${CLICKHOUSE_CLIENT} --query "DROP USER u_big_${U}"
check_rejected "restore above bound" "$(restore_as_creator "TABLE system.users" big)" "max_threads"
echo "nothing restored: $(exists users "u_big_${U}")"

echo "-- a user whose own setting fits the creator's bound"
${CLICKHOUSE_CLIENT} --query "CREATE USER u_fits_${U} SETTINGS PROFILE default, max_threads = 6"
backup "TABLE system.users" fits
${CLICKHOUSE_CLIENT} --query "DROP USER u_fits_${U}"
echo "restore within bound: $(restore_as_creator "TABLE system.users" fits)ok, exists $(exists users "u_fits_${U}")"

echo "-- a profile with one parent outside the default profile's tree"
${CLICKHOUSE_CLIENT} --query "
    CREATE SETTINGS PROFILE mixed_${U} SETTINGS INHERIT default, INHERIT elevated_${U};
    CREATE USER u_mixed_${U} SETTINGS PROFILE mixed_${U};
"
backup "TABLE system.settings_profiles, TABLE system.users" mixed
${CLICKHOUSE_CLIENT} --query "DROP USER u_mixed_${U}"
${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE mixed_${U}"
check_rejected "restore mixed" "$(restore_as_creator "TABLE system.settings_profiles, TABLE system.users" mixed)" "must be the default profile or inherit from it"
echo "nothing restored: $(exists users "u_mixed_${U}") $(exists settings_profiles "mixed_${U}")"

echo "-- a profile in the backup replaced by an existing profile of the same name, which the restore keeps"
${CLICKHOUSE_CLIENT} --query "
    CREATE SETTINGS PROFILE mask_${U} SETTINGS INHERIT elevated_${U};
    CREATE USER u_mask_${U} SETTINGS PROFILE mask_${U};
"
backup "TABLE system.settings_profiles, TABLE system.users" mask
${CLICKHOUSE_CLIENT} --query "DROP USER u_mask_${U}"
${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE mask_${U}"
${CLICKHOUSE_CLIENT} --query "CREATE SETTINGS PROFILE mask_${U} SETTINGS INHERIT default"
echo "restore with the existing profile: $(restore_as_creator "TABLE system.settings_profiles, TABLE system.users" mask)ok, exists $(exists users "u_mask_${U}")"
${CLICKHOUSE_CLIENT} --query "SELECT 'the kept profile inherits:', inherit_profile FROM system.settings_profile_elements WHERE profile_name = 'mask_${U}'" | sed -e "s/_${U}//g"

cleanup

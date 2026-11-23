#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: RESTORE of `system.users` and `system.settings_profiles` restores every entity in
# the backup, not only the ones this test created.

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Aiven patch 014: with `allow_non_default_profile = 0` locked, RESTORE only writes users, roles
# and settings profiles whose profiles are the default profile or inherit from it, like the SQL.

U="${CLICKHOUSE_TEST_UNIQUE_NAME}"
CR="creator_${U}"
PW="aiven_014_synthetic_password"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${CR}, u_good_${U}, u_bad_${U}, u_chain_${U}" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE IF EXISTS chain_b_${U}, chain_a_${U}, rootless_${U}, good_${U}, guard_${U}, elevated_${U}" 2>/dev/null
}
cleanup

${CLICKHOUSE_CLIENT} --query "
    CREATE SETTINGS PROFILE elevated_${U} SETTINGS max_threads = 3;
    CREATE SETTINGS PROFILE good_${U} SETTINGS INHERIT default;
    CREATE SETTINGS PROFILE guard_${U} SETTINGS INHERIT default, allow_non_default_profile = 0 CONST;
    CREATE USER ${CR} IDENTIFIED WITH plaintext_password BY '${PW}' SETTINGS PROFILE guard_${U};
    GRANT ACCESS MANAGEMENT ON *.* TO ${CR} WITH GRANT OPTION;
    GRANT READ ON DISK TO ${CR};
"

backup() {
    ${CLICKHOUSE_CLIENT} --query "BACKUP $1 TO Disk('backups', '${U}_$2') FORMAT Null"
}

restore_as() {
    local user="$1" what="$2" name="$3"
    local auth=()
    [[ "${user}" != default ]] && auth=(--user "${user}" --password "${PW}")
    ${CLICKHOUSE_CLIENT} "${auth[@]}" --query "
        RESTORE ${what} FROM Disk('backups', '${U}_${name}')
        SETTINGS create_access = 'create-if-not-exists', restore_access_entities_with_current_grants = 1
        FORMAT Null" 2>&1
}

check_rejected() {
    local id="$1" out="$2"
    if [[ "${out}" != *SETTING_CONSTRAINT_VIOLATION* ]]; then
        echo "${id} NOT REJECTED: ${out}"
    elif [[ "${out}" != *"must be the default profile or inherit from it"* ]]; then
        echo "${id} rejected for the wrong reason: ${out}"
    else
        echo "${id} rejected"
    fi
}

exists() {
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.$1 WHERE name = '$2'"
}

${CLICKHOUSE_CLIENT} --query "CREATE USER u_good_${U} SETTINGS PROFILE good_${U}"
backup "TABLE system.users" good_user
${CLICKHOUSE_CLIENT} --query "CREATE USER u_bad_${U} SETTINGS PROFILE elevated_${U}"
backup "TABLE system.users" bad_user
${CLICKHOUSE_CLIENT} --query "DROP USER u_good_${U}, u_bad_${U}"

echo "-- a user on a profile outside the default profile's tree"
check_rejected "restore user" "$(restore_as "${CR}" "TABLE system.users" bad_user)"
echo "nothing restored: $(exists users "u_good_${U}") $(exists users "u_bad_${U}")"
echo "restore user on a descendant: $(restore_as "${CR}" "TABLE system.users" good_user)ok, exists $(exists users "u_good_${U}")"
${CLICKHOUSE_CLIENT} --query "DROP USER u_good_${U}"

echo "-- a settings profile which does not inherit from the default profile"
${CLICKHOUSE_CLIENT} --query "CREATE SETTINGS PROFILE rootless_${U} SETTINGS max_threads = 2"
backup "TABLE system.settings_profiles" rootless
${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE rootless_${U}"
check_rejected "restore profile" "$(restore_as "${CR}" "TABLE system.settings_profiles" rootless)"
echo "nothing restored: $(exists settings_profiles "rootless_${U}")"

echo "-- a chain of profiles restored together with its user"
${CLICKHOUSE_CLIENT} --query "
    CREATE SETTINGS PROFILE chain_a_${U} SETTINGS INHERIT default;
    CREATE SETTINGS PROFILE chain_b_${U} SETTINGS INHERIT chain_a_${U};
    CREATE USER u_chain_${U} SETTINGS PROFILE chain_b_${U};
"
backup "TABLE system.settings_profiles, TABLE system.users" chain
${CLICKHOUSE_CLIENT} --query "DROP USER u_chain_${U}"
${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE chain_b_${U}, chain_a_${U}"
echo "restore chain: $(restore_as "${CR}" "TABLE system.settings_profiles, TABLE system.users" chain)ok, exists $(exists users "u_chain_${U}") $(exists settings_profiles "chain_a_${U}") $(exists settings_profiles "chain_b_${U}")"
${CLICKHOUSE_CLIENT} --query "SELECT 'user inherits the restored profile:', inherit_profile = 'chain_b_${U}' FROM system.settings_profile_elements WHERE user_name = 'u_chain_${U}'"

echo "-- without the guard any profile can be restored"
echo "restore by an unguarded user: $(restore_as default "TABLE system.users" bad_user)ok, exists $(exists users "u_bad_${U}")"

cleanup

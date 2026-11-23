#!/usr/bin/env bash

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Aiven patch 014: with `allow_non_default_profile = 0` locked, a principal managing access can
# only assign the default profile or the profiles inheriting from it, to users, roles and profiles.

U="${CLICKHOUSE_TEST_UNIQUE_NAME}"
CR="creator_${U}"
PW="aiven_014_synthetic_password"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${CR}, u_ok_${U}, u_child_${U}, u_alter_${U}, u_esc_${U}, u_set_${U}, u_writable_${U}" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "DROP ROLE IF EXISTS r_esc_${U}, r_ok_${U}" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE IF EXISTS p_esc_${U}, p_child_${U}, p_auto_${U}, p_alter_${U}, cyc_a_${U}, cyc_b_${U}, p_optout_${U}, guard_${U}, elevated_${U}" 2>/dev/null
}
cleanup

# `elevated` stands for a profile outside the default profile's tree; the default user of the test
# server is not guarded, so no default parent is injected into it.
${CLICKHOUSE_CLIENT} --query "
    CREATE SETTINGS PROFILE elevated_${U} SETTINGS max_threads = 3;
    CREATE SETTINGS PROFILE guard_${U} SETTINGS INHERIT default, allow_non_default_profile = 0 CONST;
    CREATE USER ${CR} IDENTIFIED WITH plaintext_password BY '${PW}' SETTINGS PROFILE guard_${U};
    GRANT ACCESS MANAGEMENT ON *.* TO ${CR} WITH GRANT OPTION;
"

as_creator() {
    ${CLICKHOUSE_CLIENT} --user "${CR}" --password "${PW}" --query "$1" 2>&1
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

check_ok() {
    local id="$1" out="$2"
    echo "${id} ${out:-ok}"
}

ESCAPE="must be the default profile or inherit from it"
LOCKED="should not be changed"

echo "-- assigning a profile outside the default profile's tree"
check_rejected "create user" "$(as_creator "CREATE USER u_esc_${U} SETTINGS PROFILE elevated_${U}")" "${ESCAPE}"
check_rejected "create role" "$(as_creator "CREATE ROLE r_esc_${U} SETTINGS PROFILE elevated_${U}")" "${ESCAPE}"
check_rejected "create profile" "$(as_creator "CREATE SETTINGS PROFILE p_esc_${U} SETTINGS INHERIT elevated_${U}")" "${ESCAPE}"
check_ok "create user to alter" "$(as_creator "CREATE USER u_alter_${U} SETTINGS PROFILE default")"
check_rejected "alter user" "$(as_creator "ALTER USER u_alter_${U} SETTINGS PROFILE elevated_${U}")" "${ESCAPE}"
check_ok "create role to alter" "$(as_creator "CREATE ROLE r_ok_${U} SETTINGS PROFILE default")"
check_rejected "alter role" "$(as_creator "ALTER ROLE r_ok_${U} SETTINGS PROFILE elevated_${U}")" "${ESCAPE}"
check_ok "create profile to alter" "$(as_creator "CREATE SETTINGS PROFILE p_alter_${U} SETTINGS INHERIT default")"
check_rejected "alter profile" "$(as_creator "ALTER SETTINGS PROFILE p_alter_${U} SETTINGS INHERIT elevated_${U}")" "${ESCAPE}"
check_rejected "replace profile" "$(as_creator "CREATE SETTINGS PROFILE OR REPLACE p_alter_${U} SETTINGS INHERIT elevated_${U}")" "${ESCAPE}"
${CLICKHOUSE_CLIENT} --query "SELECT 'nothing escaped:', count() FROM system.settings_profile_elements WHERE inherit_profile = 'elevated_${U}'"

echo "-- the guard itself cannot be lifted"
check_rejected "set" "$(as_creator "SET allow_non_default_profile = 1")" "${LOCKED}"
check_rejected "query setting" "$(${CLICKHOUSE_CLIENT} --user "${CR}" --password "${PW}" --allow_non_default_profile 1 --query "CREATE USER u_set_${U} SETTINGS PROFILE elevated_${U}" 2>&1)" "${LOCKED}"
check_rejected "user value" "$(as_creator "CREATE USER u_set_${U} SETTINGS allow_non_default_profile = 1")" "${LOCKED}"
check_rejected "user writable" "$(as_creator "CREATE USER u_writable_${U} SETTINGS allow_non_default_profile WRITABLE")" "${LOCKED}"
check_rejected "profile writable" "$(as_creator "CREATE SETTINGS PROFILE p_esc_${U} SETTINGS INHERIT default, allow_non_default_profile CHANGEABLE_IN_READONLY")" "${LOCKED}"
${CLICKHOUSE_CLIENT} --query "SELECT 'nothing created:', count() FROM system.users WHERE name IN ('u_esc_${U}', 'u_set_${U}', 'u_writable_${U}')"

echo "-- the default profile and its descendants stay assignable"
check_ok "create user on default" "$(as_creator "CREATE USER u_ok_${U} SETTINGS PROFILE default")"
check_ok "create profile on default" "$(as_creator "CREATE SETTINGS PROFILE p_child_${U} SETTINGS INHERIT default")"
check_ok "create user on a descendant" "$(as_creator "CREATE USER u_child_${U} SETTINGS PROFILE p_child_${U}")"

echo "-- a profile created without a parent inherits from the default profile"
check_ok "create profile without parent" "$(as_creator "CREATE SETTINGS PROFILE p_auto_${U} SETTINGS max_threads = 1")"
${CLICKHOUSE_CLIENT} --query "SELECT 'inherits default:', count() FROM system.settings_profile_elements WHERE profile_name = 'p_auto_${U}' AND inherit_profile = 'default'"

echo "-- an inheritance cycle is rejected, also when the guard is off"
${CLICKHOUSE_CLIENT} --query "
    CREATE SETTINGS PROFILE cyc_a_${U};
    CREATE SETTINGS PROFILE cyc_b_${U} SETTINGS INHERIT cyc_a_${U};
"
check_rejected "cycle" "$(${CLICKHOUSE_CLIENT} --query "ALTER SETTINGS PROFILE cyc_a_${U} SETTINGS INHERIT cyc_b_${U}" 2>&1)" "circular dependencies"

echo "-- without the guard any profile can be assigned"
check_ok "opt-out" "$(${CLICKHOUSE_CLIENT} --query "CREATE SETTINGS PROFILE p_optout_${U} SETTINGS INHERIT elevated_${U}" 2>&1)"

cleanup

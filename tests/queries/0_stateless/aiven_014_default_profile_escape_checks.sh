#!/usr/bin/env bash

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Aiven patch 014: with `allow_non_default_profile = 0` locked, a profile qualifies only if every chain of its
# parents ends at the default profile, and `ALTER SETTINGS PROFILE` keeps the parents a profile has.

U="${CLICKHOUSE_TEST_UNIQUE_NAME}"
CR="creator_${U}"
PW="aiven_014_synthetic_password"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${CR}, u_mixed_${U}, u_both_${U}" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE IF EXISTS p_alter_${U}, strict_${U}, c_mixed_${U}, both_${U}, d1_${U}, d2_${U}, mixed_${U}, guard_${U}, elevated_${U}" 2>/dev/null
}
cleanup

# The default user of the test server is not guarded, so it can create `mixed`, which inherits from the default
# profile and from `elevated`, a profile outside the default profile's tree.
${CLICKHOUSE_CLIENT} --query "
    CREATE SETTINGS PROFILE elevated_${U} SETTINGS max_threads = 3;
    CREATE SETTINGS PROFILE mixed_${U} SETTINGS INHERIT default, INHERIT elevated_${U};
    CREATE SETTINGS PROFILE guard_${U} SETTINGS INHERIT default, allow_non_default_profile = 0 CONST;
    CREATE USER ${CR} IDENTIFIED WITH plaintext_password BY '${PW}' SETTINGS PROFILE guard_${U};
    GRANT ACCESS MANAGEMENT ON *.* TO ${CR} WITH GRANT OPTION;
"

as_creator() {
    ${CLICKHOUSE_CLIENT} --user "${CR}" --password "${PW}" --query "$1" 2>&1
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

check_ok() {
    local id="$1" out="$2"
    echo "${id} ${out:-ok}"
}

show_create() {
    ${CLICKHOUSE_CLIENT} --query "SHOW CREATE SETTINGS PROFILE $1_${U}" | sed -e "s/_${U}//g"
}

echo "-- a profile with one parent outside the default profile's tree"
check_rejected "user on mixed" "$(as_creator "CREATE USER u_mixed_${U} SETTINGS PROFILE mixed_${U}")"
check_rejected "profile inheriting mixed" "$(as_creator "CREATE SETTINGS PROFILE c_mixed_${U} SETTINGS INHERIT mixed_${U}")"
${CLICKHOUSE_CLIENT} --query "SELECT 'nothing assigned:', count() FROM system.settings_profile_elements WHERE inherit_profile = 'mixed_${U}'"

echo "-- every chain ending at the default profile qualifies"
check_ok "create d1" "$(as_creator "CREATE SETTINGS PROFILE d1_${U} SETTINGS INHERIT default")"
check_ok "create d2" "$(as_creator "CREATE SETTINGS PROFILE d2_${U} SETTINGS INHERIT default")"
check_ok "create both" "$(as_creator "CREATE SETTINGS PROFILE both_${U} SETTINGS INHERIT d1_${U}, INHERIT d2_${U}")"
check_ok "user on both" "$(as_creator "CREATE USER u_both_${U} SETTINGS PROFILE both_${U}")"

echo "-- ALTER keeps the parents a profile has"
check_ok "create strict" "$(as_creator "CREATE SETTINGS PROFILE strict_${U} SETTINGS INHERIT default, max_threads = 2 MAX 2")"
check_ok "create p" "$(as_creator "CREATE SETTINGS PROFILE p_alter_${U} SETTINGS INHERIT strict_${U}")"
check_ok "alter p" "$(as_creator "ALTER SETTINGS PROFILE p_alter_${U} ADD SETTINGS max_block_size = 1000")"
show_create p_alter

echo "-- a profile left without a parent inherits from the default profile, first"
check_ok "drop parents" "$(as_creator "ALTER SETTINGS PROFILE p_alter_${U} DROP ALL PROFILES")"
show_create p_alter

cleanup

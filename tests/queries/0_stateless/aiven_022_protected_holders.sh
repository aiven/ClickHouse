#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: this test creates PROTECTED users and roles, and `TO ALL` resolves to every
# entity on the server. A concurrent test writing `TO ALL` would pick ours up and be denied
# for want of PROTECTED ACCESS MANAGEMENT.

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Aiven patch 022: a role or settings profile that a protected user holds, directly or through
# other roles and profiles, changes what applies to that user when it is altered, replaced,
# dropped or granted to. A row policy, quota or settings profile applies to users, so a role in
# its `TO` set counts through the users holding it.

U="${CLICKHOUSE_TEST_UNIQUE_NAME}"
ADMIN="admin_${U}"
NOPERM="noperm_${U}"
NOGRANT="nogrant_${U}"
PROT_USER="prot_user_${U}"
PROT_ROLE="prot_role_${U}"
REG_USER="reg_user_${U}"
TBL="${CLICKHOUSE_DATABASE}.tbl_${U}"
P_BASE="p_base_${U}"
P_HELD="p_held_${U}"
P_BASE2="p_base2_${U}"
P_CHILD="p_child_${U}"
P_FREE="p_free_${U}"
R_HELD="r_held_${U}"
R_INNER="r_inner_${U}"
R_FREE="r_free_${U}"
POLICY="policy_${U}"
PW="aiven_022_synthetic_password"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "
        DROP ROW POLICY IF EXISTS ${POLICY} ON ${TBL};
        DROP USER IF EXISTS ${ADMIN}, ${NOPERM}, ${NOGRANT}, ${PROT_USER}, ${REG_USER};
        DROP ROLE IF EXISTS ${PROT_ROLE}, ${R_HELD}, ${R_INNER}, ${R_FREE};
        DROP SETTINGS PROFILE IF EXISTS ${P_CHILD}, ${P_HELD}, ${P_BASE}, ${P_BASE2}, ${P_FREE};
        DROP TABLE IF EXISTS ${TBL};
    " 2>/dev/null
}
cleanup

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TBL} (x UInt8) ENGINE = Memory;
    INSERT INTO ${TBL} VALUES (1), (2);

    CREATE USER ${ADMIN}   IDENTIFIED WITH plaintext_password BY '${PW}';
    CREATE USER ${NOPERM}  IDENTIFIED WITH plaintext_password BY '${PW}';
    CREATE USER ${NOGRANT} IDENTIFIED WITH plaintext_password BY '${PW}';
    CREATE USER ${REG_USER} IDENTIFIED WITH plaintext_password BY '${PW}';
    CREATE USER ${PROT_USER} PROTECTED;
    CREATE ROLE ${PROT_ROLE} PROTECTED;
    GRANT ${PROT_ROLE} TO ${PROT_USER}, ${REG_USER};

    GRANT ACCESS MANAGEMENT ON *.* TO ${ADMIN}, ${NOPERM};
    GRANT PROTECTED ON *.* TO ${ADMIN};
    GRANT SELECT ON ${TBL} TO ${NOPERM}, ${ADMIN} WITH GRANT OPTION;
    GRANT SELECT ON ${TBL} TO ${REG_USER};

    CREATE SETTINGS PROFILE ${P_BASE};
    CREATE SETTINGS PROFILE ${P_HELD} SETTINGS PROFILE ${P_BASE};
    CREATE SETTINGS PROFILE ${P_BASE2};
    CREATE SETTINGS PROFILE ${P_CHILD} SETTINGS PROFILE ${P_BASE2} TO ${PROT_USER};
    CREATE SETTINGS PROFILE ${P_FREE};
    ALTER USER ${PROT_USER} SETTINGS PROFILE ${P_HELD};

    CREATE ROLE ${R_HELD};
    CREATE ROLE ${R_INNER};
    CREATE ROLE ${R_FREE};
    GRANT ${R_INNER} TO ${R_HELD};
    GRANT ${R_HELD} TO ${PROT_USER};
"

# The users of `users.xml` count as protected targets too, so the unprivileged `ALL EXCEPT` form lists them.
XML_USERS=$(${CLICKHOUSE_CLIENT} --query "SELECT arrayStringConcat(arraySort(groupArray(name)), ', ') FROM system.users WHERE storage = 'users_xml'")

as_user() {
    local user="$1"; shift
    ${CLICKHOUSE_CLIENT} --user "${user}" --password "${PW}" --query "$1" 2>&1
}

state() {
    ${CLICKHOUSE_CLIENT} --query "
        SELECT * FROM system.settings_profile_elements WHERE profile_name LIKE '%${U}' OR user_name LIKE '%${U}' ORDER BY ALL FORMAT TSV;
        SELECT * FROM system.role_grants WHERE user_name LIKE '%${U}' OR role_name LIKE '%${U}' ORDER BY ALL FORMAT TSV;
        SELECT * FROM system.grants WHERE user_name LIKE '%${U}' OR role_name LIKE '%${U}' ORDER BY ALL FORMAT TSV;
        SELECT name, apply_to_list FROM system.settings_profiles WHERE name LIKE '%${U}' ORDER BY ALL FORMAT TSV;
    " 2>&1
}

denied() {
    local id="$1" user="$2" query="$3"
    local before after out
    before=$(state)
    out=$(as_user "${user}" "${query}")
    after=$(state)
    if [[ "${out}" != *ACCESS_DENIED* ]]; then
        echo "${id} NOT DENIED: ${out}"
    elif ! grep -qE "PROTECTED[ _]ACCESS[ _]MANAGEMENT" <<< "${out}"; then
        echo "${id} denied for the wrong reason: ${out}"
    elif [[ "${before}" != "${after}" ]]; then
        echo "${id} denied but entities MUTATED"
    else
        echo "${id} denied, entities unchanged"
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

echo "=== settings profiles held by a protected user ==="
denied  "H1 alter held profile"                 "${NOPERM}" "ALTER SETTINGS PROFILE ${P_HELD} SETTINGS max_threads = 1"
denied  "H2 replace held profile"               "${NOPERM}" "CREATE SETTINGS PROFILE OR REPLACE ${P_HELD}"
denied  "H3 alter parent of held profile"       "${NOPERM}" "ALTER SETTINGS PROFILE ${P_BASE} SETTINGS max_threads = 1"
denied  "H4 drop parent of held profile"        "${NOPERM}" "DROP SETTINGS PROFILE ${P_BASE}"
denied  "H5 alter parent of profile TO protected" "${NOPERM}" "ALTER SETTINGS PROFILE ${P_BASE2} SETTINGS max_threads = 1"
allowed "H6 alter free profile"                 "${NOPERM}" "ALTER SETTINGS PROFILE ${P_FREE} SETTINGS max_threads = 1"
allowed "H7 admin alter held profile"           "${ADMIN}"  "ALTER SETTINGS PROFILE ${P_HELD} SETTINGS max_threads = 2"

echo "=== roles held by a protected user ==="
denied  "H8 alter held role"                    "${NOPERM}" "ALTER ROLE ${R_HELD} SETTINGS max_threads = 1"
denied  "H9 grant to role inside held role"     "${NOPERM}" "GRANT SELECT ON ${TBL} TO ${R_INNER}"
denied  "H10 drop role inside held role"        "${NOPERM}" "DROP ROLE ${R_INNER}"
allowed "H11 grant to free role"                "${NOPERM}" "GRANT SELECT ON ${TBL} TO ${R_FREE}"
allowed "H12 admin grant to held role"          "${ADMIN}"  "GRANT SELECT ON ${TBL} TO ${R_HELD}"

echo "=== TO sets count roles through the users holding them ==="
allowed "T1 TO ALL EXCEPT protected users" "${NOPERM}" \
    "CREATE ROW POLICY ${POLICY} ON ${TBL} USING x = 1 AS permissive TO ALL EXCEPT ${PROT_USER}, ${XML_USERS}"
echo "T2 rows seen by a user holding the protected role: $(as_user "${REG_USER}" "SELECT count() FROM ${TBL}")"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY IF EXISTS ${POLICY} ON ${TBL}"
denied  "T3 TO protected role held by a protected user" "${NOPERM}" \
    "CREATE ROW POLICY ${POLICY} ON ${TBL} USING x = 1 TO ${PROT_ROLE}"
allowed "T4 TO protected role EXCEPT its protected holder" "${NOPERM}" \
    "CREATE ROW POLICY ${POLICY} ON ${TBL} USING x = 1 TO ${PROT_ROLE} EXCEPT ${PROT_USER}"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY IF EXISTS ${POLICY} ON ${TBL}"

echo "=== MOVE of a row policy aimed at a protected user ==="
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY ${POLICY} ON ${TBL} USING 1 TO ${PROT_USER}"
OTHER_STORAGE=$(${CLICKHOUSE_CLIENT} --query "
    SELECT name FROM system.user_directories
    WHERE type IN ('memory', 'local_directory', 'replicated')
        AND name != (SELECT storage FROM system.row_policies WHERE short_name = '${POLICY}' AND database = '${CLICKHOUSE_DATABASE}')
    ORDER BY precedence LIMIT 1")
out=$(as_user "${NOPERM}" "MOVE ROW POLICY ${POLICY} ON ${TBL} TO ${OTHER_STORAGE}")
if [[ "${out}" == *ACCESS_DENIED* ]] && grep -qE "PROTECTED[ _]ACCESS[ _]MANAGEMENT" <<< "${out}"; then
    echo "M1 move denied"
else
    echo "M1 NOT DENIED: ${out}"
fi
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY IF EXISTS ${POLICY} ON ${TBL}"

echo "=== a caller without the grant learns nothing about the targets ==="
out=$(as_user "${NOGRANT}" "CREATE ROW POLICY ${POLICY} ON ${TBL} USING 1 TO ${PROT_USER}")
if [[ "${out}" == *ACCESS_DENIED* ]] && [[ "${out}" == *"CREATE ROW POLICY"* ]] && ! grep -qE "PROTECTED[ _]ACCESS[ _]MANAGEMENT" <<< "${out}"; then
    echo "O1 denied for the missing grant"
else
    echo "O1 WRONG: ${out}"
fi

cleanup

#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: this test creates PROTECTED users and roles, and `TO ALL` resolves to every
# entity on the server. A concurrent test writing `TO ALL` would pick ours up and be denied
# for want of PROTECTED ACCESS MANAGEMENT.

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Aiven patch 022, the statement forms the other 022 tests do not reach:
#   A - the authentication clauses of ALTER USER are checked per target user, not per clause;
#   B - CREATE ... OR REPLACE needs DROP (upstream) and PROTECTED (022), each reported on its own;
#   C - under EXECUTE AS the impersonated user is checked, not the caller who holds PROTECTED.

U="${CLICKHOUSE_TEST_UNIQUE_NAME}"
ADMIN="admin_${U}"
NOPERM="noperm_${U}"
CREATOR="creator_${U}"
TARGET="target_${U}"
PROT="prot_${U}"
PROT_ROLE="prot_role_${U}"
PW="aiven_022_synthetic_password"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "
        DROP ROLE IF EXISTS ${PROT_ROLE};
        DROP USER IF EXISTS ${ADMIN}, ${NOPERM}, ${CREATOR}, ${TARGET}, ${PROT};
    " 2>/dev/null
}
cleanup

${CLICKHOUSE_CLIENT} --query "
    CREATE USER ${ADMIN}   IDENTIFIED WITH plaintext_password BY '${PW}';
    CREATE USER ${NOPERM}  IDENTIFIED WITH plaintext_password BY '${PW}';
    CREATE USER ${CREATOR} IDENTIFIED WITH plaintext_password BY '${PW}';
    CREATE USER ${TARGET};
    CREATE USER ${PROT} IDENTIFIED WITH plaintext_password BY '${PW}' PROTECTED;
    CREATE ROLE ${PROT_ROLE} PROTECTED;

    GRANT ACCESS MANAGEMENT ON *.* TO ${ADMIN}, ${NOPERM};
    GRANT PROTECTED ON *.* TO ${ADMIN};
    GRANT CREATE USER, CREATE ROLE ON *.* TO ${CREATOR};
    GRANT ALTER USER ON *.* TO ${TARGET};
    GRANT IMPERSONATE ON ${TARGET} TO ${ADMIN};
"

as_user() {
    local user="$1"; shift
    ${CLICKHOUSE_CLIENT} --user "${user}" --password "${PW}" --query "$1" 2>&1
}

entity_state() {
    ${CLICKHOUSE_CLIENT} --query "SHOW CREATE USER ${PROT}; SHOW CREATE ROLE ${PROT_ROLE}" 2>&1
}

# Denied naming `$4` (a regex), with both protected entities unchanged.
denied_for() {
    local id="$1" user="$2" query="$3" reason="$4"
    local before after out
    before=$(entity_state)
    out=$(as_user "${user}" "${query}")
    after=$(entity_state)
    if [[ "${out}" != *ACCESS_DENIED* ]]; then
        echo "${id} NOT DENIED: ${out}"
    elif ! grep -qE "${reason}" <<< "${out}"; then
        echo "${id} denied for the wrong reason: ${out}"
    elif [[ "${before}" != "${after}" ]]; then
        echo "${id} denied but a protected entity CHANGED"
    else
        echo "${id} denied, protected entities unchanged"
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

PROTECTED_RE="PROTECTED[ _]ACCESS[ _]MANAGEMENT"

echo "=== A: authentication clauses ==="
denied_for "A1 ADD IDENTIFIED" "${NOPERM}" \
    "ALTER USER ${PROT} ADD IDENTIFIED WITH plaintext_password BY 'second'" "${PROTECTED_RE}"
denied_for "A2 RESET AUTHENTICATION METHODS" "${NOPERM}" \
    "ALTER USER ${PROT} RESET AUTHENTICATION METHODS TO NEW" "${PROTECTED_RE}"
denied_for "A3 VALID UNTIL" "${NOPERM}" \
    "ALTER USER ${PROT} VALID UNTIL '2000-01-01'" "${PROTECTED_RE}"
allowed "A4 admin ADD IDENTIFIED" "${ADMIN}" \
    "ALTER USER ${PROT} ADD IDENTIFIED WITH plaintext_password BY 'second'"

echo "=== B: OR REPLACE ==="
# B1: without DROP, upstream's check refuses first and names DROP.
denied_for "B1 user, no DROP"  "${CREATOR}" "CREATE USER OR REPLACE ${PROT}"      "DROP USER"
denied_for "B1 role, no DROP"  "${CREATOR}" "CREATE ROLE OR REPLACE ${PROT_ROLE}" "DROP ROLE"
# B2: with CREATE and DROP but without PROTECTED, it is 022 that refuses.
${CLICKHOUSE_CLIENT} --query "GRANT DROP USER, DROP ROLE ON *.* TO ${CREATOR}"
denied_for "B2 user, no PROTECTED" "${CREATOR}" "CREATE USER OR REPLACE ${PROT}"      "${PROTECTED_RE}"
denied_for "B2 role, no PROTECTED" "${CREATOR}" "CREATE ROLE OR REPLACE ${PROT_ROLE}" "${PROTECTED_RE}"

echo "=== C: EXECUTE AS ==="
# ADMIN holds PROTECTED, the impersonated TARGET holds ALTER USER only.
denied_for "C1 impersonated alter" "${ADMIN}" \
    "EXECUTE AS ${TARGET} ALTER USER ${PROT} SETTINGS max_threads = 1" "${PROTECTED_RE}"
allowed "C2 direct alter" "${ADMIN}" "ALTER USER ${PROT} SETTINGS max_threads = 1"

cleanup

#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: this test creates PROTECTED roles, and `TO ALL` resolves to every entity on
# the server. A concurrent test writing `TO ALL` would pick ours up and be denied for want
# of PROTECTED ACCESS MANAGEMENT.

# Keep the server log stream out of the captured output: these tests assert on
# error text, and interleaved trace lines would swamp it.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Aiven patch 022, role half. A role marked PROTECTED is gated by the same
# PROTECTED_ACCESS_MANAGEMENT privilege as a protected user: it cannot be dropped,
# renamed, replaced, re-settinged, un-protected, moved between storages, or have
# its grants changed by a principal that does not hold the privilege.
#
# Roles have no self-protection dimension - a role is not a principal - so the
# matrix here is the entity half of the user matrix, plus the two contract cases
# the control plane depends on: `CREATE ROLE IF NOT EXISTS ... PROTECTED` run
# repeatedly, and the flag surviving an unrelated ALTER.

U="${CLICKHOUSE_TEST_UNIQUE_NAME}"
ADMIN="admin_${U}"
NOPERM="noperm_${U}"
PROT_ROLE="prot_role_${U}"
REG_ROLE="reg_role_${U}"
NEW_ROLE="new_role_${U}"
REG_USER="reg_user_${U}"
CLUSTER="test_shard_localhost"
PW="aiven_022_synthetic_password"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "
        DROP ROLE IF EXISTS ${PROT_ROLE}, ${REG_ROLE}, ${NEW_ROLE}, ${PROT_ROLE}_r, ${NEW_ROLE}_r;
        DROP USER IF EXISTS ${ADMIN}, ${NOPERM}, ${REG_USER};
    " 2>/dev/null
}
cleanup

${CLICKHOUSE_CLIENT} --query "
    CREATE USER ${ADMIN}  IDENTIFIED WITH plaintext_password BY '${PW}';
    CREATE USER ${NOPERM} IDENTIFIED WITH plaintext_password BY '${PW}';
    CREATE USER ${REG_USER};
    CREATE ROLE ${PROT_ROLE} PROTECTED;
    CREATE ROLE ${REG_ROLE};

    GRANT ACCESS MANAGEMENT ON *.* TO ${ADMIN}, ${NOPERM};
    GRANT ROLE ADMIN ON *.* TO ${ADMIN}, ${NOPERM};
    GRANT CLUSTER ON *.* TO ${ADMIN}, ${NOPERM};
    GRANT SELECT, SHOW ON *.* TO ${ADMIN}, ${NOPERM} WITH GRANT OPTION;
    GRANT PROTECTED ON *.* TO ${ADMIN};

    -- So the REVOKE arm has something to take away.
    GRANT SHOW ON *.* TO ${PROT_ROLE};
"

as_user() {
    local user="$1"; shift
    ${CLICKHOUSE_CLIENT} --user "${user}" --password "${PW}" --query "$1" 2>&1
}

fingerprint() {
    ${CLICKHOUSE_CLIENT} --query "SHOW CREATE ROLE $1" 2>&1
    ${CLICKHOUSE_CLIENT} --query "SELECT storage FROM system.roles WHERE name = '$1' FORMAT TSV" 2>&1
    ${CLICKHOUSE_CLIENT} --query "SELECT access_type, database, table FROM system.grants WHERE role_name = '$1' ORDER BY access_type, database, table FORMAT TSV" 2>&1
    ${CLICKHOUSE_CLIENT} --query "SELECT granted_role_name FROM system.role_grants WHERE role_name = '$1' ORDER BY granted_role_name FORMAT TSV" 2>&1
}

denied() {
    local id="$1" user="$2" target="$3" query="$4"
    local before after out
    before=$(fingerprint "${target}")
    out=$(as_user "${user}" "${query}")
    after=$(fingerprint "${target}")
    if [[ "${out}" != *ACCESS_DENIED* ]]; then
        echo "${id} NOT DENIED: ${out}"
    elif ! grep -qE "PROTECTED[ _]ACCESS[ _]MANAGEMENT" <<< "${out}"; then
        echo "${id} denied for the wrong reason: ${out}"
    elif [[ "${before}" != "${after}" ]]; then
        echo "${id} denied but target MUTATED"
    else
        echo "${id} denied, target unchanged"
    fi
}

denied_no_target() {
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

is_protected() {
    local out
    out=$(${CLICKHOUSE_CLIENT} --query "SHOW CREATE ROLE $1" 2>&1)
    if [[ "${out}" == *"NOT PROTECTED"* ]]; then
        echo 0
    elif [[ "${out}" == *PROTECTED* ]]; then
        echo 1
    else
        echo 0
    fi
}

observe() {
    local id="$1" query="$2"
    echo "${id} $(${CLICKHOUSE_CLIENT} --query "${query}" 2>&1)"
}

echo '=== denied for a principal with ACCESS MANAGEMENT but not PROTECTED ==='

denied R1  "${NOPERM}" "${PROT_ROLE}" "DROP ROLE ${PROT_ROLE}"
denied R1c "${NOPERM}" "${PROT_ROLE}" "DROP ROLE ${PROT_ROLE} ON CLUSTER ${CLUSTER}"
denied R2  "${NOPERM}" "${PROT_ROLE}" "ALTER ROLE ${PROT_ROLE} RENAME TO ${PROT_ROLE}_r"
denied R2c "${NOPERM}" "${PROT_ROLE}" "ALTER ROLE ${PROT_ROLE} ON CLUSTER ${CLUSTER} RENAME TO ${PROT_ROLE}_r"
denied R3  "${NOPERM}" "${PROT_ROLE}" "CREATE ROLE OR REPLACE ${PROT_ROLE}"
denied R3c "${NOPERM}" "${PROT_ROLE}" "CREATE ROLE OR REPLACE ${PROT_ROLE} ON CLUSTER ${CLUSTER}"
denied R4  "${NOPERM}" "${PROT_ROLE}" "ALTER ROLE ${PROT_ROLE} SETTINGS async_insert = 1"
denied R4c "${NOPERM}" "${PROT_ROLE}" "ALTER ROLE ${PROT_ROLE} ON CLUSTER ${CLUSTER} SETTINGS async_insert = 1"
denied R5  "${NOPERM}" "${PROT_ROLE}" "ALTER ROLE ${PROT_ROLE} NOT PROTECTED"
denied R5c "${NOPERM}" "${PROT_ROLE}" "ALTER ROLE ${PROT_ROLE} ON CLUSTER ${CLUSTER} NOT PROTECTED"
denied R6  "${NOPERM}" "${REG_ROLE}" "ALTER ROLE ${REG_ROLE} PROTECTED"
denied R6c "${NOPERM}" "${REG_ROLE}" "ALTER ROLE ${REG_ROLE} ON CLUSTER ${CLUSTER} PROTECTED"
denied_no_target R7  "${NOPERM}" "CREATE ROLE ${NEW_ROLE} PROTECTED"
denied_no_target R7c "${NOPERM}" "CREATE ROLE ${NEW_ROLE} ON CLUSTER ${CLUSTER} PROTECTED"
denied R8  "${NOPERM}" "${PROT_ROLE}" "GRANT SELECT ON *.* TO ${PROT_ROLE}"
denied R8c "${NOPERM}" "${PROT_ROLE}" "GRANT SELECT ON *.* TO ${PROT_ROLE} ON CLUSTER ${CLUSTER}"
denied R9  "${NOPERM}" "${PROT_ROLE}" "REVOKE SHOW ON *.* FROM ${PROT_ROLE}"
denied R9c "${NOPERM}" "${PROT_ROLE}" "REVOKE ON CLUSTER ${CLUSTER} SHOW ON *.* FROM ${PROT_ROLE}"
denied R10  "${NOPERM}" "${PROT_ROLE}" "MOVE ROLE ${PROT_ROLE} TO memory"
denied R10c "${NOPERM}" "${PROT_ROLE}" "MOVE ROLE ${PROT_ROLE} TO memory ON CLUSTER ${CLUSTER}"

echo '=== the positive counterparts, for a principal holding PROTECTED ==='

# R11-R16 mirror R1-R7; R5 and R6 are the two directions of one toggle, so their
# positive counterpart is the single R15 arm. R8p/R9p are the grant and revoke
# positives, which the brief folds into the same block.
allowed R16 "${ADMIN}" "CREATE ROLE ${NEW_ROLE} PROTECTED"
echo "R16flag $(is_protected "${NEW_ROLE}")"
allowed R12 "${ADMIN}" "ALTER ROLE ${NEW_ROLE} RENAME TO ${NEW_ROLE}_r"
observe  R12name "SELECT count() FROM system.roles WHERE name = '${NEW_ROLE}_r'"
allowed R13 "${ADMIN}" "CREATE ROLE OR REPLACE ${NEW_ROLE}_r PROTECTED"
echo "R13flag $(is_protected "${NEW_ROLE}_r")"
allowed R14 "${ADMIN}" "ALTER ROLE ${NEW_ROLE}_r SETTINGS async_insert = 1"
allowed R15a "${ADMIN}" "ALTER ROLE ${REG_ROLE} PROTECTED"
echo "R15a $(is_protected "${REG_ROLE}")"
allowed R15b "${ADMIN}" "ALTER ROLE ${REG_ROLE} NOT PROTECTED"
echo "R15b $(is_protected "${REG_ROLE}")"
allowed R8p "${ADMIN}" "GRANT SELECT ON *.* TO ${PROT_ROLE}"
allowed R9p "${ADMIN}" "REVOKE SELECT ON *.* FROM ${PROT_ROLE}"
allowed R11 "${ADMIN}" "DROP ROLE ${NEW_ROLE}_r"
observe  R11gone "SELECT count() FROM system.roles WHERE name = '${NEW_ROLE}_r'"

echo '=== granting a protected role to an ordinary user modifies the user ==='

# R17: the entity being rewritten is the *user*, not the protected role, so this
# is allowed. Pinned here so a future change to the grantee-side check is visible.
allowed R17 "${NOPERM}" "GRANT ${PROT_ROLE} TO ${REG_USER}"
observe  R17grant "SELECT count() FROM system.role_grants WHERE user_name = '${REG_USER}' AND granted_role_name = '${PROT_ROLE}'"

echo '=== the production IF NOT EXISTS form, and the flag round-trip ==='

allowed R18a "${ADMIN}" "CREATE ROLE IF NOT EXISTS ${NEW_ROLE} PROTECTED"
echo "R18a $(is_protected "${NEW_ROLE}")"
allowed R18b "${ADMIN}" "CREATE ROLE IF NOT EXISTS ${NEW_ROLE} PROTECTED"
echo "R18b $(is_protected "${NEW_ROLE}")"

echo "R19show $(is_protected "${PROT_ROLE}")"
allowed R19alter "${ADMIN}" "ALTER ROLE ${PROT_ROLE} SETTINGS async_insert = 1"
echo "R19survives $(is_protected "${PROT_ROLE}")"
ROUNDTRIP=$(${CLICKHOUSE_CLIENT} --query "SHOW CREATE ROLE ${PROT_ROLE}" 2>&1 | sed "s/^CREATE ROLE /CREATE ROLE OR REPLACE /")
RT_OUT=$(${CLICKHOUSE_CLIENT} --query "${ROUNDTRIP}" 2>&1)
if [[ -z "${RT_OUT}" ]]; then echo "R19rt ok"; else echo "R19rt FAILED: ${RT_OUT}"; fi
echo "R19roundtrip $(is_protected "${PROT_ROLE}")"

cleanup

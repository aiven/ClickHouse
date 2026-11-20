#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: this test creates PROTECTED users, and `TO ALL` resolves to every entity on
# the server. A concurrent test writing `TO ALL` would pick ours up and be denied for want
# of PROTECTED ACCESS MANAGEMENT.

# Keep the server log stream out of the captured output: these tests assert on
# error text, and interleaved trace lines would swamp it.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Aiven patch 022: a user marked PROTECTED may only be created, altered, renamed,
# replaced, dropped, moved or re-granted by a principal holding
# PROTECTED_ACCESS_MANAGEMENT (SQL keyword PROTECTED). Self-protection is
# unconditional: nobody may drop, replace or revoke from themselves, even holding
# the privilege.
#
# Three actors, all created by the test's default user (which holds ALL, and so
# holds PROTECTED):
#
#   ADMIN   - granted PROTECTED ON *.*, i.e. holds PROTECTED_ACCESS_MANAGEMENT
#   NOPERM  - granted ACCESS MANAGEMENT ON *.* but NOT PROTECTED. PROTECTED lives
#             under ALL rather than under ACCESS MANAGEMENT, so granting a tenant
#             access management does not hand it the privilege.
#   PROT    - the protected target; REG is its unprotected twin.
#
# Every denial arm asserts two things: the statement raises ACCESS_DENIED naming
# the privilege, *and* the target's definition is byte-identical afterwards. A
# denial that leaves the entity mutated is the failure mode that matters, and an
# error-code-only assertion cannot see it. Two spellings are accepted, because
# the privilege check renders the SQL name `PROTECTED ACCESS MANAGEMENT` while
# the self-protection messages name the C++ identifier.

U="${CLICKHOUSE_TEST_UNIQUE_NAME}"
ADMIN="admin_${U}"
NOPERM="noperm_${U}"
PROT="prot_${U}"
PROT2="prot2_${U}"
REG="reg_${U}"
NEW="new_${U}"
PROFILE="profile_${U}"
CLUSTER="test_shard_localhost"
PW="aiven_022_synthetic_password"

# A synthetic sha256 hash/salt pair, in the exact shape the control plane emits.
HASH="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
SALT="aiven022salt"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "
        DROP USER IF EXISTS ${ADMIN}, ${NOPERM}, ${PROT}, ${PROT2}, ${REG}, ${NEW}, rt_${U};
        DROP SETTINGS PROFILE IF EXISTS ${PROFILE};
    " 2>/dev/null
}
cleanup

${CLICKHOUSE_CLIENT} --query "
    CREATE SETTINGS PROFILE ${PROFILE};

    CREATE USER ${ADMIN}  IDENTIFIED WITH plaintext_password BY '${PW}';
    CREATE USER ${NOPERM} IDENTIFIED WITH plaintext_password BY '${PW}';
    CREATE USER ${PROT} PROTECTED;
    CREATE USER ${REG};

    GRANT ACCESS MANAGEMENT ON *.* TO ${ADMIN}, ${NOPERM};
    GRANT CLUSTER ON *.* TO ${ADMIN}, ${NOPERM};
    -- REVOKE only works for a privilege the actor holds WITH GRANT OPTION, so
    -- grant it here; otherwise the revoke arms would be denied for the wrong reason.
    GRANT SELECT, SHOW ON *.* TO ${ADMIN}, ${NOPERM} WITH GRANT OPTION;
    -- Only ADMIN holds the extra privilege this patch introduces.
    GRANT PROTECTED ON *.* TO ${ADMIN};

    GRANT SELECT ON *.* TO ${PROT};
"

as_user() {
    local user="$1"; shift
    ${CLICKHOUSE_CLIENT} --user "${user}" --password "${PW}" --query "$1" 2>&1
}

# Everything that can be observed about a user from outside: its definition
# (which renders PROTECTED), the storage it lives in, and its grants. Used to
# prove a denied statement left the target completely untouched.
fingerprint() {
    ${CLICKHOUSE_CLIENT} --query "SHOW CREATE USER $1" 2>&1
    ${CLICKHOUSE_CLIENT} --query "SELECT storage, auth_type FROM system.users WHERE name = '$1' ORDER BY name FORMAT TSV" 2>&1
    ${CLICKHOUSE_CLIENT} --query "SELECT access_type, database, table FROM system.grants WHERE user_name = '$1' ORDER BY access_type, database, table FORMAT TSV" 2>&1
    ${CLICKHOUSE_CLIENT} --query "SELECT granted_role_name FROM system.role_grants WHERE user_name = '$1' ORDER BY granted_role_name FORMAT TSV" 2>&1
}

# Denied, and the target did not move.
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

# Denied because the target does not exist yet, so there is nothing to compare.
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

# `echo` of a scalar SELECT, so the reference pins the observed value.
observe() {
    local id="$1" query="$2"
    echo "${id} $(${CLICKHOUSE_CLIENT} --query "${query}" 2>&1)"
}

# 1 when SHOW CREATE USER renders the keyword, 0 otherwise. Protection is meant
# to be visible rather than a hidden property, so this doubles as the assertion
# that SHOW CREATE emits it.
is_protected() {
    local out
    out=$(${CLICKHOUSE_CLIENT} --query "SHOW CREATE USER $1" 2>&1)
    if [[ "${out}" == *"NOT PROTECTED"* ]]; then
        echo 0
    elif [[ "${out}" == *PROTECTED* ]]; then
        echo 1
    else
        echo 0
    fi
}

echo '=== denied for a principal with ACCESS MANAGEMENT but not PROTECTED ==='

denied U1  "${NOPERM}" "${PROT}" "DROP USER ${PROT}"
denied U1c "${NOPERM}" "${PROT}" "DROP USER ${PROT} ON CLUSTER ${CLUSTER}"
denied U2  "${NOPERM}" "${PROT}" "ALTER USER ${PROT} RENAME TO ${NEW}"
denied U2c "${NOPERM}" "${PROT}" "ALTER USER ${PROT} ON CLUSTER ${CLUSTER} RENAME TO ${NEW}"
denied U3  "${NOPERM}" "${PROT}" "CREATE USER OR REPLACE ${PROT} IDENTIFIED WITH sha256_password BY 'x'"
denied U3c "${NOPERM}" "${PROT}" "CREATE USER OR REPLACE ${PROT} ON CLUSTER ${CLUSTER} IDENTIFIED WITH sha256_password BY 'x'"
denied U4  "${NOPERM}" "${PROT}" "REVOKE ALL ON *.* FROM ${PROT}"
denied U4c "${NOPERM}" "${PROT}" "REVOKE ON CLUSTER ${CLUSTER} ALL ON *.* FROM ${PROT}"
denied U5  "${NOPERM}" "${PROT}" "GRANT SELECT ON *.* TO ${PROT}"
denied U5c "${NOPERM}" "${PROT}" "GRANT SELECT ON *.* TO ${PROT} ON CLUSTER ${CLUSTER}"
denied U6  "${NOPERM}" "${PROT}" "ALTER USER ${PROT} IDENTIFIED WITH plaintext_password BY 'x'"
denied U6c "${NOPERM}" "${PROT}" "ALTER USER ${PROT} ON CLUSTER ${CLUSTER} IDENTIFIED WITH plaintext_password BY 'x'"
denied U7  "${NOPERM}" "${PROT}" "ALTER USER ${PROT} SETTINGS async_insert = 1"
denied U7c "${NOPERM}" "${PROT}" "ALTER USER ${PROT} ON CLUSTER ${CLUSTER} SETTINGS async_insert = 1"
denied U8  "${NOPERM}" "${PROT}" "ALTER USER ${PROT} SETTINGS PROFILE '${PROFILE}'"
denied U8c "${NOPERM}" "${PROT}" "ALTER USER ${PROT} ON CLUSTER ${CLUSTER} SETTINGS PROFILE '${PROFILE}'"
denied U9  "${NOPERM}" "${PROT}" "ALTER USER ${PROT} NOT PROTECTED"
denied U9c "${NOPERM}" "${PROT}" "ALTER USER ${PROT} ON CLUSTER ${CLUSTER} NOT PROTECTED"
denied U10  "${NOPERM}" "${REG}" "ALTER USER ${REG} PROTECTED"
denied U10c "${NOPERM}" "${REG}" "ALTER USER ${REG} ON CLUSTER ${CLUSTER} PROTECTED"

denied_no_target U11  "${NOPERM}" "CREATE USER ${NEW} PROTECTED"
denied_no_target U11c "${NOPERM}" "CREATE USER ${NEW} ON CLUSTER ${CLUSTER} PROTECTED"
# The keyword is accepted on both sides of an IDENTIFIED clause; the check must
# fire either way. Production emits it after, the control plane's tests before.
denied_no_target U12  "${NOPERM}" "CREATE USER ${NEW} PROTECTED IDENTIFIED WITH sha256_password BY 'x'"
denied_no_target U12c "${NOPERM}" "CREATE USER ${NEW} ON CLUSTER ${CLUSTER} PROTECTED IDENTIFIED WITH sha256_password BY 'x'"
denied_no_target U13  "${NOPERM}" "CREATE USER ${NEW} IDENTIFIED WITH sha256_password BY 'x' PROTECTED"
denied_no_target U13c "${NOPERM}" "CREATE USER ${NEW} ON CLUSTER ${CLUSTER} IDENTIFIED WITH sha256_password BY 'x' PROTECTED"

denied U14  "${NOPERM}" "${PROT}" "MOVE USER ${PROT} TO memory"
denied U14c "${NOPERM}" "${PROT}" "MOVE USER ${PROT} TO memory ON CLUSTER ${CLUSTER}"
denied U15  "${NOPERM}" "${PROT}" "ALTER USER ${PROT} GRANTEES ${REG}"
denied U15c "${NOPERM}" "${PROT}" "ALTER USER ${PROT} ON CLUSTER ${CLUSTER} GRANTEES ${REG}"

echo '=== the same statements are allowed for a principal holding PROTECTED ==='

allowed U16 "${ADMIN}" "CREATE USER ${NEW} PROTECTED"
echo "U16flag $(is_protected "${NEW}")"
allowed U17 "${ADMIN}" "ALTER USER ${REG} PROTECTED"
echo "U17flag $(is_protected "${REG}")"
allowed U18 "${ADMIN}" "ALTER USER ${REG} NOT PROTECTED"
echo "U18flag $(is_protected "${REG}")"
allowed U19 "${ADMIN}" "CREATE USER OR REPLACE ${PROT2} PROTECTED"
echo "U19flag $(is_protected "${PROT2}")"
allowed U20 "${ADMIN}" "ALTER USER ${NEW} RENAME TO ${NEW}_renamed"
observe  U20name "SELECT count() FROM system.users WHERE name = '${NEW}_renamed'"
allowed U21a "${ADMIN}" "GRANT SELECT ON *.* TO ${PROT}"
allowed U21b "${ADMIN}" "REVOKE SELECT ON *.* FROM ${PROT}"
allowed U22 "${ADMIN}" "ALTER USER ${PROT} IDENTIFIED WITH sha256_password BY 'x'"
allowed U23 "${ADMIN}" "DROP USER ${NEW}_renamed"
observe  U23gone "SELECT count() FROM system.users WHERE name = '${NEW}_renamed'"

echo '=== self-protection: denied even for a principal holding PROTECTED ==='

denied U24  "${ADMIN}" "${ADMIN}" "DROP USER ${ADMIN}"
denied U24c "${ADMIN}" "${ADMIN}" "DROP USER ${ADMIN} ON CLUSTER ${CLUSTER}"
denied U25  "${ADMIN}" "${ADMIN}" "CREATE USER OR REPLACE ${ADMIN} IDENTIFIED WITH plaintext_password BY '${PW}'"
denied U25c "${ADMIN}" "${ADMIN}" "CREATE USER OR REPLACE ${ADMIN} ON CLUSTER ${CLUSTER} IDENTIFIED WITH plaintext_password BY '${PW}'"
denied U26  "${ADMIN}" "${ADMIN}" "REVOKE ALL ON *.* FROM ${ADMIN}"
denied U26c "${ADMIN}" "${ADMIN}" "REVOKE ON CLUSTER ${CLUSTER} ALL ON *.* FROM ${ADMIN}"
denied U27  "${ADMIN}" "${ADMIN}" "REVOKE PROTECTED ON *.* FROM ${ADMIN}"
denied U27c "${ADMIN}" "${ADMIN}" "REVOKE ON CLUSTER ${CLUSTER} PROTECTED ON *.* FROM ${ADMIN}"
# U28: the grantee resolved by name must be compared against the session user as
# well as by UUID. CURRENT_USER takes the UUID path, the spelled-out name takes
# the name path; both must be refused.
denied U28uuid "${ADMIN}" "${ADMIN}" "REVOKE SELECT ON *.* FROM CURRENT_USER"
denied U28name "${ADMIN}" "${ADMIN}" "REVOKE SELECT ON *.* FROM ${ADMIN}"

echo '=== the flag round-trips: an unrelated statement must not clear it ==='

echo "U29 $(is_protected "${PROT}")"
allowed U30a "${ADMIN}" "ALTER USER ${PROT} SETTINGS async_insert = 1"
echo "U30 $(is_protected "${PROT}")"
allowed U31a "${ADMIN}" "GRANT SELECT ON *.* TO ${PROT}"
echo "U31 $(is_protected "${PROT}")"
allowed U32a "${ADMIN}" "ALTER USER ${PROT} RENAME TO ${PROT}_r"
echo "U32 $(is_protected "${PROT}_r")"
allowed U32b "${ADMIN}" "ALTER USER ${PROT}_r RENAME TO ${PROT}"
allowed U33a "${ADMIN}" "ALTER USER ${REG} SETTINGS async_insert = 1"
echo "U33 $(is_protected "${REG}")"
allowed U34a "${ADMIN}" "ALTER USER ${PROT2} NOT PROTECTED"
echo "U34 $(is_protected "${PROT2}")"

# U35: the definition ClickHouse renders must parse back into the same thing.
# The AST round-trips through text in more places than is obvious - ON CLUSTER
# replay, on-disk and ZooKeeper entity definitions - so the emitted keyword has
# to survive a full text -> parse -> store cycle.
#
# The round-trip target is a dedicated password-less user rather than ${PROT}:
# SHOW CREATE redacts any real secret to a bare `IDENTIFIED WITH <method>`, which
# does not re-parse when anything follows it. That is a pre-existing property of
# SHOW CREATE and unrelated to the PROTECTED keyword, but it would mask what this
# case is meant to prove. A trailing SETTINGS clause is kept so the emitted token
# has a neighbour on both sides.
RTU="rt_${U}"
${CLICKHOUSE_CLIENT} --query "CREATE USER OR REPLACE ${RTU} PROTECTED SETTINGS async_insert = 1"
ROUNDTRIP=$(${CLICKHOUSE_CLIENT} --query "SHOW CREATE USER ${RTU}" 2>&1 | sed "s/^CREATE USER /CREATE USER OR REPLACE /")
RT_OUT=$(${CLICKHOUSE_CLIENT} --query "${ROUNDTRIP}" 2>&1)
if [[ -z "${RT_OUT}" ]]; then echo "U35rt ok"; else echo "U35rt FAILED: ${RT_OUT}"; fi
echo "U35rtflag $(is_protected "${RTU}")"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${RTU}"
echo "U35 $(is_protected "${PROT}")"

echo '=== the production IF NOT EXISTS form ==='

IFNE="CREATE USER IF NOT EXISTS ${NEW} IDENTIFIED WITH sha256_hash BY '${HASH}' SALT '${SALT}' PROTECTED"
allowed U36 "${ADMIN}" "${IFNE}"
echo "U36flag $(is_protected "${NEW}")"
# Re-run verbatim. The control plane issues this on every service powerup, from a
# deliberately non-exclusive action, so it must be a silent no-op the second time.
allowed U37 "${ADMIN}" "${IFNE}"
echo "U37flag $(is_protected "${NEW}")"
# The existence short-circuit must not skip the privilege check: IF NOT EXISTS is
# not a licence to name PROTECTED without holding it.
denied_no_target U38 "${NOPERM}" "CREATE USER IF NOT EXISTS ${PROT} IDENTIFIED WITH sha256_hash BY '${HASH}' SALT '${SALT}' PROTECTED"
denied_no_target U39 "${NOPERM}" "CREATE USER IF NOT EXISTS brandnew_${U} PROTECTED"
# U40: IF NOT EXISTS against an existing *unprotected* user. The statement is a
# no-op, so it does not retroactively protect the user. Pinned either way: this
# is an upgrade-path subtlety, not a behaviour the test gets to choose.
allowed U40 "${ADMIN}" "CREATE USER IF NOT EXISTS ${REG} IDENTIFIED WITH sha256_hash BY '${HASH}' SALT '${SALT}' PROTECTED"
echo "U40flag $(is_protected "${REG}")"

cleanup
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS brandnew_${U}, ${PROT}_r, ${NEW}_renamed" 2>/dev/null

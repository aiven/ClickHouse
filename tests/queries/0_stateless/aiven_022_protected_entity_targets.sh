#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: this test creates PROTECTED users and roles, and `TO ALL` resolves to every
# entity on the server. A concurrent test writing `TO ALL` would therefore pick ours up and
# be denied for want of PROTECTED ACCESS MANAGEMENT. Serialising is what keeps that
# contained; it is not about this test's own stability.

# Keep the server log stream out of the captured output: these tests assert on
# error text, and interleaved trace lines would swamp it.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Aiven patch 022, the `checkProtectedTargets` surface: the `TO <grantee>` clause
# of CREATE/ALTER ROW POLICY, CREATE/ALTER QUOTA, CREATE/ALTER SETTINGS PROFILE,
# and SET DEFAULT ROLE.
#
# The `TO` clause of the first three is parsed with allowRoles().allowUsers(), so
# a role in the set is a legitimate target, not an error. The helper therefore has
# to read each target type-erased, through IAccessEntity and the virtual
# isProtected, rather than casting to User. T5 is the regression lock for exactly
# that: a plain role in a `TO` set must succeed for an unprivileged principal, not
# merely "not crash". SET DEFAULT ROLE is different - its `TO` clause is parsed
# with allow_roles = false - so T10 pins that a role can never be a target there.

U="${CLICKHOUSE_TEST_UNIQUE_NAME}"
ADMIN="admin_${U}"
NOPERM="noperm_${U}"
PROT_USER="prot_user_${U}"
REG_USER="reg_user_${U}"
PROT_ROLE="prot_role_${U}"
REG_ROLE="reg_role_${U}"
TBL="${CLICKHOUSE_DATABASE}.tbl_${U}"
POLICY="policy_${U}"
QUOTA="quota_${U}"
PROFILE="profile_${U}"
PW="aiven_022_synthetic_password"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "
        DROP ROW POLICY IF EXISTS ${POLICY} ON ${TBL};
        DROP QUOTA IF EXISTS ${QUOTA};
        DROP SETTINGS PROFILE IF EXISTS ${PROFILE};
        DROP ROLE IF EXISTS ${PROT_ROLE}, ${REG_ROLE};
        DROP USER IF EXISTS ${ADMIN}, ${NOPERM}, ${PROT_USER}, ${REG_USER};
        DROP TABLE IF EXISTS ${TBL};
    " 2>/dev/null
}
cleanup

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TBL} (x UInt8) ENGINE = Memory;

    CREATE USER ${ADMIN}  IDENTIFIED WITH plaintext_password BY '${PW}';
    CREATE USER ${NOPERM} IDENTIFIED WITH plaintext_password BY '${PW}';
    CREATE USER ${PROT_USER} PROTECTED;
    CREATE USER ${REG_USER};
    CREATE ROLE ${PROT_ROLE} PROTECTED;
    CREATE ROLE ${REG_ROLE};

    GRANT ACCESS MANAGEMENT ON *.* TO ${ADMIN}, ${NOPERM};
    GRANT ROLE ADMIN ON *.* TO ${ADMIN}, ${NOPERM};
    GRANT PROTECTED ON *.* TO ${ADMIN};

    -- SET DEFAULT ROLE only accepts roles the user has actually been granted.
    GRANT ${REG_ROLE} TO ${PROT_USER}, ${REG_USER};
"

as_user() {
    local user="$1"; shift
    ${CLICKHOUSE_CLIENT} --user "${user}" --password "${PW}" --query "$1" 2>&1
}

# --- statement builders, one set per object kind -----------------------------

create_stmt() {
    case "$1" in
        rowpolicy) echo "CREATE ROW POLICY ${POLICY} ON ${TBL} USING 1 TO $2" ;;
        quota)     echo "CREATE QUOTA ${QUOTA} TO $2" ;;
        profile)   echo "CREATE SETTINGS PROFILE ${PROFILE} TO $2" ;;
    esac
}

alter_stmt() {
    case "$1" in
        rowpolicy) echo "ALTER ROW POLICY ${POLICY} ON ${TBL} TO $2" ;;
        quota)     echo "ALTER QUOTA ${QUOTA} TO $2" ;;
        profile)   echo "ALTER SETTINGS PROFILE ${PROFILE} TO $2" ;;
    esac
}

drop_stmt() {
    case "$1" in
        rowpolicy) echo "DROP ROW POLICY IF EXISTS ${POLICY} ON ${TBL}" ;;
        quota)     echo "DROP QUOTA IF EXISTS ${QUOTA}" ;;
        profile)   echo "DROP SETTINGS PROFILE IF EXISTS ${PROFILE}" ;;
    esac
}

# The object's `TO` set as stored, plus whether it exists at all.
state() {
    case "$1" in
        rowpolicy) ${CLICKHOUSE_CLIENT} --query "SELECT count(), any(apply_to_all), any(apply_to_list), any(apply_to_except) FROM system.row_policies WHERE short_name = '${POLICY}' FORMAT TSV" 2>&1 ;;
        quota)     ${CLICKHOUSE_CLIENT} --query "SELECT count(), any(apply_to_all), any(apply_to_list), any(apply_to_except) FROM system.quotas WHERE name = '${QUOTA}' FORMAT TSV" 2>&1 ;;
        profile)   ${CLICKHOUSE_CLIENT} --query "SELECT count(), any(apply_to_all), any(apply_to_list), any(apply_to_except) FROM system.settings_profiles WHERE name = '${PROFILE}' FORMAT TSV" 2>&1 ;;
    esac
}

reset_object() {
    ${CLICKHOUSE_CLIENT} --query "$(drop_stmt "$1")" 2>/dev/null
}

# --- assertions ---------------------------------------------------------------

denied() {
    local id="$1" kind="$2" user="$3" query="$4"
    local before after out
    before=$(state "${kind}")
    out=$(as_user "${user}" "${query}")
    after=$(state "${kind}")
    if [[ "${out}" != *ACCESS_DENIED* ]]; then
        echo "${id} NOT DENIED: ${out}"
    elif ! grep -qE "PROTECTED[ _]ACCESS[ _]MANAGEMENT" <<< "${out}"; then
        echo "${id} denied for the wrong reason: ${out}"
    elif [[ "${before}" != "${after}" ]]; then
        echo "${id} denied but object MUTATED"
    else
        echo "${id} denied, object unchanged"
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

# --- the matrix, run for every object kind and both statement forms -----------

run_matrix() {
    local kind="$1" form="$2" stmt
    local tag="${kind}/${form}"

    build() {
        if [[ "${form}" == create ]]; then create_stmt "${kind}" "$1"; else alter_stmt "${kind}" "$1"; fi
    }

    # The ALTER form needs the object to already exist, pointed somewhere neutral.
    prepare() {
        reset_object "${kind}"
        if [[ "${form}" == alter ]]; then
            ${CLICKHOUSE_CLIENT} --query "$(create_stmt "${kind}" "${REG_USER}")" 2>&1
        fi
    }

    prepare; denied  "T1 ${tag}" "${kind}" "${NOPERM}" "$(build "${PROT_USER}")"
    prepare; allowed "T2 ${tag}" "${ADMIN}"  "$(build "${PROT_USER}")"
    prepare; denied  "T3 ${tag}" "${kind}" "${NOPERM}" "$(build "${PROT_ROLE}")"
    prepare; allowed "T4 ${tag}" "${ADMIN}"  "$(build "${PROT_ROLE}")"
    # T5 is the regression lock: pre-fix the helper cast every target to User, so a
    # plain role here raised LOGICAL_ERROR instead of being accepted.
    prepare; allowed "T5 ${tag}" "${NOPERM}" "$(build "${REG_ROLE}")"
    prepare; allowed "T6 ${tag}" "${NOPERM}" "$(build "${REG_USER}")"
    prepare; denied  "T7 ${tag} noperm" "${kind}" "${NOPERM}" "$(build "${REG_ROLE}, ${REG_USER}, ${PROT_USER}")"
    prepare; allowed "T7 ${tag} admin"  "${ADMIN}"  "$(build "${REG_ROLE}, ${REG_USER}, ${PROT_USER}")"
    # T8: `ALL EXCEPT x` resolves to every user and role *except* x. Excepting the protected
    # user alone is not enough, because the protected *role* is still in the set - which is
    # what makes this a protection denial rather than an artefact of the caller being inside
    # `ALL`.
    prepare; denied  "T8 ${tag}" "${kind}" "${NOPERM}" "$(build "ALL EXCEPT ${PROT_USER}")"
    # T11: excepting every protected entity is the documented way for an unprivileged
    # principal to write a fleet-wide policy, and it has to succeed even though the caller is
    # itself inside `ALL`. Naming yourself in a `TO` set is not self-modification: the set is
    # stored on the policy, not on your user. This is the regression lock for the self-check
    # that used to refuse every `TO ALL` statement on the server.
    prepare; allowed "T11 ${tag}" "${NOPERM}" "$(build "ALL EXCEPT ${PROT_USER}, ${PROT_ROLE}")"
    # T12: plain `TO ALL` does reach the protected entities, so it needs the privilege. That
    # is the accepted cost of not silently narrowing `ALL` behind the caller's back.
    prepare; denied  "T12 ${tag} noperm" "${kind}" "${NOPERM}" "$(build "ALL")"
    prepare; allowed "T12 ${tag} admin"  "${ADMIN}"  "$(build "ALL")"
    reset_object "${kind}"
}

for kind in rowpolicy quota profile; do
    for form in create alter; do
        echo "=== ${kind} / ${form} ==="
        run_matrix "${kind}" "${form}"
    done
done

echo '=== SET DEFAULT ROLE ==='

default_roles() {
    ${CLICKHOUSE_CLIENT} --query "SELECT default_roles_all, default_roles_list FROM system.users WHERE name = '$1' FORMAT TSV" 2>&1
}

before=$(default_roles "${PROT_USER}")
out=$(as_user "${NOPERM}" "SET DEFAULT ROLE ${REG_ROLE} TO ${PROT_USER}")
after=$(default_roles "${PROT_USER}")
if [[ "${out}" != *ACCESS_DENIED* || "${out}" != *"PROTECTED ACCESS MANAGEMENT"* ]]; then
    echo "T9 noperm NOT DENIED: ${out}"
elif [[ "${before}" != "${after}" ]]; then
    echo "T9 noperm denied but target MUTATED"
else
    echo "T9 noperm denied, target unchanged"
fi
allowed "T9 admin" "${ADMIN}" "SET DEFAULT ROLE ${REG_ROLE} TO ${PROT_USER}"

# T10: the `TO` clause here is parsed with allow_roles = false, so a role name is
# resolved as a user name and simply does not exist. There is no protected-role
# case to add on this statement, and this pins that.
out=$(as_user "${ADMIN}" "SET DEFAULT ROLE ${REG_ROLE} TO ${REG_ROLE}")
if [[ "${out}" == *ACCESS_DENIED* ]]; then
    echo "T10 unexpectedly reached the protection check: ${out}"
elif [[ -z "${out}" ]]; then
    echo "T10 unexpectedly accepted a role as a SET DEFAULT ROLE target"
else
    echo "T10 rejected before any protection check"
fi

cleanup

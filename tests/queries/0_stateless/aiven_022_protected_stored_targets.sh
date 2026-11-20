#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: this test creates PROTECTED users and roles, and `TO ALL` resolves to every
# entity on the server. A concurrent test writing `TO ALL` would pick ours up and be denied
# for want of PROTECTED ACCESS MANAGEMENT.

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Aiven patch 022: an existing row policy, quota or settings profile whose stored `TO` set
# already covers a protected entity may only be changed by `ALTER` or replaced by
# `CREATE ... OR REPLACE` with PROTECTED_ACCESS_MANAGEMENT, even when the statement itself
# has no `TO` clause. Otherwise a new filter, limit or setting would reach the protected
# entity through a set nobody re-checked.

U="${CLICKHOUSE_TEST_UNIQUE_NAME}"
ADMIN="admin_${U}"
NOPERM="noperm_${U}"
PROT_USER="prot_user_${U}"
PROT_ROLE="prot_role_${U}"
REG_USER="reg_user_${U}"
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
        DROP ROLE IF EXISTS ${PROT_ROLE};
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
    CREATE ROLE ${PROT_ROLE} PROTECTED;
    CREATE USER ${REG_USER};

    GRANT ACCESS MANAGEMENT ON *.* TO ${ADMIN}, ${NOPERM};
    GRANT PROTECTED ON *.* TO ${ADMIN};

    -- A role counts through the users holding it, so the protected role needs a protected holder.
    GRANT ${PROT_ROLE} TO ${PROT_USER};
"

# The users of `users.xml` count as protected targets too, so the unprivileged `ALL EXCEPT` form lists them.
XML_USERS=$(${CLICKHOUSE_CLIENT} --query "SELECT arrayStringConcat(arraySort(groupArray(name)), ', ') FROM system.users WHERE storage = 'users_xml'")

as_user() {
    local user="$1"; shift
    ${CLICKHOUSE_CLIENT} --user "${user}" --password "${PW}" --query "$1" 2>&1
}

create_stmt() {
    case "$1" in
        rowpolicy) echo "CREATE ROW POLICY ${POLICY} ON ${TBL} USING 1 TO $2" ;;
        quota)     echo "CREATE QUOTA ${QUOTA} TO $2" ;;
        profile)   echo "CREATE SETTINGS PROFILE ${PROFILE} TO $2" ;;
    esac
}

# Statements without a `TO` clause: they keep the stored target set.
alter_stmt() {
    case "$1" in
        rowpolicy) echo "ALTER ROW POLICY ${POLICY} ON ${TBL} USING 0" ;;
        quota)     echo "ALTER QUOTA ${QUOTA} FOR INTERVAL 1 hour MAX queries = 1" ;;
        profile)   echo "ALTER SETTINGS PROFILE ${PROFILE} SETTINGS max_threads = 1" ;;
    esac
}

replace_stmt() {
    case "$1" in
        rowpolicy) echo "CREATE ROW POLICY OR REPLACE ${POLICY} ON ${TBL} USING 0 TO ${REG_USER}" ;;
        quota)     echo "CREATE QUOTA OR REPLACE ${QUOTA} TO ${REG_USER}" ;;
        profile)   echo "CREATE SETTINGS PROFILE OR REPLACE ${PROFILE} TO ${REG_USER}" ;;
    esac
}

show_stmt() {
    case "$1" in
        rowpolicy) echo "SHOW CREATE ROW POLICY ${POLICY} ON ${TBL}" ;;
        quota)     echo "SHOW CREATE QUOTA ${QUOTA}" ;;
        profile)   echo "SHOW CREATE SETTINGS PROFILE ${PROFILE}" ;;
    esac
}

drop_stmt() {
    case "$1" in
        rowpolicy) echo "DROP ROW POLICY IF EXISTS ${POLICY} ON ${TBL}" ;;
        quota)     echo "DROP QUOTA IF EXISTS ${QUOTA}" ;;
        profile)   echo "DROP SETTINGS PROFILE IF EXISTS ${PROFILE}" ;;
    esac
}

prepare() {
    local kind="$1" targets="$2"
    ${CLICKHOUSE_CLIENT} --query "$(drop_stmt "${kind}")"
    ${CLICKHOUSE_CLIENT} --query "$(create_stmt "${kind}" "${targets}")"
}

denied() {
    local id="$1" kind="$2" user="$3" query="$4"
    local before after out
    before=$(${CLICKHOUSE_CLIENT} --query "$(show_stmt "${kind}")" 2>&1)
    out=$(as_user "${user}" "${query}")
    after=$(${CLICKHOUSE_CLIENT} --query "$(show_stmt "${kind}")" 2>&1)
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

for kind in rowpolicy quota profile; do
    echo "=== ${kind} ==="

    # S1-S3: the stored set reaches a protected entity directly, through a protected role, or through `ALL`.
    for targets in "${PROT_USER}" "${PROT_ROLE}" "ALL"; do
        prepare "${kind}" "${targets}"; denied  "S1 alter TO ${targets/${U}/U}"   "${kind}" "${NOPERM}" "$(alter_stmt "${kind}")"
        prepare "${kind}" "${targets}"; denied  "S2 replace TO ${targets/${U}/U}" "${kind}" "${NOPERM}" "$(replace_stmt "${kind}")"
        prepare "${kind}" "${targets}"; allowed "S3 admin alter TO ${targets/${U}/U}" "${ADMIN}" "$(alter_stmt "${kind}")"
    done

    # S4: a stored set without protected entities stays free for an unprivileged principal.
    prepare "${kind}" "${REG_USER}"; allowed "S4 alter TO reg"   "${NOPERM}" "$(alter_stmt "${kind}")"
    prepare "${kind}" "${REG_USER}"; allowed "S4 replace TO reg" "${NOPERM}" "$(replace_stmt "${kind}")"

    # S5: `ALL EXCEPT <every protected user>` is the documented unprivileged form and must stay alterable.
    prepare "${kind}" "ALL EXCEPT ${PROT_USER}, ${XML_USERS}"
    allowed "S5 alter TO ALL EXCEPT protected" "${NOPERM}" "$(alter_stmt "${kind}")"

    ${CLICKHOUSE_CLIENT} --query "$(drop_stmt "${kind}")"
done

cleanup

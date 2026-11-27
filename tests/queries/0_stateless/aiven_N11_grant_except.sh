#!/usr/bin/env bash

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Aiven patch N11: `GRANT ... EXCEPT ... TO ...` grants access and carves a part of it out in one statement.

U="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB="${CLICKHOUSE_DATABASE}"
PW="aiven_N11_synthetic_password"
USERS="u1_${U}, u2_${U}, u3_${U}, granter_${U}"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${USERS}" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "DROP ROLE IF EXISTS r_${U}" 2>/dev/null
}
cleanup

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${DB}.t1 (x UInt32) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE ${DB}.t2 (x UInt32) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE ${DB}.t3 (x UInt32) ENGINE = MergeTree ORDER BY x;
    INSERT INTO ${DB}.t1 VALUES (1);
    INSERT INTO ${DB}.t2 VALUES (2);
    INSERT INTO ${DB}.t3 VALUES (3);
    CREATE USER u1_${U}, u2_${U}, u3_${U}, granter_${U} IDENTIFIED WITH plaintext_password BY '${PW}';
    CREATE ROLE r_${U};
"

# Stable output: the test database and the unique suffix vary between runs.
normalize() {
    sed -e "s/_${U}//g" -e "s/${DB}/db/g"
}

as_user() {
    ${CLICKHOUSE_CLIENT} --user "$1_${U}" --password "${PW}" --query "$2" 2>&1 | normalize
}

show_grants() {
    ${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR $1_${U}" | normalize
}

check_error() {
    local id="$1" out="$2" code="$3" message="$4"
    if [[ "${out}" != *"${code}"* ]]; then
        echo "${id} NOT REJECTED: ${out}"
    elif [[ "${out}" != *"${message}"* ]]; then
        echo "${id} rejected for the wrong reason: ${out}"
    else
        echo "${id} rejected"
    fi
}

check_read() {
    local user="$1" table="$2"
    local out
    out="$(as_user "${user}" "SELECT x FROM ${DB}.${table}")"
    if [[ "${out}" == *ACCESS_DENIED* ]]; then
        echo "${user} reads ${table}: denied"
    else
        echo "${user} reads ${table}: ${out}"
    fi
}

echo "-- formatting"
${CLICKHOUSE_CLIENT} --query "SELECT formatQuerySingleLine('GRANT SELECT ON *.* EXCEPT SELECT ON system.* TO u')"
${CLICKHOUSE_CLIENT} --query "SELECT formatQuerySingleLine('GRANT ALL ON *.* EXCEPT ALL ON system.*, ALL ON information_schema.* TO u')"
${CLICKHOUSE_CLIENT} --query "SELECT formatQuerySingleLine('GRANT ON CLUSTER c SELECT ON db.* EXCEPT SELECT ON db.t TO u WITH GRANT OPTION')"
${CLICKHOUSE_CLIENT} --query "SELECT formatQuerySingleLine('GRANT CURRENT GRANTS(SELECT ON db.*) EXCEPT SELECT ON db.t TO u')"

echo "-- syntax errors"
# A role list keeps its own `EXCEPT`, so access types after it do not parse.
${CLICKHOUSE_CLIENT} --query "SELECT formatQuerySingleLine('GRANT r1, r2 EXCEPT r2 TO u')"
check_error "roles then access types" "$(${CLICKHOUSE_CLIENT} --query "SELECT formatQuerySingleLine('GRANT r EXCEPT SELECT ON db.* TO u')" 2>&1)" \
    SYNTAX_ERROR "Syntax error"
check_error "with roles" "$(${CLICKHOUSE_CLIENT} --query "SELECT formatQuerySingleLine('GRANT r1, r2 EXCEPT r2 EXCEPT SELECT ON db.* TO u')" 2>&1)" \
    SYNTAX_ERROR "should be specified for access types, not roles"
check_error "with default replicated database privileges" \
    "$(${CLICKHOUSE_CLIENT} --query "SELECT formatQuerySingleLine('GRANT DEFAULT REPLICATED DATABASE PRIVILEGES ON db.* EXCEPT SELECT ON db.t TO u')" 2>&1)" \
    SYNTAX_ERROR "cannot be used with DEFAULT REPLICATED DATABASE PRIVILEGES"

echo "-- table exclusion"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${DB}.* EXCEPT SELECT ON ${DB}.t2 TO u1_${U}"
show_grants u1
check_read u1 t1
check_read u1 t2
check_read u1 t3

echo "-- database exclusion"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON *.* EXCEPT SELECT ON ${DB}.* TO u3_${U}"
show_grants u3
check_read u3 t1

echo "-- with grant option the excluded table is neither readable nor grantable"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${DB}.* EXCEPT SELECT ON ${DB}.t2 TO u2_${U} WITH GRANT OPTION"
show_grants u2
check_read u2 t1
check_read u2 t2
check_error "regrant excluded" "$(as_user u2 "GRANT SELECT ON ${DB}.t2 TO u3_${U}")" ACCESS_DENIED "Not enough privileges"
echo "regrant granted: $(as_user u2 "GRANT SELECT ON ${DB}.t1 TO u3_${U}")"

echo "-- equivalent to GRANT and REVOKE"
${CLICKHOUSE_CLIENT} --query "REVOKE ALL ON *.* FROM u1_${U}, u3_${U}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${DB}.* EXCEPT SELECT ON ${DB}.t2 TO u1_${U}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${DB}.* TO u3_${U}"
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT ON ${DB}.t2 FROM u3_${U}"
if [[ "$(show_grants u1 | sed 's/u1/u/')" == "$(show_grants u3 | sed 's/u3/u/')" ]]; then echo "same grants"; else echo "different grants"; fi

echo "-- role grantee"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${DB}.* EXCEPT SELECT ON ${DB}.t2 TO r_${U}"
${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR r_${U}" | normalize

echo "-- EXCEPT must lie inside the granted access"
${CLICKHOUSE_CLIENT} --query "REVOKE ALL ON *.* FROM u1_${U}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${DB}.t3 TO u1_${U}"
check_error "other table" "$(${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${DB}.t1 EXCEPT SELECT ON ${DB}.t3 TO u1_${U}" 2>&1)" \
    BAD_ARGUMENTS "is not part of the access granted by the same statement"
check_error "other privilege" "$(${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${DB}.* EXCEPT INSERT ON ${DB}.t2 TO u1_${U}" 2>&1)" \
    BAD_ARGUMENTS "is not part of the access granted by the same statement"
show_grants u1

echo "-- with replace option"
${CLICKHOUSE_CLIENT} --query "GRANT INSERT ON ${DB}.* TO u1_${U}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${DB}.* EXCEPT SELECT ON ${DB}.t2 TO u1_${U} WITH REPLACE OPTION"
show_grants u1

echo "-- current grants"
${CLICKHOUSE_CLIENT} --query "REVOKE ALL ON *.* FROM u1_${U}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${DB}.* TO granter_${U} WITH GRANT OPTION"
echo "grant: $(as_user granter "GRANT CURRENT GRANTS(SELECT ON ${DB}.*) EXCEPT SELECT ON ${DB}.t2 TO u1_${U}")"
show_grants u1

echo "-- unqualified names resolve to the current database, also on cluster"
${CLICKHOUSE_CLIENT} --query "REVOKE ALL ON *.* FROM u1_${U}, u3_${U}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON * EXCEPT SELECT ON t2 TO u1_${U}"
show_grants u1
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none \
    --query "GRANT ON CLUSTER test_shard_localhost SELECT ON * EXCEPT SELECT ON t2 TO u3_${U} WITH GRANT OPTION" 2>&1 | normalize
show_grants u3
check_read u3 t2

echo "-- EXCEPT revokes, so it cannot target the current user"
check_error "self" "$(as_user granter "GRANT SELECT ON ${DB}.* EXCEPT SELECT ON ${DB}.t2 TO granter_${U}")" \
    ACCESS_DENIED "cannot revoke rights from themselves"
show_grants granter

cleanup

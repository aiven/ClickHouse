#!/usr/bin/env bash

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Aiven patch N11: the checks on the `EXCEPT` part of `GRANT ... EXCEPT ... TO ...`.

U="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB="${CLICKHOUSE_DATABASE}"
PW="aiven_N11_synthetic_password"
USERS="victim_${U}, nopriv_${U}, partial_${U}, granter_${U}, shape_${U}"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${USERS}" 2>/dev/null
}
cleanup

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${DB}.t1 (x UInt32) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE ${DB}.t2 (x UInt32) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE ${DB}.tc (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;
    CREATE USER ${USERS} IDENTIFIED WITH plaintext_password BY '${PW}';
    GRANT SELECT ON ${DB}.t1 TO partial_${U} WITH GRANT OPTION;
    GRANT SELECT ON ${DB}.* TO granter_${U} WITH GRANT OPTION;
"

normalize() {
    sed -e "s/_${U}//g" -e "s/${DB}/db/g"
}

as_user() {
    local user="$1"
    shift
    ${CLICKHOUSE_CLIENT} --user "${user}_${U}" --password "${PW}" "$@" 2>&1 | normalize
}

show_grants() {
    ${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR $1_${U}" | normalize
}

reset_victim() {
    ${CLICKHOUSE_CLIENT} --query "REVOKE ALL ON *.* FROM victim_${U}"
    ${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${DB}.t2 TO victim_${U}"
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

echo "-- CURRENT GRANTS EXCEPT needs the grant option on what it revokes from the grantee"
reset_victim
out="$(as_user nopriv --query "GRANT CURRENT GRANTS(SELECT ON ${DB}.*) EXCEPT SELECT ON ${DB}.t2 TO victim_${U}")"
check_error "no privileges" "${out}" "ACCESS_DENIED" "SELECT ON db.t2 WITH GRANT OPTION"
show_grants victim

out="$(as_user partial --query "GRANT CURRENT GRANTS(SELECT ON ${DB}.*) EXCEPT SELECT ON ${DB}.t2 TO victim_${U}")"
check_error "grant option on part of it" "${out}" "ACCESS_DENIED" "SELECT ON db.t2 WITH GRANT OPTION"
show_grants victim

echo "-- allowed with the grant option"
as_user granter --query "GRANT CURRENT GRANTS(SELECT ON ${DB}.*) EXCEPT SELECT ON ${DB}.t2 TO victim_${U}"
show_grants victim

echo "-- allowed when the grantee holds nothing of the excepted part, as for REVOKE"
as_user granter --query "GRANT CURRENT GRANTS ON *.* EXCEPT ALL ON system.* TO shape_${U}"
show_grants shape
${CLICKHOUSE_CLIENT} --query "REVOKE ALL ON *.* FROM shape_${U}"

echo "-- columns and wildcards"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(a, b) ON ${DB}.tc EXCEPT SELECT(b) ON ${DB}.tc TO shape_${U}"
show_grants shape
${CLICKHOUSE_CLIENT} --query "REVOKE ALL ON *.* FROM shape_${U}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${DB}.tc EXCEPT SELECT(b) ON ${DB}.tc TO shape_${U}"
show_grants shape
${CLICKHOUSE_CLIENT} --query "REVOKE ALL ON *.* FROM shape_${U}"
out="$(${CLICKHOUSE_CLIENT} --query "GRANT SELECT(a) ON ${DB}.tc EXCEPT SELECT ON ${DB}.tc TO shape_${U}" 2>&1)"
check_error "column grant, table except" "${out}" "BAD_ARGUMENTS" "is not part of the access granted"
show_grants shape
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${DB}.tab* EXCEPT SELECT ON ${DB}.tab_secret TO shape_${U}"
show_grants shape

echo "-- EXCEPT cannot carry a filter"
reset_victim
out="$(${CLICKHOUSE_CLIENT} --query "GRANT READ ON S3 EXCEPT READ ON S3('s3://bucket/secret/.*') TO victim_${U}" 2>&1)"
check_error "filter" "${out}" "BAD_ARGUMENTS" "EXCEPT cannot carry a filter"
show_grants victim

echo "-- TABLE ENGINE names in EXCEPT are validated"
out="$(${CLICKHOUSE_CLIENT} --query "GRANT TABLE ENGINE ON * EXCEPT TABLE ENGINE ON NoSuchEngine TO victim_${U}" 2>&1)"
check_error "unknown engine" "${out}" "UNKNOWN_STORAGE" "NoSuchEngine"
show_grants victim

echo "-- users.xml applies EXCEPT"
LOCAL_DIR="${CLICKHOUSE_TMP}/aiven_N11_local_${U}"
mkdir -p "${LOCAL_DIR}"
cat > "${LOCAL_DIR}/config.xml" <<'EOF'
<clickhouse>
    <users_config>users.xml</users_config>
</clickhouse>
EOF
cat > "${LOCAL_DIR}/users.xml" <<'EOF'
<clickhouse>
    <profiles><default/></profiles>
    <quotas><default/></quotas>
    <users>
        <default>
            <password></password>
            <access_management>1</access_management>
            <profile>default</profile>
            <quota>default</quota>
        </default>
        <xml_user>
            <password></password>
            <grants>
                <query>GRANT SELECT ON db.* EXCEPT SELECT ON db.secret</query>
            </grants>
        </xml_user>
    </users>
</clickhouse>
EOF
${CLICKHOUSE_LOCAL} --config-file "${LOCAL_DIR}/config.xml" --query "SHOW GRANTS FOR xml_user"

sed -i "s|EXCEPT SELECT ON db.secret|EXCEPT READ ON S3('s3://bucket/secret/.*')|; s|SELECT ON db\.\*|READ ON S3|" "${LOCAL_DIR}/users.xml"
out="$(${CLICKHOUSE_LOCAL} --config-file "${LOCAL_DIR}/config.xml" --query "SHOW GRANTS FOR xml_user" 2>&1)"
check_error "users.xml filter" "${out}" "BAD_ARGUMENTS" "EXCEPT cannot carry a filter"
rm -rf "${LOCAL_DIR}"

cleanup

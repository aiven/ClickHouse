#!/usr/bin/env bash

# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

db=$CLICKHOUSE_DATABASE
user1="user03637_1_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user2="user03637_2_${CLICKHOUSE_TEST_UNIQUE_NAME}"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -mq "
        DROP USER IF EXISTS $user1, $user2;
        DROP TABLE IF EXISTS ${db}.t1;
        DROP TABLE IF EXISTS ${db}.t2;
        DROP TABLE IF EXISTS ${db}.t3;
    "
}
cleanup
trap cleanup EXIT

${CLICKHOUSE_CLIENT} -mq "
    CREATE TABLE ${db}.t1 (x UInt32) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE ${db}.t2 (x UInt32) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE ${db}.t3 (x UInt32) ENGINE = MergeTree ORDER BY x;
    INSERT INTO ${db}.t1 VALUES (1);
    INSERT INTO ${db}.t2 VALUES (2);
    INSERT INTO ${db}.t3 VALUES (3);
    CREATE USER $user1, $user2;
"

# -- Parser round-trip --
echo "--- parser ---"
echo "GRANT SELECT ON *.* REVOKE SELECT ON system.* TO u" | $CLICKHOUSE_FORMAT --oneline
echo "GRANT ALL ON *.* REVOKE ALL ON system.*, ALL ON information_schema.* TO u" | $CLICKHOUSE_FORMAT --oneline

# -- Table-level partial revoke via combined syntax --
echo "--- table exclusion ---"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${db}.* REVOKE SELECT ON ${db}.t2 TO $user1"

${CLICKHOUSE_CLIENT} --user $user1 --query "SELECT * FROM ${db}.t1"
${CLICKHOUSE_CLIENT} --user $user1 --query "SELECT * FROM ${db}.t3"
(( $(${CLICKHOUSE_CLIENT} --user $user1 --query "SELECT * FROM ${db}.t2" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"

# -- SHOW GRANTS displays combined format --
echo "--- show grants ---"
${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR $user1" | sed "s/$user1/user1/g" | sed "s/${db}/test_db/g"

# -- Database-level partial revoke via combined syntax --
echo "--- db exclusion ---"
${CLICKHOUSE_CLIENT} -mq "
    REVOKE ALL ON *.* FROM $user1;
    GRANT SELECT ON *.* REVOKE SELECT ON ${db}.* TO $user1;
"
(( $(${CLICKHOUSE_CLIENT} --user $user1 --query "SELECT * FROM ${db}.t1" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"

${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR $user1" | sed "s/$user1/user1/g" | sed "s/${db}/test_db/g"

# -- Combined with GRANT OPTION --
echo "--- grant option ---"
${CLICKHOUSE_CLIENT} -mq "
    REVOKE ALL ON *.* FROM $user1, $user2;
    GRANT SELECT ON ${db}.* REVOKE SELECT ON ${db}.t2 TO $user2 WITH GRANT OPTION;
"

# user2 can re-grant what they have (t1, t3)
${CLICKHOUSE_CLIENT} --user $user2 --query "GRANT SELECT ON ${db}.t1 TO $user1"
${CLICKHOUSE_CLIENT} --user $user1 --query "SELECT * FROM ${db}.t1"

# user2 cannot grant the revoked table (t2)
(( $(${CLICKHOUSE_CLIENT} --user $user2 --query "GRANT SELECT ON ${db}.t2 TO $user1" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"

# -- Combined syntax is equivalent to separate GRANT + REVOKE --
echo "--- equivalence ---"
${CLICKHOUSE_CLIENT} -mq "
    REVOKE ALL ON *.* FROM $user1, $user2;
    GRANT SELECT ON ${db}.* REVOKE SELECT ON ${db}.t2 TO $user1;
    GRANT SELECT ON ${db}.* TO $user2;
    REVOKE SELECT ON ${db}.t2 FROM $user2;
"

grants1=$(${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR $user1" | sed "s/$user1/userX/g" | sed "s/${db}/test_db/g")
grants2=$(${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR $user2" | sed "s/$user2/userX/g" | sed "s/${db}/test_db/g")

if [[ "$grants1" == "$grants2" ]]; then
    echo "EQUIVALENT"
else
    echo "NOT EQUIVALENT"
    echo "user1: $grants1"
    echo "user2: $grants2"
fi

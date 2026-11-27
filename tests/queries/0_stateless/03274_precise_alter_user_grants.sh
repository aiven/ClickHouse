#!/usr/bin/env bash
# Tags: no-replicated-database, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


user1="user03247_1_${CLICKHOUSE_DATABASE}_$RANDOM"
user2="user03247_2_${CLICKHOUSE_DATABASE}_$RANDOM"
user3="user03247_3_${CLICKHOUSE_DATABASE}_$RANDOM"
user4="user03247_4_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user1, $user2, $user3;
CREATE USER $user1;
GRANT CREATE USER ON user03247* TO $user1;
EOF

${CLICKHOUSE_CLIENT} --user $user1 --query "CREATE USER $user2"
${CLICKHOUSE_CLIENT} --user $user1 --query "CREATE USER $user3"
(( $(${CLICKHOUSE_CLIENT} --user $user1 --query "CREATE USER foobar" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "Not enough privileges" || echo "UNEXPECTED"

# Aiven patch 022 forbids a principal from altering itself, so the positive arms below use a
# third party as the actor rather than the target altering itself. What this test is actually
# about - that `GRANT ALTER USER ON <user>` permits exactly that user and no other - is
# unchanged; only the choice of actor is.
${CLICKHOUSE_CLIENT} --query "GRANT ALTER USER ON $user2 TO $user1"
(( $(${CLICKHOUSE_CLIENT} --user $user1 --query "ALTER USER $user3 IDENTIFIED BY 'bar'" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "Not enough privileges" || echo "UNEXPECTED"
${CLICKHOUSE_CLIENT} --user $user1 --query "ALTER USER $user2 IDENTIFIED BY 'bar'"
(( $(${CLICKHOUSE_CLIENT} --user $user3 --query "ALTER USER $user2 IDENTIFIED BY 'bar'" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "Not enough privileges" || echo "UNEXPECTED"

${CLICKHOUSE_CLIENT} --query "GRANT ALTER USER ON * TO $user1"
${CLICKHOUSE_CLIENT} --user $user1 --query "ALTER USER $user3 RENAME TO $user4"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user1, $user2, $user3, $user4;"

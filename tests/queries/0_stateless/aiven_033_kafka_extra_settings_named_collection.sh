#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the Kafka engine omitted from the fast build.

# Patch 033: the old `kafka_producer_compression_*` names overwrite a named collection's nested
# key like upstream's `kafka_compression_*`, so they need the same privilege and respect
# NOT OVERRIDABLE. The producer settings are applied before the collection, so the collection
# wins and using them is not an override.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

nc="${CLICKHOUSE_TEST_UNIQUE_NAME}"
nc_locked="${nc}_locked"
user="${nc}_user"
table="${nc}_table"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        DROP TABLE IF EXISTS ${table};
        DROP USER IF EXISTS $user;
        DROP NAMED COLLECTION IF EXISTS ${nc};
        DROP NAMED COLLECTION IF EXISTS ${nc_locked};
    "
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE USER $user;
    GRANT SOURCES ON *.* TO $user;
    GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.* TO $user;
    GRANT NAMED COLLECTION ON * TO $user;
    CREATE NAMED COLLECTION ${nc} AS
        kafka_broker_list = '127.0.0.1:2', kafka_topic_list = 'topic', kafka_group_name = 'group', kafka_format = 'JSONEachRow',
        \`kafka.producer.linger_ms\` = '5',
        \`kafka.producer.compression_codec\` = 'zstd';
    CREATE NAMED COLLECTION ${nc_locked} AS
        kafka_broker_list = '127.0.0.1:2', kafka_topic_list = 'topic', kafka_group_name = 'group', kafka_format = 'JSONEachRow',
        \`kafka.producer.compression_level\` = '3' NOT OVERRIDABLE;
"

function expect_error()
{
    local code="$1"
    local query="$2"
    if error=$(${CLICKHOUSE_CLIENT} --user "$user" --query "$query" 2>&1); then
        echo "Expected $code: $query"
        exit 1
    fi
    echo "$error" | { grep -o "$code" || true; } | head -1
}

${CLICKHOUSE_CLIENT} --user "$user" --query "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc}) SETTINGS kafka_num_consumers = 0, kafka_producer_linger_ms = 100"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"
echo 'producer setting OK'
expect_error ACCESS_DENIED "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc}) SETTINGS kafka_num_consumers = 0, kafka_producer_compression_codec = 'gzip'"

${CLICKHOUSE_CLIENT} --query "GRANT SHOW NAMED COLLECTIONS SECRETS ON ${nc_locked} TO $user"
expect_error "Override not allowed for 'kafka.producer.compression_level'" "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc_locked}) SETTINGS kafka_num_consumers = 0, kafka_producer_compression_level = 9"

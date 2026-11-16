#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the Kafka engine omitted from the fast build.

# Patch 029: Aiven does not offer AWS MSK IAM. A named collection can carry it either as the flat
# table setting, which the enum rejects at CREATE, or as the nested librdkafka key, which is refused
# when the consumer configuration is built - before any network I/O.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

nc_flat="${CLICKHOUSE_TEST_UNIQUE_NAME}_flat"
nc_nested="${CLICKHOUSE_TEST_UNIQUE_NAME}_nested"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        DROP TABLE IF EXISTS aiven_029_msk_flat;
        DROP TABLE IF EXISTS aiven_029_msk_nested;
        DROP NAMED COLLECTION IF EXISTS ${nc_flat};
        DROP NAMED COLLECTION IF EXISTS ${nc_nested};
    "
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE NAMED COLLECTION ${nc_flat} AS
        kafka_broker_list = '127.0.0.1:2', kafka_topic_list = 'topic', kafka_group_name = 'group', kafka_format = 'JSONEachRow',
        kafka_sasl_mechanism = 'AWS_MSK_IAM';
    CREATE NAMED COLLECTION ${nc_nested} AS
        kafka_broker_list = '127.0.0.1:2', kafka_topic_list = 'topic', kafka_group_name = 'group', kafka_format = 'JSONEachRow',
        \`kafka.sasl_mechanism\` = 'AWS_MSK_IAM';
"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE aiven_029_msk_flat (v String) ENGINE = Kafka(${nc_flat})" 2>&1 \
    | grep -o "Unexpected value of KafkaSASLMechanism: 'AWS_MSK_IAM'" | head -1

${CLICKHOUSE_CLIENT} --query "CREATE TABLE aiven_029_msk_nested (v String) ENGINE = Kafka(${nc_nested})"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM aiven_029_msk_nested LIMIT 1 SETTINGS stream_like_engine_allow_direct_select = 1" 2>&1 \
    | grep -o 'AWS MSK IAM authentication is not available on this service' | head -1

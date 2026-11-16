-- Tags: no-fasttest
-- Tag no-fasttest -- the Kafka engine is absent from the fast-test build.

-- Patch 031: `kafka_format_avro_schema_registry_url` is accepted as an alias of
-- `format_avro_schema_registry_url`, and its credentials are masked in SHOW CREATE
-- like the canonical name's. kafka_num_consumers = 0 keeps CREATE off the network.

DROP TABLE IF EXISTS aiven_031_kafka;

CREATE TABLE aiven_031_kafka (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092',
         kafka_topic_list = 't',
         kafka_group_name = 'g',
         kafka_format = 'AvroConfluent',
         kafka_num_consumers = 0,
         kafka_format_avro_schema_registry_url = 'http://aiven_031_user:aiven_031_secret@registry:8081';

SELECT 'alias accepted';

SELECT
    position(create_table_query, 'aiven_031_secret') = 0,
    position(create_table_query, 'kafka_format_avro_schema_registry_url') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 'aiven_031_kafka';

DROP TABLE aiven_031_kafka;

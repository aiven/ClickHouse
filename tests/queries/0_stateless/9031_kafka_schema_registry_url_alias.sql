-- Tags: no-fasttest
-- Tag no-fasttest -- requires the Kafka storage engine, absent from the fast-test build.

-- Aiven patch 031: `kafka_format_avro_schema_registry_url` is preserved as a
-- backward-compatibility alias for the canonical `format_avro_schema_registry_url`
-- format setting. On 26.3 the canonical name already works on the Kafka engine via
-- the generic format-settings passthrough; this test pins that the `kafka_`-prefixed
-- name a 25.3/25.8 table may carry still parses and is accepted at DDL time.
-- kafka_num_consumers = 0 keeps CREATE from connecting to a broker.

DROP TABLE IF EXISTS kafka_schema_registry_alias;

CREATE TABLE kafka_schema_registry_alias (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092',
         kafka_topic_list = 't',
         kafka_group_name = 'g',
         kafka_format = 'AvroConfluent',
         kafka_num_consumers = 0,
         kafka_format_avro_schema_registry_url = 'http://user:pass@registry:8081';

SELECT 'kafka_format_avro_schema_registry_url accepted';

DROP TABLE kafka_schema_registry_alias;

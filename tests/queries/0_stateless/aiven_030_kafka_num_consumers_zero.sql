-- Tags: no-fasttest, zookeeper
-- Tag no-fasttest -- the Kafka engine is absent from the fast-test build.
-- Tag zookeeper -- the second table uses the Keeper-backed engine (StorageKafka2).

-- Patch 030: `kafka_num_consumers = 0` is accepted, so the control plane can pause
-- Kafka consumption without dropping the table. With zero consumers nothing connects
-- to the broker, so the test needs no live Kafka.

DROP TABLE IF EXISTS aiven_030_kafka;
DROP TABLE IF EXISTS aiven_030_kafka2;

CREATE TABLE aiven_030_kafka (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = '127.0.0.1:9092',
         kafka_topic_list = 't',
         kafka_group_name = 'g',
         kafka_format = 'CSV',
         kafka_num_consumers = 0;

SELECT 'Kafka: 0 consumers accepted';

SET allow_experimental_kafka_offsets_storage_in_keeper = 1;

CREATE TABLE aiven_030_kafka2 (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = '127.0.0.1:9092',
         kafka_topic_list = 't',
         kafka_group_name = 'g',
         kafka_format = 'CSV',
         kafka_keeper_path = '/clickhouse/{database}/aiven_030_kafka2',
         kafka_replica_name = 'r1',
         kafka_num_consumers = 0;

SELECT 'Kafka2: 0 consumers accepted';

-- A paused Keeper-backed table refuses direct reads instead of crashing.
SELECT * FROM aiven_030_kafka2 SETTINGS stream_like_engine_allow_direct_select = 1; -- { serverError QUERY_NOT_ALLOWED }

DROP TABLE aiven_030_kafka;
DROP TABLE aiven_030_kafka2;

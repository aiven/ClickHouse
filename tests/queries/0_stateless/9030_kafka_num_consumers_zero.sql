-- Tags: no-fasttest
-- Tag no-fasttest -- requires the Kafka storage engine, absent from the fast-test build.

-- Aiven patch 030: kafka_num_consumers = 0 must be accepted at DDL time so that
-- Kafka consumption can be globally disabled as an ops lever. With zero consumers
-- no consumer threads are started, so the CREATE does not connect to the broker
-- and completes deterministically without a live broker.

DROP TABLE IF EXISTS kafka_num_consumers_zero;

CREATE TABLE kafka_num_consumers_zero (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092',
         kafka_topic_list = 't',
         kafka_group_name = 'g',
         kafka_format = 'CSV',
         kafka_num_consumers = 0;

SELECT 'kafka_num_consumers=0 accepted';

DROP TABLE kafka_num_consumers_zero;

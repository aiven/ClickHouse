-- Tags: no-fasttest
-- Tag no-fasttest -- requires the Kafka storage engine, absent from the fast-test build.

-- Aiven patch 033: extra Kafka producer throughput knobs.
-- Leg 1 adds six `kafka_producer_*` settings that map to librdkafka producer
-- properties (batch.size, batch.num.messages, linger.ms, queue.buffering.max.messages,
-- queue.buffering.max.kbytes, request.required.acks).
-- Leg 2 (compression) is obsoleted by upstream `kafka_compression_codec` /
-- `kafka_compression_level`, but the old `kafka_producer_compression_*` names are kept
-- as backward-compatibility aliases.
-- kafka_num_consumers = 0 keeps CREATE from connecting to a broker, so this is
-- deterministic without a live broker.

DROP TABLE IF EXISTS kafka_producer_knobs;
DROP TABLE IF EXISTS kafka_producer_alias_compression;
DROP TABLE IF EXISTS kafka_producer_canonical_compression;

-- All six new producer knobs are accepted at DDL time.
CREATE TABLE kafka_producer_knobs (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092',
         kafka_topic_list = 't',
         kafka_group_name = 'g',
         kafka_format = 'CSV',
         kafka_num_consumers = 0,
         kafka_producer_batch_size = 16384,
         kafka_producer_batch_num_messages = 1000,
         kafka_producer_linger_ms = 10,
         kafka_producer_queue_buffering_max_messages = 100000,
         kafka_producer_queue_buffering_max_kbytes = 1048576,
         kafka_producer_request_required_acks = 1;

SELECT 'kafka_producer_* knobs accepted';

-- Backward-compatibility compression aliases are accepted.
CREATE TABLE kafka_producer_alias_compression (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092',
         kafka_topic_list = 't',
         kafka_group_name = 'g',
         kafka_format = 'CSV',
         kafka_num_consumers = 0,
         kafka_producer_compression_codec = 'gzip',
         kafka_producer_compression_level = 5;

SELECT 'kafka_producer_compression_* aliases accepted';

-- Canonical upstream compression names are accepted.
CREATE TABLE kafka_producer_canonical_compression (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092',
         kafka_topic_list = 't',
         kafka_group_name = 'g',
         kafka_format = 'CSV',
         kafka_num_consumers = 0,
         kafka_compression_codec = 'zstd',
         kafka_compression_level = 3;

SELECT 'kafka_compression_* canonical names accepted';

DROP TABLE kafka_producer_knobs;
DROP TABLE kafka_producer_alias_compression;
DROP TABLE kafka_producer_canonical_compression;

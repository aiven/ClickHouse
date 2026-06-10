-- Tags: no-fasttest
-- Tag no-fasttest -- requires the Kafka storage engine, absent from the fast-test build.

-- Aiven patch 076: `kafka_auto_offset_reset_by_duration_ms`.
-- A still-needed setting: when no committed offset exists for a partition, the consumer
-- starts from the offset corresponding to `now() - kafka_auto_offset_reset_by_duration_ms`
-- (resolved via librdkafka's offsets-for-times), instead of the coarse
-- `kafka_auto_offset_reset` smallest/largest choice. 0 (the default) disables it and
-- preserves the existing `kafka_auto_offset_reset` behavior. This test is DDL-only: it
-- pins the setting's surface (accepted at CREATE, round-trips through stored DDL, rejects
-- non-numeric values) without a live broker. `kafka_num_consumers = 0` keeps CREATE from
-- connecting, so the run is deterministic.

DROP TABLE IF EXISTS kafka_ord_set;
DROP TABLE IF EXISTS kafka_ord_default;
DROP TABLE IF EXISTS kafka_ord_large;
DROP TABLE IF EXISTS kafka_ord_combined;

-- A non-zero value is accepted at DDL time.
CREATE TABLE kafka_ord_set (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092', kafka_topic_list = 't', kafka_group_name = 'g',
         kafka_format = 'CSV', kafka_num_consumers = 0, kafka_auto_offset_reset_by_duration_ms = 60000;

SELECT 'kafka_auto_offset_reset_by_duration_ms accepted';

-- It round-trips through the stored DDL (non-default settings are persisted in the table query).
SELECT extract(create_table_query, 'kafka_auto_offset_reset_by_duration_ms = [0-9]+')
FROM system.tables
WHERE database = currentDatabase() AND name = 'kafka_ord_set';

-- Omitting it is fine: the setting defaults to 0 (disabled).
CREATE TABLE kafka_ord_default (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092', kafka_topic_list = 't', kafka_group_name = 'g',
         kafka_format = 'CSV', kafka_num_consumers = 0;

SELECT 'default omitted accepted';

-- A large duration (one day) is accepted.
CREATE TABLE kafka_ord_large (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092', kafka_topic_list = 't', kafka_group_name = 'g',
         kafka_format = 'CSV', kafka_num_consumers = 0, kafka_auto_offset_reset_by_duration_ms = 86400000;

SELECT 'large value accepted';

-- It coexists with `kafka_auto_offset_reset` (the duration takes precedence at runtime when set).
CREATE TABLE kafka_ord_combined (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092', kafka_topic_list = 't', kafka_group_name = 'g',
         kafka_format = 'CSV', kafka_num_consumers = 0,
         kafka_auto_offset_reset = 'earliest', kafka_auto_offset_reset_by_duration_ms = 5000;

SELECT 'coexists with kafka_auto_offset_reset';

-- A non-numeric value is rejected at DDL time (the guardrail a plain String would lack).
CREATE TABLE kafka_ord_bogus (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092', kafka_topic_list = 't', kafka_group_name = 'g',
         kafka_format = 'CSV', kafka_num_consumers = 0, kafka_auto_offset_reset_by_duration_ms = 'notanumber'; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

SELECT 'invalid value rejected';

DROP TABLE kafka_ord_set;
DROP TABLE kafka_ord_default;
DROP TABLE kafka_ord_large;
DROP TABLE kafka_ord_combined;

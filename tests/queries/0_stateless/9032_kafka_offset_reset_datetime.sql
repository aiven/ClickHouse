-- Tags: no-fasttest
-- Tag no-fasttest -- requires the Kafka storage engine, absent from the fast-test build.

-- Aiven patch 032: `kafka_auto_offset_reset` and `kafka_date_time_input_format`.
-- Leg 1 (`kafka_auto_offset_reset`) is a still-needed enum port: `auto.offset.reset`
-- was hardcoded to `earliest` in the consumer config, with no per-table way to set it.
-- The enum (default `EARLIEST`, preserving the old hardcoded behavior) gives DDL-time
-- validation and a stable interface contract for stored DDL.
-- Leg 2 (`kafka_date_time_input_format`) is obsoleted by upstream's generic format
-- passthrough, but kept as a backward-compatibility alias for the canonical
-- `date_time_input_format` so 25.3/25.8 stored DDL still attaches.
-- kafka_num_consumers = 0 keeps CREATE from connecting to a broker, so this is
-- deterministic without a live broker.

DROP TABLE IF EXISTS kafka_offset_smallest;
DROP TABLE IF EXISTS kafka_offset_earliest;
DROP TABLE IF EXISTS kafka_offset_latest;
DROP TABLE IF EXISTS kafka_offset_end;
DROP TABLE IF EXISTS kafka_offset_ci;
DROP TABLE IF EXISTS kafka_offset_bogus;
DROP TABLE IF EXISTS kafka_dt_alias;
DROP TABLE IF EXISTS kafka_dt_canonical;

-- Leg 1: canonical values are accepted at DDL time.
CREATE TABLE kafka_offset_smallest (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092', kafka_topic_list = 't', kafka_group_name = 'g',
         kafka_format = 'CSV', kafka_num_consumers = 0, kafka_auto_offset_reset = 'smallest';

CREATE TABLE kafka_offset_earliest (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092', kafka_topic_list = 't', kafka_group_name = 'g',
         kafka_format = 'CSV', kafka_num_consumers = 0, kafka_auto_offset_reset = 'earliest';

CREATE TABLE kafka_offset_latest (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092', kafka_topic_list = 't', kafka_group_name = 'g',
         kafka_format = 'CSV', kafka_num_consumers = 0, kafka_auto_offset_reset = 'latest';

CREATE TABLE kafka_offset_end (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092', kafka_topic_list = 't', kafka_group_name = 'g',
         kafka_format = 'CSV', kafka_num_consumers = 0, kafka_auto_offset_reset = 'end';

SELECT 'kafka_auto_offset_reset canonical values accepted';

-- Leg 1: the enum is case-sensitive (the standard IMPLEMENT_SETTING_ENUM macro does
-- not lowercase the input, matching the original 25.8 behavior), so an upper-case
-- value is rejected. librdkafka itself matches auto.offset.reset case-insensitively at
-- runtime; the DDL-time enum only admits the canonical lowercase tokens.
CREATE TABLE kafka_offset_ci (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092', kafka_topic_list = 't', kafka_group_name = 'g',
         kafka_format = 'CSV', kafka_num_consumers = 0, kafka_auto_offset_reset = 'LATEST'; -- { serverError BAD_ARGUMENTS }

SELECT 'kafka_auto_offset_reset is case-sensitive';

-- Leg 1: an invalid value is rejected (the guardrail a plain String would lack).
CREATE TABLE kafka_offset_bogus (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092', kafka_topic_list = 't', kafka_group_name = 'g',
         kafka_format = 'CSV', kafka_num_consumers = 0, kafka_auto_offset_reset = 'bogus'; -- { serverError BAD_ARGUMENTS }

SELECT 'kafka_auto_offset_reset rejects invalid value';

-- Leg 2: the backward-compatibility alias is accepted at DDL time.
CREATE TABLE kafka_dt_alias (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092', kafka_topic_list = 't', kafka_group_name = 'g',
         kafka_format = 'CSV', kafka_num_consumers = 0, kafka_date_time_input_format = 'best_effort';

SELECT 'kafka_date_time_input_format alias accepted';

-- Leg 2: the canonical format setting still works directly on the Kafka table.
CREATE TABLE kafka_dt_canonical (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092', kafka_topic_list = 't', kafka_group_name = 'g',
         kafka_format = 'CSV', kafka_num_consumers = 0, date_time_input_format = 'best_effort';

SELECT 'date_time_input_format canonical setting accepted';

DROP TABLE kafka_offset_smallest;
DROP TABLE kafka_offset_earliest;
DROP TABLE kafka_offset_latest;
DROP TABLE kafka_offset_end;
DROP TABLE kafka_dt_alias;
DROP TABLE kafka_dt_canonical;

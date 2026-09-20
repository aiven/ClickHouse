-- Tags: no-fasttest
-- Tag no-fasttest -- requires the Kafka storage engine, absent from the fast-test build.

-- Aiven patch 029: per-table Kafka SSL settings in the SETTINGS clause --
-- `kafka_ssl_ca_location`, `kafka_ssl_certificate_location`, `kafka_ssl_key_location` and
-- `kafka_ssl_endpoint_identification_algorithm`. Upstream reaches the same `librdkafka`
-- properties only through the server config or a named collection; these give a single
-- table its own SSL material without one. Each is applied to `librdkafka` only when
-- non-empty, so an unset setting leaves the server config and `librdkafka`'s own defaults
-- alone -- notably `ssl.endpoint.identification.algorithm`, which `librdkafka` defaults to
-- `https`. `kafka_num_consumers = 0` keeps CREATE from connecting to a broker, so the test
-- is deterministic without one.

DROP TABLE IF EXISTS kafka_ssl_settings;

CREATE TABLE kafka_ssl_settings (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092',
         kafka_topic_list = 't',
         kafka_group_name = 'g',
         kafka_format = 'CSV',
         kafka_num_consumers = 0,
         kafka_security_protocol = 'SASL_SSL',
         kafka_sasl_mechanism = 'SCRAM-SHA-256',
         kafka_sasl_username = 'user',
         kafka_sasl_password = 'pass',
         kafka_ssl_ca_location = '/etc/ssl/ca.pem',
         kafka_ssl_certificate_location = '/etc/ssl/client.pem',
         kafka_ssl_key_location = '/etc/ssl/client.key',
         kafka_ssl_endpoint_identification_algorithm = 'none';

-- The four SSL settings round-trip through the stored DDL.
SELECT arraySort(extractAll(engine_full, 'kafka_ssl_[a-z_]* = \'[^\']*\''))
FROM system.tables
WHERE database = currentDatabase() AND name = 'kafka_ssl_settings';

DROP TABLE kafka_ssl_settings;

-- Left unset they stay at the empty-string default, so they are absent from the stored DDL
-- and cannot override the server config or `librdkafka`'s `https` endpoint identification.
CREATE TABLE kafka_ssl_settings (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092',
         kafka_topic_list = 't',
         kafka_group_name = 'g',
         kafka_format = 'CSV',
         kafka_num_consumers = 0;

SELECT arraySort(extractAll(engine_full, 'kafka_ssl_[a-z_]* = \'[^\']*\''))
FROM system.tables
WHERE database = currentDatabase() AND name = 'kafka_ssl_settings';

DROP TABLE kafka_ssl_settings;

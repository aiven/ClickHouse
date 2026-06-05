-- Tags: no-fasttest
-- Tag no-fasttest -- requires the Kafka storage engine, absent from the fast-test build.

-- Aiven patch 029: Kafka SASL/SSL configuration carried as enum settings
-- (kafka_security_protocol, kafka_sasl_mechanism, kafka_ssl_endpoint_identification_algorithm)
-- plus String SSL file locations (ca/certificate/key). The enums validate values at
-- DDL time with case-insensitive parsing (and hyphen/underscore tolerance for the
-- security protocol), which a plain String setting would not. kafka_num_consumers = 0
-- keeps CREATE from connecting to a broker, so the test is deterministic without one.

DROP TABLE IF EXISTS kafka_sasl_ssl_settings;

-- Canonical enum values + the three SSL file locations are accepted.
CREATE TABLE kafka_sasl_ssl_settings (key UInt64, value UInt64)
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
SELECT 'canonical accepted';
DROP TABLE kafka_sasl_ssl_settings;

-- Case-insensitive + hyphen/underscore tolerance: 'sasl-ssl' normalizes to SASL_SSL,
-- a lowercase SASL mechanism and an uppercase endpoint algorithm all parse via fromString.
CREATE TABLE kafka_sasl_ssl_settings (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092',
         kafka_topic_list = 't',
         kafka_group_name = 'g',
         kafka_format = 'CSV',
         kafka_num_consumers = 0,
         kafka_security_protocol = 'sasl-ssl',
         kafka_sasl_mechanism = 'scram-sha-512',
         kafka_ssl_endpoint_identification_algorithm = 'HTTPS';
SELECT 'case/hyphen variants accepted';
DROP TABLE kafka_sasl_ssl_settings;

-- Invalid enum values are rejected at DDL time (the guardrail a String setting lacks).
CREATE TABLE kafka_sasl_ssl_settings (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092',
         kafka_topic_list = 't',
         kafka_group_name = 'g',
         kafka_format = 'CSV',
         kafka_num_consumers = 0,
         kafka_ssl_endpoint_identification_algorithm = 'bogus'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE kafka_sasl_ssl_settings (key UInt64, value UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:19092',
         kafka_topic_list = 't',
         kafka_group_name = 'g',
         kafka_format = 'CSV',
         kafka_num_consumers = 0,
         kafka_security_protocol = 'NOPE'; -- { serverError BAD_ARGUMENTS }

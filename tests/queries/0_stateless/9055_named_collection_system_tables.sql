-- Tags: no-fasttest
-- The MySQL table engine is not built in the fasttest image, so every MySQL-engine
-- stateless test carries no-fasttest. The MySQL table here is created with EXPLICIT
-- columns and is NEVER queried, so no live MySQL server is contacted: StorageMySQL only
-- connects when it must infer the schema (empty columns) or on SELECT. The test is fully
-- deterministic and needs no external service.

-- Aiven patch 055: system.tables gains a `named_collection` String column, populated from
-- IStorage::getNamedCollection(). A table created from a named collection shows the
-- collection name; other tables show ''. Pre-patch the column does not exist, so the
-- SELECT below fails with UNKNOWN_IDENTIFIER -- the SQL-observable evidence-of-causation.

DROP TABLE IF EXISTS t_9055_mysql SYNC;
DROP TABLE IF EXISTS t_9055_plain SYNC;
DROP NAMED COLLECTION IF EXISTS nc_9055;

CREATE NAMED COLLECTION nc_9055 AS
    host = 'localhost', port = 3306, user = 'u', password = 'p', database = 'd', table = 'tbl';

-- MySQL engine, EXPLICIT columns -> CREATE is offline (no connection).
CREATE TABLE t_9055_mysql (a UInt64) ENGINE = MySQL(nc_9055);

-- Control: a plain table has no named collection (column shows '').
CREATE TABLE t_9055_plain (a UInt64) ENGINE = MergeTree ORDER BY a;

SELECT name, named_collection
FROM system.tables
WHERE database = currentDatabase() AND name IN ('t_9055_mysql', 't_9055_plain')
ORDER BY name;

DROP TABLE t_9055_mysql SYNC;
DROP TABLE t_9055_plain SYNC;
DROP NAMED COLLECTION nc_9055;

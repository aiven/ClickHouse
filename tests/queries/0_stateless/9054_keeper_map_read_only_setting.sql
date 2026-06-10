-- Tags: no-ordinary-database, no-fasttest
-- KeeperMap requires ZooKeeper/Keeper (absent in fasttest) and an Atomic (UUID-backed)
-- database. Every existing KeeperMap stateless test carries these tags; they are a real
-- necessity for this engine, not gratuitous no-* gating (parent policy call 5).

-- Aiven patch 054: read_only setting for KeeperMap storage.
-- Post-patch, KeeperMap supports a Bool `read_only` engine setting; when enabled, write
-- paths (INSERT / TRUNCATE / mutate) throw TABLE_IS_READ_ONLY, and ALTER is rejected
-- unless it only touches settings/comment. Pre-patch, KeeperMap declared
-- supports_settings = false, so CREATE ... SETTINGS read_only = 1 is rejected at factory
-- time -- the deterministic, SQL-observable divergence (evidence pair).

DROP TABLE IF EXISTS 9054_km_rw SYNC;
DROP TABLE IF EXISTS 9054_km_ro SYNC;

-- Control: a normal (writable) KeeperMap table accepts writes. Passes pre AND post --
-- a sanity anchor that the engine works in this environment, not the evidence.
CREATE TABLE 9054_km_rw (key UInt64, value String)
    ENGINE = KeeperMap('/' || currentDatabase() || '/9054_rw') PRIMARY KEY key;
INSERT INTO 9054_km_rw VALUES (1, 'a');
SELECT count() FROM 9054_km_rw;

-- Evidence: a read-only KeeperMap table.
-- Post-patch: CREATE ... SETTINGS read_only = 1 is accepted (engine supports settings)
--             and write paths throw TABLE_IS_READ_ONLY.
-- Pre-patch:  the CREATE itself fails (supports_settings = false) -> output diverges from
--             the .reference at this line -> test FAILS. (Evidence-of-causation.)
CREATE TABLE 9054_km_ro (key UInt64, value String)
    ENGINE = KeeperMap('/' || currentDatabase() || '/9054_ro') PRIMARY KEY key
    SETTINGS read_only = 1;
INSERT INTO 9054_km_ro VALUES (1, 'a'); -- { serverError TABLE_IS_READ_ONLY }
TRUNCATE TABLE 9054_km_ro;              -- { serverError TABLE_IS_READ_ONLY }

-- checkAlterIsPossible/alter allow MODIFY SETTING even on a read-only table; flipping
-- read_only off must re-enable writes. Exercises the new alter() path.
ALTER TABLE 9054_km_ro MODIFY SETTING read_only = 0;
INSERT INTO 9054_km_ro VALUES (2, 'b');
SELECT count() FROM 9054_km_ro;

DROP TABLE 9054_km_rw SYNC;
DROP TABLE 9054_km_ro SYNC;

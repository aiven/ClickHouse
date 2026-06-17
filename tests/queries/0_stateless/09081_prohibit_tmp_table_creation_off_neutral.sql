-- Aiven patch 062: with the server setting `aiven_prohibit_tmp_table_creation`
-- OFF (the default), creating a table whose name starts with `.tmp` must be
-- accepted exactly like upstream. The ON behavior is covered by the integration
-- test tests/integration/test_aiven_prohibit_tmp_table (the guard is a server
-- setting and cannot be flipped per session).

DROP TABLE IF EXISTS `.tmp09081`;
CREATE TABLE `.tmp09081` (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO `.tmp09081` VALUES (1), (2), (3);
SELECT count() FROM `.tmp09081`;
DROP TABLE `.tmp09081`;

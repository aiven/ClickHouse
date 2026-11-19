-- Smoke test for the Aiven stateless lane: proves the lane runs at least one
-- test against a server that can create, write and read a MergeTree table.

CREATE TABLE aiven_smoke (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO aiven_smoke VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT count(), sum(id), arraySort(groupArray(s)) FROM aiven_smoke;
DROP TABLE aiven_smoke;

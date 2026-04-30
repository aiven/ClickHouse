-- Verify that secret arguments (e.g. encryption keys) are masked in system.mutations.command,
-- matching the masking behaviour of system.query_log.query.

DROP TABLE IF EXISTS t_mut_mask SYNC;

CREATE TABLE t_mut_mask (id UInt64, payload String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_mut_mask SELECT number, '' FROM numbers(5);

-- Don't wait for the mutation: we only care about how the command is rendered in system.mutations.
ALTER TABLE t_mut_mask
    UPDATE payload = hex(encrypt('aes-128-ecb', toString(id), 'TOPSECRET-key-16'))
    WHERE id < 5
    SETTINGS mutations_sync = 0;

-- The key literal must not appear in system.mutations.command.
SELECT count() FROM system.mutations
WHERE database = currentDatabase()
  AND table = 't_mut_mask'
  AND command LIKE '%TOPSECRET%';

-- The command should still contain the masked placeholder.
SELECT count() FROM system.mutations
WHERE database = currentDatabase()
  AND table = 't_mut_mask'
  AND command LIKE '%[HIDDEN]%';

DROP TABLE t_mut_mask SYNC;

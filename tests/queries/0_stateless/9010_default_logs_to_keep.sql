-- Tags: no-parallel

DROP DATABASE IF EXISTS replicated_010_logs_to_keep;

CREATE DATABASE replicated_010_logs_to_keep
  ENGINE = Replicated('/test/aiven_010/' || currentDatabase() || '/replicated_010_logs_to_keep', 'shard_1', 'replica_1');

SELECT value FROM system.zookeeper
WHERE path = '/test/aiven_010/' || currentDatabase() || '/replicated_010_logs_to_keep'
  AND name = 'logs_to_keep';

DROP DATABASE replicated_010_logs_to_keep;

-- Tags: zookeeper

-- A new Replicated database writes its logs_to_keep to Keeper once, at creation.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier}
  ENGINE = Replicated('/test/aiven_010/' || currentDatabase(), 'shard_1', 'replica_1');

SELECT value FROM system.zookeeper
WHERE path = '/test/aiven_010/' || currentDatabase() AND name = 'logs_to_keep';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

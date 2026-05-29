-- Tags: no-parallel

DROP TABLE IF EXISTS replicated_042_leak SYNC;

CREATE TABLE replicated_042_leak (a UInt64)
ENGINE = ReplicatedMergeTree(
    '/test/aiven_042/' || currentDatabase() || '/parent_aiven_042/{shard}',
    '{replica}')
ORDER BY a;

DROP TABLE replicated_042_leak SYNC;

-- After DROP, the empty parent znode '/test/aiven_042/<db>/parent_aiven_042'
-- should be removed by the patched dropAncestorTableZnodeIfNeeded.
-- Pre-patch: count() = 1 (empty parent znode still exists; dropAncestorZnodesIfNeeded
--             early-returns because path_prefix_for_drop is empty -- no {uuid} in path).
-- Post-patch: count() = 0 (parent znode removed).
SELECT count() FROM system.zookeeper
WHERE path = '/test/aiven_042/' || currentDatabase()
  AND name = 'parent_aiven_042';

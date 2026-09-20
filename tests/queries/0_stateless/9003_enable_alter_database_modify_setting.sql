-- Tags: no-parallel

DROP DATABASE IF EXISTS replicated_003_modify_setting;

CREATE DATABASE replicated_003_modify_setting
  ENGINE = Replicated('/test/aiven_003/' || currentDatabase() || '/replicated_003_modify_setting', 'shard_1', 'replica_1');

-- Post-patch: DatabaseReplicated::applySettingsChanges applies a known setting.
-- Pre-patch: base IDatabase::applySettingsChanges throws NOT_IMPLEMENTED, so this
-- line errors out (unannotated) and the test FAILS -- the evidence-of-causation line.
ALTER DATABASE replicated_003_modify_setting MODIFY SETTING max_broken_tables_ratio = 0.5;
SELECT 'known-setting-applied';

-- An unknown setting is rejected by the Aiven check with BAD_ARGUMENTS naming the engine,
-- distinct from the base NOT_IMPLEMENTED throw pre-patch.
ALTER DATABASE replicated_003_modify_setting MODIFY SETTING this_setting_does_not_exist = 1; -- { serverError BAD_ARGUMENTS }
SELECT 'unknown-setting-rejected';

DROP DATABASE replicated_003_modify_setting;

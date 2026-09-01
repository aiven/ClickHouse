-- Regression test for Aiven patch 073: SET compatibility must skip
-- SettingsChangesHistory entries naming settings that don't exist in the
-- current build, instead of throwing UNKNOWN_SETTING.

SET compatibility = '25.2';
SELECT 1;

-- The `compatibility` setting is implemented by replaying `SettingsChangesHistory` backwards:
-- for every version newer than the requested one, each recorded change is written back by name.
-- The history is therefore a second, implicit schema for the settings list, and a build whose
-- history names a setting it does not have carries a latent runtime failure - historically this
-- happened when a backport took a history entry without the commit declaring the setting.
-- The first two arms are the tripwire on that data: they must stay empty. The name check cannot see
-- an entry whose previous value no longer fits its setting's type, so the replay arm uses the oldest
-- version, which writes back every session entry. The `MergeTree` replay is computed once per process
-- and cannot be driven from here; `aiven_073_compatibility_mergetree_full_replay.sh` covers it.

SELECT 'session-tripwire' AS arm, arraySort(groupArray(name)) AS dangling
FROM (SELECT DISTINCT arrayJoin(tupleElement(changes, 'name')) AS name FROM system.settings_changes WHERE type = 'Session')
WHERE name NOT IN (SELECT name FROM system.settings);

SELECT 'mergetree-tripwire' AS arm, arraySort(groupArray(name)) AS dangling
FROM (SELECT DISTINCT arrayJoin(tupleElement(changes, 'name')) AS name FROM system.settings_changes WHERE type = 'MergeTree')
WHERE name NOT IN (SELECT name FROM system.merge_tree_settings);

SET compatibility = '1.0';
SELECT 'replay-full' AS arm, 1 AS ok;

SET compatibility = '';
SELECT 'replay-reset' AS arm, 1 AS ok;

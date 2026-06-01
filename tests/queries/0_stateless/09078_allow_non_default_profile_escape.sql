-- Hardening against the "default profile escape": a user constrained with
-- allow_non_default_profile = 0 must not be able to create or alter a settings
-- profile that inherits from a non-default profile, which would otherwise let
-- them escape the constraints imposed by the default profile.
-- The guard is toggled here via a session SET, which drives the exact same
-- enforcement paths as locking it with a CONST constraint in a profile.

DROP SETTINGS PROFILE IF EXISTS elevated_09078, escape_09078, child_default_09078, autoinject_09078, cyc_a_09078, cyc_b_09078, optout_09078;

SELECT '-- setup: a standalone (non-default) profile created while the guard is off';
SET allow_non_default_profile = 1;
CREATE SETTINGS PROFILE elevated_09078 SETTINGS max_threads = 1;

SELECT '-- guard on: inheriting from a non-default profile is rejected';
SET allow_non_default_profile = 0;
CREATE SETTINGS PROFILE escape_09078 SETTINGS INHERIT elevated_09078; -- { serverError SETTING_CONSTRAINT_VIOLATION }

SELECT '-- guard on: inheriting from the default profile is allowed';
CREATE SETTINGS PROFILE child_default_09078 SETTINGS INHERIT default;
SHOW CREATE SETTINGS PROFILE child_default_09078;

SELECT '-- guard on: a profile created without a parent gets the default profile auto-injected';
CREATE SETTINGS PROFILE autoinject_09078 SETTINGS max_threads = 1;
SHOW CREATE SETTINGS PROFILE autoinject_09078;

SELECT '-- cycle detection is unconditional (checked here with the guard off)';
SET allow_non_default_profile = 1;
CREATE SETTINGS PROFILE cyc_a_09078;
CREATE SETTINGS PROFILE cyc_b_09078 SETTINGS INHERIT cyc_a_09078;
ALTER SETTINGS PROFILE cyc_a_09078 SETTINGS INHERIT cyc_b_09078; -- { serverError SETTING_CONSTRAINT_VIOLATION }

SELECT '-- opt-out: with the guard off, inheriting a non-default profile is allowed';
CREATE SETTINGS PROFILE optout_09078 SETTINGS INHERIT elevated_09078;
SHOW CREATE SETTINGS PROFILE optout_09078;

DROP SETTINGS PROFILE elevated_09078, child_default_09078, autoinject_09078, cyc_a_09078, cyc_b_09078, optout_09078;

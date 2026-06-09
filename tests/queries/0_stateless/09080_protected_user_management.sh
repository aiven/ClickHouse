#!/usr/bin/env bash

# Core protected-user matrix: DROP / CREATE ... PROTECTED / ALTER (NOT) PROTECTED /
# CREATE OR REPLACE / RENAME / password change / SETTINGS / SETTINGS PROFILE / REVOKE,
# for a user holding ACCESS MANAGEMENT but NOT PROTECTED_ACCESS_MANAGEMENT, versus a
# user holding PROTECTED_ACCESS_MANAGEMENT, versus self.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

PROT=prot_09080
REG=reg_09080
ADMIN=admin_09080
NOPERM=noperm_09080

cleanup() {
    $CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $PROT, $REG, $ADMIN, $NOPERM, ${PROT}_renamed, cu_09080, cu2_09080"
    $CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS prof_09080"
}
cleanup

$CLICKHOUSE_CLIENT -q "CREATE USER $PROT PROTECTED"
$CLICKHOUSE_CLIENT -q "CREATE USER $REG"
$CLICKHOUSE_CLIENT -q "CREATE USER $ADMIN PROTECTED"
$CLICKHOUSE_CLIENT -q "CREATE USER $NOPERM"
$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE prof_09080"
$CLICKHOUSE_CLIENT -q "GRANT ACCESS MANAGEMENT ON *.* TO $NOPERM"
$CLICKHOUSE_CLIENT -q "GRANT ACCESS MANAGEMENT ON *.* TO $ADMIN"
$CLICKHOUSE_CLIENT -q "GRANT PROTECTED ON *.* TO $ADMIN"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON *.* TO $PROT"
# Grant SELECT WITH GRANT OPTION so REVOKE is gated only by protection, not by a
# missing grant option (you can only revoke a privilege you hold WITH GRANT OPTION).
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON *.* TO $NOPERM WITH GRANT OPTION"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON *.* TO $ADMIN WITH GRANT OPTION"

denied() {
    # Expect ACCESS_DENIED somewhere in stderr.
    "$@" 2>&1 | grep -c -m1 'ACCESS_DENIED'
}

ok() {
    "$@" >/dev/null 2>&1 && echo OK || echo FAIL
}

echo '=== Without PROTECTED_ACCESS_MANAGEMENT (denied) ==='
echo '--- DROP protected user ---'
denied $CLICKHOUSE_CLIENT --user $NOPERM -q "DROP USER $PROT"
echo '--- CREATE USER ... PROTECTED ---'
denied $CLICKHOUSE_CLIENT --user $NOPERM -q "CREATE USER cu_09080 PROTECTED"
echo '--- ALTER USER ... PROTECTED (add) ---'
denied $CLICKHOUSE_CLIENT --user $NOPERM -q "ALTER USER $REG PROTECTED"
echo '--- ALTER USER ... NOT PROTECTED (remove) ---'
denied $CLICKHOUSE_CLIENT --user $NOPERM -q "ALTER USER $PROT NOT PROTECTED"
echo '--- CREATE USER OR REPLACE protected ---'
denied $CLICKHOUSE_CLIENT --user $NOPERM -q "CREATE USER OR REPLACE $PROT"
echo '--- ALTER protected RENAME ---'
denied $CLICKHOUSE_CLIENT --user $NOPERM -q "ALTER USER $PROT RENAME TO ${PROT}_renamed"
echo '--- ALTER protected password ---'
denied $CLICKHOUSE_CLIENT --user $NOPERM -q "ALTER USER $PROT IDENTIFIED WITH plaintext_password BY 'secret'"
echo '--- ALTER protected SETTINGS ---'
denied $CLICKHOUSE_CLIENT --user $NOPERM -q "ALTER USER $PROT SETTINGS max_memory_usage = 20000000"
echo '--- ALTER protected SETTINGS PROFILE ---'
denied $CLICKHOUSE_CLIENT --user $NOPERM -q "ALTER USER $PROT SETTINGS PROFILE prof_09080"
echo '--- REVOKE ALL FROM protected ---'
denied $CLICKHOUSE_CLIENT --user $NOPERM -q "REVOKE ALL ON *.* FROM $PROT"

echo '=== Self-protection (denied even WITH privilege) ==='
echo '--- self-drop with privilege ---'
denied $CLICKHOUSE_CLIENT --user $ADMIN -q "DROP USER $ADMIN"
echo '--- self-revoke with privilege ---'
denied $CLICKHOUSE_CLIENT --user $ADMIN -q "REVOKE ALL ON *.* FROM $ADMIN"

echo '=== With PROTECTED_ACCESS_MANAGEMENT (allowed on protected users) ==='
echo '--- alter protected SETTINGS ---'
ok $CLICKHOUSE_CLIENT --user $ADMIN -q "ALTER USER $PROT SETTINGS max_memory_usage = 20000000"
echo '--- set protected password ---'
ok $CLICKHOUSE_CLIENT --user $ADMIN -q "ALTER USER $PROT IDENTIFIED WITH plaintext_password BY 'secret'"
echo '--- set protected SETTINGS PROFILE ---'
ok $CLICKHOUSE_CLIENT --user $ADMIN -q "ALTER USER $PROT SETTINGS PROFILE prof_09080"
echo '--- revoke from protected ---'
ok $CLICKHOUSE_CLIENT --user $ADMIN -q "REVOKE ALL ON *.* FROM $PROT"
echo '--- add protection to regular user ---'
ok $CLICKHOUSE_CLIENT --user $ADMIN -q "ALTER USER $REG PROTECTED"
echo '--- remove protection ---'
ok $CLICKHOUSE_CLIENT --user $ADMIN -q "ALTER USER $PROT NOT PROTECTED"
echo '--- create protected user ---'
ok $CLICKHOUSE_CLIENT --user $ADMIN -q "CREATE USER cu2_09080 PROTECTED"
echo '--- rename protected user ---'
ok $CLICKHOUSE_CLIENT --user $ADMIN -q "ALTER USER $REG RENAME TO ${PROT}_renamed"
echo '--- replace protected user ---'
ok $CLICKHOUSE_CLIENT --user $ADMIN -q "CREATE USER OR REPLACE cu2_09080 PROTECTED"
echo '--- drop protected user ---'
ok $CLICKHOUSE_CLIENT --user $ADMIN -q "DROP USER cu2_09080"

cleanup

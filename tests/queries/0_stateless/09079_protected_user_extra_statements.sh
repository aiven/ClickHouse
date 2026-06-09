#!/usr/bin/env bash
# Tags: no-parallel

# Verifies PROTECTED_ACCESS_MANAGEMENT gating + self-protection on
# CREATE/ALTER ROW POLICY, CREATE/ALTER QUOTA, CREATE/ALTER SETTINGS PROFILE,
# and SET DEFAULT ROLE statements that target a User.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

PROT=prot_u_09079
REG=reg_u_09079
ADMIN=admin_u_09079
NOPERM=noperm_u_09079
ROLE=role_09079

cleanup() {
    $CLICKHOUSE_CLIENT -q "DROP ROW POLICY IF EXISTS p_09079 ON system.one"
    $CLICKHOUSE_CLIENT -q "DROP QUOTA IF EXISTS q_09079"
    $CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS sp_09079"
    $CLICKHOUSE_CLIENT -q "DROP ROLE IF EXISTS $ROLE"
    $CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $PROT, $REG, $ADMIN, $NOPERM"
}
cleanup

$CLICKHOUSE_CLIENT -q "CREATE USER $PROT PROTECTED"
$CLICKHOUSE_CLIENT -q "CREATE USER $REG"
$CLICKHOUSE_CLIENT -q "CREATE USER $ADMIN"
$CLICKHOUSE_CLIENT -q "CREATE USER $NOPERM"
$CLICKHOUSE_CLIENT -q "CREATE ROLE $ROLE"
$CLICKHOUSE_CLIENT -q "GRANT ACCESS MANAGEMENT ON *.* TO $ADMIN"
$CLICKHOUSE_CLIENT -q "GRANT PROTECTED ON *.* TO $ADMIN"
$CLICKHOUSE_CLIENT -q "GRANT $ROLE TO $ADMIN"
$CLICKHOUSE_CLIENT -q "GRANT ACCESS MANAGEMENT ON *.* TO $NOPERM"
$CLICKHOUSE_CLIENT -q "GRANT $ROLE TO $PROT, $REG, $ADMIN"

denied() {
    # Expect ACCESS_DENIED somewhere in stderr.
    "$@" 2>&1 | grep -c -m1 'ACCESS_DENIED'
}

ok() {
    "$@" >/dev/null 2>&1 && echo OK || echo FAIL
}

echo '--- ROW POLICY: noperm targets protected ---'
denied $CLICKHOUSE_CLIENT --user $NOPERM -q "CREATE ROW POLICY p_09079 ON system.one TO $PROT"
echo '--- ROW POLICY: noperm targets regular ---'
ok    $CLICKHOUSE_CLIENT --user $NOPERM -q "CREATE ROW POLICY p_09079 ON system.one TO $REG"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY p_09079 ON system.one"
echo '--- ROW POLICY: admin targets protected ---'
ok    $CLICKHOUSE_CLIENT --user $ADMIN -q "CREATE ROW POLICY p_09079 ON system.one TO $PROT"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY p_09079 ON system.one"
echo '--- ROW POLICY: admin targets self ---'
denied $CLICKHOUSE_CLIENT --user $ADMIN -q "CREATE ROW POLICY p_09079 ON system.one TO $ADMIN"

echo '--- QUOTA: noperm targets protected ---'
denied $CLICKHOUSE_CLIENT --user $NOPERM -q "CREATE QUOTA q_09079 TO $PROT"
echo '--- QUOTA: noperm targets regular ---'
ok    $CLICKHOUSE_CLIENT --user $NOPERM -q "CREATE QUOTA q_09079 TO $REG"
$CLICKHOUSE_CLIENT -q "DROP QUOTA q_09079"
echo '--- QUOTA: admin targets protected ---'
ok    $CLICKHOUSE_CLIENT --user $ADMIN -q "CREATE QUOTA q_09079 TO $PROT"
$CLICKHOUSE_CLIENT -q "DROP QUOTA q_09079"
echo '--- QUOTA: admin targets self ---'
denied $CLICKHOUSE_CLIENT --user $ADMIN -q "CREATE QUOTA q_09079 TO $ADMIN"

echo '--- SETTINGS PROFILE: noperm targets protected ---'
denied $CLICKHOUSE_CLIENT --user $NOPERM -q "CREATE SETTINGS PROFILE sp_09079 TO $PROT"
echo '--- SETTINGS PROFILE: noperm targets regular ---'
ok    $CLICKHOUSE_CLIENT --user $NOPERM -q "CREATE SETTINGS PROFILE sp_09079 TO $REG"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE sp_09079"
echo '--- SETTINGS PROFILE: admin targets protected ---'
ok    $CLICKHOUSE_CLIENT --user $ADMIN -q "CREATE SETTINGS PROFILE sp_09079 TO $PROT"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE sp_09079"
echo '--- SETTINGS PROFILE: admin targets self ---'
denied $CLICKHOUSE_CLIENT --user $ADMIN -q "CREATE SETTINGS PROFILE sp_09079 TO $ADMIN"

echo '--- SET DEFAULT ROLE: noperm targets protected ---'
denied $CLICKHOUSE_CLIENT --user $NOPERM -q "SET DEFAULT ROLE $ROLE TO $PROT"
echo '--- SET DEFAULT ROLE: noperm targets regular ---'
ok    $CLICKHOUSE_CLIENT --user $NOPERM -q "SET DEFAULT ROLE $ROLE TO $REG"
echo '--- SET DEFAULT ROLE: admin targets protected ---'
ok    $CLICKHOUSE_CLIENT --user $ADMIN -q "SET DEFAULT ROLE $ROLE TO $PROT"
echo '--- SET DEFAULT ROLE: admin targets self ---'
denied $CLICKHOUSE_CLIENT --user $ADMIN -q "SET DEFAULT ROLE $ROLE TO $ADMIN"

cleanup

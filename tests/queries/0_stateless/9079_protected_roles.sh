#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

prot="prot_role_${CLICKHOUSE_DATABASE}"
ctrl="ctrl_role_${CLICKHOUSE_DATABASE}"
newp="new_prot_${CLICKHOUSE_DATABASE}"
newc="new_prot_clu_${CLICKHOUSE_DATABASE}"
u="lim_user_${CLICKHOUSE_DATABASE}"
CLU="test_shard_localhost"

cleanup() {
    ${CLICKHOUSE_CLIENT} --multiquery -q "
        DROP ROLE IF EXISTS ${prot}, ${ctrl}, ${newp}, ${newc}, hijacked_${CLICKHOUSE_DATABASE};
        DROP USER IF EXISTS ${u};
    "
}
cleanup

# --- Setup, as admin (default test user holds PROTECTED_ACCESS_MANAGEMENT) ---
${CLICKHOUSE_CLIENT} --multiquery -q "
    CREATE ROLE ${prot} PROTECTED;
    CREATE ROLE ${ctrl};
    CREATE USER ${u} IDENTIFIED WITH no_password;
    GRANT CREATE ROLE, ALTER ROLE, DROP ROLE, ROLE ADMIN ON *.* TO ${u};
"

# (A) SHOW CREATE round-trips the keyword (parse + format + entity flag).
${CLICKHOUSE_CLIENT} -q "SHOW CREATE ROLE ${prot}" | grep -c "PROTECTED"

# (B) Control: the limited user CAN drop a non-protected role (privilege is genuinely held).
${CLICKHOUSE_CLIENT} --user "${u}" -q "DROP ROLE ${ctrl}" 2>/dev/null

# --- Denial matrix as the limited user (all suppressed; we assert end-state) ---
run() { ${CLICKHOUSE_CLIENT} --user "${u}" -q "$1" >/dev/null 2>&1; }
# local
run "DROP ROLE ${prot}"
run "ALTER ROLE ${prot} RENAME TO hijacked_${CLICKHOUSE_DATABASE}"
run "CREATE ROLE OR REPLACE ${prot}"
run "ALTER ROLE ${prot} SETTINGS async_insert = 1"
run "ALTER ROLE ${prot} NOT PROTECTED"
run "CREATE ROLE ${newp} PROTECTED"
# ON CLUSTER (initiator-side check fires before dispatch)
run "DROP ROLE ${prot} ON CLUSTER ${CLU}"
run "ALTER ROLE ${prot} ON CLUSTER ${CLU} NOT PROTECTED"
run "CREATE ROLE OR REPLACE ${prot} ON CLUSTER ${CLU}"
run "CREATE ROLE ${newc} ON CLUSTER ${CLU} PROTECTED"

# --- Assertions (deterministic end-state), as admin ---
# (C) protected role survived every denial
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.roles WHERE name = '${prot}'"
# (D) still protected (NOT PROTECTED was denied)
${CLICKHOUSE_CLIENT} -q "SHOW CREATE ROLE ${prot}" | grep -c "PROTECTED"
# (E) not renamed
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.roles WHERE name = 'hijacked_${CLICKHOUSE_DATABASE}'"
# (F) no new protected role created (local)
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.roles WHERE name = '${newp}'"
# (G) no new protected role created (on cluster)
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.roles WHERE name = '${newc}'"
# (H) §7 gate-specificity: an ON CLUSTER un-protect names the Aiven privilege on the initiator
${CLICKHOUSE_CLIENT} --user "${u}" -q "ALTER ROLE ${prot} ON CLUSTER ${CLU} NOT PROTECTED" 2>&1 \
    | grep -c -m1 "PROTECTED ACCESS MANAGEMENT"

cleanup

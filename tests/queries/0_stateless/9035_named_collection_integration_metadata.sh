#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Named collections are server-global; scope the name to this test's database to avoid collisions.
NC_NAME="nc_integration_meta_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP TABLE IF EXISTS tbl_9035;
DROP NAMED COLLECTION IF EXISTS ${NC_NAME};
"

# A collection carrying the Aiven integration-tracking metadata keys alongside the URL config.
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION ${NC_NAME} AS url = 'http://localhost:${CLICKHOUSE_PORT_HTTP}/', format = 'CSV', integration_id = 'abc123', integration_hash = 'def456';"

# StorageURL validates the collection against a FIXED key set {url, format, ...} that does NOT
# include integration_id/integration_hash. Pre-patch: validateNamedCollection treats integration_id
# as an unexpected key and throws BAD_ARGUMENTS. Post-patch: the metadata keys are whitelisted and
# the table is created (no network I/O — URL engine connects only on read/write).
$CLICKHOUSE_CLIENT -q "CREATE TABLE tbl_9035 (c String) ENGINE = URL(${NC_NAME})" 2>&1 \
  | grep -oE 'BAD_ARGUMENTS|Unexpected key `integration_(id|hash)`' | sort -u

$CLICKHOUSE_CLIENT -q "EXISTS TABLE tbl_9035"

$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP TABLE IF EXISTS tbl_9035;
DROP NAMED COLLECTION IF EXISTS ${NC_NAME};
"

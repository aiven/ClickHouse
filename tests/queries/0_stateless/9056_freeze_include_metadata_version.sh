#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Patch 056: ALTER TABLE ... FREEZE must preserve the per-part metadata_version.txt
# file in the frozen shadow/ directory. Pre-patch the local-disk freeze path dropped
# it (ClonePartParams::keep_metadata_version defaulted to false, so
# DataPartStorageOnDiskBase::freeze removed it from the destination); post-patch it is
# kept (keep_metadata_version = true). The observable is a FILE in the frozen output,
# not a SQL row: it is discovered via the FREEZE verbose result's part_backup_path,
# which is the absolute path of the frozen part directory on the local disk.

BACKUP_NAME="${CLICKHOUSE_DATABASE}_9056"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS freeze_mv SYNC"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE freeze_mv (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "INSERT INTO freeze_mv SELECT 1"

# The live (unfrozen) part must already carry metadata_version.txt, otherwise the
# differential below would be vacuous (pass/pass) instead of an honest fail/pass.
LIVE_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'freeze_mv' AND active LIMIT 1")
if [ -f "${LIVE_PATH}metadata_version.txt" ]; then
    echo "live_part_metadata_version: PRESENT"
else
    echo "live_part_metadata_version: ABSENT"
fi

# Freeze the table and discover the frozen part directory from the verbose result
# (last TSV column = part_backup_path, an absolute path ending with '/').
PART_BACKUP_PATH=$(${CLICKHOUSE_CLIENT} -q "ALTER TABLE freeze_mv FREEZE WITH NAME '${BACKUP_NAME}' SETTINGS alter_partition_verbose_result = 1 FORMAT TSV" | awk -F'\t' 'NR==1 {print $NF}')

if [ -f "${PART_BACKUP_PATH}metadata_version.txt" ]; then
    echo "frozen_part_metadata_version: PRESENT"
else
    echo "frozen_part_metadata_version: ABSENT"
fi

${CLICKHOUSE_CLIENT} -q "ALTER TABLE freeze_mv UNFREEZE WITH NAME '${BACKUP_NAME}'" > /dev/null 2>&1
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS freeze_mv SYNC"

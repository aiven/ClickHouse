#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Aiven patch 056: `ALTER TABLE ... FREEZE` keeps each part's `metadata_version.txt`, so the
# frozen copy under `shadow/` is a complete part. External backup tools read `shadow/` as the set
# of files a part consists of.
#   - Full part storage: the frozen part has the file, hardlinked to the live one.
#   - Packed part storage: the file lives inside `data.packed`; the frozen archive is a hardlink of
#     the live one instead of a rewritten copy without the file.
# Frozen paths come from the verbose `FREEZE` result (`part_backup_path`, an absolute directory).

inode() { stat -c %i "$1" 2>/dev/null || echo none; }

freeze_part_path()
{
    ${CLICKHOUSE_CLIENT} -q "ALTER TABLE $1 FREEZE WITH NAME '$2' SETTINGS alter_partition_verbose_result = 1 FORMAT TSV" \
        | awk -F'\t' 'NR == 1 {print $NF}'
}

live_part_path()
{
    ${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '$1' AND active LIMIT 1"
}

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS freeze_full SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS freeze_packed SYNC"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE freeze_full (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS min_bytes_for_full_part_storage = 0"
${CLICKHOUSE_CLIENT} -q "INSERT INTO freeze_full SELECT number FROM numbers(10)"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE freeze_packed (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS min_bytes_for_full_part_storage = 1000000000"
${CLICKHOUSE_CLIENT} -q "INSERT INTO freeze_packed SELECT number FROM numbers(10)"

${CLICKHOUSE_CLIENT} -q "SELECT table, part_storage_type FROM system.parts WHERE database = currentDatabase() AND active ORDER BY table"

# Full part storage. The live part must have the file, otherwise the check below is vacuous.
LIVE=$(live_part_path freeze_full)
FROZEN=$(freeze_part_path freeze_full "${CLICKHOUSE_DATABASE}_aiven_056_full")
[ -f "${LIVE}metadata_version.txt" ] && echo "full live: present" || echo "full live: absent"
[ -f "${FROZEN}metadata_version.txt" ] && echo "full frozen: present" || echo "full frozen: absent"
[ "$(inode "${LIVE}metadata_version.txt")" = "$(inode "${FROZEN}metadata_version.txt")" ] && echo "full frozen: hardlink" || echo "full frozen: not a hardlink"

# Packed part storage.
LIVE=$(live_part_path freeze_packed)
FROZEN=$(freeze_part_path freeze_packed "${CLICKHOUSE_DATABASE}_aiven_056_packed")
[ "$(inode "${LIVE}data.packed")" = "$(inode "${FROZEN}data.packed")" ] && echo "packed frozen: hardlink" || echo "packed frozen: not a hardlink"

${CLICKHOUSE_CLIENT} -q "ALTER TABLE freeze_full UNFREEZE WITH NAME '${CLICKHOUSE_DATABASE}_aiven_056_full'" > /dev/null
${CLICKHOUSE_CLIENT} -q "ALTER TABLE freeze_packed UNFREEZE WITH NAME '${CLICKHOUSE_DATABASE}_aiven_056_packed'" > /dev/null
${CLICKHOUSE_CLIENT} -q "DROP TABLE freeze_full SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE freeze_packed SYNC"

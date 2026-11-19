#!/usr/bin/env bash

# `MergeTreeSettings::applyCompatibilitySetting` runs once per process, from the first
# `Context::getMergeTreeSettings` call, with that context's `compatibility`. A running server made
# that call long ago, so only a fresh process can drive it: `clickhouse-local` makes its first
# `MergeTree` access under `--compatibility`. The oldest version writes back every `MergeTree`
# history entry, so this fails if any entry names an absent setting or carries a value that no
# longer fits its type. On a consistent build the skip-and-warn guard never fires, so its warning
# must not appear either.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

if ! output=$($CLICKHOUSE_LOCAL --compatibility 1.0 --send_logs_level warning -q "
    CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t VALUES (1);
    SELECT 'mergetree-full-replay', count() FROM t;
" 2>&1); then
    echo "$output"
fi

grep -F 'mergetree-full-replay' <<< "$output"
echo -e "guard-warnings\t$(grep -cF 'is recorded in SettingsChangesHistory' <<< "$output")"

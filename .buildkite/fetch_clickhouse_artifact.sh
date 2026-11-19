#!/usr/bin/env bash
# Download the clickhouse binary artifact from a prior Buildkite step and stage
# it where praktika job scripts look first: ci/tmp/clickhouse.
#
# Both build flavours upload the same relative path, so --step is mandatory.
#
# Usage: fetch_clickhouse_artifact.sh <buildkite-step-key>
set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <buildkite-step-key>" >&2
    exit 2
fi

step_key="$1"
artifact="ci/tmp/build/programs/self-extracting/clickhouse"

mkdir -p ci/tmp
buildkite-agent artifact download --step "$step_key" "$artifact" .
cp "$artifact" ci/tmp/clickhouse
chmod +x ci/tmp/clickhouse

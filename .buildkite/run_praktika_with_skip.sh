#!/usr/bin/env bash
# Run a praktika job, forwarding test exclusions from a file as --skip.
#
# This lives in a script (not inline in pipeline.yml) on purpose: Buildkite
# interpolates $/${...} in the pipeline YAML before parsing it, which collides
# with bash parameter expansions like ${#SKIP[@]} and fails pipeline upload with
# "Expected identifier to start with a letter, got #". Script files are not
# interpolated, so the bash here is safe.
#
# Usage: run_praktika_with_skip.sh "<praktika job name>" <exclude-file> [extra praktika args...]
set -euo pipefail

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 \"<praktika job name>\" <exclude-file> [extra praktika args...]" >&2
    exit 2
fi

job_name="$1"
exclude_file="$2"
shift 2

# Read exclusion patterns, ignoring blank lines and '#' comments. Test names have
# no spaces, so a line-at-a-time read is sufficient and portable (no bash-4 mapfile).
SKIP=()
if [ -f "$exclude_file" ]; then
    while IFS= read -r line; do
        SKIP+=("$line")
    done < <(grep -vE '^[[:space:]]*(#|$)' "$exclude_file" || true)
fi

RUN=(run "$job_name")
# Only append --skip when there is at least one pattern: praktika's --skip
# requires >=1 value, and expanding an empty array under `set -u` would error.
if [ ${#SKIP[@]} -gt 0 ]; then
    RUN+=(--skip "${SKIP[@]}")
fi
RUN+=("$@")

set -x
python3 -m ci.praktika "${RUN[@]}"

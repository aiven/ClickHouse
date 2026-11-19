#!/usr/bin/env bash
# Run all Aiven-specific integration tests.
#
# Aiven integration suites live under tests/integration/test_aiven*/.
# The regular integration harness does not treat `--test test_aiven` as a prefix
# match for directories such as test_aiven_foo, so this helper expands the exact
# module paths first and passes them to praktika via --test.
#
# The job must match the binary flavour staged by the pipeline (amd_asan_ubsan
# on 26.8). Any batch works when --test selects exact modules, but the batch
# count changes between upstream versions (6 on 26.8.2.7, 8 on 26.8.34.13), so
# the name of batch 1 is read from the PR workflow instead of hard-coded.
#
# `test_aiven_smoke` always exists, so finding no tests means the glob or the
# checkout is broken, and the lane fails instead of passing without testing.
set -euo pipefail

shopt -s nullglob

AIVEN_TESTS=()
for test_file in tests/integration/test_aiven*/test*.py; do
    AIVEN_TESTS+=("${test_file#tests/integration/}")
done

if [ ${#AIVEN_TESTS[@]} -eq 0 ]; then
    echo "ERROR: no Aiven integration tests found under tests/integration/test_aiven*/" >&2
    exit 1
fi

# All modules run in one pytest session, where two `test.py` files without a
# package `__init__.py` collide as "import file mismatch".
MISSING_INIT=()
for test_module in "${AIVEN_TESTS[@]}"; do
    test_dir="tests/integration/${test_module%/*}"
    if [ ! -f "${test_dir}/__init__.py" ]; then
        MISSING_INIT+=("${test_dir}")
    fi
done
if [ ${#MISSING_INIT[@]} -gt 0 ]; then
    echo "ERROR: Aiven integration test directories without __init__.py:" >&2
    printf '  %s\n' "${MISSING_INIT[@]}" >&2
    exit 1
fi

JOB_NAME="$(python3 - <<'EOF'
import sys

sys.path[:0] = [".", "./ci"]
from ci.workflows.pull_request import workflow

prefix = "Integration tests (amd_asan_ubsan, db disk, old analyzer, 1/"
names = [job.name for job in workflow.jobs if job.name.startswith(prefix)]
if len(names) != 1:
    sys.exit(f"ERROR: expected one job named '{prefix}N)', found {names}")
print(names[0])
EOF
)"

printf 'Found %d Aiven integration test module(s):\n' "${#AIVEN_TESTS[@]}"
printf '  %s\n' "${AIVEN_TESTS[@]}"

set -x
python3 -m ci.praktika run "${JOB_NAME}" --test "${AIVEN_TESTS[@]}"

#!/usr/bin/env bash
# Run all Aiven-specific integration tests, if present.
#
# Aiven integration suites are expected to live under tests/integration/test_aiven*/.
# The regular integration harness does not treat `--test test_aiven` as a prefix
# match for directories such as test_aiven_foo, so this helper expands the exact
# module paths first and passes them to praktika via --test.
set -euo pipefail

shopt -s nullglob

AIVEN_TESTS=()
for test_file in tests/integration/test_aiven*/test*.py; do
    AIVEN_TESTS+=("${test_file#tests/integration/}")
done

if [ ${#AIVEN_TESTS[@]} -eq 0 ]; then
    echo "No Aiven integration tests found under tests/integration/test_aiven*/; skipping Aiven lane."
    exit 0
fi

printf 'Found %d Aiven integration test module(s):\n' "${#AIVEN_TESTS[@]}"
printf '  %s\n' "${AIVEN_TESTS[@]}"

bash .buildkite/run_praktika_with_skip.sh \
    "Integration tests (amd_binary, 1/5)" \
    .buildkite/excluded_tests_integration.txt \
    --test "${AIVEN_TESTS[@]}"

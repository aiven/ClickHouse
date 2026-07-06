#!/usr/bin/env bash
# Run all Aiven-specific stateless tests, if present.
#
# Aiven stateless tests are expected to use the 9XXX range under
# tests/queries/0_stateless/. The functional test harness expects test names, not
# paths, so this helper expands 9*.sql/9*.sh files to their extensionless basenames
# and passes them to praktika via --test.
set -euo pipefail

shopt -s nullglob

AIVEN_TESTS=()
for test_file in tests/queries/0_stateless/9*.sql tests/queries/0_stateless/9*.sh; do
    test_name="${test_file##*/}"
    AIVEN_TESTS+=("${test_name%.*}")
done

if [ ${#AIVEN_TESTS[@]} -eq 0 ]; then
    echo "No Aiven stateless tests found under tests/queries/0_stateless/9*.{sql,sh}; skipping Aiven stateless lane."
    exit 0
fi

printf 'Found %d Aiven stateless test(s):\n' "${#AIVEN_TESTS[@]}"
printf '  %s\n' "${AIVEN_TESTS[@]}"

bash .buildkite/run_praktika_with_skip.sh \
    "Stateless tests (amd_debug, parallel)" \
    .buildkite/excluded_tests_stateless.txt \
    --test "${AIVEN_TESTS[@]}"

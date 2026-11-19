#!/usr/bin/env bash
# Run all Aiven-specific stateless tests.
#
# Aiven stateless tests use `aiven_<NNN>_<slug>` under tests/queries/0_stateless/
# (glob aiven_*; aligned with tests/integration/test_aiven_<slug>/ for the lane).
# The functional test harness expects test names, not paths, so this helper
# expands matching files to extensionless basenames and passes them to praktika
# via --test.
#
# `aiven_000_smoke` always exists, so finding no tests means the glob or the
# checkout is broken, and the lane fails instead of passing without testing.
set -euo pipefail

shopt -s nullglob

AIVEN_TESTS=()
for test_file in tests/queries/0_stateless/aiven_*.sql tests/queries/0_stateless/aiven_*.sh; do
    test_name="${test_file##*/}"
    AIVEN_TESTS+=("${test_name%.*}")
done

if [ ${#AIVEN_TESTS[@]} -eq 0 ]; then
    echo "ERROR: no Aiven stateless tests found under tests/queries/0_stateless/aiven_*.{sql,sh}" >&2
    exit 1
fi

printf 'Found %d Aiven stateless test(s):\n' "${#AIVEN_TESTS[@]}"
printf '  %s\n' "${AIVEN_TESTS[@]}"

set -x
python3 -m ci.praktika run "Stateless tests (amd_debug, parallel)" --test "${AIVEN_TESTS[@]}"

#!/usr/bin/env bash
# Generates the CA PEM used by the positive case of test_aiven_azure_custom_ca_path.
#
# This is the "proportionate" coverage for patch 013 (Azure custom per-disk <ca_path>): it
# verifies the config -> RequestSettings -> PocoAzureHTTPClientConfiguration -> makeCAContext
# plumbing. The end-to-end TLS-verification semantics are inherited from patch 012's S3 test
# (test_aiven_s3_custom_ca_path), which exercises the SAME makeHTTPSession / HTTPConnectionPool /
# Poco::Net::Context machinery that 013 reuses.
#
# The fixture runs this on every start, so the PEM is always freshly minted and can never expire
# out from under the test (no committed, slowly-rotting cert to maintain). The generated files are
# git-ignored.
#
# The positive case talks to Azurite over plain HTTP, so the trust content of this bundle is never
# used in a handshake. It only needs to be a file that Poco::Net::Context can load: that proves a
# valid <ca_path> is parsed and a context is built without breaking normal operation. The negative
# case uses a nonexistent path, which makes that same context construction fail.
set -euo pipefail
cd "$(dirname "$0")"

openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout azure_ca.key -out azure_ca.crt \
    -subj "/O=ClickHouse Aiven Test/CN=azure-test-ca" \
    -days 3650

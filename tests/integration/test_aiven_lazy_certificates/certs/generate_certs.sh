#!/usr/bin/env bash
# Generates the valid self-signed server certificate used by the leg-B (secure-port) node of
# test_aiven_lazy_certificates.
#
# The fixture runs this on every start, so the certificate is always freshly minted and can never
# expire out from under the test (no committed, slowly-rotting PEMs to maintain). The generated
# files (server-cert.pem, server-key.pem) are git-ignored.
#
# Leg B only needs a parseable, currently-valid certificate/key pair so that the server's
# CertificateReloader proceeds past the guard and actually loads the server certificate when a
# secure port is configured. Verification mode is "none" on both ends, so the SAN/CN values only
# need to be syntactically valid.
set -euo pipefail
cd "$(dirname "$0")"

openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout server-key.pem -out server-cert.pem \
    -subj "/O=ClickHouse Aiven Test/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
    -days 3650

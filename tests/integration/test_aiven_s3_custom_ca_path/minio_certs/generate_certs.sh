#!/usr/bin/env bash
# Generates the self-signed certificate used by test_aiven_s3_custom_ca_path.
#
# The test fixture runs this on every start, so the certificate is always freshly minted and can
# never expire out from under the test (no committed, slowly-rotting PEMs to maintain). The
# generated files are git-ignored.
#
# The certificate is self-signed and acts as both:
#   * MinIO's server certificate (minio_certs/public.crt + minio_certs/private.key), and
#   * the trust anchor ClickHouse pins per-disk via <ca_path> (configs/minio_ca.crt).
#
# The system trust store does NOT contain it, so a disk without ca_path cannot verify it - which
# is exactly what the negative test relies on. SAN must cover the MinIO container hostname (minio1).
set -euo pipefail
cd "$(dirname "$0")"

openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout private.key -out public.crt \
    -subj "/O=ClickHouse Aiven Test/CN=minio1" \
    -addext "subjectAltName=DNS:minio1,DNS:localhost,DNS:*.minio" \
    -days 3650

# MinIO expects trusted CAs under CAs/; mirror the self-signed cert there.
mkdir -p CAs
cp public.crt CAs/public.crt

# The CA file mounted into the ClickHouse container and referenced by <ca_path>.
cp public.crt ../configs/minio_ca.crt

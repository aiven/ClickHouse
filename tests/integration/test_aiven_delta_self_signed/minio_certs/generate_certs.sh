#!/usr/bin/env bash
# Generates the self-signed certificate used by test_aiven_delta_self_signed.
#
# The fixture runs this on every start, so the certificate is always freshly minted and can never
# expire out from under the test (no committed, slowly-rotting PEMs to maintain). The generated
# files are git-ignored.
#
# The certificate is self-signed and acts as MinIO's server certificate
# (minio_certs/public.crt + minio_certs/private.key, mirrored under CAs/). It is NOT installed in
# any client trust store: neither the ClickHouse container's system roots nor the delta-kernel-rs
# (Rust object_store) default root set. That untrusted-ness is exactly what the test relies on -
# the delta-kernel HTTPS handshake must FAIL pre-patch and only succeed post-patch via the
# loopback-only `allow_invalid_certificates` relaxation.
#
# SAN covers the MinIO container hostname (minio1) plus the loopback names the delta-kernel client
# actually connects to through the in-container TCP forwarder (localhost / 127.0.0.1).
set -euo pipefail
cd "$(dirname "$0")"

openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout private.key -out public.crt \
    -subj "/O=ClickHouse Aiven Test/CN=minio1" \
    -addext "subjectAltName=DNS:minio1,DNS:localhost,IP:127.0.0.1" \
    -days 3650

# MinIO expects trusted CAs under CAs/; mirror the self-signed cert there.
mkdir -p CAs
cp public.crt CAs/public.crt

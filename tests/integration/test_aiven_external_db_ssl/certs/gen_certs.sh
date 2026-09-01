#!/usr/bin/env bash
# Generate a self-contained CA plus CA-signed server certificates for the
# Aiven external-DB SSL harness. Re-runnable: every invocation regenerates the
# full chain so the test is reproducible from a clean checkout.
#
#   - ca.crt / ca.key                : the test CA (clients validate against ca.crt)
#   - pg-server.crt / pg-server.key  : PostgreSQL server cert, SAN = postgres1, postgre-sql.local
#   - mysql-server.crt / mysql-server.key : MySQL server cert, SAN = mysql80
#
# Server private keys are written 0600 (PostgreSQL refuses group/other-readable keys).
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

DAYS=3650

# --- CA -------------------------------------------------------------------
# basicConstraints CA:TRUE + keyUsage keyCertSign are REQUIRED: OpenSSL 3.x
# (Python's ssl, used by pymysql) rejects a verifying CA that lacks the
# keyCertSign key-usage bit ("CA cert does not include key usage extension").
openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout ca.key -out ca.crt -days "$DAYS" \
    -subj "/CN=aiven-external-db-test-ca" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null

# --- helper: emit a CA-signed leaf cert with subjectAltName ----------------
# usage: gen_server_cert <basename> <CN> <comma-separated SAN list>
gen_server_cert() {
    local name="$1"
    local cn="$2"
    local san="$3"

    local extfile
    extfile="$(mktemp)"
    printf 'subjectAltName=%s\n' "$san" > "$extfile"

    openssl req -newkey rsa:2048 -nodes \
        -keyout "${name}.key" -out "${name}.csr" \
        -subj "/CN=${cn}" 2>/dev/null
    openssl x509 -req -in "${name}.csr" \
        -CA ca.crt -CAkey ca.key -CAcreateserial \
        -out "${name}.crt" -days "$DAYS" \
        -extfile "$extfile" 2>/dev/null
    rm -f "${name}.csr" "$extfile"
}

gen_server_cert pg-server    postgres1 "DNS:postgres1,DNS:postgre-sql.local"
gen_server_cert mysql-server mysql80   "DNS:mysql80"

chmod 600 ca.key pg-server.key mysql-server.key
chmod 644 ca.crt pg-server.crt mysql-server.crt

echo "Generated CA + server certs in: $DIR"

"""Patch 059 - skip loading unused server certificates.

When the server certificate/key paths are configured in the openSSL.server section but the server
does not actually serve a secure port (neither tcp_port_secure nor https_port is set), ClickHouse
should NOT try to read/parse those files. Stale or non-existent paths left in the config must not
produce a spurious certificate read/parse error.

The patch adds an early-return guard at the top of `CertificateReloader::tryLoadImpl`, scoped to the
server prefix (`Poco::Net::SSLManager::CFG_SERVER_PREFIX`): when neither `tcp_port_secure` nor
`https_port` is configured, server-certificate loading is skipped and the server logs
`No server certificates needed as tcp_port_secure and https_port are not provided`.

The scoping to the server prefix is load-bearing on 26.3: client certificates (used for outgoing
mutual TLS) route through the SAME `tryLoadImpl` with the client prefix and are independent of the
secure server ports, so they must still be loaded.

Legs:
  * Leg A (the fix) - `node_no_secure`: configures non-existent server cert/key paths and NO secure
    port. Asserts the server starts, logs `No server certificates needed`, and does NOT log a
    certificate read attempt for the bogus paths. PRE-patch this same assertion fails: the server
    logs an error referencing the missing path (it tried to read it).
  * Leg B (guard does not over-skip) - `node_secure`: configures `https_port` with a valid,
    freshly-generated server certificate. Asserts the server loads the certificate
    (`Reloaded certificate`), does NOT log `No server certificates needed`, and actually serves the
    HTTPS port.
"""

import os
import ssl
import subprocess
import urllib.request

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

# Basename of the paths configured in no_secure_bad_cert.xml; intentionally never created.
MISSING_PATH_MARKER = "does_not_exist"
# The guard message is emitted by the CertificateReloader logger; scope assertions to it so that
# unrelated SSL-context consumers (e.g. MySQLHandlerFactory, which lazily builds its own context
# from openSSL.server and is NOT touched by this patch) do not pollute the signal.
RELOADER = "CertificateReloader"
GUARD_LOG = "No server certificates needed"
HTTPS_PORT = 8443

cluster = ClickHouseCluster(__file__)

# Leg A: bogus server cert paths, no secure port. The guard must short-circuit certificate loading.
node_no_secure = cluster.add_instance(
    "node_no_secure",
    main_configs=["configs/no_secure_bad_cert.xml"],
)

# Leg B: a real https_port with a valid (generated) server certificate. The guard must NOT fire.
node_secure = cluster.add_instance(
    "node_secure",
    main_configs=[
        "configs/secure.xml",
        "certs/server-cert.pem",
        "certs/server-key.pem",
    ],
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    # Mint the leg-B server certificate fresh before the containers mount it.
    subprocess.check_call(
        ["bash", os.path.join(SCRIPT_DIR, "certs", "generate_certs.sh")]
    )
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_no_secure_port_skips_certificate_loading(started_cluster):
    # The server must come up despite the bogus, never-used certificate paths.
    assert node_no_secure.query("SELECT 1").strip() == "1"

    guard_log = node_no_secure.grep_in_log(f"{RELOADER}: {GUARD_LOG}")

    # CertificateReloader must NOT have attempted to read the never-used server cert/key. Filter the
    # missing-path references to CertificateReloader lines only - other components (e.g. the MySQL
    # protocol handler) build their own SSL context and are out of scope for this patch.
    missing_path_lines = node_no_secure.grep_in_log(MISSING_PATH_MARKER)
    reloader_cert_errors = "\n".join(
        line for line in missing_path_lines.splitlines() if RELOADER in line
    )

    print(f"\n[leg A guard log] {guard_log.strip()[:400]}")
    print(f"[leg A CertificateReloader missing-path refs] {reloader_cert_errors.strip()[:400]}")

    # POST-patch: the guard short-circuits server-cert loading and logs the guard message.
    # PRE-patch: this message is absent (the guard does not exist), so this assertion fails.
    assert GUARD_LOG in guard_log, (
        f"expected the patched guard message {GUARD_LOG!r} from {RELOADER} in the server log, "
        "but it was absent (pre-patch behavior)."
    )

    # POST-patch: CertificateReloader never touches the bogus path.
    # PRE-patch: CertificateReloader tries to read it and logs an error referencing the path.
    assert reloader_cert_errors.strip() == "", (
        f"{RELOADER} should not reference the never-used certificate path, "
        f"but the log mentions it: {reloader_cert_errors.strip()[:400]!r}"
    )


def test_secure_port_still_loads_certificate(started_cluster):
    # The server must come up and serve the secure port.
    assert node_secure.query("SELECT 1").strip() == "1"

    guard_log = node_secure.grep_in_log(GUARD_LOG)
    reload_log = node_secure.grep_in_log("Reloaded certificate")

    print(f"[leg B reload log] {reload_log.strip()[:400]}")

    # The guard is scoped to the no-secure-port case, so with a secure port it must NOT fire.
    assert guard_log.strip() == "", (
        f"the guard fired even though a secure port is configured: {guard_log.strip()[:400]!r}"
    )
    # The server certificate must actually be loaded.
    assert "Reloaded certificate" in reload_log, (
        "expected the server certificate to be loaded ('Reloaded certificate') "
        f"but it was not in the log: {reload_log.strip()[:400]!r}"
    )

    # End-to-end: the HTTPS port actually serves TLS with the loaded certificate.
    url = f"https://{node_secure.ip_address}:{HTTPS_PORT}/?query=SELECT+1"
    context = ssl._create_unverified_context()
    response = urllib.request.urlopen(url, context=context, timeout=30).read()
    assert response.decode("utf-8").strip() == "1"

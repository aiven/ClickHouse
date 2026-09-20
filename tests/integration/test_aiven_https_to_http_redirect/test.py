"""Patch 065 — prohibit https->http redirect downgrades.

A redirect that downgrades the scheme from https to http is an SSRF / secure-
transport-downgrade vector: a trusted HTTPS endpoint could bounce ClickHouse to
an arbitrary internal plain-HTTP resource. The patch makes ClickHouse refuse to
follow such a redirect (error code UNACCEPTABLE_URL) on the url() read path.

Harness: three localhost servers run inside the node container (see
redirect_servers.py). The node's outbound HTTPS client trusts the self-signed
origin cert (configs/client_ssl.xml, verificationMode=none) so the handshake
succeeds and we reach the redirect-follow code.

Evidence of causation:
  * test_https_to_http_redirect_rejected — POST-patch throws UNACCEPTABLE_URL;
    PRE-patch the redirect is followed and the SELECT returns rows (so the test
    fails pre-patch on the missing rejection, not on a feature-absence error).
  * test_http_to_http_redirect_allowed — control: a same-scheme redirect keeps
    working both pre- and post-patch, proving we block only the downgrade.
"""

import os
import subprocess
import tempfile

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import wait_condition

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/client_ssl.xml"])

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

HTTPS_ORIGIN_PORT = 8443
HTTP_ORIGIN_PORT = 8081
DATA_PORT = 8000


def _start_redirect_servers():
    container_id = cluster.get_container_id("node")

    tmp = tempfile.mkdtemp()
    cert = os.path.join(tmp, "origin.crt")
    key = os.path.join(tmp, "origin.key")
    subprocess.run(
        [
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", key, "-out", cert, "-days", "36500",
            "-subj", "/CN=localhost",
        ],
        check=True,
        capture_output=True,
    )

    # copy_file_to_container runs as the container's default (non-root) user, so
    # the destination must be writable by it — /tmp is the safe universal choice.
    for src, dst in (
        (cert, "/tmp/origin.crt"),
        (key, "/tmp/origin.key"),
        (os.path.join(SCRIPT_DIR, "redirect_servers.py"), "/tmp/redirect_servers.py"),
    ):
        cluster.copy_file_to_container(container_id, src, dst)

    cluster.exec_in_container(
        container_id,
        [
            "bash",
            "-c",
            f"python3 /tmp/redirect_servers.py {HTTPS_ORIGIN_PORT} {HTTP_ORIGIN_PORT} "
            f"{DATA_PORT} /tmp/origin.crt /tmp/origin.key > /tmp/redirect_servers.log 2>&1",
        ],
        detach=True,
        user="root",
    )

    def check_servers():
        return cluster.exec_in_container(
            container_id,
            [
                "bash",
                "-c",
                f"curl -sk https://localhost:{HTTPS_ORIGIN_PORT}/; echo; "
                f"curl -s http://localhost:{HTTP_ORIGIN_PORT}/; echo; "
                f"curl -s http://localhost:{DATA_PORT}/; echo",
            ],
            nothrow=True,
        )

    try:
        wait_condition(
            check_servers,
            lambda r: r.count("ok") == 3,
            max_attempts=30,
            delay=0.5,
        )
    except Exception:
        log = cluster.exec_in_container(
            container_id,
            ["bash", "-c", "cat /tmp/redirect_servers.log 2>&1 || true"],
            nothrow=True,
        )
        print(f"\n[redirect_servers.log]\n{log}\n[last probe]\n{check_servers()}")
        raise


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        _start_redirect_servers()
        yield cluster
    finally:
        cluster.shutdown()


def test_https_to_http_redirect_rejected(started_cluster):
    err = node.query_and_get_error(
        f"SELECT a FROM url('https://localhost:{HTTPS_ORIGIN_PORT}/data', "
        f"'JSONEachRow', 'a UInt8') ORDER BY a SETTINGS max_http_get_redirects=2"
    )
    print(f"\n[https->http redirect] {err.strip()[:400]}")
    assert "UNACCEPTABLE_URL" in err or "HTTPS to HTTP" in err, (
        f"expected an https->http downgrade rejection, got: {err}"
    )


def test_http_to_http_redirect_allowed(started_cluster):
    result = node.query(
        f"SELECT a FROM url('http://localhost:{HTTP_ORIGIN_PORT}/data', "
        f"'JSONEachRow', 'a UInt8') ORDER BY a SETTINGS max_http_get_redirects=2"
    )
    assert result.strip().split("\n") == ["1", "2"], (
        f"same-scheme redirect should still work, got: {result!r}"
    )

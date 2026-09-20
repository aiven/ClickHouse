import logging
import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import wait_condition

# This test exercises the Aiven-carried feature "delegate S3 signature to a separate process"
# (per-disk <signature_delegation_url>). A small SigV4 "signing proxy" runs inside the instance
# container; ClickHouse's AWSAuthV4DelegatedSigner POSTs the AWS canonical request to it and uses
# the returned signature instead of signing locally. MinIO (plain HTTP) is the S3 backend.
#
#   * positive -> a disk whose signature_delegation_url points at the proxy's /sign endpoint can
#                 CREATE/INSERT/SELECT, and the proxy records at least one signing request (so we
#                 know delegation was actually used, not bypassed).
#   * negative -> a disk pointed at /sign_wrong, which returns a deliberately wrong signature,
#                 MUST fail with a signature/authorization error from MinIO.

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROXY_PORT = 8080

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "node",
    main_configs=[
        "configs/config.d/storage_conf.xml",
        "configs/config.d/fast_s3_retry.xml",
    ],
    with_minio=True,
    with_remote_database_disk=False,
)


def _start_signing_proxy():
    # Copy to /tmp (world-writable): copy_file_to_container runs as the container's default
    # non-root user, which cannot write to the container root.
    instance.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "signing_proxy.py"),
        "/tmp/signing_proxy.py",
    )
    instance.exec_in_container(
        [
            "bash",
            "-c",
            "python3 /tmp/signing_proxy.py > /var/log/clickhouse-server/signing_proxy.log 2>&1",
        ],
        detach=True,
        user="root",
    )

    def check_server() -> str:
        return instance.exec_in_container(
            ["curl", "-s", f"http://127.0.0.1:{PROXY_PORT}/health"],
            nothrow=True,
        )

    wait_condition(
        check_server,
        lambda response: response == "OK",
        max_attempts=20,
        delay=0.5,
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        logging.info("Starting cluster...")
        cluster.start()
        _start_signing_proxy()
        logging.info("Cluster started, signing proxy is up")
        yield cluster
    finally:
        cluster.shutdown()


def _proxy_sign_count() -> int:
    return int(
        instance.exec_in_container(
            ["curl", "-s", f"http://127.0.0.1:{PROXY_PORT}/count"],
            nothrow=True,
        )
    )


def test_signature_delegation_used_for_s3_disk(started_cluster):
    before = _proxy_sign_count()

    instance.query("DROP TABLE IF EXISTS s3_sig_ok SYNC")
    instance.query(
        """
        CREATE TABLE s3_sig_ok (id Int64, data String)
        ENGINE = MergeTree() ORDER BY id
        SETTINGS storage_policy='s3_delegated'
        """
    )
    instance.query("INSERT INTO s3_sig_ok VALUES (0, 'a'), (1, 'b')")

    assert instance.query("SELECT count() FROM s3_sig_ok").strip() == "2"
    assert instance.query("SELECT data FROM s3_sig_ok ORDER BY id").split() == ["a", "b"]

    after = _proxy_sign_count()
    assert after > before, (
        "expected the signing proxy to receive at least one canonical request, "
        f"but the counter did not advance ({before} -> {after})"
    )


def test_wrong_delegated_signature_fails(started_cluster):
    instance.query("DROP TABLE IF EXISTS s3_sig_bad SYNC")

    # CREATE on an s3 policy already writes table metadata into the bucket, so the bad signature
    # surfaces here (and at INSERT otherwise). The proxy returns a syntactically valid but wrong
    # signature, so MinIO rejects the request.
    create = (
        "CREATE TABLE s3_sig_bad (id Int64, data String) "
        "ENGINE = MergeTree() ORDER BY id SETTINGS storage_policy='s3_delegated_wrong'"
    )
    _stdout, error = instance.query_and_get_answer_with_error(create)
    if not error:
        _stdout, error = instance.query_and_get_answer_with_error(
            "INSERT INTO s3_sig_bad VALUES (0, 'a'), (1, 'b')"
        )

    assert error, "expected the wrong-signature disk to fail, but the writes succeeded"
    lowered = error.lower()
    assert any(
        kw in lowered
        for kw in ("signature", "signaturedoesnotmatch", "access denied", "forbidden", "authorization")
    ), f"expected a signature/authorization failure, got: {error}"

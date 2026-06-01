import logging
import os
import subprocess

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

# This test exercises the Aiven-carried feature "custom CA certificate path for S3 connections"
# (per-disk <ca_path>). MinIO is served over HTTPS with a self-signed certificate that the system
# trust store does NOT contain. The global openSSL client (configs/config.d/ssl.xml) verifies and
# rejects invalid certificates, so the ONLY way ClickHouse can talk to MinIO is via a disk that
# pins the MinIO CA through <ca_path>.
#
# Coverage of the sensitive parts:
#   * positive  -> a disk WITH the correct ca_path can do TLS to the self-signed MinIO.
#   * negative  -> a disk WITHOUT ca_path, pointed at the SAME host:port, MUST fail certificate
#                  verification. This simultaneously proves (a) the ca_path is load-bearing (not a
#                  vacuous success via a global accept-everything handler), and (b) the connection
#                  pool keys by SSL context: without that, the no-ca disk could silently reuse the
#                  trusting pooled connection of the with-ca disk and pass by accident.


@pytest.fixture(scope="module")
def cluster():
    cluster = ClickHouseCluster(__file__)
    try:
        # Mint the MinIO/CA certificate fresh for this run so it can never expire. Must happen
        # before the containers are configured/started, since both the MinIO certs dir and the
        # per-disk CA file are mounted at startup.
        subprocess.check_call(
            ["bash", os.path.join(SCRIPT_DIR, "minio_certs", "generate_certs.sh")]
        )
        cluster.add_instance(
            "node",
            main_configs=[
                "configs/config.d/storage_conf.xml",
                "configs/config.d/ssl.xml",
                "configs/config.d/fast_s3_retry.xml",
                "configs/minio_ca.crt",
            ],
            with_minio=True,
            minio_certs_dir="minio_certs",
            with_remote_database_disk=False,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")
        yield cluster
    finally:
        cluster.shutdown()


def _create_table(node, table, policy):
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (id Int64, data String)
        ENGINE = MergeTree() ORDER BY id
        SETTINGS storage_policy='{policy}'
        """
    )


def test_custom_ca_path_allows_https_to_self_signed_minio(cluster):
    node = cluster.instances["node"]
    _create_table(node, "s3_ca_ok", "s3_with_ca")

    node.query("INSERT INTO s3_ca_ok VALUES (0, 'a'), (1, 'b')")
    assert node.query("SELECT count() FROM s3_ca_ok").strip() == "2"
    assert node.query("SELECT data FROM s3_ca_ok ORDER BY id").split() == ["a", "b"]


def test_missing_ca_path_fails_certificate_verification(cluster):
    node = cluster.instances["node"]
    node.query("DROP TABLE IF EXISTS s3_ca_missing SYNC")

    # CREATE TABLE on an s3 storage policy already writes table metadata into the bucket, so the
    # missing trust anchor surfaces as a certificate-verification failure here (and at INSERT if
    # not). This disk points at the SAME minio1:9001 endpoint as the with-ca disk: a success here
    # would mean the connection pool handed it the with-ca disk's trusting connection, which is
    # exactly the contamination the pool-key fix prevents. It must fail instead.
    create = (
        "CREATE TABLE s3_ca_missing (id Int64, data String) "
        "ENGINE = MergeTree() ORDER BY id SETTINGS storage_policy='s3_no_ca'"
    )
    _stdout, error = node.query_and_get_answer_with_error(create)
    if not error:
        _stdout, error = node.query_and_get_answer_with_error(
            "INSERT INTO s3_ca_missing VALUES (0, 'a'), (1, 'b')"
        )

    assert error, "expected the no-ca disk to fail certificate verification, but the writes succeeded"
    lowered = error.lower()
    assert any(
        kw in lowered for kw in ("certificate", "ssl", "verify")
    ), f"expected a certificate-verification failure, got: {error}"

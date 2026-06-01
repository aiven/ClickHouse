import logging
import os
import subprocess

import pytest

from helpers.cluster import ClickHouseCluster
from test_storage_azure_blob_storage.test import azure_query

# Proportionate test for patch 013 (Azure Blob Storage custom per-disk <ca_path>).
#
# It runs against the standard *HTTP* Azurite harness (no TLS infrastructure) and asserts only the
# 013-specific surface: that a per-disk <ca_path> is read from config, threaded into the Azure Poco
# HTTP client configuration, and consumed by makeCAContext when the client is constructed.
#
# The end-to-end TLS-verification behavior of the SHARED machinery (makeHTTPSession,
# HTTPConnectionPool keyed by SSL context, Poco::Net::Context) is already covered by patch 012's S3
# test (test_aiven_s3_custom_ca_path) and is inherited here, because the Azure client reuses exactly
# that machinery. A full TLS-Azurite test would require introducing harness divergence (an
# azurite_certs_dir, mirroring minio_certs_dir) that is not justified for the config plumbing alone;
# see docs/aiven/patches/013-azure-custom-ca-path.md, section 5.
#
#   * positive -> a disk with a VALID ca_path builds its SSL context and operates normally (HTTP).
#   * negative -> a disk with a NONEXISTENT ca_path fails when the client/context is constructed,
#                 proving the value is actually read and fed to makeCAContext (not silently ignored).

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
NODE = "node"
ACCOUNT_NAME = "devstoreaccount1"
ACCOUNT_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)
# certs/azure_ca.crt is mounted by main_configs into config.d/.
VALID_CA_PATH = "/etc/clickhouse-server/config.d/azure_ca.crt"
BOGUS_CA_PATH = "/etc/clickhouse-server/config.d/this_azure_ca_file_does_not_exist.crt"


@pytest.fixture(scope="module")
def cluster():
    cluster = ClickHouseCluster(__file__)
    try:
        # Mint the CA PEM fresh for this run so it can never expire; it is mounted into config.d
        # before the container starts, so it must exist before add_instance/start.
        subprocess.check_call(
            ["bash", os.path.join(SCRIPT_DIR, "certs", "generate_ca.sh")]
        )
        cluster.add_instance(
            NODE,
            main_configs=["certs/azure_ca.crt"],
            with_azurite=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")
        yield cluster
    finally:
        cluster.shutdown()


def _create_disk_table(table, ca_path, container, port, skip_access_check):
    ca_line = f"ca_path = '{ca_path}'," if ca_path is not None else ""
    return f"""
        CREATE TABLE {table} (a Int32)
        ENGINE = MergeTree() ORDER BY tuple()
        SETTINGS disk = disk(
            type = azure_blob_storage,
            endpoint = 'http://azurite1:{port}/{ACCOUNT_NAME}/{container}',
            endpoint_contains_account_name = 'true',
            account_name = '{ACCOUNT_NAME}',
            account_key = '{ACCOUNT_KEY}',
            {ca_line}
            skip_access_check = {skip_access_check})
    """


def test_azure_custom_ca_path_valid_is_accepted(cluster):
    node = cluster.instances[NODE]
    port = cluster.azurite_port

    azure_query(node, "DROP TABLE IF EXISTS azure_ca_ok SYNC")
    azure_query(
        node,
        _create_disk_table(
            "azure_ca_ok", VALID_CA_PATH, "contcaok", port, skip_access_check=0
        ),
    )
    azure_query(node, "INSERT INTO azure_ca_ok SELECT number FROM numbers(10)")
    assert azure_query(node, "SELECT count() FROM azure_ca_ok").strip() == "10"


def test_azure_bogus_ca_path_fails(cluster):
    node = cluster.instances[NODE]
    port = cluster.azurite_port

    node.query("DROP TABLE IF EXISTS azure_ca_bogus SYNC")
    create = _create_disk_table(
        "azure_ca_bogus", BOGUS_CA_PATH, "contcabogus", port, skip_access_check=1
    )

    # The nonexistent CA file makes makeCAContext throw when the Azure HTTP client is constructed.
    # Whether the object storage is built at CREATE or on first use, the error surfaces at one of
    # these two statements - so we try CREATE first, then INSERT.
    _stdout, error = node.query_and_get_answer_with_error(create)
    if not error:
        _stdout, error = node.query_and_get_answer_with_error(
            "INSERT INTO azure_ca_bogus SELECT number FROM numbers(10)"
        )

    assert error, "expected the bogus ca_path disk to fail building its SSL context, but it succeeded"
    lowered = error.lower()
    # The error must reference the configured ca_path file: that is the precise proof that <ca_path>
    # was read from the disk config and handed to makeCAContext's file load. Without 013's plumbing
    # the setting would be silently ignored and the disk would come up fine.
    assert BOGUS_CA_PATH.lower() in lowered, (
        f"expected the failure to reference the configured ca_path, got: {error}"
    )
    assert any(
        kw in lowered for kw in ("file not found", "no such file", "cannot load")
    ), f"expected a CA-file load failure, got: {error}"

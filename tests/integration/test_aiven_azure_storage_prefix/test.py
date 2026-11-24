"""
The `storage_prefix` option of an Azure disk configured with `storage_account_url`: every blob
key is placed under the prefix, so several services can share one container under disjoint
prefixes.

Parts are written through a dynamic Azure disk on Azurite, and the blob keys are read with the
Azure SDK directly, which does not apply ClickHouse's prefix and so sees the full keys.
"""

import pytest
from azure.storage.blob import BlobServiceClient

from helpers.cluster import ClickHouseCluster
from test_storage_azure_blob_storage.test import azure_query

NODE_NAME = "node"
ACCOUNT_NAME = "devstoreaccount1"
ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
STORAGE_PREFIX = "project_a"


@pytest.fixture(scope="module")
def cluster():
    cluster = ClickHouseCluster(__file__)
    cluster.add_instance(NODE_NAME, with_azurite=True)
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def blob_names(container_name, port):
    connection_string = f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT_NAME};AccountKey={ACCOUNT_KEY};BlobEndpoint=http://127.0.0.1:{port}/{ACCOUNT_NAME};"
    client = BlobServiceClient.from_connection_string(connection_string).get_container_client(container_name)
    return [blob.name for blob in client.list_blobs()]


def write_parts(node, table, container_name, port, storage_prefix=None):
    """Writes three rows through a dynamic Azure disk and returns the resulting blob keys."""
    prefix_line = f"storage_prefix = '{storage_prefix}'," if storage_prefix is not None else ""
    azure_query(
        node,
        f"""
        DROP TABLE IF EXISTS {table} SYNC;
        CREATE TABLE {table} (a Int32)
        ENGINE = MergeTree() ORDER BY tuple()
        SETTINGS disk = disk(
                     type = azure_blob_storage,
                     storage_account_url = 'http://azurite1:{port}/{ACCOUNT_NAME}',
                     container_name = '{container_name}',
                     {prefix_line}
                     account_name = '{ACCOUNT_NAME}',
                     account_key = '{ACCOUNT_KEY}',
                     skip_access_check = 0),
                 min_bytes_for_wide_part = 0;
        INSERT INTO {table} VALUES (1), (2), (3);
        """,
    )
    assert azure_query(node, f"SELECT count() FROM {table}") == "3\n"
    names = blob_names(container_name, port)
    assert names, "expected at least one blob to be written"
    return names


def test_storage_prefix_applied_to_blob_keys(cluster):
    names = write_parts(cluster.instances[NODE_NAME], "t_prefix", "cont-prefix", cluster.azurite_port, STORAGE_PREFIX)

    assert all(name.startswith(STORAGE_PREFIX + "/") for name in names), names


def test_no_storage_prefix_leaves_keys_at_root(cluster):
    names = write_parts(cluster.instances[NODE_NAME], "t_no_prefix", "cont-no-prefix", cluster.azurite_port)

    assert not any(name.startswith(STORAGE_PREFIX + "/") for name in names), names
    assert not any(name.startswith("/") for name in names), names


def test_slash_storage_prefix_keeps_leading_slash(cluster):
    """
    The managed configuration renders an empty prefix as `/`. Keys written that way start with
    `/`, and existing containers hold such keys, so the layout must not change across versions.
    """
    names = write_parts(cluster.instances[NODE_NAME], "t_slash_prefix", "cont-slash-prefix", cluster.azurite_port, "/")

    assert all(name.startswith("/") and not name.startswith("//") for name in names), names

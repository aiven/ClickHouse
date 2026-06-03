"""
Integration test for patch 024/025 family — the Azure `storage_prefix` disk option.

Patch 025 adds a `storage_prefix` configuration option to the Azure object-storage
disk (usable with `storage_account_url` / `connection_string`). The prefix is
prepended to every blob key, so multiple ClickHouse instances / projects can share
one Azure container under disjoint path prefixes.

We verify the behavior end-to-end against Azurite by writing real parts through a
dynamic Azure disk and inspecting the resulting blob keys with the raw Azure SDK
client (which does NOT apply ClickHouse's prefix wrapper, so it sees the full keys).
"""

import pytest
from azure.storage.blob import BlobServiceClient

from helpers.cluster import ClickHouseCluster
from test_storage_azure_blob_storage.test import azure_query

NODE_NAME = "node"
ACCOUNT_NAME = "devstoreaccount1"
ACCOUNT_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)
STORAGE_PREFIX = "aiven_project_a"


@pytest.fixture(scope="module")
def cluster():
    cluster = ClickHouseCluster(__file__)
    cluster.add_instance(NODE_NAME, with_azurite=True)
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _container_client(container_name, port):
    connection_string = (
        f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT_NAME};"
        f"AccountKey={ACCOUNT_KEY};"
        f"BlobEndpoint=http://127.0.0.1:{port}/{ACCOUNT_NAME};"
    )
    client = BlobServiceClient.from_connection_string(connection_string)
    return client.get_container_client(container_name)


def _disk_def(container_name, port, with_prefix):
    prefix_line = (
        f"storage_prefix = '{STORAGE_PREFIX}'," if with_prefix else ""
    )
    return f"""disk(
        type = azure_blob_storage,
        storage_account_url = 'http://azurite1:{port}/{ACCOUNT_NAME}',
        container_name = '{container_name}',
        {prefix_line}
        account_name = '{ACCOUNT_NAME}',
        account_key = '{ACCOUNT_KEY}',
        skip_access_check = 0)"""


def _write_parts(node, table, container_name, port, with_prefix):
    azure_query(
        node,
        f"""
        DROP TABLE IF EXISTS {table} SYNC;
        CREATE TABLE {table} (a Int32)
        ENGINE = MergeTree() ORDER BY tuple()
        SETTINGS disk = {_disk_def(container_name, port, with_prefix)},
                 min_bytes_for_wide_part = 0;
        INSERT INTO {table} VALUES (1), (2), (3);
        """,
    )
    # Data must round-trip regardless of prefix.
    assert int(azure_query(node, f"SELECT count() FROM {table}").strip()) == 3


def test_storage_prefix_applied_to_blob_keys(cluster):
    node = cluster.instances[NODE_NAME]
    port = cluster.azurite_port
    container_name = "cont-prefix"

    _write_parts(node, "t_azure_prefix", container_name, port, with_prefix=True)

    names = [b.name for b in _container_client(container_name, port).list_blobs()]
    assert names, "expected at least one blob to be written"
    # With storage_prefix set, every blob key must live under '<prefix>/'.
    assert all(
        name.startswith(STORAGE_PREFIX + "/") for name in names
    ), f"blobs not under prefix '{STORAGE_PREFIX}/': {names}"


def test_no_storage_prefix_leaves_keys_at_root(cluster):
    node = cluster.instances[NODE_NAME]
    port = cluster.azurite_port
    container_name = "cont-noprefix"

    _write_parts(node, "t_azure_noprefix", container_name, port, with_prefix=False)

    names = [b.name for b in _container_client(container_name, port).list_blobs()]
    assert names, "expected at least one blob to be written"
    # Control: without storage_prefix, no blob may be placed under the prefix.
    assert not any(
        name.startswith(STORAGE_PREFIX + "/") for name in names
    ), f"unexpected prefixed blobs: {names}"

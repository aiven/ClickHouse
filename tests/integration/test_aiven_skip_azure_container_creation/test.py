"""
Integration test for patch 028 — gated skip of Azure container creation at table CREATE.

Patch 028 re-instates, behind a new default-off server setting
`aiven_skip_azure_container_creation`, the behavior of asserting that an Azure blob
container already exists when a table engine is created. With the gate ON,
`StorageAzureConfiguration::createObjectStorage` sets
`connection_params.endpoint.container_already_exists = true`, which makes
`AzureBlobStorage::getContainerClient` take the no-network `createForContainer()`
branch — skipping the existence probe (`GetProperties`) and the creation attempt
(`CreateBlobContainer`). With the gate OFF the upstream behavior is preserved: a
`CREATE TABLE` against a missing container auto-creates the container.

Two instances run on the SAME (post-patch) binary and differ ONLY by the server
setting overlay:

  * node_default — no overlay (gate off, upstream behavior).
  * node_skip    — overlay sets `aiven_skip_azure_container_creation = true`.

The differential observable is whether `CREATE TABLE ... ENGINE = AzureBlobStorage(...)`
against a container that does NOT pre-exist auto-creates that container. We verify it
out-of-band with the raw Azure `BlobServiceClient`, which talks to Azurite directly and
does not go through ClickHouse. That contrast — same binary, only the setting differs —
is the evidence of causation on the post-patch binary.
"""

import pytest
from azure.storage.blob import BlobServiceClient

from helpers.cluster import ClickHouseCluster
from test_storage_azure_blob_storage.test import azure_query

ACCOUNT_NAME = "devstoreaccount1"
ACCOUNT_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

# Container names that must NOT pre-exist before each test's CREATE. They are distinct
# per node so the two tests never collide on the shared Azurite instance.
MISSING_DEFAULT = "cont-missing-default"
MISSING_SKIP = "cont-missing-skip"

cluster = ClickHouseCluster(__file__)

node_default = cluster.add_instance(
    "node_default",
    with_azurite=True,
)
node_skip = cluster.add_instance(
    "node_skip",
    main_configs=["configs/skip_azure_container_creation.xml"],
    with_azurite=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _blob_service_client():
    # The raw client connects to Azurite directly on the host-mapped port (NOT through
    # the docker-internal `azurite1` hostname the ClickHouse nodes use). It is the
    # ground-truth observer: it never goes through ClickHouse's create path.
    connection_string = (
        f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT_NAME};"
        f"AccountKey={ACCOUNT_KEY};"
        f"BlobEndpoint=http://127.0.0.1:{cluster.azurite_port}/{ACCOUNT_NAME};"
    )
    return BlobServiceClient.from_connection_string(connection_string)


def _container_exists(name):
    return _blob_service_client().get_container_client(name).exists()


def _delete_container_if_present(name):
    client = _blob_service_client().get_container_client(name)
    if client.exists():
        client.delete_container()


def _create_azure_table(node, table, container):
    # Positional 4-arg form: (connection_string, container, blob_path, format).
    # On CREATE this reaches StorageAzureConfiguration::createObjectStorage with
    # is_readonly = false, i.e. the only path patch 028 gates.
    conn = cluster.env_variables["AZURITE_CONNECTION_STRING"]
    azure_query(
        node,
        f"""
        DROP TABLE IF EXISTS {table} SYNC;
        CREATE TABLE {table} (a Int32)
        ENGINE = AzureBlobStorage('{conn}', '{container}', 'data.parquet', 'Parquet');
        """,
    )


def test_gate_off_autocreates_container(start_cluster):
    # Upstream behavior: with the setting absent (default false), CREATE against a
    # missing container probes Azure and auto-creates the container.
    _delete_container_if_present(MISSING_DEFAULT)
    assert not _container_exists(
        MISSING_DEFAULT
    ), f"precondition: container '{MISSING_DEFAULT}' must not pre-exist"

    _create_azure_table(node_default, "t_default", MISSING_DEFAULT)

    assert _container_exists(
        MISSING_DEFAULT
    ), f"gate OFF: CREATE TABLE should have auto-created container '{MISSING_DEFAULT}'"


def test_gate_on_skips_container_creation(start_cluster):
    # Gated behavior: with aiven_skip_azure_container_creation = true, CREATE against a
    # missing container takes the no-network branch and never creates the container.
    _delete_container_if_present(MISSING_SKIP)
    assert not _container_exists(
        MISSING_SKIP
    ), f"precondition: container '{MISSING_SKIP}' must not pre-exist"

    _create_azure_table(node_skip, "t_skip", MISSING_SKIP)

    assert not _container_exists(
        MISSING_SKIP
    ), f"gate ON: CREATE TABLE must NOT create container '{MISSING_SKIP}'"

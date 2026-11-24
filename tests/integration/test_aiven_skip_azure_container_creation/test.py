"""
`aiven_skip_azure_container_creation`: with the server setting on, creating Azure object
storage from SQL assumes the container exists and makes no request to Azure; with it off,
`CREATE TABLE` checks for the container and creates a missing one, as upstream does.

Both nodes run the same binary and differ only by the setting. Whether a container exists
is read from Azurite directly, not through ClickHouse; whether a statement made a request
is read from its `AzureGetProperties` and `AzureCreateContainer` profile events.
"""

import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_default = cluster.add_instance("node_default", with_azurite=True)
node_skip = cluster.add_instance(
    "node_skip",
    main_configs=["configs/skip_azure_container_creation.xml"],
    with_azurite=True,
)

ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

# Nothing listens on port 1 of the Azurite host, so any request to this endpoint fails.
UNREACHABLE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    f"AccountKey={ACCOUNT_KEY};"
    "BlobEndpoint=http://azurite1:1/devstoreaccount1;"
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def container_exists(name):
    return cluster.blob_service_client.get_container_client(name).exists()


def delete_container(name):
    client = cluster.blob_service_client.get_container_client(name)
    if client.exists():
        client.delete_container()


def create_azure_table(node, table, container, connection_string=None):
    """Creates the table and returns the number of container requests the CREATE made."""
    connection_string = connection_string or cluster.env_variables["AZURITE_CONNECTION_STRING"]
    query_id = str(uuid.uuid4())
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"CREATE TABLE {table} (a Int32) "
        f"ENGINE = AzureBlobStorage('{connection_string}', '{container}', 'data.parquet', 'Parquet')",
        query_id=query_id,
    )
    node.query("SYSTEM FLUSH LOGS query_log")
    return int(
        node.query(
            "SELECT ProfileEvents['AzureGetProperties'] + ProfileEvents['AzureCreateContainer'] "
            f"FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )


def test_setting_off_creates_missing_container(started_cluster):
    container = "cont-missing-default"
    delete_container(container)

    requests = create_azure_table(node_default, "t_default", container)

    assert requests > 0
    assert container_exists(container)


def test_setting_on_does_not_create_missing_container(started_cluster):
    container = "cont-missing-skip"
    delete_container(container)

    requests = create_azure_table(node_skip, "t_skip", container)

    assert requests == 0
    assert not container_exists(container)
    # The missing container is reported on first write, not hidden.
    node_skip.query_and_get_error("INSERT INTO t_skip VALUES (1)")
    assert not container_exists(container)


def test_setting_on_create_makes_no_request(started_cluster):
    requests = create_azure_table(node_skip, "t_unreachable", "cont-unreachable", UNREACHABLE_CONNECTION_STRING)

    assert requests == 0
    assert node_skip.query("EXISTS TABLE t_unreachable") == "1\n"


def test_setting_on_table_function_does_not_create_missing_container(started_cluster):
    container = "cont-missing-function"
    delete_container(container)
    connection_string = cluster.env_variables["AZURITE_CONNECTION_STRING"]

    node_skip.query_and_get_error(
        f"INSERT INTO TABLE FUNCTION azureBlobStorage('{connection_string}', '{container}', "
        f"'data.parquet', 'Parquet', 'auto', 'a Int32') VALUES (1)"
    )

    assert not container_exists(container)

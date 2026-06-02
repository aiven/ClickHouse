import logging
import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import wait_condition
from test_storage_azure_blob_storage.test import azure_query

# This test exercises the Aiven-carried feature "delegate Azure signature to a separate process"
# (per-disk <account_name> + <signature_delegation_url>). A small Azure-SharedKey "signing proxy"
# runs inside the instance container; ClickHouse's AzureDelegatedKeyPolicy POSTs the Azure
# stringToSign to it and uses the returned signature instead of signing locally (the local account
# key handed to the SDK is the literal "ignored"). Azurite (plain HTTP) is the Azure backend.
#
#   * positive -> a disk whose signature_delegation_url points at the proxy's /sign endpoint can
#                 CREATE/INSERT/SELECT, and the proxy records at least one signing request (so we
#                 know delegation was actually used, not bypassed — the local key is "ignored").
#   * negative -> a disk pointed at /sign_wrong, which returns a valid-shaped but wrong signature,
#                 MUST fail with an authentication/signature error from Azurite.

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
NODE = "node"
PROXY_PORT = 8080
ACCOUNT_NAME = "devstoreaccount1"
# Azurite well-known development account key (public constant, not a secret). The proxy uses the
# same key to compute correct signatures; ClickHouse itself is handed "ignored" as the local key.
ACCOUNT_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    NODE,
    with_azurite=True,
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


def _create_disk_table(table, container, sign_path, skip_access_check, max_tries):
    port = cluster.azurite_port
    return f"""
        CREATE TABLE {table} (id Int64, data String)
        ENGINE = MergeTree() ORDER BY id
        SETTINGS disk = disk(
            type = azure_blob_storage,
            endpoint = 'http://azurite1:{port}/{ACCOUNT_NAME}/{container}',
            endpoint_contains_account_name = 'true',
            account_name = '{ACCOUNT_NAME}',
            account_key = '{ACCOUNT_KEY}',
            signature_delegation_url = 'http://127.0.0.1:{PROXY_PORT}/{sign_path}',
            max_tries = {max_tries},
            skip_access_check = {skip_access_check})
    """


def test_signature_delegation_used_for_azure_disk(started_cluster):
    before = _proxy_sign_count()

    azure_query(instance, "DROP TABLE IF EXISTS azure_sig_ok SYNC")
    azure_query(
        instance,
        _create_disk_table(
            "azure_sig_ok", "contsigok", "sign", skip_access_check=0, max_tries=10
        ),
    )
    azure_query(instance, "INSERT INTO azure_sig_ok VALUES (0, 'a'), (1, 'b')")

    assert azure_query(instance, "SELECT count() FROM azure_sig_ok").strip() == "2"
    assert azure_query(instance, "SELECT data FROM azure_sig_ok ORDER BY id").split() == [
        "a",
        "b",
    ]

    after = _proxy_sign_count()
    assert after > before, (
        "expected the signing proxy to receive at least one stringToSign, "
        f"but the counter did not advance ({before} -> {after})"
    )


def test_wrong_delegated_signature_fails(started_cluster):
    instance.query("DROP TABLE IF EXISTS azure_sig_bad SYNC")

    # CREATE on an azure disk already writes table metadata into the container, so the wrong
    # signature surfaces here (and at INSERT otherwise). The proxy returns a valid-shaped but wrong
    # signature, so Azurite rejects the request. skip_access_check + max_tries=1 make it fail fast.
    create = _create_disk_table(
        "azure_sig_bad", "contsigbad", "sign_wrong", skip_access_check=1, max_tries=1
    )
    _stdout, error = instance.query_and_get_answer_with_error(create)
    if not error:
        _stdout, error = instance.query_and_get_answer_with_error(
            "INSERT INTO azure_sig_bad VALUES (0, 'a'), (1, 'b')"
        )

    assert error, "expected the wrong-signature disk to fail, but the writes succeeded"
    lowered = error.lower()
    assert any(
        kw in lowered
        for kw in (
            "authenticationfailed",
            "authentication",
            "signature",
            "403",
            "forbidden",
        )
    ), f"expected an authentication/signature failure, got: {error}"

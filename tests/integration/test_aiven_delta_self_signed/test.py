"""Patch 067 - allow self-signed certificates for local DeltaLake endpoints.

The DeltaLake engine reads via delta-kernel-rs, which builds its own S3 client (Rust object_store)
that enforces default TLS verification. Against a self-signed HTTPS MinIO this fails with
`ObjectStoreError` / "error sending request". The patch passes `allow_invalid_certificates=true` to
the kernel builder, but ONLY when the URL is HTTPS and the endpoint host is exactly `localhost` or
`127.0.0.1` (parsed host, not a substring of the URL) - so local/test MinIO works while AWS and any
other host keep strict verification.

Harness:
  * MinIO is served over HTTPS with a freshly-minted self-signed certificate (minio_certs/), trusted
    by neither the container's system roots nor delta-kernel's default root set.
  * A tiny loopback TCP forwarder (tcp_forward.py) runs inside the node container so the delta-kernel
    client can reach MinIO at `https://127.0.0.1:<port>` (host on the patch allowlist) while bytes
    are relayed verbatim to `minio1:9001` (TLS stays end-to-end).
  * The ClickHouse-side S3 client accepts the self-signed cert (configs/client_ssl.xml,
    verificationMode=none), so that leg is constant pre/post and the only tested variable is the
    delta-kernel TLS behaviour.

Evidence of causation (worktree-flip, integration-tests runbook 7.3):
  * test_delta_read_over_self_signed_https_localhost_succeeds - POST-patch returns the rows;
    PRE-patch the same read throws the kernel TLS error (so the test fails pre-patch on the missing
    relaxation, not on a feature-absence artifact).
  * test_delta_read_over_self_signed_https_nonlocal_host_still_strict - guard: the same self-signed
    table read through the non-local `minio1` host stays strict and is rejected even POST-patch,
    proving the relaxation is anchored on the exact endpoint host (the hardening over the original
    substring match).
"""

import json
import os
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_access_key, minio_secret_key
from helpers.test_tools import wait_condition

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

# Loopback port the delta-kernel client connects to inside the node container; relayed to minio1:9001.
FORWARD_PORT = 9100
TABLE = "delta_self_signed"
NUM_ROWS = 3

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/client_ssl.xml"],
    with_minio=True,
    minio_certs_dir="minio_certs",
    # The test drives S3 over HTTPS directly; the remote database disk would add an unrelated S3 leg.
    with_remote_database_disk=False,
)


def _build_minimal_delta_table(local_dir):
    """Write a minimal, protocol-v1 Delta table (one parquet + one log commit) under local_dir."""
    os.makedirs(os.path.join(local_dir, "_delta_log"), exist_ok=True)

    table = pa.table(
        {
            "id": pa.array(list(range(1, NUM_ROWS + 1)), type=pa.int64()),
            "name": pa.array([f"row{i}" for i in range(1, NUM_ROWS + 1)], type=pa.string()),
        }
    )
    parquet_name = "part-00000.parquet"
    parquet_path = os.path.join(local_dir, parquet_name)
    pq.write_table(table, parquet_path)
    parquet_size = os.path.getsize(parquet_path)

    schema_string = json.dumps(
        {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": True, "metadata": {}},
                {"name": "name", "type": "string", "nullable": True, "metadata": {}},
            ],
        }
    )
    actions = [
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {
            "metaData": {
                "id": str(uuid.uuid4()),
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_string,
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 1700000000000,
            }
        },
        {
            "add": {
                "path": parquet_name,
                "partitionValues": {},
                "size": parquet_size,
                "modificationTime": 1700000000000,
                "dataChange": True,
            }
        },
    ]
    log_path = os.path.join(local_dir, "_delta_log", "00000000000000000000.json")
    with open(log_path, "w") as f:
        f.write("\n".join(json.dumps(a) for a in actions) + "\n")


def _upload_delta_table(local_dir):
    for root, _dirs, files in os.walk(local_dir):
        for name in files:
            full = os.path.join(root, name)
            rel = os.path.relpath(full, local_dir)
            object_name = f"{TABLE}/{rel}"
            cluster.minio_client.fput_object(cluster.minio_bucket, object_name, full)


def _start_forwarder():
    container_id = cluster.get_container_id("node")
    cluster.copy_file_to_container(
        container_id,
        os.path.join(SCRIPT_DIR, "tcp_forward.py"),
        "/tmp/tcp_forward.py",
    )
    cluster.exec_in_container(
        container_id,
        [
            "bash",
            "-c",
            f"python3 /tmp/tcp_forward.py 127.0.0.1 {FORWARD_PORT} "
            f"{cluster.minio_host} {cluster.minio_port} > /tmp/tcp_forward.log 2>&1",
        ],
        detach=True,
        user="root",
    )

    def probe():
        return cluster.exec_in_container(
            container_id,
            [
                "bash",
                "-c",
                f"exec 3<>/dev/tcp/127.0.0.1/{FORWARD_PORT} && echo open",
            ],
            nothrow=True,
        )

    wait_condition(probe, lambda r: "open" in r, max_attempts=30, delay=0.5)


@pytest.fixture(scope="module")
def started_cluster():
    import subprocess

    # Mint the self-signed cert fresh before the containers mount it.
    subprocess.check_call(
        ["bash", os.path.join(SCRIPT_DIR, "minio_certs", "generate_certs.sh")]
    )
    try:
        cluster.start()

        if (
            int(
                node.query(
                    "SELECT count() FROM system.table_engines WHERE name = 'DeltaLake'"
                ).strip()
            )
            == 0
        ):
            pytest.skip("DeltaLake engine is not available in this build")

        local_dir = os.path.join(cluster.instances_dir, "delta_table_local")
        _build_minimal_delta_table(local_dir)
        _upload_delta_table(local_dir)
        _start_forwarder()

        yield cluster
    finally:
        cluster.shutdown()


def _delta_query(endpoint_host, port):
    url = f"https://{endpoint_host}:{port}/{cluster.minio_bucket}/{TABLE}/"
    return (
        f"SELECT count() FROM deltaLake('{url}', '{minio_access_key}', '{minio_secret_key}') "
        f"SETTINGS allow_experimental_delta_kernel_rs = 1"
    )


def test_delta_read_over_self_signed_https_localhost_succeeds(started_cluster):
    # Endpoint host 127.0.0.1 is on the patch allowlist -> delta-kernel relaxes TLS for the
    # self-signed MinIO and the read succeeds. PRE-patch this same query throws the kernel TLS error.
    result = node.query(_delta_query("127.0.0.1", FORWARD_PORT))
    assert result.strip() == str(NUM_ROWS), (
        f"expected {NUM_ROWS} rows from the self-signed HTTPS localhost DeltaLake read, "
        f"got: {result!r}"
    )


def test_delta_read_over_self_signed_https_nonlocal_host_still_strict(started_cluster):
    # Endpoint host minio1 is NOT on the allowlist -> strict TLS is kept and the self-signed cert is
    # rejected even POST-patch. This proves the relaxation is anchored on the exact endpoint host
    # (the hardening over the original substring match), so e.g. `localhost.attacker.com` could not
    # disable verification.
    err = node.query_and_get_error(_delta_query(cluster.minio_host, cluster.minio_port))
    print(f"\n[non-local host strict-TLS rejection] {err.strip()[:400]}")
    assert err, "expected a strict-TLS rejection for the non-local endpoint host, but the read succeeded"

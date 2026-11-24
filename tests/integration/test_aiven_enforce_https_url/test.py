"""
Integration test for the server setting `enforce_https_for_url_storage`.

When enabled, the `URL` table engine, the `url` / `urlCluster` table functions
and HTTP dictionary sources reject `http://` endpoints. Every URL the engine
requests is checked before the request is sent, so the rejection cases need no
reachable HTTP server.

`node_enforced` enables the setting through the server configuration.
`node_default` keeps the default (false) and must accept `http://` exactly as
upstream does.

See docs/aiven/patches/018-enforce-https-url-storage.md.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_enforced = cluster.add_instance(
    "node_enforced",
    main_configs=["configs/enforce_https.xml", "configs/remote_servers.xml"],
)

node_default = cluster.add_instance("node_default")

# Starts without the setting; `test_existing_http_tables_after_enabling` turns it on.
node_toggle = cluster.add_instance("node_toggle", stay_alive=True)

ENFORCE_HTTPS_CONFIG = "<clickhouse><enforce_https_for_url_storage>true</enforce_https_for_url_storage></clickhouse>"

URL_STORAGE_MSG = "URL storage supports only HTTPS protocol"
DICT_MSG = "Only https scheme is supported for HTTPDictionarySource"

HTTP_URL = "http://example.com/x.csv"
LOCAL_HTTP_URL = "http://127.0.0.1:8123/?query=select%201%20format%20CSV"
UNREACHABLE_HTTPS_URL = "https://unreachable.invalid/x.csv"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_url_function_rejects_http(start_cluster):
    error = node_enforced.query_and_get_error(f"SELECT * FROM url('{HTTP_URL}', 'CSV', 'a UInt8')")
    assert URL_STORAGE_MSG in error, error


def test_url_engine_rejects_http(start_cluster):
    error = node_enforced.query_and_get_error(f"CREATE TABLE url_http (a UInt8) ENGINE = URL('{HTTP_URL}', 'CSV')")
    assert URL_STORAGE_MSG in error, error


def test_url_cluster_rejects_http(start_cluster):
    error = node_enforced.query_and_get_error(f"SELECT * FROM urlCluster('test_url_cluster', '{HTTP_URL}', 'CSV', 'a UInt8')")
    assert URL_STORAGE_MSG in error, error


def test_every_requested_url_is_checked(start_cluster):
    error = node_enforced.query_and_get_error(f"SELECT * FROM url('{UNREACHABLE_HTTPS_URL}|{LOCAL_HTTP_URL}', 'CSV', 'a UInt8')")
    assert URL_STORAGE_MSG in error, error


def test_rejected_url_is_named_without_credentials(start_cluster):
    error = node_enforced.query_and_get_error("SELECT * FROM url('http://reader:url_secret@example.com/x.csv', 'CSV', 'a UInt8')")
    message = next((line for line in error.splitlines() if URL_STORAGE_MSG in line), None)
    assert message is not None, error
    # The client echoes the query text after the message; only the server's message is checked.
    message = message.split("(query:")[0]
    assert "example.com/x.csv" in message, message
    assert "url_secret" not in message, message


def test_index_page_wildcard_rejects_http(start_cluster):
    error = node_enforced.query_and_get_error("SELECT * FROM url('http://example.com/dir/*.csv', 'CSV', 'a UInt8') SETTINGS allow_experimental_url_wildcard_from_index_pages = 1")
    assert URL_STORAGE_MSG in error, error


def test_https_is_not_rejected(start_cluster):
    # The host does not resolve, so the query fails, but not because of the scheme.
    error = node_enforced.query_and_get_error(f"SELECT * FROM url('{UNREACHABLE_HTTPS_URL}', 'CSV', 'a UInt8')")
    assert URL_STORAGE_MSG not in error, error


def test_http_dictionary_rejects_http(start_cluster):
    node_enforced.query("DROP DICTIONARY IF EXISTS http_dict")
    node_enforced.query(
        """
        CREATE DICTIONARY http_dict
        (
            id UInt64,
            value String
        )
        PRIMARY KEY id
        SOURCE(HTTP(url 'http://example.com/d.csv' format 'CSV'))
        LAYOUT(FLAT())
        LIFETIME(0)
        """
    )
    # Dictionaries load lazily; force the load so the source is built.
    error = node_enforced.query_and_get_error("SYSTEM RELOAD DICTIONARY http_dict")
    assert DICT_MSG in error, error
    node_enforced.query("DROP DICTIONARY IF EXISTS http_dict")


def test_setting_not_session_overridable(start_cluster):
    # A server setting has no `SET` path.
    error = node_enforced.query_and_get_error("SET enforce_https_for_url_storage = 0")
    assert "UNKNOWN_SETTING" in error, error


def test_existing_http_tables_after_enabling(start_cluster):
    node_toggle.query(f"CREATE TABLE url_engine_http (a UInt8) ENGINE = URL('{LOCAL_HTTP_URL}', 'CSV')")
    node_toggle.query(f"CREATE TABLE url_function_http AS url('{LOCAL_HTTP_URL}', 'CSV', 'a UInt8')")
    assert node_toggle.query("SELECT * FROM url_engine_http").strip() == "1"

    node_toggle.exec_in_container(
        [
            "bash",
            "-c",
            f"echo '{ENFORCE_HTTPS_CONFIG}' > /etc/clickhouse-server/config.d/enforce_https.xml",
        ]
    )
    node_toggle.restart_clickhouse()

    # The server starts and the tables load, but no request is sent over HTTP.
    assert node_toggle.query("SELECT 1").strip() == "1"
    for table in ["url_engine_http", "url_function_http"]:
        error = node_toggle.query_and_get_error(f"SELECT * FROM {table}")
        assert URL_STORAGE_MSG in error, error
        node_toggle.query(f"DROP TABLE {table}")


def test_default_off_allows_http(start_cluster):
    result = node_default.query(f"SELECT * FROM url('{LOCAL_HTTP_URL}', 'CSV', 'a UInt8')")
    assert result.strip() == "1", result

    result = node_default.query(f"SELECT * FROM url('{UNREACHABLE_HTTPS_URL}|{LOCAL_HTTP_URL}', 'CSV', 'a UInt8')")
    assert result.strip() == "1", result

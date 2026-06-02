"""
Aiven patch 018 — integration test for:

  "Enforce HTTPS for URL storage and HTTPDictionarySource (config-gated)"

The patch adds a non-overridable server setting `enforce_https_for_url_storage`
(ServerSetting, Bool, default false). When enabled, the `URL` table engine, the
`url` / `urlCluster` table functions, and HTTP dictionary sources reject any
`http://` (non-HTTPS) endpoint at storage/dictionary CONSTRUCTION time — before
any network I/O. Because the scheme check throws before any socket is opened,
the rejection cases need no reachable HTTP server: any `http://...` URL trips
the throw.

Evidence-of-causation pair (same binary, behavior switched only by config):
  * `node_enforced` has the overlay `<enforce_https_for_url_storage>true</...>`
    and REJECTS `http://` on all three surfaces (URL engine, urlCluster, HTTP
    dictionary).
  * `node_default` has NO overlay (default off) and ACCEPTS `http://` — it reads
    `1` over plain HTTP from its own HTTP port, proving byte-for-byte upstream
    behavior out of the box (no regression).

The third leg, `test_setting_not_session_overridable`, proves the control has no
client-side override path: `enforce_https_for_url_storage` lives in the
`ServerSettings` namespace, so a `SET` of it is rejected as an unknown setting —
a tenant cannot weaken it in a session, profile, or query SETTINGS clause.

See docs/aiven/patches/018-enforce-https-url-storage.md.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Enforcement ON via the server-config overlay. The remote_servers config gives
# urlCluster a cluster name to resolve (the scheme check fires on the initiator
# at StorageURLCluster construction, before any worker dispatch).
node_enforced = cluster.add_instance(
    "node_enforced",
    main_configs=["configs/enforce_https.xml", "configs/remote_servers.xml"],
)

# No overlay => enforce_https_for_url_storage defaults to false => upstream
# behavior (the control half of the evidence pair).
node_default = cluster.add_instance("node_default")

URL_STORAGE_MSG = "URL storage supports only HTTPS protocol"
DICT_MSG = "Only https scheme is supported for HTTPDictionarySource"

HTTP_URL = "http://example.com/x.csv"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_url_engine_rejects_http(start_cluster):
    error = node_enforced.query_and_get_error(
        f"SELECT * FROM url('{HTTP_URL}', 'CSV', 'a UInt8')"
    )
    assert URL_STORAGE_MSG in error, error


def test_url_cluster_rejects_http(start_cluster):
    # Proves the cluster path is closed too: StorageURLCluster has its own
    # constructor that does not route through StorageURL's ctor.
    error = node_enforced.query_and_get_error(
        f"SELECT * FROM urlCluster('test_url_cluster', '{HTTP_URL}', 'CSV', 'a UInt8')"
    )
    assert URL_STORAGE_MSG in error, error


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
    # Dictionaries are lazy by default; force the load so the source is built
    # and the scheme check fires.
    error = node_enforced.query_and_get_error("SYSTEM RELOAD DICTIONARY http_dict")
    assert DICT_MSG in error, error
    node_enforced.query("DROP DICTIONARY IF EXISTS http_dict")


def test_setting_not_session_overridable(start_cluster):
    # enforce_https_for_url_storage is a ServerSetting: there is NO SET path for
    # it, so trying to weaken it in a session is rejected with UNKNOWN_SETTING
    # ("neither a builtin setting nor started with the prefix 'custom_'").
    error = node_enforced.query_and_get_error("SET enforce_https_for_url_storage = 0")
    assert "enforce_https_for_url_storage" in error, error
    assert "neither a builtin setting" in error, error
    assert "UNKNOWN_SETTING" in error, error


def test_default_off_allows_http(start_cluster):
    # With the setting at its default (false), plain HTTP is accepted exactly as
    # upstream. Read '1' over HTTP from the node's own HTTP port to prove the
    # gate is not in the way and the upstream path still works end-to-end.
    result = node_default.query(
        "SELECT * FROM url("
        "'http://127.0.0.1:8123/?query=select%201%20format%20CSV', "
        "'CSV', 'a UInt8')"
    )
    assert result.strip() == "1", result

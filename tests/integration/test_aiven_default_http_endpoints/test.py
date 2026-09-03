import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
disabled = cluster.add_instance(
    "disabled",
    main_configs=["configs/disable_webterminal.xml"],
)
opt_in = cluster.add_instance(
    "opt_in",
    main_configs=["configs/opt_in.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_webterminal_can_be_disabled():
    response = disabled.http_request("webterminal", method="GET")
    assert response.status_code == 403
    assert "enable_webterminal" in response.text


@pytest.mark.parametrize(
    ("path", "marker"),
    [
        ("clickstack", "ClickStack"),
        ("processors-profile", "ClickHouse Processors Profile"),
    ],
)
def test_optional_ui_can_be_enabled_explicitly(path, marker):
    response = opt_in.http_request(path, method="GET")
    assert response.status_code == 200
    assert marker in response.text

"""Tests for BlobEndpoint.resolve_version."""

import pytest

from cfa.dataops.catalog import BlobEndpoint


@pytest.fixture
def blob_endpoint():
    ledger_location = {
        "account": "account_test",
        "container": "container_test",
        "prefix": "_access/test/ledger/",
    }
    return BlobEndpoint(
        account="account_test",
        container="container_test",
        prefix="test/prefix",
        ledger_location=ledger_location,
        ns="test.endpoint",
    )


def test_resolve_version_returns_metadata_without_loading_blob_data(
    mocker, blob_endpoint
):
    mocker.patch("cfa.dataops.catalog.check_ext_env", return_value=True)
    mock_get_versions = mocker.patch.object(
        blob_endpoint,
        "get_versions",
        return_value=["2026-07-08T00-00-00", "2026-07-07T00-00-00"],
    )
    mock_get_version_blobs = mocker.patch.object(blob_endpoint, "_get_version_blobs")
    mock_walk_blobs = mocker.patch("cfa.dataops.catalog.walk_blobs_in_container")
    mock_read_blob_stream = mocker.patch("cfa.dataops.catalog.read_blob_stream")

    metadata = blob_endpoint.resolve_version(version_spec="<=2026-07-09T00-00-00")

    assert metadata == {
        "version": "2026-07-08T00-00-00",
        "version_spec": "<=2026-07-09T00-00-00",
        "selection": "newest",
        "account": "account_test",
        "container": "container_test",
        "blob_prefixes": ["test/prefix/2026-07-08T00-00-00/"],
        "blob_urls": ["az://container_test/test/prefix/2026-07-08T00-00-00/"],
    }
    mock_get_versions.assert_called_once_with()
    mock_get_version_blobs.assert_not_called()
    mock_walk_blobs.assert_not_called()
    mock_read_blob_stream.assert_not_called()


def test_resolve_version_returns_all_matching_versions(mocker, blob_endpoint):
    mocker.patch("cfa.dataops.catalog.check_ext_env", return_value=True)
    mocker.patch.object(
        blob_endpoint,
        "get_versions",
        return_value=[
            "2026-07-10T00-00-00",
            "2026-07-08T00-00-00",
            "2026-07-07T00-00-00",
        ],
    )

    metadata = blob_endpoint.resolve_version(
        version_spec="<=2026-07-09T00-00-00", selection="all"
    )

    assert metadata["version"] == [
        "2026-07-08T00-00-00",
        "2026-07-07T00-00-00",
    ]
    assert metadata["blob_prefixes"] == [
        "test/prefix/2026-07-08T00-00-00/",
        "test/prefix/2026-07-07T00-00-00/",
    ]
    assert metadata["blob_urls"] == [
        "az://container_test/test/prefix/2026-07-08T00-00-00/",
        "az://container_test/test/prefix/2026-07-07T00-00-00/",
    ]


def test_get_version_blobs_reuses_resolved_version_paths(mocker, blob_endpoint):
    mocker.patch("cfa.dataops.catalog.check_ext_env", return_value=True)
    mocker.patch.object(
        blob_endpoint,
        "get_versions",
        return_value=["2026-07-08T00-00-00"],
    )
    mock_walk_blobs = mocker.patch(
        "cfa.dataops.catalog.walk_blobs_in_container",
        return_value=[
            {
                "name": "test/prefix/2026-07-08T00-00-00/data.parquet",
                "creation_time": "2026-07-08T00:00:00",
            }
        ],
    )

    blobs = blob_endpoint._get_version_blobs(
        version_spec="<=2026-07-09T00-00-00", print_version=False
    )

    assert blobs == [
        {
            "name": "test/prefix/2026-07-08T00-00-00/data.parquet",
            "creation_time": "2026-07-08T00:00:00",
        }
    ]
    mock_walk_blobs.assert_called_once_with(
        name_starts_with="test/prefix/2026-07-08T00-00-00/",
        account_name="account_test",
        container_name="container_test",
    )


def test_get_dataframe_can_suppress_version_print(mocker, blob_endpoint):
    mocker.patch("cfa.dataops.catalog.check_ext_env", return_value=True)
    mock_get_version_blobs = mocker.patch.object(
        blob_endpoint,
        "_get_version_blobs",
        return_value=[
            {
                "name": "test/prefix/2026-07-08T00-00-00/data.csv",
                "creation_time": "2026-07-08T00:00:00",
            }
        ],
    )
    mock_read_blobs = mocker.patch.object(
        blob_endpoint,
        "read_blobs",
        return_value=[b"value\n1\n"],
    )

    df = blob_endpoint.get_dataframe(print_version=False)

    assert df["value"].tolist() == [1]
    mock_get_version_blobs.assert_called_once_with(
        version_spec=None,
        selection="newest",
        print_version=False,
    )
    mock_read_blobs.assert_called_once_with(
        version_spec=None,
        selection="newest",
        print_version=False,
    )

from io import BytesIO
from types import SimpleNamespace

import pandas as pd
import polars as pl

from cfa.dataops.catalog import BlobEndpoint, DatasetEndpoint, dict_to_sn
from cfa.dataops.utils import get_dataset_dot_path, get_timestamp


class MockBlob:
    def __init__(self, data: bytes):
        self.data = data

    def content_as_bytes(self):
        return self.data


def test_dict_to_sn(simple_dataset_ns_map):
    defaults = {
        "storage": {"account": "account", "container": "container"},
        "access_ledger": {"path": "some/path/"},
    }
    result = dict_to_sn(simple_dataset_ns_map, defaults)
    print(result.space.example.config)
    assert result.space.example.config["properties"]["name"] == "etl_test"
    assert result.space.example.config["properties"]["type"] == "etl"
    assert not result.space.example.config["properties"]["automate"]
    assert result.space.example.extract.account == "account_test"
    assert result.space.example.extract.container == "container_test"
    assert (
        result.space.example.extract.prefix == "prefix_test/raw/test_dataset"
    )
    assert isinstance(result.space.example.load, BlobEndpoint)
    assert isinstance(result.space.example.extract, BlobEndpoint)
    assert isinstance(result.space.example, DatasetEndpoint)
    assert isinstance(result.space, SimpleNamespace)


def test_datasets_catalog(
    mocker, mock_write_blob_stream, dataset_ns_map, dataset_defaults
):
    datacat = dict_to_sn(dataset_ns_map, dataset_defaults)
    dataset_namespaces = get_dataset_dot_path(dataset_ns_map)
    datacat.__setattr__("__namespace_list__", dataset_namespaces)
    assert isinstance(datacat, SimpleNamespace)
    assert isinstance(datacat.tests, SimpleNamespace)
    assert isinstance(
        datacat.tests.multistage.multistage_test, DatasetEndpoint
    )
    assert isinstance(datacat.tests.reference_test.load, BlobEndpoint)
    # did defaults load
    assert datacat.tests.experiment_test.load.account == "account_test"

    mocker.patch(
        "cfa.dataops.catalog.write_blob_stream",
        mock_write_blob_stream,
    )

    out = datacat.tests.etl_test.extract.write_blob(
        file_buffer=b"test,data\n1,2\n3,4",
        path_after_prefix=f"{get_timestamp()}/path/test_file.csv",
    )
    assert out is None

    def mock_read_blob_stream(
        blob_url: str,
        account_name: str,
        container_name: str,
    ) -> bytes:
        df = pd.DataFrame(
            [
                {"test": 1, "data": 2},
                {"test": 1, "data": 2},
            ]
        )
        return df.to_parquet()

    blobs_sig_mock = [
        {
            "name": "prefix_test/transformed/test_dataset/2025-06-03T17-56-50/data.parquet",
            "container": "container_test",
        }
    ]

    mocker.patch(
        "cfa.dataops.catalog.read_blob_stream",
        mock_read_blob_stream,
    )
    mocker.patch.object(
        datacat.tests.etl_test.load,
        "get_versions",
        return_value=["2025-06-03T17-56-50"],
    )
    mocker.patch.object(
        datacat.tests.etl_test.load,
        "_get_version_blobs",
        return_value=blobs_sig_mock,
    )

    blobs_read = datacat.tests.etl_test.load.read_blobs()
    blobs_df = pd.read_parquet(BytesIO(blobs_read[0]))
    assert isinstance(blobs_df, pd.DataFrame)


def test_datasets_catalog_get_dataframe_parquet(
    mocker, mock_write_blob_stream, dataset_ns_map, dataset_defaults
):
    datacat = dict_to_sn(dataset_ns_map, dataset_defaults)
    dataset_namespaces = get_dataset_dot_path(dataset_ns_map)
    datacat.__setattr__("__namespace_list__", dataset_namespaces)

    mocker.patch(
        "cfa.dataops.catalog.write_blob_stream",
        mock_write_blob_stream,
    )
    mocker.patch.object(
        datacat.tests.etl_test.load,
        "get_versions",
        return_value=["2025-06-03T17-56-50"],
    )

    def mock_read_blob_stream_parquet_df(
        blob_url: str,
        account_name: str,
        container_name: str,
    ) -> bytes:
        df = pd.DataFrame(
            [
                {"test": 1, "data": 2},
                {"test": 1, "data": 2},
            ]
        )
        data = df.to_parquet(index=False)
        return MockBlob(data)

    mocker.patch(
        "cfa.dataops.catalog.read_blob_stream",
        mock_read_blob_stream_parquet_df,
    )
    mocker.patch.object(
        datacat.tests.etl_test.load,
        "_get_version_blobs",
        return_value=[
            {
                "name": "prefix_test/transformed/test_dataset/2025-06-03T17-56-50/data.parquet",
                "container": "container_test",
            }
        ],
    )

    blobs_df = datacat.tests.etl_test.load.get_dataframe(output="pd")
    assert isinstance(blobs_df, pd.DataFrame)
    blobs_df = datacat.tests.etl_test.load.get_dataframe(output="pl")
    assert isinstance(blobs_df, pl.DataFrame)


def test_datasets_catalog_get_dataframe_json(
    mocker, mock_write_blob_stream, dataset_ns_map, dataset_defaults
):
    datacat = dict_to_sn(dataset_ns_map, dataset_defaults)
    dataset_namespaces = get_dataset_dot_path(dataset_ns_map)
    datacat.__setattr__("__namespace_list__", dataset_namespaces)

    mocker.patch(
        "cfa.dataops.catalog.write_blob_stream",
        mock_write_blob_stream,
    )
    mocker.patch.object(
        datacat.tests.etl_test.load,
        "get_versions",
        return_value=["2025-06-03T17-56-50"],
    )

    def mock_read_blob_stream_json_df(
        blob_url: str,
        account_name: str,
        container_name: str,
    ) -> bytes:
        df = pd.DataFrame(
            [
                {"test": 1, "data": 2},
                {"test": 1, "data": 2},
            ]
        )
        data = df.to_json(index=False).encode()
        return MockBlob(data)

    mocker.patch(
        "cfa.dataops.catalog.read_blob_stream",
        mock_read_blob_stream_json_df,
    )
    mocker.patch.object(
        datacat.tests.etl_test.load,
        "_get_version_blobs",
        return_value=[
            {
                "name": "prefix_test/transformed/test_dataset/2025-06-03T17-56-50/data.json",
                "container": "container_test",
            }
        ],
    )

    blobs_df = datacat.tests.etl_test.load.get_dataframe(output="pd")
    assert isinstance(blobs_df, pd.DataFrame)
    blobs_df = datacat.tests.etl_test.load.get_dataframe(output="pl")
    assert isinstance(blobs_df, pl.DataFrame)
    blobs_df = datacat.tests.etl_test.load.get_dataframe(
        output="pl", pl_lazy=True
    )
    assert isinstance(blobs_df, pl.LazyFrame)

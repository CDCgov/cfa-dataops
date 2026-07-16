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
    assert result.space.example.extract.prefix == "prefix_test/raw/test_dataset"
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
    assert isinstance(datacat.tests.multistage.multistage_test, DatasetEndpoint)
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


def test_datasets_catalog_get_dataframe_parquet_pandas_with_metadata(
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
        return_value=["2025-06-03T17-56-50", "2025-05-30T14-50-36"],
    )

    def mock_read_blob_stream_parquet_df(
        blob_url: str,
        account_name: str,
        container_name: str,
    ) -> bytes:
        df = pd.DataFrame(
            [
                {"test": 1, "data": 2},
                {"test": 3, "data": 4},
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
                "name": "prefix_test/transformed/test_dataset/2025-05-30T14-50-36/data.parquet",
                "container": "container_test",
            }
        ],
    )

    blobs_df = datacat.tests.etl_test.load.get_dataframe(
        output="pd",
        version_spec=">=2025-05-01,<2025-07-01",
        selection="oldest",
        with_metadata=True,
    )

    assert isinstance(blobs_df, pd.DataFrame)
    assert blobs_df.attrs == {
        "version": "2025-05-30T14-50-36",
        "blob_url": "az://container_test/prefix_test/transformed/test_dataset/2025-05-30T14-50-36/*.parquet",
        "version_spec": ">=2025-05-01,<2025-07-01",
        "selection": "oldest",
    }


def test_datasets_catalog_get_dataframe_parquet_polars_with_metadata(
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

    def mock_read_blob_stream_parquet_bytes(
        blob_url: str,
        account_name: str,
        container_name: str,
    ) -> bytes:
        return MockBlob(b"parquet-bytes-not-used")

    mocker.patch(
        "cfa.dataops.catalog.read_blob_stream",
        mock_read_blob_stream_parquet_bytes,
    )

    metadata = {}

    class MockConfigMeta:
        def set(self, **kwargs):
            metadata.update(kwargs)

    class MockPolarsFrame:
        def __init__(self):
            self.config_meta = MockConfigMeta()

    mocker.patch(
        "cfa.dataops.catalog.pl.read_parquet",
        return_value=object(),
    )
    mocker.patch(
        "cfa.dataops.catalog.pl.concat",
        return_value=MockPolarsFrame(),
    )

    blobs_df = datacat.tests.etl_test.load.get_dataframe(
        output="pl",
        with_metadata=True,
    )

    assert isinstance(blobs_df, MockPolarsFrame)
    assert metadata == {
        "version": "2025-06-03T17-56-50",
        "blob_url": "az://container_test/prefix_test/transformed/test_dataset/2025-06-03T17-56-50/*.parquet",
        "version_spec": None,
        "selection": "newest",
    }


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
    blobs_df = datacat.tests.etl_test.load.get_dataframe(output="pl")
    assert isinstance(blobs_df, pl.DataFrame)


def test_datasets_catalog_get_dataframe_pl_lazy(
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
    mocker.patch.object(
        datacat.tests.etl_test.load,
        "ledger_entry",
        return_value=None,
    )

    # Avoid real auth wiring during test
    mocker.patch("cfa.dataops.catalog.ManagedIdentityCredential", return_value=object())
    mocker.patch(
        "cfa.dataops.catalog.pl.CredentialProviderAzure",
        side_effect=lambda credential: object(),
    )

    scan_calls = []

    def mock_scan_parquet(path, **kwargs):
        scan_calls.append((path, kwargs))
        return pl.DataFrame([{"test": 1, "data": 2}]).lazy()

    mocker.patch(
        "cfa.dataops.catalog.pl.scan_parquet",
        side_effect=mock_scan_parquet,
    )

    out_pl_lazy = datacat.tests.etl_test.load.get_dataframe(output="pl_lazy")
    out_lazy = datacat.tests.etl_test.load.get_dataframe(output="lazy")

    assert isinstance(out_pl_lazy, pl.LazyFrame)
    assert isinstance(out_lazy, pl.LazyFrame)
    assert len(scan_calls) == 2
    assert (
        scan_calls[0][0]
        == "az://container_test/prefix_test/transformed/test_dataset/2025-06-03T17-56-50/*.parquet"
    )
    assert (
        scan_calls[1][0]
        == "az://container_test/prefix_test/transformed/test_dataset/2025-06-03T17-56-50/*.parquet"
    )


def test_datasets_catalog_get_dataframe_pl_lazy_ndjson_with_metadata(
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
    mocker.patch.object(
        datacat.tests.etl_test.load,
        "_get_version_blobs",
        return_value=[
            {
                "name": "prefix_test/transformed/test_dataset/2025-06-03T17-56-50/data.ndjson",
                "container": "container_test",
            }
        ],
    )

    mocker.patch("cfa.dataops.catalog.ManagedIdentityCredential", return_value=object())
    mocker.patch(
        "cfa.dataops.catalog.pl.CredentialProviderAzure",
        side_effect=lambda credential: object(),
    )

    metadata = {}

    class MockConfigMeta:
        def set(self, **kwargs):
            metadata.update(kwargs)

    class MockLazyFrame:
        def __init__(self):
            self.config_meta = MockConfigMeta()

    mocker.patch(
        "cfa.dataops.catalog.pl.scan_ndjson",
        return_value=MockLazyFrame(),
    )

    out_lazy = datacat.tests.etl_test.load.get_dataframe(
        output="pl_lazy", with_metadata=True
    )

    assert isinstance(out_lazy, MockLazyFrame)
    assert metadata == {
        "version": "2025-06-03T17-56-50",
        "blob_url": "az://container_test/prefix_test/transformed/test_dataset/2025-06-03T17-56-50/*.ndjson",
        "version_spec": None,
        "selection": "newest",
    }

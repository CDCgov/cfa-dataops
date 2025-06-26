from io import BytesIO
from types import SimpleNamespace

import pandas as pd
import polars as pl
import pytest

from cfa.scenarios.dataops.datasets import datasets
from cfa.scenarios.dataops.datasets.catalog import (
    BlobEndpoint,
    dict_to_sn,
    get_data,
)
from cfa.scenarios.dataops.datasets.schemas.covid19vax_trends import (
    extract_schema,
    load_schema,
    raw_synth_data,
    tf_synth_data,
)
from cfa.scenarios.dataops.etl.utils import get_timestamp


def test_dict_to_sn():
    my_dict = {
        "name": "example",
        "version": "1.0",
        "extract": {
            "account": "my_account",
            "container": "my_container",
            "prefix": "my_prefix",
        },
        "load": {
            "account": "load_account1",
            "container": "load_container1",
            "prefix": "load_prefix1",
        },
    }
    result = dict_to_sn(my_dict)
    assert result.name == "example"
    assert result.version == "1.0"
    assert result.extract.account == "my_account"
    assert result.extract.container == "my_container"
    assert result.extract.prefix == "my_prefix"
    assert isinstance(result.load, BlobEndpoint)
    assert isinstance(result.extract, BlobEndpoint)


def test_datasets_catalog(mocker, mock_write_blob_stream):
    assert hasattr(datasets, "covid19vax_trends")
    assert datasets.covid19vax_trends.properties.name == "covid19vax_trends"
    assert isinstance(datasets.covid19vax_trends.extract, BlobEndpoint)
    assert isinstance(datasets.covid19vax_trends.load, BlobEndpoint)

    mocker.patch(
        "cfa.scenarios.dataops.datasets.catalog.write_blob_stream",
        mock_write_blob_stream,
    )

    out = datasets.covid19vax_trends.extract.write_blob(
        file_buffer=b"test,data\n1,2\n3,4",
        path_after_prefix=f"{get_timestamp()}/path/test_file.csv",
    )
    assert out is None

    def mock_read_blob_stream(
        blob_url: str,
        account_name: str,
        container_name: str,
    ) -> bytes:
        return tf_synth_data.to_parquet()

    blobs_sig_mock = [
        {
            "name": "dataops/scenarios/transformed/covid19vax_trends/2025-06-03T17-56-50/data.parquet",
            "container": "cfapredict",
        }
    ]

    mocker.patch(
        "cfa.scenarios.dataops.datasets.catalog.read_blob_stream",
        mock_read_blob_stream,
    )
    mocker.patch.object(
        datasets.covid19vax_trends.load,
        "get_versions",
        return_value=["2025-06-03T17-56-50"],
    )
    mocker.patch.object(
        datasets.covid19vax_trends.load,
        "_get_version_blobs",
        return_value=blobs_sig_mock,
    )

    blobs_read = datasets.covid19vax_trends.load.read_blobs()
    blobs_df = pd.read_parquet(BytesIO(blobs_read[0]))
    assert isinstance(load_schema(blobs_df), pd.DataFrame)


def test_get_data_raw(mocker):
    def mock_read_blob_stream_pd(
        blob_url: str,
        account_name: str,
        container_name: str,
    ) -> bytes:
        data = BytesIO(raw_synth_data.to_csv(index=False).encode())
        return data

    def mock_read_blob_stream_pl(
        blob_url: str,
        account_name: str,
        container_name: str,
    ) -> bytes:
        data = raw_synth_data.to_csv(index=False).encode()
        obj = SimpleNamespace()
        obj.content_as_bytes = lambda: data
        return obj

    blobs_sig_mock = [
        {
            "name": "dataops/scenarios/raw/covid19vax_trends/2025-06-03T17-56-50/data.parquet",
            "container": "cfapredict",
        }
    ]

    mocker.patch(
        "cfa.scenarios.dataops.datasets.catalog.read_blob_stream",
        mock_read_blob_stream_pd,
    )
    mocker.patch.object(
        datasets.covid19vax_trends.extract,
        "get_versions",
        return_value=["2025-06-03T17-56-50"],
    )
    mocker.patch.object(
        datasets.covid19vax_trends.extract,
        "_get_version_blobs",
        return_value=blobs_sig_mock,
    )

    # test name error handling
    with pytest.raises(ValueError):
        get_data(
            "covid19vax_trendy_trends",
            version="latest",
            type="raw",
            output="pandas",
        )

    # test type error handling
    with pytest.raises(ValueError):
        get_data(
            "covid19vax_trends",
            version="latest",
            type="export",
            output="pandas",
        )

    # test output error handling
    with pytest.raises(ValueError):
        get_data(
            "covid19vax_trends", version="latest", type="raw", output="spark"
        )

    pd_data = get_data(
        "covid19vax_trends", version="latest", type="raw", output="pandas"
    )

    assert isinstance(extract_schema(pd_data), pd.DataFrame)

    # test version error handling
    with pytest.raises(ValueError):
        get_data(
            "covid19vax_trends",
            version="2025-04-04T4-49-49",
            type="raw",
            output="pandas",
        )

    mocker.patch(
        "cfa.scenarios.dataops.datasets.catalog.read_blob_stream",
        mock_read_blob_stream_pl,
    )

    pl_data = get_data(
        "covid19vax_trends", version="latest", type="raw", output="polars"
    )

    assert isinstance(pl_data, pl.DataFrame)


def test_get_data_transformed(mocker):
    def mock_read_blob_stream(
        blob_url: str,
        account_name: str,
        container_name: str,
    ) -> bytes:
        data = tf_synth_data.to_parquet()
        obj = SimpleNamespace()
        obj.content_as_bytes = lambda: data
        return obj

    blobs_sig_mock = [
        {
            "name": "dataops/scenarios/transformed/covid19vax_trends/2025-06-03T17-56-50/data.parquet",
            "container": "cfapredict",
        }
    ]

    mocker.patch(
        "cfa.scenarios.dataops.datasets.catalog.read_blob_stream",
        mock_read_blob_stream,
    )
    mocker.patch.object(
        datasets.covid19vax_trends.load,
        "get_versions",
        return_value=["2025-06-03T17-56-50"],
    )
    mocker.patch.object(
        datasets.covid19vax_trends.load,
        "_get_version_blobs",
        return_value=blobs_sig_mock,
    )

    pd_data = get_data(
        "covid19vax_trends",
        version="latest",
        type="transformed",
        output="pandas",
    )

    assert isinstance(load_schema(pd_data), pd.DataFrame)

    # test version error handling
    with pytest.raises(ValueError):
        get_data(
            "covid19vax_trends",
            version="2025-04-04T4-49-49",
            type="transformed",
            output="pandas",
        )

    pl_data = get_data(
        "covid19vax_trends",
        version="latest",
        type="transformed",
        output="polars",
    )

    assert isinstance(pl_data, pl.DataFrame)

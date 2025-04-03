"""building a validated datasource namespace"""

import glob
import os
from io import BytesIO
from types import SimpleNamespace
from typing import Any, List

import pandas as pd
import polars as pl
import tomli
from cfa_azure.helpers import (
    read_blob_stream,
    walk_blobs_in_container,
    write_blob_stream,
)

from .config_validator import validate_dataset_config, verify_no_repeats

_here_dir = os.path.split(os.path.abspath(__file__))[0]
_dataset_config_paths = glob.glob(os.path.join(_here_dir, "*.toml"))

dataset_configs = []
for cp_i in _dataset_config_paths:
    config = tomli.load(cp_i)
    config["_metadata"] = dict(filename=os.path.split(cp_i)[1])
    validate_dataset_config(config)
    dataset_configs.append(config)

verify_no_repeats(dataset_configs)


class BlobEndpoint:
    """The BlobEndpoint class for including in the datasets namespace"""

    def __init__(self, account: str, container: str, prefix: str):
        """Basic functionality to interact with blobs to be included
        via the datasets configs.

        Args:
            account (str): the azure storage account to use
            container (str): the container in the account to use
            prefix (str): the path prefix in the container to use
        """
        self.account = account
        self.container = container
        self.prefix = prefix if prefix[-1] != "/" else prefix[:-1]

    def write_blob(self, file_buffer: bytes, path_after_prefix: str) -> None:
        """For writing file buffers to blob storage

        Args:
            file_buffer (bytes): the file buffer
            path_under_prefix (str): everything beyond the prefix
        """
        path_after_prefix = path_after_prefix.removesuffix("/")
        full_path = f"{self.prefix}/{path_after_prefix}"
        write_blob_stream(
            data=file_buffer,
            blob_url=full_path,
            account_name=self.account,
            container_name=self.container,
        )
        # print(f"file written to: {full_path}")

    def read_blobs(self, version: str = "latest") -> List[bytes]:
        """Read a blob in as bytes so it can be loaded into a dataframe

        Args:
            path_after_prefix (str): The path to the data (e.g.,
            {timestamp}/dataset.csv)
        """
        blobs = self._get_version_blobs(version)
        blob_bytes = [
            read_blob_stream(
                blob_url=i["name"],
                account_name=self.account,
                container_name=self.container,
            )
            for i in blobs
        ]
        return blob_bytes

    def get_versions(self) -> list:
        """For getting all the versions of data blobs, assuming path is
        structured: {prefix}/{version}/{data}

        Returns:
            list: sorted list of data version paths in descending order
            (latest first)
        """
        glob_path = f"{self.prefix}/"
        return sorted(
            [
                i["name"].removeprefix(glob_path).removesuffix("/")
                for i in walk_blobs_in_container(
                    name_starts_with=glob_path,
                    account_name=self.account,
                    container_name=self.container,
                )
            ],
            reverse=True,
        )

    def get_file_ext(self) -> str:
        """returns the file extension for handy routing of read byte types for
        DataFrame reading

        Returns:
            str: the file extension
        """
        return self._get_version_blobs()[0]["name"].split(".")[-1]

    def _get_version_blobs(self, version: str = "latest") -> list:
        if version == "latest":
            version = self.get_versions()[0]
        version = version.removesuffix("/")
        walk_path = f"{self.prefix}/{version}/"
        return sorted(
            list(
                walk_blobs_in_container(
                    name_starts_with=walk_path,
                    account_name=self.account,
                    container_name=self.container,
                )
            ),
            key=lambda x: x["creation_time"],
        )


def dict_to_sn(d: Any) -> SimpleNamespace:
    """Simple recursive namespace construction

    Args:
        d (Any): a dict, list or other

    Returns:
        SimpleNamespace: namespace representation
    """
    x = SimpleNamespace()
    _ = [
        setattr(
            x,
            k,
            BlobEndpoint(
                account=v["account"],
                container=v["container"],
                prefix=v["prefix"],
            )
            if isinstance(v, dict) and k in ["extract", "load"]
            else dict_to_sn(v)
            if isinstance(v, dict)
            else [dict_to_sn(e) for e in v]
            if isinstance(v, list)
            else v,
        )
        for k, v in d.items()
    ]
    return x


datasets = SimpleNamespace()

for i in dataset_configs:
    setattr(datasets, i["properties"]["name"], dict_to_sn(i))


def get_data(
    name: str,
    version="latest",
    type="transformed",
    output="pandas",
) -> pd.DataFrame | pl.DataFrame:
    """
    Gets the data from blob storage based on provided parameters

    Args:
        name (str): name of dataset
        version (str, optional): version of dataset. Defaults to "latest".
        type (str, optional): type of data, either 'raw' or 'transformed'. Defaults to "transformed".
        output (str, optional): dataframe output type, either 'pandas' or 'polars'. Defaults to "pandas".

    Returns:
        pd.DataFrame | pl.DataFrame: pandas or polars dataframe
    """
    # check data exists
    available_datasets = [x for x in datasets.__dict__.keys()]
    if name not in available_datasets:
        print(f"{name} not in available datasets.")
        print("Available datasets:", available_datasets)
        raise ValueError(f"{name} not in available datasets.")

    # validate type, raise error if not raw or transformed
    if type not in ["raw", "transformed"]:
        raise ValueError(f"Type {type} needs to be 'raw' or 'transformed'.")

    # validate output, raise error if not pandas or polars
    if output not in ["pandas", "polars", "pd", "pl"]:
        raise ValueError(
            f"Output {output} needs to be 'pandas', 'polars', 'pd, or 'pl'."
        )

    # continue workflow based on raw or transformed
    if type == "raw":
        # get the BlobEndpoint for the raw data
        blob_endpoint = datasets.__dict__[name].extract
        # check version exists, raise error if not
        if version != "latest":
            v_list = blob_endpoint.get_versions()
            # check if version is in the list of available versions
            if version not in v_list:
                print(f"Version {version} not in available versions.")
                print("Available versions:", v_list)
                raise ValueError(
                    f"Version {version} not in available versions."
                )
        # get blobs and convert to correct df
        blobs = blob_endpoint.read_blobs()
        if output in ["pandas", "pd"]:
            df = pd.concat([pd.read_csv(blob) for blob in blobs])
        else:
            df = pl.concat(
                [pl.read_csv(blob.content_as_bytes()) for blob in blobs],
                how="vertical_relaxed",
            )
        return df
    else:
        blob_endpoint = datasets.__dict__[name].load
        # check version exists, raise error if not
        if version != "latest":
            v_list = blob_endpoint.get_versions()
            if version not in v_list:
                print(f"Version {version} not in available versions.")
                print("Available versions:", v_list)
                raise ValueError(
                    f"Version {version} not in available versions."
                )
        # get blobs and convert to correct df
        blobs = blob_endpoint.read_blobs()
        pq_bytes = blobs[0].content_as_bytes()
        pq_file = BytesIO(pq_bytes)
        if output in ["pandas", "pd"]:
            df = pd.read_parquet(pq_file)
        else:
            df = pl.read_parquet(pq_file)
        return df


def list_datasets() -> list[str]:
    return [x for x in datasets.__dict__.keys()]

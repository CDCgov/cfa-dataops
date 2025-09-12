"""building a validated datasource namespace"""

import glob
import os
from importlib import import_module
from io import BytesIO
from types import SimpleNamespace
from typing import Any, List

import pandas as pd
import polars as pl
import tomli

from cfa.cloudops.blob_helpers import (
    read_blob_stream,
    walk_blobs_in_container,
    write_blob_stream,
)

from . import get_all_catalogs
from .config_validator import validate_dataset_config, verify_no_repeats

all_catalogs = get_all_catalogs()

all_dataset_ns_map = {}
all_reports_ns_map = {}
all_defaults = {}
for cns, cat_name, cat_path in all_catalogs:
    dataset_mod = import_module(f"{cns}.{cat_name}.datasets")
    report_mod = import_module(f"{cns}.{cat_name}.reports")
    all_dataset_ns_map.update(dataset_mod.dataset_ns_map)
    all_reports_ns_map.update(report_mod.report_ns_map)
    with open(
        os.path.join(cat_path, cat_name, "catalog_defaults.toml"), "rb"
    ) as f:
        defaults = tomli.load(f)
    for k in dataset_mod.dataset_ns_map.keys():
        all_defaults.update({k: defaults})


_here_dir = os.path.split(os.path.abspath(__file__))[0]
_dataset_config_paths = glob.glob(
    os.path.join(_here_dir, "**", "*.toml"), recursive=True
)

name_paths = []
dataset_configs = {}
for cp_i in _dataset_config_paths:
    if cp_i.startswith(os.path.join(_here_dir, "datasets")):
        ns_pre = cp_i.split(f"datasets{os.sep}")[-1].split(os.sep)[:-1]
        with open(cp_i, "rb") as f:
            config = tomli.load(f)
            config["_metadata"] = dict(
                filename=os.path.split(cp_i)[1], config_path=cp_i
            )
            validate_dataset_config(config)
            current = dataset_configs
            for part in ns_pre:
                if part not in current:
                    current[part] = {}
                current = current[part]
            current[config["properties"]["name"]] = config
            name_paths.append(
                ".".join(ns_pre + [config["properties"]["name"]])
            )

verify_no_repeats(name_paths)


class DatasetEndpoint:
    """The DatasetEndpoint class for including in the datasets namespace"""

    def __init__(self, config_path: str, defaults: dict):
        """Basic functionality to interact with datasets to be included
        via the datasets configs.

        Args:
            config_path (str): the path to the dataset config
            defaults (dict): the default configuration values
        """
        self.config_path = config_path
        self.defaults = defaults
        with open(config_path, "rb") as f:
            self.config = tomli.load(f)
        for k, v in self.config.items():
            if k in ["load", "extract"] or k.startswith("stage"):
                account = v.get("account", "")
                container = v.get("container", "")
                if account == "":
                    account = self.defaults["storage"]["account"]
                if container == "":
                    container = self.defaults["storage"]["container"]
                self.__setattr__(
                    k,
                    BlobEndpoint(
                        account=account,
                        container=container,
                        prefix=v["prefix"],
                    ),
                )


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

    def read_csv(self, suffix: str) -> pd.DataFrame:
        blob = read_blob_stream(
            blob_url=self.prefix + "/" + suffix,
            account_name=self.account,
            container_name=self.container,
        )
        df = pd.read_csv(blob)
        return df

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


def dict_to_sn(d: Any, defaults: dict = None) -> SimpleNamespace:
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
            DatasetEndpoint(v, defaults)
            if isinstance(v, str) and v.endswith(".toml")
            else dict_to_sn(v, defaults)
            if isinstance(v, dict)
            else [dict_to_sn(e, defaults) for e in v]
            if isinstance(v, list)
            else v,
        )
        for k, v in d.items()
    ]
    return x


dc = []
for k in all_dataset_ns_map.keys():
    dc.append(dict_to_sn({k: all_dataset_ns_map[k]}, all_defaults.get(k, {})))
combined_dict = {key: value for ns in dc for key, value in vars(ns).items()}
datacat = SimpleNamespace(**combined_dict)


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
    try:
        config = eval(f"datacat.{name}")
    except AttributeError as e:
        raise ValueError(
            f"{name} not in available datasets."
            f" Available datasets: {list_datasets()}"
        ) from e

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
        blob_endpoint = config.extract
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
        if config.extract.get_file_ext() == "csv":
            blobs = blob_endpoint.read_blobs()
            if output in ["pandas", "pd"]:
                df = pd.concat([pd.read_csv(blob) for blob in blobs])
            else:
                df = pl.concat(
                    [pl.read_csv(blob.content_as_bytes()) for blob in blobs],
                    how="vertical_relaxed",
                )
            return df
        elif config.extract.get_file_ext() == "json":
            blobs = blob_endpoint.read_blobs()
            if output in ["pandas", "pd"]:
                df = pd.concat(
                    [pd.read_json(blob.content_as_bytes()) for blob in blobs]
                )
            else:
                df = pl.concat(
                    [pl.read_json(blob.content_as_bytes()) for blob in blobs],
                )
            return df
    else:
        blob_endpoint = config.load
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
        pq_bytes = [blob.content_as_bytes() for blob in blobs]
        pq_files = [BytesIO(pq) for pq in pq_bytes]
        if output in ["pandas", "pd"]:
            df = pd.concat([pd.read_parquet(pq_file) for pq_file in pq_files])
        else:
            df = pl.concat(
                [pl.read_parquet(pq_file) for pq_file in pq_files],
                how="vertical_relaxed",
            )
        return df


def list_datasets() -> list[str]:
    """
    Lists all available datasets in the catalog

    Returns:
        list[str]: list of dataset names

    Examples:
        >>> datasets = list_datasets()
        >>> 'scenarios.covid19vax_trends' in datasets
        True

    """
    return name_paths

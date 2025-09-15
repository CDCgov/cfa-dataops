"""building a validated datasource namespace"""

import os
import pkgutil
from configparser import ConfigParser
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

from .config_validator import (
    ConfigValidator,
    PropertiesValidation,
    SourceValidation,
    StorageEndpointValidation,
    ValidationError,
)
from .reporting.catalog import report_dict_to_sn
from .utils import get_dataset_dot_path

_here = os.path.abspath(os.path.dirname(__file__))
_config = ConfigParser()
_config.read(os.path.join(_here, "config.ini"))


def get_all_catalogs() -> list:
    """Get a list of all available dataops catalogs.

    Returns:
        list[tuple]: A list of catalog names and paths.
    """

    catalogs = []
    catalog_nspace = _config.get("DEFAULT", "catalog_namespaces")
    try:
        catalog_pkg = import_module(catalog_nspace)
        for module_finder, modname, ispkg in pkgutil.iter_modules(
            catalog_pkg.__path__
        ):
            if ispkg:
                catalogs.append((catalog_nspace, modname, module_finder.path))
    except ModuleNotFoundError as e:
        raise ModuleNotFoundError(
            f"No catalogs exist in namespace {catalog_nspace}"
        ) from e

    return catalogs


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

dataset_namespaces = get_dataset_dot_path(all_dataset_ns_map)
report_namespaces = get_dataset_dot_path(all_reports_ns_map)


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
                    self.config[k]["account"] = self.defaults["storage"][
                        "account"
                    ]
                if container == "":
                    self.config[k]["container"] = self.defaults["storage"][
                        "container"
                    ]
        self.validate_dataset_config(config_path)
        for k, v in self.config.items():
            if k in ["load", "extract"] or k.startswith("stage"):
                self.__setattr__(
                    k,
                    BlobEndpoint(
                        account=self.config[k]["account"],
                        container=self.config[k]["container"],
                        prefix=v["prefix"],
                    ),
                )

    def validate_dataset_config(self, config_path) -> None:
        """Validate the dataset configuration using ConfigValidator."""
        try:
            config_models = {}
            for c_key, c_value in self.config.items():
                if (
                    c_key.startswith("stage_") or c_key in ["load", "extract"]
                ) and c_value is not None:
                    config_models[c_key] = StorageEndpointValidation(**c_value)
                elif c_key == "properties":
                    config_models[c_key] = PropertiesValidation(**c_value)
                elif c_key == "source":
                    config_models[c_key] = SourceValidation(**c_value)
                else:
                    config_models[c_key] = c_value
            ConfigValidator(**config_models)
        except ValidationError as e:
            raise ValueError(f"Invalid dataset {config_path}: {e}") from e


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

    def get_dataframe(
        self, output="pandas", version="latest"
    ) -> pd.DataFrame | pl.DataFrame:
        """Get the data as a pandas or polars dataframe

        Args:
            output (str, optional): the type of dataframe to return,
            either 'pandas' or 'polars'. Defaults to "pandas".
            version (str, optional): the version of the data to get.
            Defaults to "latest".

        Raises:
            ValueError: if output is not 'pandas' or 'polars'

        Returns:
            pd.DataFrame | pl.DataFrame: the dataframe
        """
        if output not in ["pandas", "polars", "pd", "pl"]:
            raise ValueError(
                f"Output {output} needs to be 'pandas', 'polars', 'pd, or 'pl'."
            )
        blobs = self.read_blobs(version)
        file_ext = self.get_file_ext()
        if file_ext == "csv":
            if output in ["pandas", "pd"]:
                df = pd.concat([pd.read_csv(blob) for blob in blobs])
            else:
                df = pl.concat(
                    [pl.read_csv(blob.content_as_bytes()) for blob in blobs],
                    how="vertical_relaxed",
                )
            return df
        elif file_ext == "json":
            if output in ["pandas", "pd"]:
                df = pd.concat([pd.read_json(blob) for blob in blobs])
            else:
                df = pl.concat(
                    [pl.read_json(blob.content_as_bytes()) for blob in blobs],
                )
            return df
        elif file_ext == "parquet" or file_ext == "parq":
            pq_bytes = [blob.content_as_bytes() for blob in blobs]
            pq_files = [BytesIO(pq) for pq in pq_bytes]
            if output in ["pandas", "pd"]:
                df = pd.concat(
                    [pd.read_parquet(pq_file) for pq_file in pq_files]
                )
            else:
                df = pl.concat(
                    [pl.read_parquet(pq_file) for pq_file in pq_files],
                    how="vertical_relaxed",
                )
            return df


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

rc = []
for k in all_reports_ns_map.keys():
    rc.append(report_dict_to_sn({k: all_reports_ns_map[k]}))
combined_reports_dict = {
    key: value for ns in rc for key, value in vars(ns).items()
}

datacat = SimpleNamespace(**combined_dict)
datacat.__setattr__("__namespace_list__", dataset_namespaces)
reportcat = SimpleNamespace(**combined_reports_dict)
reportcat.__setattr__("__namespace_list__", report_namespaces)

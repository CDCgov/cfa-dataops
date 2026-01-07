"""building a validated datasource namespace"""

import json
import os
import pkgutil
from configparser import ConfigParser
from importlib import import_module
from io import BytesIO
from types import SimpleNamespace
from typing import Any, List, Sequence

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
from .utils import (
    get_dataset_dot_path,
    get_date,
    get_timestamp,
    get_user,
    version_matcher,
)

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
    except ModuleNotFoundError:
        print(f"No catalogs exist in namespace {catalog_nspace}")

    return catalogs


# aggregating all datasets and reports into a single mapping for namespace
# and endpoint construction:
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
    """The DatasetEndpoint class for including in the datacat namespace.
    This ends the namespace branching at a config file and creates all the
    blob endpoints for each 'stag' of the config (e.g., extract, load, stage_01)."""

    def __init__(self, config_path: str, defaults: dict, ns: str):
        """Basic functionality to interact with datasets to be included
        via the datasets configs.

        Args:
            config_path (str): the path to the dataset config
            defaults (dict): the default configuration values
            ns (str): the current namespace path
        """
        self.config_path = config_path
        self.defaults = defaults
        self.__ns_str__ = ns
        with open(config_path, "rb") as f:
            self.config = tomli.load(f)
        for k, v in self.config.items():
            if k in ["load", "extract", "data"] or k.startswith("stage"):
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
        self._ledger_location = {
            "account": self.defaults["storage"]["account"],
            "container": self.defaults["storage"]["container"],
            "prefix": self.defaults["access_ledger"]["path"],
        }
        for k, v in self.config.items():
            if k in ["load", "extract"] or k.startswith("stage"):
                self.__setattr__(
                    k,
                    BlobEndpoint(
                        account=self.config[k]["account"],
                        container=self.config[k]["container"],
                        prefix=v["prefix"],
                        ledger_location=self._ledger_location,
                        ns=f"{self.__ns_str__}.{k}",
                    ),
                )

    def validate_dataset_config(self, config_path) -> None:
        """Validate the dataset configuration using ConfigValidator.
        and each of the pydantic models for each section."""
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

    def __init__(
        self,
        account: str,
        container: str,
        prefix: str,
        ledger_location: dict,
        ns: str,
    ):
        """Basic functionality to interact with blobs to be included
        via the datasets configs.

        Args:
            account (str): the azure storage account to use
            container (str): the container in the account to use
            prefix (str): the path prefix in the container to use
            ledger_location (dict): the location to write access logs to
            ns (str): the current namespace path
        """
        self.account = account
        self.container = container
        self.prefix = prefix if prefix[-1] != "/" else prefix[:-1]
        self.ledger_location = ledger_location
        self.is_ledger = True if ns == "ledger_endpoint" else False
        self.__ns_str__ = ns

    def write_blob(
        self,
        file_buffer: bytes | Sequence[bytes],
        path_after_prefix: str,
        auto_version: bool = False,
        append: bool = False,
    ) -> None:
        """For writing file buffers to blob storage. Remember to include
        the a version to the path (i.e., {version}/{file}) or use
        auto_version arg to include. Also, include the file extension in
        path_after_prefix (e.g. {version}/{filename}.{ext}, where {ext} is
        parquet, csv, or json).

        Args:
            file_buffer (bytes or List[bytes]): the file buffer or list of buffers
            path_under_prefix (str): everything beyond the prefix
            auto_version (bool, optional): whether to automatically version
            append (bool, optional): whether to append to existing file (only for single file writes).
        """
        if auto_version and not append:
            path_after_prefix = (
                f"{get_timestamp()}/{path_after_prefix.lstrip('/')}"
            )
        path_after_prefix = path_after_prefix.lstrip("/")
        full_path = f"{self.prefix}/{path_after_prefix}"
        if isinstance(file_buffer, bytes):
            file_buffer = [file_buffer]
        total_partitions = len(file_buffer)
        for idx, fb_i in enumerate(file_buffer):
            if total_partitions > 1 and not append:
                url_parts = os.path.splitext(full_path)
                auto_full_path = f"{url_parts[0]}_{str(idx).zfill(len(str(total_partitions)))}{url_parts[1]}"
            else:
                auto_full_path = full_path
            write_blob_stream(
                data=fb_i,
                blob_url=auto_full_path,
                account_name=self.account,
                container_name=self.container,
                append_blob=append,
            )
        self.ledger_entry(action="write")
        # print(f"file written to: {full_path}")

    def read_blobs(
        self, version: str = "latest", newest: bool = True
    ) -> List[bytes]:
        """Read a blob in as bytes so it can be loaded into a dataframe

        Args:
            path_after_prefix (str): The path to the data (e.g.,
            {timestamp}/dataset.csv)
            version (str, optional): the version of the data to read.
                Defaults to "latest".
            newest (bool, optional): whether to get the newest matching version. Defaults to True.
        """
        blobs = self._get_version_blobs(version, newest=newest)
        blob_bytes = [
            read_blob_stream(
                blob_url=i["name"],
                account_name=self.account,
                container_name=self.container,
            )
            for i in blobs
        ]
        self.ledger_entry(action="read")
        return blob_bytes

    def read_csv(self, suffix: str) -> pd.DataFrame:
        blob = read_blob_stream(
            blob_url=self.prefix + "/" + suffix,
            account_name=self.account,
            container_name=self.container,
        )
        df = pd.read_csv(blob)
        self.ledger_entry(action="read")
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

    def get_file_ext(self, version: str = "latest") -> str:
        """returns the file extension for handy routing of read byte types for
        DataFrame reading

        Args:
            version (str, optional): the version of the data to get.
        Returns:
            str: the file extension
        """
        return self._get_version_blobs(version=version, print_version=False)[
            0
        ]["name"].split(".")[-1]

    def _get_version_blobs(
        self, version: str = "latest", newest=True, print_version=True
    ) -> list:
        if not self.is_ledger:
            available_versions = self.get_versions()
            version = version_matcher(
                version, available_versions, newest=newest
            )
            if not version:
                raise ValueError(
                    f"Version {version} not found in available versions: {available_versions}"
                )
            if print_version:
                print(f"Using version(s): {version}")
            if isinstance(version, list):
                walk_path = [f"{self.prefix}/{v}/" for v in version]
            else:
                walk_path = f"{self.prefix}/{version}/"
        else:
            walk_path = f"{self.prefix.removesuffix('/')}/"
        if isinstance(walk_path, list):
            all_blobs = []
            for wp in walk_path:
                all_blobs.extend(
                    walk_blobs_in_container(
                        name_starts_with=wp,
                        account_name=self.account,
                        container_name=self.container,
                    )
                )
            return sorted(
                all_blobs,
                key=lambda x: x["creation_time"],
            )
        else:
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

    def download_version_to_local(
        self,
        local_path: str,
        version: str = "latest",
        force: bool = False,
        newest: bool = True,
    ) -> bool:
        """Download a specific version of the data to a local path

        Args:
            local_path (str): the local path to download to
            version (str, optional): the version to download. Defaults to "latest".
            force (bool, optional): whether to force re-download if local.
            newest (bool, optional): whether to get the newest matching version. Defaults to True.
        Returns:
            bool: whether any files were written
        """
        written = False
        blobs = self._get_version_blobs(version, newest=newest)
        for blob in blobs:
            blob_data = read_blob_stream(
                blob_url=blob["name"],
                account_name=self.account,
                container_name=self.container,
            )
            relative_path = blob["name"].removeprefix(f"{self.prefix}/")
            local_file_path = os.path.join(local_path, relative_path)
            local_dir = os.path.dirname(local_file_path)
            os.makedirs(local_dir, exist_ok=True)
            if os.path.exists(local_file_path) and not force:
                continue
            # Handle both raw bytes and objects with content_as_bytes() method
            if isinstance(blob_data, bytes):
                file_bytes = blob_data
            else:
                file_bytes = blob_data.content_as_bytes()
            with open(local_file_path, "wb") as f:
                f.write(file_bytes)
                written = True
        if written:
            self.ledger_entry(action="read")
        return written

    def get_dataframe(
        self,
        output="pandas",
        version="latest",
        pl_lazy: bool = False,
        newest: bool = True,
    ) -> pd.DataFrame | pl.DataFrame:
        """Get the data as a pandas or polars dataframe

        Args:
            output (str, optional): the type of dataframe to return,
                either 'pandas' or 'polars'. Defaults to "pandas".
            version (str, optional): the version of the data to get.
                Defaults to "latest".
            pl_lazy (bool, optional): whether to return a lazy polars dataframe.
                Defaults to False.
            newest (bool, optional): whether to get the newest matching version. Defaults to True.
                False returns the oldest matching version.

        Raises:
            ValueError: if output is not 'pandas' or 'polars'

        Returns:
            pd.DataFrame | pl.DataFrame: the dataframe
        """
        if output not in ["pandas", "polars", "pd", "pl"]:
            raise ValueError(
                f"Output {output} needs to be 'pandas', 'polars', 'pd, or 'pl'."
            )
        blobs = self.read_blobs(version, newest=newest)
        file_ext = self.get_file_ext(version=version)
        blob_bytes = [blob.content_as_bytes() for blob in blobs]
        blob_files = [BytesIO(pq) for pq in blob_bytes]
        if file_ext == "csv":
            if output in ["pandas", "pd"]:
                df = pd.concat([pd.read_csv(blob) for blob in blob_files])
                df.reset_index(inplace=True, drop=True)
            else:
                df = pl.concat(
                    [
                        pl.read_csv(blob, infer_schema_length=None)
                        for blob in blob_files
                    ],
                    how="diagonal",
                )
                if pl_lazy:
                    df = df.lazy()
            return df
        elif file_ext == "json":
            if output in ["pandas", "pd"]:
                df = pd.concat([pd.read_json(blob) for blob in blob_files])
                df.reset_index(inplace=True, drop=True)
            else:
                df = pl.concat(
                    [
                        pl.read_json(blob, infer_schema_length=None)
                        for blob in blob_files
                    ],
                    how="diagonal",
                )
                if pl_lazy:
                    df = df.lazy()
            return df
        elif file_ext == "jsonl":
            if output in ["pandas", "pd"]:
                df = pd.concat(
                    [pd.read_json(blob, lines=True) for blob in blob_files]
                )
                df.reset_index(inplace=True, drop=True)
            else:
                df = pl.concat(
                    [pl.read_ndjson(blob) for blob in blob_files],
                    how="diagonal",
                )
                if pl_lazy:
                    df = df.lazy()
            return df
        elif file_ext == "parquet" or file_ext == "parq":
            if output in ["pandas", "pd"]:
                df = pd.concat(
                    [pd.read_parquet(pq_file) for pq_file in blob_files]
                )
                df.reset_index(inplace=True, drop=True)
            else:
                df = pl.concat(
                    [pl.read_parquet(pq_file) for pq_file in blob_files],
                    how="diagonal",
                )
                if pl_lazy:
                    df = df.lazy()
            return df

    def ledger_entry(self, action: str) -> None:
        """Write an access log entry to the ledger location

        Args:
            action (str): the action taken (e.g., 'read', 'write')
        """
        if self.is_ledger:
            return
        log_entry = {
            "timestamp": get_timestamp(make_standard=True),
            "username": get_user(),
            "dataset": self.__ns_str__,
            "action": action,
        }
        log_data = (json.dumps(log_entry) + "\n").encode("utf-8")
        write_blob_stream(  # TODO: make this a streaming write to a single file (one per day parsed from get_timestamp())
            data=log_data,
            blob_url=f"{self.ledger_location['prefix']}/{get_date()}.jsonl",
            account_name=self.ledger_location["account"],
            container_name=self.ledger_location["container"],
            append_blob=True,
            overwrite=False,
        )

    def save_dataframe(
        self,
        df: pd.DataFrame | pl.DataFrame,
        path_after_prefix: str,
        file_format: str = "parquet",
        auto_version: bool = False,
    ) -> None:
        """Save a dataframe to the blob endpoint

        Args:
            df (pd.DataFrame | pl.DataFrame): the dataframe to save
            path_after_prefix (str): the path after the prefix to save to
            file_format (str, optional): the file format to save as.
            Defaults to "parquet".
            auto_version (bool, optional): whether to automatically version
            the data. Defaults to True.
            partition_cols (List[str], optional): columns to partition by
            when saving. Defaults to None.
        """
        if file_format not in ["parquet", "csv", "json", "jsonl"]:
            raise ValueError(
                f"File format {file_format} not supported. Use 'parquet', 'csv', 'json', or 'jsonl'."
            )
        if file_format in ["json", "jsonl"] and path_after_prefix.endswith(
            ".json"
        ):
            path_after_prefix = path_after_prefix[:-5] + ".jsonl"
            print("Changing file extension to .jsonl for line-delimited JSON.")
        if isinstance(df, pd.DataFrame):
            if file_format == "parquet":
                pq_bytes = df.to_parquet(index=False, compression="snappy")
                self.write_blob(
                    file_buffer=pq_bytes,
                    path_after_prefix=path_after_prefix
                    if path_after_prefix.endswith(".parquet")
                    else path_after_prefix + ".parquet",
                    auto_version=auto_version,
                )
            elif file_format == "csv":
                csv_bytes = df.to_csv(index=False).encode("utf-8")
                self.write_blob(
                    file_buffer=csv_bytes,
                    path_after_prefix=path_after_prefix
                    if path_after_prefix.endswith(".csv")
                    else path_after_prefix + ".csv",
                    auto_version=auto_version,
                )
            elif file_format in ["json", "jsonl"]:
                json_bytes = df.to_json(orient="records", lines=True).encode(
                    "utf-8"
                )
                self.write_blob(
                    file_buffer=json_bytes,
                    path_after_prefix=path_after_prefix
                    if path_after_prefix.endswith(".jsonl")
                    else path_after_prefix + ".jsonl",
                    auto_version=auto_version,
                )
        elif isinstance(df, pl.DataFrame):
            if file_format == "parquet":
                buffer = BytesIO()
                df.write_parquet(buffer, compression="snappy")
                pq_bytes = buffer.getvalue()
                self.write_blob(
                    file_buffer=pq_bytes,
                    path_after_prefix=path_after_prefix
                    if path_after_prefix.endswith(".parquet")
                    else path_after_prefix + ".parquet",
                    auto_version=auto_version,
                )
            elif file_format == "csv":
                csv_bytes = df.write_csv().encode("utf-8")
                self.write_blob(
                    file_buffer=csv_bytes,
                    path_after_prefix=path_after_prefix
                    if path_after_prefix.endswith(".csv")
                    else path_after_prefix + ".csv",
                    auto_version=auto_version,
                )
            elif file_format in ["json", "jsonl"]:
                json_bytes = df.write_ndjson().encode("utf-8")
                self.write_blob(
                    file_buffer=json_bytes,
                    path_after_prefix=path_after_prefix
                    if path_after_prefix.endswith(".jsonl")
                    else path_after_prefix + ".jsonl",
                    auto_version=auto_version,
                )

    def save_file_to_blob(
        self,
        file_path: str,
        path_after_prefix: str,
        auto_version: bool = False,
    ) -> None:
        """Save a local file to the blob endpoint

        Args:
            file_path (str): the local file path to save
            path_after_prefix (str): the path after the prefix to save to
            auto_version (bool, optional): whether to automatically version
            the data. Defaults to False.
        """
        if not os.path.isfile(file_path):
            raise ValueError(f"File {file_path} does not exist.")
        with open(file_path, "rb") as f:
            file_bytes = f.read()
        self.write_blob(
            file_buffer=file_bytes,
            path_after_prefix=path_after_prefix,
            auto_version=auto_version,
        )

    def save_dir_to_blob(
        self,
        dir_path: str,
        path_after_prefix: str,
        auto_version: bool = False,
    ) -> None:
        """Save a local directory to the blob endpoint

        Args:
            dir_path (str): the local directory path to save
            path_after_prefix (str): the path after the prefix to save to
            auto_version (bool, optional): whether to automatically version
            the data. Defaults to False.
        """
        if not os.path.isdir(dir_path):
            raise ValueError(f"Directory {dir_path} does not exist.")
        for root, _, files in os.walk(dir_path):
            for file in files:
                file_path = os.path.join(root, file)
                with open(file_path, "rb") as f:
                    file_buffer = f.read()
                rel_path = f"{'/'.join([i for i in os.path.split(root) if i])}/{'/'.join([i for i in os.path.split(file) if i])}"
                self.write_blob(
                    file_buffer=file_buffer,
                    path_after_prefix=f"{path_after_prefix}/{rel_path}",
                    auto_version=auto_version,
                )


def dict_to_sn(d: Any, defaults: dict = None, ns: str = "") -> SimpleNamespace:
    """Simple recursive namespace construction

    Args:
        d (Any): a dict, list or other
        defaults (dict, optional): the default values to use if not in d.
        ns (str, optional): the current namespace path. Defaults to ''.

    Returns:
        SimpleNamespace: namespace representation
    """
    x = SimpleNamespace()
    ns_prefix = f"{ns}." if ns != "" else ""
    _ = [
        setattr(
            x,
            k,
            DatasetEndpoint(v, defaults, f"{ns_prefix}{k}")
            if isinstance(v, str) and v.endswith(".toml")
            else dict_to_sn(v, defaults, f"{ns_prefix}{k}")
            if isinstance(v, dict)
            else [dict_to_sn(e, defaults, f"{ns_prefix}{k}") for e in v]
            if isinstance(v, list)
            else v,
        )
        for k, v in d.items()
    ]
    if ns != "" and "." not in ns:
        setattr(
            x,
            "_ledger_endpoint",
            BlobEndpoint(
                account=defaults["storage"]["account"],
                container=defaults["storage"]["container"],
                prefix=defaults["access_ledger"]["path"],
                ledger_location={},
                ns="ledger_endpoint",
            ),
        )
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

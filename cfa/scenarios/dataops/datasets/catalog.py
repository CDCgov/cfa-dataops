"""building a validated datasource namespace"""

import glob
import os
from types import SimpleNamespace
from typing import Any, List

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
    with open(cp_i, "rb") as f:
        config = tomli.load(f)
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

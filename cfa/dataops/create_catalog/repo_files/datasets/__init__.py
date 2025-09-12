"""Initialization for repo_templates datasets."""

from os import path

import tomli

from cfa.dataops.utils import get_fs_ns_map, remove_ws_and_nonalpha

from .. import _catalog_ns

_here = path.abspath(path.dirname(__file__))


def get_dataset_name(p: str) -> str:
    """Extract the dataset name from a given file path.

    Args:
        p (str): The file path to the dataset configuration file.

    Returns:
        str: The dataset name extracted from the file.
    """
    with open(p, "rb") as f:
        config = tomli.load(f)
    return remove_ws_and_nonalpha(config["properties"]["name"])


dataset_ns_map = {
    _catalog_ns: get_fs_ns_map(
        base_dir=_here, file_ext="toml", endpoint_func=get_dataset_name
    )
}

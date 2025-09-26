"""Initialization for repo_templates datasets."""

import glob
import os
from os import path
from typing import Optional

import tomli

from .. import _catalog_ns

_here = path.abspath(path.dirname(__file__))


def remove_ws_and_nonalpha(s: str) -> str:
    """Remove whitespace and non-alphanumeric characters from a string.

    Args:
        s (str): The input string.

    Returns:
        str: The cleaned string with whitespace and non-alphanumeric characters removed.

    Example:
        >>> remove_ws_and_nonalpha("Hello World! 123.ipynb")
        'hello_world_123_ipynb'
    """
    s = s.replace(" ", "_").replace(".", "_").lower()
    return "".join(c for c in s if c.isalnum() or c == "_")


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


def get_fs_ns_map(
    base_dir: str, file_ext: str, endpoint_func: Optional[callable] = None
) -> dict:
    """Get a nested dictionary representing the filesystem structure starting from base_dir.

    Args:
        base_dir (str): The base directory to start the search.
        file_ext (str): The file extension to filter files (e.g., 'ipynb').
        endpoint_func (Optional[callable]): A function that takes a file path and returns a string
            to be used as the key in the nested dictionary. If None, the filename without extension
            is used.

    Returns:
        dict: A nested dictionary representing the filesystem structure.
    """
    base_dir = os.path.abspath(base_dir)
    file_ext = file_ext.lstrip(".")
    fs_paths = glob.glob(
        os.path.join(base_dir, "**", f"*.{file_ext}"), recursive=True
    )
    fs_map = {}
    for p_i in fs_paths:
        if p_i.startswith(base_dir):
            ns_list = [
                remove_ws_and_nonalpha(i)
                for i in p_i[len(base_dir) + 1 :].split(os.sep)
            ]
            current = fs_map
            for part in ns_list[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            if endpoint_func is not None:
                ep = endpoint_func(p_i)
                if not isinstance(ep, str):
                    raise ValueError(
                        "endpoint_func must return a string when passed the path of your file."
                    )
                else:
                    current[ep] = p_i
            else:
                current[ns_list[-1][: -(len(file_ext) + 1)]] = p_i

    return fs_map


dataset_ns_map = {
    _catalog_ns: get_fs_ns_map(
        base_dir=_here, file_ext="toml", endpoint_func=get_dataset_name
    )
}

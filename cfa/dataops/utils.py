"""Utility functions for data operations."""

import getpass
import glob
import os
from datetime import datetime
from itertools import islice
from pathlib import Path
from typing import Optional


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


def get_dataset_dot_path(endpoint_map: dict) -> list[str]:
    """Get the dataset config path from the dataset name

    Args:
        endpoint_map (dict): the dataset endpoint map
    Returns:
        list[str]: list of dataset names
    """
    paths = []
    for k, v in endpoint_map.items():
        if isinstance(v, str) and (
            v.endswith(".toml") or v.endswith(".ipynb")
        ):
            paths.append(k)
        elif isinstance(v, dict):
            for i in get_dataset_dot_path(v):
                paths.append(f"{k}.{i}")
    return paths


def get_timestamp(make_standard: bool = False) -> str:
    """For getting standard datetime timestamp format

    Args:
        make_standard (bool): to return a standard timestamp with colons
            in time instead of only path-safe hyphens

    Returns:
        str:datetime string
    """
    if make_standard:
        return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    return datetime.now().strftime("%Y-%m-%dT%H-%M-%S")


def get_date() -> str:
    """Get the current date in YYYY-MM-DD format.

    Returns:
        str: the current date as a string in YYYY-MM-DD format.
    """
    return datetime.now().strftime("%Y-%m-%d")


def get_user() -> str:
    """Get the current system user

    Returns:
        str: the current system user
    """
    try:
        return getpass.getuser()
    except Exception:
        return "unknown_user"


def tree(
    dir_path: Path,
    level: int = -1,
    limit_to_directories: bool = False,
    length_limit: int = 1000,
    show_hidden: bool = False,
):
    """Given a directory Path object print a visual tree structure

    Args:
        dir_path (Path): the root directory path
        level (int): how many levels deep to traverse, -1 for unlimited
        limit_to_directories (bool): whether to only show directories
        length_limit (int): maximum number of lines to print
        show_hidden (bool): whether to show hidden files and directories
    Returns:
        str: visual tree structure
    """
    space = "    "
    branch = "│   "
    tee = "├── "
    last = "└── "
    dir_path = Path(dir_path)  # accept string coercible to Path
    files = 0
    directories = 0

    def inner(dir_path: Path, prefix: str = "", level=-1):
        nonlocal files, directories
        if not level:
            return  # 0, stop iterating
        if limit_to_directories:
            contents = [d for d in dir_path.iterdir() if d.is_dir()]
        else:
            contents = list(dir_path.iterdir())
        pointers = [tee] * (len(contents) - 1) + [last]
        for pointer, path in zip(pointers, contents):
            if not show_hidden and path.name.startswith("."):
                continue
            if path.is_dir():
                yield prefix + pointer + path.name
                directories += 1
                extension = branch if pointer == tee else space
                yield from inner(
                    path, prefix=prefix + extension, level=level - 1
                )
            elif not limit_to_directories:
                yield prefix + pointer + path.name
                files += 1

    lines = []
    lines.append(dir_path.name)
    iterator = inner(dir_path, level=level)
    for line in islice(iterator, length_limit):
        lines.append(line)
    if next(iterator, None):
        lines.append(f"... length_limit, {length_limit}, reached, counted:")
    return (
        "\n".join(lines)
        + f"\n{directories} directories"
        + (f", {files} files" if files else "")
    )

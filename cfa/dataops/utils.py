"""Utility functions for data operations."""

import getpass
import glob
import os
import re
from collections.abc import Callable
from datetime import datetime
from itertools import islice
from pathlib import Path


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
    base_dir: str, file_ext: str, endpoint_func: Callable[[str], str] | None = None
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
    fs_paths = glob.glob(os.path.join(base_dir, "**", f"*.{file_ext}"), recursive=True)
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
        if isinstance(v, str) and (v.endswith(".toml") or v.endswith(".ipynb")):
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
                yield from inner(path, prefix=prefix + extension, level=level - 1)
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


def _parse_version_datetime(version: str) -> datetime | None:
    """Parse a version string as a datetime when possible.

    Supported formats include date-only values and the timestamp format used
    by the catalog, for example ``YYYY-MM-DDT00-00-00``.
    """
    version = version.strip()
    formats = [
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H-%M-%S",
        "%Y-%m-%dT%H-%M-%S.%f",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(version, fmt)
        except ValueError:
            continue
    return None


def version_matcher(
    version: str,
    available_versions: list[str],
    newest: bool | None = True,
    and_sep=",",
) -> str | list[str]:
    """Match a version string to the closest available version.

    Args:
        version (str): The version string to match (e.g., '1.2').
        available_versions (list[str]): A list of available version strings (e.g., ['1.0', '1.1', '1.2', '2.0']).
        newest (Optional[bool]): Whether to return the newest matching version, returns oldest if False (default is True).
        and_sep (str): The separator for multiple version conditions (default is ',').

    Returns:
        list[str]: The matched version string if found, otherwise or empty list if non found.

    Example:
        >>> available_versions = ['1.0', '1.1', '1.2', '2.0']
        >>> version_matcher('>=1.1,<2.0', available_versions)
        '1.2'
        >>> version_matcher('>=1.1,<2.0', available_versions, newest=False)
        '1.1'
        >>> version_matcher('latest', available_versions)
        '2.0'
        >>> version_matcher('~=1', available_versions)
        '1.2'
        >>> version_matcher('>=1.1,<2.0', available_versions, newest=None)
        ['1.2', '1.1']
    """
    if version == "latest":
        return sorted(available_versions, reverse=True)[0]

    # If the available versions look like dates, provide clearer validation
    # before falling back to the generic comparison logic.
    parsed_available_versions = {
        av: _parse_version_datetime(av) for av in available_versions
    }
    if available_versions and all(
        parsed_version is not None
        for parsed_version in parsed_available_versions.values()
    ):
        version_no_ws = re.sub(r"\s", "", version)
        if not re.match(r"^[><=~!]+", version_no_ws):
            parsed_requested_version = _parse_version_datetime(version_no_ws)
            if parsed_requested_version is None:
                raise ValueError(
                    f"Version '{version}' could not be parsed as a date. "
                    "Expected 'YYYY-MM-DD' or 'YYYY-MM-DDTHH-MM-SS'."
                )
            newest_available_version = max(parsed_available_versions.values())
            if parsed_requested_version > newest_available_version:
                newest_version = max(
                    parsed_available_versions,
                    key=parsed_available_versions.get,
                )
                raise ValueError(
                    f"Version '{version}' is newer than the newest available version "
                    f"'{newest_version}'."
                )

    version = re.sub(r"\s", "", version)
    v_ands_parsed = []
    v_ands = version.split(and_sep)
    for v in v_ands:
        cond = re.match(r"[\>\<\=\~\!]+", v)
        if cond and cond.span(0)[0] == 0:
            v_cond = cond.group(0)
        else:
            v_cond = "=="
        v_parts = re.findall(r"\d+", v)
        v_ands_parsed.append((v_cond, ".".join(v_parts)))
    av_parsed = {}
    for avail_version in sorted(available_versions, reverse=True):
        avail_parts = re.findall(r"\d+", avail_version)
        av_parsed[avail_version] = ".".join(avail_parts)
    v_match = []
    logic_vals = []
    for idx, (v_cond, v_p) in enumerate(v_ands_parsed):
        logic_vals.append([])
        for av, av_p in av_parsed.items():
            if v_cond == "==" and v_p == av_p:
                logic_vals[idx].append(True)
            elif v_cond in [">=", "=>"] and v_p <= av_p:
                logic_vals[idx].append(True)
            elif v_cond in ["<=", "=<"] and v_p >= av_p:
                logic_vals[idx].append(True)
            elif v_cond == ">" and v_p < av_p:
                logic_vals[idx].append(True)
            elif v_cond == "<" and v_p > av_p:
                logic_vals[idx].append(True)
            elif v_cond == "!=" and v_p != av_p:
                logic_vals[idx].append(True)
            elif v_cond == "~=" and v_p == av_p[: len(v_p)]:
                logic_vals[idx].append(True)
            else:
                logic_vals[idx].append(False)
            v_match.append(av)
    keep = [all(i) for i in zip(*logic_vals)]
    matched_versions = [i for i, j in zip(v_match, keep) if j]
    if not matched_versions:
        raise ValueError(
            f"Version '{version}' was not found in available versions: {available_versions}"
        )
    if isinstance(newest, bool):
        if newest:
            return max(matched_versions)
        else:
            return min(matched_versions)
    else:
        return matched_versions

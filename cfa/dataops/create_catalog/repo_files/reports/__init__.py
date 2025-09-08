"""Initialization for repo_templates datasets."""

from os import path

from cfa.dataops.utils import get_fs_ns_map, remove_ws_and_nonalpha

from .. import _catalog_ns

_here = path.abspath(path.dirname(__file__))

dataset_ns_map = {
    _catalog_ns: get_fs_ns_map(
        base_dir=_here,
        file_ext="ipynb",
        endpoint_func=lambda p: remove_ws_and_nonalpha(path.split(p)[1]),
    )
}

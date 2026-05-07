"""Simplified importing of most commonly used objects

All data configurations should reside in this directory and be in the toml
format. Use the existing configuration files as a starting point. Validations
will run on all configurations.
"""
from cfa.dataops.utils import check_ext_env

try:
    from importlib.metadata import version

    __version__ = version(__name__)
except ImportError:
    __version__ = "unknown"

if not check_ext_env():
    raise RuntimeError("No EXT access configured.")

from .catalog import datacat, reportcat

__all__ = [__version__, datacat, reportcat]


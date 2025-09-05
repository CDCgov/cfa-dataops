"""Simplified importing of most commonly used objects

All data configurations should reside in this directory and be in the toml
format. Use the existing configuration files as a starting point. Validations
will run on all configurations.
"""

from importlib.metadata import version

from .catalog import datacat, get_data, list_datasets

__all__ = [datacat, get_data, list_datasets]
__version__ = version(__name__)
__catalog_namespace__ = (
    "cfa.catalog"  # change this if you want to use your own namespace
)

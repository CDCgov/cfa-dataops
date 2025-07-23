"""Simplified importing of most commonly used objects

All data configurations should reside in this directory and be in the toml
format. Use the existing configuration files as a starting point. Validations
will run on all configurations.
"""

from .catalog import datacat, get_data, list_datasets

__all__ = [datacat, get_data, list_datasets]

"""Simplified importing of most commonly used objects

All data configurations should reside in this directory and be in the toml
format. Use the existing configuration files as a starting point. Validations
will run on all configurations.
"""

import pkgutil
from importlib import import_module
from importlib.metadata import version

__version__ = version(__name__)
__catalog_namespace__ = (
    "cfa.catalog"  # change this if you want to use your own namespace
)


def get_all_catalogs() -> list:
    """Get a list of all available dataops catalogs.

    Returns:
        list[tuple]: A list of catalog names and paths.
    """

    catalogs = []
    try:
        catalog_pkg = import_module(__catalog_namespace__)
        for module_finder, modname, ispkg in pkgutil.iter_modules(
            catalog_pkg.__path__
        ):
            if ispkg:
                catalogs.append(
                    (__catalog_namespace__, modname, module_finder.path)
                )
    except ModuleNotFoundError as e:
        raise ModuleNotFoundError(
            f"No catalogs exist in namespace {__catalog_namespace__}"
        ) from e

    return catalogs


__all__ = [__catalog_namespace__, __version__, get_all_catalogs]

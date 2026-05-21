"""Simplified importing of most commonly used objects

All data configurations should reside in this directory and be in the toml
format. Use the existing configuration files as a starting point. Validations
will run on all configurations.
"""

try:
    from importlib.metadata import version

    __version__ = version(__name__)
except ImportError:
    __version__ = "unknown"

__all__ = ["__version__", "datacat", "reportcat"]


def __getattr__(name: str):
    if name in {"datacat", "reportcat"}:
        from .catalog import datacat, reportcat

        return datacat if name == "datacat" else reportcat
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

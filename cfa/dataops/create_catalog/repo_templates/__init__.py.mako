from os import path

_here = path.abspath(path.dirname(__file__))
_parent = path.dirname(_here)
_dataops_creation_version = '${dataops_creation_version}'
_catalog_ns = '${unique_name}'

def get_version_separation():
    try:
        from cfa.dataops import __version__ as _installed_dataops_version
        print(
            f"dataops version at catalog creation: {_dataops_creation_version}",
            f"installed dataops version: {_installed_dataops_version}",
            sep="\n"
        )
    except ImportError:
        pass

__all__ = [_here, _parent, _dataops_creation_version, _catalog_ns, get_version_separation]

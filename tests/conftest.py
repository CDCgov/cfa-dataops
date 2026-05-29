import os
import pickle
import sys
from io import BytesIO
from types import ModuleType, SimpleNamespace

import pandas as pd
from pytest import fixture


def _ensure_module(name: str, **attrs):
    module = sys.modules.get(name)
    if module is None:
        module = ModuleType(name)
        sys.modules[name] = module
    for key, value in attrs.items():
        setattr(module, key, value)
    return module


class _ConfigStub(SimpleNamespace):
    def __getattr__(self, name):
        value = SimpleNamespace()
        setattr(self, name, value)
        return value


def _install_test_stubs() -> None:
    _ensure_module(
        "cfa.cloudops.blob_helpers",
        read_blob_stream=lambda *args, **kwargs: b"",
        walk_blobs_in_container=lambda *args, **kwargs: [],
        write_blob_stream=lambda *args, **kwargs: None,
    )
    _ensure_module("cfa.cloudops")
    sys.modules["cfa.cloudops"].blob_helpers = sys.modules["cfa.cloudops.blob_helpers"]

    _ensure_module(
        "azure.identity",
        ManagedIdentityCredential=type("ManagedIdentityCredential", (), {}),
    )
    _ensure_module("azure")
    sys.modules["azure"].identity = sys.modules["azure.identity"]

    _ensure_module(
        "nbformat",
        read=lambda *args, **kwargs: SimpleNamespace(metadata={}, cells=[]),
        write=lambda *args, **kwargs: None,
    )
    _ensure_module(
        "papermill",
        inspect_notebook=lambda *args, **kwargs: {},
        execute_notebook=lambda *args, **kwargs: None,
    )
    _ensure_module(
        "nbconvert",
        HTMLExporter=type(
            "HTMLExporter",
            (),
            {
                "__init__": lambda self, *args, **kwargs: None,
                "from_filename": lambda self, *args, **kwargs: ("", {}),
            },
        ),
    )
    _ensure_module("traitlets")
    _ensure_module("traitlets.config", Config=_ConfigStub)
    sys.modules["traitlets"].config = sys.modules["traitlets.config"]
    _ensure_module(
        "httpx",
        Client=type(
            "Client",
            (),
            {
                "__enter__": lambda self: self,
                "__exit__": lambda self, exc_type, exc, tb: False,
                "get": lambda self, *args, **kwargs: SimpleNamespace(
                    raise_for_status=lambda: None,
                    json=lambda: [],
                ),
            },
        ),
    )


def _install_parquet_fallbacks() -> None:
    try:
        import pyarrow  # noqa: F401

        return
    except Exception:
        pass

    try:
        import fastparquet  # noqa: F401

        return
    except Exception:
        pass

    def _to_parquet(self, path=None, *args, index=True, **kwargs):
        frame = self if index else self.reset_index(drop=True)
        payload = pickle.dumps(frame)
        if path is None:
            return payload
        if hasattr(path, "write"):
            path.write(payload)
            return None
        with open(path, "wb") as f:
            f.write(payload)
        return None

    def _read_parquet(path, *args, **kwargs):
        if hasattr(path, "read"):
            payload = path.read()
        else:
            with open(path, "rb") as f:
                payload = f.read()
        return pickle.loads(payload)

    pd.DataFrame.to_parquet = _to_parquet
    pd.read_parquet = _read_parquet

    try:
        import polars as pl

        def _pl_read_parquet(blob, *args, **kwargs):
            if isinstance(blob, bytes):
                blob = BytesIO(blob)
            return pl.DataFrame(pd.read_parquet(blob))

        pl.read_parquet = _pl_read_parquet
    except Exception:
        pass


_install_test_stubs()
_install_parquet_fallbacks()


_here = os.path.abspath(os.path.dirname(__file__))
test_datasets_dir = os.path.join(_here, "test_datasets")


@fixture(scope="session")
def data_dir(tmpdir_factory):
    """Fixture to create a temporary data directory for tests."""
    return tmpdir_factory.mktemp("data")


@fixture(scope="session")
def mock_write_blob_stream():
    def mock_write_blob_stream(
        data,
        blob_url: str,
        account_name: str,
        container_name: str,
        append_blob: bool = False,
        overwrite: bool = True,
    ) -> None:
        return None

    return mock_write_blob_stream


@fixture(scope="session")
def dataset_ns_map():
    dataset_ns_map = {
        "tests": {
            "multistage": {
                "multistage_test": os.path.join(
                    test_datasets_dir, "multistage_test.toml"
                )
            },
            "etl_test": os.path.join(test_datasets_dir, "etl_test.toml"),
            "experiment_test": os.path.join(test_datasets_dir, "experiment_test.toml"),
            "reference_test": os.path.join(test_datasets_dir, "reference_test.toml"),
        }
    }
    return dataset_ns_map


@fixture(scope="session")
def dataset_defaults():
    return {
        "storage": {"account": "account_test", "container": "container_test"},
        "access_ledger": {"path": "_access/test/ledger/"},
    }


@fixture(scope="session")
def simple_dataset_ns_map():
    dataset_ns_map = {
        "space": {
            "example": os.path.join(test_datasets_dir, "etl_test.toml"),
        }
    }
    return dataset_ns_map


@fixture(scope="session")
def catalog_parent(tmpdir_factory):
    """Fixture to create a temporary data directory for tests."""
    return tmpdir_factory.mktemp("cat_parent")

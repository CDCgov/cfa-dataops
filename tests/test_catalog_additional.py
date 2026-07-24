from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from cfa.dataops.catalog import (
    BlobEndpoint,
    DatasetEndpoint,
    VersionMetadata,
    _attach_schema_mock_functions,
    dict_to_sn,
    get_all_catalogs,
)


class _Finder:
    def __init__(self, path: str):
        self.path = path


def _make_blob_endpoint(ns: str = "tests.example.load") -> BlobEndpoint:
    return BlobEndpoint(
        account="acct",
        container="container",
        prefix="base/prefix/",
        ledger_location={"account": "lacct", "container": "lcont", "prefix": "_access"},
        ns=ns,
    )


def test_get_all_catalogs_returns_only_packages(mocker):
    fake_pkg = SimpleNamespace(__path__=["/tmp/catalogs"])
    mocker.patch("cfa.dataops.catalog._config.get", return_value="my.catalogs")
    mocker.patch("cfa.dataops.catalog.import_module", return_value=fake_pkg)
    mocker.patch(
        "cfa.dataops.catalog.pkgutil.iter_modules",
        return_value=[
            (_Finder("/tmp/catalogs"), "alpha", True),
            (_Finder("/tmp/catalogs"), "beta", False),
            (_Finder("/tmp/catalogs"), "gamma", True),
        ],
    )

    catalogs = get_all_catalogs()

    assert catalogs == [
        ("my.catalogs", "alpha", "/tmp/catalogs"),
        ("my.catalogs", "gamma", "/tmp/catalogs"),
    ]


def test_get_all_catalogs_missing_namespace_logs_warning(mocker, caplog):
    mocker.patch("cfa.dataops.catalog._config.get", return_value="missing.catalogs")
    err = ModuleNotFoundError("not found")
    err.name = "missing.catalogs"
    mocker.patch("cfa.dataops.catalog.import_module", side_effect=err)

    with caplog.at_level("WARNING"):
        catalogs = get_all_catalogs()

    assert catalogs == []
    assert "No catalogs exist in namespace missing.catalogs" in caplog.text


def test_get_all_catalogs_reraises_non_namespace_import_error(mocker):
    mocker.patch("cfa.dataops.catalog._config.get", return_value="expected.catalogs")
    err = ModuleNotFoundError("inner import failure")
    err.name = "different.module"
    mocker.patch("cfa.dataops.catalog.import_module", side_effect=err)

    with pytest.raises(ModuleNotFoundError):
        get_all_catalogs()


def test_blob_endpoint_strips_trailing_slash_and_ledger_flag():
    normal = _make_blob_endpoint(ns="tests.example.load")
    ledger = _make_blob_endpoint(ns="ledger_endpoint")

    assert normal.prefix == "base/prefix"
    assert normal.is_ledger is False
    assert ledger.is_ledger is True


def test_write_blob_splits_partitions_with_suffix(mocker):
    endpoint = _make_blob_endpoint()
    write_mock = mocker.patch("cfa.dataops.catalog.write_blob_stream")

    endpoint.write_blob(
        file_buffer=[b"one", b"two"],
        path_after_prefix="v1/data.parquet",
        append=False,
    )

    assert write_mock.call_count == 2
    assert write_mock.call_args_list[0].kwargs["blob_url"].endswith("data_0.parquet")
    assert write_mock.call_args_list[1].kwargs["blob_url"].endswith("data_1.parquet")


def test_get_versions_requires_ext_env(mocker):
    endpoint = _make_blob_endpoint()
    mocker.patch("cfa.dataops.catalog.check_ext_env", return_value=False)

    with pytest.raises(RuntimeError, match="No EXT access configured"):
        endpoint.get_versions()


def test_get_versions_returns_desc_sorted_names(mocker):
    endpoint = _make_blob_endpoint()
    mocker.patch("cfa.dataops.catalog.check_ext_env", return_value=True)
    mocker.patch(
        "cfa.dataops.catalog.walk_blobs_in_container",
        return_value=[
            {"name": "base/prefix/2025-01-01T00-00-00/"},
            {"name": "base/prefix/2026-01-01T00-00-00/"},
        ],
    )

    versions = endpoint.get_versions()

    assert versions == ["2026-01-01T00-00-00", "2025-01-01T00-00-00"]


def test_get_version_blobs_for_ledger_uses_prefix_without_version(mocker):
    endpoint = _make_blob_endpoint(ns="ledger_endpoint")
    mocker.patch("cfa.dataops.catalog.check_ext_env", return_value=True)
    walk_mock = mocker.patch(
        "cfa.dataops.catalog.walk_blobs_in_container",
        return_value=[
            {"name": "_access/2026-01-01.jsonl", "creation_time": 2},
            {"name": "_access/2025-12-31.jsonl", "creation_time": 1},
        ],
    )

    blobs, version = endpoint._get_version_blobs(print_version=False)

    assert version is None
    assert [b["creation_time"] for b in blobs] == [1, 2]
    assert walk_mock.call_args.kwargs["name_starts_with"] == "base/prefix/"


def test_get_dataframe_raises_for_invalid_output(mocker):
    endpoint = _make_blob_endpoint()
    mocker.patch("cfa.dataops.catalog.check_ext_env", return_value=True)

    with pytest.raises(ValueError, match="needs to be 'pandas'"):
        endpoint.get_dataframe(output="arrow")


def test_ledger_entry_noop_for_ledger_endpoint(mocker):
    endpoint = _make_blob_endpoint(ns="ledger_endpoint")
    write_mock = mocker.patch("cfa.dataops.catalog.write_blob_stream")

    endpoint.ledger_entry(action="read")

    write_mock.assert_not_called()


def test_resolve_version_returns_empty_metadata_when_no_match(mocker):
    endpoint = _make_blob_endpoint()
    mocker.patch.object(endpoint, "_get_version_blobs", side_effect=ValueError("no match"))

    out = endpoint.resolve_version(version_spec=">2030", selection="newest")

    assert out == VersionMetadata(
        version=None,
        blob_url=None,
        version_spec=">2030",
        selection="newest",
    )


def test_attach_schema_mock_functions_attaches_extract_and_load(dataset_defaults, tmp_path, mocker):
    config_path = tmp_path / "dataset.toml"
    config_path.write_text(
        """
[properties]
name = "dataset"
type = "etl"
automate = false

[extract]
prefix = "raw/data"

[load]
prefix = "final/data"
""".strip()
    )

    ns = dict_to_sn({"space": {"example": str(config_path)}}, dataset_defaults)
    dataset_endpoint: DatasetEndpoint = ns.space.example

    schema_mod = ModuleType("mock.schema")

    def extract_mock_data():
        return "extract"

    def load_mock_data():
        return "load"

    schema_mod.extract_mock_data = extract_mock_data
    schema_mod.load_mock_data = load_mock_data

    expected_path = "mockns.space.datasets.schemas.example"

    def _fake_import(path: str):
        if path == expected_path:
            return schema_mod
        raise ModuleNotFoundError(path)

    mocker.patch("cfa.dataops.catalog.import_module", side_effect=_fake_import)

    _attach_schema_mock_functions(ns, [("mockns", "space", str(Path("/tmp")))])

    assert dataset_endpoint.extract.mock_data() == "extract"
    assert dataset_endpoint.load.mock_data() == "load"

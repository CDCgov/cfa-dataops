import re
from datetime import datetime
from pathlib import Path

import pytest

from cfa.dataops import utils


def test_remove_ws_and_nonalpha_normalizes_text():
    assert (
        utils.remove_ws_and_nonalpha("Hello World! 123.ipynb")
        == "hello_world_123_ipynb"
    )


def test_get_fs_ns_map_with_endpoint_func(tmp_path):
    datasets = tmp_path / "datasets"
    datasets.mkdir()
    file_path = datasets / "etl_test.toml"
    file_path.write_text("[properties]\nname='etl_test'\n")

    fs_map = utils.get_fs_ns_map(
        str(datasets),
        "toml",
        endpoint_func=lambda p: f"endpoint_{Path(p).stem}",
    )

    assert fs_map["endpoint_etl_test"] == str(file_path)


def test_get_fs_ns_map_endpoint_func_must_return_string(tmp_path):
    datasets = tmp_path / "datasets"
    datasets.mkdir()
    file_path = datasets / "etl_test.toml"
    file_path.write_text("[properties]\nname='etl_test'\n")

    with pytest.raises(ValueError, match="endpoint_func must return a string"):
        utils.get_fs_ns_map(str(datasets), "toml", endpoint_func=lambda _: 123)


def test_get_timestamp_formats(mocker):
    fixed = datetime(2026, 7, 24, 10, 11, 12)
    dt_cls = mocker.patch("cfa.dataops.utils.datetime")
    dt_cls.now.return_value = fixed

    path_safe = utils.get_timestamp()
    standard = utils.get_timestamp(make_standard=True)

    assert path_safe == "2026-07-24T10-11-12"
    assert standard == "2026-07-24T10:11:12"


def test_get_date_format(mocker):
    fixed = datetime(2026, 7, 24, 10, 11, 12)
    dt_cls = mocker.patch("cfa.dataops.utils.datetime")
    dt_cls.now.return_value = fixed

    assert utils.get_date() == "2026-07-24"


def test_get_user_success(mocker):
    mocker.patch("cfa.dataops.utils.getpass.getuser", return_value="analyst")
    assert utils.get_user() == "analyst"


def test_get_user_fallback_on_exception(mocker):
    mocker.patch("cfa.dataops.utils.getpass.getuser", side_effect=RuntimeError("boom"))
    assert utils.get_user() == "unknown_user"


def test_normalize_replaces_time_delimiters():
    assert utils.normalize("2026-07-24T10-11-12") == "2026.07.24.10.11.12"


def test_get_timestamp_matches_expected_pattern():
    value = utils.get_timestamp()
    assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}$", value)

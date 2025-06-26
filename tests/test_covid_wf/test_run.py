import json
import tempfile

import pytest

from cfa.scenarios.dataops.workflows.covid.run import (
    read_and_validate_config,
)


def test_read_and_validate_config_valid():
    # Create a valid config file
    config_data = {
        "start": "2022-02-12",
        "doses": 3,
        "state_sample": ["AK"],
        "age_groups": [
            17,
            49,
        ],
        "tslie_ranges": [69, 139],
        "variant_types": ["Omicron"],
        "data_folder_path": "covid",
        "output_folder_path": "covid",
    }
    with tempfile.NamedTemporaryFile(
        "w+", suffix=".json", delete=False
    ) as tmp:
        json.dump(config_data, tmp)
        tmp_path = tmp.name

    config = read_and_validate_config(tmp_path)
    assert isinstance(config, tuple)
    assert config[0]["start"] == "2022-02-12"
    assert config[0]["doses"] == 3


def test_read_and_validate_config_invalid(tmp_path):
    # Write invalid json
    bad_json = "start: value1\ndoses: 2"
    file_path = tmp_path / "bad.json"
    file_path.write_text(bad_json)
    with pytest.raises(Exception):
        read_and_validate_config(str(file_path))


def test_read_and_validate_config_missing_file():
    with pytest.raises(FileNotFoundError):
        read_and_validate_config("nonexistent_config.json")

"""The dataset config validator."""

from collections import Counter
from typing import List


def validate_dataset_config(config: dict) -> None:
    """Dataset config validator

    Args:
        config (dict): A dictionary created once loading the toml config file.

    Raises:
        KeyError: when toml config is missing keys
        AssertionError: when the property "name" is not formatted in such a way
            that it can be used as an object property/attribute
        AttributeError: if there is no property name
    """
    match [*config.keys()]:
        case ["properties", "source", "extract", "load", "_metadata"]:
            # _metadata is added by config path loading to pass useful
            # debugging information
            pass
        case _:
            print([*config.keys()])
            raise KeyError(
                f'The config {config["_metadata"]["filename"]} is missing '
                'keys. It should have exactly 4 keys: "properties", "source", '
                '"extract", and "load"'
            )
    for blob_field in ["extract", "load"]:
        match [*config[f"{blob_field}"].keys()]:
            case ["account", "container", "path"]:
                pass
            case _:
                raise KeyError(
                    f'"{blob_field}" key in{config["_metadata"]["filename"]} is missing '
                    'keys. It should have exactly 3 keys: "account", "container", '
                    'and "path"'
                )
    try:
        assert config["properties"].get("name").replace("_", "").isalnum()
        assert config["properties"].get("name").islower()
    except AssertionError as exc:
        raise AssertionError(
            f'The property name in {config["_metadata"]["filename"]} '
            'must be lowercase alphanumeric with only underscores "_" '
            'used to indicate whitespace.'
        ) from exc
    except AttributeError as exc:
        raise AttributeError(
            f'Config {config["_metadata"]["filename"]} lacks a properties '
            'name.'
        ) from exc


def verify_no_repeats(configs: List[dict]) -> None:
    """To make sure no config names repeat.

    Args:
        configs (List[dict]): All the configs.

    Raises:
        AttributeError: When the names repeat.
    """
    name_counts = Counter([i["properties"]["name"] for i in configs])
    repeats = [k for k, v in name_counts.items() if v > 1]
    if repeats:
        names_in_files = []
        for n_i in set(repeats):
            names_in_files += [
                f'  - {n_i} in {config["_metadata"]["filename"]}'
                for config in configs
                if config["properties"]["name"] == n_i
            ]
        error_file_strings = ("\n").join(names_in_files)
        raise AttributeError(
            "More than one config shares a the same name property: "
            f"{error_file_strings}"
        )

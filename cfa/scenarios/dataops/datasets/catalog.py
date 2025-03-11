"""building a validated datasource namespace"""

import glob
import os
from types import SimpleNamespace
from typing import Any

import tomli

from .config_validator import validate_dataset_config, verify_no_repeats

_here_dir = os.path.split(os.path.abspath(__file__))[0]
_dataset_config_paths = glob.glob(os.path.join(_here_dir, "*.toml"))

dataset_configs = []
for cp_i in _dataset_config_paths:
    with open(cp_i, "rb") as f:
        config = tomli.load(f)
        config["_metadata"] = dict(filename=os.path.split(cp_i)[1])
        validate_dataset_config(config)
        dataset_configs.append(config)

verify_no_repeats(dataset_configs)


def dict_to_sn(d: Any) -> SimpleNamespace:
    """Simple recursive namespace construction

    Args:
        d (Any): a dict, list or other

    Returns:
        SimpleNamespace: namespace representation
    """
    x = SimpleNamespace()
    _ = [
        setattr(
            x,
            k,
            dict_to_sn(v)
            if isinstance(v, dict)
            else [dict_to_sn(e) for e in v]
            if isinstance(v, list)
            else v,
        )
        for k, v in d.items()
    ]
    return x


datasets = SimpleNamespace()

for i in dataset_configs:
    setattr(datasets, i["properties"]["name"], dict_to_sn(i))

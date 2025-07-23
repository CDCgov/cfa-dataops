"""ETL general utils and helpers"""

import os
from datetime import datetime

from mako.lookup import TemplateLookup

_here_dir = os.path.split(os.path.abspath(__file__))[0]
template_dirs = [os.path.join(_here_dir, "transform_templates")]
transform_template_lookup = TemplateLookup(
    directories=template_dirs, cache_enabled=False
)


def get_timestamp() -> str:
    """For getting standard datetime timestamp format

    Returns:
        str:datetime string
    """
    return datetime.now().strftime("%Y-%m-%dT%H-%M-%S")


def get_today_date() -> str:
    """For getting standard date format

    Returns:
        str:date string
    """
    return datetime.now().strftime("%Y-%m-%d")

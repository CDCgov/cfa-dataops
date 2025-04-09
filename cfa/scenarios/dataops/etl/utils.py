"""ETL general utils and helpers"""

import os
from datetime import datetime

from mako.lookup import TemplateLookup

_here_dir = os.path.split(os.path.abspath(__file__))[0]
transform_template_lookup = TemplateLookup(
    directories=[os.path.join(_here_dir, "transform_templates")]
)


def get_timestamp() -> str:
    """For getting standard datetime timestamp format

    Returns:
        str:datetime string
    """
    return datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

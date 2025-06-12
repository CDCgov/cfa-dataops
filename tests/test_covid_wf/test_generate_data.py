import os
import tempfile
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
import json

from cfa.scenarios.dataops.workflows.covid.generate_data import generate_vaccination_data

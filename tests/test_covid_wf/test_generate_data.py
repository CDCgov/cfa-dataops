import os
import tempfile
from unittest.mock import patch

import pandas as pd

from cfa.dataops.datasets.scenarios.schemas.covid19vax_trends import (
    tf_synth_data,
)
from cfa.dataops.datasets.scenarios.schemas.hospitalization import (
    tf_synth_data as tf_synth_hosp_data,
)
from cfa.dataops.workflows.covid.generate_data import (
    generate_hospitalization_data,
    generate_vaccination_data,
)


@patch("cfa.dataops.workflows.covid.generate_data.get_data")
def test_generate_vaccination_data_file(mock_get_data):
    # Mock get_data to return a DataFrame with the expected columns for the "raw" CDC data
    df = tf_synth_data.copy()
    mock_get_data.return_value = df

    with tempfile.TemporaryDirectory() as tmpdir:
        df = generate_vaccination_data(tmpdir, blob=False)
        # Check output file exists
        out_file = os.path.join(tmpdir, "vaccination_data.csv")
        assert os.path.exists(out_file)
        # Check DataFrame structure
        assert isinstance(df, pd.DataFrame)
        assert set(
            ["date", "age", "census", "total", "percentage", "dose"]
        ).issubset(df.columns)


@patch("cfa.dataops.workflows.covid.generate_data.get_data")
def test_generate_hospitalization_data_file(mock_get_data):
    # Mock transformed hospitalization data
    mock_hosp = tf_synth_hosp_data.copy()
    # Mock region_id data
    mock_region = pd.DataFrame(
        {
            "stusps": ["CA", "TX", "NY", "FL", "IL"],
            "stname": [
                "California",
                "Texas",
                "New York",
                "Florida",
                "Illinois",
            ],
        }
    )

    # get_data is called twice: first for hospitalization, then for region_id
    mock_get_data.side_effect = [mock_hosp, mock_region]

    with tempfile.TemporaryDirectory() as tmpdir:
        df = generate_hospitalization_data(tmpdir, blob=False)
        # Check output file for California exists
        out_file = os.path.join(tmpdir, "weekly_hospital_california.csv")
        assert os.path.exists(out_file)
        # Check DataFrame structure
        assert isinstance(df, pd.DataFrame)
        assert set(["date", "state", "total"]).issubset(df.columns)
        assert not df.empty

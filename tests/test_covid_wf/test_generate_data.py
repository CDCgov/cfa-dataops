import os
import tempfile
from unittest.mock import MagicMock, patch

import pandas as pd

from cfa.scenarios.dataops.datasets.schemas.covid19vax_trends import (
    tf_synth_data,
)
from cfa.scenarios.dataops.workflows.covid.generate_data import (
    generate_hospitalization_data,
    generate_vaccination_data,
)


@patch("cfa.scenarios.dataops.workflows.covid.generate_data.get_data")
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


@patch("cfa.scenarios.dataops.workflows.covid.generate_data.get_data")
@patch("cfa.scenarios.dataops.workflows.covid.generate_data.requests.get")
def test_generate_hospitalization_data_file(mock_requests_get, mock_get_data):
    # Mock region_id DataFrame
    mock_get_data.return_value = pd.DataFrame(
        {"stusps": ["US", "CA"], "stname": ["United States", "California"]}
    )

    # Mock CDC API response
    fake_api_response = [
        {
            "week_end_date": "2024-06-01T00:00:00.000",
            "jurisdiction": "USA",
            "total_admissions_all_covid_confirmed": 100,
        },
        {
            "week_end_date": "2024-06-01T00:00:00.000",
            "jurisdiction": "CA",
            "total_admissions_all_covid_confirmed": 50,
        },
    ]
    mock_requests_get.return_value = MagicMock(
        text=pd.io.json.dumps(fake_api_response)
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        df = generate_hospitalization_data(tmpdir, blob=False)
        # Check output file for California exists
        out_file = os.path.join(tmpdir, "weekly_hospital_california.csv")
        assert os.path.exists(out_file)
        # Check DataFrame structure
        assert isinstance(df, pd.DataFrame)
        assert set(["date", "state", "total"]).issubset(df.columns)
        assert not df.empty

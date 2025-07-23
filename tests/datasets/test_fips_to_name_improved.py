from unittest.mock import patch

import pandas as pd
import pandera.pandas as pa
import pytest
from pandera.errors import SchemaError

from cfa.dataops.datasets.scenarios.schemas.fips_to_name import (
    tf_synth_data as fips_synth_data,
)
from cfa.dataops.datasets.scenarios.schemas.fips_to_name_improved import (
    extract_schema,
    load_schema,
    raw_synth_data,
    tf_synth_data,
)
from cfa.dataops.etl.scenarios.fips_to_name_improved import transform


def test_fips_to_name_improved_schemas():
    assert isinstance(extract_schema, pa.DataFrameSchema)
    assert isinstance(load_schema, pa.DataFrameSchema)

    # Check if the schemas have the expected columns
    assert isinstance(extract_schema(raw_synth_data), pd.DataFrame)
    assert isinstance(load_schema(tf_synth_data), pd.DataFrame)

    with pytest.raises(SchemaError):
        ex_cols = raw_synth_data.columns
        extract_schema(
            raw_synth_data.rename(
                columns={col: col + "_renamed" for col in ex_cols}
            )
        )

    with pytest.raises(SchemaError):
        tf_cols = tf_synth_data.columns
        load_schema(
            tf_synth_data.rename(
                columns={col: col + "_renamed" for col in tf_cols}
            )
        )


def test_fips_to_name_improved_transform():
    # Create mock FIPS data that would be returned by get_data
    # This should match the structure expected by the SQL template
    # and include state abbreviations that might appear in the raw data
    mock_fips_data = fips_synth_data

    extract_df = raw_synth_data

    # Mock the get_data function to return our mock FIPS data
    with patch(
        "cfa.dataops.etl.scenarios.fips_to_name_improved.get_data"
    ) as mock_get_data:
        mock_get_data.return_value = mock_fips_data
        tf_df = transform(extract_df)

    # Check if the transformed DataFrame matches the expected schema
    assert isinstance(load_schema(tf_df), pd.DataFrame)

import pandas as pd
import pandera.pandas as pa
import pytest
from pandera.errors import SchemaError

from cfa.scenarios.dataops.datasets.scenarios.schemas.seroprevalence import (
    extract_schema,
    load_schema,
    raw_synth_data,
    tf_synth_data,
)
from cfa.scenarios.dataops.etl.scenarios.seroprevalence import transform


def test_seroprevalence_schemas():
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


def test_seroprevalence_transform():
    extract_df = raw_synth_data
    tf_df = transform(extract_df)

    # Check if the transformed DataFrame matches the expected schema
    assert isinstance(load_schema(tf_df), pd.DataFrame)

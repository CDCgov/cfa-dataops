from typing import Iterable, Optional, Union

import pandas as pd
import pandera.pandas as pa
from pandera import dtypes
from pandera.engines import pandas_engine


@pandas_engine.Engine.register_dtype
@dtypes.immutable
class FixedLenArray(pandas_engine.NpString):
    def check(
        self,
        pandera_dtype: dtypes.DataType,
        data_container: Optional[pd.Series] = None,
    ) -> Union[bool, Iterable[bool]]:
        # ensure that the data container's data type is a string,
        # using the parent class's check implementation
        correct_type = super().check(pandera_dtype)
        if not correct_type:
            return correct_type

        # ensure the filepaths actually exist locally
        if data_container is None:
            return True
        else:
            length = len(data_container[0])
            return data_container.map(lambda x: len(x) == length)

    def __str__(self) -> str:
        return str(self.__class__.__name__)

    def __repr__(self) -> str:
        return f"DataType({self})"


extract_schema = pa.DataFrameSchema(
    {
        "date": pa.Column(
            str,
            checks=[
                pa.Check.str_matches(
                    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}$"
                )
            ],
        ),
        "location": pa.Column(
            str, checks=[pa.Check.str_matches(r"^[A-Z]{2}$")]
        ),
        "demographic_category": pa.Column(
            str, checks=[pa.Check.str_matches(r"^[A-Za-z0-9\_\-]+$")]
        ),
        "census": pa.Column(
            int, checks=[pa.Check.in_range(0, 10_000_000_000)], nullable=True
        ),
        "administered_dose1": pa.Column(
            int, checks=[pa.Check.in_range(0, 10_000_000_000)], nullable=True
        ),
        "series_complete_yes": pa.Column(
            int, checks=[pa.Check.in_range(0, 10_000_000_000)], nullable=True
        ),
        "booster_doses": pa.Column(
            int, checks=[pa.Check.in_range(0, 10_000_000_000)], nullable=True
        ),
        "second_booster": pa.Column(
            int, checks=[pa.Check.in_range(0, 10_000_000_000)], nullable=True
        ),
        "administered_dose1_pct_agegroup": pa.Column(
            float, checks=[pa.Check.in_range(0, 100)], nullable=True
        ),
        "series_complete_pop_pct_agegroup": pa.Column(
            float, checks=[pa.Check.in_range(0, 100)], nullable=True
        ),
        "booster_doses_vax_pct_agegroup": pa.Column(
            float, checks=[pa.Check.in_range(0, 100)], nullable=True
        ),
        "second_booster_vax_pct_agegroup": pa.Column(
            float, checks=[pa.Check.in_range(0, 100)], nullable=True
        ),
    }
)

load_schema = pa.DataFrameSchema(
    {
        "date": pa.Column(
            str,
            checks=[
                pa.Check.str_matches(
                    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}$"
                )
            ],
        ),
        "state": pa.Column(str, checks=[pa.Check.str_matches(r"^[A-Z]{2}$")]),
        "age": pa.Column(
            str, checks=[pa.Check.isin(["65+", "18-49", "0-17", "50-64"])]
        ),
        "census": pa.Column(
            int, checks=[pa.Check.in_range(0, 10_000_000_000)], coerce=True
        ),
        "total": pa.Column(FixedLenArray),
        "percentage": pa.Column(FixedLenArray),
        "dose": pa.Column(FixedLenArray),
    }
)

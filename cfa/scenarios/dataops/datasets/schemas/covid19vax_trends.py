import random
from typing import Iterable, Optional, Union

import numpy as np
import pandas as pd
import pandera.pandas as pa
from faker import Faker
from pandera import dtypes
from pandera.engines import pandas_engine

fake = Faker()
df_len = 100


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
                    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}(T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3})?$"
                )
            ],
        ),
        "location": pa.Column(
            str, checks=[pa.Check.str_matches(r"^[A-Z]{2}$")]
        ),
        "demographic_category": pa.Column(
            str, checks=[pa.Check.str_matches(r"^[A-Za-z0-9\_\-\+\<\>]+$")]
        ),
        "census": pa.Column(
            int, checks=[pa.Check.in_range(0, 10_000_000_000)], nullable=True
        ),
        "administered_dose1": pa.Column(
            int,
            checks=[pa.Check.in_range(0, 10_000_000_000)],
            nullable=True,
            coerce=True,
        ),
        "series_complete_yes": pa.Column(
            int,
            checks=[pa.Check.in_range(0, 10_000_000_000)],
            nullable=True,
            coerce=True,
        ),
        "booster_doses": pa.Column(
            int,
            checks=[pa.Check.in_range(0, 10_000_000_000)],
            nullable=True,
            coerce=True,
        ),
        "second_booster": pa.Column(
            int,
            checks=[pa.Check.in_range(0, 10_000_000_000)],
            nullable=True,
            coerce=True,
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
                    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}(T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3})?$"
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

locations = ["CA", "ME", "NV", "MD"]
ages = ["<5", "5-11_", "12-17_", "18-24_", "25-49_", "50-64_", "65+_"]
tf_ages = ["0-17", "18-49", "50-64", "65+"]

raw_synth_data = pd.DataFrame()
raw_synth_data = raw_synth_data.assign(
    date=pd.Series(
        str(
            fake.date_between_dates(
                pd.to_datetime("2021-01-29"), pd.to_datetime("2023-05-10")
            )
        )
        for _ in range(df_len)
    ),
    location=pd.Series(random.choice(locations) for _ in range(df_len)),
    demographic_category=pd.Series(
        ["Ages_" + random.choice(ages) + "yrs" for _ in range(df_len)]
    ),
    census=pd.Series(
        fake.unique.random_int(min=200000, max=1000000) for _ in range(df_len)
    ),
    administered_dose1_pct_agegroup=pd.Series(
        random.uniform(1.0, 50.0) for _ in range(df_len)
    ),
    series_complete_pop_pct_agegroup=pd.Series(
        random.uniform(0.1, 25.0) for _ in range(df_len)
    ),
    booster_doses_vax_pct_agegroup=pd.Series(
        random.uniform(0.0, 10.0) for _ in range(df_len)
    ),
    second_booster_vax_pct_agegroup=pd.Series(
        random.uniform(0.0, 10.0) for _ in range(df_len)
    ),
)
raw_synth_data["administered_dose1"] = raw_synth_data["census"] * [
    random.uniform(0.1, 0.5) for _ in range(df_len)
]
raw_synth_data["series_complete_yes"] = raw_synth_data[
    "administered_dose1"
] * [random.uniform(0.1, 0.5) for _ in range(df_len)]
raw_synth_data["booster_doses"] = raw_synth_data["administered_dose1"] * [
    random.uniform(0.1, 0.25) for _ in range(df_len)
]
raw_synth_data["second_booster"] = raw_synth_data["booster_doses"] * [
    random.uniform(0.1, 0.5) for _ in range(df_len)
]

tf_synth_data = pd.DataFrame()
tf_synth_data = tf_synth_data.assign(
    date=pd.Series(
        str(
            fake.date_between_dates(
                pd.to_datetime("2021-01-29"), pd.to_datetime("2023-05-10")
            )
        )
        for _ in range(df_len)
    ),
    state=pd.Series(random.choice(locations) for _ in range(df_len)),
    age=pd.Series([random.choice(tf_ages) for _ in range(df_len)]),
    census=pd.Series(
        fake.unique.random_int(min=200000, max=1000000) for _ in range(df_len)
    ),
    percentage=pd.Series(
        np.array(
            [
                random.uniform(0.5, 0.99),
                random.uniform(0.25, 0.5),
                random.uniform(0.1, 0.24),
            ]
        )
        for _ in range(df_len)
    ),
    dose=pd.Series([1, 2, 3] for _ in range(df_len)),
)
tf_synth_data["total"] = tf_synth_data["census"] * tf_synth_data["percentage"]

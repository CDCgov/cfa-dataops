import pandera.pandas as pa
import pandas as pd
import numpy as np
import random
from faker import Faker
fake=Faker()
df_len = 100

extract_schema = pa.DataFrameSchema(
    {
        "date": pa.Column(str),
        "location": pa.Column(str),
        "demographic_category": pa.Column(str),
        "census": pa.Column(float, nullable=True),
        "administered_dose1": pa.Column(float, nullable=True),
        "series_complete_yes": pa.Column(float, nullable=True),
        "booster_doses": pa.Column(float, nullable=True),
        "second_booster": pa.Column(float, nullable=True),
        "administered_dose1_pct_agegroup": pa.Column(float, nullable=True),
        "series_complete_pop_pct_agegroup": pa.Column(float, nullable=True),
        "booster_doses_vax_pct_agegroup": pa.Column(float, nullable=True),
        "second_booster_vax_pct_agegroup": pa.Column(float, nullable=True),
    }
)

load_schema = pa.DataFrameSchema(
    {
        "date": pa.Column(str),
        "state": pa.Column(str),
        "age": pa.Column(object),
        "census": pa.Column(int, coerce=True),
        "total": pa.Column(object),
        "percentage": pa.Column(object),
        "dose": pa.Column(object),
    }
)

locations = ['CA', 'ME', 'NV', 'MD']
ages = ["<5", "5-11_", "12-17_", "18-24_", "25-49_", "50-64_", "65+_"]
tf_ages = ["0-17", "18-49", "50-64", "65+"]

raw_synth_data = pd.DataFrame()
raw_synth_data = raw_synth_data.assign(
    date = pd.Series(str(fake.date_between_dates(pd.to_datetime("2021-01-29"), pd.to_datetime("2023-05-10"))) for _ in range(df_len)),
    location = pd.Series(random.choice(locations) for _ in range(df_len)),
    demographic_category = pd.Series(["Ages_"+random.choice(ages)+"yrs" for _ in range(df_len)]),
    census = pd.Series(fake.unique.random_int(min = 200000, max = 1000000) for _ in range(df_len)),
    administered_dose1_pct_agegroup = pd.Series(random.uniform(1.0, 50.0) for _ in range(df_len)),
    series_complete_pop_pct_agegroup = pd.Series(random.uniform(0.1, 25.0) for _ in range(df_len)),
    booster_doses_vax_pct_agegroup = pd.Series(random.uniform(0.0, 10.0) for _ in range(df_len)),
    second_booster_vax_pct_agegroup = pd.Series(random.uniform(0.0, 10.0) for _ in range(df_len)),
)
raw_synth_data['administered_dose1'] = raw_synth_data['census'] * [random.uniform(0.1, 0.5) for _ in range(df_len)]
raw_synth_data['series_complete_yes'] = raw_synth_data['administered_dose1'] * [random.uniform(0.1, 0.5) for _ in range(df_len)]
raw_synth_data['booster_doses'] = raw_synth_data['administered_dose1'] * [random.uniform(0.1, 0.25) for _ in range(df_len)]
raw_synth_data['second_booster'] = raw_synth_data['booster_doses'] * [random.uniform(0.1, 0.5) for _ in range(df_len)]

tf_synth_data = pd.DataFrame()
tf_synth_data = tf_synth_data.assign(
    date = pd.Series(str(fake.date_between_dates(pd.to_datetime("2021-01-29"), pd.to_datetime("2023-05-10"))) for _ in range(df_len)),
    state = pd.Series(random.choice(locations) for _ in range(df_len)),
    age = pd.Series([random.choice(tf_ages) for _ in range(df_len)]),
    census = pd.Series(fake.unique.random_int(min = 200000, max = 1000000) for _ in range(df_len)),
    percentage = pd.Series(np.array([random.uniform(0.5, 0.99), random.uniform(0.25, 0.5), random.uniform(0.1, 0.24)]) for _ in range(df_len)),
    dose = pd.Series([1,2,3] for _ in range(df_len))
)
tf_synth_data['total'] = tf_synth_data['census'] * tf_synth_data['percentage']

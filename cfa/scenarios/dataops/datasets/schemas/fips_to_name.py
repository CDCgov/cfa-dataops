import pandas as pd
import pandera.pandas as pa
from faker import Faker

fake = Faker()
df_len = 100

extract_schema = pa.DataFrameSchema(
    {
        "state_name": pa.Column(str),
        "county_name": pa.Column(str, nullable=True),
        "city_name": pa.Column(str, nullable=True),
        "state_code": pa.Column(str),
        "state_fipcode": pa.Column(int),
        "county_code": pa.Column(str, nullable=True),
        "county_fipcode": pa.Column(float, nullable=True),
        "city_code": pa.Column(float, nullable=True),
        "city_fipcode": pa.Column(float, nullable=True),
    }
)

load_schema = pa.DataFrameSchema(
    {
        "stname": pa.Column(str),
        "st": pa.Column(object),
        "stusps": pa.Column(str),
    }
)

raw_synth_data = pd.DataFrame()

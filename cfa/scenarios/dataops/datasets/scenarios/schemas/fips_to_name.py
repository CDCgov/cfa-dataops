import random

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

stname = [
    "Alabama",
    "Alaska",
    "Arizona",
    "Arkansas",
    "California",
    "Colorado",
    "Connecticut",
    "Delaware",
    "District Of Columbia",
    "Florida",
    "Georgia",
    "Hawaii",
    "Idaho",
    "Illinois",
    "Indiana",
    "Iowa",
    "Kansas",
    "Kentucky",
    "Louisiana",
    "Maine",
    "Maryland",
    "Massachusetts",
    "Michigan",
    "Minnesota",
    "Mississippi",
    "Missouri",
    "Montana",
    "Nebraska",
    "Nevada",
    "New Hampshire",
    "New Jersey",
    "New Mexico",
    "New York",
    "North Carolina",
    "North Dakota",
    "Ohio",
    "Oklahoma",
    "Oregon",
    "Pennsylvania",
    "Rhode Island",
    "South Carolina",
    "South Dakota",
    "Tennessee",
    "Texas",
    "United States",
    "Utah",
    "Vermont",
    "Virginia",
    "Washington",
    "West Virginia",
    "Wisconsin",
    "Wyoming",
]

raw_synth_data = pd.DataFrame()
raw_synth_data = raw_synth_data.assign(
    state_name=pd.Series(
        [random.choice(stname).upper() for _ in range(df_len)]
    ),
    county_name=pd.Series([fake.city() for _ in range(df_len)]),
    city_name=pd.Series([fake.city() for _ in range(df_len)]),
    state_code=pd.Series([fake.state_abbr() for _ in range(df_len)]),
    state_fipcode=pd.Series(
        [fake.random_int(min=1, max=56) for _ in range(df_len)]
    ),
    county_code=pd.Series(
        ["C" + str(fake.random_int(min=1, max=999)) for _ in range(df_len)]
    ),
    county_fipcode=pd.Series(
        [float(fake.random_int(min=1, max=99999)) for _ in range(df_len)]
    ),
    city_code=pd.Series(
        [float(fake.random_int(min=1, max=999)) for _ in range(df_len)]
    ),
    city_fipcode=pd.Series(
        [float(fake.random_int(min=1, max=99999)) for _ in range(df_len)]
    ),
)

st = [
    "1",
    "2",
    "4",
    "5",
    "6",
    "8",
    "9",
    "10",
    "11",
    "12",
    "13",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
    "24",
    "25",
    "26",
    "27",
    "28",
    "29",
    "30",
    "31",
    "32",
    "33",
    "34",
    "35",
    "36",
    "37",
    "38",
    "39",
    "40",
    "41",
    "42",
    "44",
    "45",
    "46",
    "47",
    "48",
    "US",
    "49",
    "50",
    "51",
    "53",
    "54",
    "55",
    "56",
]
stusps = [
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "DC",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "US",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
]

tf_synth_data = pd.DataFrame()
tf_synth_data = tf_synth_data.assign(
    stname=pd.Series(stname),
    st=pd.Series(st),
    stusps=pd.Series(stusps),
)

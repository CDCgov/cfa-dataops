import pandera.pandas as pa
import pandas as pd
import random
from faker import Faker

fake=Faker()
df_len = 100

extract_schema = pa.DataFrameSchema(
    {
        "indicator": pa.Column(str),
        "geographic_area": pa.Column(str),
        "geographic_identifier": pa.Column(object),
        "race": pa.Column(str),
        "sex": pa.Column(str),
        "age": pa.Column(str),
        "time_period": pa.Column(str),
        "n_unweighted": pa.Column(float, nullable=True),
        "estimate_weighted": pa.Column(float, nullable=True),
        "_2_5": pa.Column(float, nullable=True),
        "_97_5": pa.Column(float, nullable=True),
    }
)

load_schema = pa.DataFrameSchema(
    {
        "indicator": pa.Column(str),
        "geographic_area": pa.Column(str),
        "geographic_identifier": pa.Column(object),
        "race": pa.Column(str),
        "sex": pa.Column(str),
        "age": pa.Column(str),
        "time_period": pa.Column(str),
        "n_unweighted": pa.Column(float, nullable=True),
        "estimate_weighted": pa.Column(float, nullable=True),
        "_2_5": pa.Column(float, nullable=True),
        "_97_5": pa.Column(float, nullable=True),
    }
)

indicator_opts = ["Overall",
                  "Past infection with or without vaccination",
                  "Presumed vaccination without infection",
                  "Neither past infection nor vaccination",
                  "Indeterminate",
                  "Combined seroprevalence"
                  ]

geographic_area_opts = ['Overall', 'Northeast', 'Midwest', 'South', 'West', 'Alabama', 'Arkansas',
 'Arizona', 'California', 'Colorado', 'Connecticut', 'District of Columbia',
 'Delaware', 'Florida', 'Georgia', 'Iowa', 'Idaho', 'Illinois', 'Indiana',
 'Kansas', 'Kentucky', 'Louisiana', 'Massachusetts', 'Maryland', 'Maine',
 'Michigan', 'Minnesota', 'Missouri', 'Mississippi', 'Montana',
 'North Carolina', 'North Dakota', 'Nebraska', 'New Hampshire', 'New Jersey',
 'New Mexico', 'Nevada', 'New York', 'Ohio', 'Oklahoma', 'Oregon',
 'Pennsylvania', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah',
 'Virginia', 'Vermont', 'Washington', 'Wisconsin', 'West Virginia', 'Wyoming']
geographic_ind_opts = ['USA', '1', '2', '3', '4', '01', '05', '04', '06', '08', '09', '11', '10', '12', '13',
 '19', '16', '17', '18', '20', '21', '22', '25', '24', '23', '26', '27', '29', '28',
 '30', '37', '38', '31', '33', '34', '35', '32', '36', '39', '40', '41', '42', '45',
 '46', '47', '48', '49', '51', '50', '53', '55', '54', '56']
race_opts = ['Overall', 'Hispanic', 'Non-Hispanic Asian', 'Non-Hispanic Black',
 'Non-Hispanic White', 'Other']
sex_opts = ["Overall", "Female", "Male"]
age_opts = ["Overall", "16 to 29", "30 to 49", "50 to 64", "65 and over"]
time_per_opts = ["2022 Quarter 1", "2022 Quarter 4"]

raw_synth_data = pd.DataFrame()
raw_synth_data = raw_synth_data.assign(
    indicator = [random.choice(indicator_opts) for _ in range(df_len)],
    geographic_area = [random.choice(geographic_area_opts) for _ in range(df_len)],
    geographic_identifier = [random.choice(geographic_ind_opts) for _ in range(df_len)],
    race = [random.choice(race_opts) for _ in range(df_len)],
    sex = [random.choice(sex_opts) for _ in range(df_len)],
    age = [random.choice(age_opts) for _ in range(df_len)],
    time_period = [random.choice(time_per_opts) for _ in range(df_len)],
    n_unweighted = [fake.random.uniform(0.0, 75687.0) for _ in range(df_len)],
    estimate_weighted = [fake.random.uniform(0, 100) for _ in range(df_len)],
    _2_5 = [fake.random.uniform(0, 100) for _ in range(df_len)],
    _97_5 = [fake.random.uniform(0, 100) for _ in range(df_len)],
)

tf_synth_data = raw_synth_data.copy()


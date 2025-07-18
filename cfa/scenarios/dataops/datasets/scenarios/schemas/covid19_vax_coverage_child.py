import random

import pandas as pd
import pandera.pandas as pa
from faker import Faker

fake = Faker()
df_len = 1000

extract_schema = pa.DataFrameSchema(
    {
        "vaccine": pa.Column(str, checks=pa.Check.equal_to("COVID")),
        "geographic_level": pa.Column(str),
        "geographic_name": pa.Column(str),
        "demographic_level": pa.Column(str),
        "demographic_name": pa.Column(str),
        "indicator_category_label": pa.Column(str),
        "indicator_label": pa.Column(str),
        "month_week": pa.Column(str),
        "week_ending": pa.Column(str),
        "estimate": pa.Column(float, nullable=True),
        "ci_half_width_95pct": pa.Column(float, nullable=True),
        "unweighted_sample_size": pa.Column(int, nullable=True),
        "suppression_flag": pa.Column(int),
    }
)

load_schema = pa.DataFrameSchema(
    {
        "date": pa.Column(str),
        "demographic_level": pa.Column(str, pa.Check.equal_to("Overall")),
        "demographic_name": pa.Column(str, pa.Check.equal_to("0-17")),
        "indicator_category_label": pa.Column(
            str, pa.Check.equal_to("Vaccinated")
        ),
        "estimate": pa.Column(float),
        "covid_season": pa.Column(
            str, pa.Check.isin(["2023-2024", "2024-2025"])
        ),
        "date1": pa.Column(str),
    }
)

geo_level_opts = ["National", "Region", "State", "Substate"]
geo_name_opts = [
    "National",
    "Region 4",
    "Region 5",
    "Region 7",
    "Region 8",
    "Region 9",
    "Alabama",
    "Arizona",
    "Arkansas",
    "Delaware",
    "District of Columbia",
    "Florida",
    "Georgia",
    "Idaho",
    "Illinois",
    "Region 1",
    "Indiana",
    "Region 10",
    "Iowa",
    "Kentucky",
    "Louisiana",
    "Maine",
    "Region 3",
    "Massachusetts",
    "Michigan",
    "Minnesota",
    "Mississippi",
    "Region 2",
    "Missouri",
    "Region 6",
    "Montana",
    "Nebraska",
    "Nevada",
    "New Jersey",
    "New Mexico",
    "New York",
    "North Carolina",
    "North Dakota",
    "Ohio",
    "Oklahoma",
    "Oregon",
    "Pennsylvania",
    "Alaska",
    "Puerto Rico",
    "Rhode Island",
    "South Carolina",
    "South Dakota",
    "Tennessee",
    "Texas",
    "Vermont",
    "Virginia",
    "California",
    "Colorado",
    "Washington",
    "West Virginia",
    "Wisconsin",
    "Wyoming",
    "Illinois-City of Chicago",
    "Connecticut",
    "Illinois-Rest of State",
    "New York-New York City",
    "New York-Rest of State",
    "Hawaii",
    "Pennsylvania-Rest of State",
    "Texas-Rest of State",
    "Kansas",
    "Maryland",
    "New Hampshire",
    "Utah",
    "Pennsylvania-Philadelphia County",
    "Texas-Bexar County",
    "Texas-City of Houston",
    "U.S. Virgin Islands",
    "Guam",
]
demo_level_opts = [
    "Overall",
    "Poverty Status",
    "Race and Ethnicity",
    "Urbanicity",
    "Age",
    "Mother's Education",
]
demo_name_opts = ["6 months-17 years"]
ind_label_opts = ["4-level vaccination and intent", "Up-to-date"]
ind_cat_label_opts = [
    "Probably will get a vaccine or are unsure",
    "Definitely will get a vaccine",
    "Yes",
    "Vaccinated",
    "Definitely or probably will not get a vaccine",
]

raw_synth_data = pd.DataFrame()
raw_synth_data = raw_synth_data.assign(
    vaccine=["COVID" for _ in range(df_len)],
    geographic_level=[random.choice(geo_level_opts) for _ in range(df_len)],
    geographic_name=[random.choice(geo_name_opts) for _ in range(df_len)],
    demographic_level=[random.choice(demo_level_opts) for _ in range(df_len)],
    demographic_name=[random.choice(demo_name_opts) for _ in range(df_len)],
    indicator_category_label=[
        random.choice(ind_cat_label_opts) for _ in range(df_len)
    ],
    indicator_label=[random.choice(ind_label_opts) for _ in range(df_len)],
    month_week=[
        fake.month_name() + " " + str(random.randint(1, 4))
        for _ in range(df_len)
    ],
    week_ending=[
        fake.date_between(start_date="-2y", end_date="today").strftime(
            "%Y-%m-%d"
        )
        for _ in range(df_len)
    ],
    estimate=[
        random.uniform(0, 1000) if random.random() > 0.1 else None
        for _ in range(df_len)
    ],
    ci_half_width_95pct=[
        random.uniform(0.05, 0.5) if random.random() > 0.1 else None
        for _ in range(df_len)
    ],
    unweighted_sample_size=[random.randint(1, 10000) for _ in range(df_len)],
    suppression_flag=[random.randint(0, 99) for _ in range(df_len)],
)
tf_synth_data = pd.DataFrame()
tf_synth_data = tf_synth_data.assign(
    date=[
        fake.date_between(start_date="-2y", end_date="today").strftime(
            "%Y-%m-%d"
        )
        for _ in range(df_len)
    ],
    demographic_level=["Overall" for _ in range(df_len)],
    demographic_name=["0-17" for _ in range(df_len)],
    indicator_category_label=["Vaccinated" for _ in range(df_len)],
    estimate=[random.uniform(0, 1000) for _ in range(df_len)],
    covid_season=[
        random.choice(["2023-2024", "2024-2025"]) for _ in range(df_len)
    ],
    date1=[
        fake.date_between(start_date="-1y", end_date="today").strftime(
            "%Y-%m-%d"
        )
        for _ in range(df_len)
    ],
)

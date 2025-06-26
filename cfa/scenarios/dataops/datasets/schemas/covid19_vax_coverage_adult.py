import random

import pandas as pd
import pandera.pandas as pa
from faker import Faker

fake = Faker()
df_len = 100

extract_schema = pa.DataFrameSchema(
    {
        "vaccine": pa.Column(str),
        "geographic_level": pa.Column(str),
        "geographic_name": pa.Column(str),
        "demographic_level": pa.Column(str),
        "demographic_name": pa.Column(str),
        "indicator_label": pa.Column(str),
        "indicator_category_label": pa.Column(str),
        "month_week": pa.Column(str),
        "week_ending": pa.Column(str),
        "estimate": pa.Column(float, nullable=True),
        "ci_half_width_95pct": pa.Column(float, nullable=True),
        "unweighted_sample_size": pa.Column(int, nullable=True),
        "current_season_week_ending": pa.Column(str, nullable=True),
        "covid_season": pa.Column(str),
        "suppression_flag": pa.Column(int),
    }
)

load_schema = pa.DataFrameSchema(
    {
        "vaccine": pa.Column(str),
        "geographic_level": pa.Column(str),
        "geographic_name": pa.Column(str),
        "demographic_level": pa.Column(str),
        "demographic_name": pa.Column(str),
        "indicator_label": pa.Column(str),
        "indicator_category_label": pa.Column(str),
        "month_week": pa.Column(str),
        "week_ending": pa.Column(str),
        "estimate": pa.Column(
            float, checks=pa.Check.in_range(0, 100), nullable=True
        ),
        "ci_half_width_95pct": pa.Column(
            float, checks=pa.Check.in_range(0, 100), nullable=True
        ),
        "unweighted_sample_size": pa.Column(
            int, checks=pa.Check.in_range(0, 10_000_000), nullable=True
        ),
        "covid_season": pa.Column(str),
        "suppression_flag": pa.Column(int),
        "date": pa.Column(str),
        "date1": pa.Column(str),
    }
)
geo_name_opts = [
    "National",
    "Region 7",
    "Pennsylvania-Rest of State",
    "Oregon",
    "Tennessee",
    "Region 3",
    "Iowa",
    "New Jersey",
    "Pennsylvania",
    "New York-New York City",
    "New York",
    "Michigan",
    "Rhode Island",
    "Colorado",
    "Delaware",
    "New York-Rest of State",
    "Arkansas",
    "Arizona",
    "Illinois-City of Chicago",
    "Region 9",
    "North Carolina",
    "Nevada",
    "Utah",
    "West Virginia",
    "Alaska",
    "Pennsylvania-Philadelphia County",
    "Region 10",
    "Region 4",
    "Wisconsin",
    "Texas-City of Houston",
    "Maine",
    "Nebraska",
    "Kentucky",
    "Region 2",
    "Florida",
    "Texas-Bexar County",
    "Virginia",
    "New Hampshire",
    "Georgia",
    "Wyoming",
    "Region 5",
    "New Mexico",
    "Louisiana",
    "Mississippi",
    "Puerto Rico",
    "Missouri",
    "Kansas",
    "Oklahoma",
    "Texas",
    "Alabama",
    "Indiana",
    "Massachusetts",
    "South Dakota",
    "Minnesota",
    "District of Columbia",
    "Texas-Rest of State",
    "Ohio",
    "Illinois-Rest of State",
    "Region 1",
    "North Dakota",
    "Washington",
    "South Carolina",
    "Connecticut",
    "Montana",
    "Illinois",
    "Idaho",
    "Region 6",
    "Vermont",
    "Hawaii",
    "Region 8",
    "California",
    "Maryland",
    "U.S. Virgin Islands",
    "Guam",
]
demo_level_opts = [
    "Race and Ethnicity",
    "Urbanicity",
    "Overall",
    "Age",
    "Sexual Orientation",
    "Sex",
    "Disability Status",
    "Poverty Status",
    "Health Insurance",
    "Health Insurance Among 18-64 Years",
]
demo_name_opts = [
    "Multiple Race/Other (Excludes Asian, AIAN, PI/NH), Non-Hispanic",
    "Rural (Non-MSA)",
    "Asian, Non-Hispanic",
    "White, Non-Hispanic",
    "18+ years",
    "65+ years",
    "Other, Non-Hispanic",
    "Gay/Lesbian/Bisexual/Other",
    "65-74 years",
    "Urban MSA Principal City",
    "Female",
    "75+ years",
    "Yes",
    "30-39 years",
    "Don't Know/Refused",
    "American Indian/Alaska Native, Non-Hispanic",
    "Straight",
    "Above Poverty, Income < $75k",
    "Male",
    "Above Poverty, Income >= $75k",
    "No",
    "Uninsured",
    "Below Poverty",
    "Pacific Islander/Native Hawaiian, Non-Hispanic",
    "50-64 years",
    "60+ years",
    "Insured",
    "Poverty Status Unknown",
    "Suburban (MSA Non-Principal City)",
    "18-49 years",
    "40-49 years",
    "Black, Non-Hispanic",
    "18-29 years",
    "Hispanic",
]
ind_label_opts = ["4-level vaccination and intent", "Up-to-date"]
ind_cat_label_opts = [
    "Definitely will get a vaccine",
    "Received a vaccination",
    "Yes",
    "Probably will get a vaccine or are unsure",
    "Definitely or probably will not get a vaccine",
]
year_opts = ["2023-2024", "2024-2025"]

raw_synth_data = pd.DataFrame()
raw_synth_data = raw_synth_data.assign(
    vaccine=["COVID" for _ in range(df_len)],
    geographic_level=[
        random.choice(["National", "Region", "Substate", "State"])
        for _ in range(df_len)
    ],
    geographic_name=[random.choice(geo_name_opts) for _ in range(df_len)],
    demographic_level=[random.choice(demo_level_opts) for _ in range(df_len)],
    demo_name_opts=[random.choice(demo_name_opts) for _ in range(df_len)],
    indicator_label=[random.choice(ind_label_opts) for _ in range(df_len)],
    indicator_category_label=[
        random.choice(ind_cat_label_opts) for _ in range(df_len)
    ],
    month_week=[
        fake.month_name() + " " + str(random.randint(1, 52))
        for _ in range(df_len)
    ],
    week_ending=[fake.date_this_year() for _ in range(df_len)],
    estimate=[random.uniform(0, 100) for _ in range(df_len)],
    ci_half_width_95pct=[random.uniform(0, 100) for _ in range(df_len)],
    unweighted_sample_size=[
        random.randint(1, 10_000_000) for _ in range(df_len)
    ],
    current_season_week_ending=[
        random.choice(year_opts) for _ in range(df_len)
    ],
    covid_season=[fake.year() for _ in range(df_len)],
    suppression_flag=[random.randint(0, 99) for _ in range(df_len)],
)
tf_synth_data = raw_synth_data.copy()
tf_synth_data["date"] = tf_synth_data["week_ending"].apply(
    lambda x: x.strftime("%Y-%m-%d")
)
tf_synth_data["date1"] = tf_synth_data["week_ending"].apply(
    lambda x: x.strftime("%Y-%m-%d")
)

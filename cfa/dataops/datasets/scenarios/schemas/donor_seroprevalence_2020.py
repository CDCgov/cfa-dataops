import random

import pandas as pd
import pandera.pandas as pa
from faker import Faker

fake = Faker()
df_len = 100

extract_schema = pa.DataFrameSchema(
    {
        "region": pa.Column(str),
        "region_abbreviation": pa.Column(str),
        "year_and_month": pa.Column(str),
        "median_donation_date": pa.Column(str),
        "n_total_prevalence": pa.Column(int),
        "rate_total_prevalence": pa.Column(float),
        "lower_ci_total_prevalence": pa.Column(float),
        "upper_ci_total_prevalence": pa.Column(float),
        "n_16_29_years_prevalence": pa.Column(int),
        "rate_16_29_years_prevalence": pa.Column(float),
        "lower_ci_16_29_years_prevalence": pa.Column(float),
        "upper_ci_16_29_years_prevalence": pa.Column(float),
        "n_30_49_years_prevalence": pa.Column(int),
        "rate_30_49_years_prevalence": pa.Column(float),
        "lower_ci_30_49_years_prevalence": pa.Column(float),
        "upper_ci_30_49_years_prevalence": pa.Column(float),
        "n_50_64_years_prevalence": pa.Column(int),
        "rate_50_64_years_prevalence": pa.Column(float),
        "lower_ci_50_64_years_prevalence": pa.Column(float),
        "upper_ci_50_64_years_prevalence": pa.Column(float),
        "n_65_years_prevalence": pa.Column(int),
        "rate_65_years_prevalence": pa.Column(float),
        "lower_ci_65_years_prevalence": pa.Column(float),
        "upper_ci_65_years_prevalence": pa.Column(float),
        "n_male_prevalence": pa.Column(int),
        "rate_male_prevalence": pa.Column(float),
        "lower_ci_male_prevalence": pa.Column(float),
        "upper_ci_male_prevalence": pa.Column(float),
        "n_female_prevalence": pa.Column(int),
        "rate_female_prevalence": pa.Column(float),
        "lower_ci_female_prevalence": pa.Column(float),
        "upper_ci_female_prevalence": pa.Column(float),
        "n_white_prevalence": pa.Column(float, nullable=True),
        "rate_white_prevalence": pa.Column(float, nullable=True),
        "lower_ci_white_prevalence": pa.Column(float, nullable=True),
        "upper_ci_white_prevalence": pa.Column(float, nullable=True),
        "n_black_prevalence": pa.Column(float, nullable=True),
        "rate_black_prevalence": pa.Column(float, nullable=True),
        "lower_ci_black_prevalence": pa.Column(float, nullable=True),
        "upper_ci_black_prevalence": pa.Column(float, nullable=True),
        "n_asian_prevalence": pa.Column(float, nullable=True),
        "rate_asian_prevalence": pa.Column(float, nullable=True),
        "lower_ci_asian_prevalence": pa.Column(float, nullable=True),
        "upper_ci_asian_prevalence": pa.Column(float, nullable=True),
        "n_hispanic_prevalence": pa.Column(float, nullable=True),
        "rate_hispanic_prevalence": pa.Column(float, nullable=True),
        "lower_ci_hispanic_prevalence": pa.Column(float, nullable=True),
        "upper_ci_hispanic_prevalence": pa.Column(float, nullable=True),
        "n_other_race_prevalence": pa.Column(float, nullable=True),
        "rate_other_race_prevalence": pa.Column(float, nullable=True),
        "lower_ci_other_race_prevalence": pa.Column(float, nullable=True),
        "upper_ci_other_race_prevalence": pa.Column(float, nullable=True),
    }
)

load_schema = pa.DataFrameSchema(
    {
        "region": pa.Column(str),
        "region_abbreviation": pa.Column(str),
        "year_and_month": pa.Column(str),
        "median_donation_date": pa.Column(str),
        "n_total_prevalence": pa.Column(int),
        "rate_total_prevalence": pa.Column(float),
        "lower_ci_total_prevalence": pa.Column(float),
        "upper_ci_total_prevalence": pa.Column(float),
        "n_16_29_years_prevalence": pa.Column(int),
        "rate_16_29_years_prevalence": pa.Column(float),
        "lower_ci_16_29_years_prevalence": pa.Column(float),
        "upper_ci_16_29_years_prevalence": pa.Column(float),
        "n_30_49_years_prevalence": pa.Column(int),
        "rate_30_49_years_prevalence": pa.Column(float),
        "lower_ci_30_49_years_prevalence": pa.Column(float),
        "upper_ci_30_49_years_prevalence": pa.Column(float),
        "n_50_64_years_prevalence": pa.Column(int),
        "rate_50_64_years_prevalence": pa.Column(float),
        "lower_ci_50_64_years_prevalence": pa.Column(float),
        "upper_ci_50_64_years_prevalence": pa.Column(float),
        "n_65_years_prevalence": pa.Column(int),
        "rate_65_years_prevalence": pa.Column(float),
        "lower_ci_65_years_prevalence": pa.Column(float),
        "upper_ci_65_years_prevalence": pa.Column(float),
        "n_male_prevalence": pa.Column(int),
        "rate_male_prevalence": pa.Column(float),
        "lower_ci_male_prevalence": pa.Column(float),
        "upper_ci_male_prevalence": pa.Column(float),
        "n_female_prevalence": pa.Column(int),
        "rate_female_prevalence": pa.Column(float),
        "lower_ci_female_prevalence": pa.Column(float),
        "upper_ci_female_prevalence": pa.Column(float),
        "n_white_prevalence": pa.Column(float, nullable=True),
        "rate_white_prevalence": pa.Column(float, nullable=True),
        "lower_ci_white_prevalence": pa.Column(float, nullable=True),
        "upper_ci_white_prevalence": pa.Column(float, nullable=True),
        "n_black_prevalence": pa.Column(float, nullable=True),
        "rate_black_prevalence": pa.Column(float, nullable=True),
        "lower_ci_black_prevalence": pa.Column(float, nullable=True),
        "upper_ci_black_prevalence": pa.Column(float, nullable=True),
        "n_asian_prevalence": pa.Column(float, nullable=True),
        "rate_asian_prevalence": pa.Column(float, nullable=True),
        "lower_ci_asian_prevalence": pa.Column(float, nullable=True),
        "upper_ci_asian_prevalence": pa.Column(float, nullable=True),
        "n_hispanic_prevalence": pa.Column(float, nullable=True),
        "rate_hispanic_prevalence": pa.Column(float, nullable=True),
        "lower_ci_hispanic_prevalence": pa.Column(float, nullable=True),
        "upper_ci_hispanic_prevalence": pa.Column(float, nullable=True),
        "n_other_race_prevalence": pa.Column(float, nullable=True),
        "rate_other_race_prevalence": pa.Column(float, nullable=True),
        "lower_ci_other_race_prevalence": pa.Column(float, nullable=True),
        "upper_ci_other_race_prevalence": pa.Column(float, nullable=True),
    }
)


raw_synth_data = pd.DataFrame()
raw_synth_data = raw_synth_data.assign(
    region=[fake.state() for _ in range(df_len)],
    region_abbreviation=[fake.state_abbr() for _ in range(df_len)],
    year_and_month=[
        fake.date_this_decade().strftime("%Y-%m") for _ in range(df_len)
    ],
    median_donation_date=[
        fake.date_this_decade().strftime("%Y-%m-%d") for _ in range(df_len)
    ],
    n_total_prevalence=[random.randint(100, 5000) for _ in range(df_len)],
    rate_total_prevalence=[
        round(random.uniform(0.1, 43.0), 2) for _ in range(df_len)
    ],
    lower_ci_total_prevalence=[
        round(random.uniform(0.05, 41.0), 2) for _ in range(df_len)
    ],
    upper_ci_total_prevalence=[
        round(random.uniform(42.0, 95.0), 2) for _ in range(df_len)
    ],
    n_16_29_years_prevalence=[random.randint(10, 900) for _ in range(df_len)],
    rate_16_29_years_prevalence=[
        round(random.uniform(0.1, 41.0), 2) for _ in range(df_len)
    ],
    lower_ci_16_29_years_prevalence=[
        round(random.uniform(0.1, 41.0), 2) for _ in range(df_len)
    ],
    upper_ci_16_29_years_prevalence=[
        round(random.uniform(42.0, 95.0), 2) for _ in range(df_len)
    ],
    n_30_49_years_prevalence=[random.randint(20, 300) for _ in range(df_len)],
    rate_30_49_years_prevalence=[
        round(random.uniform(0.1, 40.0), 2) for _ in range(df_len)
    ],
    lower_ci_30_49_years_prevalence=[
        round(random.uniform(0.1, 40.0), 2) for _ in range(df_len)
    ],
    upper_ci_30_49_years_prevalence=[
        round(random.uniform(45.0, 95.0), 2) for _ in range(df_len)
    ],
    n_50_64_years_prevalence=[random.randint(30, 400) for _ in range(df_len)],
    rate_50_64_years_prevalence=[
        round(random.uniform(0.1, 40.0), 2) for _ in range(df_len)
    ],
    lower_ci_50_64_years_prevalence=[
        round(random.uniform(0.05, 40.0), 2) for _ in range(df_len)
    ],
    upper_ci_50_64_years_prevalence=[
        round(random.uniform(41.0, 60.0), 2) for _ in range(df_len)
    ],
    n_65_years_prevalence=[random.randint(40, 500) for _ in range(df_len)],
    rate_65_years_prevalence=[
        round(random.uniform(0.1, 25.0), 2) for _ in range(df_len)
    ],
    lower_ci_65_years_prevalence=[
        round(random.uniform(0.08, 40.0), 2) for _ in range(df_len)
    ],
    upper_ci_65_years_prevalence=[
        round(random.uniform(41.0, 80.0), 2) for _ in range(df_len)
    ],
    n_male_prevalence=[random.randint(50, 600) for _ in range(df_len)],
    rate_male_prevalence=[
        round(random.uniform(0.1, 18.0), 2) for _ in range(df_len)
    ],
    lower_ci_male_prevalence=[
        round(random.uniform(0.1, 40.0), 2) for _ in range(df_len)
    ],
    upper_ci_male_prevalence=[
        round(random.uniform(41.0, 85.0), 2) for _ in range(df_len)
    ],
    n_female_prevalence=[random.randint(50, 600) for _ in range(df_len)],
    rate_female_prevalence=[
        round(random.uniform(0.1, 40.0), 2) for _ in range(df_len)
    ],
    lower_ci_female_prevalence=[
        round(random.uniform(0.1, 45.0), 2) for _ in range(df_len)
    ],
    upper_ci_female_prevalence=[
        round(random.uniform(46.0, 95.0), 2) for _ in range(df_len)
    ],
    n_white_prevalence=[random.uniform(0, 100) for _ in range(df_len)],
    rate_white_prevalence=[
        round(random.uniform(0.1, 9), 2) for _ in range(df_len)
    ],
    lower_ci_white_prevalence=[
        round(random.uniform(0.05, 40.0), 2) for _ in range(df_len)
    ],
    upper_ci_white_prevalence=[
        round(random.uniform(41.0, 95), 2) for _ in range(df_len)
    ],
    n_black_prevalence=[random.uniform(0, 100) for _ in range(df_len)],
    rate_black_prevalence=[
        round(random.uniform(0.1, 9), 2) for _ in range(df_len)
    ],
    lower_ci_black_prevalence=[
        round(random.uniform(0.05, 40), 2) for _ in range(df_len)
    ],
    upper_ci_black_prevalence=[
        round(random.uniform(41.0, 95.0), 2) for _ in range(df_len)
    ],
    n_asian_prevalence=[random.uniform(0, 100) for _ in range(df_len)],
    rate_asian_prevalence=[
        round(random.uniform(0.1, 0.9), 2) for _ in range(df_len)
    ],
    lower_ci_asian_prevalence=[
        round(random.uniform(0.05, 40.0), 2) for _ in range(df_len)
    ],
    upper_ci_asian_prevalence=[
        round(random.uniform(40.0, 60.0), 2) for _ in range(df_len)
    ],
    n_hispanic_prevalence=[random.uniform(0, 100) for _ in range(df_len)],
    rate_hispanic_prevalence=[
        round(random.uniform(0.1, 0.9), 2) for _ in range(df_len)
    ],
    lower_ci_hispanic_prevalence=[
        round(random.uniform(0.05, 40.0), 2) for _ in range(df_len)
    ],
    upper_ci_hispanic_prevalence=[
        round(random.uniform(60.0, 95.0), 2) for _ in range(df_len)
    ],
    n_other_race_prevalence=[random.uniform(0, 100) for _ in range(df_len)],
    rate_other_race_prevalence=[
        round(random.uniform(0.1, 0.9), 2) for _ in range(df_len)
    ],
    lower_ci_other_race_prevalence=[
        round(random.uniform(0.05, 25.0), 2) for _ in range(df_len)
    ],
    upper_ci_other_race_prevalence=[
        round(random.uniform(26.0, 50.0), 2) for _ in range(df_len)
    ],
)
raw_synth_data["year_and_month"] = raw_synth_data[
    "median_donation_date"
].apply(lambda x: x[:7])  # Extract year and month from median_donation_date

tf_synth_data = pd.DataFrame()
tf_synth_data = tf_synth_data.assign(
    region=[fake.state() for _ in range(df_len)],
    region_abbreviation=[fake.state_abbr() for _ in range(df_len)],
    year_and_month=[
        fake.date_this_decade().strftime("%Y-%m") for _ in range(df_len)
    ],
    median_donation_date=[
        fake.date_this_decade().strftime("%Y-%m-%d") for _ in range(df_len)
    ],
    n_total_prevalence=[random.randint(100, 5000) for _ in range(df_len)],
    rate_total_prevalence=[
        round(random.uniform(0.1, 43.0), 2) for _ in range(df_len)
    ],
    lower_ci_total_prevalence=[
        round(random.uniform(0.05, 41.0), 2) for _ in range(df_len)
    ],
    upper_ci_total_prevalence=[
        round(random.uniform(42.0, 95.0), 2) for _ in range(df_len)
    ],
    n_16_29_years_prevalence=[random.randint(10, 900) for _ in range(df_len)],
    rate_16_29_years_prevalence=[
        round(random.uniform(0.1, 41.0), 2) for _ in range(df_len)
    ],
    lower_ci_16_29_years_prevalence=[
        round(random.uniform(0.1, 41.0), 2) for _ in range(df_len)
    ],
    upper_ci_16_29_years_prevalence=[
        round(random.uniform(42.0, 95.0), 2) for _ in range(df_len)
    ],
    n_30_49_years_prevalence=[random.randint(20, 300) for _ in range(df_len)],
    rate_30_49_years_prevalence=[
        round(random.uniform(0.1, 40.0), 2) for _ in range(df_len)
    ],
    lower_ci_30_49_years_prevalence=[
        round(random.uniform(0.1, 40.0), 2) for _ in range(df_len)
    ],
    upper_ci_30_49_years_prevalence=[
        round(random.uniform(45.0, 95.0), 2) for _ in range(df_len)
    ],
    n_50_64_years_prevalence=[random.randint(30, 400) for _ in range(df_len)],
    rate_50_64_years_prevalence=[
        round(random.uniform(0.1, 40.0), 2) for _ in range(df_len)
    ],
    lower_ci_50_64_years_prevalence=[
        round(random.uniform(0.05, 40.0), 2) for _ in range(df_len)
    ],
    upper_ci_50_64_years_prevalence=[
        round(random.uniform(41.0, 60.0), 2) for _ in range(df_len)
    ],
    n_65_years_prevalence=[random.randint(40, 500) for _ in range(df_len)],
    rate_65_years_prevalence=[
        round(random.uniform(0.1, 25.0), 2) for _ in range(df_len)
    ],
    lower_ci_65_years_prevalence=[
        round(random.uniform(0.08, 40.0), 2) for _ in range(df_len)
    ],
    upper_ci_65_years_prevalence=[
        round(random.uniform(41.0, 80.0), 2) for _ in range(df_len)
    ],
    n_male_prevalence=[random.randint(50, 600) for _ in range(df_len)],
    rate_male_prevalence=[
        round(random.uniform(0.1, 18.0), 2) for _ in range(df_len)
    ],
    lower_ci_male_prevalence=[
        round(random.uniform(0.1, 40.0), 2) for _ in range(df_len)
    ],
    upper_ci_male_prevalence=[
        round(random.uniform(41.0, 85.0), 2) for _ in range(df_len)
    ],
    n_female_prevalence=[random.randint(50, 600) for _ in range(df_len)],
    rate_female_prevalence=[
        round(random.uniform(0.1, 40.0), 2) for _ in range(df_len)
    ],
    lower_ci_female_prevalence=[
        round(random.uniform(0.1, 45.0), 2) for _ in range(df_len)
    ],
    upper_ci_female_prevalence=[
        round(random.uniform(46.0, 95.0), 2) for _ in range(df_len)
    ],
    n_white_prevalence=[random.uniform(0, 100) for _ in range(df_len)],
    rate_white_prevalence=[
        round(random.uniform(0.1, 9), 2) for _ in range(df_len)
    ],
    lower_ci_white_prevalence=[
        round(random.uniform(0.05, 40.0), 2) for _ in range(df_len)
    ],
    upper_ci_white_prevalence=[
        round(random.uniform(41.0, 95), 2) for _ in range(df_len)
    ],
    n_black_prevalence=[random.uniform(0, 100) for _ in range(df_len)],
    rate_black_prevalence=[
        round(random.uniform(0.1, 9), 2) for _ in range(df_len)
    ],
    lower_ci_black_prevalence=[
        round(random.uniform(0.05, 40), 2) for _ in range(df_len)
    ],
    upper_ci_black_prevalence=[
        round(random.uniform(41.0, 95.0), 2) for _ in range(df_len)
    ],
    n_asian_prevalence=[random.uniform(0, 100) for _ in range(df_len)],
    rate_asian_prevalence=[
        round(random.uniform(0.1, 0.9), 2) for _ in range(df_len)
    ],
    lower_ci_asian_prevalence=[
        round(random.uniform(0.05, 40.0), 2) for _ in range(df_len)
    ],
    upper_ci_asian_prevalence=[
        round(random.uniform(40.0, 60.0), 2) for _ in range(df_len)
    ],
    n_hispanic_prevalence=[random.uniform(0, 100) for _ in range(df_len)],
    rate_hispanic_prevalence=[
        round(random.uniform(0.1, 0.9), 2) for _ in range(df_len)
    ],
    lower_ci_hispanic_prevalence=[
        round(random.uniform(0.05, 40.0), 2) for _ in range(df_len)
    ],
    upper_ci_hispanic_prevalence=[
        round(random.uniform(60.0, 95.0), 2) for _ in range(df_len)
    ],
    n_other_race_prevalence=[random.uniform(0, 100) for _ in range(df_len)],
    rate_other_race_prevalence=[
        round(random.uniform(0.1, 0.9), 2) for _ in range(df_len)
    ],
    lower_ci_other_race_prevalence=[
        round(random.uniform(0.05, 25.0), 2) for _ in range(df_len)
    ],
    upper_ci_other_race_prevalence=[
        round(random.uniform(26.0, 50.0), 2) for _ in range(df_len)
    ],
)

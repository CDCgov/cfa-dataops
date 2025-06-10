import pandera.pandas as pa
import pandas as pd
import random
from faker import Faker
import warnings
warnings.filterwarnings("ignore")
fake=Faker()
df_len = 100

extract_schema = pa.DataFrameSchema(
    {
        "site": pa.Column(str),
        "date_range_of_specimen": pa.Column(str, nullable=True),
        "round": pa.Column(int),
        "catchment_fips_code": pa.Column(object),
        "catchment_area_description": pa.Column(str),
        "catchment_population": pa.Column(int),
        "n_0_4_prevalence": pa.Column(float, nullable=True),
        "rate_0_4_prevalence": pa.Column(float, nullable=True),
        "lower_ci_0_4_prevalence": pa.Column(float, nullable=True),
        "upper_ci_0_4_prevalence": pa.Column(float, nullable=True),
        "ci_flag_0_4_prevalence": pa.Column(float, nullable=True),
        "n_5_11_prevalence": pa.Column(float, nullable=True),
        "rate_5_11_prevalence": pa.Column(float, nullable=True),
        "lower_ci_5_11_prevalence": pa.Column(float, nullable=True),
        "upper_ci_5_11_prevalence": pa.Column(float, nullable=True),
        "ci_flag_5_11_prevalence": pa.Column(float, nullable=True),
        "n_0_11_prevalence": pa.Column(float, nullable=True),
        "rate_0_11_prevalence": pa.Column(float, nullable=True),
        "lower_ci_0_11_prevalence": pa.Column(float, nullable=True),
        "upper_ci_0_11_prevalence": pa.Column(float, nullable=True),
        "ci_flag_0_11_prevalence": pa.Column(float, nullable=True),
        "n_12_17_prevalence": pa.Column(float, nullable=True),
        "rate_12_17_prevalence": pa.Column(float, nullable=True),
        "lower_ci_12_17_prevalence": pa.Column(float, nullable=True),
        "upper_ci_12_17_prevalence": pa.Column(float, nullable=True),
        "ci_flag_12_17_prevalence": pa.Column(float, nullable=True),
        "n_0_17_prevalence": pa.Column(float, nullable=True),
        "rate_0_17_prevalence": pa.Column(float, nullable=True),
        "lower_ci_0_17_prevalence": pa.Column(float, nullable=True),
        "upper_ci_0_17_prevalence": pa.Column(float, nullable=True),
        "ci_flag_0_17_prevalence": pa.Column(float, nullable=True),
        "n_18_49_prevalence": pa.Column(float, nullable=True),
        "rate_18_49_prevalence": pa.Column(float, nullable=True),
        "lower_ci_18_49_prevalence": pa.Column(float, nullable=True),
        "upper_ci_18_49_prevalence": pa.Column(float, nullable=True),
        "ci_flag_18_49_prevalence": pa.Column(float, nullable=True),
        "n_50_64_prevalence": pa.Column(float, nullable=True),
        "rate_50_64_prevalence": pa.Column(float, nullable=True),
        "lower_ci_50_64_prevalence": pa.Column(float, nullable=True),
        "upper_ci_50_64_prevalence": pa.Column(float, nullable=True),
        "ci_flag_50_64_prevalence": pa.Column(float, nullable=True),
        "n_65_prevalence": pa.Column(float, nullable=True),
        "rate_65_prevalence": pa.Column(float, nullable=True),
        "lower_ci_65_prevalence": pa.Column(float, nullable=True),
        "upper_ci_65_prevalence": pa.Column(float, nullable=True),
        "ci_flag_65_prevalence": pa.Column(float, nullable=True),
        "n_cumulative_prevalence": pa.Column(float, nullable=True),
        "rate_cumulative_prevalence": pa.Column(float, nullable=True),
        "lower_ci_cumulative_prevalence": pa.Column(float, nullable=True),
        "upper_ci_cumulative_prevalence": pa.Column(float, nullable=True),
        "ci_flag_cumulative_prevalence": pa.Column(float, nullable=True),
        "n_male_prevalence": pa.Column(float, nullable=True),
        "rate_male_prevalence": pa.Column(float, nullable=True),
        "lower_ci_male_prevalence": pa.Column(float, nullable=True),
        "upper_ci_male_prevalence": pa.Column(float, nullable=True),
        "ci_flag_male_prevalence": pa.Column(float, nullable=True),
        "n_female_prevalence": pa.Column(float, nullable=True),
        "rate_female_prevalence": pa.Column(float, nullable=True),
        "lower_ci_female_prevalence": pa.Column(float, nullable=True),
        "upper_ci_female_prevalence": pa.Column(float, nullable=True),
        "ci_flag_female_prevalence": pa.Column(float, nullable=True),
        "n_male_0_17_prevalence": pa.Column(float, nullable=True),
        "rate_male_0_17_prevalence": pa.Column(float, nullable=True),
        "lower_ci_male_0_17_prevalence": pa.Column(float, nullable=True),
        "upper_ci_male_0_17_prevalence": pa.Column(float, nullable=True),
        "ci_flag_male_0_17_prevalence": pa.Column(float, nullable=True),
        "n_female_0_17_prevalence": pa.Column(float, nullable=True),
        "rate_female_0_17_prevalence": pa.Column(float, nullable=True),
        "lower_ci_female_0_17_prevalence": pa.Column(float, nullable=True),
        "upper_ci_female_0_17_prevalence": pa.Column(float, nullable=True),
        "ci_flag_female_0_17_prevalence": pa.Column(float, nullable=True),
        "n_anti_s_0_4_years_prevalence_": pa.Column(float, nullable=True),
        "rate_anti_s_0_4_years_prevalence": pa.Column(float, nullable=True),
        "lower_ci_anti_s_0_4_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "upper_ci_anti_s_0_4_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "ci_flag_anti_s_0_4_years_prevalence": pa.Column(float, nullable=True),
        "n_anti_s_5_11_years_prevalence": pa.Column(float, nullable=True),
        "rate_anti_s_5_11_years_prevalence": pa.Column(float, nullable=True),
        "lower_ci_anti_s_5_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "upper_ci_anti_s_5_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "ci_flag_anti_s_5_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "n_anti_s_0_11_years_prevalence": pa.Column(float, nullable=True),
        "rate_anti_s_0_11_years_prevalence": pa.Column(float, nullable=True),
        "lower_ci_anti_s_0_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "upper_ci_anti_s_0_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "ci_flag_anti_s_0_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "n_anti_s_12_17_years_prevalence": pa.Column(float, nullable=True),
        "rate_anti_s_12_17_years_prevalence": pa.Column(float, nullable=True),
        "lower_ci_anti_s_12_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "upper_ci_anti_s_12_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "ci_flag_anti_s_12_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "n_anti_s_0_17_years_prevalence": pa.Column(float, nullable=True),
        "rate_anti_s_0_17_years_prevalence": pa.Column(float, nullable=True),
        "lower_ci_anti_s_0_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "upper_ci_anti_s_0_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "ci_flag_anti_s_0_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "n_anti_s_male_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "rate_anti_s_male_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "lower_ci_anti_s_male_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "upper_ci_anti_s_male_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "ci_flag_anti_s_male_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "n_anti_s_female_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "rate_anti_s_female_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "lower_ci_anti_s_female_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "upper_ci_anti_s_female_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "ci_flag_anti_s_female_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "n_combined_0_4_years_prevalence": pa.Column(float, nullable=True),
        "rate_combined_0_4_years_prevalence": pa.Column(float, nullable=True),
        "lower_ci_combined_0_4_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "upper_ci_combined_0_4_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "ci_flag_combined_0_4_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "n_combined_5_11_years_prevalence": pa.Column(float, nullable=True),
        "rate_combined_5_11_years_prevalence": pa.Column(float, nullable=True),
        "lower_ci_combined_5_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "upper_ci_combined_5_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "ci_flag_combined_5_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "n_combined_0_11_years_prevalence": pa.Column(float, nullable=True),
        "rate_combined_0_11_years_prevalence": pa.Column(float, nullable=True),
        "lower_ci_combined_0_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "upper_ci_combined_0_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "ci_flag_combined_0_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "n_combined_12_17_years_prevalence": pa.Column(float, nullable=True),
        "rate_combined_12_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "lower_ci_combined_12_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "upper_ci_combined_12_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "ci_flag_combined_12_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "n_combined_0_17_years_prevalence": pa.Column(float, nullable=True),
        "rate_combined_0_17_years_prevalence": pa.Column(float, nullable=True),
        "lower_ci_combined_0_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "upper_ci_combined_0_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "ci_flag_combined_0_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "n_combined_male_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "rate_combined_male_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "lower_ci_combined_male_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "upper_ci_combined_male_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "ci_flag_combined_male_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "n_combined_female_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "rate_combined_female_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "lower_ci_combined_female_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "upper_ci_combined_female_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "ci_flag_combined_female_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "cases_reported_by_date_of_last_specimen_collection": pa.Column(
            float, nullable=True
        ),
        "estimated_cumulative_infections_all_count": pa.Column(
            float, nullable=True
        ),
        "estimated_cumulative_infections_all_lower_ci": pa.Column(
            float, nullable=True
        ),
        "estimated_cumulative_infections_all_upper_ci": pa.Column(
            float, nullable=True
        ),
        "estimated_reported_ratio": pa.Column(float, nullable=True),
        "estimated_reported_lower_ci": pa.Column(float, nullable=True),
        "estimated_reported_upper_ci": pa.Column(float, nullable=True),
        "estimated_cumulative_infections_0_17_count": pa.Column(
            float, nullable=True
        ),
        "estimated_cumulative_infections_0_17_lower_ci": pa.Column(
            float, nullable=True
        ),
        "estimated_cumulative_infections_0_17_upper_ci": pa.Column(
            float, nullable=True
        ),
        "all_age_sex_strata_had_at_least_one_positive": pa.Column(
            bool, nullable=True
        ),
        "all_age_sex_strata_had_at_least_one_negative": pa.Column(
            bool, nullable=True
        ),
        "site_large_ci_flag": pa.Column(bool, nullable=True),
    }
)


load_schema = pa.DataFrameSchema(
    {
        "site": pa.Column(str),
        "date_range_of_specimen": pa.Column(str, nullable=True),
        "round": pa.Column(int),
        "catchment_fips_code": pa.Column(object),
        "catchment_area_description": pa.Column(str),
        "catchment_population": pa.Column(int),
        "n_0_4_prevalence": pa.Column(float, nullable=True),
        "rate_0_4_prevalence": pa.Column(float, nullable=True),
        "lower_ci_0_4_prevalence": pa.Column(float, nullable=True),
        "upper_ci_0_4_prevalence": pa.Column(float, nullable=True),
        "ci_flag_0_4_prevalence": pa.Column(float, nullable=True),
        "n_5_11_prevalence": pa.Column(float, nullable=True),
        "rate_5_11_prevalence": pa.Column(float, nullable=True),
        "lower_ci_5_11_prevalence": pa.Column(float, nullable=True),
        "upper_ci_5_11_prevalence": pa.Column(float, nullable=True),
        "ci_flag_5_11_prevalence": pa.Column(float, nullable=True),
        "n_0_11_prevalence": pa.Column(float, nullable=True),
        "rate_0_11_prevalence": pa.Column(float, nullable=True),
        "lower_ci_0_11_prevalence": pa.Column(float, nullable=True),
        "upper_ci_0_11_prevalence": pa.Column(float, nullable=True),
        "ci_flag_0_11_prevalence": pa.Column(float, nullable=True),
        "n_12_17_prevalence": pa.Column(float, nullable=True),
        "rate_12_17_prevalence": pa.Column(float, nullable=True),
        "lower_ci_12_17_prevalence": pa.Column(float, nullable=True),
        "upper_ci_12_17_prevalence": pa.Column(float, nullable=True),
        "ci_flag_12_17_prevalence": pa.Column(float, nullable=True),
        "n_0_17_prevalence": pa.Column(float, nullable=True),
        "rate_0_17_prevalence": pa.Column(float, nullable=True),
        "lower_ci_0_17_prevalence": pa.Column(float, nullable=True),
        "upper_ci_0_17_prevalence": pa.Column(float, nullable=True),
        "ci_flag_0_17_prevalence": pa.Column(float, nullable=True),
        "n_18_49_prevalence": pa.Column(float, nullable=True),
        "rate_18_49_prevalence": pa.Column(float, nullable=True),
        "lower_ci_18_49_prevalence": pa.Column(float, nullable=True),
        "upper_ci_18_49_prevalence": pa.Column(float, nullable=True),
        "ci_flag_18_49_prevalence": pa.Column(float, nullable=True),
        "n_50_64_prevalence": pa.Column(float, nullable=True),
        "rate_50_64_prevalence": pa.Column(float, nullable=True),
        "lower_ci_50_64_prevalence": pa.Column(float, nullable=True),
        "upper_ci_50_64_prevalence": pa.Column(float, nullable=True),
        "ci_flag_50_64_prevalence": pa.Column(float, nullable=True),
        "n_65_prevalence": pa.Column(float, nullable=True),
        "rate_65_prevalence": pa.Column(float, nullable=True),
        "lower_ci_65_prevalence": pa.Column(float, nullable=True),
        "upper_ci_65_prevalence": pa.Column(float, nullable=True),
        "ci_flag_65_prevalence": pa.Column(float, nullable=True),
        "n_cumulative_prevalence": pa.Column(float, nullable=True),
        "rate_cumulative_prevalence": pa.Column(float, nullable=True),
        "lower_ci_cumulative_prevalence": pa.Column(float, nullable=True),
        "upper_ci_cumulative_prevalence": pa.Column(float, nullable=True),
        "ci_flag_cumulative_prevalence": pa.Column(float, nullable=True),
        "n_male_prevalence": pa.Column(float, nullable=True),
        "rate_male_prevalence": pa.Column(float, nullable=True),
        "lower_ci_male_prevalence": pa.Column(float, nullable=True),
        "upper_ci_male_prevalence": pa.Column(float, nullable=True),
        "ci_flag_male_prevalence": pa.Column(float, nullable=True),
        "n_female_prevalence": pa.Column(float, nullable=True),
        "rate_female_prevalence": pa.Column(float, nullable=True),
        "lower_ci_female_prevalence": pa.Column(float, nullable=True),
        "upper_ci_female_prevalence": pa.Column(float, nullable=True),
        "ci_flag_female_prevalence": pa.Column(float, nullable=True),
        "n_male_0_17_prevalence": pa.Column(float, nullable=True),
        "rate_male_0_17_prevalence": pa.Column(float, nullable=True),
        "lower_ci_male_0_17_prevalence": pa.Column(float, nullable=True),
        "upper_ci_male_0_17_prevalence": pa.Column(float, nullable=True),
        "ci_flag_male_0_17_prevalence": pa.Column(float, nullable=True),
        "n_female_0_17_prevalence": pa.Column(float, nullable=True),
        "rate_female_0_17_prevalence": pa.Column(float, nullable=True),
        "lower_ci_female_0_17_prevalence": pa.Column(float, nullable=True),
        "upper_ci_female_0_17_prevalence": pa.Column(float, nullable=True),
        "ci_flag_female_0_17_prevalence": pa.Column(float, nullable=True),
        "n_anti_s_0_4_years_prevalence_": pa.Column(float, nullable=True),
        "rate_anti_s_0_4_years_prevalence": pa.Column(float, nullable=True),
        "lower_ci_anti_s_0_4_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "upper_ci_anti_s_0_4_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "ci_flag_anti_s_0_4_years_prevalence": pa.Column(float, nullable=True),
        "n_anti_s_5_11_years_prevalence": pa.Column(float, nullable=True),
        "rate_anti_s_5_11_years_prevalence": pa.Column(float, nullable=True),
        "lower_ci_anti_s_5_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "upper_ci_anti_s_5_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "ci_flag_anti_s_5_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "n_anti_s_0_11_years_prevalence": pa.Column(float, nullable=True),
        "rate_anti_s_0_11_years_prevalence": pa.Column(float, nullable=True),
        "lower_ci_anti_s_0_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "upper_ci_anti_s_0_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "ci_flag_anti_s_0_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "n_anti_s_12_17_years_prevalence": pa.Column(float, nullable=True),
        "rate_anti_s_12_17_years_prevalence": pa.Column(float, nullable=True),
        "lower_ci_anti_s_12_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "upper_ci_anti_s_12_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "ci_flag_anti_s_12_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "n_anti_s_0_17_years_prevalence": pa.Column(float, nullable=True),
        "rate_anti_s_0_17_years_prevalence": pa.Column(float, nullable=True),
        "lower_ci_anti_s_0_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "upper_ci_anti_s_0_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "ci_flag_anti_s_0_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "n_anti_s_male_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "rate_anti_s_male_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "lower_ci_anti_s_male_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "upper_ci_anti_s_male_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "ci_flag_anti_s_male_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "n_anti_s_female_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "rate_anti_s_female_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "lower_ci_anti_s_female_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "upper_ci_anti_s_female_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "ci_flag_anti_s_female_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "n_combined_0_4_years_prevalence": pa.Column(float, nullable=True),
        "rate_combined_0_4_years_prevalence": pa.Column(float, nullable=True),
        "lower_ci_combined_0_4_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "upper_ci_combined_0_4_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "ci_flag_combined_0_4_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "n_combined_5_11_years_prevalence": pa.Column(float, nullable=True),
        "rate_combined_5_11_years_prevalence": pa.Column(float, nullable=True),
        "lower_ci_combined_5_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "upper_ci_combined_5_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "ci_flag_combined_5_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "n_combined_0_11_years_prevalence": pa.Column(float, nullable=True),
        "rate_combined_0_11_years_prevalence": pa.Column(float, nullable=True),
        "lower_ci_combined_0_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "upper_ci_combined_0_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "ci_flag_combined_0_11_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "n_combined_12_17_years_prevalence": pa.Column(float, nullable=True),
        "rate_combined_12_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "lower_ci_combined_12_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "upper_ci_combined_12_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "ci_flag_combined_12_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "n_combined_0_17_years_prevalence": pa.Column(float, nullable=True),
        "rate_combined_0_17_years_prevalence": pa.Column(float, nullable=True),
        "lower_ci_combined_0_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "upper_ci_combined_0_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "ci_flag_combined_0_17_years_prevalence": pa.Column(
            float, nullable=True
        ),
        "n_combined_male_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "rate_combined_male_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "lower_ci_combined_male_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "upper_ci_combined_male_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "ci_flag_combined_male_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "n_combined_female_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "rate_combined_female_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "lower_ci_combined_female_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "upper_ci_combined_female_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "ci_flag_combined_female_0_17_prevalence_rounds_31_and_later": pa.Column(
            float, nullable=True
        ),
        "cases_reported_by_date_of_last_specimen_collection": pa.Column(
            float, nullable=True
        ),
        "estimated_cumulative_infections_all_count": pa.Column(
            float, nullable=True
        ),
        "estimated_cumulative_infections_all_lower_ci": pa.Column(
            float, nullable=True
        ),
        "estimated_cumulative_infections_all_upper_ci": pa.Column(
            float, nullable=True
        ),
        "estimated_reported_ratio": pa.Column(float, nullable=True),
        "estimated_reported_lower_ci": pa.Column(float, nullable=True),
        "estimated_reported_upper_ci": pa.Column(float, nullable=True),
        "estimated_cumulative_infections_0_17_count": pa.Column(
            float, nullable=True
        ),
        "estimated_cumulative_infections_0_17_lower_ci": pa.Column(
            float, nullable=True
        ),
        "estimated_cumulative_infections_0_17_upper_ci": pa.Column(
            float, nullable=True
        ),
        "all_age_sex_strata_had_at_least_one_positive": pa.Column(
            bool, nullable=True
        ),
        "all_age_sex_strata_had_at_least_one_negative": pa.Column(
            bool, nullable=True
        ),
        "site_large_ci_flag": pa.Column(bool, coerce=True, nullable=True),
    }
)
site_opts = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID',
 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC',
 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'PR', 'RI', 'SC',
 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY', 'US']
fips_opts = ['Statewide', '50 States, DC, & PR', '50 States & DC']
area_opts = ['Statewide', '50 States, DC, & PR', '50 States & DC']
def gen_date_range(sdt):
    edt = sdt+ pd.Timedelta(days=random.randint(7, 14))
    sd = sdt.strftime("%d")
    sm = sdt.strftime("%b")
    ed = edt.strftime("%d")
    em = edt.strftime("%b")
    ey = edt.strftime("%Y")
    return f"{sm} {sd} - {em} {ed}, {ey}"
raw_synth_data = pd.DataFrame()
raw_synth_data = raw_synth_data.assign(
    site = [random.choice(site_opts) for _ in range(df_len)],
    date_range_of_specimen = [gen_date_range(fake.date_between(start_date='-2y', end_date='today')) for _ in range(df_len)],
    round = [random.randint(1, 50) for _ in range(df_len)],
    catchment_fips_code = [random.choice(fips_opts) for _ in range(df_len)],
    catchment_area_description = [random.choice(area_opts) for _ in range(df_len)],
    catchment_population = [random.randint(1000, 100000) for _ in range(df_len)],
    n_0_4_prevalence = [random.uniform(0, 100) for _ in range(df_len)],
    rate_0_4_prevalence = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_0_4_prevalence = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_0_4_prevalence = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_0_4_prevalence = [random.choice([0.0, 1.0]) for _ in range(df_len)],
    n_5_11_prevalence = [random.uniform(0, 100) for _ in range(df_len)],
    rate_5_11_prevalence = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_5_11_prevalence = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_5_11_prevalence = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_5_11_prevalence = [random.choice([0.0, 1.]) for _ in range(df_len)],
    n_0_11_prevalence = [random.uniform(0, 100) for _ in range(df_len)],
    rate_0_11_prevalence = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_0_11_prevalence = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_0_11_prevalence = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_0_11_prevalence = [random.choice([0.0, 1.0]) for _ in range(df_len)],
    n_12_17_prevalence = [random.uniform(0, 100) for _ in range(df_len)],
    rate_12_17_prevalence = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_12_17_prevalence = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_12_17_prevalence = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_12_17_prevalence = [random.choice([0., 1.]) for _ in range(df_len)],
    n_0_17_prevalence = [random.uniform(0, 100) for _ in range(df_len)],
    rate_0_17_prevalence = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_0_17_prevalence = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_0_17_prevalence = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_0_17_prevalence = [random.choice([0.0, 1.0]) for _ in range(df_len)],
    n_18_49_prevalence = [random.uniform(0, 100) for _ in range(df_len)],
    rate_18_49_prevalence = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_18_49_prevalence = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_18_49_prevalence = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_18_49_prevalence = [random.choice([0., 1.]) for _ in range(df_len)],
    n_50_64_prevalence = [random.uniform(0, 100) for _ in range(df_len)],
    rate_50_64_prevalence = [random.uniform(0, 1) for _ in range(df_len)], 
    lower_ci_50_64_prevalence = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_50_64_prevalence = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_50_64_prevalence = [random.choice([0., 1.]) for _ in range(df_len)], 
    n_65_prevalence = [random.uniform(0, 100) for _ in range(df_len)],
    rate_65_prevalence = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_65_prevalence = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_65_prevalence = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_65_prevalence = [random.choice([0., 1.]) for _ in range(df_len)],
    n_cumulative_prevalence = [random.uniform(0, 100) for _ in range(df_len)],
    rate_cumulative_prevalence = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_cumulative_prevalence = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_cumulative_prevalence = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_cumulative_prevalence = [random.choice([0., 1.]) for _ in range(df_len)],
    n_male_prevalence = [random.uniform(0, 100) for _ in range(df_len)],
    rate_male_prevalence = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_male_prevalence = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_male_prevalence = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_male_prevalence = [random.choice([0., 1.]) for _ in range(df_len)],
    n_female_prevalence = [random.uniform(0, 100) for _ in range(df_len)],
    rate_female_prevalence = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_female_prevalence = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_female_prevalence = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_female_prevalence = [random.choice([0., 1.]) for _ in range(df_len)],
    n_male_0_17_prevalence = [random.uniform(0, 100) for _ in range(df_len)],
    rate_male_0_17_prevalence = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_male_0_17_prevalence = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_male_0_17_prevalence = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_male_0_17_prevalence = [random.choice([0., 1.]) for _ in range(df_len)],
    n_female_0_17_prevalence = [random.uniform(0, 100) for _ in range(df_len)],
    rate_female_0_17_prevalence = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_female_0_17_prevalence = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_female_0_17_prevalence = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_female_0_17_prevalence = [random.choice([0., 1.]) for _ in range(df_len)],
    n_anti_s_0_4_years_prevalence_ = [random.uniform(0, 100) for _ in range(df_len)],
    rate_anti_s_0_4_years_prevalence = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_anti_s_0_4_years_prevalence = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_anti_s_0_4_years_prevalence = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_anti_s_0_4_years_prevalence = [random.choice([0., 1.]) for _ in range(df_len)],
    n_anti_s_5_11_years_prevalence = [random.uniform(0, 100) for _ in range(df_len)],
    rate_anti_s_5_11_years_prevalence = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_anti_s_5_11_years_prevalence = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_anti_s_5_11_years_prevalence = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_anti_s_5_11_years_prevalence = [random.choice([0., 1.]) for _ in range(df_len)],
    n_anti_s_0_11_years_prevalence = [random.uniform(0, 100) for _ in range(df_len)],
    rate_anti_s_0_11_years_prevalence = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_anti_s_0_11_years_prevalence = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_anti_s_0_11_years_prevalence = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_anti_s_0_11_years_prevalence = [random.choice([0., 1.]) for _ in range(df_len)],
    n_anti_s_12_17_years_prevalence = [random.uniform(0, 100) for _ in range(df_len)],
    rate_anti_s_12_17_years_prevalence = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_anti_s_12_17_years_prevalence = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_anti_s_12_17_years_prevalence = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_anti_s_12_17_years_prevalence = [random.choice([0., 1.]) for _ in range(df_len)],
    n_anti_s_0_17_years_prevalence = [random.uniform(0, 100) for _ in range(df_len)],
    rate_anti_s_0_17_years_prevalence = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_anti_s_0_17_years_prevalence = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_anti_s_0_17_years_prevalence = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_anti_s_0_17_years_prevalence = [random.choice([0., 1.]) for _ in range(df_len)],
    n_anti_s_male_0_17_prevalence_rounds_31_and_later = [random.uniform(0, 100) for _ in range(df_len)],
    rate_anti_s_male_0_17_prevalence_rounds_31_and_later = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_anti_s_male_0_17_prevalence_rounds_31_and_later = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_anti_s_male_0_17_prevalence_rounds_31_and_later = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_anti_s_male_0_17_prevalence_rounds_31_and_later = [random.choice([0., 1.]) for _ in range(df_len)],
    n_anti_s_female_0_17_prevalence_rounds_31_and_later = [random.uniform(0, 100) for _ in range(df_len)],
    rate_anti_s_female_0_17_prevalence_rounds_31_and_later = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_anti_s_female_0_17_prevalence_rounds_31_and_later = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_anti_s_female_0_17_prevalence_rounds_31_and_later = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_anti_s_female_0_17_prevalence_rounds_31_and_later = [random.choice([0., 1.]) for _ in range(df_len)],
    n_combined_0_4_years_prevalence = [random.uniform(0, 100) for _ in range(df_len)],
    rate_combined_0_4_years_prevalence = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_combined_0_4_years_prevalence = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_combined_0_4_years_prevalence = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_combined_0_4_years_prevalence = [random.choice([0., 1.]) for _ in range(df_len)],
    n_combined_5_11_years_prevalence = [random.uniform(0, 100) for _ in range(df_len)],
    rate_combined_5_11_years_prevalence = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_combined_5_11_years_prevalence = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_combined_5_11_years_prevalence = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_combined_5_11_years_prevalence = [random.choice([0., 1.]) for _ in range(df_len)],
    n_combined_0_11_years_prevalence = [random.uniform(0, 100) for _ in range(df_len)],
    rate_combined_0_11_years_prevalence = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_combined_0_11_years_prevalence = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_combined_0_11_years_prevalence = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_combined_0_11_years_prevalence = [random.choice([0., 1.]) for _ in range(df_len)],
    n_combined_12_17_years_prevalence = [random.uniform(0, 100) for _ in range(df_len)],
    rate_combined_12_17_years_prevalence = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_combined_12_17_years_prevalence = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_combined_12_17_years_prevalence = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_combined_12_17_years_prevalence = [random.choice([0., 1.]) for _ in range(df_len)],
    n_combined_0_17_years_prevalence = [random.uniform(0, 100) for _ in range(df_len)],
    rate_combined_0_17_years_prevalence = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_combined_0_17_years_prevalence = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_combined_0_17_years_prevalence = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_combined_0_17_years_prevalence = [random.choice([0., 1.]) for _ in range(df_len)],
    n_combined_male_0_17_prevalence_rounds_31_and_later = [random.uniform(0, 100) for _ in range(df_len)],
    rate_combined_male_0_17_prevalence_rounds_31_and_later = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_combined_male_0_17_prevalence_rounds_31_and_later = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_combined_male_0_17_prevalence_rounds_31_and_later = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_combined_male_0_17_prevalence_rounds_31_and_later = [random.choice([0., 1.]) for _ in range(df_len)],
    n_combined_female_0_17_prevalence_rounds_31_and_later = [random.uniform(0, 100) for _ in range(df_len)],
    rate_combined_female_0_17_prevalence_rounds_31_and_later = [random.uniform(0, 1) for _ in range(df_len)],
    lower_ci_combined_female_0_17_prevalence_rounds_31_and_later = [random.uniform(0, 50) for _ in range(df_len)],
    upper_ci_combined_female_0_17_prevalence_rounds_31_and_later = [random.uniform(50, 100) for _ in range(df_len)],
    ci_flag_combined_female_0_17_prevalence_rounds_31_and_later = [random.choice([0., 1.]) for _ in range(df_len)],
    cases_reported_by_date_of_last_specimen_collection = [random.uniform(0, 1000) for _ in range(df_len)],
    estimated_cumulative_infections_all_count = [random.uniform(0, 10000) for _ in range(df_len)],
    estimated_cumulative_infections_all_lower_ci = [random.uniform(0, 5000) for _ in range(df_len)],
    estimated_cumulative_infections_all_upper_ci = [random.uniform(5000, 10000) for _ in range(df_len)],
    estimated_reported_ratio = [random.uniform(0, 1) for _ in range(df_len)],
    estimated_reported_lower_ci = [random.uniform(0, 0.5) for _ in range(df_len)],
    estimated_reported_upper_ci = [random.uniform(0.5, 1) for _ in range(df_len)],
    estimated_cumulative_infections_0_17_count = [random.uniform(0, 5000) for _ in range(df_len)],
    estimated_cumulative_infections_0_17_lower_ci = [random.uniform(0, 2500) for _ in range(df_len)],
    estimated_cumulative_infections_0_17_upper_ci = [random.uniform(2500, 5000) for _ in range(df_len)],
    all_age_sex_strata_had_at_least_one_positive = [random.choice([True, False]) for _ in range(df_len)],
    all_age_sex_strata_had_at_least_one_negative = [random.choice([True, False]) for _ in range(df_len)],
    site_large_ci_flag = [random.choice([True, False]) for _ in range(df_len)],
)

tf_synth_data = raw_synth_data.copy()

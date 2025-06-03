import pandera.pandas as pa

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
        "site_large_ci_flag": pa.Column(object, nullable=True),
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
        "site_large_ci_flag": pa.Column(bool, nullable=True),
    }
)

WITH temp as (
    SELECT vaccine,
           geographic_level,
           geographic_name,
           demographic_level,
           demographic_name,
           indicator_label,
           indicator_category_label,
           month_week,
           week_ending,
           estimate,
           ci_half_width_95pct,
           unweighted_sample_size,
           LEFT(current_season_week_ending, 10) as date,
           covid_season,
           suppression_flag
    FROM ${data_source}
)
SELECT vaccine, geographic_level, geographic_name,
demographic_level, demographic_name,
indicator_label, indicator_category_label, month_week, week_ending,
estimate, ci_half_width_95pct, unweighted_sample_size, covid_season, suppression_flag,
    LEFT(date, 10) as date,
    CASE
        WHEN covid_season = '2023-2024' THEN LEFT(CAST((strptime(date, '%Y-%m-%d') - INTERVAL 21 DAYS)::DATE AS VARCHAR), 10)
        ELSE LEFT(CAST(strptime(date, '%Y-%m-%d')::DATE AS VARCHAR), 10)
    END as date1
FROM temp

WITH binned_and_filtered AS (
    SELECT
        date,
        location as state,
        CASE
            WHEN demographic_category in ('Ages_<5yrs', 'Ages_5-11_yrs', 'Ages_12-17_yrs') THEN '0-17'
            WHEN demographic_category in ('Ages_18-24_yrs', 'Ages_25-49_yrs') THEN '18-49'
            WHEN demographic_category = 'Ages_50-64_yrs' THEN '50-64'
            WHEN demographic_category = 'Ages_65+_yrs' THEN '65+'
            ELSE null
        END as age,
        census,
        administered_dose1 as dose_1,
        series_complete_yes as dose_2,
        booster_doses as dose_3
        -- , administered_dose1_pct_agegroup as dose_1_percent,
        -- series_complete_pop_pct_agegroup as dose_2_percent,
        -- booster_doses_vax_pct_agegroup as dose_3_percent
    FROM ${data_source}
    WHERE demographic_category in (
        'Ages_<5yrs', 'Ages_5-11_yrs', 'Ages_12-17_yrs',
        'Ages_18-24_yrs', 'Ages_25-49_yrs', 'Ages_50-64_yrs',
        'Ages_65+_yrs'
    )
)
SELECT
    date,
    state,
    age,
    SUM(census)::INT as census,
    Array [SUM(dose_1)::INT, SUM(dose_2)::INT, SUM(dose_3)::INT] as total,
    Array [
        SUM(dose_1)::DOUBLE / SUM(census)::DOUBLE,
        SUM(dose_2)::DOUBLE / SUM(census)::DOUBLE,
        SUM(dose_3)::DOUBLE / SUM(census)::DOUBLE
    ] as percentage,
    Array [1, 2, 3] as dose
FROM binned_and_filtered
GROUP BY 1, 2, 3

WITH temp as (
    SELECT
        CASE
            WHEN CAST(Week_ending as DATE) <= CAST('2024-08-01' as DATE)
                THEN LEFT(CAST((strptime(Week_ending, '%Y-%m-%d') + INTERVAL 365 DAYS)::DATE AS VARCHAR), 10)
            ELSE Week_ending
            END as date,
        geographic_name,
       Demographic_Level as demographic_level,
       demographic_name,
        Indicator_category_label as indicator_category_label,
       CASE
            WHEN CAST(Week_ending as DATE) <= CAST('2024-08-01' as DATE) THEN '2023-2024'
            ELSE '2024-2025'
        END as covid_season,
        Estimate as estimate
    FROM df
    WHERE Estimate IS NOT NULL
)
SELECT
    date,
    demographic_level,
    indicator_category_label,
    '0-17' as demographic_name,
    SUM(COALESCE(estimate, 0))*100 AS estimate,
    covid_season,
    CASE
        WHEN CAST(date as DATE) <= CAST('2024-08-01' as DATE)  THEN LEFT(CAST((strptime(date, '%Y-%m-%d') + INTERVAL 7 DAYS)::DATE AS VARCHAR), 10)
        ELSE date
        END as date1
FROM temp
WHERE geographic_name = 'National'
    AND demographic_level = 'Overall'
    AND indicator_category_label = 'Vaccinated'
GROUP BY date, demographic_level, indicator_category_label, demographic_name, covid_season
ORDER BY DATE asc, covid_season asc

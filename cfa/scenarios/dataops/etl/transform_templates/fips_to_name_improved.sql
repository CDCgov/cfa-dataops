WITH region_data AS (
    SELECT 'USA' AS region, 'US' AS states
    UNION SELECT region, states FROM ${data_source}

),
fips_data AS (
    SELECT * FROM ${fips}
)
SELECT fips_data.stname, fips_data.st, fips_data.stusps, region_data.region FROM fips_data
LEFT JOIN
region_data
ON
fips_data.stusps = region_data.states
ORDER BY fips_data.stname

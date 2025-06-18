WITH data AS (
    SELECT "state_name" AS stname,
        "state_fipcode" as st,
        "state_code" as stusps
    FROM ${data_source}
    )
SELECT 'United States' AS stname,
    'US' AS st,
    'US' AS stusps
UNION
SELECT DISTINCT stname, st, stusps
FROM data
WHERE stname <> 'Puerto Rico'
ORDER BY stname

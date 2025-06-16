WITH tmp as (
    SELECT LEFT(week_end_date, 10) as date, 
    REPLACE(jurisdiction, 'USA', 'US') as state, 
    total_admissions_all_covid_confirmed as total
    FROM ${data_source}
)
SELECT tmp.date as "date", tmp.state, CAST(tmp.total as int) as total, LOWER(REPLACE(ri.stname, ' ', '_')) as stname 
FROM tmp
RIGHT JOIN ${region_id} as ri
ON tmp.state = ri.stusps
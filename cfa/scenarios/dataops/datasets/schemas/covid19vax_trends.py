import pandera.pandas as pa

extract_schema = pa.DataFrameSchema({
        "date": pa.Column(str),
        "location": pa.Column(str),
        "demographic_category": pa.Column(str),
        "census": pa.Column(float, nullable = True),
        "administered_dose1":  pa.Column(float, nullable = True),
        "series_complete_yes": pa.Column(float, nullable = True),
        "booster_doses": pa.Column(float, nullable = True),
        "second_booster": pa.Column(float, nullable = True),
        "administered_dose1_pct_agegroup": pa.Column(float, nullable = True),
        "series_complete_pop_pct_agegroup": pa.Column(float, nullable = True),
        "booster_doses_vax_pct_agegroup": pa.Column(float, nullable = True),
        "second_booster_vax_pct_agegroup": pa.Column(float, nullable = True)
    })

load_schema = pa.DataFrameSchema({
    "date": pa.Column(str),
    "state": pa.Column(str),
    "age": pa.Column(object),
    "census": pa.Column(int, coerce = True),
    "total": pa.Column(object),
    "percentage": pa.Column(object),
    "dose": pa.Column(object)
    })
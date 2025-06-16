import random

import pandas as pd
import pandera.pandas as pa
from faker import Faker

fake = Faker()
df_len = 1000

extract_schema = pa.DataFrameSchema(
    {
        "week_end_date": pa.Column(str),
        "jurisdiction": pa.Column(str),
        "weekly_actual_days_reporting_any_data": pa.Column(float, nullable=True, coerce = True),
        "weekly_percent_days_reporting_any_data": pa.Column(float, nullable=True, coerce = True),
        "num_hospitals_previous_day_admission_adult_covid_confirmed": pa.Column('int', nullable=True),
        "num_hospitals_previous_day_admission_pediatric_covid_confirmed": pa.Column('int', nullable=True),
        "num_hospitals_previous_day_admission_influenza_confirmed": pa.Column('int', nullable=True),
        "num_hospitals_total_patients_hospitalized_confirmed_influenza": pa.Column('int', nullable=True),
        "num_hospitals_icu_patients_confirmed_influenza": pa.Column('int', nullable=True),
        "num_hospitals_inpatient_beds":  pa.Column('int', nullable=True),
        "num_hospitals_total_icu_beds": pa.Column('int', nullable=True),
        "num_hospitals_inpatient_beds_used": pa.Column('int', nullable=True),
        "num_hospitals_icu_beds_used": pa.Column('int', nullable=True),
        "num_hospitals_percent_inpatient_beds_occupied": pa.Column('int', nullable=True),
        "num_hospitals_percent_staff_icu_beds_occupied": pa.Column('int', nullable=True),
        "num_hospitals_percent_inpatient_beds_covid": pa.Column('int', nullable=True),
        "num_hospitals_percent_inpatient_beds_influenza": pa.Column('int', nullable=True),
        "num_hospitals_percent_staff_icu_beds_covid":  pa.Column('int', nullable=True),
        "num_hospitals_percent_icu_beds_influenza":  pa.Column('int', nullable=True),
        "num_hospitals_admissions_all_covid_confirmed": pa.Column('int', nullable=True),
        "num_hospitals_total_patients_hospitalized_covid_confirmed": pa.Column('int', nullable=True),
        "num_hospitals_staff_icu_patients_covid_confirmed": pa.Column('int', nullable=True),
        "avg_admissions_adult_covid_confirmed": pa.Column('float', nullable=True),
        "total_admissions_adult_covid_confirmed": pa.Column('float', nullable=True),
        "avg_admissions_pediatric_covid_confirmed": pa.Column('float', nullable=True),
        "total_admissions_pediatric_covid_confirmed": pa.Column('float', nullable=True),
        "avg_admissions_all_covid_confirmed": pa.Column('float', nullable=True),
        "total_admissions_all_covid_confirmed": pa.Column('float', nullable=True),
        "avg_admissions_all_influenza_confirmed": pa.Column('float', nullable=True),
        "total_admissions_all_influenza_confirmed": pa.Column('float', nullable=True),
        "avg_total_patients_hospitalized_covid_confirmed": pa.Column('float', nullable=True),
        "avg_total_patients_hospitalized_influenza_confirmed": pa.Column('float', nullable=True),
        "avg_staff_icu_patients_covid_confirmed": pa.Column('float', nullable=True),
        "avg_icu_patients_influenza_confirmed": pa.Column('float', nullable=True),
        "avg_inpatient_beds": pa.Column('float', nullable=True),
        "avg_total_icu_beds": pa.Column('float', nullable=True),
        "avg_inpatient_beds_used": pa.Column('float', nullable=True),
        "avg_icu_beds_used": pa.Column('float', nullable=True),
        "avg_percent_inpatient_beds_occupied": pa.Column('float', nullable=True),
        "avg_percent_staff_icu_beds_occupied": pa.Column('float', nullable=True),
        "avg_percent_inpatient_beds_covid": pa.Column('float', nullable=True),
        "avg_percent_inpatient_beds_influenza": pa.Column('float', nullable=True),
        "avg_percent_staff_icu_beds_covid": pa.Column('float', nullable=True),
        "avg_percent_icu_beds_influenza": pa.Column('float', nullable=True),
        "percent_adult_covid_admissions": pa.Column('float', nullable=True),
        "percent_pediatric_covid_admissions": pa.Column('float', nullable=True),
        "percent_hospitals_previous_day_admission_adult_covid_confirmed": pa.Column('float', nullable=True),
        "percent_hospitals_previous_day_admission_pediatric_covid_confirmed": pa.Column('float', nullable=True),
        "percent_hospitals_previous_day_admission_influenza_confirmed": pa.Column('float', nullable=True),
        "percent_hospitals_total_patients_hospitalized_confirmed_influenza":  pa.Column('float', nullable=True),
        "percent_hospitals_icu_patients_confirmed_influenza": pa.Column('float', nullable=True),
        "percent_hospitals_inpatient_beds":  pa.Column('float', nullable=True),
        "percent_hospitals_total_icu_beds": pa.Column('float', nullable=True),
        "percent_hospitals_inpatient_beds_used": pa.Column('float', nullable=True),
        "percent_hospitals_icu_beds_used": pa.Column('float', nullable=True),
        "percent_hospitals_percent_inpatient_beds_occupied": pa.Column('float', nullable=True),
        "percent_hospitals_percent_staff_icu_beds_occupied": pa.Column('float', nullable=True),
        "percent_hospitals_percent_inpatient_beds_covid": pa.Column('float', nullable=True),
        "percent_hospitals_percent_inpatient_beds_influenza": pa.Column('float', nullable=True),
        "percent_hospitals_percent_staff_icu_beds_covid": pa.Column('float', nullable=True),
        "percent_hospitals_percent_icu_beds_influenza": pa.Column('float', nullable=True),
        "percent_hospitals_admissions_all_covid_confirmed": pa.Column('float', nullable=True),
        "percent_hospitals_total_patients_hospitalized_covid_confirmed": pa.Column('float', nullable=True),
        "percent_hospitals_staff_icu_patients_covid_confirmed": pa.Column('float', nullable=True),
        "abs_chg_percent_hospitals_previous_day_admission_adult_covid_confirmed": pa.Column('float', nullable=True),
        "abs_chg_percent_hospitals_previous_day_admission_pediatric_covid_confirmed": pa.Column('float', nullable=True),
        "abs_chg_percent_hospitals_previous_day_admission_influenza_confirmed": pa.Column('float', nullable=True),
        "abs_chg_percent_hospitals_total_patients_hospitalized_confirmed_influenza": pa.Column('float', nullable=True),
        "abs_chg_percent_hospitals_icu_patients_confirmed_influenza": pa.Column('float', nullable=True),
        "abs_chg_percent_hospitals_inpatient_beds": pa.Column('float', nullable=True),
        "abs_chg_percent_hospitals_total_icu_beds": pa.Column('float', nullable=True),
        "abs_chg_percent_hospitals_inpatient_beds_used": pa.Column('float', nullable=True),
        "abs_chg_percent_hospitals_icu_beds_used": pa.Column('float', nullable=True),
        "abs_chg_percent_hospitals_percent_inpatient_beds_occupied": pa.Column('float', nullable=True),
        "abs_chg_percent_hospitals_percent_staff_icu_beds_occupied": pa.Column('float', nullable=True),
        "abs_chg_percent_hospitals_percent_inpatient_beds_covid": pa.Column('float', nullable=True),
        "abs_chg_percent_hospitals_percent_inpatient_beds_influenza": pa.Column('float', nullable=True),
        "abs_chg_percent_hospitals_percent_staff_icu_beds_covid": pa.Column('float', nullable=True),
        "abs_chg_percent_hospitals_percent_icu_beds_influenza": pa.Column('float', nullable=True),
        "abs_chg_percent_hospitals_admissions_all_covid_confirmed": pa.Column('float', nullable=True),
        "abs_chg_percent_hospitals_total_patients_hospitalized_covid_confirmed": pa.Column('float', nullable=True),
        "abs_chg_percent_hospitals_staff_icu_patients_covid_confirmed": pa.Column('float', nullable=True)
    }
)

load_schema = pa.DataFrameSchema({
    "date": pa.Column(str, coerce=True),
    "state": pa.Column(str, coerce=True),
    "total": pa.Column(str, coerce = True, nullable = True),
    "stname" : pa.Column(str, coerce=True),
})


raw_synth_data = pd.DataFrame({})

stname_tf = {
    "CA": "california",
    "TX": "texas",
    "NY": "new_york",
    "FL": "florida",
    "IL": "illinois"
}
tf_synth_data = pd.DataFrame({
    "date": [fake.date_this_year() for _ in range(df_len)],
    "state": [random.choice(["CA", "TX", "NY", "FL", "IL"]) for _ in range(df_len)],
    "total": [random.randint(0, 1000) for _ in range(df_len)],
    "stname": [fake.state() for _ in range(df_len)],
})
tf_synth_data["stname"] = tf_synth_data["state"].map(stname_tf)
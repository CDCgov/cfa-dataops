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
        "state": pa.Column(str),
        "date": pa.Column(str),
        "critical_staffing_shortage_today_yes": pa.Column(int),
        "critical_staffing_shortage_today_no": pa.Column(int),
        "critical_staffing_shortage_today_not_reported": pa.Column(int),
        "critical_staffing_shortage_anticipated_within_week_yes": pa.Column(
            int
        ),
        "critical_staffing_shortage_anticipated_within_week_no": pa.Column(
            int
        ),
        "critical_staffing_shortage_anticipated_within_week_not_reported": pa.Column(
            int
        ),
        "hospital_onset_covid": pa.Column(float, nullable=True),
        "hospital_onset_covid_coverage": pa.Column(int),
        "inpatient_beds": pa.Column(float, nullable=True),
        "inpatient_beds_coverage": pa.Column(int),
        "inpatient_beds_used": pa.Column(float, nullable=True),
        "inpatient_beds_used_coverage": pa.Column(int),
        "inpatient_beds_used_covid": pa.Column(float, nullable=True),
        "inpatient_beds_used_covid_coverage": pa.Column(int),
        "previous_day_admission_adult_covid_confirmed": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_confirmed_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_suspected": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_suspected_coverage": pa.Column(
            int
        ),
        "previous_day_admission_pediatric_covid_confirmed": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_pediatric_covid_confirmed_coverage": pa.Column(
            int
        ),
        "previous_day_admission_pediatric_covid_suspected": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_pediatric_covid_suspected_coverage": pa.Column(
            int
        ),
        "staffed_adult_icu_bed_occupancy": pa.Column(float, nullable=True),
        "staffed_adult_icu_bed_occupancy_coverage": pa.Column(int),
        "staffed_icu_adult_patients_confirmed_and_suspected_covid": pa.Column(
            float, nullable=True
        ),
        "staffed_icu_adult_patients_confirmed_and_suspected_covid_coverage": pa.Column(
            int
        ),
        "staffed_icu_adult_patients_confirmed_covid": pa.Column(
            float, nullable=True
        ),
        "staffed_icu_adult_patients_confirmed_covid_coverage": pa.Column(int),
        "total_adult_patients_hospitalized_confirmed_and_suspected_covid": pa.Column(
            float, nullable=True
        ),
        "total_adult_patients_hospitalized_confirmed_and_suspected_covid_coverage": pa.Column(
            int
        ),
        "total_adult_patients_hospitalized_confirmed_covid": pa.Column(
            float, nullable=True
        ),
        "total_adult_patients_hospitalized_confirmed_covid_coverage": pa.Column(
            int
        ),
        "total_pediatric_patients_hospitalized_confirmed_and_suspected_covid": pa.Column(
            float, nullable=True
        ),
        "total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_coverage": pa.Column(
            int
        ),
        "total_pediatric_patients_hospitalized_confirmed_covid": pa.Column(
            float, nullable=True
        ),
        "total_pediatric_patients_hospitalized_confirmed_covid_coverage": pa.Column(
            int
        ),
        "total_staffed_adult_icu_beds": pa.Column(float, nullable=True),
        "total_staffed_adult_icu_beds_coverage": pa.Column(int),
        "inpatient_beds_utilization": pa.Column(float, nullable=True),
        "inpatient_beds_utilization_coverage": pa.Column(float, nullable=True),
        "inpatient_beds_utilization_numerator": pa.Column(
            float, nullable=True
        ),
        "inpatient_beds_utilization_denominator": pa.Column(
            float, nullable=True
        ),
        "percent_of_inpatients_with_covid": pa.Column(float, nullable=True),
        "percent_of_inpatients_with_covid_coverage": pa.Column(
            float, nullable=True
        ),
        "percent_of_inpatients_with_covid_numerator": pa.Column(
            float, nullable=True
        ),
        "percent_of_inpatients_with_covid_denominator": pa.Column(
            float, nullable=True
        ),
        "inpatient_bed_covid_utilization": pa.Column(float, nullable=True),
        "inpatient_bed_covid_utilization_coverage": pa.Column(
            float, nullable=True
        ),
        "inpatient_bed_covid_utilization_numerator": pa.Column(
            float, nullable=True
        ),
        "inpatient_bed_covid_utilization_denominator": pa.Column(
            float, nullable=True
        ),
        "adult_icu_bed_covid_utilization": pa.Column(float, nullable=True),
        "adult_icu_bed_covid_utilization_coverage": pa.Column(
            float, nullable=True
        ),
        "adult_icu_bed_covid_utilization_numerator": pa.Column(
            float, nullable=True
        ),
        "adult_icu_bed_covid_utilization_denominator": pa.Column(
            float, nullable=True
        ),
        "adult_icu_bed_utilization": pa.Column(float, nullable=True),
        "adult_icu_bed_utilization_coverage": pa.Column(float, nullable=True),
        "adult_icu_bed_utilization_numerator": pa.Column(float, nullable=True),
        "adult_icu_bed_utilization_denominator": pa.Column(
            float, nullable=True
        ),
        "geocoded_state": pa.Column(str, nullable=True),
        "previous_day_admission_adult_covid_confirmed_18_19": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_confirmed_18_19_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_confirmed_20_29": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_confirmed_20_29_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_confirmed_30_39": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_confirmed_30_39_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_confirmed_40_49": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_confirmed_40_49_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_confirmed_50_59": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_confirmed_50_59_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_confirmed_60_69": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_confirmed_60_69_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_confirmed_70_79": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_confirmed_70_79_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_confirmed_80": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_confirmed_80_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_confirmed_unknown": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_confirmed_unknown_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_suspected_18_19": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_suspected_18_19_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_suspected_20_29": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_suspected_20_29_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_suspected_30_39": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_suspected_30_39_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_suspected_40_49": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_suspected_40_49_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_suspected_50_59": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_suspected_50_59_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_suspected_60_69": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_suspected_60_69_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_suspected_70_79": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_suspected_70_79_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_suspected_80_": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_suspected_80_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_suspected_unknown": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_suspected_unknown_coverage": pa.Column(
            int
        ),
        "deaths_covid": pa.Column(float, nullable=True),
        "deaths_covid_coverage": pa.Column(int, nullable=True),
        "on_hand_supply_therapeutic_a_casirivimab_imdevimab_courses": pa.Column(
            float, nullable=True
        ),
        "on_hand_supply_therapeutic_b_bamlanivimab_courses": pa.Column(
            float, nullable=True
        ),
        "on_hand_supply_therapeutic_c_bamlanivimab_etesevimab_courses": pa.Column(
            float, nullable=True
        ),
        "previous_week_therapeutic_a_casirivimab_imdevimab_courses_used": pa.Column(
            float, nullable=True
        ),
        "previous_week_therapeutic_b_bamlanivimab_courses_used": pa.Column(
            float, nullable=True
        ),
        "previous_week_therapeutic_c_bamlanivimab_etesevimab_courses_used": pa.Column(
            float, nullable=True
        ),
        "icu_patients_confirmed_influenza": pa.Column(float, nullable=True),
        "icu_patients_confirmed_influenza_coverage": pa.Column(int),
        "previous_day_admission_influenza_confirmed": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_influenza_confirmed_coverage": pa.Column(int),
        "previous_day_deaths_covid_and_influenza": pa.Column(
            float, nullable=True
        ),
        "previous_day_deaths_covid_and_influenza_coverage": pa.Column(int),
        "previous_day_deaths_influenza": pa.Column(float, nullable=True),
        "previous_day_deaths_influenza_coverage": pa.Column(int),
        "total_patients_hospitalized_confirmed_influenza": pa.Column(
            float, nullable=True
        ),
        "total_patients_hospitalized_confirmed_influenza_and_covid": pa.Column(
            float, nullable=True
        ),
        "total_patients_hospitalized_confirmed_influenza_and_covid_coverage": pa.Column(
            int
        ),
        "total_patients_hospitalized_confirmed_influenza_coverage": pa.Column(
            int
        ),
        "all_pediatric_inpatient_bed_occupied": pa.Column(
            float, nullable=True
        ),
        "all_pediatric_inpatient_bed_occupied_coverage": pa.Column(int),
        "all_pediatric_inpatient_beds": pa.Column(float, nullable=True),
        "all_pediatric_inpatient_beds_coverage": pa.Column(int),
        "previous_day_admission_pediatric_covid_confirmed_0_4": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_pediatric_covid_confirmed_0_4_coverage": pa.Column(
            int, nullable=True
        ),
        "previous_day_admission_pediatric_covid_confirmed_12_17": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_pediatric_covid_confirmed_12_17_coverage": pa.Column(
            int
        ),
        "previous_day_admission_pediatric_covid_confirmed_5_11": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_pediatric_covid_confirmed_5_11_coverage": pa.Column(
            int
        ),
        "previous_day_admission_pediatric_covid_confirmed_unknown": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_pediatric_covid_confirmed_unknown_coverage": pa.Column(
            int
        ),
        "staffed_icu_pediatric_patients_confirmed_covid": pa.Column(
            float, nullable=True
        ),
        "staffed_icu_pediatric_patients_confirmed_covid_coverage": pa.Column(
            int
        ),
        "staffed_pediatric_icu_bed_occupancy": pa.Column(float, nullable=True),
        "staffed_pediatric_icu_bed_occupancy_coverage": pa.Column(int),
        "total_staffed_pediatric_icu_beds": pa.Column(float, nullable=True),
        "total_staffed_pediatric_icu_beds_coverage": pa.Column(int),
    }
)

load_schema = pa.DataFrameSchema(
    {
        "state": pa.Column(str),
        "date": pa.Column(str),
        "critical_staffing_shortage_today_yes": pa.Column(int),
        "critical_staffing_shortage_today_no": pa.Column(int),
        "critical_staffing_shortage_today_not_reported": pa.Column(int),
        "critical_staffing_shortage_anticipated_within_week_yes": pa.Column(
            int
        ),
        "critical_staffing_shortage_anticipated_within_week_no": pa.Column(
            int
        ),
        "critical_staffing_shortage_anticipated_within_week_not_reported": pa.Column(
            int
        ),
        "hospital_onset_covid": pa.Column(float, nullable=True),
        "hospital_onset_covid_coverage": pa.Column(int),
        "inpatient_beds": pa.Column(float, nullable=True),
        "inpatient_beds_coverage": pa.Column(int),
        "inpatient_beds_used": pa.Column(float, nullable=True),
        "inpatient_beds_used_coverage": pa.Column(int),
        "inpatient_beds_used_covid": pa.Column(float, nullable=True),
        "inpatient_beds_used_covid_coverage": pa.Column(int),
        "previous_day_admission_adult_covid_confirmed": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_confirmed_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_suspected": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_suspected_coverage": pa.Column(
            int
        ),
        "previous_day_admission_pediatric_covid_confirmed": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_pediatric_covid_confirmed_coverage": pa.Column(
            int
        ),
        "previous_day_admission_pediatric_covid_suspected": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_pediatric_covid_suspected_coverage": pa.Column(
            int
        ),
        "staffed_adult_icu_bed_occupancy": pa.Column(float, nullable=True),
        "staffed_adult_icu_bed_occupancy_coverage": pa.Column(int),
        "staffed_icu_adult_patients_confirmed_and_suspected_covid": pa.Column(
            float, nullable=True
        ),
        "staffed_icu_adult_patients_confirmed_and_suspected_covid_coverage": pa.Column(
            int
        ),
        "staffed_icu_adult_patients_confirmed_covid": pa.Column(
            float, nullable=True
        ),
        "staffed_icu_adult_patients_confirmed_covid_coverage": pa.Column(int),
        "total_adult_patients_hospitalized_confirmed_and_suspected_covid": pa.Column(
            float, nullable=True
        ),
        "total_adult_patients_hospitalized_confirmed_and_suspected_covid_coverage": pa.Column(
            int
        ),
        "total_adult_patients_hospitalized_confirmed_covid": pa.Column(
            float, nullable=True
        ),
        "total_adult_patients_hospitalized_confirmed_covid_coverage": pa.Column(
            int
        ),
        "total_pediatric_patients_hospitalized_confirmed_and_suspected_covid": pa.Column(
            float, nullable=True
        ),
        "total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_coverage": pa.Column(
            int
        ),
        "total_pediatric_patients_hospitalized_confirmed_covid": pa.Column(
            float, nullable=True
        ),
        "total_pediatric_patients_hospitalized_confirmed_covid_coverage": pa.Column(
            int
        ),
        "total_staffed_adult_icu_beds": pa.Column(float, nullable=True),
        "total_staffed_adult_icu_beds_coverage": pa.Column(int),
        "inpatient_beds_utilization": pa.Column(float, nullable=True),
        "inpatient_beds_utilization_coverage": pa.Column(float, nullable=True),
        "inpatient_beds_utilization_numerator": pa.Column(
            float, nullable=True
        ),
        "inpatient_beds_utilization_denominator": pa.Column(
            float, nullable=True
        ),
        "percent_of_inpatients_with_covid": pa.Column(float, nullable=True),
        "percent_of_inpatients_with_covid_coverage": pa.Column(
            float, nullable=True
        ),
        "percent_of_inpatients_with_covid_numerator": pa.Column(
            float, nullable=True
        ),
        "percent_of_inpatients_with_covid_denominator": pa.Column(
            float, nullable=True
        ),
        "inpatient_bed_covid_utilization": pa.Column(float, nullable=True),
        "inpatient_bed_covid_utilization_coverage": pa.Column(
            float, nullable=True
        ),
        "inpatient_bed_covid_utilization_numerator": pa.Column(
            float, nullable=True
        ),
        "inpatient_bed_covid_utilization_denominator": pa.Column(
            float, nullable=True
        ),
        "adult_icu_bed_covid_utilization": pa.Column(float, nullable=True),
        "adult_icu_bed_covid_utilization_coverage": pa.Column(
            float, nullable=True
        ),
        "adult_icu_bed_covid_utilization_numerator": pa.Column(
            float, nullable=True
        ),
        "adult_icu_bed_covid_utilization_denominator": pa.Column(
            float, nullable=True
        ),
        "adult_icu_bed_utilization": pa.Column(float, nullable=True),
        "adult_icu_bed_utilization_coverage": pa.Column(float, nullable=True),
        "adult_icu_bed_utilization_numerator": pa.Column(float, nullable=True),
        "adult_icu_bed_utilization_denominator": pa.Column(
            float, nullable=True
        ),
        "geocoded_state": pa.Column(str, nullable=True),
        "previous_day_admission_adult_covid_confirmed_18_19": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_confirmed_18_19_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_confirmed_20_29": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_confirmed_20_29_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_confirmed_30_39": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_confirmed_30_39_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_confirmed_40_49": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_confirmed_40_49_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_confirmed_50_59": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_confirmed_50_59_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_confirmed_60_69": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_confirmed_60_69_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_confirmed_70_79": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_confirmed_70_79_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_confirmed_80": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_confirmed_80_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_confirmed_unknown": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_confirmed_unknown_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_suspected_18_19": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_suspected_18_19_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_suspected_20_29": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_suspected_20_29_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_suspected_30_39": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_suspected_30_39_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_suspected_40_49": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_suspected_40_49_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_suspected_50_59": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_suspected_50_59_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_suspected_60_69": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_suspected_60_69_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_suspected_70_79": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_suspected_70_79_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_suspected_80_": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_suspected_80_coverage": pa.Column(
            int
        ),
        "previous_day_admission_adult_covid_suspected_unknown": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_adult_covid_suspected_unknown_coverage": pa.Column(
            int
        ),
        "deaths_covid": pa.Column(float, nullable=True),
        "deaths_covid_coverage": pa.Column(int, nullable=True),
        "on_hand_supply_therapeutic_a_casirivimab_imdevimab_courses": pa.Column(
            float, nullable=True
        ),
        "on_hand_supply_therapeutic_b_bamlanivimab_courses": pa.Column(
            float, nullable=True
        ),
        "on_hand_supply_therapeutic_c_bamlanivimab_etesevimab_courses": pa.Column(
            float, nullable=True
        ),
        "previous_week_therapeutic_a_casirivimab_imdevimab_courses_used": pa.Column(
            float, nullable=True
        ),
        "previous_week_therapeutic_b_bamlanivimab_courses_used": pa.Column(
            float, nullable=True
        ),
        "previous_week_therapeutic_c_bamlanivimab_etesevimab_courses_used": pa.Column(
            float, nullable=True
        ),
        "icu_patients_confirmed_influenza": pa.Column(float, nullable=True),
        "icu_patients_confirmed_influenza_coverage": pa.Column(int),
        "previous_day_admission_influenza_confirmed": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_influenza_confirmed_coverage": pa.Column(int),
        "previous_day_deaths_covid_and_influenza": pa.Column(
            float, nullable=True
        ),
        "previous_day_deaths_covid_and_influenza_coverage": pa.Column(int),
        "previous_day_deaths_influenza": pa.Column(float, nullable=True),
        "previous_day_deaths_influenza_coverage": pa.Column(int),
        "total_patients_hospitalized_confirmed_influenza": pa.Column(
            float, nullable=True
        ),
        "total_patients_hospitalized_confirmed_influenza_and_covid": pa.Column(
            float, nullable=True
        ),
        "total_patients_hospitalized_confirmed_influenza_and_covid_coverage": pa.Column(
            int
        ),
        "total_patients_hospitalized_confirmed_influenza_coverage": pa.Column(
            int
        ),
        "all_pediatric_inpatient_bed_occupied": pa.Column(
            float, nullable=True
        ),
        "all_pediatric_inpatient_bed_occupied_coverage": pa.Column(int),
        "all_pediatric_inpatient_beds": pa.Column(float, nullable=True),
        "all_pediatric_inpatient_beds_coverage": pa.Column(int),
        "previous_day_admission_pediatric_covid_confirmed_0_4": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_pediatric_covid_confirmed_0_4_coverage": pa.Column(
            int, nullable=True
        ),
        "previous_day_admission_pediatric_covid_confirmed_12_17": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_pediatric_covid_confirmed_12_17_coverage": pa.Column(
            int
        ),
        "previous_day_admission_pediatric_covid_confirmed_5_11": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_pediatric_covid_confirmed_5_11_coverage": pa.Column(
            int
        ),
        "previous_day_admission_pediatric_covid_confirmed_unknown": pa.Column(
            float, nullable=True
        ),
        "previous_day_admission_pediatric_covid_confirmed_unknown_coverage": pa.Column(
            int
        ),
        "staffed_icu_pediatric_patients_confirmed_covid": pa.Column(
            float, nullable=True
        ),
        "staffed_icu_pediatric_patients_confirmed_covid_coverage": pa.Column(
            int
        ),
        "staffed_pediatric_icu_bed_occupancy": pa.Column(float, nullable=True),
        "staffed_pediatric_icu_bed_occupancy_coverage": pa.Column(int),
        "total_staffed_pediatric_icu_beds": pa.Column(float, nullable=True),
        "total_staffed_pediatric_icu_beds_coverage": pa.Column(int),
    }
)

raw_synth_data = pd.DataFrame()
raw_synth_data = raw_synth_data.assign(
    state=[fake.state_abbr() for _ in range(df_len)],
    date=[fake.date_time_this_year().isoformat() for _ in range(df_len)],
    critical_staffing_shortage_today_yes=[
        random.randint(0, 100) for _ in range(df_len)
    ],
    critical_staffing_shortage_today_no=[
        random.randint(0, 100) for _ in range(df_len)
    ],
    critical_staffing_shortage_today_not_reported=[
        random.randint(0, 100) for _ in range(df_len)
    ],
    critical_staffing_shortage_anticipated_within_week_yes=[
        random.randint(0, 100) for _ in range(df_len)
    ],
    critical_staffing_shortage_anticipated_within_week_no=[
        random.randint(0, 100) for _ in range(df_len)
    ],
    critical_staffing_shortage_anticipated_within_week_not_reported=[
        random.randint(0, 100) for _ in range(df_len)
    ],
    hospital_onset_covid=[random.uniform(0, 100) for _ in range(df_len)],
    hospital_onset_covid_coverage=[random.randint(1, 10) for _ in range(df_len)],
    inpatient_beds=[random.uniform(0, 1000) for _ in range(df_len)],
    inpatient_beds_coverage=[random.randint(1, 10) for _ in range(df_len)],
    inpatient_beds_used=[random.uniform(0, 1000) for _ in range(df_len)],
    inpatient_beds_used_coverage=[random.randint(1, 10) for _ in range(df_len)],
    inpatient_beds_used_covid=[random.uniform(0, 500) for _ in range(df_len)],
    inpatient_beds_used_covid_coverage=[random.randint(1, 10) for _ in range(df_len)],
    previous_day_admission_adult_covid_confirmed=[
        random.uniform(0, 100) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_confirmed_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_suspected=[
        random.uniform(0, 100) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_suspected_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_pediatric_covid_confirmed=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    previous_day_admission_pediatric_covid_confirmed_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_pediatric_covid_suspected=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    previous_day_admission_pediatric_covid_suspected_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    staffed_adult_icu_bed_occupancy=[
        random.uniform(0, 100) for _ in range(df_len)
    ],
    staffed_adult_icu_bed_occupancy_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    staffed_icu_adult_patients_confirmed_and_suspected_covid=[
        random.uniform(0, 100) for _ in range(df_len)
    ],
    staffed_icu_adult_patients_confirmed_and_suspected_covid_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    staffed_icu_adult_patients_confirmed_covid=[
        random.uniform(0, 100) for _ in range(df_len)
    ],
    staffed_icu_adult_patients_confirmed_covid_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    total_adult_patients_hospitalized_confirmed_and_suspected_covid=[
        random.uniform(0, 200) for _ in range(df_len)
    ],
    total_adult_patients_hospitalized_confirmed_and_suspected_covid_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    total_adult_patients_hospitalized_confirmed_covid=[
        random.uniform(0, 200) for _ in range(df_len)
    ],
    total_adult_patients_hospitalized_confirmed_covid_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    total_pediatric_patients_hospitalized_confirmed_and_suspected_covid=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    total_pediatric_patients_hospitalized_confirmed_covid=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    total_pediatric_patients_hospitalized_confirmed_covid_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    total_staffed_adult_icu_beds=[random.uniform(0, 200) for _ in range(df_len)],
    total_staffed_adult_icu_beds_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    inpatient_beds_utilization=[random.uniform(0, 100) for _ in range(df_len)],
    inpatient_beds_utilization_coverage=[
        random.uniform(0, 100) for _ in range(df_len)
    ],
    inpatient_beds_utilization_numerator=[
        random.uniform(0, 1000) for _ in range(df_len)
    ],
    inpatient_beds_utilization_denominator=[
        random.uniform(0, 1000) for _ in range(df_len)
    ],
    percent_of_inpatients_with_covid=[
        random.uniform(0, 100) for _ in range(df_len)
    ],
    percent_of_inpatients_with_covid_coverage= [
        random.uniform(0, 100) for _ in range(df_len)
    ],
    percent_of_inpatients_with_covid_numerator=[
        random.uniform(0, 1000) for _ in range(df_len)
    ],
    percent_of_inpatients_with_covid_denominator=[
        random.uniform(0, 1000) for _ in range(df_len)
    ],
    inpatient_bed_covid_utilization=[random.uniform(0, 100) for _ in range(df_len)],
    inpatient_bed_covid_utilization_coverage=[
        random.uniform(0, 100) for _ in range(df_len)
    ],
    inpatient_bed_covid_utilization_numerator=[
        random.uniform(0, 1000) for _ in range(df_len)
    ],
    inpatient_bed_covid_utilization_denominator=[
        random.uniform(0, 1000) for _ in range(df_len)
    ],
    adult_icu_bed_covid_utilization=[random.uniform(0, 100) for _ in range(df_len)],
    adult_icu_bed_covid_utilization_coverage=[
        random.uniform(0, 100) for _ in range(df_len)
    ],
    adult_icu_bed_covid_utilization_numerator=[
        random.uniform(0, 1000) for _ in range(df_len)
    ],
    adult_icu_bed_covid_utilization_denominator=[
        random.uniform(0, 1000) for _ in range(df_len)
    ],
    adult_icu_bed_utilization=[random.uniform(0, 100) for _ in range(df_len)],
    adult_icu_bed_utilization_coverage=[
        random.uniform(0, 100) for _ in range(df_len)
    ],
    adult_icu_bed_utilization_numerator=[
        random.uniform(0, 1000) for _ in range(df_len)
    ],
    adult_icu_bed_utilization_denominator=[
        random.uniform(0, 1000) for _ in range(df_len)
    ],
    geocoded_state=[fake.state_abbr() for _ in range(df_len)],
    previous_day_admission_adult_covid_confirmed_18_19=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_confirmed_18_19_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_confirmed_20_29=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_confirmed_20_29_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_confirmed_30_39=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_confirmed_30_39_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_confirmed_40_49=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_confirmed_40_49_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_confirmed_50_59=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_confirmed_50_59_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_confirmed_60_69=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_confirmed_60_69_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_confirmed_70_79=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_confirmed_70_79_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_confirmed_80=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_confirmed_80_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_confirmed_unknown=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_confirmed_unknown_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_suspected_18_19=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_suspected_18_19_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_suspected_20_29=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_suspected_20_29_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_suspected_30_39=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_suspected_30_39_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_suspected_40_49=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_suspected_40_49_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_suspected_50_59=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_suspected_50_59_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_suspected_60_69=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_suspected_60_69_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_suspected_70_79=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_suspected_70_79_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_suspected_80_=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_suspected_80_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_suspected_unknown=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    previous_day_admission_adult_covid_suspected_unknown_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    deaths_covid=[random.uniform(0, 10) for _ in range(df_len)],
    deaths_covid_coverage=[random.randint(1, 10) for _ in range(df_len)],
    on_hand_supply_therapeutic_a_casirivimab_imdevimab_courses=[
        random.uniform(0, 1000) for _ in range(df_len)
    ],
    on_hand_supply_therapeutic_b_bamlanivimab_courses=[
        random.uniform(0, 1000) for _ in range(df_len)
    ],
    on_hand_supply_therapeutic_c_bamlanivimab_etesevimab_courses=[
        random.uniform(0, 1000) for _ in range(df_len)
    ],
    previous_week_therapeutic_a_casirivimab_imdevimab_courses_used=[
        random.uniform(0, 100) for _ in range(df_len)
    ],
    previous_week_therapeutic_b_bamlanivimab_courses_used=[
        random.uniform(0, 100) for _ in range(df_len)
    ],
    previous_week_therapeutic_c_bamlanivimab_etesevimab_courses_used=[
        random.uniform(0, 100) for _ in range(df_len)
    ],
    icu_patients_confirmed_influenza=[random.uniform(0, 50) for _ in range(df_len)],
    icu_patients_confirmed_influenza_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_influenza_confirmed=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    previous_day_admission_influenza_confirmed_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_deaths_covid_and_influenza=[
        random.uniform(0, 10) for _ in range(df_len)
    ],
    previous_day_deaths_covid_and_influenza_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_deaths_influenza=[random.uniform(0, 10) for _ in range(df_len)],
    previous_day_deaths_influenza_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    total_patients_hospitalized_confirmed_influenza=[
        random.uniform(0, 100) for _ in range(df_len)
    ],
    total_patients_hospitalized_confirmed_influenza_and_covid=[
        random.uniform(0, 100) for _ in range(df_len)
    ],
    total_patients_hospitalized_confirmed_influenza_and_covid_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    total_patients_hospitalized_confirmed_influenza_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    all_pediatric_inpatient_bed_occupied=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    all_pediatric_inpatient_bed_occupied_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    all_pediatric_inpatient_beds=[random.uniform(0, 100) for _ in range(df_len)],
    all_pediatric_inpatient_beds_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_pediatric_covid_confirmed_0_4=[
        random.uniform(0, 20) for _ in range(df_len)
    ],
    previous_day_admission_pediatric_covid_confirmed_0_4_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_pediatric_covid_confirmed_12_17=[
        random.uniform(0, 20) for _ in range(df_len)
    ],
    previous_day_admission_pediatric_covid_confirmed_12_17_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_pediatric_covid_confirmed_5_11=[
        random.uniform(0, 20) for _ in range(df_len)
    ],
    previous_day_admission_pediatric_covid_confirmed_5_11_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    previous_day_admission_pediatric_covid_confirmed_unknown=[
        random.uniform(0, 20) for _ in range(df_len)
    ],
    previous_day_admission_pediatric_covid_confirmed_unknown_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    staffed_icu_pediatric_patients_confirmed_covid=[
        random.uniform(0, 20) for _ in range(df_len)
    ],
    staffed_icu_pediatric_patients_confirmed_covid_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    staffed_pediatric_icu_bed_occupancy=[
        random.uniform(0, 50) for _ in range(df_len)
    ],
    staffed_pediatric_icu_bed_occupancy_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
    total_staffed_pediatric_icu_beds=[random.uniform(0, 50) for _ in range(df_len)],
    total_staffed_pediatric_icu_beds_coverage=[
        random.randint(1, 10) for _ in range(df_len)
    ],
)

tf_synth_data = raw_synth_data.copy()

import pandera.pandas as pa
import pandas as pd

import random
from faker import Faker
fake=Faker()
df_len = 100

extract_schema = pa.DataFrameSchema(
    {
        "usa_or_hhsregion": pa.Column(str, coerce=True),
        "week_ending": pa.Column(str),
        "variant": pa.Column(str, nullable=True),
        "share": pa.Column(float, nullable=True),
        "share_hi": pa.Column(float, nullable=True),
        "share_lo": pa.Column(float, nullable=True),
        "count_lt10": pa.Column(float, nullable=True),
        "modeltype": pa.Column(str),
        "time_interval": pa.Column(str),
        "creation_date": pa.Column(str),
    }
)

load_schema = pa.DataFrameSchema(
    {
        "usa_or_hhsregion": pa.Column(str, coerce=True),
        "week_ending": pa.Column(str),
        "variant": pa.Column(str, nullable=True),
        "share": pa.Column(float, nullable=True),
        "share_hi": pa.Column(float, nullable=True),
        "share_lo": pa.Column(float, nullable=True),
        "count_lt10": pa.Column(float, nullable=True),
        "modeltype": pa.Column(str),
        "time_interval": pa.Column(str),
        "creation_date": pa.Column(str),
    }
)

region_opts =['USA', '2', '5', '8', '3', '9', '4', '7']
week_ending_opts = ['2025-02-15 00:00:00', '2025-03-01 00:00:00', '2025-01-04 00:00:00',
 '2025-01-18 00:00:00', '2024-09-28 00:00:00', '2024-10-12 00:00:00',
 '2024-08-31 00:00:00', '2024-09-14 00:00:00', '2021-05-15 00:00:00',
 '2021-05-29 00:00:00']
variant_opts = ['MC.10.1', 'XEQ', 'XEC.4', 'MC.19', 'MC.28.1', 'LB.1', 'KS.1', 'KP.1.1.3',
 'KP.2.3', 'LP.8.1', 'XEC', 'LF.7', 'KP.3', 'KP.3.1.1', 'LB.1.3.1', 'BA.2.86',
 'MC.1', 'XEK', 'Other', 'JN.1.18.6', 'JN.1' ,'LP.1', 'KP.2', 'JN.1.18',
 'JN.1.16.1', 'JN.1.11.1', 'LF.3.1', 'KP.2.15', 'KP.4.1', 'JN.1.7' ,'XDV.1',
 'KW.1.1', 'JN.1.16', 'BA.2', 'KQ.1', 'XBB.1.9.1', 'JN.1.13.1', 'BA.2.12.1',
 'XBB.1.5', 'KP.1.2', 'KP.1.1', 'EG.5', 'XDP', 'JN.1.32', 'BQ.1.1', 'HV.1',
 'JN.1.8.1', 'EG.5.1.8', 'B.1.1.529', 'B.1.617.2', 'BA.1.1', 'BA.2.75',
 'BA.2.75.2', 'BA.4', 'BA.4.6', 'BA.5', 'BA.5.2.6', 'BF.11', 'BF.7', 'BN.1',
 'BQ.1', 'CH.1.1', 'EG.6.1', 'EU.1.1', 'FD.1.1', 'FD.2', 'FE.1.1', 'FL.1.5.1',
 'GE.1', 'GK.1.1', 'GK.2', 'HF.1', 'HK.3', 'JD.1.1', 'JF.1', 'JG.3', 'JN.1.13',
 'JN.1.4.3', 'KV.2', 'XBB', 'XBB.1.16', 'XBB.1.16.1', 'XBB.1.16.11',
 'XBB.1.16.15', 'XBB.1.16.17', 'XBB.1.16.6', 'XBB.1.42.2', 'XBB.1.5.1',
 'XBB.1.5.10', 'XBB.1.5.59', 'XBB.1.5.68', 'XBB.1.5.70', 'XBB.1.5.72',
 'XBB.1.9.2', 'XBB.2.3', 'XBB.2.3.8']
model_opts = ['smoothed', 'weighted']
date_opts = ['2025-02-28 00:00:00', '2025-01-17 00:00:00', '2024-10-11 00:00:00',
 '2024-09-13 00:00:00']
raw_synth_data = pd.DataFrame()
raw_synth_data = raw_synth_data.assign(
    usa_or_hhsregion=[random.choice(region_opts) for _ in range(df_len)],
    week_ending=[random.choice(week_ending_opts) for _ in range(df_len)],
    variant=[random.choice(variant_opts) for _ in range(df_len)],
    share=[random.uniform(0.0, 1.0) for _ in range(df_len)],
    share_hi=[random.uniform(0.0, 1.0) for _ in range(df_len)],
    share_lo=[random.uniform(0.0, 1.0) for _ in range(df_len)],
    count_lt10=[random.uniform(0.0, 1.0) for _ in range(df_len)],
    modeltype=[random.choice(model_opts) for _ in range(df_len)],
    time_interval=["biweekly" for _ in range(df_len)],
    creation_date=[random.choice(date_opts) for _ in range(df_len)]
)

tf_synth_data = raw_synth_data.copy()
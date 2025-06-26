import random


import pandas as pd
import pandera.pandas as pa
from faker import Faker


fake = Faker()
df_len = 100

extract_schema = pa.DataFrameSchema(
    {
        
    }
)

load_schema = pa.DataFrameSchema(
    {
        
    }
)

raw_synth_data = pd.DataFrame()
raw_synth_data = raw_synth_data.assign(
    
)
tf_synth_data = raw_synth_data.copy()

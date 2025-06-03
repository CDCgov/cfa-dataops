import pandera.pandas as pa 

extract_schema = pa.DataFrameSchema({
    "indicator": pa.Column(str),
    "geographic_area": pa.Column(str),
    "geographic_identifier": pa.Column(str),
    "race": pa.Column(str),
    "sex": pa.Column(str),
    "age": pa.Column(str),
    "time_period": pa.Column(str),
    "n_unweighted": pa.Column(float, nullable = True),
    "estimate_weighted": pa.Column(float, nullable = True),
    "_2_5": pa.Column(float, nullable = True),
    "_97_5": pa.Column(float, nullable = True)
})

load_schema = pa.DataFrameSchema({
    "indicator": pa.Column(str),
    "geographic_area": pa.Column(str),
    "geographic_identifier": pa.Column(str),
    "race": pa.Column(str),
    "sex": pa.Column(str),
    "age": pa.Column(str),
    "time_period": pa.Column(str),
    "n_unweighted": pa.Column(float, nullable = True),
    "estimate_weighted": pa.Column(float, nullable = True),
    "_2_5": pa.Column(float, nullable = True),
    "_97_5": pa.Column(float, nullable = True)
})
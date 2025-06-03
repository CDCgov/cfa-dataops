import pandera.pandas as pa

extract_schema = pa.DataFrameSchema(
    {
        "name": pa.Column(str),
        "region": pa.Column(int),
        "address": pa.Column(str),
        "city": pa.Column(str),
        "state": pa.Column(str),
        "zipCode": pa.Column(str),
        "states": pa.Column(object),
        "loc": pa.Column(object),
        "lastRefresh": pa.Column(str),
        "hash": pa.Column(str),
        "id": pa.Column(str),
    }
)

load_schema = pa.DataFrameSchema(
    {
        "stname": pa.Column(str),
        "st": pa.Column(object),
        "stusps": pa.Column(str),
    }
)

import pandera.pandas as pa

extract_schema = pa.DataFrameSchema(
    {
        "name": pa.Column(str),
        "region": pa.Column(int, nullable = True),
        "address": pa.Column(str, nullable = True),
        "city": pa.Column(str, nullable = True),
        "state": pa.Column(str, nullable = True),
        "zipCode": pa.Column(str, nullable = True),
        "states": pa.Column(object, nullable = True),
        "loc": pa.Column(object, nullable = True),
        "lastRefresh": pa.Column(str, nullable = True),
        "hash": pa.Column(str, nullable = True),
        "id": pa.Column(str, nullable = True),
    }
)

load_schema = pa.DataFrameSchema(
    {
        "stname": pa.Column(str),
        "st": pa.Column(object),
        "stusps": pa.Column(str),
    }
)

import pandera.pandas as pa

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

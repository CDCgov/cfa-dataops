"""For ETL of the COVID 19 vaccination trends"""

import json
from io import BytesIO
from typing import Optional

# import duckdb
# import pandera.pandas as pa
import polars as pl

from ... import datacat, get_data
from ...soda import Query
from ..utils import get_timestamp

config = datacat.mcmv.respnet


def extract(
    app_token: Optional[str] = None,
) -> pl.DataFrame:
    """
    Download data from:
    <https://data.cdc.gov/Public-Health-Surveillance/Rates-of-Laboratory-Confirmed-RSV-COVID-19-and-Flu/kvib-3txy/about_data>

    Note that `start_date` and `end_date` are expected to be Sundays, in accordance
    with the rest of the codebase, but the RESP-NET data has `_weekenddate`, which
    are Saturdays.

    Args:
        app_token (Optional[str]): Application token for accessing the CDC API

    Returns:
        pl.DataFrame: Polars DataFrame containing the requested data, plus a
            `date` column that is the Sunday that starts each week
    """

    extract_blob_version = get_timestamp()

    q = Query(
        domain=config.source.domain,
        id=config.source.id,
        # clauses=clauses,
        app_token=app_token,
    )
    dfs = []
    for idx, i in enumerate(q.get_pages()):
        dfs.append(pl.from_dicts(i))
        config.extract.write_blob(
            file_buffer=bytes(json.dumps(i, indent=2), "utf-8"),
            path_after_prefix=f"{extract_blob_version}/part_{idx}.json",
        )
    data = pl.concat(dfs)

    return data


def transform(df: pl.DataFrame) -> pl.DataFrame:  # noqa: W0613
    """Run a SQL transform on the raw DataFrame

    Args:
        df (pd.DataFrame): the extracted data

    Returns:
        pd.DataFrame: the transformed data
    """
    return (
        df.select(
            [
                "surveillance_network",
                "age_group",
                pl.col("weekly_rate").cast(pl.Float64).alias("weekly_rate"),
                "_weekenddate",
                "type",
                "site",
                "sex",
                "race_ethnicity",
            ]
        )
        .with_columns(
            weekenddate=pl.col("_weekenddate").str.to_date(
                format="%Y-%m-%d %H:%M:%S"
            )
        )
        .filter(
            (pl.col("site") == "Overall")
            & (pl.col("sex") == "Overall")
            & (pl.col("race_ethnicity") == "Overall")
        )
        .drop(["site", "sex", "race_ethnicity", "_weekenddate"])
    )


def load(df: pl.DataFrame) -> None:
    """Loads transformed data to blob storage

    Args:
        df (pd.DataFrame): the transformed data to be loaded as parquet
    """
    load_blob_version = get_timestamp()
    buffer = BytesIO()
    df.write_parquet(buffer)
    config.load.write_blob(
        file_buffer=buffer.getvalue(),
        path_after_prefix=f"{load_blob_version}/data.parquet",
    )
    buffer.close()


def main(run_extract: bool = False) -> None:
    """ETL main runner

    Args:
        extract (bool, optional): should the extraction run. Useful in the case
        that the raw data is static and doesn't require update while iteration
        on the transformation of the raw data. Defaults to False.
    """
    if run_extract:
        raw_df = extract()
    else:
        raw_df = get_data(
            name="mcmv.respnet",
            version="latest",
            type="raw",
            output="polars",
        )
    transformed_df = transform(raw_df)
    load(transformed_df)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        prog="RESPNET etl pipeline",
    )
    parser.add_argument("--extract", "-e", action="store_true", default=False)
    args = parser.parse_args()
    main(
        run_extract=args.extract,
    )

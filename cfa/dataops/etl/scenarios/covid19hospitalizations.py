"""For ETL of the COVID 19 hospitalizations"""

from io import StringIO

import duckdb
import httpx
import pandas as pd
import pandera.pandas as pa
from tqdm import tqdm

from ... import datacat

# import schema
from ...datasets.scenarios.schemas.covid19hospitalizations import (
    extract_schema,
    load_schema,
)
from ..utils import get_timestamp, transform_template_lookup

config = datacat.scenarios.covid19hospitalizations


def extract() -> pd.DataFrame:
    """Get data and return raw values as a DataFrame

    Returns:
        pd.DataFrame: the extracted data
    """

    r_count = httpx.get(config.source.url, params={"$select": "count(*)"})
    dataset_len = int("".join(char for char in r_count.text if char.isdigit()))
    offsets = range(0, dataset_len, config.source.pagination.limit)

    extract_blob_version = f"{get_timestamp()}"

    get_dfs = []

    for idx, offset_i in enumerate(tqdm(offsets)):
        params = {
            "$limit": config.source.pagination.limit,
            "$offset": offset_i,
        }

        r = httpx.get(config.source.url, params=params)

        config.extract.write_blob(
            file_buffer=bytes(r.text, "utf-8"),
            path_after_prefix=f"{extract_blob_version}/part_{idx}.csv",
        )

        get_dfs.append(pd.read_csv(StringIO(r.text)))

    return pd.concat(get_dfs)


def transform(df: pd.DataFrame) -> pd.DataFrame:  # noqa: W0613
    """Run a SQL transform on the raw DataFrame

    Args:
        df (pd.DataFrame): the extracted data

    Returns:
        pd.DataFrame: the transformed data
    """
    template = transform_template_lookup.get_template(
        config.properties.transform_template
    )
    query = template.render(data_source="df")
    transformed_db = duckdb.sql(query).df()
    return transformed_db


def load(df: pd.DataFrame) -> None:
    """Loads transformed data to blob storage

    Args:
        df (pd.DataFrame): the transformed data to be loaded as parquet
    """

    load_blob_version = f"{get_timestamp()}"

    config.load.write_blob(
        file_buffer=df.to_parquet(),
        path_after_prefix=f"{load_blob_version}/data.parquet",
    )


def main(
    run_extract: bool = False, val_raw: bool = False, val_tf: bool = False
) -> None:
    """ETL main runner

    Args:
        extract (bool, optional): should the extraction run. Useful in the case
        that the raw data is static and doesn't require update while iteration
        on the transformation of the raw data. Defaults to False.
        val_raw (bool, optional): whether to validate the raw data schema during extraction.
        val_tf (bool, optional): whether to validate the transformed data schema before loading to Blob Storage.
    """

    if run_extract:
        raw_df = extract()
        if val_raw:
            # check raw_df against schema
            try:
                extract_schema.validate(raw_df)
            except pa.errors.SchemaError as exc:
                raise (exc)

    else:
        try:
            buffers = config.extract.read_blobs()
            raw_df = pd.concat([pd.read_csv(i) for i in buffers])
        except IndexError as e:
            raise AttributeError(
                "Run extract set to False, but no latest version of extract "
                "data to use. Run with extraction step to fetch latest raw "
                "version."
            ) from e
    # transform dataframe
    transformed_df = transform(raw_df)
    # validate transformed df
    if val_tf:
        try:
            load_schema.validate(transformed_df)
        except pa.errors.SchemaError as exc:
            print(
                "Validation of tranformed dataframe failed. Data not loaded to Blob Storage."
            )
            print("Fix the pipeline and try again.")
            raise exc
    # load data to blob storage
    load(transformed_df)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        prog="COVID 19 vaccination trends data etl pipeline",
    )
    parser.add_argument("--extract", "-e", action="store_true", default=False)
    parser.add_argument(
        "--validate_raw", "-v", action="store_true", default=False
    )
    parser.add_argument(
        "--validate_transf", "-t", action="store_true", default=False
    )
    args = parser.parse_args()
    main(
        run_extract=args.extract,
        val_raw=args.validate_raw,
        val_tf=args.validate_transf,
    )

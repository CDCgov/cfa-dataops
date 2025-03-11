"""For ETL of the COVID 19 vaccination trends"""

from io import StringIO

import duckdb
import httpx
import pandas as pd
from cfa_azure.helpers import read_blob_stream, write_blob_stream
from tqdm import tqdm

from ..datasets import configs
from .utils import get_timestamp, transform_template_lookup

config = configs.covid19vax


def extract() -> pd.DataFrame:
    """Get data and return raw values as a DataFrame

    Returns:
        pd.DataFrame: the extracted data
    """
    offsets = range(
        0, config.source.pagination.n_records, config.source.pagination.limit
    )

    extract_blob_account = config.extract.account
    extract_blob_container = config.extract.container
    extract_blob_path = f"{config.extract.path}/{get_timestamp()}"

    get_dfs = []

    for idx, offset_i in enumerate(tqdm(offsets)):
        params = {
            "$limit": config.source.pagination.limit,
            "$offset": offset_i,
        }

        r = httpx.get(config.source.url, params=params)

        write_blob_stream(
            data=bytes(r.text, "utf-8"),
            blob_url=f"{extract_blob_path}/part_{idx}.csv",
            account_name=extract_blob_account,
            container_name=extract_blob_container,
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
    template = transform_template_lookup.get_template("covid19vax.sql")
    query = template.render(data_source="df")
    transformed_db = duckdb.sql(query).df()
    return transformed_db


def load(df: pd.DataFrame) -> None:
    """Loads transformed data to blob storage

    Args:
        df (pd.DataFrame): the transformed data to be loaded as parquet
    """

    blob_account = config.load.account
    blob_container = config.load.container
    blob_path = f"{config.load.path}/{get_timestamp()}"

    write_blob_stream(
        data=df.to_parquet(),
        blob_url=f"{blob_path}/data.parquet",
        account_name=blob_account,
        container_name=blob_container,
    )


def main(run_extract: bool = False) -> None:
    """ETL main runner

    Args:
        extract (bool, optional): should the extraction run. Useful in the case
        that the raw data is static and doesn't require update while iteration
        on the transformation of the raw data. Defaults to False.
    """
    extract_blob_account = config.extract.account
    extract_blob_container = config.extract.container
    if run_extract:
        raw_df = extract()

    else:
        extract_blob_path = f"{config.extract.path}/2025-03-10T13-30-13/*.csv"
        # TODO: this functionality needs to be added
        buffer = read_blob_stream(
            blob_url=extract_blob_path,
            account_name=extract_blob_account,
            container_name=extract_blob_container,
        )
        raw_df = pd.read_csv(buffer)
    # transform dataframe
    transformed_df = transform(raw_df)
    # load data to blob storage
    load(transformed_df)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        prog="COVID 19 vaccination trends data etl pipeline",
    )
    parser.add_argument("--extract", "-e", action="store_true", default=False)
    args = parser.parse_args()
    main(args.extract)

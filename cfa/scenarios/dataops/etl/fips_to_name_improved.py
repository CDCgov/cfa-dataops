"""For ETL of the FIPS to name dataset"""

import json

import duckdb
import httpx
import pandas as pd
from tqdm import tqdm

from ..datasets import datasets
from ..datasets.catalog import get_data
from .utils import get_timestamp, transform_template_lookup

config = datasets.fips_to_name_improved


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

        r = httpx.get(config.source.url, params=params, timeout=30)

        config.extract.write_blob(
            file_buffer=bytes(r.text, "utf-8"),
            path_after_prefix=f"{extract_blob_version}/part_{idx}.json",
        )
        df = pd.DataFrame(json.loads(r.text)["FemaRegions"])

        get_dfs.append(df)

    return pd.concat(get_dfs)


def transform(df: pd.DataFrame) -> pd.DataFrame:  # noqa: W0613
    """Run a SQL transform on the raw DataFrame

    Args:
        df (pd.DataFrame): the extracted data

    Returns:
        pd.DataFrame: the transformed data
    """
    fips = get_data("fips_to_name", type="transformed")  # noqa
    df = df.dropna()
    df["region"] = df["region"].apply(lambda x: int(x))
    df = df.explode("states")
    template = transform_template_lookup.get_template(
        config.properties.transform_template
    )
    query = template.render(data_source="df", fips="fips")
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
        try:
            buffers = config.extract.read_blobs()
            raw_df = pd.concat(
                [
                    pd.DataFrame(
                        json.loads(i.content_as_bytes().decode("utf-8"))[
                            "FemaRegions"
                        ]
                    )
                    for i in buffers
                ]
            )
        except IndexError as e:
            raise AttributeError(
                "Run extract set to False, but no latest version of extract "
                "data to use. Run with extraction step to fetch latest raw "
                "version."
            ) from e
    # transform dataframe
    transformed_df = transform(raw_df)
    # load data to blob storage
    load(transformed_df)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        prog="FIPS to name etl pipeline",
    )
    parser.add_argument("--extract", "-e", action="store_true", default=False)
    args = parser.parse_args()
    main(run_extract=args.extract)

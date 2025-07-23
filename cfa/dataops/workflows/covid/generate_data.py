import argparse
import os
import warnings

import pandas as pd

from cfa.dataops.catalog import BlobEndpoint, get_data
from cfa.dataops.etl.utils import get_today_date

warnings.filterwarnings("ignore")

gen_data_blob = BlobEndpoint(
    account="cfadatalakeprd",
    container="cfapredict",
    prefix="dataops/scenarios/workflows/covid/data",
)


def generate_vaccination_data(
    data_folder_path: str, blob: bool
) -> pd.DataFrame:
    """
    generate_vaccination_data
    Generate vaccination data from CDC API and save to csv file or to blob storage.


    Args:
        data_folder_path (str): path to save the csv file
        blob (bool): whether to save the csv file to blob storage

    Returns:
        pd.DataFrame: vaccination data
    """
    vac = get_data("covid19vax_trends", type="transformed")

    # filter to US
    vac = vac[vac["state"] == "US"].reset_index(drop=True)
    vac = vac.drop(["state"], axis=1)
    vac["date"] = vac["date"].apply(
        lambda x: str(x)[:10]
    )  # Convert to string and keep only date part
    vac = vac.sort_values(["age", "date"]).reset_index(drop=True)
    # unnest the arrays
    vac = vac.explode(["total", "percentage", "dose"]).reset_index(drop=True)
    if blob:
        load_blob_version = f"{get_today_date()}"
        gen_data_blob.write_blob(
            file_buffer=vac.to_csv(),
            path_after_prefix=f"{load_blob_version}/vaccination_data.csv",
        )
    else:
        os.makedirs(data_folder_path, exist_ok=True)
        filename = data_folder_path + "/vaccination_data.csv"
        vac.to_csv(filename, index=False)
    return vac


def generate_hospitalization_data(
    data_folder_path: str, blob: bool
) -> pd.DataFrame:
    """
    generate_hospitalization_data
    Generate hospitalization data from CDC API and save to csv file or to blob storage.

    Args:
        data_folder_path (str): path to save the csv file
        blob (bool): whether to save the csv file to blob storage

    Returns:
        pd.DataFrame: hospitalization data
    """
    if not blob:
        os.makedirs(data_folder_path, exist_ok=True)
    df = get_data("hospitalization", type="transformed")
    region_id = get_data("fips_to_name_improved")

    # loop through all states
    for region in region_id.stusps:
        output = df[df["state"] == region].reset_index(drop=True)
        # get long region name
        long_name = output["stname"].iloc[0]
        output = output.drop(["stname"], axis=1)
        filename = "weekly_hospital_" + long_name + ".csv"

        if blob:
            load_blob_version = f"{get_today_date()}"
            gen_data_blob.write_blob(
                file_buffer=output.to_csv(index=False),
                path_after_prefix=f"{load_blob_version}/{filename}",
            )
        else:
            filename = data_folder_path + "/" + filename
            output.to_csv(filename, index=False)
    return output


def run(data_folder_path: str, blob: bool = False) -> None:
    """
    run
    Main function to generate vaccination and hospitalization data.

    Args:
        data_folder_path (str): path to save the csv file
        blob (bool, optional): whether to save outputs to blob storage. Defaults to False.
    """
    generate_vaccination_data(data_folder_path, blob)
    generate_hospitalization_data(data_folder_path, blob)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        "--path",
        type=str,
        help="output path for generated data",
        default="covid/data",
    )
    parser.add_argument(
        "-b",
        "--blob",
        action="store_true",
        help="flag to store generated data to Blob Storage",
    )
    args = parser.parse_args()
    path = args.path
    blob = args.blob
    output = run(data_folder_path=path, blob=blob)

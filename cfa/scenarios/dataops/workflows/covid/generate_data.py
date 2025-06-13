import argparse
import json
import os
import warnings
from datetime import datetime

import pandas as pd
import requests
from cfa.scenarios.dataops.datasets.catalog import get_data
from cfa.scenarios.dataops.datasets.catalog import BlobEndpoint, get_data
from cfa.scenarios.dataops.etl.utils import get_today_date

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

    #filter to US
    vac = vac[vac["state"] == "US"].reset_index(drop=True)
    vac = vac.drop(["state"], axis = 1)
    vac["date"] = vac['date'].apply(lambda x: str(x)[:10])  # Convert to string and keep only date part
    vac = vac.sort_values(["age", "date"]).reset_index(drop=True)
    #unnest the arrays
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
    region_id = get_data("fips_to_name_improved")

    text = ["https://data.cdc.gov/resource/aemt-mg7g.json?$limit=50000"]
    api = "".join(text)
    response_API = requests.get(api)
    data = response_API.text
    data_raw = json.loads(data)
    temp = pd.DataFrame.from_dict(data_raw, orient="columns")
    temp.loc[:, "week_end_date"] = temp["week_end_date"].astype(str).str[0:10]
    temp.loc[:, "total_admissions_all_covid_confirmed"]

    subsection_data = pd.DataFrame(
        {
            "date": temp.week_end_date,
            "state": temp.jurisdiction,
            "total": temp.total_admissions_all_covid_confirmed,
        }
    )

    for i in range(len(region_id)):
        if region_id.stusps[i] == "US":
            output = subsection_data[
                subsection_data.state == "USA"
            ].reset_index(drop=True)
            output.state = "US"
        else:
            output = subsection_data[
                subsection_data.state == region_id.stusps[i]
            ].reset_index(drop=True)

        filename = (
            "weekly_hospital_"
            + region_id.stname[i].lower().replace(" ", "_")
            + ".csv"
        )

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

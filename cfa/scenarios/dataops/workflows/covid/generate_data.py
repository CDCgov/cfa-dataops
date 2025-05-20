import argparse
import json
import os
import warnings
from datetime import datetime

import pandas as pd
import requests

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
    given_groups = [
        "Ages_<5yrs",
        "Ages_5-11_yrs",
        "Ages_12-17_yrs",
        "Ages_18-24_yrs",
        "Ages_25-49_yrs",
        "Ages_50-64_yrs",
        "Ages_65%2B_yrs",
    ]
    given_to_desired = [[0, 1, 2], [3, 4], [5], [6]]
    desired_groups = ["0-17", "18-49", "50-64", "65+"]
    for j in range(len(given_groups)):
        text = [
            "https://data.cdc.gov/resource/5i5k-6cmh.json?demographic_category=",
            given_groups[j],
            "&location=US&$limit=50000",
        ]  # https://data.cdc.gov/Vaccinations/COVID-19-Vaccination-Age-and-Sex-Trends-in-the-Uni/5i5k-6cmh/data_preview
        api = "".join(text)
        response_API = requests.get(api)
        data = response_API.text
        data_raw = json.loads(data)
        temp = pd.DataFrame.from_dict(data_raw, orient="columns")
        temp.loc[:, "date"] = temp["date"].astype(str).str[0:10]
        if len(temp.columns) == 12:
            temp = temp.drop(
                columns=["second_booster", "second_booster_vax_pct_agegroup"]
            )
        temp.columns = [
            "date",
            "state",
            "age",
            "census",
            "dose_1",
            "dose_2",
            "dose_3",
            "dose_1_percent",
            "dose_2_percent",
            "dose_3_percent",
        ]
        if j == 0:
            output = pd.DataFrame(temp)
        else:
            output = pd.concat([output, temp], ignore_index=True)
    # save to blob or data_folder_path
    if blob:
        load_blob_version = f"{get_today_date()}"
        gen_data_blob.write_blob(
            file_buffer=output.to_csv(index=False),
            path_after_prefix=f"{load_blob_version}/vaccination_raw.csv",
        )
    else:
        os.makedirs(data_folder_path, exist_ok=True)
        filename = (
            data_folder_path
            + "/"
            + datetime.today().strftime("%Y_%m_%d")
            + "vaccination_raw.csv"
        )
        output.to_csv(filename, index=False)

    vac = pd.DataFrame(
        columns=["date", "age", "census", "total", "percentage", "dose"]
    )
    all_days = list(set(output.date))
    all_days.sort()
    for i in range(len(given_to_desired)):
        given = list(map(given_groups.__getitem__, given_to_desired[i]))
        if given[-1] == given_groups[-1]:
            given[-1] = "Ages_65+_yrs"
        output_aged = output[output["age"].isin(given)].reset_index(drop=True)
        for n in range(len(all_days)):
            output_date = output_aged[
                output_aged.date == all_days[n]
            ].reset_index(drop=True)
            census = sum([int(item) for item in set(output_date.census)])
            for q in range(len(given)):
                if str(output_date.dose_1[q]) == "nan":
                    output_date.loc[q, "dose_1"] = "0"
                if str(output_date.dose_2[q]) == "nan":
                    output_date.loc[q, "dose_2"] = "0"
                if str(output_date.dose_3[q]) == "nan":
                    output_date.loc[q, "dose_3"] = "0"
            dose1 = sum([int(item) for item in set(output_date.dose_1)])
            dose2 = sum([int(item) for item in set(output_date.dose_2)])
            dose3 = sum([int(item) for item in set(output_date.dose_3)])
            dose1_percent = dose1 / census
            dose2_percent = dose2 / census
            dose3_percent = dose3 / census
            vac = pd.concat(
                [
                    vac,
                    pd.DataFrame(
                        {
                            "date": all_days[n],
                            "age": desired_groups[i],
                            "census": census,
                            "total": [dose1, dose2, dose3],
                            "percentage": [
                                dose1_percent,
                                dose2_percent,
                                dose3_percent,
                            ],
                            "dose": [1, 2, 3],
                        }
                    ),
                ],
                ignore_index=True,
            )
    if blob:
        load_blob_version = f"{get_today_date()}"
        gen_data_blob.write_blob(
            file_buffer=vac.to_csv(),
            path_after_prefix=f"{load_blob_version}/vaccination_data.csv",
        )
    else:
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

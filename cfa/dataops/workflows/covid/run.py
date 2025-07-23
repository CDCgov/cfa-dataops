import argparse
import inspect
import json
import os
import warnings
from datetime import datetime
from shutil import copytree

import pandas as pd
from tqdm import tqdm

from cfa.dataops.catalog import BlobEndpoint, get_data
from cfa.dataops.etl.utils import get_today_date

from .funcs.check import check_blob_date_exists
from .funcs.formatting_data import format_vac_data, output_generation
from .funcs.import_data import (
    import_hospitalization_data,
    import_population_data,
    import_sero_data,
    import_vaccination_data,
    import_variant_data,
)
from .funcs.process_data import (
    cases_by_variant,
    individual_infection_totals,
    total_cases_using_hospital_and_sero,
)
from .funcs.tslie import get_tslie2

warnings.filterwarnings("ignore")


def running_subfunctions(
    doses: int,
    start: str,
    age_groups: list[int],
    tslie_ranges: list[int],
    variant_types: list[str],
    data_folder_path: list[str],
    state: pd.Series,
    blob: bool = False,
) -> pd.DataFrame:
    """
    running_subfunctions
    The main running function for the run module. It imports and processes the data.

    Args:
        doses (int): number of doses to track
        start (str): start date for tracking data
        age_groups (list[int]): list of upper limits of each age group
        tslie_ranges (list[int]): list of upper limits of each tslie group
        variant_types (list[str]): list of variant types, such as ["Omicron", "Other"]
        data_folder_path (list[str]): path to the data folder
        state (pd.Series): series object of state information
        blob (bool, optional): if True, use blob storage. If False, will use data_folder_path and run/store locally. Defaults to False.

    Returns:
        pd.DataFrame: output dataframe
    """
    if blob:
        blob_ep = BlobEndpoint(
            account="cfadatalakeprd",
            container="cfapredict",
            prefix="dataops/scenarios/workflows/covid/data",
        )
    else:
        blob_ep = None
    vac_raw = import_vaccination_data(
        start, data_folder_path, blob_ep
    )  # history: unprocessed vaccination data for all dates and age groups
    pop = import_population_data(
        state, age_groups
    )  # pop: census data of each age group
    vac_history, vac = format_vac_data(
        vac_raw, start, doses
    )  # vac_history: Vaccination for all dates for all age groups, vac: vaccination rate for 'start' date

    variant = import_variant_data(
        start, state
    )  # variant: processed data of proportion of variants that are omicron and non-omicron
    hospital = import_hospitalization_data(
        start, data_folder_path, state, blob_ep
    )  # hospital: weekly hospital data non-aged
    sero = import_sero_data(
        state
    )  # sero : national seroprevelence with no age groups
    cases = total_cases_using_hospital_and_sero(
        hospital, sero, pop
    )  # cases: estimated total cases using sero for total case estimates and hospitalization for distriubtion of cases
    infect = cases_by_variant(
        cases, variant, pop, variant_types
    )  # infect: total cases by variant per week
    infections = individual_infection_totals(
        infect, pop, variant_types
    )  # infections : number of people by age group with no infections, pre-omicron, omicron, or both at time 'start'
    output = output_generation(
        infections, vac, pop, doses, variant_types
    )  # output: proportion of people by age group based on doses and infections
    # tslie = get_tslie(infect, vac_history, output, pop) # tslie: number of people by age group
    new_output = get_tslie2(
        infect, vac_history, output, pop, start, tslie_ranges
    )
    return new_output


def save_solution(
    output: pd.DataFrame,
    output_folder_path: str,
    state: pd.Series,
    blob: bool = False,
) -> str:
    """
    save_solution
    The function saves the output of the run module to a specified folder. It will save the output to a blob storage if blob is True, otherwise it will save it locally.

    Args:
        output (pd.DataFrame): output dataframe to save
        output_folder_path (str): path to the folder where the output will be saved if blob is False
        state (pd.Series): series object of state information
        blob (bool, optional): whether to save to Blob Storage. Defaults to False.

    Returns:
        str: filename of the saved output
    """
    if blob:
        gen_data_blob = BlobEndpoint(
            account="cfadatalakeprd",
            container="cfapredict",
            prefix="dataops/scenarios/workflows/covid/storage",
        )
        d = get_today_date()
        filename = f"run_{d}/output/{state.stname}_initialization.csv".replace(
            " ", "_"
        )
        gen_data_blob.write_blob(
            file_buffer=output.to_csv(),
            path_after_prefix=filename,
        )
        return filename

    else:
        folder_path = (
            output_folder_path
            + "run_"
            + datetime.today().strftime("%Y_%m_%d")
            + "/"
        )
        os.makedirs(os.path.dirname(folder_path), exist_ok=True)
        output_storage = folder_path + "output/"
        os.makedirs(os.path.dirname(output_storage), exist_ok=True)
        filename = output_storage + state.stname + "_initialization.csv"
        filename = filename.replace(" ", "_")
        output.to_csv(filename, index=False)
        return folder_path


def copy_data(folder_path: str, data_folder_path: str, blob: bool = False):
    """
    copy_data
    This function copies data from the data_folder_path to the folder_path if blob is False.

    Args:
        folder_path (str): path to the folder where the data will be copied
        data_folder_path (str): path to the folder where the data is stored
        blob (bool, optional): whether to save to Blob. Defaults to False.
    """
    if blob:
        pass
    else:
        data_storage = folder_path + "data/"
        os.makedirs(os.path.dirname(data_storage), exist_ok=True)
        copytree(data_folder_path, data_storage, dirs_exist_ok=True)


def run(
    doses: int,
    start: str,
    state_sample: list[str],
    age_groups: list[int],
    tslie_ranges: list[int],
    variant_types: list[str],
    data_folder_path: str,
    output_folder_path: str,
    blob: bool = False,
) -> pd.DataFrame:
    """
    run
    The main function for the run module. It takes in a number of parameters and runs the initialization process. It will save the output to a specified folder or blob storage.

    Args:
        doses (int): number of doses to track
        start (str): start date for tracking data
        state_sample (list[str]): list of states to track in abbreviated form, such as "CA". "All" will run all states.
        age_groups (list[int]): list of upper limits of each age group, such as [17, 49, 64, 100]
        tslie_ranges (list[int]): list of upper limits of each tslie group, such as [69, 123, 150]
        variant_types (list[str]): list of variant types, such as ["Omicron", "Other"]
        data_folder_path (str): path to the data folder
        output_folder_path (str): _ath to the folder where the output will be saved
        blob (bool, optional): whether to save to Blob Storage. Defaults to False.

    Returns:
        pd.DataFrame: output dataframe
    """
    ##### The inputs for the "run" function must be inputted into a "initialization_config.json" file with the formatting of each variable as below.
    # doses: int, from 0 to 3 which accounts for how many doses of vaccination should be tracked by an individual
    # start: str, should be a date in format "2022-02-12" which will be the first date in which data will be tracked
    # state_sample: list of strings, using USPS code for each state that is desired to run, or US for national. Ex: ["AL", "AK", "CA"]. Also, can use ["All"] to get all 50 states and National initialization
    # age_groups: list of ints, formated like [17, 49, 64]
    # tslie_ranges: list of ints, formatted [69, 123, 150] which will output results of initialization such that time since last immunization event bins are of 0-69 days, 70-123 days, 123-150 days, and 151+ days as the bins for a SIR model
    # variant_types: list of strings, formatted ["Omicron", "Other"] which will identify new infections of individuals by variant. "Other" is all variants not included in the list.
    # data_folder_path: string, formatted like "covid/data/". This is the relativistic folder location of the data, which should be defaulted as the output of generated_data.py file
    # output_folder_path: sting, formatted like "covid/storage/". This is the relativistic folder loction to the deisred folder where the output will be saved to. This is currently in a generic folder named "storage".
    if blob:
        # check blob existence for generate_data
        gen_data_blob = BlobEndpoint(
            account="cfadatalakeprd",
            container="cfapredict",
            prefix="dataops/scenarios/workflows/covid/data",
        )
        check_blob = check_blob_date_exists(gen_data_blob)
        if not check_blob:
            print("Please run `generate_data` before running `run`")
            return None
    all_states = get_data("fips_to_name")
    if "All" in state_sample:
        for i in tqdm(range(len(all_states))):
            state = all_states.loc[i, :]
            if "ND" in state.stusps:
                state.stusps = "R8"
            if state.stusps == "DC":
                print("No Sero Data for DC")
            else:
                output = running_subfunctions(
                    doses,
                    start,
                    age_groups,
                    tslie_ranges,
                    variant_types,
                    data_folder_path,
                    state,
                    blob=blob,
                )
                new_output = output.drop("percent", axis=1)

                folder_path = save_solution(
                    new_output, output_folder_path, state, blob=blob
                )
    else:
        state_sample = list(set(state_sample) & set(all_states.stusps))
        all_states = all_states[all_states.stusps.isin(state_sample)]
        all_states = all_states.reset_index(drop=True)
        for i in tqdm(range(len(state_sample))):
            state = all_states.loc[i, :]
            if "ND" in state.stusps:
                state.stusps = "R8"
            if state.stusps == ["DC"]:
                print("No Sero Data for DC")
            else:
                output = running_subfunctions(
                    doses,
                    start,
                    age_groups,
                    tslie_ranges,
                    variant_types,
                    data_folder_path,
                    state,
                    blob=blob,
                )
                new_output = output.drop("percent", axis=1)
                folder_path = save_solution(
                    new_output, output_folder_path, state, blob=blob
                )
    # copy data to  used in the run
    copy_data(folder_path, data_folder_path, blob)
    return new_output


def read_and_validate_config(config_path: str) -> tuple[dict, list[str]]:
    """
    read_and_validate_config
    This function reads the config file and validates the necessary arguments for the run function. It will raise an assertion error if any of the necessary arguments are not found in the config file.

    Args:
        config_path (str): path to config json file

    Returns:
        tuple[dict, list[str]]: returns the json object and the list of necessary arguments
    """
    j = json.load(open(config_path))
    necessary_args = list(inspect.signature(run).parameters.keys())
    necessary_args.remove("blob")
    for arg in necessary_args:
        assert arg in j.keys(), (
            "Necessary parameter not found in config file %s" % arg
        )

    return j, necessary_args


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        help="path to the config file",
        default="cfa/scenarios/dataops/workflows/covid/initialization_config.json",
    )
    parser.add_argument(
        "-b",
        "--blob",
        action="store_true",
        help="flag to store generated data to Blob Storage",
    )
    args = parser.parse_args()
    blob = args.blob
    config = args.config
    j, necessary_args = read_and_validate_config(config)
    arguments = {k: j[k] for k in necessary_args}
    output = run(**arguments, blob=blob)

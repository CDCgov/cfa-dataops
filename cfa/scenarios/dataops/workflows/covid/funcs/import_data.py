import pandas as pd

from cfa.scenarios.dataops.datasets.catalog import BlobEndpoint, get_data
from cfa.scenarios.dataops.etl.utils import get_today_date


def import_vaccination_data(
    start: str, data_folder_path: str, blob_ep: BlobEndpoint = None
) -> pd.DataFrame:
    """
    import_vaccination_data
    This function imports the vaccination data from the CDC website. The data is
    then sorted by date and reset index. The data is then filtered to only include
    the data up to the start date. The data is then returned.
    If a BlobEndpoint is provided, the data is read from the blob storage.

    Args:
        start (str): The start date for the data to be imported in format yyyy-mm-dd.
        data_folder_path (str): The path to the data folder.
        blob_ep (BlobEndpoint, optional): an instance of BlobEndpoint. Defaults to None.

    Returns:
        pd.DataFrame: The vaccination data as a pandas DataFrame.
    """
    if blob_ep:
        d = get_today_date()
        vac_raw = blob_ep.read_csv(f"{d}/vaccination_data.csv")
    else:
        # Vaccination data from https://data.cdc.gov/Vaccinations/COVID-19-Vaccination-Age-and-Sex-Trends-in-the-Uni/5i5k-6cmh
        filename = data_folder_path + "vaccination_data.csv"
        vac_raw = pd.read_csv(filename)
    vac_raw = vac_raw.sort_values(by=["date"], ascending=True)
    vac_raw = vac_raw.reset_index(drop=True)
    vac_raw = vac_raw[vac_raw.date <= start]
    return vac_raw


def import_population_data(
    state: pd.Series, age_groups: list[int]
) -> pd.DataFrame:
    """
    import_population_data
    This functionuses US age distribution data to create population info based on state and age groups.

    Args:
        state (pd.Series): a state series from the fips_to_name dataset:
        age_groups (list[int]): a list integers representing upper limit of age groups

    Returns:
        pd.DataFrame: dataframe of population data
    """
    age_distrbution = get_data("us_age_distribution")
    stname = state.stname.replace(" ", "_")
    age_distribution = age_distrbution[age_distrbution.region == stname]
    age_distribution = age_distribution[["age", "census"]]

    for i in range(len(age_groups) + 1):
        if i == 0:
            age_group_list = ["0-" + str(age_groups[0])]
        elif i == (len(age_groups)):
            age_group_list.append(str(age_groups[i - 1] + 1) + "+")
        else:
            age_group_list.append(
                str(age_groups[i - 1] + 1) + "-" + str(age_groups[i])
            )
    pop = pd.DataFrame(
        {"age": age_group_list, "proportion": 0.0, "census": 0}
    )  # Extracting the different age groups present and their respective population total
    for i in range(len(pop)):
        if i == len(pop) - 1:
            pop.loc[i, "census"] = sum(age_distrbution.census) - sum(
                pop.census
            )
        else:
            pop.loc[i, "census"] = sum(
                age_distrbution[age_distrbution.age <= age_groups[i]].census
            ) - sum(pop.census)
    pop.proportion = pop.census / sum(
        pop.census
    )  # Calculating proportion of population each age group contains
    pop = pop.reset_index(drop=True)
    return pop


def import_variant_data(start: str, state: pd.Series) -> pd.DataFrame:
    """
    import_variant_data
    This function imports the variant data from blob storage and combines with region data to filter to the state(region) of interest.

    Args:
        start (str): start date in form yyyy-mm-dd
        state (pd.Series): a state series from the fips_to_name dataset

    Returns:
        pd.DataFrame: _description_
    """
    # Variant data from https://data.cdc.gov/Laboratory-Surveillance/SARS-CoV-2-Variant-Proportions/jr58-6ysp/data_preview
    # Proportion of Omicron vs non-omicron over time
    region_id = get_data("fips_to_name_improved")
    state_region = region_id[region_id.stname == state.stname].region
    variant = get_data("omicron_variant_regions")
    # variant data with columns: date, variant, proportion
    variant = variant[variant.region == state_region.values[0]]
    variant = variant.sort_values(by=["date"], ascending=True)
    variant = variant.reset_index(drop=True)
    variant = variant[variant.date <= start]
    return variant


def import_hospitalization_data(
    start: str,
    data_folder_path: str,
    state: pd.Series,
    blob_ep: BlobEndpoint = None,
) -> pd.DataFrame:
    """
    import_hospitalization_data
    This function imports the hospitalization data for the specified state from blob storage or local directory.

    Args:
        start (str): start date in form yyyy-mm-dd
        data_folder_path (str): path where local data is stored
        state (pd.Series): a state series from the fips_to_name dataset
        blob_ep (BlobEndpoint, optional): instance of BlobEndpoint. Defaults to None.

    Returns:
        pd.DataFrame: _description_
    """
    if blob_ep:
        d = get_today_date()
        path = f"{d}/weekly_hospital_{state.stname.lower()}.csv"
        path = path.replace(" ", "_")
        hospital = blob_ep.read_csv(path)
    else:
        # Hospitalization data from https://covid.cdc.gov/covid-data-tracker/#trends_weeklyhospitaladmissions_select_00
        filename = (
            data_folder_path
            + "weekly_hospital_"
            + state.stname.lower()
            + ".csv"
        )
        filename = filename.replace(" ", "_")
        hospital = pd.read_csv(
            filename
        )  # weekly hospitalization data with columns: date, hospital_weekly
    hospital.columns = ["date", "state", "hospital_weekly"]
    hospital = hospital.dropna()  # Removing NA entirely since can't make any assesment about how many infections prior
    hospital = hospital.sort_values(by=["date"], ascending=True)
    hospital = hospital.reset_index(drop=True)
    hospital = hospital[hospital.date <= start]
    return hospital


def import_sero_data(state: pd.Series) -> pd.DataFrame:
    """
    import_sero_data
    This function imports the serorprevalence data from blob storage and filters to the state of interest.


    Args:
        state (pd.Series): a state series from the fips_to_name dataset

    Returns:
        pd.DataFrame: dataframe of seroprevalence data for the state of interest
    """
    # Sero from https://covid19serohub.nih.gov/
    # sero = pd.read_csv('seroprevolence_average.csv') # National Seroprevolence average with columns: date, sero
    # Data from https://covid.cdc.gov/covid-data-tracker/?ref=dailybrief.net#national-lab
    sero = get_data("seroprevalence_50states")
    sero.columns = ["state", "age", "date", "sero"]
    sero = sero[sero.state == state.stusps]
    sero = sero.reset_index(drop=True)
    sero.columns = ["state", "age", "date", "sero"]
    return sero

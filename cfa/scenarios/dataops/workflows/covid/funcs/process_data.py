import pandas as pd


def total_cases_using_hospital_and_sero(
    hospital: pd.DataFrame, sero: pd.DataFrame, pop: pd.DataFrame
) -> pd.DataFrame:
    """
    total_cases_using_hospital_and_sero
    This function combines the dataframes to produce total cases using seroprevelence and hospitalization data.
    Args:
        hospital (pd.DataFrame): hospitalization data
        sero (pd.DataFrame): seroprevelence data
        pop (pd.DataFrame): population data

    Returns:
        pd.DataFrame: dataframe of total cases
    """
    for age_group in range(len(pop)):
        if age_group == 0:
            cases = pd.DataFrame(
                {
                    "date": hospital.date,
                    "age": pop.age[0],
                    "cases": hospital.hospital_weekly,
                }
            )  # Store estimated cases for each day where hospitalization data is known
            len_cases = len(cases)
        else:
            cases = pd.concat(
                [
                    cases,
                    pd.DataFrame(
                        {
                            "date": hospital.date,
                            "age": pop.age[age_group],
                            "cases": hospital.hospital_weekly,
                        }
                    ),
                ],
                ignore_index=True,
            )
    for j in range(len(pop)):
        sero_age = sero[sero.age == pop.age[j]]
        sero_age = sero_age.reset_index(drop=True)
        lower_infections = (
            sero_age.sero[0] * pop.census[j]
        )  # Estimated population that has been infected using sero prevelence at first time step in data
        upper_infections = (
            sero_age.sero[1] * pop.census[j]
        )  # Estimated population that has been infected using sero prevelence at second time step in data
        if upper_infections < lower_infections:
            upper_infections = lower_infections
        lower_index = 0
        lower = sero_age.date[
            lower_index
        ]  # Closest date of sero data less than hosiptalization date
        upper = sero_age.date[
            lower_index + 1
        ]  # Closest date of sero data less than hosiptalization date
        for i in range(
            len(hospital)
        ):  # Going through each day where we have hospitalization data and scale to match based on closest sero data
            if cases.date[i] > upper:
                if cases.date[i] > max(sero_age.date):
                    cases.loc[i + j * len_cases, "cases"] = cases.loc[
                        i + j * len_cases - 1, "cases"
                    ]
                else:
                    lower_index = (
                        lower_index + 1
                    )  # Adjusts lower_index if current date is below upper_index
                    lower = sero_age.date[
                        lower_index
                    ]  # Identifying new closest sero date below
                    upper = sero_age.date[
                        lower_index + 1
                    ]  # Identifying new closest sero date above
                    lower_infections = (
                        sero_age.sero[lower_index] * pop.census[j]
                    )
                    upper_infections = (
                        sero_age.sero[lower_index + 1] * pop.census[j]
                    )
                    if upper_infections < lower_infections:
                        upper_infections = lower_infections
                    a = hospital[
                        hospital.date > lower
                    ]  # Finding all hospitalization data between the two sero dates
                    a = a[a.date <= upper]
                    multiplier = (
                        (upper_infections - lower_infections)
                        / max(sum(a.hospital_weekly), 1)
                    )  # Multiplier to scale the hospitalization data such that it sums to the new infections between two sero dates
                    cases.loc[i + j * len_cases, "cases"] *= multiplier
            else:
                a = hospital[hospital.date > lower]
                a = a[a.date <= upper]
                multiplier = (
                    (upper_infections - lower_infections)
                    / max(sum(a.hospital_weekly), 1)
                )  # Multiplier to scale the hospitalization data such that it sums to the new infections between two sero dates
                cases.loc[i + j * len_cases, "cases"] *= multiplier
    return cases


def cases_by_variant(
    cases: pd.DataFrame,
    variant: pd.DataFrame,
    pop: pd.DataFrame,
    variant_types: list[str],
) -> pd.DataFrame:
    """
    cases_by_variant
    This function takes the cases and variant dataframes and combines them to produce a dataframe of cases by variant.

    Args:
        cases (pd.DataFrame): dataframe output from total_cases_using_hospital_and_sero
        variant (pd.DataFrame): variant dataframe from import_variant_data
        pop (pd.DataFrame): population dataframe
        variant_types (list[str]): list of variant types

    Returns:
        pd.DataFrame: dataframe of cases by variant
    """
    index = 0  # Identifying location of most recent variant proportion to use in event that variant date is not identical to hospitalization date
    num_variants = len(variant_types)
    for i in range(len(pop)):
        df_cases = cases[cases.age == pop.age[i]]
        df_cases = df_cases.reset_index(drop=True)
        df = pd.DataFrame(
            {
                "date": df_cases.date,
                "cases": df_cases.cases,
                "age": pop.age[i],
                "strain": variant_types[0],
            }
        )  # Storing total cases each date and whether they are certain variant
        if num_variants > 1:
            n = len(df)
            for q in range(1, num_variants):
                df = pd.concat(num_variants * [df], ignore_index=True)
                df.loc[q * n : (q + 1) * n, "strain"] = variant_types[
                    q
                ]  # Adding rows to included data for other variants
        for j in range(n):
            if (
                variant.date[0] > df.date[j]
            ):  # If variant data is unknown, then no cases, due to data being oldest data
                df.loc[j, "cases"] = 0
            else:
                a = variant[variant.date == df.date[j]]
                for cur_variant in range(num_variants):
                    if (
                        len(a) == 0
                    ):  # if variant date and case dates don't match exactly, uses most recent proportion
                        a = variant.iloc[index : index + num_variants]
                    else:
                        index = a.index[0]
                    b = a[a.variant == variant_types[cur_variant]].reset_index(
                        drop=True
                    )
                    df.loc[j + n * cur_variant, "cases"] *= b.proportion[
                        0
                    ]  # Proportion of given variant

        if i == 0:  # Adding each age group together
            infect = df
        else:
            infect = pd.concat([infect, df], ignore_index=True)
    return infect


def individual_infection_totals(
    infect: pd.DataFrame, pop: pd.DataFrame, variant_types: list[str]
) -> pd.DataFrame:
    """
    individual_infection_totals
    This function takes the infection data and population data and combines them to produce a dataframe of individual infection totals.

    Args:
        infect (pd.DataFrame): infection dataframe
        pop (pd.DataFrame): population dataframe
        variant_types (list[str]): list of variant types

    Returns:
        pd.DataFrame: individual infections totals dataframe
    """
    type = [
        "None"
    ]  # Options of type of infections, where Both means an individual got Omicron and Pre-Omicron
    type.extend(variant_types)
    if len(variant_types) > 1:
        type.extend(["Both"])
    for i in range(
        len(pop)
    ):  # Seperating between types assuming likelihood of being infected by either infection type is independent
        df = infect[infect.age == pop.age[i]]
        group_total = [0] * len(type)
        group_percent = [0] * len(type)
        absolute_cases = [0] * len(variant_types)
        if len(type) > 2:
            for j in range(len(variant_types)):
                group_total[j + 1] = sum(
                    df[df.strain == variant_types[j]].cases
                )
                group_total[-1] += group_total[j + 1]
                absolute_cases[j] = group_total[j + 1]

            for j in range(len(variant_types)):
                group_total[j + 1] *= (
                    1
                    - (sum(absolute_cases) - absolute_cases[j]) / pop.census[i]
                )
                group_total[-1] -= group_total[j + 1]

            group_total[0] = pop.census[i] - sum(group_total)
            group_percent = group_total / pop.census[i]

            if i == 0:  # Combining data set for each age group
                infections = pd.DataFrame(
                    {
                        "age": pop.age[i],
                        "type": type,
                        "percent": group_percent,
                        "total": group_total,
                    }
                )
            else:
                infections = pd.concat(
                    [
                        infections,
                        pd.DataFrame(
                            {
                                "age": pop.age[i],
                                "type": type,
                                "percent": group_percent,
                                "total": group_total,
                            }
                        ),
                    ],
                    ignore_index=True,
                )

        else:
            group_total[1] = sum(df[df.strain == type[1]].cases)
            group_total[0] = pop.census[i] - group_total[1]
            group_percent = group_total / pop.census[i]

            if i == 0:  # Combining data set for each age group
                infections = pd.DataFrame(
                    {
                        "age": pop.age[i],
                        "type": type,
                        "percent": group_percent,
                        "total": group_total,
                    }
                )
            else:
                infections = pd.concat(
                    [
                        infections,
                        pd.DataFrame(
                            {
                                "age": pop.age[i],
                                "type": type,
                                "percent": group_percent,
                                "total": group_total,
                            }
                        ),
                    ],
                    ignore_index=True,
                )
    return infections

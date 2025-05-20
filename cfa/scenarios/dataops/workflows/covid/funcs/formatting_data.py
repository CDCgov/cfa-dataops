import pandas as pd


def format_vac_data(vac_raw: pd.DataFrame, start: str, doses: int):
    """
    format_vac_date
    This function takes the raw vaccination data and prepares it to vaccination history by dose.

    Args:
        vac_raw (pd.DataFrame): dataframe of raw vaccination data
        start (str): start date in the form yyyy-mm-dd
        doses (int): number of doeses

    Returns:
        pd.DataFrame: formatted vaccination data
    """
    days = vac_raw.date.unique()  # Track the unique days in 'history' data set
    for i in range(
        len(days)
    ):  # Identifying individuals with zero doses and identenfying who have exactly one dose for all days
        data = vac_raw[vac_raw.date == days[i]]
        data = data.sort_values(by=["age"], ascending=True)
        data = data.reset_index(drop=True)
        for j in range(max(data.dose) + 1):
            if j == 0:
                upper = data[data.dose == (j + 1)].reset_index(drop=True)
                vac = pd.DataFrame(
                    {
                        "date": days[i],
                        "age": upper.age,
                        "dose": j,
                        "percent": 1 - upper.percentage,
                        "total": upper.census - upper.total,
                    }
                )  # Generating data for individuals with zero doses from info on those with at least one dose
            elif j == (doses - 1):
                lower = data[data.dose == j].reset_index(drop=True)
                vac = pd.DataFrame(
                    {
                        "date": days[i],
                        "age": lower.age,
                        "dose": j,
                        "percent": lower.percentage,
                        "total": lower.total,
                    }
                )  # Vaccination data for j+ doses
            elif j >= doses:
                lower = data[data.dose == j].reset_index(drop=True)
                vac = pd.DataFrame(
                    {
                        "date": days[i],
                        "age": lower.age,
                        "dose": j,
                        "percent": lower.percentage,
                        "total": lower.total,
                    }
                )
            else:
                upper = data[data.dose == (j + 1)].reset_index(drop=True)
                lower = data[data.dose == j].reset_index(drop=True)
                vac = pd.DataFrame(
                    {
                        "date": days[i],
                        "age": lower.age,
                        "dose": j,
                        "percent": lower.percentage - upper.percentage,
                        "total": lower.total - upper.total,
                    }
                )  # Generating data for individuals with exactly j doses
            if (
                i == 0
            ):  # Combinging the vaccination data for each day and storing
                vac_history = vac.reset_index(drop=True)
            else:
                vac_history = pd.concat([vac_history, vac]).reset_index(
                    drop=True
                )
    vac = vac_history[vac_history.date == start].reset_index(drop=True)
    return vac_history, vac


def output_generation(
    infections: pd.DataFrame,
    vac: pd.DataFrame,
    pop: pd.DataFrame,
    doses: int,
    variant_types: list[str],
) -> pd.DataFrame:
    """
    output_generation
    This function takes infections data, vaccinationdata, population data and generates the output data for each age group, type of infection and dose of vaccine.

    Args:
        infections (pd.DataFrame): infections dataframe
        vac (pd.DataFrame): vaccination dataframe
        pop (pd.DataFrame): population dataframe
        doses (int): number of doses
        variant_types (list[str]): list of variant types, such "Other" or "Omicron"

    Returns:
        pd.DataFrame: output dataframe
    """
    type = ["None"]
    type.extend(variant_types)
    if len(variant_types) > 1:
        type.extend(["Both"])
    for j in range(
        len(pop)
    ):  # combining vaccination and infection data assuming each group is equally likely
        df_infections = infections[infections.age == pop.age[j]]
        df_vac = vac[vac.age == pop.age[j]]
        for i in range(len(type)):  # iteritively for each type of infection
            df1 = df_infections[df_infections.type == type[i]]
            df1 = df1.reset_index(drop=True)
            for n in range(doses):  # iteritively for each dose of vaccine
                df2 = df_vac[df_vac.dose == n]
                df2 = df2.reset_index(drop=True)
                if i + j + n == 0:
                    output = pd.DataFrame(
                        {
                            "age": df1.age,
                            "type": df1.type,
                            "dose": df2.dose,
                            "percent": df1.percent * df2.percent,
                        }
                    )
                else:
                    output = pd.concat(
                        [
                            output,
                            pd.DataFrame(
                                {
                                    "age": df1.age,
                                    "type": df1.type,
                                    "dose": df2.dose,
                                    "percent": df1.percent * df2.percent,
                                }
                            ),
                        ],
                        ignore_index=True,
                    )
    return output

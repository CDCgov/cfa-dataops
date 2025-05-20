from datetime import datetime, timedelta

import pandas as pd


def get_tslie(
    infect: pd.DataFrame,
    vac_history: pd.DataFrame,
    output: pd.DataFrame,
    pop: pd.DataFrame,
) -> pd.DataFrame:
    """
    get_tslie
    This function calculates the time since last immunization event (tslie) for each level in the population.

    Args:
        infect (pd.DataFrame): infection data
        vac_history (pd.DataFrame): vaccination history data
        output (pd.DataFrame): output dataframe from output_generation function
        pop (pd.DataFrame): population dataframe

    Returns:
        pd.DataFrame: tslie dataframe
    """
    all_days = list(
        set(list(set(infect.date)) + list(set(vac_history.date)))
    )  # Making list of every day that vaccination or infection data exists
    all_days.sort()
    tslie = pd.DataFrame(
        {"date": all_days, "age": pop.age[0], "people": 0.0}
    )  # Variable to account for time since last immunization event
    tslie = pd.concat(
        [
            tslie,
            pd.DataFrame({"date": all_days, "age": pop.age[1], "people": 0.0}),
            pd.DataFrame({"date": all_days, "age": pop.age[2], "people": 0.0}),
            pd.DataFrame({"date": all_days, "age": pop.age[3], "people": 0.0}),
        ],
        ignore_index=True,
    )
    dim_storage = pd.concat(
        [
            pd.DataFrame({"date": all_days, "age": pop.age[0], "people": 0.0}),
            pd.DataFrame({"date": all_days, "age": pop.age[1], "people": 0.0}),
            pd.DataFrame({"date": all_days, "age": pop.age[2], "people": 0.0}),
            pd.DataFrame({"date": all_days, "age": pop.age[3], "people": 0.0}),
        ],
        ignore_index=True,
    )

    for n in range(
        len(output)
    ):  # finding most recent event for each indiivdual, setting no events as first date with data
        dose = output.dose[n]
        age = output.age[n]
        if output.type[n] == "Both":
            type = "Omicron"
        else:
            type = output.type[n]

        if (
            dose == 0 and type == "None"
        ):  # If no vaccination or infection, setting tslie to first date
            tslie.loc[0, "people"] += (
                pop[pop.age == age].census * output.percent[n]
            )
        elif dose == 0:
            df_infect = infect[infect.age == age]
            df_infect = df_infect[df_infect.strain == type]
            df_infect = df_infect.reset_index(drop=True)
            df_infect.cases = df_infect["cases"].apply(
                lambda x: x
                * (
                    1
                    / sum(df_infect.cases)
                    * pop[pop.age == age].census
                    * output.percent[n]
                )
            )
            tslie_infect = dim_storage[dim_storage.date.isin(df_infect.date)]
            tslie_infect = tslie_infect[tslie_infect.age == age]
            df_infect.index = tslie_infect.index
            df_infect = (
                (tslie_infect.people + df_infect.cases) + dim_storage.people
            ).fillna(0)
            tslie.people += df_infect
        elif type == "None":
            df_vac = vac_history[vac_history.dose == dose]
            df_vac = df_vac[df_vac.age == age]
            df_vac = df_vac.reset_index(drop=True)
            df_vac["new"] = df_vac["total"].diff()
            df_vac.loc[0, "new"] = df_vac.total[0] * 1
            df_vac.new = df_vac["new"].apply(
                lambda x: x
                * (
                    1
                    / sum(df_vac.new)
                    * pop[pop.age == age].census
                    * output.percent[n]
                )
            )
            tslie_vac = dim_storage[dim_storage.date.isin(df_vac.date)]
            tslie_vac = tslie_vac[tslie_vac.age == age]
            df_vac.index = tslie_vac.index
            df_vac = (
                (tslie_vac.people + df_vac.new) + dim_storage.people
            ).fillna(0)
            tslie.people += df_vac
        else:
            df_vac = vac_history[vac_history.dose == dose]
            df_vac = df_vac[df_vac.age == age]
            df_vac = df_vac.reset_index(drop=True)
            df_vac["new"] = df_vac["total"].diff()
            df_vac.loc[0, "new"] = df_vac.total[0] * 1
            df_vac.new = df_vac["new"].apply(
                lambda x: x
                * 1
                / sum(df_vac.new)
                * pop[pop.age == age].census
                * output.percent[n]
            )
            tslie_vac = dim_storage[dim_storage.date.isin(df_vac.date)]
            tslie_vac = tslie_vac[tslie_vac.age == age]
            df_vac.index = tslie_vac.index
            df_vac = (
                (tslie_vac.people + df_vac.new) + dim_storage.people
            ).fillna(0)

            df_infect = infect[infect.age == age]
            df_infect = df_infect[df_infect.strain == type]
            df_infect = df_infect.reset_index(drop=True)
            df_infect.cases = df_infect["cases"].apply(
                lambda x: x
                * (
                    1
                    / sum(df_infect.cases)
                    * pop[pop.age == age].census
                    * output.percent[n]
                )
            )
            tslie_infect = dim_storage[dim_storage.date.isin(df_infect.date)]
            tslie_infect = tslie_infect[tslie_infect.age == age]
            df_infect.index = tslie_infect.index
            df_infect = (
                (tslie_infect.people + df_infect.cases) + dim_storage.people
            ).fillna(0)

            joint_dist = pd.DataFrame(
                {"date": dim_storage.date, "age": dim_storage.age, "prob": 0.0}
            )
            tot = sum(df_vac)
            for j in range(len(df_infect)):
                if j == 0:
                    joint_dist.loc[0, "prob"] = (
                        (df_infect[0]) / tot * (df_vac[0]) / tot
                    )
                else:
                    joint_dist.loc[j, "prob"] = (
                        (df_infect[j]) / tot * (df_vac[j]) / tot
                        + (df_infect[j]) / tot * sum(df_vac[0:j]) / tot
                        + sum(df_infect[0:j]) / tot * (df_vac[j]) / tot
                    )

            tslie.people += joint_dist.prob * tot
    return tslie


def get_tslie2(
    infect: pd.DataFrame,
    vac_history: pd.DataFrame,
    output: pd.DataFrame,
    pop: pd.DataFrame,
    start: str,
    tslie_ranges: list[int],
) -> pd.DataFrame:
    """
    get_tslie2
    This function calculates the time since last immunization event (tslie) for each level in the population.
    This function is a modified version of the get_tslie function that includes the tslie_ranges parameter.

    Args:
        infect (pd.DataFrame): infections dataframe
        vac_history (pd.DataFrame): vaccination history dataframe
        output (pd.DataFrame): output dataframe from output_generation function
        pop (pd.DataFrame): population dataframe
        start (str): start date in the format yyyy-mm-dd
        tslie_ranges (list[int]): list of upper limit of each tslie group

    Returns:
        pd.DataFrame: tslie dataframe
    """
    for i in range(len(tslie_ranges) + 1):
        if i == 0:
            tslie_group_name = ["tslie_group_0"]
        else:
            tslie_group_name.append("tslie_group_" + str(i))
        output[tslie_group_name[i]] = 0.0
    all_days = list(
        set(list(set(infect.date)) + list(set(vac_history.date)))
    )  # Making list of every day that vaccination or infection data exists
    all_days.sort()
    tslie = pd.DataFrame(
        {"date": all_days, "age": pop.age[0], "people": 0.0}
    )  # Variable to account for time since last immunization event
    tslie = pd.concat(
        [
            tslie,
            pd.DataFrame({"date": all_days, "age": pop.age[1], "people": 0.0}),
            pd.DataFrame({"date": all_days, "age": pop.age[2], "people": 0.0}),
            pd.DataFrame({"date": all_days, "age": pop.age[3], "people": 0.0}),
        ],
        ignore_index=True,
    )
    dim_storage = pd.concat(
        [
            pd.DataFrame({"date": all_days, "age": pop.age[0], "people": 0.0}),
            pd.DataFrame({"date": all_days, "age": pop.age[1], "people": 0.0}),
            pd.DataFrame({"date": all_days, "age": pop.age[2], "people": 0.0}),
            pd.DataFrame({"date": all_days, "age": pop.age[3], "people": 0.0}),
        ],
        ignore_index=True,
    )
    yng = tslie[tslie.age == pop.age[0]]
    age_group_len = len(yng)
    for j in range(len(tslie_ranges) + 1):
        if j == 0:
            index = [
                yng[
                    yng.date
                    >= str(
                        datetime.strptime(start, "%Y-%m-%d")
                        - timedelta(days=tslie_ranges[0] + 1)
                    )
                ].index
            ]
        elif j == len(tslie_ranges):
            index.append(
                yng[
                    yng.date
                    <= str(
                        datetime.strptime(start, "%Y-%m-%d")
                        - timedelta(days=tslie_ranges[j - 1] + 1)
                    )
                ].index
            )
        else:
            a = yng[
                yng.date
                <= str(
                    datetime.strptime(start, "%Y-%m-%d")
                    - timedelta(days=tslie_ranges[j - 1] + 1)
                )
            ]
            index.append(
                a[
                    a.date
                    >= str(
                        datetime.strptime(start, "%Y-%m-%d")
                        - timedelta(days=tslie_ranges[j] + 1)
                    )
                ].index
            )

    age_bin = pop.age

    for n in range(
        len(output)
    ):  # finding most recent event for each indiivdual, setting no events as first date with data
        dose = output.dose[n]
        age = output.age[n]
        for q in range(len(age_bin)):
            if age == age_bin[q]:
                age_index_adjustment = q

        if output.type[n] == "Both":
            type = "Omicron"
        else:
            type = output.type[n]

        if (
            dose == 0 and type == "None"
        ):  # If no vaccination or infection, setting tslie to first date
            tslie.loc[0, "people"] += (
                pop[pop.age == age].census.values[0] * output.percent[n]
            )
            output.loc[n, tslie_group_name[-1]] = output.percent[n] * 1
        elif dose == 0:
            df_infect = infect[infect.age == age]
            df_infect = df_infect[df_infect.strain == type]
            df_infect = df_infect.reset_index(drop=True)
            df_infect.cases = df_infect["cases"].apply(
                lambda x: x
                * (
                    1
                    / sum(df_infect.cases)
                    * pop[pop.age == age].census
                    * output.percent[n]
                )
            )
            tslie_infect = dim_storage[dim_storage.date.isin(df_infect.date)]
            tslie_infect = tslie_infect[tslie_infect.age == age]
            df_infect.index = tslie_infect.index
            df_infect = (
                (tslie_infect.people + df_infect.cases) + dim_storage.people
            ).fillna(0)
            df_infect[df_infect < 0] = 0
            tslie.people += df_infect
            for index_group in range(len(index)):
                output.loc[n, tslie_group_name[index_group]] = (
                    sum(
                        df_infect[
                            index[index_group]
                            + age_index_adjustment * age_group_len
                        ]
                    )
                    / max(1, sum(df_infect))
                    * output.percent[n]
                )
        elif type == "None":
            if dose == max(output.dose) & dose < max(vac_history.dose):
                newest_vac = vac_history[
                    vac_history.dose == max(vac_history.dose)
                ]
                newest_vac = newest_vac[newest_vac.age == age]
                df_vac = vac_history[vac_history.dose == dose]
                df_vac = df_vac[df_vac.age == age]
                df_vac = df_vac.reset_index(drop=True)
                newest_vac = newest_vac[newest_vac.date.isin(df_vac.date)]
                newest_vac = newest_vac.reset_index(drop=True)
                ratio_newest = max(newest_vac.total) / max(df_vac.total)

                newest_vac["new"] = newest_vac["total"].diff()
                newest_vac.loc[0, "new"] = newest_vac.total[0] * 1
                newest_vac.new = newest_vac["new"].apply(
                    lambda x: x
                    * 1
                    / sum(newest_vac.new)
                    * pop[pop.age == age].census
                    * output.percent[n]
                    * ratio_newest
                )

                df_vac["new"] = df_vac["total"].diff()
                df_vac.loc[0, "new"] = df_vac.total[0] * 1
                df_vac.new = df_vac["new"].apply(
                    lambda x: x
                    * 1
                    / sum(df_vac.new)
                    * pop[pop.age == age].census
                    * output.percent[n]
                    * (1 - ratio_newest)
                )

                tslie_vac = dim_storage[dim_storage.date.isin(df_vac.date)]
                tslie_vac = tslie_vac[tslie_vac.age == age]
                df_vac.index = tslie_vac.index
                tslie_newest = dim_storage[
                    dim_storage.date.isin(newest_vac.date)
                ]
                tslie_newest = tslie_newest[tslie_newest.age == age]
                df_vac.index = tslie_vac.index
                newest_vac.index = tslie_vac.index

                df_vac = (
                    (tslie_vac.people + df_vac.new + newest_vac.new)
                    + dim_storage.people
                ).fillna(0)
            else:
                df_vac = vac_history[vac_history.dose == dose]
                df_vac = df_vac[df_vac.age == age]
                df_vac = df_vac.reset_index(drop=True)
                df_vac["new"] = df_vac["total"].diff()

                df_vac.loc[0, "new"] = df_vac.total[0]
                df_vac.loc[df_vac.new < 0, "new"] = 0
                df_vac.new = df_vac["new"].apply(
                    lambda x: x
                    * (
                        1
                        / sum(df_vac.new)
                        * pop[pop.age == age].census
                        * output.percent[n]
                    )
                )
                tslie_vac = dim_storage[dim_storage.date.isin(df_vac.date)]
                tslie_vac = tslie_vac[tslie_vac.age == age]
                df_vac.index = tslie_vac.index
                df_vac = (
                    (tslie_vac.people + df_vac.new) + dim_storage.people
                ).fillna(0)
            tslie.people += df_vac
            for index_group in range(len(index)):
                output.loc[n, tslie_group_name[index_group]] = (
                    sum(
                        df_vac[
                            index[index_group]
                            + age_index_adjustment * age_group_len
                        ]
                    )
                    / max(1, sum(df_vac))
                    * output.percent[n]
                )
        elif dose == max(output.dose) & dose < max(vac_history.dose):
            newest_vac = vac_history[vac_history.dose == max(vac_history.dose)]
            newest_vac = newest_vac[newest_vac.age == age]
            df_vac = vac_history[vac_history.dose == dose]
            df_vac = df_vac[df_vac.age == age]
            df_vac = df_vac.reset_index(drop=True)
            newest_vac = newest_vac[newest_vac.date.isin(df_vac.date)]
            newest_vac = newest_vac.reset_index(drop=True)
            ratio_newest = max(newest_vac.total) / max(df_vac.total)

            newest_vac["new"] = newest_vac["total"].diff()
            newest_vac.loc[0, "new"] = newest_vac.total[0] * 1
            newest_vac.loc[newest_vac.new < 0, "new"] = 0
            newest_vac.new = newest_vac["new"].apply(
                lambda x: x
                * 1
                / sum(newest_vac.new)
                * pop[pop.age == age].census
                * output.percent[n]
                * ratio_newest
            )

            df_vac["new"] = df_vac["total"].diff()
            df_vac.loc[0, "new"] = df_vac.total[0] * 1
            df_vac.loc[df_vac.new < 0, "new"] = 0
            df_vac.new = df_vac["new"].apply(
                lambda x: x
                * 1
                / sum(df_vac.new)
                * pop[pop.age == age].census
                * output.percent[n]
                * (1 - ratio_newest)
            )

            tslie_vac = dim_storage[dim_storage.date.isin(df_vac.date)]
            tslie_vac = tslie_vac[tslie_vac.age == age]
            df_vac.index = tslie_vac.index
            tslie_newest = dim_storage[dim_storage.date.isin(newest_vac.date)]
            tslie_newest = tslie_newest[tslie_newest.age == age]
            df_vac.index = tslie_vac.index
            newest_vac.index = tslie_vac.index

            df_vac = (
                (tslie_vac.people + df_vac.new + newest_vac.new)
                + dim_storage.people
            ).fillna(0)

            df_infect = infect[infect.age == age]
            df_infect = df_infect[df_infect.strain == type]
            df_infect = df_infect.reset_index(drop=True)
            df_infect.cases = df_infect["cases"].apply(
                lambda x: x
                * (
                    1
                    / sum(df_infect.cases)
                    * pop[pop.age == age].census
                    * output.percent[n]
                )
            )
            tslie_infect = dim_storage[dim_storage.date.isin(df_infect.date)]
            tslie_infect = tslie_infect[tslie_infect.age == age]
            df_infect.index = tslie_infect.index
            df_infect = (
                (tslie_infect.people + df_infect.cases) + dim_storage.people
            ).fillna(0)

            joint_dist = pd.DataFrame(
                {"date": dim_storage.date, "age": dim_storage.age, "prob": 0.0}
            )
            tot_vac = max(sum(df_vac), 1)
            tot_infect = max(sum(df_infect), 1)
            for j in range(len(df_infect)):
                if j == 0:
                    joint_dist.loc[0, "prob"] = (
                        (df_infect[0]) / tot_infect * (df_vac[0]) / tot_vac
                    )
                else:
                    joint_dist.loc[j, "prob"] = (
                        (df_infect[j]) / tot_infect * (df_vac[j]) / tot_vac
                        + (df_infect[j])
                        / tot_infect
                        * sum(df_vac[0:j])
                        / tot_vac
                        + sum(df_infect[0:j])
                        / tot_infect
                        * (df_vac[j])
                        / tot_vac
                    )

            tslie.people += joint_dist.prob * tot_vac
            for tslie_group in range(len(tslie_ranges) + 1):
                if tslie_group == 0:
                    output.loc[n, tslie_group_name[tslie_group]] = sum(
                        joint_dist[
                            joint_dist.date
                            >= str(
                                datetime.strptime(start, "%Y-%m-%d")
                                - timedelta(days=tslie_ranges[0] + 1)
                            )
                        ].prob
                    ) / max(1, sum(joint_dist.prob) * output.percent[n])
                elif tslie_group == len(tslie_ranges):
                    output.loc[n, tslie_group_name[tslie_group]] = (
                        sum(
                            joint_dist[
                                joint_dist.date
                                <= str(
                                    datetime.strptime(start, "%Y-%m-%d")
                                    - timedelta(
                                        days=tslie_ranges[tslie_group - 1] + 1
                                    )
                                )
                            ].prob
                        )
                        * output.percent[n]
                    )
                else:
                    a = joint_dist[
                        joint_dist.date
                        <= str(
                            datetime.strptime(start, "%Y-%m-%d")
                            - timedelta(days=tslie_ranges[tslie_group - 1] + 1)
                        )
                    ]
                    output.loc[n, tslie_group_name[tslie_group]] = sum(
                        a[
                            a.date
                            >= str(
                                datetime.strptime(start, "%Y-%m-%d")
                                - timedelta(days=tslie_ranges[tslie_group] + 1)
                            )
                        ].prob
                    ) / max(1, sum(joint_dist.prob) * output.percent[n])
        else:
            df_vac = vac_history[vac_history.dose == dose]
            df_vac = df_vac[df_vac.age == age]
            df_vac = df_vac.reset_index(drop=True)
            df_vac["new"] = df_vac["total"].diff()
            df_vac.loc[0, "new"] = df_vac.total[0] * 1
            df_vac.loc[df_vac.new < 0, "new"] = 0
            df_vac.new = df_vac["new"].apply(
                lambda x: x
                * 1
                / sum(df_vac.new)
                * pop[pop.age == age].census
                * output.percent[n]
            )
            tslie_vac = dim_storage[dim_storage.date.isin(df_vac.date)]
            tslie_vac = tslie_vac[tslie_vac.age == age]
            df_vac.index = tslie_vac.index
            df_vac = (
                (tslie_vac.people + df_vac.new) + dim_storage.people
            ).fillna(0)

            df_infect = infect[infect.age == age]
            df_infect = df_infect[df_infect.strain == type]
            df_infect = df_infect.reset_index(drop=True)
            df_infect.cases = df_infect["cases"].apply(
                lambda x: x
                * (
                    1
                    / sum(df_infect.cases)
                    * pop[pop.age == age].census
                    * output.percent[n]
                )
            )
            tslie_infect = dim_storage[dim_storage.date.isin(df_infect.date)]
            tslie_infect = tslie_infect[tslie_infect.age == age]
            df_infect.index = tslie_infect.index
            df_infect = (
                (tslie_infect.people + df_infect.cases) + dim_storage.people
            ).fillna(0)
            df_infect[df_infect < 0] = 0

            joint_dist = pd.DataFrame(
                {"date": dim_storage.date, "age": dim_storage.age, "prob": 0.0}
            )
            tot_vac = max(sum(df_vac), 1)
            tot_infect = max(sum(df_infect), 1)
            for j in range(len(df_infect)):
                if j == 0:
                    joint_dist.loc[0, "prob"] = (
                        (df_infect[0]) / tot_infect * (df_vac[0]) / tot_vac
                    )
                else:
                    joint_dist.loc[j, "prob"] = (
                        (df_infect[j]) / tot_infect * (df_vac[j]) / tot_vac
                        + (df_infect[j])
                        / tot_infect
                        * sum(df_vac[0:j])
                        / tot_vac
                        + sum(df_infect[0:j])
                        / tot_infect
                        * (df_vac[j])
                        / tot_vac
                    )

            tslie.people += joint_dist.prob * tot_vac
            for tslie_group in range(len(tslie_ranges) + 1):
                if tslie_group == 0:
                    output.loc[n, tslie_group_name[tslie_group]] = sum(
                        joint_dist[
                            joint_dist.date
                            >= str(
                                datetime.strptime(start, "%Y-%m-%d")
                                - timedelta(days=tslie_ranges[0] + 1)
                            )
                        ].prob
                    ) / max(1, sum(joint_dist.prob) * output.percent[n])
                elif tslie_group == len(tslie_ranges):
                    output.loc[n, tslie_group_name[tslie_group]] = (
                        sum(
                            joint_dist[
                                joint_dist.date
                                <= str(
                                    datetime.strptime(start, "%Y-%m-%d")
                                    - timedelta(
                                        days=tslie_ranges[tslie_group - 1] + 1
                                    )
                                )
                            ].prob
                        )
                        * output.percent[n]
                    )
                else:
                    a = joint_dist[
                        joint_dist.date
                        <= str(
                            datetime.strptime(start, "%Y-%m-%d")
                            - timedelta(days=tslie_ranges[tslie_group - 1] + 1)
                        )
                    ]
                    output.loc[n, tslie_group_name[tslie_group]] = sum(
                        a[
                            a.date
                            >= str(
                                datetime.strptime(start, "%Y-%m-%d")
                                - timedelta(days=tslie_ranges[tslie_group] + 1)
                            )
                        ].prob
                    ) / max(1, sum(joint_dist.prob) * output.percent[n])

    return output

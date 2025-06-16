from typing import Optional

import altair as alt
import jax.numpy as jnp
import numpy as np
import pandas as pd
from tqdm import tqdm

from ...visualization.composition import merge_charts_in_pdf
from ...visualization.y_vs_datetime import (
    interval_chart,
    line_chart,
    point_chart,
)
from .files_io import (
    get_blob_tree,
    read_blob_json,
)

alt.data_transformers.disable_max_rows()


def state_process(
    state: str,
    blob_experiment_prefix: str,
    blob_account_name: str,
    blob_container_name: str,
    initial_date: pd.Timestamp,
    blob_tree: Optional[list] = None,
) -> dict:
    """
    Process the state data from the blob storage.

    Args:
        state (str): The state to process, typicaly in two letter abreviation (e.g., NY).
        blob_experiment_prefix (str, optional): The prefix for the blob experiment.
        blob_account_name (str, optional): The name of the blob account.
        blob_container_name (str, optional): The name of the blob container.
        initial_date (pd.Timestamp): The initial date for the analysis.
        blob_tree (list, optional): The list of blob paths. Defaults to None.
    """
    blob_path_posteriors = (
        f"{blob_experiment_prefix}{state}/predictive_output.json"
    )
    blob_path_samples = f"{blob_experiment_prefix}{state}/samples.json"

    if blob_tree:
        if (
            blob_path_posteriors not in blob_tree
            or blob_path_samples not in blob_tree
        ):
            raise ValueError(
                "Blob paths for states not found in the provided blob tree:"
                f"    {blob_path_posteriors}"
                f"    {blob_path_samples}"
            )

    posterior_pred = read_blob_json(
        blob_path=blob_path_posteriors,
        account_name=blob_account_name,
        container_name=blob_container_name,
    )

    sample_json = read_blob_json(
        blob_path=blob_path_samples,
        account_name=blob_account_name,
        container_name=blob_container_name,
    )

    posterior_pred.update((k, np.array(v)) for k, v in posterior_pred.items())
    fitted_medians = {
        k: jnp.median(jnp.array(v), axis=(0, 1))
        for k, v in sample_json.items()
    }
    fitted_medians["state"] = state

    data_dict = {
        "pred_hosps": posterior_pred["incidence"],
        "pred_hosps_all": jnp.sum(
            posterior_pred["incidence"], axis=2
        ),  # collapse age
        "pred_inf": posterior_pred["infection_incidence"],
        "pred_var": posterior_pred["variant_proportion"],
        "seasonality": posterior_pred["seasonality_mult"],
    }

    quantiles_dict = {}
    for k, v in data_dict.items():
        quantiles_dict[f"{k}_q05"] = np.quantile(v, 0.05, 0)
        quantiles_dict[f"{k}_q50"] = np.quantile(v, 0.50, 0)
        quantiles_dict[f"{k}_q95"] = np.quantile(v, 0.95, 0)

    # median_df = pd.DataFrame(fitted_medians, index=[state])

    obs_hosps_age = posterior_pred["obs_hosps_weekly"]
    obs_hosps_all = np.sum(obs_hosps_age, axis=-1)  # collapse age
    obs_var_prop = posterior_pred["obs_var_prop"]
    week0_day = posterior_pred["week0_day"]
    obs_hosps_dates = (
        posterior_pred["obs_hosps_weeks"] * 7 + week0_day
    ) * pd.Timedelta(days=1) + initial_date
    obs_var_dates = (
        posterior_pred["obs_var_weeks"] * 7 + week0_day
    ) * pd.Timedelta(days=1) + initial_date
    sim_hosps_dates = (
        posterior_pred["hosps_weeks"] * 7 + week0_day
    ) * pd.Timedelta(days=1) + initial_date
    sim_var_dates = (
        posterior_pred["var_weeks"] * 7 + week0_day
    ) * pd.Timedelta(days=1) + initial_date

    obs_hosps_coverage = np.mean(
        (
            obs_hosps_all
            > quantiles_dict["pred_hosps_all_q05"][0 : len(obs_hosps_all)]
        )
        & (
            obs_hosps_all
            < quantiles_dict["pred_hosps_all_q95"][0 : len(obs_hosps_all)]
        )
    )

    obs_var_coverage = np.mean(
        (obs_var_prop > quantiles_dict["pred_var_q05"][0 : len(obs_var_prop)])
        & (
            obs_var_prop
            < quantiles_dict["pred_var_q95"][0 : len(obs_var_prop)]
        )
    )

    obs_hosps_coverage = np.round(obs_hosps_coverage * 100, decimals=1)
    obs_var_coverage = np.round(obs_var_coverage * 100, decimals=1)

    df_hosp_sim = pd.DataFrame(
        dict(
            date=sim_hosps_dates,
            pred_hosps_q05=quantiles_dict["pred_hosps_all_q05"],
            pred_hosps_q50=quantiles_dict["pred_hosps_all_q50"],
            pred_hosps_q95=quantiles_dict["pred_hosps_all_q95"],
        )
    )

    df_hosp_obs = pd.DataFrame(
        dict(date=obs_hosps_dates, obs_hosps=obs_hosps_all)
    )

    df_var_sim = pd.concat(
        [
            pd.DataFrame(
                dict(
                    date=sim_var_dates,
                    pred_var_q50=quantiles_dict["pred_var_q50"].T[i],
                    pred_var_q05=quantiles_dict["pred_var_q05"].T[i],
                    pred_var_q95=quantiles_dict["pred_var_q95"].T[i],
                    variant=i,
                )
            )
            for i in range(quantiles_dict["pred_var_q50"].shape[1])
        ]
    )

    df_var_obs = pd.concat(
        [
            pd.DataFrame(
                dict(
                    date=obs_var_dates,
                    obs=posterior_pred["obs_var_prop"].T[i],
                    variant=i,
                )
            )
            for i in range(posterior_pred["obs_var_prop"].shape[1])
        ]
    )

    df_seasonality = pd.DataFrame(
        dict(
            date=sim_hosps_dates,
            seasonality_q50=quantiles_dict["seasonality_q50"],
            seasonality_q05=quantiles_dict["seasonality_q05"],
            seasonality_q95=quantiles_dict["seasonality_q95"],
        )
    )

    df_inf = pd.DataFrame(
        dict(
            date=sim_hosps_dates,
            pred_inf_q50=quantiles_dict["pred_inf_q50"],
            pred_inf_q05=quantiles_dict["pred_inf_q05"],
            pred_inf_q95=quantiles_dict["pred_inf_q95"],
        )
    )

    # plot 1: hosps
    title = f"Observed and Predicted Hospitalizations in {state}"
    hosps_plot = (
        line_chart(
            df=df_hosp_sim,
            dt_col="date",
            y_col="pred_hosps_q50",
            x_title="Date",
            y_title="Hospitalizations",
            title=title,
        )
        + interval_chart(
            df=df_hosp_sim,
            dt_col="date",
            y_col_lower="pred_hosps_q05",
            y_col_upper="pred_hosps_q95",
            x_title="Date",
            y_title="Hospitalizations",
            title=title,
        )
        + line_chart(
            df=df_hosp_obs,
            dt_col="date",
            y_col="obs_hosps",
            x_title="Date",
            y_title="Hospitalizations",
            title=title,
            strokeDash=[6, 6],
            strokeWidth=2,
            color="black",
        )
    )

    # plot 2: Infection incidence
    title = f"Predicted Infection Incidence in {state}"
    inf_plot = line_chart(
        df=df_inf,
        dt_col="date",
        y_col="pred_inf_q50",
        x_title="Date",
        y_title="Infection Incidence",
        title=title,
    ) + interval_chart(
        df=df_inf,
        dt_col="date",
        y_col_lower="pred_inf_q05",
        y_col_upper="pred_inf_q95",
        x_title="Date",
        y_title="Infection Incidence",
        title=title,
    )

    # plot 3: variant proportion
    title = f"Observed and Predicted Variant Proportions in {state}"
    var_plot = (
        line_chart(
            df=df_var_sim,
            dt_col="date",
            y_col="pred_var_q50",
            color_col="variant:N",
            legend_title="Variant",
            x_title="Date",
            y_title="Variant Proportions",
            title=title,
        )
        + interval_chart(
            df=df_var_sim,
            dt_col="date",
            y_col_lower="pred_var_q05",
            y_col_upper="pred_var_q95",
            color_col="variant:N",
            legend_title="Variant",
            x_title="Date",
            y_title="Variant Proportions",
            title=title,
        )
        + point_chart(
            df=df_var_obs,
            dt_col="date",
            y_col="obs",
            color_col="variant:N",
            legend_title="Variant",
            x_title="Date",
            y_title="Variant Proportions",
            title=title,
        )
    )

    # plot 4: seasonality
    title = f"Predicted Seasonality Multiplier in {state}"
    seasonality_plot = alt.Chart(df_seasonality).mark_line().encode(
        x=alt.X("date:T", title="Date"),
        y=alt.Y("seasonality_q50:Q", title="Seasonality Mult.").scale(
            zero=False
        ),
    ).properties(title=title, width=900, height=200) + alt.Chart(
        df_seasonality
    ).mark_errorband().encode(
        x=alt.X("date:T", title="Date"),
        y=alt.Y("seasonality_q05:Q", title="Seasonality Mult.").scale(
            zero=False
        ),
        y2="seasonality_q95:Q",
    ).properties(title=title, width=900, height=200)

    each_plot_props = dict(height=200, width=800)

    return alt.vconcat(
        hosps_plot.properties(**each_plot_props),
        inf_plot.properties(**each_plot_props),
        var_plot.properties(**each_plot_props),
        seasonality_plot.properties(**each_plot_props),
    ).resolve_scale(x="shared")


def main() -> None:
    """
    Main function to process all states and generate plots.

    Args:
        blob_experiment_prefix (str): The prefix for the blob experiment.
        blob_account_name (str): The name of the blob account.
        blob_container_name (str): The name of the blob container.
        report_out_path (str): The path to save the report.
        initial_date (str): The initial date for the analysis in 'YYYY-MM-DD' format.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Process state data and generate report."
    )
    parser.add_argument(
        "--blob_experiment_prefix",
        "-b",
        type=str,
        required=True,
        help="Blob experiment prefix.",
    )
    parser.add_argument(
        "--blob_account_name",
        "-a",
        type=str,
        required=True,
        help="Blob account name.",
    )
    parser.add_argument(
        "--blob_container_name",
        "-c",
        type=str,
        required=True,
        help="Blob container name.",
    )
    parser.add_argument(
        "--report_out_path",
        "-p",
        type=str,
        required=True,
        help="Path to save the report.",
    )
    parser.add_argument(
        "--initial_date",
        "-i",
        type=str,
        required=True,
        help="Initial date for the analysis in 'YYYY-MM-DD' format.",
    )

    args = parser.parse_args()
    initial_date = pd.to_datetime(args.initial_date)

    blob_tree = get_blob_tree(
        start_blob_path=args.blob_experiment_prefix,
        account_name=args.blob_account_name,
        container_name=args.blob_container_name,
        glob_path="**/*.json",
    )
    all_states = sorted(set([i.split("/")[-2] for i in blob_tree]))

    pages = []
    print("Processing state data ...")
    for state in tqdm(all_states):
        try:
            pages.append(
                state_process(
                    state=state,
                    blob_experiment_prefix=args.blob_experiment_prefix,
                    blob_account_name=args.blob_account_name,
                    blob_container_name=args.blob_container_name,
                    initial_date=initial_date,
                    blob_tree=blob_tree,
                )
            )
        except Exception as e:
            print(f"Error processing state {state}: {e}")
    print(
        f"Saving {len(all_states)} state report to: {args.report_out_path} ..."
    )
    merge_charts_in_pdf(charts=pages, filename=args.report_out_path)


if __name__ == "__main__":
    main()

from .funcs import utils as utils
from argparse import ArgumentParser, Namespace
import pandas as pd
from pathlib import Path

def run(scenario, state, output, input_dir, output_dir, final_vax_date):
    print(scenario, state, output, input_dir, output_dir)
    #read and process source data
    covid_vax_2324 = pd.read_csv(Path("cfa/scenarios/dataops/workflows/vax_hill_fitting/data/COVID_RD18_Vaccination_curves.csv"))
    covid_vax_2324 = utils.format_vax_2324(covid_vax_2324)
    print(covid_vax_2324.head())
    scenario = scenario.lower()

    if state.lower() == "all":
        state_nameids = utils.state_nameids
    elif utils.state_name_to_nameid(state) in utils.state_nameids:
        state_nameids = [utils.state_name_to_nameid(state)]
    else:
        raise ValueError(f"State '{state}' not recognized. Please provide a valid state abbreviation.")

    #loop through scenarios
    if scenario == "low" or scenario == "all":
        print("Processing low scenario data...")
        vax_low = pd.read_csv(Path(input_dir) / "vax_low.csv")
        vax_low = utils.process_vax_data(vax_low)
        print(vax_low.head())
        vax_low = utils.add_vax_2324(vax_low, covid_vax_2324)

        # Fit the scenario
        utils.fit_scenario(
            vax_low,
            "low",
            final_vax_date,
            state_nameids,
            utils.age_factor_levels,
            output_dir
            )
    if scenario == "med" or scenario == "all":
        vax_med = pd.read_csv(Path(input_dir) / "vax_med.csv")
        vax_med = utils.process_vax_data(vax_med)
        vax_med = utils.add_vax_2324(vax_med, covid_vax_2324)
        # Fit the scenario
        utils.fit_scenario(
            vax_med,
            "med",
            final_vax_date,
            state_nameids,
            utils.age_factor_levels,
            output_dir
            )
    if scenario == "high" or scenario == "all":
        vax_high = pd.read_csv(Path(input_dir) / "vax_high.csv")
        vax_high = utils.process_vax_data(vax_high)
        vax_high = utils.add_vax_2324(vax_high, covid_vax_2324)
        # Fit the scenario
        utils.fit_scenario(
            vax_high,
            "high",
            final_vax_date,
            state_nameids,
            utils.age_factor_levels,
            output_dir
            )


if __name__ == "__main__":
    #setup argparsers
    parser = ArgumentParser(description="Run the vaccine hill fitting process.")
    parser.add_argument("-s", "--scenario", type=str, default = "all",required=False, help="options are all, low, med, high")
    parser.add_argument("--state", type=str, default = "all",required=False, help="state name abbreviation or all")
    parser.add_argument("-o","--output", type=str, default = "both",required=False, help="type of output to produce. Options are json, pdf, or both")
    parser.add_argument("-id","--input_dir", type=str, default = "data",required=False, help="input directory")
    parser.add_argument("-od","--output_dir", type=str, default = "scenarios_new",required=False, help="output directory")
    parser.add_argument("-d", "--final_vax_date", type=str, default = "2025-06-30", required=False, help="final date for vaccination data in YYYY-MM-DD format")
    args = parser.parse_args()


    # Convert args to Namespace
    args = Namespace(
        scenario=args.scenario,
        state=args.state,
        output=args.output,
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        final_vax_date=args.final_vax_date
    )
    # Run the main function
    run(scenario = args.scenario,
        state = args.state,
        output = args.output,
        input_dir = args.input_dir,
        output_dir = args.output_dir,
        final_vax_date = args.final_vax_date)
    
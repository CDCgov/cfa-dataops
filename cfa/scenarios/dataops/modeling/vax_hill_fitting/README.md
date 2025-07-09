# Vax Hill Fitting

This modeling workflow was adapted from https://github.com/cdcent/scenarios_covideda/tree/master/vaxfitting and converted into python. It designed to be executed as a command line tool with vaccination data as inputs. It outputs

## How to Run

In order to run this modeling from the command line, the cfa_scenarios_dataops repo needs to be installed. In your terminal, run
```bash
pip install git+https://github.com/cdcent/cfa-scenarios-dataops.git
```

The workflow can be kicked off by running the following in the terminal:
```bash
python3 -m cfa.scenarios.dataops.modeling.vax_hill_fitting.run
```

By default, this command takes in vax data produced by the vaxfitting repo mentioned above stored in a folder called `data`. The following command line arguments can be used to customize the execution:
- -s or --scenario: options are `all`, `low`, `med`, or `high`. Default is `all`.
- --state: two letter state abbreviation to run  like `CA` for California, or `all` to run all states.
- -o or --output: type of output to save. Choices are `json`, `pdf`, or `both`. Default is `both`.
- -id or --input_dir: the input directory that holds vax_low.csv, vax_med.csv, or vax_high.csv, depending on the scenario being run. Default is `data`.
- -od or --output_dir: the output directory for output files. Default is `scenarios_new`.
- -d or --final_vax_date: the final vaccination date to use in yyyy-mm-dd format. Default is `2025-06-30`.

## Example
Suppose we want to run the vax hill fitting on low, med, and high scenarios for all states where the csv's are stored in a folder called vax_data, and output all information in a folder called scenarios_output. The following two command line executions will produce these results.

The simple way:
```
python3 -m cfa.scenarios.dataops.modeling.vax_hill_fitting \
    -id vax_data \
    -od scenarios_output
```

The more verbose way:
```
python3 -m cfa.scenarios.dataops.modeling.vax_hill_fitting \
    -s all \
    --state all \
    -id vax_data \
    -od scenarios_output \
    -o both
```

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Calendar Versioning](https://calver.org/).
The versioning pattern is `YYYY.MM.DD.micro(a/b/{none if release})

---

## [2025.08.15.0a]

## Fixes

- Typo in QAQC github workflow and testing operation

## [2025.08.14.2a]

### Fixes

- Iteration on last update since main is the default branch only needs to push to HEAD (not HEAD:<branch>)
- Changing condition to on pull_request closure instead of push to main

## [2025.08.14.1a]

### Fixes

- Conditional coverage badge update only when pulling to main

## [2025.08.14.0a]

- Updates to documentation
  - Reduced Getting Started material in `README.md`
  - Migration information that was in `README.md` to the developer guide
  - Removed duplicated user guide that was at the bottom of the developer guide
- Add support for mkdocs
  - Add CI to automatically serve/update the docs on GitHub Pages

## [2025.08.01.0a]

### Fixes

- pre-commit workflow for removing notebook outputs

## [2025.07.31.0a]

### Added

- `cfa.dataops.reporting` functionality to generate client-side rendering
  interactive html reports authored as jupyter notebooks. This conversion is invoked
  using the `reportcat` namespace. Example:

  ```python
  >>> from cfa.dataops.reporting import reportcat
  >>> reportcat.examples.dataset_report_ipynb.nb_to_html_file('tmp/test3.html')
  ```
  ``` bash
  Executing: 100%|█████████████████████████████████| 21/21 [00:17<00:00,  1.20cell/s]
  HTML report saved to /.../.../.../.../tmp/test3.html
  ```

## [2025.07.24.0a]

### Updated

- Modified `README.md` with edits, new repo name, and added ryan to list of maintainers

### Added

- `DISCLAIMER.md`


## [2025.07.21.0a]

### Updated

- refactored and updated namespace imports to generalize from `cfa.scenarios.dataops` --> `cfa.dataops`
- included `list_datasets()` function to `cfa.dataops.__init__.py` for easy import

### Added

- Documentation:
  - `docs/dataset_developer.md`
  - `docs/dataset_user.md`

## [2025.07.16.0a]

### Updated

- **Major refactor** to namespace. This was to enable the hierarchical namespacing for various groups.  This should also support multi-layer organization.
- `get_data` new an easy import at `cfa.scenarios.dataops` and also handles .json raw data.
- All import paths and dataset namespaces to reflect new pattern in workflows, tests and etl scripts
- changed `datasets` the namespace that gets created from all the available configurations files, which was also being renamed to catalog sometimes to the less
abstract, and unique to this library: `datacat`

### Added

- `dataops.datasets.mcmv.respnet` dataset config
- `dataops.etl.mcmv.respnet` dataset etl script for pulling all respnet raw data and transforming to keep specific columns and filter on categories
- `soda.py` from [https://github.com/CDCgov/cfasodapy/tree/main/src/cfasodapy](https://github.com/CDCgov/cfasodapy/tree/main/src/cfasodapy) changing
the backend to use httpx instead of requests and adding `clauses` argument.

### Removed

- `todo.md` since all the todos are done and we are capturing issues in repo

## [2025.07.09.1a]

### Added

- vax_hill_fitting modeling capability

## [2025.06.26.1a]

### Added

- more tests for datasets

## [2025.06.26.0a]

### Added

- blob mocking and testing using synthetic data to increase coverage of dataset catalog
- tests for covid19_vax_trends dataset and transform that can be duplicated for other tests

### Updated

- `cfa/scenarios/dataops/datasets/schemas/covid19vax_trends.py` to fix some errors with schema validation in synthetic data
- `tests/conftest.py` to include blob write function mocking fixture so it can be reused

## [2025.06.10.0a]

### Added
- raw synthetic data and transformed synthetic data generation in each schema file

## [2025.06.06.0a]

### Added
- `conftest.py` placeholder

### Updated
- `cfa/scenarios/dataops/datasets/schemas/covid19vax_trends.py` to include checks and custom dtype for array columns

### Fixed
- `.github/workflows/run_qaqc.yaml` to fix error related to coverage badge creation when merging to main

## [2025.06.03.0a]

### Added
- dataset schemas and validation logic

## [2025.06.02.0a]

### Added
- pytest-cov and other testing dependencies
- new coverage badge
- workflow to run tests
- First unit test

### Updated
- README to include badge

## [2025.05.28.0a]

### Added
- `cfa.scenarios.dataops.workflows.post_processing.files_io` for some utilities to build a file tree and read json files
- `cfa.scenarios.dataops.workflows.post_processing.state_report_pdf` report generation based on Ben's report

### Updated
- `cfa.scenarios.dataops.visualization.y_vs_datetime`

## [2025.05.15.0a]

### Added
- workflow for generating covid data
- new datasets toml files

## [2025.05.14.0a]

### Added

- `cfa.scenarios.dataops.visualization` submodule
- `cfa.scenarios.dataops.visualization.y_vs_datetime` plotting functions for lines, points and intervals
- `cfa.scenarios.dataops.visualization.composition` pdf report generation using list of altair plots

## [2025.04.03.1a]

### Added

- ability to download Blob datasets to a dataframe via `get_data()` function.
- ability to list available Blob datasets via `list_datasets()` function.


## [2025.04.03.0a]

### Added

- ETL pipeline for `covid19hospitalizations` data source
- ETL pipeline for `donor_seroprevalance_2020` data source
- ETL pipeline for `donor_seroprevalence_2022` data source
- ETL pipeline for `sars_cov2_proportions` data source
- ETL pipeline for `seroprevalence` data source

## [2025.03.24.0a]

### Added

- `BlobEndpoint` class to `datasets` namespace for simplified access to data

### Changed

- naming on first dataset config, sql template, and etl script for more specificity
- `covid19vax_trends.py` to include new `BlobEndpoint` data read/write pattern
- config validation to ensure no names match "extract" or "load"
- new data version allows for skipping extraction step and pulling latest

## [2025.03.10.0a]

### Added

- Example for ETL end-to-end with COVID 19 trends dataset
  - SQL transform template
  - using the config namespace
  - using new blob storage patterns
- validators for config files
- utils for time stamping and mako template loading
- changelog

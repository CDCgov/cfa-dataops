# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Calendar Versioning](https://calver.org/).
The versioning pattern is `YYYY.MM.DD.micro(a/b/{none if release})

---

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

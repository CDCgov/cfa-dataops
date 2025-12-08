# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Calendar Versioning](https://calver.org/).
The versioning pattern is `YYYY.MM.DD.micro(a/b/{none if release})

---

## [2025.12.08.1a]

### Fixed

- Polars dataframe loading+concat from csv/json files that contain varying number of columns (more updates)

## [2025.12.08.0a]

### Fixed

- Polars dataframe loading+concat from csv/json files that contain varying number of columns
- Namespace package configuration for created catalogs

## [2025.11.18.0a]

### Updated

- `docs/data_user_guide.md` and added cli documentation to mkdocs nav.

## [2025.11.13.0a]

### Added

- `cfa.dataops.utils.py:version_matcher` utility function and testing via doctests

### Updated

- Data versions (when retrieved/`get_`) can now use conditional logic:
  - Examples:
    ```python
    >>> from cfa.dataops import datacat
    >>> datacat.public.my_dataset.load.get_versions()
    ['2025-10-31',
     '2025-09-19',
     '2025-06-01',
     '2024-12-08',
     '2024-11-21']
    >>> df = datacat.public.my_dataset.load.get_dataframe(version=">2024.12.01,<2025.08")
    Using version: 2025-06-01
    >>> df = datacat.public.my_dataset.load.get_dataframe(version=">2024-12.01,<2025.08", newest=False)
    Using version: 2024-12-08
    >>> df = datacat.public.my_dataset.load.get_dataframe(version="~=2024/11")
    Using version: 2024-11-21
    >>> df = datacat.public.my_dataset.load.get_dataframe(version="latest")
    Using version: 2025-10-31
    ```
- Links in `README.md`
- pytest adopts in `pyproject.toml` to include `cfa/dataops/utils.py` for doctests

## [2025.10.31.0a]

### Added

- **New CLI commands** for dataset management:
  - `dataops_datasets`: List available datasets with optional prefix filtering
  - `dataops_stages`: View available stages for a specific dataset
  - `dataops_versions`: List available versions for a dataset stage
  - `dataops_save`: Download and save dataset versions locally
- **Tree utility function**: New `tree()` function in `utils.py` for displaying directory structures
- **Local data download**: `download_version_to_local()` method for BlobEndpoint to download datasets to local filesystem
- **Tests**: Comprehensive test coverage for new CLI commands and utilities
- New docs for CLI-tools

### Updated

- Documentation formatting: Changed "Description" to "Summary" with adjusted heading levels

## [2025.10.14.0a]

### Added

- Support for getting polars LazyFrames

## [2025.10.03.0a]

### Added

- Save helper methods to BlobEndpoint

### Updated

- Made ledger append to date files

## [2025.09.25.0a]

### Updated

- create_catalog pyproject.toml template for a namespace packaging requirement when not using poetry and key name update for dependency group "dev"

### Fixed

- Circular import error in catalogs by copying dependency over to ne catalog `__init__.py` from utils.

## [2025.09.22.0a]

### Added

- **New catalog creation CLI**: `dataops_catalog_init` command for creating structured catalog repositories
- **Multiple catalog support**: Ability to install and manage multiple catalog libraries in the same Python environment
- **Unified access interfaces**: `datacat` and `reportcat` provide unified access to datasets and reports across all installed catalogs
- **Comprehensive catalog management guide**: New `docs/managing_catalogs.md` with complete setup and multi-catalog workflows
- **Catalog repository structure**: Standardized structure with `datasets/`, `reports/`, and `workflows/` directories
- **Dataset access ledger**: automatically saves an log of dataset access to a *.jsonl file.

### Updated

- **Complete documentation restructure**: Eliminated duplication and simplified navigation across all guides
- **API changes**:
  - `list_datasets()` → `datacat.__namespace_list__`
  - `get_data()` → `datacat.{catalog}.{dataset}.{load/extract/stage_##}.get_dataframe()`
  - Added `datacat.{catalog}.{dataset}.{load/extract/stage_##}.get_dataframe()` for raw data access
  - Added sequences of bytes to `datacat.{catalog}.{dataset}.{load/extract/stage_##}.write_blob()` for data writing multiple files and auto versioning
- **Documentation flow**: Clear progression from setup (Managing Catalogs) to usage guides
- **Report access**: All examples updated to use `reportcat.__namespace_list__` and namespace access
- **ETL examples**: Updated to show proper `datacat` usage for configuration access and data operations

### Changed

- **Catalog-centric approach**: All development now happens within catalog repositories rather than the main cfa-dataops repo
- **Prerequisites handling**: Centralized setup instructions in managing_catalogs.md with references from other guides
- **Documentation organization**:
  - `docs/index.md`: New welcome page with logical flow and quick reference
  - `docs/data_user_guide.md`: Simplified to focus purely on data access patterns
  - `docs/data_developer_guide.md`: Streamlined to focus on ETL development within catalogs
  - `docs/report_generation.md`: Simplified to focus on report generation using reportcat
- **Namespace structure**: Catalogs now use `cfa.catalog.{unique_name}` namespace pattern
- **Storage configuration**: Default storage account and container settings in `catalog_defaults.toml`

### Technical Details

- **CLI implementation**: Complete command-line interface in `cfa/dataops/create_catalog/command.py`
- **Template system**: Mako templates in `cfa/dataops/create_catalog/repo_templates/`
- **Repository files**: Pre-configured files in `cfa/dataops/create_catalog/repo_files/`
- **Name sanitization**: Automatic cleanup of catalog names (lowercase, underscores, alphanumeric only)
- **Interactive confirmation**: User confirmation before catalog creation
- **Error handling**: Proper validation for existing directories and permissions
- **Version tracking**: Catalog creation version tracking in generated `__init__.py` files


## [2025.09.03.0a]

### Updated

- Swapped `cfa_azure` for `cfa-cloudops`

## [2025.08.21.0a]

### Added

- Release presentation

### Updated

- Documentation pointers
- Coverage badge step to only update main branch
- tagging workflow to only attempt tag if the tag doesn't already exist

## [2025.08.18.0a]

### Added

- Documentation for the reporting functionality
- Tests for reporting functionality
- Ability to create custom notebook metadata title/HTML title for report

## [2025.08.15.0a]

### Fixes

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

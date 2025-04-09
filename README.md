# CFA Scenarios DataOps

## Overview

This project provides ETL (Extract, Transform, Load) pipelines for various data scenarios within the CDC's Center for Analytics (CFA). It currently includes an end-to-end ETL pipeline data, demonstrating best practices for data processing within the CFA environment.

### Key Features

- Configuration-driven ETL pipelines using TOML files
- SQL-based transformations using DuckDB
- Azure Blob Storage integration for raw and transformed data storage
- Utilities for timestamping and template loading
- Data validation and quality checks

## Getting started

To use this repository:

1. Clone the repository
2. Install dependencies (**requires: `poetry >= 2.0`**):
   ```
   poetry install
   ```
3. Example: run the COVID-19 vaccination trends ETL pipeline:
   ```
   python -m cfa.scenarios.dataops.etl.covid19vax_trends --extract
   ```
4. [Optional] installing developer dependencies:
   ```
   poetry install --with dev
   ```

To add a new dataset:
1. Create a new TOML configuration file in `cfa/scenarios/dataops/datasets/`
2. Create a new ETL script in `cfa/scenarios/dataops/etl/`
3. Add SQL transformation templates in `cfa/scenarios/dataops/etl/transform_templates/`

## Accessing Datasets

When the ETL pipelines are run, the data sources (raw and/or transformed) are stored into Azure Blob Storage. There will be times when we want to access these datasets directly. The function `get_data()` found in `cfa.scenarios.dataops.datasets.catalog` helps retrieve that data, compile into a single dataframe, and return that dataframe. The parameters for `get_data()` are as follows:
- name: the name of the data source
- version: either 'latest' or string containing the datetime of required version. Default is 'latest'.
- type: either 'raw' or 'transformed'. Default is 'transformed'.
- output: the type of dataframe to output, either 'pandas' or 'polars'. Default is 'pandas'.

The available datasets can be found by running `list_datasets()`, which can be found in the `cfa.scenarios.dataops.datasets.catalog` submodule.

An example for getting the polars dataframes for the latest raw versions of the covid19vax_trends and seroprevalence datasets is below:
```python
from cfa.scenarios.dataops.datasets.catalog import get_data
vax_df = get_data("covid19vax_trends", type = "transformed", output = "polars")
sero_df = get_data("seroprevalence", type = "transformed", output = "polars")
```

## Creating A Release

This repository contains a workflow for creating releases called release.yaml. When ready to create a new release follow the steps below.
When creating a release tag follow the versioning pattern: `YYYY.MM.DD.micro(a/b/{none if release})

```bash
git checkout release
git pull
export RELEASE=YYYY.MM.DD
git commit --allow-empty -m "Release $RELEASE"
git tag -a $RELEASE -m "Version $RELEASE"
git push --tags 
```

Once a release tag in format YYYY.MM.DD is pushed the workflow will automate the process of creating a new release automatically.

## Project admins

- Thomas Hladish <utx5@cdc.gov> (CDC/OD/ORR/CFA)
- Phillip Rogers <ap66@cdc.gov> (CDC/OD/ORR/CFA)(CTR)

## Disclaimers

### General Disclaimer

This repository was created for use by CDC programs to collaborate on public health related projects in support of the [CDC mission](https://www.cdc.gov/about/organization/mission.htm). GitHub is not hosted by the CDC, but is a third party website used by CDC and its partners to share information and collaborate on software. CDC use of GitHub does not imply an endorsement of any one particular service, product, or enterprise.

### Public Domain Standard Notice

This repository constitutes a work of the United States Government and is not
subject to domestic copyright protection under 17 USC § 105. This repository is in
the public domain within the United States, and copyright and related rights in
the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/).
All contributions to this repository will be released under the CC0 dedication. By
submitting a pull request you are agreeing to comply with this waiver of
copyright interest.

### License Standard Notice

This repository is licensed under ASL v2 or later.

This source code in this repository is free: you can redistribute it and/or modify it under
the terms of the Apache Software License version 2, or (at your option) any
later version.

This source code in this repository is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the Apache Software License for more details.

You should have received a copy of the Apache Software License along with this
program. If not, see http://www.apache.org/licenses/LICENSE-2.0.html

The source code forked from other open source projects will inherit its license.

### Privacy Standard Notice

This repository contains only non-sensitive, publicly available data and
information. All material and community participation is covered by the
[Disclaimer](https://github.com/CDCgov/template/blob/master/DISCLAIMER.md)
and [Code of Conduct](https://github.com/CDCgov/template/blob/master/code-of-conduct.md).
For more information about CDC's privacy policy, please visit [http://www.cdc.gov/other/privacy.html](https://www.cdc.gov/other/privacy.html).

### Contributing Standard Notice

Anyone is encouraged to contribute to the repository by [forking](https://help.github.com/articles/fork-a-repo)
and submitting a pull request. (If you are new to GitHub, you might start with a
[basic tutorial](https://help.github.com/articles/set-up-git).) By contributing
to this project, you grant a world-wide, royalty-free, perpetual, irrevocable,
non-exclusive, transferable license to all users under the terms of the
[Apache Software License v2](http://www.apache.org/licenses/LICENSE-2.0.html) or
later.

All comments, messages, pull requests, and other submissions received through
CDC including this GitHub page may be subject to applicable federal law, including but not limited to the Federal Records Act, and may be archived. Learn more at [http://www.cdc.gov/other/privacy.html](http://www.cdc.gov/other/privacy.html).

### Records Management Standard Notice

This repository is not a source of government records but is a copy to increase
collaboration and collaborative potential. All government records will be
published through the [CDC web site](http://www.cdc.gov).

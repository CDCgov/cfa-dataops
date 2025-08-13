# Data Developer Guide

## How to use this guide

This guide explains how to add new datasets to the CFA Scenarios DataOps system.

This repository is designed and maintained so that if a core set of patterns are followed, adding new datasets for easy access by other teams is a straightforward procedure.

This document guides you through:

- Adding a new dataset
- Creating an ETL process
- Using Pandera schemas and synthetic data to test your ETL scripts
- Using workflows

## Overview

The ETL pipeline system is built around:

- TOML configuration files that define dataset properties
- Python ETL scripts that handle extraction, transformation and loading
- SQL templates for transformations (optional)
- Schema validation using Pandera

### Example

There are many datasets in this repository that can serve as references while creating yours. For example, the `covid19vax_trends` dataset:

- [TOML configuration](https://github.com/CDCgov/cfa-dataops/blob/main/cfa/dataops/datasets/scenarios/covid19_vaccination_trends.toml)
- [Schema and synthetic data](https://github.com/CDCgov/cfa-dataops/blob/main/cfa/dataops/datasets/scenarios/schemas/covid19vax_trends.py)
- [ETL script](https://github.com/CDCgov/cfa-dataops/blob/main/cfa/dataops/etl/scenarios/covid19vax_trends.py)
- [ETL SQL](https://github.com/CDCgov/cfa-dataops/blob/main/cfa/dataops/etl/transform_templates/scenarios/covid19vax_trends.sql)
- [Unit tests](https://github.com/CDCgov/cfa-dataops/blob/main/tests/datasets/test_covid19_vax_trends.py)

## Update an existing dataset

From the command line:

```
python -m cfa.dataops.etl.covid19vax_trends --extract
```

## Adding a New Dataset

To add a new dataset:

1. Create a TOML configuration file in `cfa/dataops/datasets/{team_dir}/{dataset_name}.toml`
1. Optionally, create a schema validation file
1. Create a new ETL script in `cfa/dataops/etl/{team_dir}/`
1. Add SQL transformation templates in `cfa/dataops/etl/transform_templates/{team_dir}/` (is using SQL for transforms). These are [Mako templates](https://www.makotemplates.org/)

### Configuration file

```toml title="cfa/dataops/datasets/{team_dir}/{dataset_name}.toml"
[properties]
name = "dataset_name"
version = "1.0"

[source]
# any dependencies you may want to use for your unique data source
# all fields are optional
url = "https://your.data.address/"


[extract]
account = "storage_account_name"
container = "container_name"
prefix = "path/to/raw/data"

[load]
account = "storage_account_name"
container = "container_name"
prefix = "path/to/transformed/data"
```

### Schema validation file

```python title="cfa/dataops/datasets/{team_dir}/schemas/{dataset_name}.py"
import pandera.pandas as pa

extract_schema = pa.DataFrameSchema({
    "column1": pa.Column(str),
    "column2": pa.Column(float)
})

load_schema = pa.DataFrameSchema({
    "transformed_col1": pa.Column(str),
    "transformed_col2": pa.Column(float)
})
```

### ETL script

```python title="cfa/dataops/etl/{team_dir}/{dataset_name}.py"
from ... import datacat
from ...datasets.{team_dir}.schemas.{dataset_name} import extract_schema, load_schema

def extract() -> pd.DataFrame:
    # Extract implementation
    pass

def transform(df: pd.DataFrame) -> pd.DataFrame:
    # Transform implementation
    pass

def load(df: pd.DataFrame) -> None:
    # Load implementation
    pass

def main(run_extract: bool = False, val_raw: bool = False, val_tf: bool = False) -> None:
    # Main ETL runner
    pass
```

### [optional] SQL templates

```sql title="cfa/dataops/etl/transform_templates/{team_dir}/{dataset_name}.sql"
SELECT
    column1,
    column2
FROM ${table_name}
WHERE condition = true
```

### [optional] Schemas and synthetic data

```python title="cfa/dataops/datasets/{team_dir}/schemas/{dataset_name}.py"
import numpy as np
import pandas as pd
import pandera.pandas as pa

# Define the schemas for validation
extract_schema = pa.DataFrameSchema({
    "date": pa.Column(pd.DatetimeTZDtype(tz='UTC')),
    "value": pa.Column(float, checks=pa.Check.greater_than(0)),
    "category": pa.Column(str, checks=pa.Check.isin(['A', 'B', 'C']))
})

load_schema = pa.DataFrameSchema({
    "date": pa.Column(pd.DatetimeTZDtype(tz='UTC')),
    "normalized_value": pa.Column(float),
    "category": pa.Column(str)
})

# Add synthetic data generation for testing
def raw_synthetic_data(n_rows: int = 100) -> pd.DataFrame:
    return pd.DataFrame({
            "date": pd.date_range(
                start="2023-01-01",
                periods=n_rows,
                tz='UTC'
            ),
            "value": np.random.uniform(1, 100, n_rows),
            "category": np.random.choice(['A', 'B', 'C'], n_rows)
    })

# Validate synthetic data matches schema
if __name__ == "__main__":
    test_df = generate_synthetic_data()
    extract_schema.validate(test_df)
```

### Testing

Create unit tests for your dataset:

```python title="tests/datasets/test_{dataset_name}.py"
import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaError

from cfa.dataops.datasets.{team_dir}.schemas.{dataset_name} import extract_schema, load_schema
from cfa.dataops.etl.{team_dir}.{dataset_name} import transform

def test_{dataset_name}_schemas():
    # Test schema validation
    pass

def test_{dataset_name}_transform():
    # Test transformation logic
    pass
```

### Best Practices

1. Use meaningful dataset and column names
2. Include comprehensive schema validation
3. Add synthetic test data generation in schema files
4. Document all assumptions and data requirements
5. Follow the existing code patterns for consistency
6. Add appropriate error handling
7. Include logging where appropriate

## Workflows

This `cfa.dataops` repository contains a `workflows` module. The following workflows are currently available:

- `covid`

Workflows can be run in a python virtual environment terminal where `cfa.dataops` is installed with the following format:

```bash
python3 -m cfa.dataops.workflows.<name>.<module> --<args>
```

### `covid` Workflow

There are two modules to the `covid` workflow with the following optional command line arguments:

- generate_data (this must be run before the next module)
  - -p, --path: path to store generated data; default is covid/data. Not needed if -b flag is used.
  - -b, --blob: whether to store generated data to Blob Storage (flag)
- run:
  - -c, --config: path to intialization config
  - -b, --blob: whether to pull from and push prepped data to Blob Storage (flag)

Ex:

```bash
python3 -m cfa.dataops.workflows.covid.generate_data -b
python3 -m cfa.dataops.workflows.covid.run -b
```

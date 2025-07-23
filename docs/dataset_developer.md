# Dataset Developer Guide

This guide explains how to add new datasets to the CFA Scenarios DataOps system.

This repository is designed and maintained so that if a core set of patterns are followed, adding new datasets for easy access by other teams is a straightforward procedure.

This document guides you through:
- Adding a new dataset
- Creating an ETL process
- Using Pandera schemas and synthetic data to test your ETL scripts

There are many dataset examples in this repository that can serve as references while creating yours. To get started, take a look at [`cfa/dataops/datasets/scenarios/covid19_vaccination_trends.toml`](../cfa/dataops/datasets/scenarios/covid19_vaccination_trends.toml).

The code related to this example dataset can be found in these files:

- [`cfa/dataops/datasets/scenarios/schemas/covid19vax_trends.py`](../cfa/dataops/datasets/scenarios/schemas/covid19vax_trends.py)
- [`cfa/dataops/etl/scenarios/covid19vax_trends.py`](../cfa/dataops/etl/scenarios/covid19vax_trends.py)
- [`cfa/dataops/etl/transform_templates/scenarios/covid19vax_trends.sql`](../cfa/dataops/etl/transform_templates/scenarios/covid19vax_trends.sql)
- [`tests/datasets/test_covid19_vax_trends.py`](../tests/datasets/test_covid19_vax_trends.py)

## Overview

The ETL pipeline system is built around:
- TOML configuration files that define dataset properties
- Python ETL scripts that handle extraction, transformation and loading
- SQL templates for transformations (optional)
- Schema validation using Pandera

## Adding a New Dataset

1. Create TOML Configuration:
   ```toml
   # filepath: cfa/dataops/datasets/{team_dir}/{dataset_name}.toml
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

2. Create Schema File:
   ```python
   # filepath: cfa/dataops/datasets/{team_dir}/schemas/{dataset_name}.py
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

3. Create ETL Script:
   ```python
   # filepath: cfa/dataops/etl/{team_dir}/{dataset_name}.py
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

4. Add SQL Templates (Optional):
   ```sql
   -- filepath: cfa/dataops/etl/transform_templates/{team_dir}/{dataset_name}.sql
   SELECT
     column1,
     column2
   FROM ${table_name}
   WHERE condition = true
   ```

5. Adding schemas and synthetic data (Optional):
    ```python
    # filepath: cfa/dataops/datasets/{team_dir}/schemas/{dataset_name}.py
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

## Testing

Create unit tests for your dataset:

```python
# filepath: tests/datasets/test_{dataset_name}.py
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

## Best Practices

1. Use meaningful dataset and column names
2. Include comprehensive schema validation
3. Add synthetic test data generation in schema files
4. Document all assumptions and data requirements
5. Follow the existing code patterns for consistency
6. Add appropriate error handling
7. Include logging where appropriate

# Dataset User Guide

This guide explains how to access and use datasets in the CFA Scenarios DataOps system.

## Available Datasets

To list all available datasets:

```python
from cfa.dataops.catalog import list_datasets

available_datasets = list_datasets()
print(available_datasets)
```

## Accessing Data

The primary way to access datasets is through the `get_data()` function:

```python
from cfa.dataops import get_data

# Get latest transformed data as pandas DataFrame
df = get_data("scenarios.covid19vax_trends")

# Get raw data as polars DataFrame
df = get_data(
    name="scenarios.seroprevalence",
    type="raw",
    output="polars"
)

# Get specific version
df = get_data(
    name="scenarios.covid19vax_trends",
    version="2025-06-03T17-56-50"
)
```

### Parameters

- `name`: Dataset identifier (required)
- `version`: Either 'latest' or specific version timestamp (default: 'latest')
- `type`: Either 'raw' or 'transformed' (default: 'transformed')
- `output`: Either 'pandas' or 'polars' DataFrame (default: 'pandas')

## Working with Data

### Data Versions

Data is versioned using timestamps. Each version represents a snapshot of the data at that point in time.

To get a specific version:

```python
df = get_data(
    "scenarios.covid19vax_trends",
    version="2025-06-03T17-56-50"
)
```

### Data Validation

All datasets have schema validation for both raw and transformed data. The schemas define:
- Required columns
- Data types
- Valid value ranges/options
- Required/optional fields

## Examples

### COVID-19 Vaccination Trends

```python
from cfa.dataops import get_data

# Get latest transformed data
vax_df = get_data("scenarios.covid19vax_trends")

# Get raw data for analysis
raw_vax = get_data(
    "scenarios.covid19vax_trends",
    type="raw"
)
```

### Seroprevalence Data

```python
from cfa.dataops import get_data

# Get as polars DataFrame
sero_df = get_data(
    "scenarios.seroprevalence",
    output="polars"
)
```

## Common Issues

1. Dataset Not Found
   - Verify dataset name using `list_datasets()`
   - Check for typos in namespace path

2. Version Not Found
   - Use 'latest' to get most recent version
   - Check available versions in Azure Blob Storage

3. Schema Validation Errors
   - Ensure data matches expected schema
   - Check for missing required columns
   - Verify data types are correct

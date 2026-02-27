# Data Developer Guide

This guide explains how to add new datasets and ETL processes to your catalog repositories.

> **Prerequisites**: You need to have a catalog repository created and installed. See [Managing Catalogs](managing_catalogs.md) for setup instructions.

## Quick Start

```python
from cfa.dataops import datacat

# Update an existing dataset
datacat.private.scenarios.covid19vax_trends.extract()
datacat.private.scenarios.covid19vax_trends.transform()
datacat.private.scenarios.covid19vax_trends.load()
```

## Overview

The ETL pipeline system is built around:

- TOML configuration files that define dataset properties
- Python ETL scripts that handle extraction, transformation and loading
- SQL templates for transformations (optional)
- Schema validation using Pandera

## Update an existing dataset

You can update datasets using datacat or from the command line:

```python
from cfa.dataops import datacat

# Trigger ETL for a specific dataset
datacat.private.scenarios.covid19vax_trends.extract()
datacat.private.scenarios.covid19vax_trends.transform()
datacat.private.scenarios.covid19vax_trends.load()
```

Or from the command line:

```
python -m cfa.dataops.etl.covid19vax_trends --extract
```

## Adding a New Dataset

To add a new dataset to your catalog repository:

1. Create a TOML configuration file in `{your_catalog}/datasets/{dataset_name}.toml`
2. Optionally, create a schema validation file in your catalog's structure
3. Create a new ETL script in `{your_catalog}/workflows/{workflow_type}/`
4. Add SQL transformation templates if using SQL for transforms (these are [Mako templates](https://www.makotemplates.org/))

### Configuration file

```toml title="{your_catalog}/datasets/{dataset_name}.toml"
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

```python title="{your_catalog}/workflows/{workflow_type}/{dataset_name}.py"
from cfa.dataops import datacat
import pandas as pd
import io

def extract() -> pd.DataFrame:
    # Extract implementation
    # Access dataset configuration via datacat
    config = datacat.{catalog_name}.{dataset_name}.config
    # Extract data from source
    pass

def transform(df: pd.DataFrame) -> pd.DataFrame:
    # Transform implementation
    pass

def load(df: pd.DataFrame) -> None:
    # Load implementation using datacat
    # Convert DataFrame to bytes buffer
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    # Write to blob storage
    datacat.{catalog_name}.{dataset_name}.load.write_blob(
        file_buffer=buffer.getvalue(),
        path_after_prefix="data.parquet",
        auto_version=True
    )

def main(run_extract: bool = False, val_raw: bool = False, val_tf: bool = False) -> None:
    # Main ETL runner
    if run_extract:
        raw_data = extract()
        if val_raw:
            # Validate raw data
            pass

    if val_tf:
        # Transform and validate
        transformed_data = transform(raw_data)
        load(transformed_data)
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

# Add mock data generation for testing
# prefix with 'extract' or 'load'
def extract_mock_data(output="pandas", size=10) -> pd.DataFrame|pl.DataFrame:
    data = {
            "date": pd.date_range(
                start="2023-01-01",
                periods=n_rows,
                tz='UTC'
            ),
            "value": np.random.uniform(1, 100, n_rows),
            "category": np.random.choice(['A', 'B', 'C'], n_rows)
    }
    df = pd.DataFrame(data)
    return df if output == "pandas" or output == "pd" else pl.from_pandas(df)

# Validate synthetic data matches schema
if __name__ == "__main__":
    test_df = extract_mock_data()
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

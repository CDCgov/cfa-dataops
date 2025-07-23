# Dataset User Guide

This guide explains how to access and use datasets in the CFA Scenarios DataOps system.

## Available Datasets

To list all available datasets:

```python
from cfa.dataops import list_datasets

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
    version="2025-06-03T17-59-16"
)
```

In order to see what versions are availabl, use the data catalog's convenient namespace methods:

```python
>>> from cfa.dataops import datacat
>>> # these follow hierarchical naming created using the dataset
>>> # config TOML, so extract or load are the makes assigned to
>>> # raw or transformed datasets per the get_data function
>>> datacat.scenarios.covid19vax_trends.load.get_versions()
['2025-06-03T17-59-16',
 '2025-05-30T19-55-51',
 '2025-05-30T14-50-36',
 '2025-03-24T15-30-31']
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

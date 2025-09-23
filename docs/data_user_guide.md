# Data User Guide

This guide explains how to access and use datasets in the CFA DataOps system.

> **Prerequisites**: You need to have catalog repositories created and installed. See [Managing Catalogs](managing_catalogs.md) for setup instructions.

## Quick Start

```python
from cfa.dataops import datacat

# List all available datasets
print("Available datasets:", datacat.__namespace_list__)

# Access a dataset
df = datacat.private.scenarios.covid19vax_trends.load.get_dataframe()
```

## Accessing Data

When the ETL pipelines are run, the data sources (raw and/or transformed) are stored into Azure Blob Storage. You can access these datasets directly using the `datacat` interface:

```python
from cfa.dataops import datacat

# Get latest transformed data as pandas DataFrame
df = datacat.private.scenarios.covid19vax_trends.load.get_dataframe()

# Get raw data as polars DataFrame
df = datacat.private.scenarios.seroprevalence.extract.get_dataframe(output="polars")

# Get specific version
df = datacat.private.scenarios.covid19vax_trends.load.get_dataframe(
    version="2025-06-03T17-56-50"
)
```

### Dataset Access Methods

- `datacat.{catalog}.{dataset}.load.get_dataframe()`: Access transformed data
- `datacat.{catalog}.{dataset}.extract.get_dataframe()`: Access raw data
- Parameters for `get_dataframe()`:
  - `version`: Either 'latest' or specific version timestamp (default: 'latest')
  - `output`: Either 'pandas' or 'polars' DataFrame (default: 'pandas')

## Working with Data

### Data Versions

Data is versioned using timestamps. Each version represents a snapshot of the data at that point in time.

To get a specific version:

```python
df = datacat.private.scenarios.covid19vax_trends.load.get_dataframe(
    version="2025-06-03T17-59-16"
)
```

In order to see what versions are available, use the data catalog's convenient namespace methods:

```python
>>> from cfa.dataops import datacat
>>> # these follow hierarchical naming created using the dataset
>>> # config TOML, so extract or load are the makes assigned to
>>> # raw or transformed datasets per the get_data function
>>> datacat.private.scenarios.covid19vax_trends.load.get_versions()
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
from cfa.dataops import datacat

# Get latest transformed data
vax_df = datacat.private.scenarios.covid19vax_trends.load.get_dataframe()

# Get raw data for analysis
raw_vax = datacat.private.scenarios.covid19vax_trends.extract.get_dataframe()
```

### Seroprevalence Data

```python
from cfa.dataops import datacat

# Get as polars DataFrame
sero_df = datacat.private.scenarios.seroprevalence.load.get_dataframe(output="polars")
```

## Common Issues

1. Dataset Not Found
   - Verify dataset name using `datacat.__namespace_list__`
   - Check for typos in namespace path
   - Ensure the catalog containing the dataset is installed
2. Version Not Found
   - Use 'latest' to get most recent version (default)
   - Check available versions using `datacat.{catalog}.{dataset}.load.get_versions()`
3. Schema Validation Errors
   - Ensure data matches expected schema
   - Check for missing required columns
   - Verify data types are correct

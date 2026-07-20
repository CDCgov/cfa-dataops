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
    version_spec="==2025-06-03T17-56-50"
)
```

### Dataset Access Methods

- `datacat.{catalog}.{dataset}.load.get_dataframe()`: Access transformed data
- `datacat.{catalog}.{dataset}.extract.get_dataframe()`: Access raw data
- Parameters for `get_dataframe()`:
   - `output`: One of `pandas`, `polars`, or `pl_lazy` (default: `pandas`)
   - `version_spec`: Version constraint string used to resolve matching dataset versions
   - `selection`: Which matching version to return, such as `newest` or `oldest`
   - `print_version`: Print the resolved version while loading data

## Working with Data

### Data Versions

Data is versioned using timestamps. Each version represents a snapshot of the data at that point in time.

If you want to see which version will be returned before loading the dataframe, use `resolve_version()` with the same `version_spec` and `selection` values you plan to pass to `get_dataframe()`:

```python
from cfa.dataops import datacat

resolved = datacat.private.scenarios.covid19vax_trends.load.resolve_version(
   version_spec=">=2025-05-01,<2025-06-01",
   selection="newest",
)

print(resolved["version"])
print(resolved["blob_url"])
```

`resolve_version()` returns a dictionary with `version`, `blob_url`, `version_spec`, and `selection`. Use those same arguments in `get_dataframe()` to load the dataframe you previewed.

To get a specific version:

```python
df = datacat.private.scenarios.covid19vax_trends.load.get_dataframe(
    version_spec="==2025-06-03T17-59-16"
)
```

To see what versions are available, use the data catalog's convenient namespace methods:

```python
>>> from cfa.dataops import datacat
>>> # These follow hierarchical naming created using the dataset
>>> # config TOML, so extract or load are the names assigned to
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

### Fetching Versions within a Range

When `get_dataframe(...)` completes successfully, it prints a short confirmation line like:

Used version: [...]

Examples:

```python
from cfa.dataops import datacat

# newest match in the range (single version)
df = datacat.private.scenarios.covid19vax_trends.load.get_dataframe(
   version_spec=">=2025-05-01,<2025-06-01"
)
```

*Console output (example):*
```
Used version: '2025-05-30T19-55-51'
```

```python
# oldest match in the same range (selection=oldest)
df_old = datacat.private.scenarios.covid19vax_trends.load.get_dataframe(
   version_spec=">=2025-05-01,<2025-06-01", selection = "oldest"
)
```

*Console output (example):*
```
Used version: '2025-05-30T14-50-36'
```

```python
# Fetch all matches and concatenate all tables
df_v = datacat.private.scenarios.covid19vax_trends.load.get_dataframe(
   version_spec=">=2025-05-01,<2025-06-01",
   selection="all"
)
```

*Console output (example):*
```
Used version: ['2025-05-30T19-55-51', '2025-05-30T14-50-36']
```

Use the helper `version_matcher` (from `cfa.dataops.utils`) to experiment with version boundary logic to see what matches occur prior to loading large datasets into memory.

For a direct preview from the dataset endpoint itself, call `resolve_version()` with the same `version_spec` and `selection` arguments you plan to use with `get_dataframe()`.

```python
>>> from cfa.dataops.utils import version_matcher
>>> available_versions = ['1.0', '1.1', '1.2', '2.0']
>>> version_matcher('>=1.1,<2.0', available_versions)
'1.2'
>>> version_matcher('>=1.1,<2.0', available_versions, selection = "oldest")
'1.1'
>>> version_matcher(None, available_versions)
'2.0'
>>> version_matcher('~=1', available_versions)
'1.2'
>>> version_matcher('>=1.1,<2.0', available_versions, selection = "all")
['1.2', '1.1']
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

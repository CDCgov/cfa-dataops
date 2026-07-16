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

# Include resolved version metadata on the returned dataframe
df_with_meta = datacat.private.scenarios.covid19vax_trends.load.get_dataframe(
   with_metadata=True
)
print(df_with_meta.attrs["version"])
```

## Accessing Data

When the ETL pipelines are run, the data sources (raw and/or transformed) are stored into Azure Blob Storage. You can access these datasets directly using the `datacat` interface:

```python
from cfa.dataops import datacat

# Get latest transformed data as pandas DataFrame
df = datacat.private.scenarios.covid19vax_trends.load.get_dataframe()

# Get raw data as polars DataFrame
df = datacat.private.scenarios.seroprevalence.extract.get_dataframe(output="polars")

# Get transformed data with metadata attached
df_with_meta = datacat.private.scenarios.covid19vax_trends.load.get_dataframe(
   with_metadata=True
)

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
   - `with_metadata`: Attach resolved version metadata to the returned dataframe
   - `print_version`: Print the resolved version while loading data

### Accessing Metadata with `with_metadata=True`

When `with_metadata=True`, `get_dataframe()` adds the resolved dataset metadata to the returned dataframe.

Available metadata keys:

- `version`: The resolved dataset version that was loaded
- `blob_url`: The Azure blob URL pattern used for the load
- `version_spec`: The version constraint passed to `get_dataframe()`
- `selection`: The version selection mode used during resolution

For pandas outputs, metadata is stored in `df.attrs`:

```python
from cfa.dataops import datacat

df = datacat.private.scenarios.covid19vax_trends.load.get_dataframe(
      with_metadata=True
)

print(df.attrs["version"])
print(df.attrs["blob_url"])
print(df.attrs["version_spec"])
print(df.attrs["selection"])
```

For polars `DataFrame` outputs, metadata is stored in `df.config_meta`:

```python
from cfa.dataops import datacat

df = datacat.private.scenarios.seroprevalence.extract.get_dataframe(
      output="polars",
      with_metadata=True,
)

print(df.config_meta.get("version"))
print(df.config_meta.get("blob_url"))
print(df.config_meta.get("version_spec"))
print(df.config_meta.get("selection"))
```

The same metadata access pattern applies to lazy polars outputs returned with `output="pl_lazy"`.

## Working with Data

### Data Versions

Data is versioned using timestamps. Each version represents a snapshot of the data at that point in time.

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

# Get transformed data plus resolved version metadata
vax_df_with_meta = datacat.private.scenarios.covid19vax_trends.load.get_dataframe(
   with_metadata=True
)
print(vax_df_with_meta.attrs["version"])
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

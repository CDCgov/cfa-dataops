# Data User Guide

This guide explains how to access and use datasets in the CFA DataOps system. This assumes you have already created one or more catalog repositories using the `dataops_catalog_init` CLI tool and have installed them in your Python environment.

## Prerequisites

Before using datasets, you need to:

1. **Create a catalog repository** using `dataops_catalog_init` (see [Catalog Creation Guide](catalog_creation.md))
2. **Install the catalog library** in your Python environment:
   ```bash
   cd /path/to/your/catalog
   pip install -e .[dev]
   ```
3. **Multiple catalogs can be installed** in the same Python environment, and all datasets will be accessible through the unified `datacat` interface

## Catalog Structure

Once you have created and installed catalog repositories, all datasets become accessible through the `datacat` namespace, regardless of which specific catalog library they come from. This allows you to:

- Install multiple catalog libraries (e.g., `cfa.catalog.scenarios`, `cfa.catalog.surveillance`, `cfa.catalog.my_project`)
- Access all datasets through a single interface
- Maintain separation of concerns between different data domains

## Available Datasets

To list all available datasets using datacat:

```python
from cfa.dataops import datacat

# List all available datasets
available_datasets = datacat.__namespace_list__
print(available_datasets)

# Or explore the datacat namespace directly
print(dir(datacat))  # Shows top-level catalog namespaces
```

## Unified Access Through datacat and reportcat

The DataOps system provides unified access to all datasets and reports through two main interfaces:

- **`datacat`**: Provides access to all datasets from all installed catalog libraries
- **`reportcat`**: Provides access to all reports from all installed catalog libraries

### Multiple Catalog Support

You can install multiple catalog libraries in the same Python environment:

```bash
# Install multiple catalogs
pip install -e /path/to/scenarios-catalog[dev]
pip install -e /path/to/surveillance-catalog[dev]
pip install -e /path/to/my-project-catalog[dev]
```

All datasets and reports become accessible through the unified interfaces:

```python
from cfa.dataops import datacat
from cfa.dataops.reporting import reportcat

# Access datasets from any installed catalog
datacat.scenarios.covid19vax_trends.load.get_dataframe()
datacat.surveillance.flu_trends.load.get_dataframe()
datacat.my_project.custom_dataset.load.get_dataframe()

# Access reports from any installed catalog
reportcat.scenarios.examples.basics_ipynb
reportcat.surveillance.weekly.summary_ipynb
reportcat.my_project.analysis.trend_report_ipynb
```

## Accessing Data

When the ETL pipelines are run, the data sources (raw and/or transformed) are stored into Azure Blob Storage. You can access these datasets directly using the `datacat` interface:

```python
from cfa.dataops import datacat

# Get latest transformed data as pandas DataFrame
df = datacat.scenarios.covid19vax_trends.load.get_dataframe()

# Get raw data as polars DataFrame
df = datacat.scenarios.seroprevalence.extract.get_dataframe(output="polars")

# Get specific version
df = datacat.scenarios.covid19vax_trends.load.get_dataframe(
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
df = datacat.scenarios.covid19vax_trends.load.get_dataframe(
    version="2025-06-03T17-59-16"
)
```

In order to see what versions are available, use the data catalog's convenient namespace methods:

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
from cfa.dataops import datacat

# Get latest transformed data
vax_df = datacat.scenarios.covid19vax_trends.load.get_dataframe()

# Get raw data for analysis
raw_vax = datacat.scenarios.covid19vax_trends.extract.get_dataframe()
```

### Seroprevalence Data

```python
from cfa.dataops import datacat

# Get as polars DataFrame
sero_df = datacat.scenarios.seroprevalence.load.get_dataframe(output="polars")
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

# Creating a DataOps Data Catalog Repository

This guide explains how to create a new dataops data catalog repository using the `dataops_catalog_init` CLI console script.

## Overview

The `dataops_catalog_init` command creates a structured repository for managing data catalogs within the CFA DataOps framework. It generates a complete Python package with predefined templates for datasets, workflows, and reports.

## Prerequisites

- Python 3.10 (required)
- `cfa.dataops` package installed
- Access to create directories in your target location

## Installation

The `dataops_catalog_init` command is automatically available after installing the `cfa.dataops` package:

```bash
pip install cfa.dataops
```

## Basic Usage

### Command Syntax

```bash
dataops_catalog_init <unique_name> <location> [options]
```

### Parameters

- **`unique_name`** (required): A unique module name that will be appended to the catalog namespace (`cfa.catalog.<unique_name>`)
- **`location`** (required): The local directory path where you want to create the catalog repository

### Options

- **`--expanded_repo_for_cfa`** or **`-x`**: Creates an expanded repository structure with additional CFA-specific files including:
  - Pre-commit configuration (`.pre-commit-config.yaml`)
  - Security baseline (`.secrets.baseline`)
  - License and disclaimer files
  - Ruff configuration for code formatting
  - Git attributes configuration
  - GitHub workflows (if applicable)

## Examples

### Basic Catalog Creation

```bash
dataops_catalog_init my_project /path/to/my/catalogs
```

This creates a catalog module named `cfa.catalog.my_project` in the specified location.

### CFA Expanded Repository

```bash
dataops_catalog_init my_project /path/to/my/catalogs --expanded_repo_for_cfa
```

This creates the same catalog but includes additional CFA-specific configuration files.

## Generated Repository Structure

When you run the command, it creates the following structure:

```
<location>/
├── cfa/
│   └── catalog/
│       └── <unique_name>/
│           ├── __init__.py
│           ├── catalog_defaults.toml
│           ├── datasets/
│           │   ├── __init__.py
│           │   ├── etl_example.toml
│           │   ├── experiment_tracking_example.toml
│           │   ├── multistage_example.toml
│           │   └── reference_data_example.toml
│           ├── reports/
│           │   ├── __init__.py
│           │   └── examples/
│           │       └── basics.ipynb
│           └── workflows/
│               ├── __init__.py
│               ├── etl/
│               │   └── __init__.py
│               ├── multistage/
│               │   └── __init__.py
│               └── reference_data/
│                   └── __init__.py
├── .gitignore
├── MANIFEST.in
└── pyproject.toml
```

### Additional Files (with `--expanded_repo_for_cfa`)

```
<location>/
├── .gitattributes
├── .pre-commit-config.yaml
├── .secrets.baseline
├── DISCLAIMER.md
├── LICENSE
├── ruff.toml
└── .github/          # GitHub workflows (if present)
```

## Key Components

### 1. Catalog Defaults (`catalog_defaults.toml`)

Contains default configuration for storage and access:

```toml
[storage]
account = "cfadatalakeprd"
container = "cfapredict"

[access_ledger]
path = "_access/<unique_name>/ledger/"
```

### 2. Dataset Examples

The generated repository includes several dataset template examples:

#### ETL Dataset (`etl_example.toml`)
- For data extracted from external sources, transformed, and loaded
- Includes source, extract, and load configurations
- Suitable for unstable external data sources

#### Reference Data (`reference_data_example.toml`)
- For static or infrequently changing reference data
- Typically used across multiple projects
- Only requires load configuration (no extract needed)

#### Experiment Tracking (`experiment_tracking_example.toml`)
- For tracking machine learning experiments and model data

#### Multistage (`multistage_example.toml`)
- For complex data pipelines with multiple processing stages

### 3. Project Configuration (`pyproject.toml`)

Generated with:
- Package name: `cfa.catalog.<unique_name>`
- Required dependencies for dataops functionality
- Development dependencies for testing
- Proper Python version constraints (>=3.10,<3.11)

## Dataset Configuration Structure

Each dataset TOML file follows this general structure:

```toml
[properties]
name = "dataset_name"           # Required
automate = false               # Optional
transform_templates = []       # Optional
schemas = ""                   # Optional
type = "etl|reference|experiment|multistage"  # Optional

[source]
# Source-specific configuration
url = "https://api.example.com/data"
# Additional source parameters...

[extract]  # For ETL datasets
account = ""     # Uses default if empty
container = ""   # Uses default if empty
prefix = "dataops/{group}/raw/{dataset}"

[load]
account = ""     # Uses default if empty
container = ""   # Uses default if empty
prefix = "dataops/{group}/transformed/{dataset}"
```

## Installation and Development

After creating your catalog repository:

1. **Navigate to the created directory:**
   ```bash
   cd /path/to/your/catalog
   ```

2. **Install in editable mode:**
   ```bash
   pip install -e .[dev]
   ```

3. **Start developing your datasets, workflows, and reports**

## Interactive Confirmation

The command includes an interactive confirmation step:

```
This will create a new dataops catalog module named cfa.catalog.<unique_name>. Continue? (y/n)
```

- Type `y` to proceed with creation
- Type `n` to abort the operation

## Name Sanitization

The `unique_name` parameter is automatically sanitized:
- Converted to lowercase
- Hyphens and spaces replaced with underscores
- Only alphanumeric characters and underscores retained

For example:
- `My-Project Name` becomes `my_project_name`
- `test-catalog-2024` becomes `test_catalog_2024`

## Error Handling

### Directory Already Exists
If the target location already exists, the command will:
- Display an error message: "Directory {location} already exists."
- Exit without making any changes

### Invalid Permissions
Ensure you have write permissions to the target location before running the command.

## Best Practices

1. **Choose descriptive names**: Use clear, descriptive names for your catalog modules
2. **Organize by purpose**: Consider creating separate catalogs for different projects or data domains
3. **Version control**: Initialize git in your catalog repository for version tracking
4. **Documentation**: Update the generated examples with your actual dataset configurations
5. **Testing**: Use the included test framework to validate your catalog configurations

## Next Steps

After creating your catalog repository:

1. Review and customize the example dataset configurations
2. Add your actual data sources and configurations
3. Develop custom workflows for your data processing needs
4. Create reports and visualizations using the provided templates
5. Set up automated testing and validation

## Troubleshooting

### Common Issues

**Import errors after installation:**
- Ensure you're in the correct directory when running `pip install -e .[dev]`
- Verify Python version compatibility (3.10 required)

**Permission denied errors:**
- Check write permissions for the target directory
- Consider using a different location or adjusting permissions

**Template rendering errors:**
- Ensure all required dependencies are installed
- Check that the `cfa.dataops` package is properly installed

For additional support, refer to the main DataOps documentation or contact the development team.

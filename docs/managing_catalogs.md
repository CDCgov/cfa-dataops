# Managing Multiple Catalogs

This guide explains how to work with multiple catalog repositories in the CFA DataOps system.

## Overview

The DataOps system is designed around catalog repositories that you create using `dataops_catalog_init`. Multiple catalogs can be installed in the same Python environment, providing unified access to all datasets and reports through `datacat` and `reportcat`.

## Creating Your First Catalog

1. **Create a catalog repository**:
   ```bash
   dataops_catalog_init my_project /path/to/catalogs
   ```

2. **Install in development mode**:
   ```bash
   cd /path/to/catalogs
   pip install -e .[dev]
   ```

3. **Verify installation**:
   ```python
   from cfa.dataops import datacat, reportcat

   print(datacat.__namespace_list__)
   print(reportcat.__namespace_list__)
   ```

## Working with Multiple Catalogs

### Installing Multiple Catalogs

You can install multiple catalog libraries in the same environment:

```bash
# Create and install different catalogs
dataops_catalog_init scenarios /path/to/scenarios-catalog
dataops_catalog_init surveillance /path/to/surveillance-catalog
dataops_catalog_init my_project /path/to/my-project-catalog

# Install each catalog
cd /path/to/scenarios-catalog && pip install -e .[dev]
cd /path/to/surveillance-catalog && pip install -e .[dev]
cd /path/to/my-project-catalog && pip install -e .[dev]
```

### Unified Access

All datasets and reports become accessible through unified interfaces:

```python
from cfa.dataops import datacat
from cfa.dataops.reporting import reportcat

# Access datasets from any installed catalog
datacat.private.scenarios.covid19vax_trends.load.get_dataframe()
datacat.private.surveillance.flu_trends.load.get_dataframe()
datacat.private.my_project.custom_dataset.load.get_dataframe()

# Access reports from any installed catalog
reportcat.private.scenarios.examples.basics_ipynb
reportcat.private.surveillance.weekly.summary_ipynb
reportcat.private.my_project.analysis.trend_report_ipynb
```

### Listing Available Resources

```python
# List all datasets across all catalogs
print("Available datasets:", datacat.__namespace_list__)

# List all reports across all catalogs
print("Available reports:", reportcat.__namespace_list__)

# Explore specific catalog namespaces
print("Scenarios datasets:", dir(datacat.scenarios))
print("Surveillance reports:", dir(reportcat.surveillance))
```

## Catalog Repository Structure

Each catalog repository contains:

```
my-catalog/
├── cfa/
│   └── catalog/
│       └── my_catalog/
│           ├── __init__.py
│           ├── catalog_defaults.toml
│           ├── datasets/           # Dataset configurations (TOML files)
│           │   ├── dataset1.toml
│           │   └── dataset2.toml
│           ├── reports/            # Jupyter notebook templates
│           │   ├── examples/
│           │   └── analysis/
│           └── workflows/          # ETL and processing scripts
│               ├── etl/
│               ├── multistage/
│               └── reference_data/
├── pyproject.toml
├── MANIFEST.in
└── .gitignore
```

## Best Practices

### Organization by Domain
- **scenarios**: COVID-19 modeling and forecasting datasets
- **surveillance**: Disease surveillance and monitoring data
- **reference**: Static reference data used across projects
- **my_project**: Project-specific datasets and analyses

### Naming Conventions
- Use descriptive catalog names that reflect their purpose
- Keep dataset names consistent within each catalog
- Use clear, hierarchical organization for reports

### Development Workflow
1. Create separate catalogs for different data domains
2. Install all relevant catalogs in your development environment
3. Use `datacat` and `reportcat` for unified access
4. Develop datasets and reports within their appropriate catalog repositories

### Sharing Catalogs
- Catalog repositories can be shared via Git repositories
- Teams can install each other's catalogs to access shared datasets
- Use proper versioning and documentation for shared catalogs

## Common Patterns

### Cross-Catalog Analysis
```python
# Combine data from multiple catalogs
scenarios_data = datacat.scenarios.covid19vax_trends.load.get_dataframe()
surveillance_data = datacat.surveillance.flu_trends.load.get_dataframe()

# Create combined analysis
combined_analysis = analyze_trends(scenarios_data, surveillance_data)
```

### Catalog-Specific Reports
```python
# Generate reports using data from specific catalogs
report = reportcat.private.scenarios.analysis.trend_analysis_ipynb
report.nb_to_html_file(
    html_out_path="trend_report.html",
    dataset_namespace="scenarios.covid19vax_trends"
)
```

## Troubleshooting

### Catalog Not Found
- Ensure the catalog is properly installed: `pip list | grep cfa.catalog`
- Check that you're in the correct Python environment
- Verify the catalog was created successfully

### Import Errors
- Reinstall the catalog in development mode: `pip install -e .[dev]`
- Check for naming conflicts between catalogs
- Ensure all dependencies are installed

### Namespace Conflicts
- Use unique catalog names to avoid conflicts
- Check `datacat.__namespace_list__` for existing namespaces
- Consider renaming conflicting catalogs

# CFA DataOps Documentation

Welcome to the CFA DataOps system - a catalog-based approach to data management, ETL, and reporting.

## Getting Started

1. **[Managing Catalogs](managing_catalogs.md)** - Create and manage multiple catalog repositories
2. **[Catalog Creation](catalog_creation.md)** - Detailed guide for creating new catalogs with `dataops_catalog_init`

## Using the System

3. **[Data User Guide](data_user_guide.md)** - Access and work with datasets using `datacat`
4. **[Data Developer Guide](data_developer_guide.md)** - Add datasets and ETL processes to catalogs
5. **[Report Generation](report_generation.md)** - Create and generate reports using `reportcat`

## Quick Reference

```python
# Essential imports
from cfa.dataops import datacat, reportcat

# List available resources
print("Datasets:", datacat.__namespace_list__)
print("Reports:", reportcat.__namespace_list__)

# Access data and reports (example)
df = datacat.private.scenarios.covid19vax_trends.load.get_dataframe()
report_html = reportcat.private.examples.basics_ipynb.nb_to_html_str()
```

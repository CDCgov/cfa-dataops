# Report Generation

Generate parameterized reports from Jupyter notebooks stored in your catalog repositories.

> **Prerequisites**: You need to have catalog repositories with report templates created and installed. See [Managing Catalogs](managing_catalogs.md) for setup instructions.

## Quick Start

```python
from cfa.dataops import reportcat

# List all available reports
print("Available reports:", reportcat.__namespace_list__)

# Generate a report
report = reportcat.examples.basics_ipynb
report.nb_to_html_file(html_out_path="my_report.html")
```

## Available Reports

Reports are organized in a namespace structure. You can explore available reports using reportcat:

```python
from cfa.dataops import reportcat

# List all available reports
available_reports = reportcat.__namespace_list__
print(available_reports)

# Or explore the reportcat namespace directly
print(dir(reportcat))  # Shows top-level catalog namespaces

# Access specific reports
reportcat.examples.basics_ipynb
reportcat.examples.dataset_report_ipynb
```

## Using Report Templates

Each report is represented by a `NotebookEndpoint` object that provides methods for:

- Getting report parameters (`get_params()`)
- Generating HTML output (`nb_to_html_str()`, `nb_to_html_file()`, `nb_to_html_blob()`)

### Example: Dataset Report

The dataset_report.ipynb template demonstrates a basic report that:

1. Takes a dataset namespace as a parameter
2. Shows dataset configuration
3. Displays available versions
4. Provides data summaries and samples
5. Creates visualizations

Example usage:

```python
# Get the report endpoint
report = reportcat.examples.dataset_report_ipynb

# View available parameters
report.print_params()

# Generate HTML report
report.nb_to_html_file(
    html_out_path="my_report.html",
    dataset_namespace="scenarios.covid19vax_trends"
)

# Or save directly to blob storage
report.nb_to_html_blob(
    blob_account="myaccount",
    blob_container="mycontainer",
    blob_path="reports/dataset_report.html",
    dataset_namespace="scenarios.covid19vax_trends"
)
```

## Creating Custom Reports

To create a new report in your catalog repository:

1. Create a Jupyter notebook in your catalog's `reports/` directory (e.g., `{your_catalog}/reports/my_report.ipynb`)
2. Use cell tags `parameters` and `remove_input` for parameter cells
3. Add help text as comments for parameters
4. Use markdown cells for documentation
5. The report will automatically be available in the `reportcat` namespace after installing your catalog

### Report Organization

Reports can be organized within subdirectories in your catalog's `reports/` folder:

```
{your_catalog}/reports/
├── examples/
│   └── basics.ipynb
├── analysis/
│   └── trend_analysis.ipynb
└── summary/
    └── monthly_report.ipynb
```

These will be accessible as:
- `reportcat.{catalog_name}.examples.basics_ipynb`
- `reportcat.{catalog_name}.analysis.trend_analysis_ipynb`
- `reportcat.{catalog_name}.summary.monthly_report_ipynb`

### Report Parameters

Parameters are defined in cells tagged with "parameters":

```python
dataset_namespace: str = "scenarios.covid19vax_trends"  # help string
```

### Template Best Practices

- Use cell tags to control input/output visibility
- Add clear markdown documentation
- Handle errors gracefully
- Include data validation
- Use consistent styling
- Test with different parameter

## Screenshot Example of a Report

Below is a screenshot for the basics report highlighting just a few of the things you can do with this reporting functionality. In addition to the myriad of jupyter compatible libraries that embed JavaScript (like ITables), authoring new functionality is also pretty simple, using `IPython.display` submodule, to author all sorts of add-ons that can be included in reports (e.g., a floating table of contents, or a temporary annotation layer).

![Simple report example](assets/annotated_report_example.png)

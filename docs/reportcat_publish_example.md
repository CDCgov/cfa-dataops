# Publishing Reports with Reportcat

This guide demonstrates how to generate a **parameterized notebook report** and publish it to **Azure Blob Storage** using `NotebookEndpoint`.

This example builds on the **Reportcat Quickstart** and shows how to automate report publishing.

---

## Example Location

Example files in this repository:

	examples/reportcat/
	├── minimal_report.ipynb
	├── run_minimal_report.py
	└── publish_report.py


The `publish_report.py` example demonstrates how to execute a notebook and upload the resulting HTML report directly to Azure Blob Storage.

---

## Prerequisites

From the repository root:

```powershell
python -m venv venv
venv\Scripts\activate
pip install -e .
```
You must also have Azure access to a storage account.

---

## Azure Configuration

Set the required environment variables before running the example.

PowerShell:

```powershell
$env:REPORTCAT_BLOB_ACCOUNT="storage_account_name"
$env:REPORTCAT_BLOB_CONTAINER="container"
$env:REPORTCAT_BLOB_PATH="TID/Test_Data/minimal_report.html"
```

These values specify where the generated report will be uploaded.

---

## Running the Publishing Example

Execute the script:

	python examples/reportcat/publish_report.py


During execution the workflow performs:

- Notebook parameter injection
- Notebook execution via papermill
- Conversion to HTML
- Upload to Azure Blob Storage

---

Example Script:

	examples/reportcat/publish_report.py

Expected Result

After successful execution:
- The notebook runs with injected parameters
- An HTML report is generated
- The report is uploaded to Azure Blob Storage

Example output location:

	https://<storage_account>.blob.core.windows.net/<container>/TID/Test_Data/minimal_report.html

---

## When to Use This Pattern

This workflow is useful when:

- generating scheduled reports
- publishing automated analytics outputs
- creating reproducible reporting pipelines
- integrating notebook reports into cloud storage workflows

---

## Next Steps

Teams can extend this pattern by:

- integrating report generation into GitHub Actions
- scheduling report execution
- publishing reports to shared storage locations
- parameterizing reports with pipeline inputs

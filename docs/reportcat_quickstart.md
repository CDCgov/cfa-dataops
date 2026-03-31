# Reportcat Quickstart

This guide demonstrates how to use `cfa.dataops.reporting.catalog.NotebookEndpoint` to execute a **parameterized Jupyter notebook** and generate an **HTML report**.

The example in this repository provides a minimal workflow that users can adapt for automated reporting.

---

## What reportcat provides

`NotebookEndpoint` enables:

- Parameterized notebook execution using **papermill**
- Report rendering using **nbconvert**
- Exporting results to **HTML reports**
- Optional publishing to **Azure Blob Storage**

This approach is useful for teams that want to:

- Standardize notebook-based reporting
- Automate periodic report generation
- Run reproducible notebook pipelines

---

## Repository example

This repository includes a minimal working example located at:

examples/reportcat/

	### Files included
	examples/reportcat/
	├── minimal_report.ipynb
	└── run_minimal_report.py


---

## `minimal_report.ipynb`

A simple notebook that:

- Accepts parameters
- Performs a small computation
- Outputs results for the report

---

## `run_minimal_report.py`

A script that:

- Executes the notebook
- Injects parameters
- Generates an HTML report

---

## Prerequisites

From the **cfa-dataops** repository root:

```powershell
python -m venv venv
venv\Scripts\activate
pip install -e .
```
This installs the cfa-dataops package in editable mode.

---

## Running the example

Execute the example report generation script:

	python examples/reportcat/run_minimal_report.py

During execution, NotebookEndpoint will:

- Inject parameters into the notebook
- Execute the notebook via papermill
- Convert the executed notebook to HTML

Example console output:

	Executing: 100%
	HTML report saved to out/minimal_report.html
	Report generated: out/minimal_report.html

---

## Output

The generated report will appear in:

	out/minimal_report.html

Opening the file will show the executed notebook results rendered as an HTML report.

Example output:
	x = 10
	y = 20
	result = 30

## Understanding parameters

The notebook defines parameters in a dedicated cell:

	# parameters
	x = 1
	y = 2

The execution script injects new values:

	ep.nb_to_html_file(
		html_out_path="out/minimal_report.html",
		nb_title="Minimal Report",
		kernel_name="python3",
		x=10,
		y=20
	)

These values override the defaults during notebook execution.

---

## When to use reportcat

Reportcat is useful when:

- notebooks are used to produce repeatable reports
- reports must be generated programmatically
- teams want parameterized notebook pipelines
- outputs need to be published as HTML artifacts

---

## Troubleshooting

Notebook kernel not found

List available kernels:

	python -m jupyter kernelspec list

Ensure your script uses the correct kernel:

	kernel_name="python3"

---

## Editable install issues

Reinstall the package:


	pip install -e .

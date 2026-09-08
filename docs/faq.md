# CFA DataOps Frequently Asked Questions (FAQ)

## Purpose
This FAQ document addressess common questions about the **cfa-dataops** repository and serves as a supplementary reference to existing documentation-including the technical guides, user guides, and quick start materials-to support consistent  and effective use of the repository.

## Access, Accounts, and Environments

**1. What access do I need before I can use cfa-dataops?**

    You should have access to:
    - CDC network 
    - CFA DataOps Github 
    - CDC.gov repositories
    - CDCent entrerprise repos
    - cloud resources assigned to your project

If something is missing, [email cfatools](mailto:cfatoolsteam@cdc.gov) for technical support.

**2. Where should I work day-to-day?**

    Use cfa-dataops VAP (Virtual Access Platform) for secure, cloud-supported work. Organize notebooks by project, use the dataops CLI in VAP for consistency, and sign out after each session for security.

**3. How do I clone and set up the repo?**

Use Git to clone and `uv` for dependency management.

```bash
git clone https://github.com/CDCgov/cfa-dataops

cd cfa-dataops

uv sync
```

**4. Do I need Azure login to run everything?**

    Only workflows that touch CDC cloud resources need Azure auth. If required for your tasks, run:

`az login --identity`

## Core Concepts and Everyday Tasks

**5. What is the "catalog" and why is it important to cfa-dataops?**

The catalog standardizes dataset metadata, versioning, and accessibility for discoverability, reporducibility, and governance.  Data scientists review catalog entries before building ETL tasks and validate versions and schema.

**6. How do I list available datasets and load one?**

```
python

from cfa.dataops import datacat

print("Available datasets:", datacat._namespace_list_)

df = datacat.private.scenarios.covid19vax_trends.load.get_dataframe()
```

**7. How do I preview which version will load before fetching the dataframe?**

Use resolve_version with the same version_spec and selection you plan to pass to get_dataframe.

```
python

from cfa.dataops import datacat

resolved = datacat.private.scenarios.covid19vax_trends.load.resolve_version(version_spec = ">=2025-05-01, <2025-06-01", selection="newest")

print(resolved.version)
```

***8. Where is the data stored after ETL runs?

Raw and transformed datasets are written to Azure Blob Storage.  Access them through `datacat` and the configured catalog paths.

## ETL, Validation, and Developer Workflow

**9. How do I trigger ETL stages programmatically?**

```
python

from cfa.dataops import datacat

datacat.private.scenarios.covid19vax_trends.extract()

datacat.private.scenarios.covid19vax_trends.transform()

datacat.private.scenarios.covid19vax_trends.load()
```

ETL structure, TOML configs, and schema validation are detailed in the [Data Developer Guide](https://github.com/CDCgov/cfa-dataops/blob/main/docs/data_developer_guide.md)

**10. How is schema validation handled?**

Datasets include validation for raw and transformed data.  If validation errors occur, check schema expectations and adjust ETL or input sources accordingly.  See the [Data User Guide](https://github.com/CDCgov/cfa-dataops/blob/main/docs/data_user_guide.md) for validation patterns.

## Troubleshooting and Help

**11. Where do I start when something breaks?**

Use the internal [Troubleshooting Guide](https://github.com/CDCgov/cfa-dataops/blob/main/docs/troubleshooting-guide.md) for quick environment checks and minimal working examples.

**12. Where do I ask for help or escalate issues?**

[Email CFA Tools](mailto:cfatoolsteam@cdc.gov) for access or platform questions.



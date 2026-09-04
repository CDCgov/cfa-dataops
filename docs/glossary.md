# CFA DataOps Glossary
This glossary provides clear, CDC-context definiitons of key technologies, tools and concepts frequently used in the **cfa-dataops** environment.  It is intended to support new developers onboarding into CFA DataOps workflows.


## Azure Blob Storage
**Azure Blob Storage** is Microsoft Azure's cloud object storage solution used for storing large volumes of unstructured data such as CSV files, Parquet datasets, model outputs, logs, and other artifacts.

### Why it matters in cfa-dataops
    - It provides secure, scablable storage for ingestion pipelines, cleaned datasets, and analytical outputs used in CFA modeling and analytics
    - Many cfa-dataops integration tests rely on Blob Storage access, which requires authenticating with 'az login --identity`
    - Enables cloub-based pipelines that mirror production environments, making local-to-cloud reproducibility easier


## Catalog (CFA Catalog)
The **CFA Catalog** is a central structured repository of datasets used by CFA modeling teams. It provides metadata, versioning, provenance, and standardized accessibility, enabling discoverability, reproducibility, and governance.

### Why it matters in cfa-dataops
    - Ensures datasets are well-documented and versioned
    - Allows analytics teams to locate authoratative ("source of truth") datasets quickly
    - Supports publication workflows for modeling and public-facing data products
    - Ensure reproducible analytics across CFA teams.


## DuckDB
DuckDB is an in-process OLAP (analytical) database designed for fast, local analytical queries.  It runs inside Python and supports fast SQL queries on large data files without requiring a server.

### Why it matters in cfa-dataops
    - Supports SQL, making transformations readable and standardized
    - Enables reproducible local pipelines before cloud publication
    - Ideal for rapid local development and reproducible ETL workflows
    - Efficient for working with large CSV/Parquet datasets locally


## Hypothesis
Hypothesis is a property-based testing framework for Python.  Instead of manually specifying inputs, Hypothesis automatially generates input data to explore edge cases.

### Why it matters in cfa-dataops
    - Helps ensure reliability of ingestion and transformation functions
    - Useful for validating data schemas or catalog consistency rules
    - Integrated into cfa-dataops testing alongside pytest (unit + property-based tests, unit + randomized checks)


## Polars
Polars is a high-performance DataFrame library for Rust and Python, optimized for tabular data processing.

### Why it matters in cfa-dataops
    - Extremely fast for cleaning, filtering, merging, and reshaping datasets
    - Offers better performance compared to pandas for large datasets
    - Works seamlessly with DuckDB to deliver flexible, efficient ETL patterns
    - Offers declarative query patterns and efficient lazy computation


## Pytest
**pytest** is a Python testing framework used to write and execute test suites, including unit tests, integration tests, and property-based tests.

### Why it matters in cfa-dataops
    - CFA DataOps uses pytest as its primary test runner, including support for:
        - Discovery of test files
        - Mocking with pytest-mock
        - Coverage reporting
        - Property-based tests via Hypothesis
        - Unit tests,
        - Integration tests
    - pytest integrates seamlessly with uv (uv run pytest)
    - supports node ID selection for running specific tests.


## UV
`uv` is a fast, modern Python package environment manager designed to replace slower and heavier tools sucha as pip and virtualenv.  It ensures reproducible environments and predictable dependency resolution.

### Why it matters in cfa-dataops
    - uv provides reliable installs and consistent execution environments across developer machines and CI
    - In cfa-dataops, uv is the recommended setup tool for running tests and syncing dependencies (uv sync, uv run pytest)
    - It improves the stability of pipelines and reduces environment drift

# CFA DataOps Tests

## Overview
The cfa-dataops/tests directory contains automated checks to help ensure the reliability of **cfa-dataops** library and its supporting utilities.  The suite is designed to run locally and in CI, emphasizing fast unit tests while allowing (optional) integration tests that touch cloud resources used by CFA DataOps (e.g. Azure Blob Storage)


## Quick Start Checklist
1. Python: install Python 3.10 or newer.

2. Clone the repo

    `git clone https://github.com/CDCgov/cfa-dataops.git`

    `cd cfa-dataops`

3.	Set up the environment (recommended: uv)

    \# Install project dependencies using uv

    `uv sync`

4.	Authenticate to Azure (Optional)

    if you will run integration tests that touch cloud resources:

    `az login –identity`

5.	Run the tests

    \# All tests (recommended)

    `uv run pytest`


## Getting Started
1.  Install & Setup

    #### With uv (recommended)

    \# from the repository root

    `uv sync`

    `uv run pytest`

    #### With pip (alternative)

    `python -m venv .venv`

    `source .venv/bin/activate'

    \# Windows:

    `.venv\Scripts\activate`

    `python -m pip install --upgrade pip`

    `pip install -e .`

    `pytest`

2. Running Specific Tests

    #### Single file or node ID (pytest standard)

    `uv run pytest tests/path/to/testmodule.py::TestClass::testmethod`


    Selecting tests via node IDs is a standard pytest feature.
    - Show detailed output

        `uv run pytest -vv`

3. Coverage (optional)

    If you’d like coverage reports:

    `uv run pytest --cov=cfa.dataops --cov-report=term-missing`

4. Cloud-Dependent Tests (optional)

    Some tests may rely on access to CDC cloud resources.

    \# Authenticate (if applicable)

    `az login –identity`


## Docs for developers
    Project documentation explains how data catalogs and ETL/reporting components work:
    - Project documentation
    - Data User Guide
    - Data Developer Guide
    - CLI Tools Reference

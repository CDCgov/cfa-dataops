[project]
name = "${catalog_namespace}.${unique_name}"
version = "0.0.0"
description = "CFA Public Catalog"
authors = [{name = "cfa-dataops"}]
readme = "README.md"
requires-python = ">=3.10,<3.11"
dependencies = [
    "cfa-dataops @ git+https://github.com/CDCgov/cfa-dataops.git@main",
    "pandera[pandas] (>=0.25.0)",
    "tomli (>=2.2.1,<3.0.0)",
    "httpx (>=0.28.1,<0.29.0)",
    "mako (>=1.3.9,<2.0.0)",
    "duckdb (>=1.2.0,<2.0.0)",
    "pyarrow (>=19.0.1,<20.0.0)",
    "fastparquet (>=2024.11.0,<2025.0.0)",
    "pandas (>=2.2.3)",
    "polars (>=1.26.0)",
    "nbconvert (>=7.16.6,<8.0.0)",
    "jupyter (>=1.1.1,<2.0.0)",
    "rich (>=14.1.0,<15.0.0)",
    "faker (>=37.8.0,<38.0.0)",
]

[tool.setuptools.packages.find]
include = ["${catalog_namespace}*"]

[project.optional-dependencies]
dev = [
    "pytest (>=8.3.5)",
    "pytest-mock (>=3.14.0)",
    "pytest-cov (>=6.1.1)"
]

[project.scripts]
my-client = "my_package.my_module:main_cli"

[tool.pytest.ini_options]
addopts = "--doctest-modules -vv --cov=cfa --cov-report html --cov-report term"

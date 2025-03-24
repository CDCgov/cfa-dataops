# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Calendar Versioning](https://calver.org/).
The versioning pattern is `YYYY.MM.DD.micro(a/b/{none if release})

---

## [2025.03.24.0a]

### Added

- `BlobEndpoint` class to `datasets` namespace for simplified access to data

### Changed

- naming on first dataset config, sql template, and etl script for more specificity
- `covid19vax_trends.py` to include new `BlobEndpoint` data read/write pattern
- config validation to ensure no names match "extract" or "load"
- new data version allows for skipping extraction step and pulling latest

## [2025.03.10.0a]

### Added

- Example for ETL end-to-end with COVID 19 trends dataset
  - SQL transform template
  - using the config namespace
  - using new blob storage patterns
- validators for config files
- utils for time stamping and mako template loading
- changelog

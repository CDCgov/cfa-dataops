# Data Quality Checks

The `cfa.dataops.quality` module provides version-to-version data quality checks
for catalog datasets. Pandera schemas validate a single version's shape; these
checks validate that a new version still looks like the dataset it claims to
be: fresh enough, similar in size, with stable columns, and without silent
distribution shifts.

## When to use it

Run the suite at the end of an ETL pipeline (before `load()`) or in CI after a
new version is staged. A failing report means the new version regressed
somewhere — an upstream feed broke, a join started missing, or a format
changed — and the version should not be promoted.

## Python API

```python
import pandas as pd
from cfa.dataops.quality import run_quality_suite, default_config

new_df = pd.read_parquet("nssp_gold_v2_new.parquet")
old_df = pd.read_parquet("nssp_gold_v2_previous.parquet")

config = default_config()
config["freshness"] = {"date_column": "reference_date", "max_age_days": 14}

report = run_quality_suite(new_df, old_df, config)
print(report.summary())
report.raise_on_failure()  # raises QualityCheckError listing every failure
```

When there is no previous version yet (first ever load), pass `old_df=None`:
comparison checks are skipped and single-version checks (freshness) still run.

## Available checks

| Check | What it does | Key options |
| --- | --- | --- |
| `freshness` | newest record date is within `max_age_days` of the reference date | `date_column`, `max_age_days`, `reference_date` |
| `row_count` | new version is non-empty and within `max_change_ratio` of the old row count | `min_rows`, `max_change_ratio` |
| `columns` | no unexpected added/missing columns | `allow_new`, `allow_missing` |
| `dtypes` | shared columns kept the same dtype | — |
| `null_rates` | per-column null fractions drifted by no more than `max_delta` | `columns`, `max_delta` |
| `numeric_drift` | numeric column means shifted by no more than `max_std_shift` old-std units | `columns`, `max_std_shift` |
| `categorical_values` | no new or vanished categories in string columns | `columns` |

Each check returns a `CheckResult` (`name`, `passed`, `message`, `details`,
`skipped`); the suite aggregates them into a `QualityReport` with `.passed`,
`.failed`, `.summary()`, `.as_dict()`, and `.raise_on_failure()` for CI
gating.

## CLI

Compare two local files (parquet, csv, or json) without writing Python:

```bash
python -m cfa.dataops.quality --new new.parquet --old old.parquet --config checks.toml
```

`checks.toml` maps check names to keyword arguments:

```toml
[freshness]
date_column = "reference_date"
max_age_days = 14

[row_count]
max_change_ratio = 0.5

[columns]
[dtypes]
[null_rates]
[numeric_drift]
[categorical_values]
```

The command prints a one-line-per-check summary and exits non-zero if any
check fails. Pass `--fail-on-skip` for strict CI mode, where skipped checks
(no previous version) also fail the run.

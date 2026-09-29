"""Version-to-version data quality and drift checks for catalog datasets.

Pandera schemas validate a single version's shape. These checks validate that a
new version still looks like the dataset it claims to be: fresh enough, similar
in size, with stable columns, and without silent distribution shifts. They are
designed to run at the end of an ETL pipeline or in CI and fail loudly when a
new version regresses.

Example:
    >>> import pandas as pd
    >>> old = pd.DataFrame({"report_date": ["2026-09-20", "2026-09-21"],
    ...                     "value": [10.0, 12.0]})
    >>> new = pd.DataFrame({"report_date": ["2026-09-27", "2026-09-28"],
    ...                     "value": [11.0, 13.0]})
    >>> report = run_quality_suite(
    ...     new,
    ...     old,
    ...     config={"freshness": {"date_column": "report_date",
    ...                           "max_age_days": 36500,
    ...                           "reference_date": "2026-09-28"}},
    ... )
    >>> report.passed
    True
"""

import argparse
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class CheckResult:
    """Outcome of a single quality check."""

    name: str
    passed: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    skipped: bool = False

    def __post_init__(self) -> None:
        if self.skipped:
            self.passed = True

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation of the result."""
        return {
            "name": self.name,
            "passed": self.passed,
            "skipped": self.skipped,
            "message": self.message,
            "details": self.details,
        }


@dataclass
class QualityReport:
    """Aggregated results of a quality-check run.

    Example:
        >>> r = QualityReport(results=[CheckResult("demo", True, "ok")])
        >>> r.passed
        True
        >>> r.raise_on_failure() is None
        True
    """

    results: list[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """True when no check failed (skipped checks do not fail the report)."""
        return all(r.passed for r in self.results)

    @property
    def failed(self) -> list[CheckResult]:
        """The checks that failed."""
        return [r for r in self.results if not r.passed]

    def raise_on_failure(self) -> None:
        """Raise a QualityCheckError listing every failed check.

        This is the CI-gating entry point: call it at the end of an ETL run
        and a non-zero exit follows from the uncaught exception.

        Raises:
            QualityCheckError: if any check failed.
        """
        if not self.passed:
            failures = "; ".join(f"{r.name}: {r.message}" for r in self.failed)
            raise QualityCheckError(
                f"{len(self.failed)} quality check(s) failed: {failures}"
            )

    def summary(self) -> str:
        """One line per check, suitable for CI logs."""
        lines = []
        for r in self.results:
            status = "SKIP" if r.skipped else ("PASS" if r.passed else "FAIL")
            lines.append(f"[{status}] {r.name}: {r.message}")
        return "\n".join(lines)

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation of the report."""
        return {
            "passed": self.passed,
            "results": [r.as_dict() for r in self.results],
        }


class QualityCheckError(Exception):
    """Raised by QualityReport.raise_on_failure when any check fails."""


def _skip(name: str, reason: str) -> CheckResult:
    return CheckResult(name=name, passed=True, message=reason, skipped=True)


# Date columns tried, in order, when check_freshness is not given an explicit
# date_column (e.g. via the default config).
_FRESHNESS_DATE_CANDIDATES = ("report_date", "reference_date", "date")


def check_freshness(
    df: pd.DataFrame,
    date_column: str | None = None,
    max_age_days: int = 14,
    reference_date: str | date | datetime | None = None,
) -> CheckResult:
    """Check that the newest record is no older than ``max_age_days``.

    Args:
        df: the dataframe to check.
        date_column: column holding the record date (parsed with
            ``pd.to_datetime``). If None, the first of
            ``report_date`` / ``reference_date`` / ``date`` present in the
            dataframe is used; the check is skipped if none is found.
        max_age_days: maximum allowed age in days of the newest record.
        reference_date: date to measure age against; defaults to today.

    Example:
        >>> df = pd.DataFrame({"report_date": ["2026-09-27", "2026-09-28"]})
        >>> check_freshness(df, "report_date", 7,
        ...                 reference_date="2026-09-28").passed
        True
        >>> check_freshness(df, "report_date", 7,
        ...                 reference_date="2027-01-01").passed
        False
    """
    name = "freshness"
    if date_column is None:
        date_column = next(
            (c for c in _FRESHNESS_DATE_CANDIDATES if c in df.columns), None
        )
        if date_column is None:
            return _skip(
                name,
                "no date column found; set freshness.date_column to enable",
            )
    if date_column not in df.columns:
        return CheckResult(name, False, f"date column '{date_column}' not present")
    try:
        dates = pd.to_datetime(df[date_column], errors="raise")
    except Exception as exc:
        return CheckResult(
            name, False, f"could not parse date column '{date_column}': {exc}"
        )
    if dates.empty or dates.isna().all():
        return CheckResult(name, False, "date column has no valid dates")
    ref = (
        pd.to_datetime(reference_date)
        if reference_date is not None
        else pd.Timestamp.today().normalize()
    )
    newest = dates.max()
    age_days = (ref - newest).days
    details = {
        "newest": newest.isoformat(),
        "age_days": age_days,
        "max_age_days": max_age_days,
    }
    if age_days < 0:
        return CheckResult(
            name,
            False,
            f"newest record {newest.date()} is in the future relative to {ref.date()}",
            details,
        )
    if age_days > max_age_days:
        return CheckResult(
            name,
            False,
            f"newest record is {age_days} days old (limit {max_age_days})",
            details,
        )
    return CheckResult(name, True, f"newest record is {age_days} days old", details)


def check_row_count(
    new_df: pd.DataFrame,
    old_df: pd.DataFrame | None,
    min_rows: int = 1,
    max_change_ratio: float = 0.5,
) -> CheckResult:
    """Check the new version is non-empty and similar in size to the old one.

    Args:
        new_df: the new version.
        old_df: the previous version, or None to skip the comparison.
        min_rows: minimum acceptable row count for the new version.
        max_change_ratio: maximum allowed ``abs(new - old) / old``.

    Example:
        >>> old = pd.DataFrame({"a": [1, 2, 3, 4]})
        >>> new = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
        >>> check_row_count(new, old).passed
        True
        >>> check_row_count(new, old, max_change_ratio=0.1).passed
        False
    """
    name = "row_count"
    n_new = len(new_df)
    if n_new < min_rows:
        return CheckResult(
            name,
            False,
            f"new version has {n_new} rows (minimum {min_rows})",
            {"new_rows": n_new, "min_rows": min_rows},
        )
    if old_df is None:
        return _skip(name, "no previous version to compare against")
    n_old = len(old_df)
    if n_old == 0:
        return CheckResult(
            name, False, "previous version is empty; cannot compare row counts"
        )
    change_ratio = abs(n_new - n_old) / n_old
    details = {
        "new_rows": n_new,
        "old_rows": n_old,
        "change_ratio": round(change_ratio, 4),
        "max_change_ratio": max_change_ratio,
    }
    if change_ratio > max_change_ratio:
        return CheckResult(
            name,
            False,
            f"row count changed by {change_ratio:.1%} (limit {max_change_ratio:.0%})",
            details,
        )
    return CheckResult(name, True, f"row count changed by {change_ratio:.1%}", details)


def check_columns(
    new_df: pd.DataFrame,
    old_df: pd.DataFrame | None,
    allow_new: bool = False,
    allow_missing: bool = False,
) -> CheckResult:
    """Check that the column set did not silently change between versions.

    Args:
        new_df: the new version.
        old_df: the previous version, or None to skip the comparison.
        allow_new: permit columns present only in the new version.
        allow_missing: permit columns missing from the new version.

    Example:
        >>> old = pd.DataFrame({"a": [1], "b": [2]})
        >>> new = pd.DataFrame({"a": [1], "b": [2]})
        >>> check_columns(new, old).passed
        True
        >>> check_columns(new.drop(columns="b"), old).passed
        False
    """
    name = "columns"
    if old_df is None:
        return _skip(name, "no previous version to compare against")
    new_cols = set(new_df.columns)
    old_cols = set(old_df.columns)
    added = sorted(new_cols - old_cols)
    missing = sorted(old_cols - new_cols)
    details = {"added": added, "missing": missing}
    problems = []
    if added and not allow_new:
        problems.append(f"new columns: {added}")
    if missing and not allow_missing:
        problems.append(f"missing columns: {missing}")
    if problems:
        return CheckResult(name, False, "; ".join(problems), details)
    note = (
        "column sets match"
        if not (added or missing)
        else (f"column changes within policy (added={added}, missing={missing})")
    )
    return CheckResult(name, True, note, details)


def check_dtypes(new_df: pd.DataFrame, old_df: pd.DataFrame | None) -> CheckResult:
    """Check that shared columns did not change dtype between versions.

    Dtype changes (e.g. int64 -> object) usually signal an upstream format
    change that will break downstream consumers.

    Example:
        >>> old = pd.DataFrame({"a": [1, 2]})
        >>> new = pd.DataFrame({"a": [1, 2]})
        >>> check_dtypes(new, old).passed
        True
        >>> check_dtypes(new.assign(a=["x", "y"]), old).passed
        False
    """
    name = "dtypes"
    if old_df is None:
        return _skip(name, "no previous version to compare against")
    shared = [c for c in new_df.columns if c in old_df.columns]
    changed = {
        c: {"old": str(old_df[c].dtype), "new": str(new_df[c].dtype)}
        for c in shared
        if str(new_df[c].dtype) != str(old_df[c].dtype)
    }
    if changed:
        return CheckResult(
            name,
            False,
            f"dtype changed for columns: {sorted(changed)}",
            {"changed": changed},
        )
    return CheckResult(name, True, f"dtypes stable across {len(shared)} shared columns")


def _numeric_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]


def check_null_rates(
    new_df: pd.DataFrame,
    old_df: pd.DataFrame | None,
    columns: list[str] | None = None,
    max_delta: float = 0.05,
) -> CheckResult:
    """Check that per-column null rates did not drift between versions.

    A sudden jump in nulls usually means an upstream feed broke or a join
    started missing.

    Args:
        new_df: the new version.
        old_df: the previous version, or None to skip the comparison.
        columns: columns to check; defaults to all shared columns.
        max_delta: maximum allowed absolute change in null fraction.

    Example:
        >>> old = pd.DataFrame({"a": [1.0, None, 3.0, 4.0]})
        >>> new = pd.DataFrame({"a": [1.0, 2.0, 3.0, 4.0]})
        >>> check_null_rates(new, old, max_delta=0.3).passed
        True
    """
    name = "null_rates"
    if old_df is None:
        return _skip(name, "no previous version to compare against")
    cols = (
        columns
        if columns is not None
        else [c for c in new_df.columns if c in old_df.columns]
    )
    drifted: dict[str, dict[str, float]] = {}
    for c in cols:
        old_rate = float(old_df[c].isna().mean())
        new_rate = float(new_df[c].isna().mean())
        delta = abs(new_rate - old_rate)
        if delta > max_delta:
            drifted[c] = {
                "old_null_rate": round(old_rate, 4),
                "new_null_rate": round(new_rate, 4),
                "delta": round(delta, 4),
            }
    if drifted:
        return CheckResult(
            name,
            False,
            f"null rate drifted beyond {max_delta} for columns: {sorted(drifted)}",
            {"drifted": drifted, "max_delta": max_delta},
        )
    return CheckResult(name, True, f"null rates stable across {len(cols)} columns")


def check_numeric_drift(
    new_df: pd.DataFrame,
    old_df: pd.DataFrame | None,
    columns: list[str] | None = None,
    max_std_shift: float = 3.0,
) -> CheckResult:
    """Check that numeric column means did not shift dramatically.

    The shift is measured in units of the old version's standard deviation:
    ``abs(new_mean - old_mean) / old_std``. A large shift usually means the
    underlying measurement or population changed.

    Args:
        new_df: the new version.
        old_df: the previous version, or None to skip the comparison.
        columns: numeric columns to check; defaults to all shared numeric
            columns.
        max_std_shift: maximum allowed mean shift in old-std units.

    Example:
        >>> old = pd.DataFrame({"v": [10.0, 11.0, 9.0, 10.0]})
        >>> new = pd.DataFrame({"v": [10.5, 11.0, 9.5, 10.0]})
        >>> check_numeric_drift(new, old).passed
        True
        >>> check_numeric_drift(new.assign(v=[100.0] * 4), old).passed
        False
    """
    name = "numeric_drift"
    if old_df is None:
        return _skip(name, "no previous version to compare against")
    if columns is None:
        columns = [c for c in _numeric_columns(new_df) if c in old_df.columns]
    drifted: dict[str, dict[str, float]] = {}
    for c in columns:
        old_mean = float(old_df[c].mean())
        old_std = float(old_df[c].std())
        new_mean = float(new_df[c].mean())
        if old_std == 0:
            if new_mean != old_mean:
                drifted[c] = {
                    "old_mean": old_mean,
                    "new_mean": new_mean,
                    "shift_std_units": float("inf"),
                }
            continue
        shift = abs(new_mean - old_mean) / old_std
        if shift > max_std_shift:
            drifted[c] = {
                "old_mean": round(old_mean, 4),
                "new_mean": round(new_mean, 4),
                "shift_std_units": round(shift, 2),
            }
    if drifted:
        return CheckResult(
            name,
            False,
            f"mean shifted beyond {max_std_shift} std for columns: {sorted(drifted)}",
            {"drifted": drifted, "max_std_shift": max_std_shift},
        )
    return CheckResult(
        name, True, f"numeric distributions stable across {len(columns)} columns"
    )


def check_categorical_values(
    new_df: pd.DataFrame,
    old_df: pd.DataFrame | None,
    columns: list[str] | None = None,
) -> CheckResult:
    """Check for new or vanished categories in categorical columns.

    New categories can break downstream ``isin`` validations (like the Pandera
    schemas used for NSSP gold datasets); vanished categories can signal a
    dropped feed.

    Args:
        new_df: the new version.
        old_df: the previous version, or None to skip the comparison.
        columns: columns to check; defaults to shared object/string columns.

    Example:
        >>> old = pd.DataFrame({"disease": ["COVID", "Flu"]})
        >>> new = pd.DataFrame({"disease": ["COVID", "Flu"]})
        >>> check_categorical_values(new, old).passed
        True
        >>> check_categorical_values(new.assign(disease=["COVID", "RSV"]),
        ...                          old).passed
        False
    """
    name = "categorical_values"
    if old_df is None:
        return _skip(name, "no previous version to compare against")
    if columns is None:
        columns = [
            c
            for c in new_df.columns
            if c in old_df.columns
            and (
                pd.api.types.is_object_dtype(new_df[c])
                or isinstance(new_df[c].dtype, pd.StringDtype)
                or isinstance(new_df[c].dtype, pd.CategoricalDtype)
            )
        ]
    changed: dict[str, dict[str, list[str]]] = {}
    for c in columns:
        old_vals = set(old_df[c].dropna().unique().tolist())
        new_vals = set(new_df[c].dropna().unique().tolist())
        added = sorted(str(v) for v in new_vals - old_vals)
        removed = sorted(str(v) for v in old_vals - new_vals)
        if added or removed:
            changed[c] = {"added": added, "removed": removed}
    if changed:
        cols = sorted(changed)
        return CheckResult(
            name,
            False,
            f"category sets changed for columns: {cols}",
            {"changed": changed},
        )
    return CheckResult(
        name, True, f"category sets stable across {len(columns)} columns"
    )


_CHECK_FUNCTIONS: dict[str, Any] = {
    "freshness": check_freshness,
    "row_count": check_row_count,
    "columns": check_columns,
    "dtypes": check_dtypes,
    "null_rates": check_null_rates,
    "numeric_drift": check_numeric_drift,
    "categorical_values": check_categorical_values,
}

# Checks that only need the new version (no previous version required).
_SINGLE_VERSION_CHECKS = {"freshness"}


def default_config() -> dict[str, dict[str, Any]]:
    """Return the default check configuration.

    Example:
        >>> cfg = default_config()
        >>> sorted(cfg)
        ['categorical_values', 'columns', 'dtypes', 'freshness', 'null_rates', 'numeric_drift', 'row_count']
    """
    return {
        "freshness": {"max_age_days": 14},
        "row_count": {"min_rows": 1, "max_change_ratio": 0.5},
        "columns": {"allow_new": False, "allow_missing": False},
        "dtypes": {},
        "null_rates": {"max_delta": 0.05},
        "numeric_drift": {"max_std_shift": 3.0},
        "categorical_values": {},
    }


def run_quality_suite(
    new_df: pd.DataFrame,
    old_df: pd.DataFrame | None = None,
    config: dict[str, dict[str, Any]] | None = None,
) -> QualityReport:
    """Run the configured quality checks and return a QualityReport.

    Args:
        new_df: the new dataset version.
        old_df: the previous version, or None if there is none yet.
        config: mapping of check name to keyword arguments. Unknown check
            names raise ValueError. Defaults to :func:`default_config`.

    Raises:
        ValueError: if ``config`` names an unknown check or a check is
            missing a required argument.

    Example:
        >>> new = pd.DataFrame({"a": [1, 2]})
        >>> report = run_quality_suite(new, None, config={"row_count": {}})
        >>> report.passed
        True
        >>> [r.name for r in report.results]
        ['row_count']
    """
    cfg = default_config() if config is None else config
    unknown = sorted(set(cfg) - set(_CHECK_FUNCTIONS))
    if unknown:
        raise ValueError(f"unknown quality checks: {unknown}")
    results: list[CheckResult] = []
    for check_name, kwargs in cfg.items():
        func = _CHECK_FUNCTIONS[check_name]
        try:
            if check_name in _SINGLE_VERSION_CHECKS:
                results.append(func(new_df, **kwargs))
            else:
                results.append(func(new_df, old_df, **kwargs))
        except TypeError as exc:
            raise ValueError(
                f"invalid arguments for quality check '{check_name}': {exc}"
            ) from exc
    report = QualityReport(results=results)
    logger.info(
        "quality suite: %d passed, %d failed, %d skipped",
        sum(r.passed and not r.skipped for r in results),
        sum(not r.passed for r in results),
        sum(r.skipped for r in results),
    )
    return report


def _read_dataframe(path: str | Path) -> pd.DataFrame:
    suffix = Path(path).suffix.lower()
    if suffix in {".parquet", ".parq"}:
        return pd.read_parquet(path)
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".json", ".ndjson", ".jsonl"}:
        return pd.read_json(path, lines=suffix != ".json")
    raise ValueError(f"unsupported file type for quality check input: {suffix}")


def _load_config(path: str | Path | None) -> dict[str, dict[str, Any]]:
    if path is None:
        return default_config()
    try:
        import tomllib
    except ImportError:  # Python 3.10
        import tomli as tomllib  # type: ignore[no-redef]
    with open(path, "rb") as f:
        return tomllib.load(f)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point: compare two dataset versions and exit non-zero on failure.

    Example:
        python -m cfa.dataops.quality --new new.parquet --old old.parquet
    """
    parser = argparse.ArgumentParser(
        description="Run data-quality drift checks between two dataset versions."
    )
    parser.add_argument(
        "--new", required=True, help="new version file (parquet/csv/json)"
    )
    parser.add_argument("--old", help="previous version file (parquet/csv/json)")
    parser.add_argument("--config", help="TOML file mapping check names to kwargs")
    parser.add_argument(
        "--fail-on-skip",
        action="store_true",
        help="treat skipped checks as failures (strict CI mode)",
    )
    args = parser.parse_args(argv)

    new_df = _read_dataframe(args.new)
    old_df = _read_dataframe(args.old) if args.old else None
    report = run_quality_suite(new_df, old_df, _load_config(args.config))
    print(report.summary())
    if args.fail_on_skip and any(r.skipped for r in report.results):
        skipped = [r.name for r in report.results if r.skipped]
        print(f"FAILED: skipped checks: {skipped}")
        return 1
    if not report.passed:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "CheckResult",
    "QualityCheckError",
    "QualityReport",
    "check_categorical_values",
    "check_columns",
    "check_dtypes",
    "check_freshness",
    "check_null_rates",
    "check_numeric_drift",
    "check_row_count",
    "default_config",
    "run_quality_suite",
]

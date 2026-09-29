"""Tests for cfa.dataops.quality (no Azure access; synthetic data only)."""

import pandas as pd
import pytest

from cfa.dataops import quality
from cfa.dataops.quality import (
    CheckResult,
    QualityCheckError,
    QualityReport,
    check_categorical_values,
    check_columns,
    check_dtypes,
    check_freshness,
    check_null_rates,
    check_numeric_drift,
    check_row_count,
    default_config,
    run_quality_suite,
)


def _nssp_like(n=200, seed=7):
    rng = __import__("random").Random(seed)
    dates = pd.date_range("2026-09-01", periods=n, freq="D").strftime("%Y-%m-%d")
    return pd.DataFrame(
        {
            "report_date": dates,
            "reference_date": dates,
            "metric": [
                rng.choice(["count_ed_visits", "percent_ed_visits"]) for _ in range(n)
            ],
            "geo_value": [rng.choice(["CA", "TX", "NY"]) for _ in range(n)],
            "disease": [
                rng.choice(["COVID-19/Omicron", "Influenza"]) for _ in range(n)
            ],
            "value": [float(rng.randint(0, 1000)) for _ in range(n)],
        }
    )


# ---------------------------------------------------------------------------
# Report / result plumbing


def test_report_passed_and_failed_lists():
    report = QualityReport(
        results=[
            CheckResult("a", True, "ok"),
            CheckResult("b", False, "bad"),
        ]
    )
    assert not report.passed
    assert [r.name for r in report.failed] == ["b"]
    with pytest.raises(QualityCheckError, match="quality check"):
        report.raise_on_failure()


def test_report_raise_on_failure_lists_all_failures():
    report = QualityReport(
        results=[
            CheckResult("a", False, "first problem"),
            CheckResult("b", False, "second problem"),
        ]
    )
    with pytest.raises(QualityCheckError) as exc_info:
        report.raise_on_failure()
    assert "a: first problem" in str(exc_info.value)
    assert "b: second problem" in str(exc_info.value)


def test_skipped_checks_do_not_fail_report():
    report = QualityReport(
        results=[CheckResult("a", True, "skipped: no old version", skipped=True)]
    )
    assert report.passed
    report.raise_on_failure()  # must not raise


def test_report_summary_and_as_dict():
    report = QualityReport(
        results=[
            CheckResult("a", True, "ok"),
            CheckResult("b", False, "bad"),
            CheckResult("c", True, "n/a", skipped=True),
        ]
    )
    summary = report.summary()
    assert "[PASS] a: ok" in summary
    assert "[FAIL] b: bad" in summary
    assert "[SKIP] c: n/a" in summary
    d = report.as_dict()
    assert d["passed"] is False
    assert [r["name"] for r in d["results"]] == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# check_freshness


def test_freshness_passes_for_recent_data():
    df = pd.DataFrame({"report_date": ["2026-09-27", "2026-09-28"]})
    result = check_freshness(df, "report_date", 7, reference_date="2026-09-28")
    assert result.passed
    assert result.details["age_days"] == 0


def test_freshness_fails_for_stale_data():
    df = pd.DataFrame({"report_date": ["2026-08-01"]})
    result = check_freshness(df, "report_date", 7, reference_date="2026-09-28")
    assert not result.passed
    assert result.details["age_days"] == 58


def test_freshness_fails_for_future_dates():
    df = pd.DataFrame({"report_date": ["2026-10-01"]})
    result = check_freshness(df, "report_date", 7, reference_date="2026-09-28")
    assert not result.passed
    assert "future" in result.message


def test_freshness_fails_for_missing_column():
    df = pd.DataFrame({"other": ["2026-09-28"]})
    result = check_freshness(df, "report_date", 7, reference_date="2026-09-28")
    assert not result.passed


def test_freshness_fails_for_unparsable_dates():
    df = pd.DataFrame({"report_date": ["not-a-date"]})
    result = check_freshness(df, "report_date", 7, reference_date="2026-09-28")
    assert not result.passed


# ---------------------------------------------------------------------------
# check_row_count


def test_row_count_passes_within_ratio():
    old = pd.DataFrame({"a": range(100)})
    new = pd.DataFrame({"a": range(120)})
    assert check_row_count(new, old).passed


def test_row_count_fails_on_big_drop():
    old = pd.DataFrame({"a": range(100)})
    new = pd.DataFrame({"a": range(10)})
    result = check_row_count(new, old)
    assert not result.passed
    assert result.details["change_ratio"] == pytest.approx(0.9)


def test_row_count_fails_on_empty_new_version():
    old = pd.DataFrame({"a": range(100)})
    new = pd.DataFrame({"a": []})
    result = check_row_count(new, old)
    assert not result.passed
    assert "0 rows" in result.message


def test_row_count_skips_without_old_version():
    new = pd.DataFrame({"a": range(10)})
    result = check_row_count(new, None)
    assert result.skipped and result.passed


# ---------------------------------------------------------------------------
# check_columns / check_dtypes


def test_columns_pass_when_matching():
    old = pd.DataFrame({"a": [1], "b": [2]})
    assert check_columns(old.copy(), old).passed


def test_columns_fail_on_missing_column():
    old = pd.DataFrame({"a": [1], "b": [2]})
    new = pd.DataFrame({"a": [1]})
    result = check_columns(new, old)
    assert not result.passed
    assert result.details["missing"] == ["b"]


def test_columns_allow_missing_flag():
    old = pd.DataFrame({"a": [1], "b": [2]})
    new = pd.DataFrame({"a": [1]})
    assert check_columns(new, old, allow_missing=True).passed


def test_columns_fail_on_new_column_by_default():
    old = pd.DataFrame({"a": [1]})
    new = pd.DataFrame({"a": [1], "b": [2]})
    result = check_columns(new, old)
    assert not result.passed
    assert result.details["added"] == ["b"]


def test_dtypes_pass_when_stable():
    old = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    assert check_dtypes(old.copy(), old).passed


def test_dtypes_fail_on_change():
    old = pd.DataFrame({"a": [1, 2]})
    new = pd.DataFrame({"a": ["1", "2"]})
    result = check_dtypes(new, old)
    assert not result.passed
    assert "a" in result.details["changed"]


# ---------------------------------------------------------------------------
# check_null_rates


def test_null_rates_pass_when_stable():
    old = pd.DataFrame({"a": [1.0, None, 3.0, 4.0]})
    new = pd.DataFrame({"a": [1.0, 2.0, 3.0, 4.0]})
    assert check_null_rates(new, old, max_delta=0.3).passed


def test_null_rates_fail_on_jump():
    old = pd.DataFrame({"a": [1.0, 2.0, 3.0, 4.0]})
    new = pd.DataFrame({"a": [None, None, None, 4.0]})
    result = check_null_rates(new, old)
    assert not result.passed
    assert result.details["drifted"]["a"]["delta"] == pytest.approx(0.75)


# ---------------------------------------------------------------------------
# check_numeric_drift


def test_numeric_drift_passes_for_small_shift():
    old = pd.DataFrame({"v": [10.0, 11.0, 9.0, 10.0]})
    new = pd.DataFrame({"v": [10.5, 11.0, 9.5, 10.0]})
    assert check_numeric_drift(new, old).passed


def test_numeric_drift_fails_for_large_shift():
    old = pd.DataFrame({"v": [10.0, 11.0, 9.0, 10.0]})
    new = pd.DataFrame({"v": [100.0, 101.0, 99.0, 100.0]})
    result = check_numeric_drift(new, old)
    assert not result.passed
    assert result.details["drifted"]["v"]["shift_std_units"] > 3.0


def test_numeric_drift_zero_std_constant_shift_fails():
    old = pd.DataFrame({"v": [5.0, 5.0, 5.0]})
    new = pd.DataFrame({"v": [6.0, 6.0, 6.0]})
    result = check_numeric_drift(new, old)
    assert not result.passed


def test_numeric_drift_zero_std_no_shift_passes():
    old = pd.DataFrame({"v": [5.0, 5.0, 5.0]})
    new = pd.DataFrame({"v": [5.0, 5.0, 5.0]})
    assert check_numeric_drift(new, old).passed


# ---------------------------------------------------------------------------
# check_categorical_values


def test_categorical_values_pass_when_stable():
    old = pd.DataFrame({"disease": ["COVID", "Flu"]})
    assert check_categorical_values(old.copy(), old).passed


def test_categorical_values_fail_on_new_category():
    old = pd.DataFrame({"disease": ["COVID", "Flu"]})
    new = pd.DataFrame({"disease": ["COVID", "RSV"]})
    result = check_categorical_values(new, old)
    assert not result.passed
    assert result.details["changed"]["disease"]["added"] == ["RSV"]
    assert result.details["changed"]["disease"]["removed"] == ["Flu"]


def test_categorical_values_ignores_nulls():
    old = pd.DataFrame({"disease": ["COVID", None]})
    new = pd.DataFrame({"disease": ["COVID", None]})
    assert check_categorical_values(new, old).passed


# ---------------------------------------------------------------------------
# run_quality_suite


def test_suite_passes_for_healthy_nssp_like_versions():
    old = _nssp_like()
    new = _nssp_like(seed=99)
    config = default_config()
    config["freshness"] = {
        "date_column": "reference_date",
        "max_age_days": 36500,
        "reference_date": "2027-04-01",
    }
    report = run_quality_suite(new, old, config)
    assert report.passed, report.summary()


def test_suite_fails_for_broken_new_version():
    old = _nssp_like()
    new = _nssp_like(seed=99).drop(columns=["value"])
    new["disease"] = "Novel Pathogen X"
    config = default_config()
    config["freshness"] = {
        "date_column": "reference_date",
        "max_age_days": 36500,
        "reference_date": "2027-04-01",
    }
    report = run_quality_suite(new, old, config)
    assert not report.passed
    failed_names = {r.name for r in report.failed}
    assert "columns" in failed_names
    assert "categorical_values" in failed_names


def test_suite_skips_comparison_checks_without_old_version():
    new = _nssp_like()
    report = run_quality_suite(new, None, config={"row_count": {}})
    assert report.passed
    assert report.results[0].skipped


def test_suite_rejects_unknown_check():
    with pytest.raises(ValueError, match="unknown quality checks"):
        run_quality_suite(pd.DataFrame({"a": [1]}), None, config={"nope": {}})


def test_suite_rejects_bad_arguments():
    with pytest.raises(ValueError, match="invalid arguments"):
        run_quality_suite(
            pd.DataFrame({"a": [1]}), None, config={"freshness": {"bogus_arg": 1}}
        )


# ---------------------------------------------------------------------------
# CLI


def test_cli_passes_and_returns_zero(tmp_path):
    new_path = tmp_path / "new.csv"
    old_path = tmp_path / "old.csv"
    _nssp_like().to_csv(new_path, index=False)
    _nssp_like(seed=99).to_csv(old_path, index=False)
    config_path = tmp_path / "checks.toml"
    config_path.write_text(
        '[freshness]\ndate_column = "reference_date"\n'
        'max_age_days = 36500\nreference_date = "2027-04-01"\n'
        "[row_count]\n[columns]\n[dtypes]\n[null_rates]\n"
        "[numeric_drift]\n[categorical_values]\n"
    )
    rc = quality.main(
        ["--new", str(new_path), "--old", str(old_path), "--config", str(config_path)]
    )
    assert rc == 0


def test_cli_returns_one_on_failure(tmp_path):
    new_path = tmp_path / "new.csv"
    old_path = tmp_path / "old.csv"
    _nssp_like(seed=99).drop(columns=["value"]).to_csv(new_path, index=False)
    _nssp_like().to_csv(old_path, index=False)
    rc = quality.main(["--new", str(new_path), "--old", str(old_path)])
    assert rc == 1


def test_cli_rejects_unsupported_file_type(tmp_path):
    bad = tmp_path / "new.txt"
    bad.write_text("hello")
    with pytest.raises(ValueError, match="unsupported file type"):
        quality.main(["--new", str(bad)])

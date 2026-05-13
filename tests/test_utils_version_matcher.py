"""Tests for utils.version_matcher function."""

import pytest

from cfa.dataops.utils import version_matcher


class TestVersionMatcher:
    """Tests for the version_matcher function."""

    def test_latest(self):
        available_versions = [
            "2025-12-15T00-00-00",
            "2025-12-16T00-00-00",
            "2025-12-17T00-00-00",
        ]

        assert version_matcher("latest", available_versions) == "2025-12-17T00-00-00"

    def test_newer_than_latest(self):
        available_versions = [
            "2025-12-15T00-00-00",
            "2025-12-16T00-00-00",
            "2025-12-17T00-00-00",
        ]

        with pytest.raises(ValueError, match="newer than the newest available version"):
            version_matcher("2025-12-18T00-00-00", available_versions)

    def test_invalid_date(self):
        available_versions = [
            "2025-12-15T00-00-00",
            "2025-12-16T00-00-00",
            "2025-12-17T00-00-00",
        ]

        with pytest.raises(ValueError, match="could not be parsed as a date"):
            version_matcher("2025-99-99T00-00-00", available_versions)

    def test_date_range(self):
        available_versions = [
            "2025-12-15T00-00-00",
            "2025-12-16T00-00-00",
            "2025-12-17T00-00-00",
            "2025-12-18T00-00-00",
        ]

        assert (
            version_matcher(
                ">=2025-12-16T00-00-00,<2025-12-18T00-00-00",
                available_versions,
            )
            == "2025-12-17T00-00-00"
        )

    def test_date_range_multiple_matches(self):
        available_versions = [
            "2025-12-15T00-00-00",
            "2025-12-16T00-00-00",
            "2025-12-17T00-00-00",
            "2025-12-18T00-00-00",
        ]

        matches = version_matcher(
            ">=2025-12-16T00-00-00,<2025-12-18T00-00-00",
            available_versions,
            newest=None,
        )

        assert matches == ["2025-12-17T00-00-00", "2025-12-16T00-00-00"]

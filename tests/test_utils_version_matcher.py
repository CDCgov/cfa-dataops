"""Tests for utils.version_matcher function."""

import pytest

from cfa.dataops.utils import version_matcher


class TestVersionMatcher:
    """Tests for the version_matcher function."""

    def test_version_matcher_latest(self):
        """Test that latest returns the newest available version."""
        available_versions = [
            "2025-12-15T00-00-00",
            "2025-12-16T00-00-00",
            "2025-12-17T00-00-00",
        ]

        assert version_matcher("latest", available_versions) == "2025-12-17T00-00-00"

    def test_version_matcher_newer_than_latest(self):
        """Test that a version newer than the newest available version errors clearly."""
        available_versions = [
            "2025-12-15T00-00-00",
            "2025-12-16T00-00-00",
            "2025-12-17T00-00-00",
        ]

        with pytest.raises(ValueError, match="newer than the newest available version"):
            version_matcher("2025-12-18T00-00-00", available_versions)

    def test_version_matcher_invalid_date(self):
        """Test that an invalid date-like version gets a clear error."""
        available_versions = [
            "2025-12-15T00-00-00",
            "2025-12-16T00-00-00",
            "2025-12-17T00-00-00",
        ]

        with pytest.raises(ValueError, match="could not be parsed as a date"):
            version_matcher("2025-99-99T00-00-00", available_versions)

    def test_version_matcher_existing_version(self):
        """Test that an exact version still resolves normally."""
        available_versions = [
            "2025-12-15T00-00-00",
            "2025-12-16T00-00-00",
            "2025-12-17T00-00-00",
        ]

        assert (
            version_matcher("2025-12-16T00-00-00", available_versions)
            == "2025-12-16T00-00-00"
        )

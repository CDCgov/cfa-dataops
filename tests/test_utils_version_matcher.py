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

        assert (
            version_matcher(selection="newest", available_versions=available_versions)
            == "2025-12-17T00-00-00"
        )

    def test_latest_oldest_when_newest_false(self):
        available_versions = [
            "2025-12-15T00-00-00",
            "2025-12-16T00-00-00",
            "2025-12-17T00-00-00",
        ]

        assert (
            version_matcher(selection="oldest", available_versions=available_versions)
            == "2025-12-15T00-00-00"
        )

    def test_newer_than_latest(self):
        from packaging.specifiers import InvalidSpecifier

        available_versions = [
            "2025-12-15T00-00-00",
            "2025-12-16T00-00-00",
            "2025-12-17T00-00-00",
        ]

        with pytest.raises(InvalidSpecifier):
            version_matcher("2025-12-18T00-00-00", available_versions)

    def test_invalid_date(self):
        from packaging.specifiers import InvalidSpecifier

        available_versions = [
            "2025-12-15T00-00-00",
            "2025-12-16T00-00-00",
            "2025-12-17T00-00-00",
        ]

        with pytest.raises(InvalidSpecifier):
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
            selection="all",
        )

        assert matches == ["2025-12-17T00-00-00", "2025-12-16T00-00-00"]

    def test_date_range_respects_upper_bound(self):
        available_versions = [
            "2026-03-14T23-59-59",
            "2026-03-15T00-10-00",
            "2026-04-10T12-00-00",
            "2026-04-15T00-19-59",
            "2026-04-15T00-20-00",
            "2026-04-16T00-00-00",
        ]

        assert (
            version_matcher(
                ">=2026-03-15T00-10-00,<2026-04-15T00-20-00",
                available_versions,
            )
            == "2026-04-15T00-19-59"
        )

    @pytest.mark.parametrize(
        "spec,available_versions,expected_newest,expected_oldest,expected_all",
        [
            (
                "==2025-12-15",
                [
                    "2025-12-14T23-59-59",
                    "2025-12-15T00-00-00",
                    "2025-12-15T12-30-00",
                    "2025-12-16T00-00-00",
                ],
                "2025-12-15T00-00-00",
                "2025-12-15T00-00-00",
                ["2025-12-15T00-00-00"],
            ),
            (
                "<2025-12-15",
                [
                    "2025-12-14T23-59-59",
                    "2025-12-15T00-00-00",
                    "2025-12-16T00-00-00",
                ],
                "2025-12-14T23-59-59",
                "2025-12-14T23-59-59",
                ["2025-12-14T23-59-59"],
            ),
            (
                ">2025",
                [
                    "2024-12-31T23-59-59",
                    "2025-01-01T00-00-00",
                    "2025-12-15T00-00-00",
                    "2026-01-01T00-00-00",
                ],
                "2026-01-01T00-00-00",
                "2025-01-01T00-00-00",
                [
                    "2026-01-01T00-00-00",
                    "2025-12-15T00-00-00",
                    "2025-01-01T00-00-00",
                ],
            ),
        ],
    )
    def test_partial_date_specifiers(
        self,
        spec,
        available_versions,
        expected_newest,
        expected_oldest,
        expected_all,
    ):
        assert version_matcher(spec, available_versions) == expected_newest
        assert (
            version_matcher(spec, available_versions, selection="oldest")
            == expected_oldest
        )
        assert (
            version_matcher(spec, available_versions, selection="all") == expected_all
        )

    def test_partial_date_specifier_equal_date(self):
        available_versions = [
            "2025-12-14T23-59-59",
            "2025-12-15T00-00-00",
            "2025-12-15T12-30-00",
            "2025-12-16T00-00-00",
        ]

        assert (
            version_matcher("==2025-12-15", available_versions) == "2025-12-15T00-00-00"
        )
        assert (
            version_matcher("==2025-12-15", available_versions, selection="oldest")
            == "2025-12-15T00-00-00"
        )
        assert version_matcher("==2025-12-15", available_versions, selection="all") == [
            "2025-12-15T00-00-00",
        ]

    def test_partial_date_specifier_less_than_date(self):
        available_versions = [
            "2025-12-14T23-59-59",
            "2025-12-15T00-00-00",
            "2025-12-16T00-00-00",
        ]

        assert (
            version_matcher("<2025-12-15", available_versions) == "2025-12-14T23-59-59"
        )
        assert (
            version_matcher("<2025-12-15", available_versions, selection="oldest")
            == "2025-12-14T23-59-59"
        )
        assert version_matcher("<2025-12-15", available_versions, selection="all") == [
            "2025-12-14T23-59-59"
        ]

    def test_partial_date_specifier_greater_than_year(self):
        available_versions = [
            "2024-12-31T23-59-59",
            "2025-01-01T00-00-00",
            "2025-12-15T00-00-00",
            "2026-01-01T00-00-00",
        ]

        assert version_matcher(">2025", available_versions) == "2026-01-01T00-00-00"
        assert (
            version_matcher(">2025", available_versions, selection="oldest")
            == "2025-01-01T00-00-00"
        )
        assert version_matcher(">2025", available_versions, selection="all") == [
            "2026-01-01T00-00-00",
            "2025-12-15T00-00-00",
            "2025-01-01T00-00-00",
        ]

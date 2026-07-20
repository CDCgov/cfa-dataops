"""Tests for utils.version_matcher function."""

import pytest

from cfa.dataops.utils import construct_version_spec, version_matcher


class TestConstructVersionSpec:
    """Tests for construct_version_spec helper."""

    @pytest.mark.parametrize(
        "raw_version,expected",
        [
            ("2025-12-15T00-00-00", "==2025-12-15T00-00-00"),
            (">2025-12-15T00-00-00", ">2025-12-15T00-00-00"),
            ("<2025-12-15T00-00-00", "<2025-12-15T00-00-00"),
            ("==2025-12-15T00-00-00", "==2025-12-15T00-00-00"),
            ("~=2025-12-15T00-00-00", "~=2025-12-15T00-00-00"),
            ("!=2025-12-15T00-00-00", "!=2025-12-15T00-00-00"),
            ("", ""),
            ("   ", ""),
        ],
    )
    def test_construct_version_spec(self, raw_version, expected):
        assert construct_version_spec(raw_version) == expected

    def test_version_matcher_accepts_raw_version(self):
        available_versions = [
            "2025-12-15T00-00-00",
            "2025-12-16T00-00-00",
            "2025-12-17T00-00-00",
        ]
        assert (
            version_matcher("2025-12-16T00-00-00", available_versions)
            == "2025-12-16T00-00-00"
        )

class TestVersionMatcher:
    """Tests for the version_matcher function."""

    def test_latest(self):
        available_versions = [
            "2025-12-15T00-00-00",
            "2025-12-16T00-00-00",
            "2025-12-17T00-00-00",
        ]

        assert (
            version_matcher(None, available_versions, selection="newest")
            == "2025-12-17T00-00-00"
        )

    def test_latest_oldest_when_selection_oldest(self):
        available_versions = [
            "2025-12-15T00-00-00",
            "2025-12-16T00-00-00",
            "2025-12-17T00-00-00",
        ]

        assert (
            version_matcher(None, available_versions, selection="oldest")
            == "2025-12-15T00-00-00"
        )

    def test_newer_than_latest(self):

        available_versions = [
            "2025-12-15T00-00-00",
            "2025-12-16T00-00-00",
            "2025-12-17T00-00-00",
        ]

        assert (
            version_matcher(
                ">2025-12-17T00-00-00", available_versions, selection="newest"
            )
            is None
        )

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

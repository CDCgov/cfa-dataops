"""Tests for command.py functions"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from cfa.dataops.command import (
    _get_dataset_namespaces,
    _get_stages_list,
    _get_versions_list,
    get_available_data,
    get_dataset_stages,
    get_dataset_versions,
    save_data_locally,
)


@pytest.fixture
def mock_datacat(mocker):
    """Create a mock datacat object for testing"""
    # Create a custom namespace that behaves like the real datacat
    from types import SimpleNamespace

    # Create mock stages
    mock_extract = MagicMock()
    mock_load = MagicMock()
    mock_load.get_versions.return_value = [
        "2025-01-02T12-00-00",
        "2025-01-01T12-00-00",
    ]
    mock_load.download_version_to_local.return_value = True
    mock_stage_01 = MagicMock()

    # Create dataset with __dict__ for stages
    dataset1 = SimpleNamespace(
        extract=mock_extract,
        load=mock_load,
        stage_01=mock_stage_01,
    )

    # Create test namespace
    test_ns = SimpleNamespace(dataset1=dataset1, dataset2=MagicMock())

    # Create prod namespace
    prod_ns = SimpleNamespace(dataset1=MagicMock())

    # Create main catalog
    catalog = SimpleNamespace(
        test=test_ns,
        prod=prod_ns,
        __namespace_list__=[
            "test.dataset1",
            "test.dataset2",
            "prod.dataset1",
        ],
    )

    mocker.patch("cfa.dataops.command.datacat", catalog)
    return catalog


class TestGetDatasetNamespaces:
    """Tests for _get_dataset_namespaces helper function"""

    def test_get_dataset_namespaces(self, mock_datacat):
        """Test getting dataset namespaces"""
        result = _get_dataset_namespaces()

        assert isinstance(result, list)
        assert "test.dataset1" in result
        assert "test.dataset2" in result
        assert "prod.dataset1" in result


class TestGetStagesList:
    """Tests for _get_stages_list helper function"""

    def test_get_stages_list_valid_dataset(self, mock_datacat, capsys):
        """Test getting stages for a valid dataset"""
        result = _get_stages_list("test.dataset1")

        assert isinstance(result, list)
        assert "extract" in result
        assert "load" in result
        assert "stage_01" in result
        # Verify stages are sorted
        assert result == sorted(result)

    def test_get_stages_list_invalid_dataset(self, mock_datacat, capsys):
        """Test getting stages for invalid dataset prints error"""
        result = _get_stages_list("nonexistent.dataset")

        assert result is None

        # Check error message was printed
        captured = capsys.readouterr()
        assert "Error" in captured.out
        assert "Dataset namespace 'nonexistent.dataset' not found" in captured.out

    def test_get_stages_list_filters_correct_prefixes(self, mock_datacat):
        """Test that only stages with correct prefixes are returned"""
        # Add a mock dataset with various keys
        mock_dataset = MagicMock()
        mock_dataset.__dict__ = {
            "extract": MagicMock(),
            "load": MagicMock(),
            "stage_01": MagicMock(),
            "stage_02": MagicMock(),
            "config": {},  # Should be filtered
            "metadata": {},  # Should be filtered
        }
        mock_datacat.test.dataset2 = mock_dataset

        result = _get_stages_list("test.dataset2")

        assert "extract" in result
        assert "load" in result
        assert "stage_01" in result
        assert "stage_02" in result
        assert "config" not in result
        assert "metadata" not in result


class TestGetVersionsList:
    """Tests for _get_versions_list helper function"""

    def test_get_versions_list_valid_stage(self, mock_datacat, capsys):
        """Test getting versions for a valid stage"""
        result = _get_versions_list("test.dataset1", "load")

        assert isinstance(result, list)
        assert "2025-01-02T12-00-00" in result
        assert "2025-01-01T12-00-00" in result

    def test_get_versions_list_invalid_stage(self, mock_datacat, capsys):
        """Test getting versions for invalid stage prints error"""
        result = _get_versions_list("test.dataset1", "nonexistent_stage")

        assert result is None

        # Check error message was printed
        captured = capsys.readouterr()
        assert "Error" in captured.out
        assert "Stage 'nonexistent_stage' not found" in captured.out


class TestHelperFunctionsIntegration:
    """Integration tests for command helper functions"""

    def test_stages_sorted_alphabetically(self, mock_datacat):
        """Test that stages are returned in sorted order"""
        # Create a dataset with unsorted stages
        mock_dataset = MagicMock()
        mock_dataset.__dict__ = {
            "stage_03": MagicMock(),
            "load": MagicMock(),
            "stage_01": MagicMock(),
            "extract": MagicMock(),
            "stage_02": MagicMock(),
        }
        mock_datacat.test.dataset2 = mock_dataset

        result = _get_stages_list("test.dataset2")

        expected_order = [
            "extract",
            "load",
            "stage_01",
            "stage_02",
            "stage_03",
        ]
        assert result == expected_order


class TestCliCommands:
    """Tests for CLI command entry points"""

    def test_get_available_data_with_prefix_filter(self, mock_datacat, mocker):
        """Test listing datasets filtered by prefix"""
        mocker.patch(
            "cfa.dataops.command.ArgumentParser.parse_args",
            return_value=SimpleNamespace(prefix="test"),
        )
        print_mock = mocker.patch("cfa.dataops.command.Console.print")

        get_available_data()

        print_mock.assert_called_once()
        output = print_mock.call_args.args[0]
        assert "Available Datasets" in output
        assert "- test.dataset1" in output
        assert "- test.dataset2" in output
        assert "prod.dataset1" not in output

    def test_get_dataset_stages_formats_default_stage(self, mock_datacat, mocker):
        """Test listing stages highlights the final stage as default"""
        mocker.patch(
            "cfa.dataops.command.ArgumentParser.parse_args",
            return_value=SimpleNamespace(dataset="test.dataset1"),
        )
        print_mock = mocker.patch("cfa.dataops.command.Console.print")

        get_dataset_stages()

        print_mock.assert_called_once()
        output = print_mock.call_args.args[0]
        assert "Stages for test.dataset1" in output
        assert "- extract" in output
        assert "- load" in output
        assert "- [red]stage_01[/red]" in output
        assert "Stages in [red]red[/red] indicate the default stage" in output

    def test_get_dataset_versions_uses_requested_stage(self, mock_datacat, mocker):
        """Test listing versions for an explicit stage"""
        mocker.patch(
            "cfa.dataops.command.ArgumentParser.parse_args",
            return_value=SimpleNamespace(dataset="test.dataset1", stage="load"),
        )
        print_mock = mocker.patch("cfa.dataops.command.Console.print")

        get_dataset_versions()

        print_mock.assert_called_once()
        output = print_mock.call_args.args[0]
        assert "[bold]test.dataset1[/bold]" in output
        assert "- [red]2025-01-02T12-00-00[/red]" in output
        assert "- 2025-01-01T12-00-00" in output

    def test_save_data_locally_prints_tree_on_download(self, mock_datacat, mocker):
        """Test save_data_locally prints tree output after a download"""
        mocker.patch(
            "cfa.dataops.command.ArgumentParser.parse_args",
            return_value=SimpleNamespace(
                dataset="test.dataset1",
                location="downloads",
                stage="load",
                version="2025-01-02T12-00-00",
                force=True,
                oldest=True,
            ),
        )
        mocker.patch(
            "cfa.dataops.command.os.path.abspath",
            return_value="/tmp/downloads",
        )
        tree_mock = mocker.patch(
            "cfa.dataops.command.tree",
            return_value="tree output",
        )
        print_mock = mocker.patch("cfa.dataops.command.Console.print")

        save_data_locally()

        mock_datacat.test.dataset1.load.download_version_to_local.assert_called_once_with(
            "/tmp/downloads",
            version_spec="2025-01-02T12-00-00",
            force=True,
            selection="oldest",
        )
        tree_mock.assert_called_once_with("/tmp/downloads", show_hidden=False)
        print_mock.assert_called_once()
        output = print_mock.call_args.args[0]
        assert "has been saved locally" in output
        assert "/tmp/downloads" in output
        assert "tree output" in output

    def test_save_data_locally_notifies_existing_download(self, mock_datacat, mocker):
        """Test save_data_locally notifies when the dataset already exists"""
        mock_datacat.test.dataset1.load.download_version_to_local.return_value = False
        mocker.patch(
            "cfa.dataops.command.ArgumentParser.parse_args",
            return_value=SimpleNamespace(
                dataset="test.dataset1",
                location="downloads",
                stage="load",
                version="2025-01-02T12-00-00",
                force=False,
                oldest=False,
            ),
        )
        mocker.patch(
            "cfa.dataops.command.os.path.abspath",
            return_value="/tmp/downloads",
        )
        tree_mock = mocker.patch("cfa.dataops.command.tree")
        print_mock = mocker.patch("cfa.dataops.command.Console.print")

        save_data_locally()

        mock_datacat.test.dataset1.load.download_version_to_local.assert_called_once_with(
            "/tmp/downloads",
            version_spec="2025-01-02T12-00-00",
            force=False,
            selection="newest",
        )
        tree_mock.assert_not_called()
        print_mock.assert_called_once()
        output = print_mock.call_args.args[0]
        assert "is already present" in output
        assert "Use --force to re-download" in output

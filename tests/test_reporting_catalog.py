from types import SimpleNamespace

import pytest

from cfa.dataops.reporting.catalog import (
    NotebookEndpoint,
    nb_to_html,
    remove_ws_and_nonalpha,
    report_dict_to_sn,
    retitle_notebook,
)


class TestRemoveWsAndNonalpha:
    """Test the remove_ws_and_nonalpha function."""

    def test_remove_whitespace_and_special_chars(self):
        """Test removing whitespace and special characters."""
        result = remove_ws_and_nonalpha("Hello World! 123.ipynb")
        assert result == "hello_world_123_ipynb"

    def test_remove_spaces_and_dots(self):
        """Test removing spaces and dots."""
        result = remove_ws_and_nonalpha("test file.name.ipynb")
        assert result == "test_file_name_ipynb"

    def test_empty_string(self):
        """Test with empty string."""
        result = remove_ws_and_nonalpha("")
        assert result == ""

    def test_only_alphanumeric(self):
        """Test with only alphanumeric characters."""
        result = remove_ws_and_nonalpha("test123")
        assert result == "test123"

    def test_underscores_preserved(self):
        """Test that underscores are preserved."""
        result = remove_ws_and_nonalpha("test_file_name")
        assert result == "test_file_name"


class TestRetitleNotebook:
    """Test the retitle_notebook function."""

    def test_retitle_notebook_success(self, mocker):
        """Test successfully retitling a notebook."""
        # Mock notebook content - create a proper notebook object with metadata attribute
        mock_notebook = SimpleNamespace()
        mock_notebook.metadata = {"title": "Old Title"}
        mock_notebook.cells = []

        # Mock file operations
        mock_exists = mocker.patch("os.path.exists", return_value=True)
        mock_nbformat_read = mocker.patch(
            "nbformat.read", return_value=mock_notebook
        )
        mock_nbformat_write = mocker.patch("nbformat.write")
        _ = mocker.patch(
            "builtins.open", mocker.mock_open(read_data="notebook_content")
        )

        # Test the function
        retitle_notebook("test.ipynb", "New Title")

        # Verify calls
        mock_exists.assert_called_once_with("test.ipynb")
        mock_nbformat_read.assert_called_once()
        mock_nbformat_write.assert_called_once()
        assert mock_notebook.metadata["title"] == "New Title"

    def test_retitle_notebook_file_not_exists(self, mocker):
        """Test retitle_notebook with non-existent file."""
        mocker.patch("os.path.exists", return_value=False)

        with pytest.raises(AssertionError, match="does not exist"):
            retitle_notebook("nonexistent.ipynb", "New Title")

    def test_retitle_notebook_wrong_extension(self, mocker):
        """Test retitle_notebook with wrong file extension."""
        mocker.patch("os.path.exists", return_value=True)

        with pytest.raises(AssertionError, match="must be a .ipynb file"):
            retitle_notebook("test.txt", "New Title")


class TestNbToHtml:
    """Test the nb_to_html function."""

    def test_nb_to_html_success(self, mocker):
        """Test successful notebook to HTML conversion."""
        mock_html_content = "<html><body>Test HTML</body></html>"
        mock_exporter = mocker.MagicMock()
        mock_exporter.from_filename.return_value = (mock_html_content, {})

        mocker.patch(
            "cfa.dataops.reporting.catalog.HTMLExporter",
            return_value=mock_exporter,
        )

        result = nb_to_html("test.ipynb")

        assert result == mock_html_content
        mock_exporter.from_filename.assert_called_once_with("test.ipynb")


class TestNotebookEndpoint:
    """Test the NotebookEndpoint class."""

    @pytest.fixture
    def notebook_endpoint(self):
        """Create a NotebookEndpoint instance for testing."""
        return NotebookEndpoint("test_notebook.ipynb")

    def test_init(self, notebook_endpoint):
        """Test NotebookEndpoint initialization."""
        assert notebook_endpoint.notebook_path == "test_notebook.ipynb"

    def test_get_params(self, mocker, notebook_endpoint):
        """Test getting notebook parameters."""
        mock_params = {"param1": "value1", "param2": "value2"}
        mock_inspect = mocker.patch(
            "papermill.inspect_notebook", return_value=mock_params
        )

        result = notebook_endpoint.get_params()

        assert result == mock_params
        mock_inspect.assert_called_once_with("test_notebook.ipynb")

    def test_print_params(self, mocker, notebook_endpoint):
        """Test printing notebook parameters."""
        mock_params = {"param1": "value1", "param2": "value2"}
        mocker.patch("papermill.inspect_notebook", return_value=mock_params)
        mock_pprint = mocker.patch("cfa.dataops.reporting.catalog.pprint")

        notebook_endpoint.print_params()

        mock_pprint.assert_called_once_with(mock_params)

    def test_nb_to_html_str(self, mocker, notebook_endpoint):
        """Test converting notebook to HTML string."""
        mock_html_content = "<html><body>Test HTML</body></html>"

        # Mock TemporaryDirectory
        mock_temp_dir = mocker.patch(
            "cfa.dataops.reporting.catalog.TemporaryDirectory"
        )
        mock_temp_dir.return_value.__enter__.return_value = "/tmp/test"

        # Mock papermill execution
        mock_execute = mocker.patch("papermill.execute_notebook")

        # Mock nb_to_html function
        mock_nb_to_html = mocker.patch(
            "cfa.dataops.reporting.catalog.nb_to_html",
            return_value=mock_html_content,
        )

        # Mock os.path.join
        mocker.patch("os.path.join", return_value="/tmp/test/output.ipynb")

        result = notebook_endpoint.nb_to_html_str(param1="value1")

        assert result == mock_html_content
        mock_execute.assert_called_once_with(
            input_path="test_notebook.ipynb",
            output_path="/tmp/test/output.ipynb",
            parameters={"param1": "value1"},
            progress_bar=True,
        )
        mock_nb_to_html.assert_called_once_with("/tmp/test/output.ipynb")

    def test_nb_to_html_str_with_retitle(self, mocker, notebook_endpoint):
        """Test converting notebook to HTML string with retitling."""
        mock_html_content = "<html><body>Test HTML</body></html>"

        # Mock TemporaryDirectory
        mock_temp_dir = mocker.patch(
            "cfa.dataops.reporting.catalog.TemporaryDirectory"
        )
        mock_temp_dir.return_value.__enter__.return_value = "/tmp/test"

        # Mock papermill execution
        _ = mocker.patch("papermill.execute_notebook")

        # Mock retitle_notebook
        mock_retitle = mocker.patch(
            "cfa.dataops.reporting.catalog.retitle_notebook"
        )

        # Mock nb_to_html function
        _ = mocker.patch(
            "cfa.dataops.reporting.catalog.nb_to_html",
            return_value=mock_html_content,
        )

        # Mock os.path.join
        mocker.patch("os.path.join", return_value="/tmp/test/output.ipynb")

        result = notebook_endpoint.nb_to_html_str(nb_title="New Title")

        assert result == mock_html_content
        mock_retitle.assert_called_once_with(
            "/tmp/test/output.ipynb", "New Title"
        )

    def test_nb_to_html_file(self, mocker, notebook_endpoint):
        """Test converting notebook to HTML file."""
        mock_html_content = "<html><body>Test HTML</body></html>"

        # Mock the nb_to_html_str method
        mock_nb_to_html_str = mocker.patch.object(
            notebook_endpoint, "nb_to_html_str", return_value=mock_html_content
        )

        # Mock file operations
        mock_makedirs = mocker.patch("os.makedirs")
        _ = mocker.patch("os.path.dirname", return_value="/path/to")
        _ = mocker.patch(
            "os.path.abspath", return_value="/absolute/path/to/output.html"
        )
        mock_file_open = mocker.patch("builtins.open", mocker.mock_open())
        mock_print = mocker.patch("builtins.print")

        # Test the function
        notebook_endpoint.nb_to_html_file(
            "/path/to/output.html", nb_title="Test Title", param1="value1"
        )

        # Verify calls
        mock_nb_to_html_str.assert_called_once_with(
            nb_title="Test Title", param1="value1"
        )
        mock_makedirs.assert_called_once_with("/path/to", exist_ok=True)
        mock_file_open.assert_called_once_with(
            "/path/to/output.html", "w", encoding="utf-8"
        )
        mock_print.assert_called_once_with(
            "HTML report saved to /absolute/path/to/output.html"
        )

    def test_nb_to_html_file_wrong_extension(self, notebook_endpoint):
        """Test nb_to_html_file with wrong file extension."""
        with pytest.raises(AssertionError, match="must end with .html"):
            notebook_endpoint.nb_to_html_file("/path/to/output.txt")

    def test_nb_to_html_blob(self, mocker, notebook_endpoint):
        """Test converting notebook to HTML blob."""
        mock_html_content = "<html><body>Test HTML</body></html>"

        # Mock the nb_to_html_str method
        mock_nb_to_html_str = mocker.patch.object(
            notebook_endpoint, "nb_to_html_str", return_value=mock_html_content
        )

        # Mock write_blob_stream
        mock_write_blob = mocker.patch(
            "cfa.dataops.reporting.catalog.write_blob_stream"
        )

        # Test the function
        notebook_endpoint.nb_to_html_blob(
            blob_account="test_account",
            blob_container="test_container",
            blob_path="reports/output.html",
            nb_title="Test Title",
            param1="value1",
        )

        # Verify calls
        mock_nb_to_html_str.assert_called_once_with(
            nb_title="Test Title", param1="value1"
        )
        mock_write_blob.assert_called_once_with(
            account_name="test_account",
            container_name="test_container",
            blob_url="reports/output.html",
            data=mock_html_content.encode("utf-8"),
        )

    def test_nb_to_html_blob_wrong_extension(self, notebook_endpoint):
        """Test nb_to_html_blob with wrong file extension."""
        with pytest.raises(AssertionError, match="must end with .html"):
            notebook_endpoint.nb_to_html_blob(
                blob_account="test_account",
                blob_container="test_container",
                blob_path="reports/output.txt",
            )


class TestReportDictToSn:
    """Test the report_dict_to_sn function."""

    def test_simple_dict_conversion(self):
        """Test converting a simple dictionary to SimpleNamespace."""
        test_dict = {"key1": "value1", "key2": "value2"}
        result = report_dict_to_sn(test_dict)

        assert isinstance(result, SimpleNamespace)
        assert result.key1 == "value1"
        assert result.key2 == "value2"

    def test_nested_dict_conversion(self):
        """Test converting nested dictionaries to SimpleNamespace."""
        test_dict = {
            "level1": {
                "level2": {"key": "value"},
                "simple_key": "simple_value",
            }
        }
        result = report_dict_to_sn(test_dict)

        assert isinstance(result, SimpleNamespace)
        assert isinstance(result.level1, SimpleNamespace)
        assert isinstance(result.level1.level2, SimpleNamespace)
        assert result.level1.level2.key == "value"
        assert result.level1.simple_key == "simple_value"

    def test_notebook_endpoint_creation(self):
        """Test that .ipynb files create NotebookEndpoint instances."""
        test_dict = {
            "reports": {
                "example_ipynb": "/path/to/notebook.ipynb",
                "regular_key": "regular_value",
            }
        }
        result = report_dict_to_sn(test_dict)

        assert isinstance(result, SimpleNamespace)
        assert isinstance(result.reports, SimpleNamespace)
        assert isinstance(result.reports.example_ipynb, NotebookEndpoint)
        assert (
            result.reports.example_ipynb.notebook_path
            == "/path/to/notebook.ipynb"
        )
        assert result.reports.regular_key == "regular_value"

    def test_list_conversion(self):
        """Test converting lists within dictionaries."""
        # The report_dict_to_sn function only processes dictionaries at the top level
        # Lists are processed recursively only if they contain dictionaries
        test_dict = {
            "list_key": [
                {"nested_key": "nested_value"},
            ]
        }
        result = report_dict_to_sn(test_dict)

        assert isinstance(result, SimpleNamespace)
        assert isinstance(result.list_key, list)
        assert len(result.list_key) == 1
        assert isinstance(result.list_key[0], SimpleNamespace)
        assert result.list_key[0].nested_key == "nested_value"

    def test_mixed_content_conversion(self):
        """Test converting mixed content types."""
        test_dict = {
            "notebook_example_ipynb": "/path/to/example.ipynb",
            "nested_dict": {
                "inner_notebook_ipynb": "/path/to/inner.ipynb",
                "inner_list": [
                    {"dict_in_list": "value"},
                ],
            },
            "simple_value": "test",
            "number_value": 42,
        }
        result = report_dict_to_sn(test_dict)

        # Check notebook endpoint creation
        assert isinstance(result.notebook_example_ipynb, NotebookEndpoint)
        assert (
            result.notebook_example_ipynb.notebook_path
            == "/path/to/example.ipynb"
        )

        # Check nested structure
        assert isinstance(result.nested_dict, SimpleNamespace)
        assert isinstance(
            result.nested_dict.inner_notebook_ipynb, NotebookEndpoint
        )
        assert (
            result.nested_dict.inner_notebook_ipynb.notebook_path
            == "/path/to/inner.ipynb"
        )

        # Check list handling
        assert isinstance(result.nested_dict.inner_list, list)
        assert isinstance(result.nested_dict.inner_list[0], SimpleNamespace)
        assert result.nested_dict.inner_list[0].dict_in_list == "value"

        # Check simple values
        assert result.simple_value == "test"
        assert result.number_value == 42

    def test_empty_dict_conversion(self):
        """Test converting an empty dictionary."""
        result = report_dict_to_sn({})
        assert isinstance(result, SimpleNamespace)
        assert len(vars(result)) == 0


class TestReportingCatalogIntegration:
    """Integration tests for the reporting catalog functionality."""

    def test_report_namespace_creation(self, mocker):
        """Test creating a report namespace similar to dataset catalog."""
        # Mock the glob.glob to return some test notebook paths
        mock_glob = mocker.patch("glob.glob")
        mock_glob.return_value = [
            "/path/to/reporting/reports/examples/basic_ipynb",
            "/path/to/reporting/reports/analysis/advanced_ipynb",
        ]

        # Mock os.path.split and os.path.join
        mocker.patch(
            "os.path.split",
            return_value=("/path/to/reporting", "catalog.py"),
        )
        mocker.patch("os.path.join")

        # Test that we can create a namespace structure
        test_report_map = {
            "examples": {
                "basic_ipynb": "/path/to/basic.ipynb",
                "tutorial_ipynb": "/path/to/tutorial.ipynb",
            },
            "analysis": {
                "advanced_ipynb": "/path/to/advanced.ipynb",
            },
        }

        result = report_dict_to_sn(test_report_map)

        # Verify structure
        assert isinstance(result, SimpleNamespace)
        assert isinstance(result.examples, SimpleNamespace)
        assert isinstance(result.analysis, SimpleNamespace)

        # Verify notebook endpoints
        assert isinstance(result.examples.basic_ipynb, NotebookEndpoint)
        assert isinstance(result.examples.tutorial_ipynb, NotebookEndpoint)
        assert isinstance(result.analysis.advanced_ipynb, NotebookEndpoint)

        # Verify paths
        assert (
            result.examples.basic_ipynb.notebook_path == "/path/to/basic.ipynb"
        )
        assert (
            result.examples.tutorial_ipynb.notebook_path
            == "/path/to/tutorial.ipynb"
        )
        assert (
            result.analysis.advanced_ipynb.notebook_path
            == "/path/to/advanced.ipynb"
        )

    def test_end_to_end_notebook_execution(self, mocker):
        """Test end-to-end notebook execution workflow."""
        # Create a test notebook endpoint
        notebook_endpoint = NotebookEndpoint("/path/to/test.ipynb")

        # Mock all the dependencies
        mock_temp_dir = mocker.patch(
            "cfa.dataops.reporting.catalog.TemporaryDirectory"
        )
        mock_temp_dir.return_value.__enter__.return_value = "/tmp/test"

        mock_execute = mocker.patch("papermill.execute_notebook")
        mock_retitle = mocker.patch(
            "cfa.dataops.reporting.catalog.retitle_notebook"
        )
        mock_nb_to_html = mocker.patch(
            "cfa.dataops.reporting.catalog.nb_to_html",
            return_value="<html>Test Report</html>",
        )
        mock_write_blob = mocker.patch(
            "cfa.dataops.reporting.catalog.write_blob_stream"
        )
        mocker.patch("os.path.join", return_value="/tmp/test/output.ipynb")

        # Execute the workflow
        notebook_endpoint.nb_to_html_blob(
            blob_account="test_account",
            blob_container="reports",
            blob_path="test_report.html",
            nb_title="Test Report",
            param1="test_value",
            param2=42,
        )

        # Verify the workflow
        mock_execute.assert_called_once_with(
            input_path="/path/to/test.ipynb",
            output_path="/tmp/test/output.ipynb",
            parameters={"param1": "test_value", "param2": 42},
            progress_bar=True,
        )
        mock_retitle.assert_called_once_with(
            "/tmp/test/output.ipynb", "Test Report"
        )
        mock_nb_to_html.assert_called_once_with("/tmp/test/output.ipynb")
        mock_write_blob.assert_called_once_with(
            account_name="test_account",
            container_name="reports",
            blob_url="test_report.html",
            data="<html>Test Report</html>".encode("utf-8"),
        )

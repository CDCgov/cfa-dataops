import os
from types import SimpleNamespace

from cfa.dataops.reporting import reportcat
from cfa.dataops.reporting.catalog import NotebookEndpoint, get_report_catalog


def test_catalog(data_dir):
    """Test the report catalog."""
    catalog = get_report_catalog()
    assert catalog.__dict__.keys() == reportcat.__dict__.keys()
    assert isinstance(catalog, SimpleNamespace)

    # Check if a specific report exists
    examples = catalog.examples
    assert isinstance(examples, SimpleNamespace)
    example_report = examples.basics_ipynb
    assert isinstance(example_report, NotebookEndpoint)
    assert example_report.notebook_path.endswith(".ipynb")
    assert hasattr(example_report, "nb_to_html_str")
    assert callable(example_report.nb_to_html_str)
    assert hasattr(example_report, "nb_to_html_file")
    assert callable(example_report.nb_to_html_file)
    assert hasattr(example_report, "nb_to_html_blob")
    assert callable(example_report.nb_to_html_blob)
    assert hasattr(example_report, "notebook_path")
    assert hasattr(example_report, "print_params")
    assert callable(example_report.print_params)

    # Test conversion to HTML string
    html_content = example_report.nb_to_html_str()
    assert isinstance(html_content, str)
    assert (
        "Basic Notebook Example" in html_content
    )  # Basic check for HTML content

    # Test saving to HTML file
    html_out_path = os.path.join(data_dir, "test_output.html")
    example_report.nb_to_html_file(html_out_path, nb_title="Test Report 1234")
    with open(html_out_path, "r") as f:
        saved_content = f.read()
        assert "Basic Notebook Example" in saved_content
        assert "Test Report 1234" in saved_content

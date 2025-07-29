import glob
import os
from pprint import pprint
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import Any

import papermill as pm
from cfa_azure.blob_helpers import write_blob_stream
from nbconvert import HTMLExporter
from traitlets.config import Config

_here_dir = os.path.split(os.path.abspath(__file__))[0]
_report_paths = glob.glob(
    os.path.join(_here_dir, "**", "**.ipynb"), recursive=True
)


def remove_ws_and_nonalpha(s: str) -> str:
    """Remove whitespace and non-alphanumeric characters from a string.

    Args:
        s (str): The input string.

    Returns:
        str: The cleaned string with whitespace and non-alphanumeric characters removed.

    Example:
        >>> remove_ws_and_nonalpha("Hello World! 123.ipynb")
        'hello_world_123_ipynb'
    """
    s = s.replace(" ", "_").replace(".", "_").lower()
    return "".join(c for c in s if c.isalnum() or c == "_")


# get the namespace mapping for the available reports
report_ns_map = {}
for rp_i in _report_paths:
    if rp_i.startswith(os.path.join(_here_dir, "reports")):
        ns_list = [
            remove_ws_and_nonalpha(i)
            for i in rp_i.split(f"reports{os.sep}")[-1].split(os.sep)
        ]
        current = report_ns_map
        for part in ns_list[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[ns_list[-1]] = rp_i


def nb_to_html(nb_path: str) -> str:
    """Convert the executed notebook to HTML format.

    Returns:
        str: The HTML content of the notebook.
    """
    c = Config()
    c.TagRemovePreprocessor.remove_cell_tags = ("remove_cell",)
    c.TagRemovePreprocessor.remove_input_tags = ("remove_input",)
    c.TagRemovePreprocessor.remove_all_outputs_tags = ("remove_outputs",)
    c.TagRemovePreprocessor.enabled = True
    c.HTMLExporter.preprocessors = [
        "nbconvert.preprocessors.TagRemovePreprocessor"
    ]
    body, _ = HTMLExporter(template_name="lab", config=c).from_filename(
        nb_path
    )
    return body


class NotebookEndpoint:
    """Class to handle the execution and conversion of Jupyter notebooks to HTML."""

    def __init__(self, notebook_path: str):
        self.notebook_path = notebook_path

    def get_params(self) -> dict:
        """Get the parameters for the notebook execution."""
        return pm.inspect_notebook(self.notebook_path)

    def print_params(self):
        """Pretty print the parameters of the notebook."""
        pprint(self.get_params())

    def nb_to_html_str(self, **kwargs) -> str:
        """Convert the notebook to HTML format and optionally save it to a file.

        Args:
            html_out_path (str): Path to save the HTML output. If None, does not save.
            **kwargs: Parameters to replace defaults in notebook.

        Returns:
            str: The HTML content of the notebook.
        """
        with TemporaryDirectory() as temp_dir:
            out_file_path = os.path.join(temp_dir, "output.ipynb")
            pm.execute_notebook(
                input_path=self.notebook_path,
                output_path=out_file_path,
                parameters=kwargs,
                progress_bar=True,
            )
            html_content = nb_to_html(out_file_path)

        return html_content

    def nb_to_html_file(self, html_out_path: str, **kwargs) -> None:
        """Convert the notebook to HTML format.

        Args:
            **kwargs: Parameters to replace defaults in notebook.

        Returns:
            None, saves the html content to a file.
        """
        assert html_out_path.endswith(
            ".html"
        ), "Output path must end with .html"
        html_content = self.nb_to_html_str(**kwargs)
        if html_out_path:
            with open(html_out_path, "w") as f:
                f.write(html_content)
        print(f"HTML report saved to {os.path.abspath(html_out_path)}")

    def nb_to_html_blob(
        self, blob_account: str, blob_container: str, blob_path: str, **kwargs
    ) -> None:
        """Convert the notebook to HTML format save to blob storage.

        Args:
            **kwargs: Parameters to replace defaults in notebook.

        Returns:
            None, saves the HTML content to a blob storage.
        """
        assert blob_path.endswith(".html"), "Output path must end with .html"
        html_content = self.nb_to_html_str(**kwargs)
        write_blob_stream(
            account_name=blob_account,
            container_name=blob_container,
            blob_url=blob_path,
            data=html_content.encode("utf-8"),
        )


def report_dict_to_sn(d: Any) -> SimpleNamespace:
    """Simple recursive namespace construction

    Args:
        d (Any): a dict, list or other

    Returns:
        SimpleNamespace: namespace representation
    """
    x = SimpleNamespace()
    _ = [
        setattr(
            x,
            k,
            NotebookEndpoint(notebook_path=v)
            if k.endswith("ipynb")
            else report_dict_to_sn(v)
            if isinstance(v, dict)
            else [report_dict_to_sn(e) for e in v]
            if isinstance(v, list)
            else v,
        )
        for k, v in d.items()
    ]
    return x


reportcat = report_dict_to_sn(report_ns_map)

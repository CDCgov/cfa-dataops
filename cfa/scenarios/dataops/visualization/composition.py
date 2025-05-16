"""Functions used to merge together multiple visualizations
into a single report."""

from os.path import join
from tempfile import TemporaryDirectory
from typing import List

from altair import Chart
from pypdf import PdfWriter


def merge_charts_in_pdf(
    charts: List[Chart], filename: str = "merged_report.pdf"
) -> None:
    """Merge multiple Altair charts into a single PDF report.

    Args:
        charts (List[Chart]): List of Altair charts to be merged.
        filename (str): The name of the output PDF file. Defaults to "merged_report.pdf".
    """

    # Create a temporary directory to store the individual chart PDFs
    page_count = len(charts)
    with TemporaryDirectory() as temp_dir:
        pdf_files = []
        for i, chart in enumerate(charts):
            # Save each chart as a PDF file
            pdf_file = join(
                temp_dir, f"chart_{str(i).zfill(len(str(page_count)))}.pdf"
            )
            chart.save(pdf_file)
            pdf_files.append(pdf_file)

        # Create a PDF writer object
        merger = PdfWriter()

        # Add each chart PDF to the writer
        for pdf_file in pdf_files:
            merger.append(pdf_file)

        merger.write(filename)
        merger.close()

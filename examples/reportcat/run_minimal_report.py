from pathlib import Path

from cfa.dataops.reporting.catalog import NotebookEndpoint


def main() -> None:
    """Generate a minimal HTML report from a parameterized notebook."""
    repo_root = Path(__file__).resolve().parents[2]
    notebook_path = (
        repo_root / "examples" / "reportcat" / "minimal_report.ipynb"
    )
    output_path = repo_root / "out" / "minimal_report.html"

    ep = NotebookEndpoint(str(notebook_path))
    ep.nb_to_html_file(
        html_out_path=str(output_path),
        nb_title="Minimal Report",
        kernel_name="python3",
        x=10,
        y=20,
    )

    print(f"Report generated: {output_path}")


if __name__ == "__main__":
    main()

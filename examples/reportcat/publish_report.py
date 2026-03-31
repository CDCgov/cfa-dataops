import os
from pathlib import Path

from cfa.dataops.reporting.catalog import NotebookEndpoint


def must_get(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def main() -> None:
    """Generate and publish a minimal HTML report to Azure Blob Storage."""
    repo_root = Path(__file__).resolve().parents[2]
    notebook_path = (
        repo_root / "examples" / "reportcat" / "minimal_report.ipynb"
    )

    blob_account = must_get("REPORTCAT_BLOB_ACCOUNT")
    blob_container = must_get("REPORTCAT_BLOB_CONTAINER")
    blob_path = must_get("REPORTCAT_BLOB_PATH")

    ep = NotebookEndpoint(str(notebook_path))
    ep.nb_to_html_blob(
        blob_account=blob_account,
        blob_container=blob_container,
        blob_path=blob_path,
        nb_title="Minimal Report",
        kernel_name="python3",
        x=10,
        y=20,
    )

    print("Report published to Azure Blob Storage.")


if __name__ == "__main__":
    main()

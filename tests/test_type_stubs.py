import ast
from pathlib import Path

import pytest

from cfa.dataops.type_stubs import (
    render_catalog_stub,
    render_init_stub,
    write_catalog_stubs,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _write_dataset_config(path: Path) -> Path:
    path.write_text(
        """
[properties]
name = "example"
type = "etl"
automate = false

[extract]
account = "account"
container = "container"
prefix = "raw/example"

[stage_01]
account = "account"
container = "container"
prefix = "stage/example"

[load]
account = "account"
container = "container"
prefix = "load/example"
""".strip(),
        encoding="utf-8",
    )
    return path


def _representative_catalog_stub(dataset_config: Path) -> str:
    return render_catalog_stub(
        dataset_ns_map={
            "public": {
                "stf": {
                    "nhsn_hrd_prelim": str(dataset_config),
                },
            },
        },
        report_ns_map={
            "public": {
                "examples": {
                    "basic_report": "/reports/basic_report.ipynb",
                },
            },
        },
    )


def test_render_catalog_stub_matches_expected_fixture(tmp_path):
    dataset_config = _write_dataset_config(tmp_path / "example.toml")
    stub = _representative_catalog_stub(dataset_config)
    expected = (FIXTURES_DIR / "expected_catalog_stub.pyi").read_text(
        encoding="utf-8"
    )

    assert stub.strip() == expected.strip()


def test_render_catalog_stub_outputs_parseable_python_syntax(tmp_path):
    dataset_config = _write_dataset_config(tmp_path / "example.toml")
    stub = _representative_catalog_stub(dataset_config)

    ast.parse(stub)


def test_render_catalog_stub_uses_real_dataset_paths_and_stages(tmp_path):
    dataset_config = _write_dataset_config(tmp_path / "example.toml")

    stub = render_catalog_stub(
        dataset_ns_map={
            "public": {
                "stf": {
                    "nhsn_hrd_prelim": str(dataset_config),
                },
            },
        },
        report_ns_map={},
    )

    assert "class _DataCatalogPublicStfNhsnHrdPrelimDataset(DatasetEndpoint):" in stub
    assert "nhsn_hrd_prelim: _DataCatalogPublicStfNhsnHrdPrelimDataset" in stub
    assert "stage_01: BlobEndpoint" in stub
    assert 'output: Literal["pl_lazy", "lazy"],' in stub
    assert ") -> pl.LazyFrame: ..." in stub

    public_namespace = stub.split(
        "class _DataCatalogPublicNamespace(CatalogNamespace):"
    )[1].split("class DataCatalog(CatalogNamespace):")[0]
    assert "stf: _DataCatalogPublicStfNamespace" in public_namespace
    assert "load: BlobEndpoint" not in public_namespace


def test_render_catalog_stub_uses_real_report_paths():
    stub = render_catalog_stub(
        dataset_ns_map={},
        report_ns_map={
            "public": {
                "examples": {
                    "basic_report": "/reports/basic_report.ipynb",
                },
            },
        },
    )

    assert "class _ReportCatalogPublicExamplesBasicReportReport(NotebookEndpoint):" in stub
    assert "basic_report: _ReportCatalogPublicExamplesBasicReportReport" in stub


def test_render_init_stub_outputs_parseable_python_syntax():
    ast.parse(render_init_stub())


def test_render_catalog_stub_rejects_non_identifier_segments(tmp_path):
    dataset_config = _write_dataset_config(tmp_path / "example.toml")

    with pytest.raises(ValueError, match="not a valid Python identifier"):
        render_catalog_stub(
            dataset_ns_map={"public-data": {"example": str(dataset_config)}},
            report_ns_map={},
        )


def test_write_catalog_stubs_creates_project_local_stub_tree(tmp_path):
    dataset_config = _write_dataset_config(tmp_path / "example.toml")
    output_root = tmp_path / "typings"

    written = write_catalog_stubs(
        output_root,
        dataset_ns_map={"public": {"example": str(dataset_config)}},
        report_ns_map={},
    )

    assert written == [
        output_root / "cfa" / "dataops" / "catalog.pyi",
        output_root / "cfa" / "dataops" / "__init__.pyi",
    ]
    assert written[0].exists()
    assert written[1].exists()
    assert "datacat: DataCatalog" in written[0].read_text(encoding="utf-8")
    assert "from .catalog import datacat as datacat" in written[1].read_text(
        encoding="utf-8"
    )

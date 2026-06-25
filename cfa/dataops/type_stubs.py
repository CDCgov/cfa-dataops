"""Generate static type stubs for the installed dataops catalog."""

import argparse
import keyword
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import tomli

DATASET_STAGE_PREFIX = "stage"
DEFAULT_STUB_ROOT = Path("typings")


@dataclass
class _Node:
    children: dict[str, "_Node"] = field(default_factory=dict)
    dataset_config_path: str | None = None
    report_path: str | None = None


def _ensure_identifier(name: str, namespace: tuple[str, ...]) -> str:
    if not name.isidentifier() or keyword.iskeyword(name):
        dotted = ".".join((*namespace, name))
        raise ValueError(
            f"Catalog path segment {dotted!r} is not a valid Python identifier."
        )
    return name


def _class_fragment(name: str) -> str:
    return "".join(part.capitalize() for part in name.split("_") if part)


def _class_name(prefix: str, path: tuple[str, ...], suffix: str) -> str:
    fragments = "".join(_class_fragment(part) for part in path)
    return f"_{prefix}{fragments}{suffix}"


def _insert_node(
    root: _Node,
    path: tuple[str, ...],
    *,
    dataset_config_path: str | None = None,
    report_path: str | None = None,
) -> None:
    node = root
    for segment in path:
        node = node.children.setdefault(segment, _Node())
    node.dataset_config_path = dataset_config_path
    node.report_path = report_path


def _walk_dataset_map(
    mapping: Mapping[str, Any],
    namespace: tuple[str, ...] = (),
) -> list[tuple[tuple[str, ...], str]]:
    datasets: list[tuple[tuple[str, ...], str]] = []
    for key, value in mapping.items():
        segment = _ensure_identifier(str(key), namespace)
        path = (*namespace, segment)
        if isinstance(value, str) and value.endswith(".toml"):
            datasets.append((path, value))
        elif isinstance(value, Mapping):
            datasets.extend(_walk_dataset_map(value, path))
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, Mapping):
                    datasets.extend(_walk_dataset_map(item, path))
    return datasets


def _walk_report_map(
    mapping: Mapping[str, Any],
    namespace: tuple[str, ...] = (),
) -> list[tuple[tuple[str, ...], str]]:
    reports: list[tuple[tuple[str, ...], str]] = []
    for key, value in mapping.items():
        segment = _ensure_identifier(str(key), namespace)
        path = (*namespace, segment)
        if isinstance(value, str) and value.endswith(".ipynb"):
            reports.append((path, value))
        elif isinstance(value, Mapping):
            reports.extend(_walk_report_map(value, path))
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, Mapping):
                    reports.extend(_walk_report_map(item, path))
    return reports


def _dataset_stage_names(config_path: str) -> list[str]:
    with open(config_path, "rb") as f:
        config = tomli.load(f)
    return [
        key
        for key in config
        if key in {"load", "extract", "data"} or key.startswith(DATASET_STAGE_PREFIX)
    ]


def _indent(lines: list[str], level: int = 1) -> list[str]:
    prefix = "    " * level
    return [f"{prefix}{line}" if line else "" for line in lines]


def _empty_body(lines: list[str]) -> list[str]:
    return lines if lines else ["pass"]


def _emit_dataset_class(
    lines: list[str],
    class_name: str,
    config_path: str,
) -> None:
    stages = _dataset_stage_names(config_path)
    body = ["config: dict[str, Any]"]
    body.extend(f"{stage}: BlobEndpoint" for stage in stages)
    lines.append(f"class {class_name}(DatasetEndpoint):")
    lines.extend(_indent(_empty_body(body)))
    lines.append("")


def _emit_report_class(lines: list[str], class_name: str) -> None:
    lines.append(f"class {class_name}(NotebookEndpoint):")
    lines.extend(_indent(["pass"]))
    lines.append("")


def _emit_namespace_class(
    lines: list[str],
    node: _Node,
    *,
    prefix: str,
    path: tuple[str, ...],
    root_name: str,
    is_dataset_catalog: bool,
) -> str:
    child_attrs: list[tuple[str, str]] = []
    for name, child in sorted(node.children.items()):
        child_path = (*path, name)
        if child.dataset_config_path is not None:
            child_class = _class_name(prefix, child_path, "Dataset")
            _emit_dataset_class(lines, child_class, child.dataset_config_path)
        elif child.report_path is not None:
            child_class = _class_name(prefix, child_path, "Report")
            _emit_report_class(lines, child_class)
        else:
            child_class = _emit_namespace_class(
                lines,
                child,
                prefix=prefix,
                path=child_path,
                root_name=root_name,
                is_dataset_catalog=is_dataset_catalog,
            )
        child_attrs.append((name, child_class))

    class_name = root_name if not path else _class_name(prefix, path, "Namespace")
    body = [f"{name}: {child_class}" for name, child_class in child_attrs]
    if not path:
        body.append("__namespace_list__: list[str]")
    elif is_dataset_catalog and len(path) == 1:
        body.append("_ledger_endpoint: BlobEndpoint")

    lines.append(f"class {class_name}(CatalogNamespace):")
    lines.extend(_indent(_empty_body(body)))
    lines.append("")
    return class_name


def _build_dataset_tree(dataset_ns_map: Mapping[str, Any]) -> _Node:
    root = _Node()
    for path, config_path in _walk_dataset_map(dataset_ns_map):
        _insert_node(root, path, dataset_config_path=config_path)
    return root


def _build_report_tree(report_ns_map: Mapping[str, Any]) -> _Node:
    root = _Node()
    for path, report_path in _walk_report_map(report_ns_map):
        _insert_node(root, path, report_path=report_path)
    return root


def render_catalog_stub(
    dataset_ns_map: Mapping[str, Any],
    report_ns_map: Mapping[str, Any],
) -> str:
    """Render a precise ``cfa.dataops.catalog`` stub for installed catalogs."""

    lines = [
        "from collections.abc import Sequence",
        "from types import SimpleNamespace",
        "from typing import Any, Literal, overload",
        "",
        "import pandas as pd",
        "import polars as pl",
        "",
        "from .reporting.catalog import NotebookEndpoint",
        "",
        "",
        "def get_all_catalogs() -> list[tuple[str, str, str]]: ...",
        "",
        "",
        "class CatalogNamespace(SimpleNamespace):",
        "    pass",
        "",
        "",
        "class DatasetEndpoint:",
        "    config_path: str",
        "    defaults: dict[str, Any]",
        "    config: dict[str, Any]",
        "    __ns_str__: str",
        "    _ledger_location: dict[str, Any]",
        "",
        "",
        "class BlobEndpoint:",
        "    account: str",
        "    container: str",
        "    prefix: str",
        "    ledger_location: dict[str, Any]",
        "    is_ledger: bool",
        "    __ns_str__: str",
        "    def write_blob(",
        "        self,",
        "        file_buffer: bytes | Sequence[bytes],",
        "        path_after_prefix: str,",
        "        auto_version: bool = False,",
        "        append: bool = False,",
        "    ) -> None: ...",
        "    def read_blobs(",
        "        self,",
        "        version_spec: str | None = None,",
        '        selection: Literal["newest", "oldest", "all"] = "newest",',
        "        print_version: bool = True,",
        "    ) -> list[bytes]: ...",
        "    def read_csv(self, suffix: str) -> pd.DataFrame: ...",
        "    def get_versions(self) -> list[str]: ...",
        "    def get_file_ext(",
        "        self,",
        "        version_spec: str | None = None,",
        '        selection: Literal["newest", "oldest", "all"] = "newest",',
        "    ) -> str: ...",
        "    def download_version_to_local(",
        "        self,",
        "        local_path: str,",
        "        version_spec: str | None = None,",
        "        force: bool = False,",
        '        selection: Literal["newest", "oldest", "all"] = "newest",',
        "    ) -> bool: ...",
        "    @overload",
        "    def get_dataframe(",
        "        self,",
        '        output: Literal["pandas", "pd"] = "pandas",',
        "        version_spec: str | None = None,",
        '        selection: Literal["newest", "oldest"] = "newest",',
        "    ) -> pd.DataFrame: ...",
        "    @overload",
        "    def get_dataframe(",
        "        self,",
        '        output: Literal["polars", "pl"],',
        "        version_spec: str | None = None,",
        '        selection: Literal["newest", "oldest"] = "newest",',
        "    ) -> pl.DataFrame: ...",
        "    @overload",
        "    def get_dataframe(",
        "        self,",
        '        output: Literal["pl_lazy", "lazy"],',
        "        version_spec: str | None = None,",
        '        selection: Literal["newest", "oldest"] = "newest",',
        "    ) -> pl.LazyFrame: ...",
        "    def ledger_entry(self, action: str) -> None: ...",
        "    def save_dataframe(",
        "        self,",
        "        df: pd.DataFrame | pl.DataFrame,",
        "        path_after_prefix: str,",
        '        file_format: str = "parquet",',
        "        auto_version: bool = False,",
        "    ) -> None: ...",
        "    def save_file_to_blob(",
        "        self,",
        "        file_path: str,",
        "        path_after_prefix: str,",
        "        auto_version: bool = False,",
        "    ) -> None: ...",
        "    def save_dir_to_blob(",
        "        self,",
        "        dir_path: str,",
        "        path_after_prefix: str,",
        "        auto_version: bool = False,",
        "    ) -> None: ...",
        "",
        "",
        "def dict_to_sn(",
        "    d: Any,",
        "    defaults: dict[str, Any] | None = None,",
        '    ns: str = "",',
        ") -> CatalogNamespace: ...",
        "",
        "",
    ]

    _emit_namespace_class(
        lines,
        _build_dataset_tree(dataset_ns_map),
        prefix="DataCatalog",
        path=(),
        root_name="DataCatalog",
        is_dataset_catalog=True,
    )
    _emit_namespace_class(
        lines,
        _build_report_tree(report_ns_map),
        prefix="ReportCatalog",
        path=(),
        root_name="ReportCatalog",
        is_dataset_catalog=False,
    )
    lines.extend(
        [
            "datacat: DataCatalog",
            "reportcat: ReportCatalog",
            "",
        ]
    )
    return "\n".join(lines)


def render_init_stub() -> str:
    return "\n".join(
        [
            "from .catalog import datacat as datacat, reportcat as reportcat",
            "",
            "__version__: str",
            '__all__: list[str]',
            "",
        ]
    )


def write_catalog_stubs(
    output_root: str | Path = DEFAULT_STUB_ROOT,
    *,
    dataset_ns_map: Mapping[str, Any],
    report_ns_map: Mapping[str, Any],
) -> list[Path]:
    output_root = Path(output_root)
    package_root = output_root / "cfa" / "dataops"
    package_root.mkdir(parents=True, exist_ok=True)

    catalog_stub = package_root / "catalog.pyi"
    init_stub = package_root / "__init__.pyi"
    catalog_stub.write_text(
        render_catalog_stub(dataset_ns_map, report_ns_map),
        encoding="utf-8",
    )
    init_stub.write_text(render_init_stub(), encoding="utf-8")
    return [catalog_stub, init_stub]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate project-local type stubs for the installed dataops catalog."
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_STUB_ROOT,
        help="Root directory for generated stubs. Defaults to ./typings.",
    )
    args = parser.parse_args()

    from .catalog import all_dataset_ns_map, all_reports_ns_map

    written = write_catalog_stubs(
        args.output_root,
        dataset_ns_map=all_dataset_ns_map,
        report_ns_map=all_reports_ns_map,
    )
    for path in written:
        print(path)


if __name__ == "__main__":
    main()

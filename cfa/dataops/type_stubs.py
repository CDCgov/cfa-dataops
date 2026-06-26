"""Generate static type stubs for the installed dataops catalog."""

import argparse
import keyword
from collections.abc import Mapping
from dataclasses import dataclass, field
from importlib.resources import files
from pathlib import Path
from typing import Any

import tomli
from mako.template import Template

DATASET_STAGE_PREFIX = "stage"
DEFAULT_STUB_ROOT = Path("typings")
TEMPLATE_PACKAGE = "cfa.dataops.stub_templates"


@dataclass
class _Node:
    children: dict[str, "_Node"] = field(default_factory=dict)
    dataset_config_path: str | None = None
    report_path: str | None = None


@dataclass(frozen=True)
class _StubAttribute:
    name: str
    type_name: str


@dataclass(frozen=True)
class _StubClass:
    name: str
    base: str
    attributes: tuple[_StubAttribute, ...] = ()


@dataclass(frozen=True)
class _StubModel:
    classes: tuple[_StubClass, ...]


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


def _dataset_class(class_name: str, config_path: str) -> _StubClass:
    attributes = [_StubAttribute("config", "dict[str, Any]")]
    attributes.extend(
        _StubAttribute(stage, "BlobEndpoint")
        for stage in _dataset_stage_names(config_path)
    )
    return _StubClass(
        name=class_name,
        base="DatasetEndpoint",
        attributes=tuple(attributes),
    )


def _report_class(class_name: str) -> _StubClass:
    return _StubClass(name=class_name, base="NotebookEndpoint")


def _namespace_classes(
    node: _Node,
    *,
    prefix: str,
    path: tuple[str, ...],
    root_name: str,
    is_dataset_catalog: bool,
) -> tuple[list[_StubClass], str]:
    classes: list[_StubClass] = []
    child_attrs: list[tuple[str, str]] = []
    for name, child in sorted(node.children.items()):
        child_path = (*path, name)
        if child.dataset_config_path is not None:
            child_class = _class_name(prefix, child_path, "Dataset")
            classes.append(_dataset_class(child_class, child.dataset_config_path))
        elif child.report_path is not None:
            child_class = _class_name(prefix, child_path, "Report")
            classes.append(_report_class(child_class))
        else:
            child_classes, child_class = _namespace_classes(
                child,
                prefix=prefix,
                path=child_path,
                root_name=root_name,
                is_dataset_catalog=is_dataset_catalog,
            )
            classes.extend(child_classes)
        child_attrs.append((name, child_class))

    class_name = root_name if not path else _class_name(prefix, path, "Namespace")
    attributes = [
        _StubAttribute(name, child_class) for name, child_class in child_attrs
    ]
    if not path:
        attributes.append(_StubAttribute("__namespace_list__", "list[str]"))
    elif is_dataset_catalog and len(path) == 1:
        attributes.append(_StubAttribute("_ledger_endpoint", "BlobEndpoint"))

    classes.append(
        _StubClass(
            name=class_name,
            base="CatalogNamespace",
            attributes=tuple(attributes),
        )
    )
    return classes, class_name


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


def _build_stub_model(
    dataset_ns_map: Mapping[str, Any],
    report_ns_map: Mapping[str, Any],
) -> _StubModel:
    dataset_classes, _ = _namespace_classes(
        _build_dataset_tree(dataset_ns_map),
        prefix="DataCatalog",
        path=(),
        root_name="DataCatalog",
        is_dataset_catalog=True,
    )
    report_classes, _ = _namespace_classes(
        _build_report_tree(report_ns_map),
        prefix="ReportCatalog",
        path=(),
        root_name="ReportCatalog",
        is_dataset_catalog=False,
    )
    return _StubModel(classes=tuple([*dataset_classes, *report_classes]))


def _render_template(template_name: str, **context: Any) -> str:
    template = (
        files(TEMPLATE_PACKAGE).joinpath(template_name).read_text(encoding="utf-8")
    )
    return Template(template).render(**context)


def render_catalog_stub(
    dataset_ns_map: Mapping[str, Any],
    report_ns_map: Mapping[str, Any],
) -> str:
    """Render a precise ``cfa.dataops.catalog`` stub for installed catalogs."""

    return _render_template(
        "catalog.pyi.mako",
        model=_build_stub_model(dataset_ns_map, report_ns_map),
    )


def render_init_stub() -> str:
    return _render_template("init.pyi.mako")


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

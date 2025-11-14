"""
CLI tools for managing data operations in the CFA project.
"""

import os
from argparse import ArgumentParser

from rich.console import Console

from .catalog import datacat
from .utils import tree


def _get_dataset_namespaces() -> list[str]:
    """
    Helper function to get a list of dataset namespaces.
    """
    return datacat.__namespace_list__


def _get_stages_list(dataset_namespace: str) -> list[str]:
    """
    Helper function to get a list of stages for a given dataset namespace.
    """
    stages_start_with = ["load", "extract", "stage"]
    if dataset_namespace not in _get_dataset_namespaces():
        Console().print(
            f"[bold red]Error:[/bold red] Dataset namespace '{dataset_namespace}' not found. Available datasets are:\n"
            + "\n".join(f"- {ds}" for ds in _get_dataset_namespaces())
        )
        return
    dataset_dict = eval(f"datacat.{dataset_namespace}.__dict__")
    stages = [
        key
        for key in dataset_dict.keys()
        if any(key.startswith(prefix) for prefix in stages_start_with)
    ]
    return sorted(stages)


def _get_versions_list(dataset_namespace: str, stage: str) -> list[str]:
    """
    Helper function to get a list of versions for a given dataset namespace and stage.
    """
    stages = _get_stages_list(dataset_namespace)
    if stage not in stages:
        Console().print(
            f"[bold red]Error:[/bold red] Stage '{stage}' not found for dataset '{dataset_namespace}'. Available stages are:\n"
            + "\n".join(f"- {s}" for s in stages)
        )
        return
    versions = eval(f"datacat.{dataset_namespace}.{stage}.get_versions()")
    return versions


def get_available_data():
    """
    Retrieve a list of available datasets for CFA.
    """
    parser = ArgumentParser(description="Get list of available datasets")
    parser.add_argument(
        "-p", "--prefix", help="optional prefix filter", default=None
    )
    args = parser.parse_args()
    datasets = datacat.__namespace_list__
    if args.prefix:
        datasets = [ds for ds in datasets if ds.startswith(args.prefix)]
    formatted_list = "\n".join(f"- {dataset}" for dataset in sorted(datasets))
    Console().print(f"[bold]Available Datasets:[/bold]\n{formatted_list}")


def get_dataset_stages():
    """
    Retrieve stages of available datasets for CFA.
    """
    parser = ArgumentParser(description="Get dataset stages")
    parser.add_argument("dataset", help="full dataset namespace")
    args = parser.parse_args()
    dataset = args.dataset
    stages = _get_stages_list(dataset)
    formatted_stages = "\n".join(
        f"- [red]{stage}[/red]" if stage == stages[-1] else f"- {stage}"
        for stage in stages
    )
    disclaimer = "[italic yellow]Note: Stages in [red]red[/red] indicate the default stage for loading the dataset.[/italic yellow]"
    Console().print(
        f"[bold]Stages for {dataset}:[/bold]\n{formatted_stages}\n{disclaimer}"
    )


def get_dataset_versions():
    """
    Retrieve versions of available datasets for CFA.
    """
    parser = ArgumentParser(description="Get dataset versions")
    parser.add_argument("dataset", help="full dataset namespace")
    parser.add_argument(
        "--stage", "-s", help="specific stage to get version for", default=None
    )
    args = parser.parse_args()
    dataset = args.dataset
    available_stages = _get_stages_list(dataset)
    if args.stage is None:
        stage = available_stages[-1]
    else:
        stage = args.stage
    versions = _get_versions_list(dataset, stage)
    formatted_versions = "\n".join(
        f"- [red]{version}[/red]" if version == versions[0] else f"- {version}"
        for version in versions
    )
    Console().print(f"[bold]{dataset}[/bold]:\n{formatted_versions}")


def save_data_locally():
    """
    Save a datasets to the local cache.
    """
    parser = ArgumentParser(description="Download a dataset locally")
    parser.add_argument("dataset", help="full dataset namespace")
    parser.add_argument(
        "location",
        help="local location (directory) to save dataset to. If it does not exist, it will be created.",
    )
    parser.add_argument(
        "--stage", "-s", help="specific stage to get version for", default=None
    )
    parser.add_argument(
        "--version", "-v", help="specific version to get", default=None
    )
    parser.add_argument(
        "--force", "-f", help="force re-download of data", action="store_true"
    )
    parser.add_argument(
        "--oldest",
        "-o",
        help="download the oldest version of data instead of the newest",
        action="store_true",
    )
    parser.add_argument(
        "--full_range",
        "-r",
        help="download the full range of data versions that meet the version criteria",
        action="store_true",
    )
    args = parser.parse_args()
    dataset = args.dataset
    stage = args.stage
    version = args.version
    if stage is None:
        stages = _get_stages_list(dataset)
        stage = stages[-1]
    if version is None:
        versions = _get_versions_list(dataset, stage)
        version = versions[0]
    local_path = os.path.abspath(args.location)
    if args.oldest:
        newest = False
    elif args.full_range:
        newest = None
    else:
        newest = True
    written = eval(
        f"datacat.{dataset}.{stage}.download_version_to_local('{local_path}', version='{version}', force={args.force}, newest={newest})"
    )
    if not written:
        Console().print(
            f"[bold yellow]Dataset '{dataset}' version '{version}' at stage '{stage}' is already present at location '{local_path}'. Use --force to re-download.[/bold yellow]"
        )
        return
    else:
        tree_output = tree(local_path, show_hidden=False)
        Console().print(
            f"[bold green]Dataset '{dataset}' version '{version}' at stage '{stage}' has been saved locally.[/bold green]\n\n{local_path}\n{tree_output}"
        )

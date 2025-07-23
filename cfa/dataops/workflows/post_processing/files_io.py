import json
from fnmatch import fnmatch
from typing import Optional

from cfa_azure.blob_helpers import read_blob_stream, walk_blobs_in_container


def get_blob_tree(
    start_blob_path: str,
    account_name: str,
    container_name: str,
    glob_path: Optional[str] = None,
) -> list:
    """
    Get a list of all blobs in a container, starting from a specific path.

    Args:
        start_blob_path (str): The path to start searching from.
        account_name (str): The name of the Azure storage account.
        container_name (str): The name of the container to search in.
        glob_path (str, optional): A glob pattern to filter the blobs. Defaults to None.

    Returns:
        list: A sorted list of blob paths.
    """
    blob_paths = []
    for i in walk_blobs_in_container(
        name_starts_with=start_blob_path,
        account_name=account_name,
        container_name=container_name,
    ):
        bp = i["name"]
        if bp.endswith("/"):
            blob_paths += get_blob_tree(
                start_blob_path=bp,
                account_name=account_name,
                container_name=container_name,
            )
        else:
            blob_paths.append(bp)
    if glob_path:
        blob_paths = [bp for bp in blob_paths if fnmatch(bp, glob_path)]
    return sorted(blob_paths)


def read_blob_json(
    blob_path: str,
    account_name: str,
    container_name: str,
) -> dict:
    """
    Read a JSON blob from Azure Blob Storage.

    Args:
        blob_path (str): The path to the blob.
        account_name (str): The name of the Azure storage account.
        container_name (str): The name of the container.

    Returns:
        dict: The content of the blob as a dictionary.
    """
    blob = read_blob_stream(
        blob_url=blob_path,
        account_name=account_name,
        container_name=container_name,
    )
    return json.load(blob)

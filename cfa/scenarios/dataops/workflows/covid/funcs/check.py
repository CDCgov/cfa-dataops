from datetime import datetime

from cfa.scenarios.dataops.datasets.catalog import BlobEndpoint


def check_blob_date_exists(blob_ep: BlobEndpoint) -> bool:
    """
    check_blob_date_exists
    A check to determine if the data for today's date exists in blob storage

    Args:
        blob_ep (BlobEndpoint): an instance of the BlobEndpoint class

    Returns:
        bool: True if data exists for today, False otherwise
    """
    today = datetime.today().strftime("%Y-%m-%d")
    versions = blob_ep.get_versions()
    if today in versions:
        return True
    else:
        print("No data generated today in blob storage.")
        return False

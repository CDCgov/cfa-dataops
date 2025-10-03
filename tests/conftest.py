import os

from pytest import fixture

_here = os.path.abspath(os.path.dirname(__file__))
test_datasets_dir = os.path.join(_here, "test_datasets")


@fixture(scope="session")
def data_dir(tmpdir_factory):
    """Fixture to create a temporary data directory for tests."""
    return tmpdir_factory.mktemp("data")


@fixture(scope="session")
def mock_write_blob_stream():
    def mock_write_blob_stream(
        data,
        blob_url: str,
        account_name: str,
        container_name: str,
        append_blob: bool = False,
    ) -> None:
        return None

    return mock_write_blob_stream


@fixture(scope="session")
def dataset_ns_map():
    dataset_ns_map = {
        "tests": {
            "multistage": {
                "multistage_test": os.path.join(
                    test_datasets_dir, "multistage_test.toml"
                )
            },
            "etl_test": os.path.join(test_datasets_dir, "etl_test.toml"),
            "experiment_test": os.path.join(
                test_datasets_dir, "experiment_test.toml"
            ),
            "reference_test": os.path.join(
                test_datasets_dir, "reference_test.toml"
            ),
        }
    }
    return dataset_ns_map


@fixture(scope="session")
def dataset_defaults():
    return {
        "storage": {"account": "account_test", "container": "container_test"},
        "access_ledger": {"path": "_access/test/ledger/"},
    }


@fixture(scope="session")
def simple_dataset_ns_map():
    dataset_ns_map = {
        "space": {
            "example": os.path.join(test_datasets_dir, "etl_test.toml"),
        }
    }
    return dataset_ns_map


@fixture(scope="session")
def catalog_parent(tmpdir_factory):
    """Fixture to create a temporary data directory for tests."""
    return tmpdir_factory.mktemp("cat_parent")

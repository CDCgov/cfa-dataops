from pytest import fixture


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
    ) -> None:
        return None

    return mock_write_blob_stream

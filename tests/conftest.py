from pytest import fixture


@fixture(scope="session")
def data_dir(tmpdir_factory):
    """Fixture to create a temporary data directory for tests."""
    return tmpdir_factory.mktemp("data")

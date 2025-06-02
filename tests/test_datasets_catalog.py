from cfa.scenarios.dataops.datasets.catalog import BlobEndpoint, dict_to_sn


def test_dict_to_sn():
    my_dict = {
        "name": "example",
        "version": "1.0",
        "extract": {
            "account": "my_account",
            "container": "my_container",
            "prefix": "my_prefix",
        },
        "load": {
            "account": "load_account1",
            "container": "load_container1",
            "prefix": "load_prefix1",
        },
    }
    result = dict_to_sn(my_dict)
    assert result.name == "example"
    assert result.version == "1.0"
    assert result.extract.account == "my_account"
    assert result.extract.container == "my_container"
    assert result.extract.prefix == "my_prefix"
    assert isinstance(result.load, BlobEndpoint)
    assert isinstance(result.extract, BlobEndpoint)

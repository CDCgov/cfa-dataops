from cfa.scenarios.dataops.datasets.catalog import (
    BlobEndpoint,
    dict_to_sn,
    list_datasets,
)


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


def test_list_datasets():
    datasets = [
        "covid19hospitalizations",
        "covid19vax_trends",
        "donor_seroprevalence_2020",
        "donor_seroprevalence_2022",
        "fips_to_name_improved",
        "fips_to_name",
        "hospitalization",
        "sars_cov2_proportions",
        "seroprevalence",
        "omicron_variant_regions",
        "seroprevalence_50states",
        "us_age_distribution",
    ]
    actual_retrieved = list_datasets()
    assert set(datasets) == set(actual_retrieved)


def test_blob_endpoint():
    test_be = BlobEndpoint(
        account="test_account",
        container="test_container",
        prefix="test_prefix",
    )
    assert test_be.account == "test_account"
    assert test_be.container == "test_container"
    assert test_be.prefix == "test_prefix"

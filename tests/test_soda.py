from cfa.scenarios.dataops.soda import Query


def test_build_url():
    """
    Test the _build_url method of the Query class.
    """
    domain = "data.cdc.gov"
    id = "abc123"
    expected_url = "https://data.cdc.gov/resource/abc123.json"

    assert Query(domain=domain, id=id).url == expected_url


def test_build_payload_select_string():
    select = "field1"
    expected_payload = {"$select": "field1", "$offset": 0}

    assert Query._build_payload(select=select) == expected_payload


def test_build_payload_select_list():
    select = ["field1", "field2"]
    expected_payload = {"$select": "field1,field2", "$offset": 0}

    assert Query._build_payload(select=select) == expected_payload

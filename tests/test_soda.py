from unittest.mock import MagicMock

import pytest

from cfa.dataops.soda import Query


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


def test_build_payload_with_where_limit_and_offset():
    expected_payload = {
        "$select": "field1",
        "$where": "field1 > 0",
        "$limit": 5,
        "$offset": 3,
    }

    assert (
        Query._build_payload(select="field1", where="field1 > 0", limit=5, offset=3)
        == expected_payload
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"limit": 0},
        {"limit": "5"},
        {"offset": -1},
    ],
)
def test_build_payload_rejects_invalid_limit_and_offset(kwargs):
    with pytest.raises(AssertionError):
        Query._build_payload(**kwargs)


def test_n_rows_applies_offset_and_limit(mocker):
    get_request = mocker.patch.object(
        Query,
        "_get_request",
        return_value=[{"count_id": "10"}],
    )

    result = Query(domain="data.cdc.gov", id="abc123", offset=2, limit=5).n_rows

    assert result == 5
    get_request.assert_called_once()


def test_n_rows_warns_for_empty_dataset(mocker):
    mocker.patch.object(Query, "_get_request", return_value=[{"count_id": "0"}])

    with pytest.warns(UserWarning, match="has no rows"):
        result = Query(domain="data.cdc.gov", id="abc123").n_rows

    assert result == 0


def test_n_rows_warns_when_offset_exceeds_dataset(mocker):
    mocker.patch.object(Query, "_get_request", return_value=[{"count_id": "3"}])

    with pytest.warns(UserWarning, match="Offset 5 is larger"):
        result = Query(domain="data.cdc.gov", id="abc123", offset=5).n_rows

    assert result == 0


def test_get_all_builds_payload_and_returns_records(mocker):
    get_request = mocker.patch.object(
        Query,
        "_get_request",
        return_value=[{"id": 1}, {"id": 2}],
    )

    result = Query(
        domain="data.cdc.gov",
        id="abc123",
        select=["id"],
        where="id > 0",
        limit=2,
        offset=1,
        verbose=False,
    ).get_all()

    assert result == [{"id": 1}, {"id": 2}]
    assert get_request.call_count == 1
    request_kwargs = get_request.call_args.kwargs
    assert request_kwargs["params"] == {
        "$select": "id",
        "$where": "id > 0",
        "$limit": 2,
        "$offset": 1,
    }


def test_get_pages_yields_expected_page_ranges(mocker):
    get_records = mocker.patch.object(
        Query,
        "_get_records",
        side_effect=[[{"id": 1}, {"id": 2}], [{"id": 3}]],
    )
    mocker.patch.object(
        Query, "n_rows", new_callable=mocker.PropertyMock, return_value=3
    )

    result = list(
        Query(domain="data.cdc.gov", id="abc123", verbose=False).get_pages(page_size=2)
    )

    assert result == [[{"id": 1}, {"id": 2}], [{"id": 3}]]
    assert get_records.call_args_list == [
        mocker.call(start=0, end=1),
        mocker.call(start=2, end=3),
    ]


def test_get_records_uses_offset_and_limit(mocker):
    get_request = mocker.patch.object(Query, "_get_request", return_value=[{"id": 3}])
    query = Query(
        domain="data.cdc.gov",
        id="abc123",
        select="id",
        where="id > 0",
        offset=2,
        verbose=False,
    )

    result = query._get_records(start=1, end=1)

    assert result == [{"id": 3}]
    get_request.assert_called_once_with(
        query.url,
        params={"$select": "id", "$where": "id > 0", "$limit": 1, "$offset": 3},
        app_token=None,
    )


def test_get_request_adds_app_token_header(mocker):
    response = MagicMock()
    response.json.return_value = [{"id": 1}]
    client = MagicMock()
    client.get.return_value = response
    client_context = MagicMock()
    client_context.__enter__.return_value = client
    client_context.__exit__.return_value = None
    mocker.patch("cfa.dataops.soda.httpx.Client", return_value=client_context)

    result = Query._get_request(
        "https://data.cdc.gov/resource/abc123.json",
        params={"$limit": 1},
        app_token="token",
    )

    assert result == [{"id": 1}]
    client.get.assert_called_once_with(
        "https://data.cdc.gov/resource/abc123.json",
        headers={"X-App-Token": "token"},
        params={"$limit": 1},
    )
    response.raise_for_status.assert_called_once_with()

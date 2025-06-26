from unittest import mock
from unittest.mock import MagicMock, patch

import pandas as pd

from cfa.scenarios.dataops.workflows.covid.funcs.check import (
    check_blob_date_exists,
)
from cfa.scenarios.dataops.workflows.covid.funcs.formatting_data import (
    format_vac_data,
    output_generation,
)
from cfa.scenarios.dataops.workflows.covid.funcs.import_data import (
    import_hospitalization_data,
    import_population_data,
    import_sero_data,
    import_vaccination_data,
    import_variant_data,
)
from cfa.scenarios.dataops.workflows.covid.funcs.process_data import (
    cases_by_variant,
    individual_infection_totals,
    total_cases_using_hospital_and_sero,
)


class DummyBlobEndpoint:
    def __init__(self, versions):
        self._versions = versions

    def get_versions(self):
        return self._versions


def test_check_blob_date_exists_today(monkeypatch):
    today = "2024-06-11"
    with mock.patch(
        "cfa.scenarios.dataops.workflows.covid.funcs.check.datetime"
    ) as mock_datetime:
        mock_datetime.today.return_value.strftime.return_value = today
        blob_ep = DummyBlobEndpoint([today, "2024-06-10"])
        assert check_blob_date_exists(blob_ep) is True


def test_check_blob_date_exists_not_today(monkeypatch, capsys):
    today = "2024-06-11"
    with mock.patch(
        "cfa.scenarios.dataops.workflows.covid.funcs.check.datetime"
    ) as mock_datetime:
        mock_datetime.today.return_value.strftime.return_value = today
        blob_ep = DummyBlobEndpoint(["2024-06-10"])
        assert check_blob_date_exists(blob_ep) is False
        captured = capsys.readouterr()
        assert "No data generated today in blob storage." in captured.out


def test_import_vaccination_data_local(tmp_path):
    # Create a fake CSV file
    data = pd.DataFrame(
        {"date": ["2024-06-01", "2024-06-02", "2024-06-03"], "val": [1, 2, 3]}
    )
    file_path = tmp_path / "vaccination_data.csv"
    data.to_csv(file_path, index=False)
    # Should only return rows with date <= start
    result = import_vaccination_data("2024-06-02", str(tmp_path) + "/")
    assert isinstance(result, pd.DataFrame)
    assert all(result["date"] <= "2024-06-02")
    assert set(result.columns) == set(data.columns)


def test_import_vaccination_data_blob():
    # Mock blob_ep.read_csv to return a DataFrame
    mock_blob = MagicMock()
    mock_blob.read_csv.return_value = pd.DataFrame(
        {"date": ["2024-06-01", "2024-06-02"], "val": [1, 2]}
    )
    with patch(
        "cfa.scenarios.dataops.workflows.covid.funcs.import_data.get_today_date",
        return_value="2024-06-02",
    ):
        result = import_vaccination_data(
            "2024-06-02", "unused_path/", blob_ep=mock_blob
        )
    assert isinstance(result, pd.DataFrame)
    assert all(result["date"] <= "2024-06-02")


def test_import_population_data_basic():
    # Mock state Series
    state = pd.Series({"stname": "California"})
    age_groups = [17, 49, 64]

    # Create a fake age distribution DataFrame
    fake_age_dist = pd.DataFrame(
        {
            "region": ["California"] * 5,
            "age": [5, 15, 25, 55, 70],
            "census": [100, 200, 300, 400, 500],
        }
    )

    with patch(
        "cfa.scenarios.dataops.workflows.covid.funcs.import_data.get_data"
    ) as mock_get_data:
        mock_get_data.return_value = fake_age_dist

        pop = import_population_data(state, age_groups)

    # Check output DataFrame structure
    assert isinstance(pop, pd.DataFrame)
    assert set(pop.columns) == {"age", "proportion", "census"}
    # Should have len(age_groups)+1 rows
    assert len(pop) == len(age_groups) + 1
    # Proportions should sum to 1
    assert abs(pop["proportion"].sum() - 1.0) < 1e-6
    # Census should be distributed among groups
    assert pop["census"].sum() == fake_age_dist["census"].sum()


def test_import_variant_data_filters_and_returns():
    # Mock state Series
    state = pd.Series({"stname": "California"})

    # Fake region lookup DataFrame
    fake_region = pd.DataFrame(
        {"stname": ["California", "Texas"], "region": ["West", "South"]}
    )

    # Fake variant DataFrame
    fake_variant = pd.DataFrame(
        {
            "region": ["West", "West", "South"],
            "date": ["2024-06-01", "2024-06-02", "2024-06-01"],
            "variant": ["Omicron", "Delta", "Omicron"],
            "proportion": [0.8, 0.2, 0.5],
        }
    )

    # Patch get_data to return fake data in order
    with patch(
        "cfa.scenarios.dataops.workflows.covid.funcs.import_data.get_data"
    ) as mock_get_data:
        mock_get_data.side_effect = [fake_region, fake_variant]
        result = import_variant_data("2024-06-01", state)

    # Should only return rows for region "West" and date <= "2024-06-01"
    assert isinstance(result, pd.DataFrame)
    assert all(result["region"] == "West")
    assert all(result["date"] <= "2024-06-01")
    assert set(["date", "variant", "proportion", "region"]).issubset(
        result.columns
    )


def test_import_hospitalization_data_local(tmp_path):
    # Create a fake CSV file for a state
    data = pd.DataFrame(
        {
            "date": ["2024-06-01", "2024-06-02", "2024-06-03"],
            "state": ["CA", "CA", "CA"],
            "hospital_weekly": [10, 20, 30],
        }
    )
    file_path = tmp_path / "weekly_hospital_california.csv"
    data.to_csv(file_path, index=False)
    state = pd.Series({"stname": "California"})
    # Should only return rows with date <= start
    result = import_hospitalization_data(
        "2024-06-02", str(tmp_path) + "/", state
    )
    assert isinstance(result, pd.DataFrame)
    assert all(result["date"] <= "2024-06-02")
    assert set(result.columns) == {"date", "state", "hospital_weekly"}


def test_import_hospitalization_data_blob():
    # Mock blob_ep.read_csv to return a DataFrame
    mock_blob = MagicMock()
    mock_blob.read_csv.return_value = pd.DataFrame(
        {
            "date": ["2024-06-01", "2024-06-02"],
            "state": ["CA", "CA"],
            "hospital_weekly": [10, 20],
        }
    )
    state = pd.Series({"stname": "California"})
    with patch(
        "cfa.scenarios.dataops.workflows.covid.funcs.import_data.get_today_date",
        return_value="2024-06-02",
    ):
        result = import_hospitalization_data(
            "2024-06-02", "unused_path/", state, blob_ep=mock_blob
        )
    assert isinstance(result, pd.DataFrame)
    assert all(result["date"] <= "2024-06-02")
    assert set(result.columns) == {"date", "state", "hospital_weekly"}


def test_import_sero_data_filters_state():
    # Mock state Series
    state = pd.Series({"stusps": "CA"})

    # Fake seroprevalence DataFrame
    fake_sero = pd.DataFrame(
        {
            "state": ["CA", "CA", "TX"],
            "age": ["0-17", "18-49", "0-17"],
            "date": ["2024-06-01", "2024-06-01", "2024-06-01"],
            "sero": [0.1, 0.2, 0.3],
        }
    )

    with patch(
        "cfa.scenarios.dataops.workflows.covid.funcs.import_data.get_data"
    ) as mock_get_data:
        mock_get_data.return_value = fake_sero
        result = import_sero_data(state)

    # Should only return rows for state "CA"
    assert isinstance(result, pd.DataFrame)
    assert all(result["state"] == "CA")
    assert set(result.columns) == {"state", "age", "date", "sero"}


def test_format_vac_data_basic():
    # Create a simple fake vaccination DataFrame
    data = pd.DataFrame(
        {
            "date": [
                "2024-06-01",
                "2024-06-01",
                "2024-06-01",
                "2024-06-02",
                "2024-06-02",
            ],
            "age": ["0-17", "18-49", "50-64", "0-17", "18-49"],
            "dose": [0, 1, 2, 0, 1],
            "percentage": [0.1, 0.2, 0.3, 0.15, 0.25],
            "census": [1000, 2000, 1500, 1000, 2000],
            "total": [100, 400, 450, 120, 500],
        }
    )
    # Test for 3 doses, and pick a start date that exists
    vac_history, vac = format_vac_data(data, "2024-06-01", 3)
    # Check output types and columns
    assert isinstance(vac_history, pd.DataFrame)
    assert isinstance(vac, pd.DataFrame)
    assert set(["date", "age", "dose", "percent", "total"]).issubset(
        vac_history.columns
    )
    assert set(["date", "age", "dose", "percent", "total"]).issubset(
        vac.columns
    )
    # Check that vac is filtered to the correct date
    assert all(vac["date"] == "2024-06-01")
    # Should not be empty
    assert not vac_history.empty
    assert not vac.empty


def test_output_generation_basic():
    # Minimal population DataFrame
    pop = pd.DataFrame(
        {
            "age": ["0-17", "18-49"],
            "proportion": [0.2, 0.8],
            "census": [1000, 4000],
        }
    )

    # Minimal infections DataFrame
    infections = pd.DataFrame(
        {
            "age": ["0-17", "18-49", "0-17", "18-49"],
            "type": ["None", "None", "Omicron", "Omicron"],
            "percent": [0.7, 0.8, 0.3, 0.2],
        }
    )

    # Minimal vaccination DataFrame
    vac = pd.DataFrame(
        {
            "age": ["0-17", "18-49", "0-17", "18-49"],
            "dose": [0, 0, 1, 1],
            "percent": [0.6, 0.7, 0.4, 0.3],
            "total": [600, 2800, 400, 1200],
            "date": ["2024-06-01"] * 4,
        }
    )

    doses = 2
    variant_types = ["Omicron"]

    output = output_generation(infections, vac, pop, doses, variant_types)

    # Check output DataFrame structure
    assert isinstance(output, pd.DataFrame)
    assert set(["age", "type", "dose", "percent"]).issubset(output.columns)
    # Check that all ages, types, and doses are present in output
    assert set(output["age"]) <= set(pop["age"])
    assert set(output["type"]) <= set(["None", "Omicron"])
    assert set(output["dose"]) <= set([0, 1])
    # Check that percent values are correct for at least one row
    row = output.iloc[0]
    inf = infections[
        (infections.age == row["age"]) & (infections.type == row["type"])
    ]["percent"].values[0]
    vax = vac[(vac.age == row["age"]) & (vac.dose == row["dose"])][
        "percent"
    ].values[0]
    assert abs(row["percent"] - (inf * vax)) < 1e-8


def test_total_cases_using_hospital_and_sero_basic():
    # Minimal population DataFrame
    pop = pd.DataFrame({"age": ["0-17", "18-49"], "census": [1000, 4000]})

    # Minimal hospital DataFrame
    hospital = pd.DataFrame(
        {"date": ["2024-06-01", "2024-06-08"], "hospital_weekly": [10, 20]}
    )

    # Minimal sero DataFrame (two time steps per age group)
    sero = pd.DataFrame(
        {
            "age": ["0-17", "0-17", "18-49", "18-49"],
            "date": ["2024-06-01", "2024-06-08", "2024-06-01", "2024-06-08"],
            "sero": [0.1, 0.15, 0.2, 0.25],
        }
    )

    result = total_cases_using_hospital_and_sero(hospital, sero, pop)

    # Check output DataFrame structure
    assert isinstance(result, pd.DataFrame)
    assert set(["date", "age", "cases"]).issubset(result.columns)
    # Should have len(pop) * len(hospital) rows
    assert len(result) == len(pop) * len(hospital)
    # All ages and dates should be present
    assert set(result["age"]) == set(pop["age"])
    assert set(result["date"]) == set(hospital["date"])
    # Cases should be non-negative
    assert (result["cases"] >= 0).all()


def test_cases_by_variant_basic():
    # Minimal population DataFrame
    pop = pd.DataFrame({"age": ["0-17", "18-49"]})

    # Minimal cases DataFrame (output from total_cases_using_hospital_and_sero)
    cases = pd.DataFrame(
        {
            "date": ["2024-06-01", "2024-06-01", "2024-06-08", "2024-06-08"],
            "age": ["0-17", "18-49", "0-17", "18-49"],
            "cases": [100, 200, 150, 250],
        }
    )

    # Minimal variant DataFrame
    variant = pd.DataFrame(
        {
            "date": ["2024-06-01", "2024-06-01", "2024-06-08", "2024-06-08"],
            "variant": ["Omicron", "Delta", "Omicron", "Delta"],
            "proportion": [0.7, 0.3, 0.6, 0.4],
        }
    )

    variant_types = ["Omicron", "Delta"]

    result = cases_by_variant(cases, variant, pop, variant_types)

    # Check output DataFrame structure
    assert isinstance(result, pd.DataFrame)
    assert set(["date", "cases", "age", "strain"]).issubset(result.columns)
    # Check that all ages and strains are present
    assert set(result["age"]) <= set(pop["age"])
    assert set(result["strain"]) == set(variant_types)
    # Check that cases are split by variant proportion
    for _, row in result.iterrows():
        # Find matching variant proportion
        vprop = variant[
            (variant["date"] == row["date"])
            & (variant["variant"] == row["strain"])
        ]["proportion"]
        # Find original cases for this age/date
        orig_cases = cases[
            (cases["date"] == row["date"]) & (cases["age"] == row["age"])
        ]["cases"]
        if not vprop.empty and not orig_cases.empty:
            expected = orig_cases.values[0] * vprop.values[0]
            assert abs(row["cases"] - expected) < 1e-8


def test_individual_infection_totals_single_variant():
    # Single variant type (e.g., only Omicron)
    pop = pd.DataFrame({"age": ["0-17", "18-49"], "census": [1000, 2000]})
    infect = pd.DataFrame(
        {
            "age": ["0-17", "18-49"],
            "strain": ["Omicron", "Omicron"],
            "cases": [300, 800],
        }
    )
    variant_types = ["Omicron"]

    result = individual_infection_totals(infect, pop, variant_types)
    assert isinstance(result, pd.DataFrame)
    assert set(result.columns) == {"age", "type", "percent", "total"}
    assert set(result["type"]) == {"None", "Omicron"}
    # Check that percent sums to 1 for each age group
    for age in pop["age"]:
        group = result[result["age"] == age]
        assert abs(group["percent"].sum() - 1.0) < 1e-8
        # "Omicron" total should match cases, "None" should be census - cases
        omicron_row = group[group["type"] == "Omicron"].iloc[0]
        none_row = group[group["type"] == "None"].iloc[0]
        assert (
            omicron_row["total"]
            == infect[infect["age"] == age]["cases"].values[0]
        )
        assert (
            none_row["total"]
            == pop[pop["age"] == age]["census"].values[0]
            - omicron_row["total"]
        )


def test_individual_infection_totals_two_variants():
    # Two variant types (e.g., Omicron and Delta)
    pop = pd.DataFrame({"age": ["0-17"], "census": [1000]})
    infect = pd.DataFrame(
        {
            "age": ["0-17", "0-17"],
            "strain": ["Omicron", "Delta"],
            "cases": [300, 200],
        }
    )
    variant_types = ["Omicron", "Delta"]

    result = individual_infection_totals(infect, pop, variant_types)
    assert isinstance(result, pd.DataFrame)
    assert set(result.columns) == {"age", "type", "percent", "total"}
    # Should have "None", "Omicron", "Delta", "Both"
    assert set(result["type"]) == {"None", "Omicron", "Delta", "Both"}
    # Percent should sum to 1
    assert abs(result["percent"].sum() - 1.0) < 1e-8
    # Totals should sum to census
    assert abs(result["total"].sum() - 1000) < 1e-8


def test_get_tslie2():
    from cfa.scenarios.dataops.workflows.covid.funcs.tslie import get_tslie2

    # Minimal population DataFrame
    pop = pd.DataFrame(
        {
            "age": ["0-17", "18-49", "50-64", "65+"],
            "census": [1000, 2000, 3000, 4000],
        }
    )

    # Minimal infection DataFrame
    infect = pd.DataFrame(
        {
            "date": ["2024-06-01", "2024-06-02", "2024-06-01", "2024-06-02"],
            "age": ["0-17", "0-17", "18-49", "18-49"],
            "strain": ["Omicron", "Omicron", "Omicron", "Omicron"],
            "cases": [10, 20, 30, 40],
        }
    )

    # Minimal vaccination history DataFrame
    vac_history = pd.DataFrame(
        {
            "date": ["2024-06-01", "2024-06-02", "2024-06-01", "2024-06-02"],
            "age": ["0-17", "0-17", "18-49", "18-49"],
            "dose": [0, 1, 0, 1],
            "total": [100, 120, 200, 220],
        }
    )

    # Minimal output DataFrame (from output_generation)
    output = pd.DataFrame(
        {
            "age": ["0-17", "18-49"],
            "dose": [0, 0],
            "type": ["None", "None"],
            "percent": [0.5, 0.5],
        }
    )

    start = "2024-06-01"
    tslie_ranges = [0, 30, 60, 90]

    tslie2 = get_tslie2(
        infect=infect,
        vac_history=vac_history,
        output=output,
        pop=pop,
        start=start,
        tslie_ranges=tslie_ranges,
    )

    # Check output DataFrame structure
    assert isinstance(tslie2, pd.DataFrame)
    # Should have all age groups and correct columns
    assert set(tslie2["age"]).issubset(pop["age"])
    assert set(tslie2.columns).issubset(
        set(
            [
                "age",
                "dose",
                "type",
                "percent",
                "tslie_group_0",
                "tslie_group_1",
                "tslie_group_2",
                "tslie_group_3",
                "tslie_group_4",
            ]
        )
    )
    # check percents between 0 and 100
    assert (tslie2["percent"] >= 0).all() and (tslie2["percent"] <= 100).all()

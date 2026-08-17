"""End-to-end tests for the package-level public API."""

from datetime import date

import polars as pl
import pytest

from showstats import make_stats_tbl, show_stats

MIXED = pl.DataFrame(
    {
        "second_number": [10, 20, 30, 40],
        "first_number": [1.0, 2.0, 3.0, 4.0],
        "category": ["a", "b", "a", None],
        "date": [date(2024, 1, day) for day in range(1, 5)],
    }
)


def test_show_stats_runs_end_to_end_from_the_package(capsys):
    show_stats(
        MIXED,
        table_type="all",
        top_cols="first_number",
        quantiles=[0.25],
        fold_quantiles=False,
    )

    output = capsys.readouterr().out
    assert "Date and datetime columns (N=4)" in output
    assert "Numerical columns (N=4)" in output
    assert "Categorical columns (N=4)" in output
    assert "first_number" in output
    assert "Q25" in output
    assert "category" in output
    assert "date" in output


def test_make_stats_tbl_runs_end_to_end_from_the_package():
    tables = make_stats_tbl(
        MIXED,
        table_type="all",
        top_cols="first_number",
        quantiles=[0.25],
        fold_quantiles=False,
    )

    assert list(tables) == ["time", "num", "cat"]
    assert all(isinstance(table, pl.DataFrame) for table in tables.values())
    assert tables["time"]["Col"].to_list() == ["date"]
    assert tables["num"]["Col"].to_list() == ["first_number", "second_number"]
    assert tables["num"].columns == [
        "Col",
        "NA%",
        "Avg",
        "Median",
        "SD",
        "Q25",
        "Min",
        "Max",
    ]
    assert tables["cat"]["Col"].to_list() == ["category"]
    assert tables["cat"]["Top 1"].to_list() == ["a (50%)"]


def test_make_stats_tbl_default_returns_the_numerical_table():
    table = make_stats_tbl(MIXED)

    assert table["Col"].to_list() == ["first_number", "second_number"]
    assert "category" not in table["Col"].to_list()
    assert "date" not in table["Col"].to_list()


def test_make_stats_tbl_returns_none_when_the_type_is_absent():
    assert make_stats_tbl(MIXED.select("category"), table_type="num") is None


@pytest.mark.parametrize("api", [show_stats, make_stats_tbl])
def test_public_api_rejects_an_invalid_table_type(api):
    with pytest.raises(ValueError, match="table_type 'invalid' not supported"):
        api(MIXED, table_type="invalid")


@pytest.mark.parametrize("api", [show_stats, make_stats_tbl])
@pytest.mark.parametrize(
    "empty",
    [
        pytest.param(pl.DataFrame(), id="no-columns"),
        pytest.param(
            pl.DataFrame({"value": []}, schema={"value": pl.Int64}), id="no-rows"
        ),
    ],
)
def test_public_api_rejects_empty_dataframes(api, empty):
    with pytest.raises(ValueError, match="must have rows and columns"):
        api(empty)


@pytest.mark.parametrize("api", [show_stats, make_stats_tbl])
def test_public_api_rejects_out_of_range_quantiles(api):
    with pytest.raises(ValueError, match=r"quantiles must lie in \[0, 1\]"):
        api(MIXED, table_type="num", quantiles=[1.01])


def test_show_stats_rejects_an_invalid_output():
    with pytest.raises(ValueError, match="out 'html' not supported"):
        show_stats(MIXED, out="html")

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


@pytest.mark.parametrize(
    ("style", "column", "expected"),
    [
        pytest.param("mean_sd", "Avg (SD)", "26.5 (49.01)", id="mean-sd"),
        pytest.param("median_mad", "Median (MAD)", "2.5 (1.0)", id="median-mad"),
        pytest.param(
            "median_iqr",
            "Median [Q1, Q3]",
            "2.5 [1.75, 27.25]",
            id="median-iqr",
        ),
    ],
)
def test_table_one_combines_location_and_spread(style, column, expected):
    table = make_stats_tbl(
        pl.DataFrame({"x": [1.0, 2.0, 3.0, 100.0]}),
        table_type="num",
        table_one=style,
    )

    assert table.columns == ["Col", "NA%", column]
    assert table[column].item() == expected


@pytest.mark.parametrize("style", ["mean_sd", "median_mad", "median_iqr"])
def test_table_one_accepts_lazy_input(style):
    frame = pl.DataFrame({"x": [1.0, 2.0, None, 4.0]})

    eager = make_stats_tbl(frame, table_type="num", table_one=style)
    lazy = make_stats_tbl(frame.lazy(), table_type="num", table_one=style)

    assert lazy.equals(eager)


def test_table_one_rejects_conflicting_or_unsupported_options():
    with pytest.raises(ValueError, match="table_one must be one of"):
        make_stats_tbl(MIXED, "num", table_one="unknown")
    with pytest.raises(ValueError, match="cannot be used together"):
        make_stats_tbl(MIXED, "num", table_one="mean_sd", quantiles=[0.5])
    with pytest.raises(ValueError, match="only available for numerical"):
        make_stats_tbl(MIXED, "cat", table_one="mean_sd")

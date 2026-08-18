"""End-to-end tests for the package-level public API."""

from datetime import date

import polars as pl
import pytest

from showstats import make_stats_tbl, show_stats, table_one

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
        MIXED.select("first_number", pl.all().exclude("first_number")),
        table_type="all",
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
    ("style", "label", "expected"),
    [
        pytest.param("mean_sd", "mean (SD)", "26.5 (49.01)", id="mean-sd"),
        pytest.param("median_mad", "median (MAD)", "2.5 (1.0)", id="median-mad"),
        pytest.param(
            "median_iqr",
            "median [Q1, Q3]",
            "2.5 [1.75, 27.25]",
            id="median-iqr",
        ),
    ],
)
def test_table_one_combines_location_and_spread(capsys, style, label, expected):
    table_one(
        pl.DataFrame({"x": [1.0, 2.0, 3.0, 100.0]}),
        style=style,
    )

    output = capsys.readouterr().out
    assert f"x ({label})" in output
    assert expected in output


@pytest.mark.parametrize("style", ["mean_sd", "median_mad", "median_iqr"])
def test_table_one_accepts_lazy_input(capsys, style):
    frame = pl.DataFrame(
        {
            "x": [1.0, 2.0, None, 4.0],
            "group": ["a", "b", "a", None],
        }
    )

    table_one(frame.lazy(), style=style)
    lazy_output = capsys.readouterr().out
    table_one(frame, style=style)

    assert lazy_output == capsys.readouterr().out


def test_table_one_includes_percent_missing(capsys):
    table_one(pl.DataFrame({"x": [1.0, 2.0, None, 4.0]}))

    output = capsys.readouterr().out
    assert "NA%" in output
    assert "x (mean (SD))  25" in output


def test_table_one_shows_categorical_percent_missing_once(capsys):
    table_one(pl.DataFrame({"group": ["a", "b", None, "a"]}))

    rows = [line for line in capsys.readouterr().out.splitlines() if "group" in line]
    assert "group = a (%)  25" in rows[0]
    assert "group = b (%)       1 (25%)" in rows[1]


def test_table_one_can_hide_percent_missing(capsys):
    table_one(
        pl.DataFrame(
            {
                "x": [1.0, 2.0, None, 4.0],
                "group": ["a", "b", None, "a"],
            }
        ),
        show_missing=False,
    )

    output = capsys.readouterr().out
    assert "NA%" not in output
    assert "x (mean (SD))  2.33 (1.53)" in output


def test_table_one_shows_three_categories_by_default(capsys):
    table_one(pl.DataFrame({"group": ["a", "b", "c", "d"]}))

    output = capsys.readouterr().out
    assert all(f"group = {value} (%)" in output for value in ("a", "b", "c"))
    assert "group = d (%)" not in output


def test_table_one_controls_the_number_of_categories(capsys):
    table_one(
        pl.DataFrame({"group": ["a", "b", "c", "d"]}),
        n_categories=2,
    )

    output = capsys.readouterr().out
    assert all(f"group = {value} (%)" in output for value in ("a", "b"))
    assert "group = c (%)" not in output


def test_table_one_rejects_an_unsupported_style():
    with pytest.raises(ValueError, match="style must be one of"):
        table_one(MIXED, style="unknown")


@pytest.mark.parametrize("n_categories", [0, -1])
def test_table_one_rejects_too_few_categories(n_categories):
    with pytest.raises(ValueError, match="at least 1"):
        table_one(MIXED, n_categories=n_categories)


@pytest.mark.parametrize("n_categories", [1.5, True])
def test_table_one_rejects_a_noninteger_category_count(n_categories):
    with pytest.raises(TypeError, match="must be an integer"):
        table_one(MIXED, n_categories=n_categories)


@pytest.mark.parametrize("api", [show_stats, table_one])
def test_public_api_rejects_an_invalid_format(api):
    with pytest.raises(ValueError, match="fmt 'html' not supported"):
        api(MIXED, fmt="html")


@pytest.mark.parametrize("api", [show_stats, table_one])
def test_color_missing_requires_gt_output(api):
    with pytest.raises(ValueError, match='color_missing=True requires fmt="gt"'):
        api(MIXED, color_missing=True)


def test_table_one_color_missing_requires_the_missing_column():
    with pytest.raises(
        ValueError, match="color_missing=True requires show_missing=True"
    ):
        table_one(MIXED, fmt="gt", show_missing=False, color_missing=True)


def test_show_stats_rejects_an_invalid_format():
    with pytest.raises(ValueError, match="fmt 'html' not supported"):
        show_stats(MIXED, fmt="html")


def test_color_missing_requires_gt_output():
    with pytest.raises(ValueError, match='color_missing=True requires fmt="gt"'):
        show_stats(MIXED, color_missing=True)

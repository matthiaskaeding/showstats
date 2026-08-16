"""
Tests to verify showstats works correctly with different dataframe backends.
"""

from datetime import date, datetime

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from showstats import show_stats
from showstats.showstats import make_stats_tbl
from tests.helpers import cell, row_for, stats_frame

# The same data in three backends, converted through polars so the dtypes
# correspond: a hand-built pandas frame would differ in ways that have
# nothing to do with showstats (int64 vs float64 for a column with nulls,
# say), and the comparisons below would then be measuring the conversion.
MIXED_PL = pl.DataFrame(
    {
        "int_col": [1, 2, 3, 4, 5],
        "float_col": [1.5, 2.5, 3.5, 4.5, 5.5],
        "big_col": [1e-7, 1.0, 12345.0, 9.87e9, 3.0],
        "bool_col": [True, False, True, False, True],
        "str_col": ["a", "b", "c", "a", "b"],
    }
)
MIXED_PD = MIXED_PL.to_pandas()
MIXED_PA = MIXED_PL.to_arrow()


def test_polars_backend_basic():
    """Test basic functionality with polars DataFrame"""
    df = pl.DataFrame(
        {
            "int_col": [1, 2, 3, 4, 5],
            "float_col": [1.1, 2.2, 3.3, 4.4, 5.5],
            "str_col": ["a", "b", "c", "d", "e"],
        }
    )

    # Should not raise any errors
    show_stats(df)

    # Test individual table types
    show_stats(df, "num")
    show_stats(df, "cat")

    # Test make_stats_tbl
    result = make_stats_tbl(df, "num")
    assert result is not None
    assert stats_frame(result).shape[0] == 2  # int_col and float_col


def test_pandas_backend_basic():
    """Test basic functionality with pandas DataFrame"""
    df = pd.DataFrame(
        {
            "int_col": [1, 2, 3, 4, 5],
            "float_col": [1.1, 2.2, 3.3, 4.4, 5.5],
            "str_col": ["a", "b", "c", "d", "e"],
        }
    )

    # Should not raise any errors
    show_stats(df)

    # Test individual table types
    show_stats(df, "num")
    show_stats(df, "cat")

    # Test make_stats_tbl
    result = make_stats_tbl(df, "num")
    assert result is not None
    assert stats_frame(result).shape[0] == 2  # int_col and float_col


def test_pyarrow_backend_basic():
    """pyarrow input, which used to be accepted at the door and then die.

    `_check_input_maybe_try_transform` has always taken pyarrow tables
    (test_utils.py::test_input_check_pyarrow), but `_Table.__init__` read
    the aggregate row with `.iloc[0]` whenever the frame was not polars —
    treating "not polars" as "pandas". Every pyarrow table therefore
    raised AttributeError.
    """
    result = make_stats_tbl(MIXED_PA, "num")
    # int_col, float_col, big_col, bool_col — str_col is not numerical
    assert stats_frame(result).shape[0] == 4


def test_pyarrow_backend_categorical_top_values():
    result = make_stats_tbl(MIXED_PA, "cat")
    frame = stats_frame(result)
    assert "Top 1" in frame.columns
    assert frame["Top 1"][0] == "a (40%)"


def test_pyarrow_backend_all_types(capsys):
    show_stats(MIXED_PA, "all")
    assert "int_col" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("frame", "native_type"),
    [
        pytest.param(
            pl.DataFrame(
                {
                    "number": [1, 2],
                    "category": ["a", "b"],
                    "date": [date(2020, 1, 1), date(2020, 1, 2)],
                }
            ),
            pl.DataFrame,
            id="polars",
        ),
        pytest.param(
            pd.DataFrame(
                {
                    "number": [1, 2],
                    "category": ["a", "b"],
                    "date": pd.to_datetime(["2020-01-01", "2020-01-02"]),
                }
            ),
            pd.DataFrame,
            id="pandas",
        ),
        pytest.param(
            pa.table(
                {
                    "number": [1, 2],
                    "category": ["a", "b"],
                    "date": pa.array(
                        [date(2020, 1, 1), date(2020, 1, 2)], type=pa.date32()
                    ),
                }
            ),
            pa.Table,
            id="pyarrow",
        ),
    ],
)
def test_make_stats_tbl_all_returns_each_table_in_the_input_backend(frame, native_type):
    result = make_stats_tbl(frame, "all")

    assert list(result) == ["time", "num", "cat"]
    assert all(isinstance(table, native_type) for table in result.values())
    assert stats_frame(result["time"])["Col"].to_list() == ["date"]
    assert stats_frame(result["num"])["Col"].to_list() == ["number"]
    assert stats_frame(result["cat"])["Col"].to_list() == ["category"]


def test_make_stats_tbl_all_omits_empty_table_types():
    result = make_stats_tbl(pl.DataFrame({"number": [1, 2]}), "all")

    assert list(result) == ["num"]


@pytest.mark.parametrize(
    "df",
    [pytest.param(MIXED_PD, id="pandas"), pytest.param(MIXED_PA, id="pyarrow")],
)
@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param({"table_type": "all"}, id="all"),
        pytest.param({"table_type": "num"}, id="num"),
        pytest.param({"table_type": "cat"}, id="cat"),
        pytest.param({"table_type": "num", "quantiles": [0.25, 0.75]}, id="quantiles"),
        pytest.param(
            {"table_type": "num", "quantiles": [0.5], "fold_quantiles": False},
            id="unfolded",
        ),
    ],
)
def test_rendering_does_not_depend_on_the_input_backend(capsys, df, kwargs):
    """Identical data must print identically whatever frame carries it.

    Four separate ways it did not, each caught here rather than by
    inspection, and each traceable to one backend's idea of how a value
    becomes text:

    - pandas printed int Min/Max as `1.0` and Uniques as `3.00`, because
      reading the aggregate row through `.iloc[0]` gave a Series, which
      has a single dtype.
    - pyarrow's double-to-string cast drops a trailing `.0`, so `3.0`
      printed as `3`.
    - pandas renders a boolean as `True` where polars and pyarrow give
      `true`.
    - pyarrow has no `add` kernel for two strings, so building `"1.0E5"`
      by concatenation raised instead of printing anything at all.

    polars is the reference, since README.md is generated from it — hence
    comparing against the polars rendering rather than merely checking the
    two agree with each other.
    """
    show_stats(df, **kwargs)
    from_backend = capsys.readouterr().out
    show_stats(MIXED_PL, **kwargs)
    assert from_backend == capsys.readouterr().out


TEMPORAL_PL = pl.DataFrame(
    {
        "date_col": [date(2020, 1, 1), date(2020, 6, 1), None, date(2020, 12, 1)],
        "dt_col": [
            datetime(2020, 1, 1),  # noqa: DTZ001 — naive, as the column is
            datetime(2020, 1, 3),  # noqa: DTZ001
            None,
            datetime(2020, 1, 2),  # noqa: DTZ001
        ],
    }
)


@pytest.mark.parametrize("backend", ["polars", "pandas", "pyarrow"])
def test_temporal_columns_summarise_on_every_backend(backend):
    """A date column on a pyarrow table used to raise outright.

    The median went through Int64 — the detour pandas needs, since it
    refuses median() on a datetime — and Arrow refuses that cast:
    `Unsupported cast from date32[day] to int64`. It has no quantile
    kernel for dates either, so there is no expression that works
    everywhere; the median is computed from the sorted values instead
    (#86).
    """
    frame = {
        "polars": TEMPORAL_PL,
        "pandas": TEMPORAL_PL.to_pandas(),
        "pyarrow": TEMPORAL_PL.to_arrow(),
    }[backend]

    rows = {
        row["Col"]: row
        for row in stats_frame(make_stats_tbl(frame, "time")).rows(named=True)
    }

    # The middle of three real dates, with the null ignored rather than
    # dragged in as the int64 minimum.
    assert rows["date_col"]["Median"].startswith("2020-06-01")
    assert rows["date_col"]["Min"].startswith("2020-01-01")
    assert rows["date_col"]["Max"].startswith("2020-12-01")
    assert rows["date_col"]["NA%"] == 25
    assert rows["dt_col"]["Median"].startswith("2020-01-02")


def test_temporal_median_of_an_even_count_is_the_midpoint():
    """Two middle instants average, and a date truncates to whole days."""
    even = pl.DataFrame({"d": [date(2020, 1, 1), date(2020, 1, 2)]})
    row = stats_frame(make_stats_tbl(even, "time")).rows(named=True)[0]
    assert row["Median"] == "2020-01-01"

    even_dt = pl.DataFrame(
        {"t": [datetime(2020, 1, 1), datetime(2020, 1, 2)]}  # noqa: DTZ001
    )
    row = stats_frame(make_stats_tbl(even_dt, "time")).rows(named=True)[0]
    assert row["Median"] == "2020-01-01 12:00:00"


def test_show_stats_accepts_pyarrow_date_and_datetime(capsys):
    """Pin the public API reproduction from #86."""
    frame = pl.DataFrame(
        {
            "date_col": [date(2020, 1, 1), date(2020, 6, 1)],
            "dt_col": [
                datetime(2020, 1, 1),  # noqa: DTZ001
                datetime(2020, 1, 2),  # noqa: DTZ001
            ],
        }
    ).to_arrow()

    show_stats(frame, "time")

    output = capsys.readouterr().out
    assert "date_col" in output
    assert "2020-01-01" in output
    assert "dt_col" in output
    assert "2020-01-01 12:00:00" in output


def test_polars_vs_pandas_numeric_stats():
    """Test that numeric statistics are consistent between polars and pandas"""
    # Create identical data in both formats
    data = {
        "a": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "b": [10.5, 20.5, 30.5, 40.5, 50.5, 60.5, 70.5, 80.5, 90.5, 100.5],
    }

    df_polars = pl.DataFrame(data)
    df_pandas = pd.DataFrame(data)

    result_polars = make_stats_tbl(df_polars, "num")
    result_pandas = make_stats_tbl(df_pandas, "num")

    # Both should have same shape
    assert stats_frame(result_polars).shape == stats_frame(result_pandas).shape

    # Variable names should be the same
    assert result_polars["Col"].to_list() == result_pandas["Col"].to_list()

    # NA% should be the same (both 0)
    assert result_polars["NA%"].to_list() == result_pandas["NA%"].to_list()

    # Mean values should be the same
    assert result_polars["Avg"].to_list() == result_pandas["Avg"].to_list()


def test_polars_vs_pandas_categorical_stats():
    """Test that categorical statistics are consistent between polars and pandas"""
    # Create identical data in both formats
    data = {
        "cat1": ["A", "B", "C", "A", "B", "C", "A", "A", "B", "C"],
        "cat2": ["X", "Y", "X", "Y", "X", "Y", "X", "Y", "X", "Y"],
    }

    df_polars = pl.DataFrame(data)
    df_pandas = pd.DataFrame(data)

    result_polars = make_stats_tbl(df_polars, "cat")
    result_pandas = make_stats_tbl(df_pandas, "cat")

    # Both should have same shape
    assert stats_frame(result_polars).shape == stats_frame(result_pandas).shape

    # Variable names should be the same
    assert result_polars["Col"].to_list() == result_pandas["Col"].to_list()

    # Number of uniques should be the same
    assert result_polars["Uniques"].to_list() == result_pandas["Uniques"].to_list()


def test_polars_backend_with_nulls():
    """Test polars backend handles null values correctly"""
    df = pl.DataFrame(
        {
            "col_with_nulls": [1, 2, None, 4, None, 6, 7, 8, 9, 10],
            "col_no_nulls": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    )

    result = make_stats_tbl(df, "num")

    # col_with_nulls should have 20% NA (2 out of 10)
    assert row_for(result, "col_with_nulls")["NA%"][0] == 20

    # col_no_nulls should have 0% NA
    assert row_for(result, "col_no_nulls")["NA%"][0] == 0


def test_pandas_backend_with_nulls():
    """Test pandas backend handles null values correctly"""
    df = pd.DataFrame(
        {
            "col_with_nulls": [1, 2, None, 4, None, 6, 7, 8, 9, 10],
            "col_no_nulls": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    )

    result = make_stats_tbl(df, "num")

    # col_with_nulls should have 20% NA (2 out of 10)
    assert row_for(result, "col_with_nulls")["NA%"][0] == 20

    # col_no_nulls should have 0% NA
    assert row_for(result, "col_no_nulls")["NA%"][0] == 0


def test_polars_backend_datetime():
    """Test polars backend with datetime columns"""
    df = pl.DataFrame(
        {
            "date_col": pl.date_range(
                pl.date(2020, 1, 1), pl.date(2020, 1, 10), "1d", eager=True
            ),
            "datetime_col": pl.datetime_range(
                pl.datetime(2020, 1, 1), pl.datetime(2020, 1, 10), "1d", eager=True
            ),
        }
    )

    result = make_stats_tbl(df, "time")

    # Should have 2 rows (date_col and datetime_col)
    assert stats_frame(result).shape[0] == 2

    # Should have columns: Col, NA%, Min, Max, Median
    assert "Col" in stats_frame(result).columns
    assert "NA%" in stats_frame(result).columns
    assert "Min" in stats_frame(result).columns
    assert "Max" in stats_frame(result).columns
    assert "Median" in stats_frame(result).columns


def test_pandas_backend_datetime():
    """Test pandas backend with datetime columns"""
    df = pd.DataFrame(
        {"datetime_col": pd.date_range("2020-01-01", periods=10, freq="D")}
    )

    result = make_stats_tbl(df, "time")

    assert result is not None
    assert "Median" in stats_frame(result).columns
    # The median of ten consecutive days is midday on the fifth. Getting this
    # right is what the Int64 round-trip in _median_expr is for: a wrong time
    # unit would silently land in 1970.
    assert cell(result, 0, "Median").startswith("2020-01-05 12:00:00")
    assert cell(result, 0, "Min").startswith("2020-01-01")
    assert cell(result, 0, "Max").startswith("2020-01-10")


def test_polars_backend_boolean():
    """Test polars backend with boolean columns"""
    df = pl.DataFrame(
        {
            "bool_col": [
                True,
                False,
                True,
                False,
                True,
                False,
                True,
                False,
                True,
                False,
            ],
            "int_col": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    )

    result = make_stats_tbl(df, "num")

    # Should have 2 rows (bool_col and int_col)
    assert stats_frame(result).shape[0] == 2

    # bool_col should be included
    assert "bool_col" in result["Col"].to_list()


def test_pandas_backend_boolean():
    """Test pandas backend with boolean columns"""
    df = pd.DataFrame(
        {
            "bool_col": [
                True,
                False,
                True,
                False,
                True,
                False,
                True,
                False,
                True,
                False,
            ],
            "int_col": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    )

    result = make_stats_tbl(df, "num")

    assert result is not None
    assert stats_frame(result).shape[0] >= 1
    # Five True out of ten, so the median of the boolean column is 0.5 —
    # the same value polars reports for median() on a boolean.
    assert row_for(result, "bool_col")["Median"][0] == "0.5"


def test_polars_backend_all_types():
    """Test polars backend with all table types"""
    df = pl.DataFrame(
        {
            "int_col": [1, 2, 3, 4, 5],
            "float_col": [1.1, 2.2, 3.3, 4.4, 5.5],
            "str_col": ["a", "b", "c", "d", "e"],
            "date_col": pl.date_range(
                pl.date(2020, 1, 1), pl.date(2020, 1, 5), "1d", eager=True
            ),
            "bool_col": [True, False, True, False, True],
        }
    )

    # Should not raise any errors
    show_stats(df, "all")


def test_pandas_backend_all_types():
    """Test pandas backend with all table types"""
    df = pd.DataFrame(
        {
            "int_col": [1, 2, 3, 4, 5],
            "float_col": [1.1, 2.2, 3.3, 4.4, 5.5],
            "str_col": ["a", "b", "c", "d", "e"],
            "datetime_col": pd.date_range("2020-01-01", periods=5, freq="D"),
            "bool_col": [True, False, True, False, True],
        }
    )

    # Must not raise: this mixes the two dtypes whose median used to fail on
    # non-polars backends (datetime and boolean) with the ones that never did.
    show_stats(df, "all")


def test_backend_with_empty_categorical():
    """Test that both backends handle dataframes with no categorical columns"""
    # Polars
    df_polars = pl.DataFrame(
        {
            "int_col": [1, 2, 3],
            "float_col": [1.1, 2.2, 3.3],
        }
    )
    result_polars = make_stats_tbl(df_polars, "cat")
    assert result_polars is None

    # Pandas
    df_pandas = pd.DataFrame(
        {
            "int_col": [1, 2, 3],
            "float_col": [1.1, 2.2, 3.3],
        }
    )
    result_pandas = make_stats_tbl(df_pandas, "cat")
    assert result_pandas is None


def test_backend_with_empty_numeric():
    """Test that both backends handle dataframes with no numeric columns"""
    # Polars
    df_polars = pl.DataFrame(
        {
            "str_col": ["a", "b", "c"],
        }
    )
    result_polars = make_stats_tbl(df_polars, "num")
    assert result_polars is None

    # Pandas
    df_pandas = pd.DataFrame(
        {
            "str_col": ["a", "b", "c"],
        }
    )
    result_pandas = make_stats_tbl(df_pandas, "num")
    assert result_pandas is None


def test_polars_backend_categorical_top_values():
    """Test that polars backend correctly computes top categorical values"""
    df = pl.DataFrame(
        {
            "cat_col": ["A"] * 5 + ["B"] * 3 + ["C"] * 2,
        }
    )

    result = make_stats_tbl(df, "cat")

    # Should have Top 1, Top 2, Top 3 columns
    assert "Top 1" in stats_frame(result).columns
    assert "Top 2" in stats_frame(result).columns
    assert "Top 3" in stats_frame(result).columns

    # Top 1 should be A with 50%
    assert "A (50%)" in result["Top 1"][0]


def test_pandas_backend_categorical_top_values():
    """Test that pandas backend correctly computes top categorical values"""
    df = pd.DataFrame(
        {
            "cat_col": ["A"] * 5 + ["B"] * 3 + ["C"] * 2,
        }
    )

    result = make_stats_tbl(df, "cat")

    # Should have Top 1, Top 2, Top 3 columns
    assert "Top 1" in stats_frame(result).columns
    assert "Top 2" in stats_frame(result).columns
    assert "Top 3" in stats_frame(result).columns

    # Top 1 should be A with 50%
    assert "A (50%)" in result["Top 1"][0]


def test_backend_compatibility_numeric_edge_cases():
    """Test edge cases in numeric data across backends"""
    # Test with zeros
    df_polars = pl.DataFrame({"col": [0.0, 0.0, 0.0]})
    df_pandas = pd.DataFrame({"col": [0.0, 0.0, 0.0]})

    result_polars = make_stats_tbl(df_polars, "num")
    result_pandas = make_stats_tbl(df_pandas, "num")

    # Both should handle zeros correctly
    assert result_polars["Avg"][0] == "0.0"
    assert result_pandas["Avg"][0] == "0.0"

    # Test with negative numbers
    df_polars = pl.DataFrame({"col": [-1, -2, -3, -4, -5]})
    df_pandas = pd.DataFrame({"col": [-1, -2, -3, -4, -5]})

    result_polars = make_stats_tbl(df_polars, "num")
    result_pandas = make_stats_tbl(df_pandas, "num")

    # Both should handle negatives correctly
    assert result_polars.shape[0] == 1
    assert result_pandas.shape[0] == 1

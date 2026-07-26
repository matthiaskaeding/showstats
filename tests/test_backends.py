"""
Tests to verify showstats works correctly with different dataframe backends.
"""

import pandas as pd
import polars as pl
from showstats import show_stats
from showstats.showstats import make_stats_tbl


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
    assert result.shape[0] == 2  # int_col and float_col


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
    assert result.shape[0] == 2  # int_col and float_col


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
    assert result_polars.shape == result_pandas.shape

    # Variable names should be the same
    assert (
        result_polars["Col (N=10)"].to_list() == result_pandas["Col (N=10)"].to_list()
    )

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
    assert result_polars.shape == result_pandas.shape

    # Variable names should be the same
    assert (
        result_polars["Col (N=10)"].to_list() == result_pandas["Col (N=10)"].to_list()
    )

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
    assert result.filter(pl.col("Col (N=10)") == "col_with_nulls")["NA%"][0] == 20

    # col_no_nulls should have 0% NA
    assert result.filter(pl.col("Col (N=10)") == "col_no_nulls")["NA%"][0] == 0


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
    col_with_nulls_row = result.filter(pl.col("Col (N=10)") == "col_with_nulls")
    assert col_with_nulls_row["NA%"][0] == 20

    # col_no_nulls should have 0% NA
    col_no_nulls_row = result.filter(pl.col("Col (N=10)") == "col_no_nulls")
    assert col_no_nulls_row["NA%"][0] == 0


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
    assert result.shape[0] == 2

    # Should have columns: Col (N=10), NA%, Min, Max, Median
    assert "Col (N=10)" in result.columns
    assert "NA%" in result.columns
    assert "Min" in result.columns
    assert "Max" in result.columns
    assert "Median" in result.columns


def test_pandas_backend_datetime():
    """Test pandas backend with datetime columns"""
    df = pd.DataFrame(
        {"datetime_col": pd.date_range("2020-01-01", periods=10, freq="D")}
    )

    result = make_stats_tbl(df, "time")

    assert result is not None
    assert "Median" in result.columns
    # The median of ten consecutive days is midday on the fifth. Getting this
    # right is what the Int64 round-trip in _median_expr is for: a wrong time
    # unit would silently land in 1970.
    assert result.item(0, "Median").startswith("2020-01-05 12:00:00")
    assert result.item(0, "Min").startswith("2020-01-01")
    assert result.item(0, "Max").startswith("2020-01-10")


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
    assert result.shape[0] == 2

    # bool_col should be included
    assert "bool_col" in result["Col (N=10)"].to_list()


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
    assert result.shape[0] >= 1
    # Five True out of ten, so the median of the boolean column is 0.5 —
    # the same value polars reports for median() on a boolean.
    bool_row = result.filter(pl.col(result.columns[0]) == "bool_col")
    assert bool_row.item(0, "Median") == "0.5"


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
    assert "Top 1" in result.columns
    assert "Top 2" in result.columns
    assert "Top 3" in result.columns

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
    assert "Top 1" in result.columns
    assert "Top 2" in result.columns
    assert "Top 3" in result.columns

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

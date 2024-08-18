from showstats.showstats import show_stats
import polars as pl
import pytest


def test_edge_cases():
    # Test with a DataFrame containing only one row
    df_one_row = pl.DataFrame({"a": [1], "b": ["x"]})
    show_stats(df_one_row)

    # Test with a DataFrame containing only one column
    df_one_col = pl.DataFrame({"a": range(100)})
    show_stats(df_one_col)

    # Test with a DataFrame containing all null values
    df_all_null = pl.DataFrame({"a": [None] * 100, "b": [None] * 100})
    show_stats(df_all_null)

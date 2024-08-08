import polars as pl
from showstats.showstats import _make_stats_df


def test__make_stats_df(sample_df):
    summary_table = _make_stats_df(sample_df)
    col_0 = summary_table.columns[0]
    sorted_cols = sorted(sample_df.columns)
    assert sorted(summary_table.get_column(col_0)) == sorted_cols

    assert (
        summary_table.filter(pl.col(col_0).eq("float_mean_2")).item(0, "Mean") == "2.0"
    )
    assert (
        summary_table.filter(pl.col(col_0).eq("float_std_2")).item(0, "Std.") == "2.0"
    )
    assert summary_table.filter(pl.col(col_0).eq("float_min_7")).item(0, "Min") == "7.0"
    assert (
        summary_table.filter(pl.col(col_0).eq("float_max_17")).item(0, "Max") == "17.0"
    )

    summary_table_pandas = _make_stats_df(sample_df.to_pandas())
    assert sorted(summary_table_pandas.get_column(col_0)) == sorted_cols

    # Sorting

    # Test with list
    top_cols = ["int_with_missings", "bool_col"]
    res_sorted = _make_stats_df(sample_df, top_cols)
    assert res_sorted.get_column(res_sorted.columns[0]).head(2).to_list() == top_cols

    # Test with singleton
    res_sorted = _make_stats_df(sample_df, "enum_col")
    assert res_sorted.get_column(res_sorted.columns[0]).item(0) == "enum_col"

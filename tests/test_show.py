import polars as pl
import pytest

from showstats.showstats import show_stats


def test_show(sample_df, capsys):
    show_stats(sample_df)
    captured = capsys.readouterr()
    assert "(N=500)" in captured.out
    assert "float_mean_2" in captured.out
    assert "float_min_-7" in captured.out
    show_stats(sample_df, "cat")
    captured = capsys.readouterr()
    assert "(N=500)" in captured.out
    assert "float_mean_2" not in captured.out
    assert "float_min_-7" not in captured.out
    assert "str_col" in captured.out
    assert "enum_col" in captured.out
    assert "categorical_col" in captured.out
    show_stats(sample_df, "time")
    captured = capsys.readouterr()
    assert "(N=500)" in captured.out
    assert "float_mean_2" not in captured.out
    assert "float_min_-7" not in captured.out
    assert "date_col" in captured.out
    assert "datetime_col" in captured.out
    assert "datetime_col_2" in captured.out
    with pytest.raises(ValueError):
        show_stats(sample_df, "NONSENSE")


def test_show_subsets(sample_df, capsys):
    show_stats(sample_df.select("U", "int_col"))
    captured = capsys.readouterr()
    assert "(N=500)" in captured.out
    assert "int_col" in captured.out

    show_stats(sample_df.select("categorical_col"))
    captured = capsys.readouterr()
    assert "categorical_col" in captured.out


def test_show_empty(sample_df, capsys):
    show_stats(sample_df.select("U", "int_col"), "cat")
    captured = capsys.readouterr()
    assert captured.out == "No categorical columns found\n"
    show_stats(sample_df.select("str_col"), "num")
    captured = capsys.readouterr()
    assert captured.out == "No numerical columns found\n"


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
